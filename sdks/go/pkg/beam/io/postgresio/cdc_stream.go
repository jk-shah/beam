// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package postgresio

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"

	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

// ReplicationStream defines the abstraction for reading raw CopyData replication
// frames and sending client keepalive acknowledgements.
type ReplicationStream interface {
	NextMessage(ctx context.Context) ([]byte, error)
	SendStandbyStatus(ctx context.Context, status StandbyStatus) error
	Close() error
}

// ReplicationStreamFactory creates a ReplicationStream for the given options.
type ReplicationStreamFactory func(ctx context.Context, opts CDCOptions) (ReplicationStream, error)

// MockReplicationStream provides an in-memory stream for deterministic testing.
type MockReplicationStream struct {
	messages   [][]byte
	msgIdx     int
	statusLog  []StandbyStatus
	mu         sync.Mutex
	closed     bool
	blockUntil chan struct{}
}

// NewMockReplicationStream creates a mock replication stream initialized with the given messages.
func NewMockReplicationStream(messages ...[]byte) *MockReplicationStream {
	return &MockReplicationStream{
		messages:   messages,
		blockUntil: make(chan struct{}),
	}
}

// NextMessage returns the next configured message or io.EOF.
func (m *MockReplicationStream) NextMessage(ctx context.Context) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, io.EOF
	}
	if m.msgIdx >= len(m.messages) {
		return nil, io.EOF
	}
	msg := m.messages[m.msgIdx]
	m.msgIdx++
	return msg, nil
}

// SendStandbyStatus records a status update sent by the client or heartbeat.
func (m *MockReplicationStream) SendStandbyStatus(ctx context.Context, status StandbyStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statusLog = append(m.statusLog, status)
	return nil
}

// Close marks the mock stream closed.
func (m *MockReplicationStream) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// GetStatusLog returns all received standby status updates.
func (m *MockReplicationStream) GetStatusLog() []StandbyStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make([]StandbyStatus, len(m.statusLog))
	copy(res, m.statusLog)
	return res
}

// NativeReplicationStream establishes a pure-Go logical replication connection over TCP/TLS.
type NativeReplicationStream struct {
	conn               net.Conn
	writeMu            sync.Mutex
	readMu             sync.Mutex
	opts               CDCOptions
	serverMajorVersion int
	serverVersion      string
	closed             bool
}

// NewNativeReplicationStream connects to PostgreSQL in replication mode.
func NewNativeReplicationStream(ctx context.Context, opts CDCOptions) (*NativeReplicationStream, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	addr := fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	var conn net.Conn
	var err error

	if opts.DialFunc != nil {
		conn, err = opts.DialFunc(ctx, "tcp", addr)
	} else {
		dialer := &net.Dialer{Timeout: 10 * time.Second}
		conn, err = dialer.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to dial postgres host %s: %w", addr, err)
	}

	// Negotiate SSL/TLS via the PostgreSQL SSLRequest protocol.
	//
	// buildTLSConfig returns a nil config only for sslmode=disable, so an unset
	// or empty mode negotiates TLS rather than silently falling back to
	// cleartext.
	tlsConfig, err := buildTLSConfig(tlsSettings{
		Mode:     opts.SSLMode,
		Host:     opts.Host,
		RootCert: opts.SSLRootCert,
		Cert:     opts.SSLCert,
		Key:      opts.SSLKey,
	})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	if tlsConfig != nil {
		sslReq := []byte{0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f}
		if _, err := conn.Write(sslReq); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("failed to send SSLRequest: %w", err)
		}
		var sslResp [1]byte
		if _, err := io.ReadFull(conn, sslResp[:]); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("failed to read SSLRequest response: %w", err)
		}
		if sslResp[0] == 'S' {
			tlsConn := tls.Client(conn, tlsConfig)
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				_ = conn.Close()
				return nil, fmt.Errorf("TLS handshake failed: %w", err)
			}
			conn = tlsConn
		} else {
			// The server refused TLS. Every mode that reaches this point asked
			// for encryption, so this is a hard failure rather than a downgrade.
			_ = conn.Close()
			return nil, fmt.Errorf("postgresio: server does not support SSL but sslmode=%q requires it", opts.SSLMode)
		}
	}

	stream := &NativeReplicationStream{
		conn: conn,
		opts: opts,
	}

	if err := stream.handshake(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("replication handshake failed: %w", err)
	}

	return stream, nil
}

func (s *NativeReplicationStream) handshake(ctx context.Context) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	// 1. Send StartupMessage with replication=database
	var body []byte
	body = binary.BigEndian.AppendUint32(body, 196608) // Protocol 3.0
	params := map[string]string{
		"user":        s.opts.Username,
		"database":    s.opts.Database,
		"replication": "database",
	}
	for k, v := range params {
		body = append(body, []byte(k)...)
		body = append(body, 0)
		body = append(body, []byte(v)...)
		body = append(body, 0)
	}
	body = append(body, 0) // Terminating null

	msgLen := uint32(len(body) + 4)
	var startupPacket []byte
	startupPacket = binary.BigEndian.AppendUint32(startupPacket, msgLen)
	startupPacket = append(startupPacket, body...)

	if _, err := s.conn.Write(startupPacket); err != nil {
		return fmt.Errorf("failed to write startup packet: %w", err)
	}

	// 2. Read authentication and readiness
	authenticated := false
	for !authenticated {
		msgType, payload, err := s.readRawMessage()
		if err != nil {
			return err
		}
		switch msgType {
		case 'R': // Authentication request
			authType := binary.BigEndian.Uint32(payload[0:4])
			switch authType {
			case 0: // AuthenticationOk
				authenticated = true
			case 3: // CleartextPassword
				pwd, err := s.resolvePassword(ctx)
				if err != nil {
					return err
				}
				if err := s.sendPasswordMessage(pwd); err != nil {
					return err
				}
			case 5: // MD5Password
				pwd, err := s.resolvePassword(ctx)
				if err != nil {
					return err
				}
				if len(payload) < 8 {
					return fmt.Errorf("malformed MD5 auth challenge")
				}
				salt := payload[4:8]
				h1 := md5.Sum([]byte(pwd + s.opts.Username))
				hex1 := hex.EncodeToString(h1[:])
				h2 := md5.Sum(append([]byte(hex1), salt...))
				token := "md5" + hex.EncodeToString(h2[:])
				if err := s.sendPasswordMessage(token); err != nil {
					return err
				}
			case 10: // AuthenticationSASL
				pwd, err := s.resolvePassword(ctx)
				if err != nil {
					return err
				}
				if err := s.authenticateSASL(payload[4:], pwd); err != nil {
					return err
				}
			case 11, 12:
				// SASLContinue / SASLFinal are consumed inside authenticateSASL.
				return fmt.Errorf("unexpected SASL message %d outside of a SASL exchange", authType)
			default:
				return fmt.Errorf("unsupported authentication type %d in replication mode", authType)
			}

		case 'E': // ErrorResponse
			return fmt.Errorf("database error during startup: %s", string(payload))
		}
	}

	// Read until ReadyForQuery ('Z'), capturing ParameterStatus ('S')
	for {
		msgType, payload, err := s.readRawMessage()
		if err != nil {
			return err
		}
		if msgType == 'S' {
			parts := bytes.Split(payload, []byte{0})
			if len(parts) >= 2 {
				paramName := string(parts[0])
				paramVal := string(parts[1])
				if paramName == "server_version" {
					s.serverVersion = paramVal
					s.serverMajorVersion = parseMajorVersion(paramVal)
				}
			}
		}
		if msgType == 'Z' {
			break
		}
	}

	// 3. Issue search_path isolation query
	if err := s.sendQuery("SET search_path = pg_catalog, pg_temp;"); err != nil {
		return fmt.Errorf("failed to enforce search_path isolation: %w", err)
	}
	if err := s.drainQueryResponses(); err != nil {
		return fmt.Errorf("failed during search_path setup: %w", err)
	}

	// 4. Issue START_REPLICATION command with version negotiation (Options A & B for PG >= 19)
	startLSNStr := "0/0"
	if s.opts.StartLSN != 0 {
		startLSNStr = fmt.Sprintf("%X/%X", uint32(s.opts.StartLSN>>32), uint32(s.opts.StartLSN))
	}
	repQuery := s.BuildStartReplicationQuery(startLSNStr)

	if err := s.sendQuery(repQuery); err != nil {
		return fmt.Errorf("failed to send START_REPLICATION: %w", err)
	}

	// Server should respond with CopyBothResponse ('W') or CopyOutResponse ('H')
	msgType, _, err := s.readRawMessage()
	if err != nil {
		return err
	}
	if msgType != 'W' && msgType != 'H' {
		return fmt.Errorf("expected CopyResponse ('W' or 'H'), got %c", msgType)
	}

	return nil
}

func (s *NativeReplicationStream) resolvePassword(ctx context.Context) (string, error) {
	if s.opts.TokenProvider != nil {
		return s.opts.TokenProvider.GetPassword(ctx)
	}
	if s.opts.Password != "" {
		return s.opts.Password, nil
	}
	return os.Getenv("PGPASSWORD"), nil
}

func (s *NativeReplicationStream) sendPasswordMessage(password string) error {
	payload := append([]byte(password), 0)
	packet := make([]byte, 5+len(payload))
	packet[0] = 'p'
	binary.BigEndian.PutUint32(packet[1:5], uint32(len(packet)-1))
	copy(packet[5:], payload)
	_, err := s.conn.Write(packet)
	return err
}

// authenticateSASL runs the SASL exchange that follows AuthenticationSASL.
//
// mechanismList is the payload of the AuthenticationSASL message after the
// auth-type word: a sequence of null-terminated mechanism names ended by an
// empty name.
func (s *NativeReplicationStream) authenticateSASL(mechanismList []byte, password string) error {
	mechanisms := parseNullTerminatedList(mechanismList)

	client, err := newSCRAMClient(s.opts.Username, password, mechanisms, s.channelBindingData())
	if err != nil {
		return err
	}

	// SASLInitialResponse: mechanism name, then an int32 length, then the
	// client-first-message.
	clientFirst := client.ClientFirst()
	if err := s.sendSASLInitialResponse(client.Mechanism(), clientFirst); err != nil {
		return fmt.Errorf("failed to send SASLInitialResponse: %w", err)
	}

	// Expect AuthenticationSASLContinue (11) carrying server-first-message.
	serverFirst, err := s.readSASLMessage(11)
	if err != nil {
		return err
	}

	clientFinal, err := client.ClientFinal(serverFirst)
	if err != nil {
		return err
	}

	if err := s.sendSASLResponse(clientFinal); err != nil {
		return fmt.Errorf("failed to send SASLResponse: %w", err)
	}

	// Expect AuthenticationSASLFinal (12) carrying the server signature.
	serverFinal, err := s.readSASLMessage(12)
	if err != nil {
		return err
	}

	// Mutual authentication: prove the server also knows the password.
	return client.VerifyServerFinal(serverFinal)
}

// sendSASLInitialResponse writes the 'p' message that opens a SASL exchange.
func (s *NativeReplicationStream) sendSASLInitialResponse(mechanism, clientFirst string) error {
	var payload []byte
	payload = append(payload, mechanism...)
	payload = append(payload, 0)
	payload = binary.BigEndian.AppendUint32(payload, uint32(len(clientFirst)))
	payload = append(payload, clientFirst...)

	packet := make([]byte, 5+len(payload))
	packet[0] = 'p'
	binary.BigEndian.PutUint32(packet[1:5], uint32(len(packet)-1))
	copy(packet[5:], payload)

	_, err := s.conn.Write(packet)
	return err
}

// sendSASLResponse writes a continuation 'p' message with a raw SASL payload.
func (s *NativeReplicationStream) sendSASLResponse(data string) error {
	packet := make([]byte, 5+len(data))
	packet[0] = 'p'
	binary.BigEndian.PutUint32(packet[1:5], uint32(len(packet)-1))
	copy(packet[5:], data)

	_, err := s.conn.Write(packet)
	return err
}

// readSASLMessage reads one 'R' message and asserts the expected auth subtype.
func (s *NativeReplicationStream) readSASLMessage(wantAuthType uint32) (string, error) {
	msgType, payload, err := s.readRawMessage()
	if err != nil {
		return "", fmt.Errorf("failed to read SASL response: %w", err)
	}
	if msgType == 'E' {
		return "", fmt.Errorf("database error during SASL authentication: %s", string(payload))
	}
	if msgType != 'R' {
		return "", fmt.Errorf("expected an authentication message during SASL exchange, got %q", msgType)
	}
	if len(payload) < 4 {
		return "", fmt.Errorf("malformed authentication message during SASL exchange")
	}

	gotAuthType := binary.BigEndian.Uint32(payload[0:4])
	if gotAuthType != wantAuthType {
		return "", fmt.Errorf("expected SASL auth type %d, got %d", wantAuthType, gotAuthType)
	}

	return string(payload[4:]), nil
}

// channelBindingData returns tls-server-end-point binding data when the
// connection is TLS, and nil otherwise.
//
// RFC 5929 defines tls-server-end-point as the hash of the server
// certificate, using the certificate's own signature hash algorithm, with
// SHA-256 substituted when that algorithm is MD5 or SHA-1.
func (s *NativeReplicationStream) channelBindingData() []byte {
	tlsConn, ok := s.conn.(*tls.Conn)
	if !ok {
		return nil
	}

	certs := tlsConn.ConnectionState().PeerCertificates
	if len(certs) == 0 {
		return nil
	}

	var sum []byte
	switch certs[0].SignatureAlgorithm {
	case x509.SHA384WithRSA, x509.ECDSAWithSHA384, x509.SHA384WithRSAPSS:
		h := sha512.Sum384(certs[0].Raw)
		sum = h[:]
	case x509.SHA512WithRSA, x509.ECDSAWithSHA512, x509.SHA512WithRSAPSS:
		h := sha512.Sum512(certs[0].Raw)
		sum = h[:]
	default:
		// Covers SHA-256 signatures and the MD5/SHA-1 substitution rule.
		h := sha256.Sum256(certs[0].Raw)
		sum = h[:]
	}
	return sum
}

// parseNullTerminatedList splits a PostgreSQL null-terminated string list.
func parseNullTerminatedList(b []byte) []string {
	var out []string
	for _, part := range bytes.Split(b, []byte{0}) {
		if len(part) > 0 {
			out = append(out, string(part))
		}
	}
	return out
}

func (s *NativeReplicationStream) sendQuery(query string) error {
	payload := append([]byte(query), 0)
	packet := make([]byte, 5+len(payload))
	packet[0] = 'Q'
	binary.BigEndian.PutUint32(packet[1:5], uint32(len(packet)-1))
	copy(packet[5:], payload)
	_, err := s.conn.Write(packet)
	return err
}

func (s *NativeReplicationStream) drainQueryResponses() error {
	for {
		msgType, payload, err := s.readRawMessage()
		if err != nil {
			return err
		}
		if msgType == 'Z' {
			return nil
		}
		if msgType == 'E' {
			return fmt.Errorf("query error: %s", string(payload))
		}
	}
}

func (s *NativeReplicationStream) readRawMessage() (byte, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(s.conn, header); err != nil {
		return 0, nil, err
	}
	msgType := header[0]
	msgLen := binary.BigEndian.Uint32(header[1:5])
	if msgLen < 4 {
		return 0, nil, fmt.Errorf("invalid message length %d", msgLen)
	}
	payloadLen := int(msgLen - 4)
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(s.conn, payload); err != nil {
		return 0, nil, err
	}
	return msgType, payload, nil
}

// NextMessage reads the next CopyData ('d') replication message.
func (s *NativeReplicationStream) NextMessage(ctx context.Context) ([]byte, error) {
	s.readMu.Lock()
	defer s.readMu.Unlock()

	for {
		msgType, payload, err := s.readRawMessage()
		if err != nil {
			return nil, err
		}
		switch msgType {
		case 'd': // CopyData
			return payload, nil
		case 'c': // CopyDone
			return nil, io.EOF
		case 'E': // ErrorResponse
			return nil, fmt.Errorf("replication stream error: %s", string(payload))
		}
	}
}

// SendStandbyStatus sends a CopyData ('d') message with StandbyStatusUpdate ('r').
// Protected by writeMu to ensure it does not interleave with other connection writes.
func (s *NativeReplicationStream) SendStandbyStatus(ctx context.Context, status StandbyStatus) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	body := FormatStandbyStatusUpdate(status.WriteLSN, status.FlushLSN, status.ApplyLSN, status.ClientTime, status.ReplyRequested)
	packet := make([]byte, 5+len(body))
	packet[0] = 'd'
	binary.BigEndian.PutUint32(packet[1:5], uint32(len(packet)-1))
	copy(packet[5:], body)

	_, err := s.conn.Write(packet)
	return err
}

// Close terminates the TCP connection.
func (s *NativeReplicationStream) Close() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.closed = true
	return s.conn.Close()
}

// ServerMajorVersion returns the parsed major version of the connected PostgreSQL server (e.g. 19, 18, 16).
func (s *NativeReplicationStream) ServerMajorVersion() int {
	return s.serverMajorVersion
}

// ServerVersion returns the raw server_version string reported by PostgreSQL ParameterStatus.
func (s *NativeReplicationStream) ServerVersion() string {
	return s.serverVersion
}

// BuildStartReplicationQuery constructs the START_REPLICATION command, automatically negotiating
// proto_version '4', binary mode 'true' (Option A), and parallel streaming 'parallel' (Option B)
// when connected to PostgreSQL >= 19, while reverting cleanly to proto_version '1' without binary
// or parallel streaming on older PostgreSQL versions.
func (s *NativeReplicationStream) BuildStartReplicationQuery(startLSNStr string) string {
	var opts []string

	isPG19OrHigher := s.serverMajorVersion >= 19

	// 1. Protocol Version (Option A): 4 for PG >= 19, 1 for older PG
	protoVer := 1
	if isPG19OrHigher {
		protoVer = 4
	}
	if s.opts.ProtoVersion > 0 {
		protoVer = s.opts.ProtoVersion
	}
	opts = append(opts, fmt.Sprintf("proto_version '%d'", protoVer))

	// 2. Publication name
	opts = append(opts, fmt.Sprintf("publication_names '\"%s\"'", s.opts.Publication))

	// 3. Binary Mode (Option A): binary 'true' only on PostgreSQL >= 19 or if explicitly enabled
	useBinary := isPG19OrHigher
	if s.opts.BinaryMode != nil {
		useBinary = *s.opts.BinaryMode
	}
	if useBinary {
		opts = append(opts, "binary 'true'")
	}

	// 4. In-progress transaction streaming (Option B): streaming 'parallel' on PostgreSQL >= 19 or if explicitly enabled
	streaming := ""
	if isPG19OrHigher {
		streaming = "parallel"
	}
	if s.opts.StreamingMode != "" {
		streaming = s.opts.StreamingMode
	}
	if streaming != "" && streaming != "off" {
		opts = append(opts, fmt.Sprintf("streaming '%s'", streaming))
	}

	// 5. Two-phase commit decoding
	if s.opts.TwoPhaseCommit {
		opts = append(opts, "two_phase 'true'")
	}

	// 6. Replication origin filter (loop prevention)
	if s.opts.OriginFilter == "none" {
		opts = append(opts, "origin 'none'")
	}

	return fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL %s (%s);",
		s.opts.SlotName, startLSNStr, strings.Join(opts, ", "))
}

// parseMajorVersion extracts the leading integer from a server_version string (e.g. "19.0", "18.6 (Debian 18.6-1)").
func parseMajorVersion(verStr string) int {
	var major int
	for _, ch := range verStr {
		if ch >= '0' && ch <= '9' {
			major = major*10 + int(ch-'0')
		} else {
			break
		}
	}
	return major
}
