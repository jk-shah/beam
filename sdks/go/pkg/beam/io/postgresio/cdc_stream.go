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
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
)

var (
	// frameReadTimeout bounds how long a partially read protocol frame may
	// take to complete. It is a transport-level failure detector, not an idle
	// budget: the server has already begun a frame, so silence past this point
	// means the connection is gone rather than quiet.
	//
	// A variable rather than a constant so tests can exercise the expiry
	// without waiting a minute.
	frameReadTimeout = 60 * time.Second

	// maxReplicationFrameBytes bounds the payload this client will allocate for
	// a single protocol frame. It matches PostgreSQL's own 1GB ceiling on a
	// protocol message, so it rejects only lengths the server could not have
	// sent legitimately.
	maxReplicationFrameBytes = 1 << 30

	// standbyWriteTimeout bounds a standby status write. Without it, a write
	// to an unresponsive primary blocks for the kernel's TCP retransmission
	// budget, which stalls the keepalive goroutine and, through it, worker
	// teardown.
	standbyWriteTimeout = 10 * time.Second
)

// errStreamDesynchronized reports that a protocol frame was begun but not
// completed. The connection cannot be reused: the unread remainder of the
// frame would be misread as the next frame's header. Callers must treat this
// as a connection failure and reconnect, never as an idle stream.
var errStreamDesynchronized = errors.New("postgresio: replication frame did not complete")

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

	// idleWhenDrained makes NextMessage block until the caller's deadline once
	// the scripted messages are exhausted, instead of reporting end of stream.
	// That models a connected but quiet database, which is the condition under
	// which a source must still return so its bundle can finalize.
	idleWhenDrained bool
}

// NewMockReplicationStream creates a mock replication stream initialized with the given messages.
func NewMockReplicationStream(messages ...[]byte) *MockReplicationStream {
	return &MockReplicationStream{
		messages:   messages,
		blockUntil: make(chan struct{}),
	}
}

// SetIdleWhenDrained controls what the stream does after its scripted messages
// run out: block until the read deadline (true) or report io.EOF (false).
func (m *MockReplicationStream) SetIdleWhenDrained(idle bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.idleWhenDrained = idle
}

// NextMessage returns the next configured message or io.EOF.
func (m *MockReplicationStream) NextMessage(ctx context.Context) ([]byte, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, io.EOF
	}
	if m.msgIdx >= len(m.messages) {
		idle := m.idleWhenDrained
		m.mu.Unlock()
		if !idle {
			return nil, io.EOF
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	msg := m.messages[m.msgIdx]
	m.msgIdx++
	m.mu.Unlock()
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

	// slotCreation records the outcome of CREATE_REPLICATION_SLOT when this
	// stream created the slot. It carries the exported snapshot name a backfill
	// needs, and is nil when the slot already existed or was not created here.
	slotCreation *SlotCreationResult
}

// NewNativeReplicationStream connects to PostgreSQL in replication mode.
func NewNativeReplicationStream(ctx context.Context, opts CDCOptions) (*NativeReplicationStream, error) {
	if opts.RequiresDialFunc && opts.DialFunc == nil {
		return nil, errDialFuncLost()
	}
	if opts.RequiresTokenProvider && opts.TokenProvider == nil {
		return nil, errTokenProviderLost()
	}
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
	if !s.opts.AllowPublisherRowSecurity {
		// Logical decoding evaluates publisher row security policies unless the
		// role is SUPERUSER or BYPASSRLS. A least-privilege replication role is
		// neither, so without this a table owner can cause policy expressions
		// to run inside the replication session. With row_security=off,
		// PostgreSQL halts replication instead, which is loud and recoverable
		// rather than silent.
		params["options"] = "-c row_security=off"
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

	// 4. Create the replication slot if requested. This must precede
	// START_REPLICATION, which fails if the slot does not exist, and must run on
	// this same replication connection because an exported snapshot is only
	// valid for the session that created it.
	if s.opts.CreateSlotIfMissing {
		if err := s.ensureReplicationSlot(); err != nil {
			return err
		}
	}

	// 5. Issue START_REPLICATION, negotiating protocol options against the
	// server version reported during startup.
	startLSNStr := "0/0"
	if s.opts.StartLSN != 0 {
		startLSNStr = fmt.Sprintf("%X/%X", uint32(s.opts.StartLSN>>32), uint32(s.opts.StartLSN))
	}
	repQuery, unsupported := s.buildStartReplicationQuery(startLSNStr)
	// A requested option that the server cannot honour is omitted rather than
	// sent, so it has to be reported here or the caller has no way to tell that
	// the feature they configured is not in effect.
	for _, reason := range unsupported {
		log.Warnf(ctx, "postgresio: replication option not enabled (server %s): %s", s.serverVersion, reason)
	}

	if err := s.sendQuery(repQuery); err != nil {
		return fmt.Errorf("failed to send START_REPLICATION: %w", err)
	}

	// Server should respond with CopyBothResponse ('W') or CopyOutResponse ('H')
	msgType, payload, err := s.readRawMessage()
	if err != nil {
		return err
	}
	if msgType != 'W' && msgType != 'H' {
		if msgType == 'E' {
			return fmt.Errorf("START_REPLICATION failed: %s", sanitizeErrorPayload(payload))
		}
		return fmt.Errorf("expected CopyResponse ('W' or 'H'), got %c (payload: %s)", msgType, string(payload))
	}

	return nil
}

func (s *NativeReplicationStream) resolvePassword(ctx context.Context) (string, error) {
	if s.opts.RequiresTokenProvider && s.opts.TokenProvider == nil {
		return "", errTokenProviderLost()
	}
	if s.opts.TokenProvider != nil {
		return s.opts.TokenProvider.GetPassword(ctx)
	}
	return s.opts.ResolvePassword(), nil
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

// SlotCreation returns the outcome of CREATE_REPLICATION_SLOT if this stream
// created the slot, and nil otherwise. The SnapshotName it carries is what an
// initial backfill must adopt to read a view of the tables consistent with the
// slot's starting LSN.
func (s *NativeReplicationStream) SlotCreation() *SlotCreationResult {
	return s.slotCreation
}

// ensureReplicationSlot issues CREATE_REPLICATION_SLOT for the configured slot.
//
// A duplicate-slot error is not a failure. The slot is durable server state
// that outlives the pipeline, so on every restart after the first the slot
// already exists; and when several workers start concurrently exactly one wins
// the create. Treating the collision as fatal would make the option unusable
// beyond a single cold start.
//
// When the slot already exists no snapshot is exported, because the snapshot is
// only produced at creation time. Callers must therefore not assume
// SlotCreation() is non-nil.
func (s *NativeReplicationStream) ensureReplicationSlot() error {
	query, err := buildCreateSlotQuery(s.opts.SlotName, "pgoutput", true, s.opts.TwoPhaseCommit,
		s.opts.FailoverSlot, s.serverMajorVersion)
	if err != nil {
		return err
	}

	if err := s.sendQuery(query); err != nil {
		return fmt.Errorf("failed to send CREATE_REPLICATION_SLOT: %w", err)
	}

	var fields []string
	for {
		msgType, payload, err := s.readRawMessage()
		if err != nil {
			return fmt.Errorf("failed reading CREATE_REPLICATION_SLOT response: %w", err)
		}
		switch msgType {
		case 'D': // DataRow: slot_name, consistent_point, snapshot_name, output_plugin
			fields, err = parseDataRow(payload)
			if err != nil {
				return fmt.Errorf("failed parsing CREATE_REPLICATION_SLOT row: %w", err)
			}
		case 'E': // ErrorResponse
			qerr := fmt.Errorf("CREATE_REPLICATION_SLOT failed: %s", sanitizeErrorPayload(payload))
			if isDuplicateSlotError(qerr) {
				// The slot is already present. Drain to ReadyForQuery so the
				// connection is usable for START_REPLICATION.
				if derr := s.drainToReadyForQuery(); derr != nil {
					return derr
				}
				s.slotCreation = &SlotCreationResult{
					SlotName:       s.opts.SlotName,
					AlreadyExisted: true,
				}
				return nil
			}
			return qerr
		case 'Z': // ReadyForQuery
			if fields == nil {
				return fmt.Errorf("CREATE_REPLICATION_SLOT returned no result row for slot %q", s.opts.SlotName)
			}
			res, perr := parseCreateSlotResponse(fields)
			if perr != nil {
				return perr
			}
			s.slotCreation = &res
			return nil
		}
	}
}

// drainToReadyForQuery consumes messages until the server reports it is ready
// for the next command, discarding any further error payloads.
func (s *NativeReplicationStream) drainToReadyForQuery() error {
	for {
		msgType, _, err := s.readRawMessage()
		if err != nil {
			return err
		}
		if msgType == 'Z' {
			return nil
		}
	}
}

// sanitizeErrorPayload renders an ErrorResponse body as readable text. The
// body is a sequence of null-terminated, single-byte-tagged fields; joining
// them with spaces keeps the SQLSTATE and message visible for classification
// without interpreting the wire format field by field.
func sanitizeErrorPayload(payload []byte) string {
	parts := parseNullTerminatedList(payload)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if len(p) > 1 {
			out = append(out, string(p[1:]))
		}
	}
	return strings.Join(out, " ")
}

func (s *NativeReplicationStream) readRawMessage() (byte, []byte, error) {
	return s.readRawMessageBefore(time.Time{})
}

// readRawMessageBefore reads one protocol frame, waiting no later than
// idleDeadline for the frame to begin.
//
// idleDeadline covers only the frame's first byte, and a zero value means wait
// indefinitely, which is the behaviour the handshake path requires. Expiry
// there is benign: no bytes were consumed, so the connection is still framed on
// a message boundary and the caller can come back later.
//
// Once the first byte has been consumed the deadline changes meaning. It
// becomes frameReadTimeout, a hard transport timeout, for two reasons. It must
// not remain at idleDeadline, because a read that expires midway through a
// frame leaves the connection desynchronized: the unread tail would be parsed
// as the next frame's header. It must also not be cleared, because io.ReadFull
// does not observe context cancellation, so a silently dropped TCP connection
// would block the read, and therefore ProcessElement, forever.
//
// A mid-frame expiry is reported as errStreamDesynchronized so callers do not
// mistake it for an idle stream. The connection is unusable at that point and
// the only correct recovery is to drop it and reconnect.
func (s *NativeReplicationStream) readRawMessageBefore(idleDeadline time.Time) (byte, []byte, error) {
	header := make([]byte, 5)

	if !idleDeadline.IsZero() {
		if err := s.conn.SetReadDeadline(idleDeadline); err != nil {
			return 0, nil, err
		}
		if _, err := io.ReadFull(s.conn, header[:1]); err != nil {
			_ = s.conn.SetReadDeadline(time.Time{})
			return 0, nil, err
		}
	} else if _, err := io.ReadFull(s.conn, header[:1]); err != nil {
		return 0, nil, err
	}

	defer func() { _ = s.conn.SetReadDeadline(time.Time{}) }()
	if err := s.conn.SetReadDeadline(time.Now().Add(frameReadTimeout)); err != nil {
		return 0, nil, err
	}
	if _, err := io.ReadFull(s.conn, header[1:]); err != nil {
		return 0, nil, fmt.Errorf("%w: header incomplete: %v", errStreamDesynchronized, err)
	}

	msgType := header[0]
	msgLen := binary.BigEndian.Uint32(header[1:5])
	if msgLen < 4 {
		return 0, nil, fmt.Errorf("invalid message length %d", msgLen)
	}
	payloadLen := int(msgLen - 4)
	// msgLen is read straight off the socket, so a corrupted or desynchronized
	// header can declare up to 4GB and this allocation would attempt it before
	// the read fails. Unlike the in-memory frame parsers there is no buffer to
	// bound it against, so it is bounded by PostgreSQL's own protocol ceiling.
	if payloadLen > maxReplicationFrameBytes {
		return 0, nil, fmt.Errorf("%w: declared payload of %d bytes exceeds maximum %d",
			errStreamDesynchronized, payloadLen, maxReplicationFrameBytes)
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(s.conn, payload); err != nil {
		return 0, nil, fmt.Errorf("%w: payload of %d bytes incomplete: %v", errStreamDesynchronized, payloadLen, err)
	}
	return msgType, payload, nil
}

// NextMessage reads the next CopyData ('d') replication message.
//
// A deadline on ctx bounds how long this blocks waiting for data. The source
// relies on that: it is a self-checkpointing splittable DoFn, and a read that
// blocked indefinitely would stop ProcessElement from ever returning, which in
// turn would stop bundles from finalizing and the replication slot from being
// acknowledged.
func (s *NativeReplicationStream) NextMessage(ctx context.Context) ([]byte, error) {
	s.readMu.Lock()
	defer s.readMu.Unlock()

	var idleDeadline time.Time
	if dl, ok := ctx.Deadline(); ok {
		idleDeadline = dl
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		msgType, payload, err := s.readRawMessageBefore(idleDeadline)
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

	// The write deadline is what keeps worker shutdown bounded. This is called
	// from the session's keepalive goroutine, and a bare Write on an
	// unresponsive TCP connection blocks until the kernel gives up, which can
	// exceed ten minutes. Teardown waits for that goroutine, so without a
	// deadline an unreachable primary stalls the whole worker.
	if err := s.conn.SetWriteDeadline(time.Now().Add(standbyWriteTimeout)); err != nil {
		return err
	}
	defer func() { _ = s.conn.SetWriteDeadline(time.Time{}) }()

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

// negotiateProtoVersion returns the highest pgoutput protocol version the given
// server major version supports.
//
// The thresholds come from the PostgreSQL logical replication protocol
// specification, not from a release the connector was developed against:
//
//	v2  server 14+   streaming of large in-progress transactions
//	v3  server 15+   streaming of two-phase commits
//	v4  server 16+   in-progress streams applied in parallel
//
// A server version of 0 means detection failed, in which case the baseline is
// the only safe choice.
func negotiateProtoVersion(serverMajor int) int {
	switch {
	case serverMajor >= 16:
		return 4
	case serverMajor >= 15:
		return 3
	case serverMajor >= 14:
		return 2
	default:
		return 1
	}
}

// BuildStartReplicationQuery constructs the START_REPLICATION command,
// negotiating the pgoutput protocol version against the connected server's
// actual capabilities.
//
// Protocol version is negotiated upward automatically because declaring a
// higher version does not by itself change the message stream: v2, v3 and v4
// only alter framing once streaming or two_phase is also requested. The
// features that do change decoding -- binary mode and in-progress streaming --
// are left off unless asked for, so enabling them is an explicit decision
// rather than a side effect of upgrading the server.
//
// Where a requested feature exceeds what the server supports, this builds the
// query without it and reports the reason through unsupported. Silently sending
// an unsupported combination is worse than omitting it: PostgreSQL rejects the
// whole START_REPLICATION, and the resulting error names the protocol version
// rather than the option the caller actually set.
func (s *NativeReplicationStream) BuildStartReplicationQuery(startLSNStr string) string {
	query, _ := s.buildStartReplicationQuery(startLSNStr)
	return query
}

func (s *NativeReplicationStream) buildStartReplicationQuery(startLSNStr string) (query string, unsupported []string) {
	var opts []string

	// 1. Protocol version.
	protoVer := negotiateProtoVersion(s.serverMajorVersion)
	if s.opts.ProtoVersion > 0 {
		protoVer = s.opts.ProtoVersion
	}
	opts = append(opts, fmt.Sprintf("proto_version '%d'", protoVer))

	// 2. Publication name.
	opts = append(opts, fmt.Sprintf("publication_names '\"%s\"'", s.opts.Publication))

	// 3. Binary mode. Available from server 14. Off unless requested, because
	// it switches every column onto the binary decode path.
	if s.opts.BinaryMode != nil && *s.opts.BinaryMode {
		if s.serverMajorVersion >= 14 || s.serverMajorVersion == 0 {
			opts = append(opts, "binary 'true'")
		} else {
			unsupported = append(unsupported,
				fmt.Sprintf("binary mode requires server 14 or later, server is %d", s.serverMajorVersion))
		}
	}

	// 4. In-progress transaction streaming. 'on' needs protocol 2, 'parallel'
	// needs protocol 4. Default is off, matching the server's own default.
	switch s.opts.StreamingMode {
	case "", "off":
		// Nothing to send.
	case "on":
		if protoVer >= 2 {
			opts = append(opts, "streaming 'on'")
		} else {
			unsupported = append(unsupported,
				fmt.Sprintf("streaming 'on' requires proto_version 2 or later, negotiated %d", protoVer))
		}
	case "parallel":
		// Degrading to 'on' is deliberately not done: it would silently give
		// the caller serial apply when they asked for parallel.
		if protoVer >= 4 {
			opts = append(opts, "streaming 'parallel'")
		} else {
			unsupported = append(unsupported,
				fmt.Sprintf("streaming 'parallel' requires proto_version 4 or later, negotiated %d", protoVer))
		}
	default:
		unsupported = append(unsupported, fmt.Sprintf("unknown streaming mode %q", s.opts.StreamingMode))
	}

	// 5. Two-phase commit decoding. Requires protocol 3. Previously this was
	// appended unconditionally, which paired two_phase 'true' with
	// proto_version '1' and made the server reject the command outright.
	if s.opts.TwoPhaseCommit {
		if protoVer >= 3 {
			opts = append(opts, "two_phase 'true'")
		} else {
			unsupported = append(unsupported,
				fmt.Sprintf("two-phase commit requires proto_version 3 or later, negotiated %d", protoVer))
		}
	}

	// 6. Replication origin filter (loop prevention).
	if s.opts.OriginFilter == "none" {
		opts = append(opts, "origin 'none'")
	}

	return fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL %s (%s);",
		s.opts.SlotName, startLSNStr, strings.Join(opts, ", ")), unsupported
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
