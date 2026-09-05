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
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
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
	messages     [][]byte
	msgIdx       int
	statusLog    []StandbyStatus
	mu           sync.Mutex
	closed       bool
	blockUntil   chan struct{}
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
	conn      net.Conn
	writeMu   sync.Mutex
	readMu    sync.Mutex
	opts      CDCOptions
	closed    bool
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
			default:
				return fmt.Errorf("unsupported authentication type %d in replication mode", authType)
			}
		case 'E': // ErrorResponse
			return fmt.Errorf("database error during startup: %s", string(payload))
		}
	}

	// Read until ReadyForQuery ('Z')
	for {
		msgType, _, err := s.readRawMessage()
		if err != nil {
			return err
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

	// 4. Issue START_REPLICATION command
	startLSNStr := "0/0"
	if s.opts.StartLSN != 0 {
		startLSNStr = fmt.Sprintf("%X/%X", uint32(s.opts.StartLSN>>32), uint32(s.opts.StartLSN))
	}
	repQuery := fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL %s (proto_version '1', publication_names '\"%s\"');",
		s.opts.SlotName, startLSNStr, s.opts.Publication)

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
	return s.opts.Password, nil
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
