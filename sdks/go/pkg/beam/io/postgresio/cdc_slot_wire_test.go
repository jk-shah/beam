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
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// --- wire helpers for a stub replication server over net.Pipe ---

var (
	pgAuthOk      = []byte{'R', 0, 0, 0, 8, 0, 0, 0, 0}
	pgReadyForQry = []byte{'Z', 0, 0, 0, 5, 'I'}
	pgCopyBoth    = []byte{'W', 0, 0, 0, 7, 0, 0, 0}
)

// readStartupMessage consumes the length-prefixed startup packet, which unlike
// every later message carries no leading type byte, and returns the parameters
// it carried.
func readStartupMessage(conn net.Conn) (map[string]string, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return nil, err
	}
	body := make([]byte, binary.BigEndian.Uint32(lenBuf[:])-4)
	if _, err := io.ReadFull(conn, body); err != nil {
		return nil, err
	}
	return parseStartupParams(body), nil
}

// parseStartupParams reads the NUL-separated key/value pairs that follow the
// four-byte protocol version.
func parseStartupParams(body []byte) map[string]string {
	params := map[string]string{}
	if len(body) < 4 {
		return params
	}
	fields := bytes.Split(body[4:], []byte{0})
	for i := 0; i+1 < len(fields); i += 2 {
		key := string(fields[i])
		if key == "" {
			break
		}
		params[key] = string(fields[i+1])
	}
	return params
}

// captureStartupParams drives a real connection attempt far enough to read the
// startup packet, then abandons it. The handshake is expected not to complete;
// the parameters are what the test is after.
func captureStartupParams(t *testing.T, extra ...CDCOption) map[string]string {
	t.Helper()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	got := make(chan map[string]string, 1)
	go func() {
		params, err := readStartupMessage(serverConn)
		if err != nil {
			close(got)
		} else {
			got <- params
		}
		serverConn.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := []CDCOption{
		WithCDCHost("localhost"),
		WithCDCPort(5432),
		WithCDCDatabase("testdb"),
		WithCDCUsername("beam_test"),
		WithCDCSlotName("beam_slot"),
		WithCDCPublication("test_pub"),
		WithCDCSSLMode("disable"),
		WithCDCDialFunc(func(ctx context.Context, network, addr string) (net.Conn, error) {
			return clientConn, nil
		}),
	}
	opts = append(opts, extra...)

	// Expected to fail: the stub closes the connection after the startup packet.
	if stream, err := NewNativeReplicationStream(ctx, NewCDCOptions(opts...)); err == nil {
		stream.Close()
	}

	select {
	case params, ok := <-got:
		if !ok {
			t.Fatal("the startup packet was never read")
		}
		return params
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the startup packet")
		return nil
	}
}

// TestReplicationConnectionDisablesRowSecurity covers the default.
//
// Logical decoding evaluates publisher row security policies unless the role is
// SUPERUSER or BYPASSRLS. The least-privilege role this connector documents is
// neither, so without row_security=off a table owner can cause policy
// expressions to execute inside the replication session. With it, PostgreSQL
// halts replication instead, which is loud and recoverable.
func TestReplicationConnectionDisablesRowSecurity(t *testing.T) {
	params := captureStartupParams(t)

	got, ok := params["options"]
	if !ok {
		t.Fatalf("the startup packet carried no options parameter, so publisher row security policies "+
			"will execute under the replication role; params=%v", params)
	}
	if !strings.Contains(got, "row_security=off") {
		t.Errorf("options = %q, want it to contain row_security=off", got)
	}
}

// TestReplicationConnectionCanAllowRowSecurity checks the escape hatch, for a
// published table that legitimately carries a policy and where halting is worse
// than evaluating it.
func TestReplicationConnectionCanAllowRowSecurity(t *testing.T) {
	params := captureStartupParams(t, WithCDCAllowPublisherRowSecurity(true))

	if got, ok := params["options"]; ok && strings.Contains(got, "row_security=off") {
		t.Errorf("options = %q, but row security was explicitly allowed", got)
	}
}

// readSimpleQuery consumes one 'Q' message and returns the SQL text without
// its terminating NUL.
func readSimpleQuery(conn net.Conn) (string, error) {
	var typeBuf [1]byte
	if _, err := io.ReadFull(conn, typeBuf[:]); err != nil {
		return "", err
	}
	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return "", err
	}
	body := make([]byte, binary.BigEndian.Uint32(lenBuf[:])-4)
	if _, err := io.ReadFull(conn, body); err != nil {
		return "", err
	}
	return strings.TrimRight(string(body), "\x00"), nil
}

// encodeDataRow builds a 'D' message carrying the given text fields. An empty
// string is encoded as SQL NULL (length -1), matching what PostgreSQL returns
// for snapshot_name under NOEXPORT_SNAPSHOT.
func encodeDataRow(fields ...string) []byte {
	body := make([]byte, 2)
	binary.BigEndian.PutUint16(body[0:2], uint16(len(fields)))
	for _, f := range fields {
		var lenBytes [4]byte
		if f == "" {
			binary.BigEndian.PutUint32(lenBytes[:], ^uint32(0)) // -1
			body = append(body, lenBytes[:]...)
			continue
		}
		binary.BigEndian.PutUint32(lenBytes[:], uint32(len(f)))
		body = append(body, lenBytes[:]...)
		body = append(body, f...)
	}

	msg := make([]byte, 5+len(body))
	msg[0] = 'D'
	binary.BigEndian.PutUint32(msg[1:5], uint32(len(body)+4))
	copy(msg[5:], body)
	return msg
}

// encodeErrorResponse builds an 'E' message with the severity, SQLSTATE and
// message fields PostgreSQL always supplies.
func encodeErrorResponse(sqlState, message string) []byte {
	var body []byte
	body = append(body, 'S')
	body = append(body, "ERROR"...)
	body = append(body, 0)
	body = append(body, 'C')
	body = append(body, sqlState...)
	body = append(body, 0)
	body = append(body, 'M')
	body = append(body, message...)
	body = append(body, 0)
	body = append(body, 0) // terminator

	msg := make([]byte, 5+len(body))
	msg[0] = 'E'
	binary.BigEndian.PutUint32(msg[1:5], uint32(len(body)+4))
	copy(msg[5:], body)
	return msg
}

// slotStubResponse tells the stub server how to answer CREATE_REPLICATION_SLOT.
type slotStubResponse struct {
	dataRow []string // sent as a 'D' row when non-nil
	errCode string   // sent as an 'E' response when non-empty
	errMsg  string
}

// runSlotStubServer speaks just enough of the PostgreSQL v3 protocol to carry a
// replication handshake to the point where the stream is live, recording every
// simple query the client issued so a test can assert on the exact command
// sequence.
func runSlotStubServer(t *testing.T, conn net.Conn, resp *slotStubResponse, queries *[]string, done chan<- struct{}) {
	t.Helper()
	go func() {
		defer close(done)

		if _, err := readStartupMessage(conn); err != nil {
			return
		}
		if _, err := conn.Write(pgAuthOk); err != nil {
			return
		}
		if _, err := conn.Write(pgReadyForQry); err != nil {
			return
		}

		// search_path isolation query.
		q, err := readSimpleQuery(conn)
		if err != nil {
			return
		}
		*queries = append(*queries, q)
		if _, err := conn.Write(pgReadyForQry); err != nil {
			return
		}

		// CREATE_REPLICATION_SLOT, only when the client is configured to send it.
		if resp != nil {
			q, err = readSimpleQuery(conn)
			if err != nil {
				return
			}
			*queries = append(*queries, q)

			switch {
			case resp.errCode != "":
				if _, err := conn.Write(encodeErrorResponse(resp.errCode, resp.errMsg)); err != nil {
					return
				}
			case resp.dataRow != nil:
				if _, err := conn.Write(encodeDataRow(resp.dataRow...)); err != nil {
					return
				}
			}
			if _, err := conn.Write(pgReadyForQry); err != nil {
				return
			}
		}

		// START_REPLICATION.
		q, err = readSimpleQuery(conn)
		if err != nil {
			return
		}
		*queries = append(*queries, q)
		if _, err := conn.Write(pgCopyBoth); err != nil {
			return
		}
	}()
}

func slotStubOptions(clientConn net.Conn, createSlot bool) CDCOptions {
	opts := []CDCOption{
		WithCDCHost("localhost"),
		WithCDCPort(5432),
		WithCDCDatabase("testdb"),
		WithCDCUsername("beam_test"),
		WithCDCSlotName("beam_slot"),
		WithCDCPublication("test_pub"),
		WithCDCSSLMode("disable"),
		WithCDCCreateSlotIfMissing(createSlot),
		WithCDCDialFunc(func(ctx context.Context, network, addr string) (net.Conn, error) {
			return clientConn, nil
		}),
	}
	return NewCDCOptions(opts...)
}

// TestHandshakeCreatesSlotWhenRequested is the behavioral proof for FINDING
// P0-5. It drives the real handshake against a stub server and asserts that the
// connector actually put CREATE_REPLICATION_SLOT on the wire before
// START_REPLICATION, and captured the exported snapshot name.
func TestHandshakeCreatesSlotWhenRequested(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var queries []string
	done := make(chan struct{})
	runSlotStubServer(t, serverConn, &slotStubResponse{
		dataRow: []string{"beam_slot", "0/16B3748", "00000003-00000002-1", "pgoutput"},
	}, &queries, done)

	stream, err := NewNativeReplicationStream(ctx, slotStubOptions(clientConn, true))
	if err != nil {
		t.Fatalf("NewNativeReplicationStream() err = %v", err)
	}
	defer stream.Close()

	<-done

	var createQuery string
	createIdx, startIdx := -1, -1
	for i, q := range queries {
		if strings.Contains(q, "CREATE_REPLICATION_SLOT") {
			createQuery = q
			createIdx = i
		}
		if strings.Contains(q, "START_REPLICATION") {
			startIdx = i
		}
	}

	if createIdx == -1 {
		t.Fatalf("client never sent CREATE_REPLICATION_SLOT; WithCDCCreateSlotIfMissing is still inert. queries=%v", queries)
	}
	if startIdx == -1 {
		t.Fatalf("client never sent START_REPLICATION. queries=%v", queries)
	}
	if createIdx > startIdx {
		t.Errorf("CREATE_REPLICATION_SLOT was sent after START_REPLICATION; the slot must exist first. queries=%v", queries)
	}
	if !strings.Contains(createQuery, "EXPORT_SNAPSHOT") || strings.Contains(createQuery, "NOEXPORT_SNAPSHOT") {
		t.Errorf("slot must be created with EXPORT_SNAPSHOT so a backfill can read a consistent view, got %q", createQuery)
	}

	res := stream.SlotCreation()
	if res == nil {
		t.Fatal("SlotCreation() = nil; the exported snapshot name was not captured")
	}
	if res.SnapshotName != "00000003-00000002-1" {
		t.Errorf("SnapshotName = %q, want 00000003-00000002-1", res.SnapshotName)
	}
	if res.ConsistentLSN != "0/16B3748" {
		t.Errorf("ConsistentLSN = %q, want 0/16B3748", res.ConsistentLSN)
	}
	if res.AlreadyExisted {
		t.Error("AlreadyExisted = true for a freshly created slot")
	}
}

// TestHandshakeSkipsSlotCreationByDefault asserts the option is opt-in. A
// connector that created slots unasked would leave WAL-retaining server state
// behind after a typo in the slot name.
func TestHandshakeSkipsSlotCreationByDefault(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var queries []string
	done := make(chan struct{})
	runSlotStubServer(t, serverConn, nil, &queries, done)

	stream, err := NewNativeReplicationStream(ctx, slotStubOptions(clientConn, false))
	if err != nil {
		t.Fatalf("NewNativeReplicationStream() err = %v", err)
	}
	defer stream.Close()

	<-done

	for _, q := range queries {
		if strings.Contains(q, "CREATE_REPLICATION_SLOT") {
			t.Errorf("client sent CREATE_REPLICATION_SLOT without the option being set: %q", q)
		}
	}
	if stream.SlotCreation() != nil {
		t.Error("SlotCreation() must be nil when no slot was created")
	}
}

// TestHandshakeToleratesExistingSlot covers the common case: every pipeline
// restart after the first finds the slot already present. Failing there would
// make the option usable exactly once.
func TestHandshakeToleratesExistingSlot(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var queries []string
	done := make(chan struct{})
	runSlotStubServer(t, serverConn, &slotStubResponse{
		errCode: "42710",
		errMsg:  `replication slot "beam_slot" already exists`,
	}, &queries, done)

	stream, err := NewNativeReplicationStream(ctx, slotStubOptions(clientConn, true))
	if err != nil {
		t.Fatalf("handshake failed on a pre-existing slot, which happens on every restart: %v", err)
	}
	defer stream.Close()

	<-done

	sawStart := false
	for _, q := range queries {
		if strings.Contains(q, "START_REPLICATION") {
			sawStart = true
		}
	}
	if !sawStart {
		t.Errorf("replication did not start after a duplicate-slot error. queries=%v", queries)
	}

	res := stream.SlotCreation()
	if res == nil {
		t.Fatal("SlotCreation() = nil after a duplicate-slot error")
	}
	if !res.AlreadyExisted {
		t.Error("AlreadyExisted = false, want true for a pre-existing slot")
	}
	if res.SnapshotName != "" {
		t.Errorf("SnapshotName = %q; no snapshot is exported when the slot already exists, "+
			"and reporting one would make a backfill read at the wrong LSN", res.SnapshotName)
	}
}

// TestHandshakeFailsOnNonDuplicateSlotError asserts real errors still surface.
// Insufficient privilege is the usual one, and silently continuing would start
// replication against a slot that does not exist.
func TestHandshakeFailsOnNonDuplicateSlotError(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var queries []string
	done := make(chan struct{})
	runSlotStubServer(t, serverConn, &slotStubResponse{
		errCode: "42501",
		errMsg:  "permission denied to create replication slot",
	}, &queries, done)

	_, err := NewNativeReplicationStream(ctx, slotStubOptions(clientConn, true))
	if err == nil {
		t.Fatal("handshake succeeded despite a permission error creating the slot")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("error does not name the cause, which makes this undiagnosable: %v", err)
	}
}

// --- DataRow wire parsing ---

func TestParseDataRow(t *testing.T) {
	fields, err := parseDataRow(encodeDataRow("beam_slot", "0/16B3748", "snap-1", "pgoutput")[5:])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"beam_slot", "0/16B3748", "snap-1", "pgoutput"}
	if len(fields) != len(want) {
		t.Fatalf("got %d fields, want %d: %v", len(fields), len(want), fields)
	}
	for i := range want {
		if fields[i] != want[i] {
			t.Errorf("field %d = %q, want %q", i, fields[i], want[i])
		}
	}
}

// TestParseDataRowNullField covers the NOEXPORT_SNAPSHOT case, where
// snapshot_name arrives as SQL NULL rather than an empty value.
func TestParseDataRowNullField(t *testing.T) {
	fields, err := parseDataRow(encodeDataRow("beam_slot", "0/16B3748", "", "pgoutput")[5:])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fields) != 4 {
		t.Fatalf("got %d fields, want 4: %v", len(fields), fields)
	}
	if fields[2] != "" {
		t.Errorf("NULL field decoded as %q, want empty string", fields[2])
	}
	if fields[3] != "pgoutput" {
		t.Errorf("field after a NULL decoded as %q; the NULL length was likely consumed as data", fields[3])
	}
}

func TestParseDataRowRejectsTruncated(t *testing.T) {
	full := encodeDataRow("beam_slot", "0/16B3748")[5:]

	for _, n := range []int{0, 1, 3, 6, len(full) - 1} {
		if n < 0 || n >= len(full) {
			continue
		}
		if _, err := parseDataRow(full[:n]); err == nil {
			t.Errorf("parseDataRow accepted a %d-byte truncation of a %d-byte row", n, len(full))
		}
	}
}

func TestParseDataRowEmptyRow(t *testing.T) {
	fields, err := parseDataRow([]byte{0, 0})
	if err != nil {
		t.Fatalf("unexpected error on a zero-field row: %v", err)
	}
	if len(fields) != 0 {
		t.Errorf("got %d fields, want 0", len(fields))
	}
}

func TestSanitizeErrorPayload(t *testing.T) {
	body := encodeErrorResponse("42710", `replication slot "beam_slot" already exists`)[5:]
	got := sanitizeErrorPayload(body)

	for _, want := range []string{"ERROR", "42710", "already exists"} {
		if !strings.Contains(got, want) {
			t.Errorf("sanitizeErrorPayload output %q does not contain %q", got, want)
		}
	}
	// The rendered text is what isDuplicateSlotError classifies on, so the
	// round trip must hold.
	if !isDuplicateSlotError(errorString(got)) {
		t.Errorf("rendered error %q was not classified as a duplicate slot", got)
	}
}

type errorString string

func (e errorString) Error() string { return string(e) }
