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
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
)

// Replicates upstream PostgreSQL toast.sql semantics (unchanged & modified).
func TestUpstreamToastSemantics_UnchangedAndModified(t *testing.T) {
	parser := NewPgOutputParser()
	cols := []ColumnDef{
		{Flags: 1, Name: "id", TypeOID: 23, TypeModifier: -1},
		{Flags: 0, Name: "name", TypeOID: 25, TypeModifier: -1},
		{Flags: 0, Name: "toast_payload", TypeOID: 25, TypeModifier: -1},
	}
	relPayload := buildMockRelationPayload(16400, "public", "upstream_items", 'd', cols)
	_, _ = parser.ParseMessage(relPayload)

	largePayload := strings.Repeat("TOAST_DATA_CHUNK_", 200)

	// 1. INSERT with full large TOAST payload
	insertPayload := buildMockInsertPayload(16400, []string{"1", "Alpha", largePayload})
	event, err := parser.ParseMessage(insertPayload)
	if err != nil {
		t.Fatalf("unexpected error parsing Insert: %v", err)
	}
	if event == nil || event.Operation != OpInsert {
		t.Fatalf("expected OpInsert event, got %+v", event)
	}
	if event.After["id"] != int32(1) || event.After["toast_payload"] != largePayload {
		t.Errorf("unexpected Insert after map: %+v", event.After)
	}

	// 2. UPDATE modifying only name, with unchanged TOAST column ('u')
	updatePayload := buildMockUpdatePayload(16400, []string{"1"}, []string{"1", "Alpha_Updated", "<unchanged>"})
	event, err = parser.ParseMessage(updatePayload)
	if err != nil {
		t.Fatalf("unexpected error parsing Update: %v", err)
	}
	if event == nil || event.Operation != OpUpdate {
		t.Fatalf("expected OpUpdate event, got %+v", event)
	}
	if event.After["name"] != "Alpha_Updated" {
		t.Errorf("expected updated name Alpha_Updated, got %v", event.After["name"])
	}
	// An unchanged TOASTed column is not transmitted by the server, so it must
	// be absent from After and reported in UnchangedColumns rather than
	// carrying a placeholder value that would violate the column's type.
	if _, present := event.After["toast_payload"]; present {
		t.Errorf("unchanged TOAST column must be omitted from After, got %v", event.After["toast_payload"])
	}
	if !containsString(event.UnchangedColumns, "toast_payload") {
		t.Errorf("expected toast_payload in UnchangedColumns, got %v", event.UnchangedColumns)
	}

}

// Replicates upstream PostgreSQL truncate.sql multi-table and cascade semantics.
func TestUpstreamTruncateSemantics_MultiTableAndCascade(t *testing.T) {
	parser := NewPgOutputParser()
	cols := []ColumnDef{{Flags: 1, Name: "id", TypeOID: 23, TypeModifier: -1}}
	rel1 := buildMockRelationPayload(16400, "public", "orders", 'd', cols)
	rel2 := buildMockRelationPayload(16401, "public", "order_items", 'd', cols)
	_, _ = parser.ParseMessage(rel1)
	_, _ = parser.ParseMessage(rel2)

	var truncateBuf bytes.Buffer
	truncateBuf.WriteByte('T')
	_ = binary.Write(&truncateBuf, binary.BigEndian, uint32(2)) // 2 relations
	truncateBuf.WriteByte(0x03)                                 // CASCADE | RESTART IDENTITY
	_ = binary.Write(&truncateBuf, binary.BigEndian, uint32(16400))
	_ = binary.Write(&truncateBuf, binary.BigEndian, uint32(16401))

	event, err := parser.ParseMessage(truncateBuf.Bytes())
	if err != nil {
		t.Fatalf("unexpected error parsing Truncate: %v", err)
	}
	if event == nil || event.Operation != OpTruncate {
		t.Fatalf("expected OpTruncate event, got %+v", event)
	}
	if event.Table != "orders" {
		t.Errorf("expected orders table, got %s", event.Table)
	}
}

// Replicates upstream PostgreSQL stream.sql and spill.sql streaming transactions.
func TestUpstreamInFlightStreamingAndSpill_CommitAndAbort(t *testing.T) {
	parser := NewPgOutputParser()

	// 1. STREAM START (xid = 5001)
	var streamStart bytes.Buffer
	streamStart.WriteByte('S')
	_ = binary.Write(&streamStart, binary.BigEndian, uint32(5001))
	streamStart.WriteByte(1) // first segment
	event, err := parser.ParseMessage(streamStart.Bytes())
	if err != nil || event != nil {
		t.Errorf("expected nil event for Stream Start, got: %v, err: %v", event, err)
	}

	// 2. STREAM STOP ('E')
	event, err = parser.ParseMessage([]byte{'E'})
	if err != nil || event != nil {
		t.Errorf("expected nil event for Stream Stop, got: %v, err: %v", event, err)
	}

	// 3. STREAM COMMIT ('c')
	var streamCommit bytes.Buffer
	streamCommit.WriteByte('c')
	_ = binary.Write(&streamCommit, binary.BigEndian, uint32(5001))
	streamCommit.WriteByte(0) // flags
	_ = binary.Write(&streamCommit, binary.BigEndian, uint64(3020))
	_ = binary.Write(&streamCommit, binary.BigEndian, uint64(3025))
	event, err = parser.ParseMessage(streamCommit.Bytes())
	if err != nil || event != nil {
		t.Errorf("expected nil event for Stream Commit, got: %v, err: %v", event, err)
	}

	// 4. STREAM ABORT ('A')
	var streamAbort bytes.Buffer
	streamAbort.WriteByte('A')
	_ = binary.Write(&streamAbort, binary.BigEndian, uint32(5002))
	_ = binary.Write(&streamAbort, binary.BigEndian, uint32(0))
	event, err = parser.ParseMessage(streamAbort.Bytes())
	if err != nil || event != nil {
		t.Errorf("expected nil event for Stream Abort, got: %v, err: %v", event, err)
	}
}

// Replicates upstream PostgreSQL prepared.sql two-phase prepared transactions.
//
// The parser does not implement two-phase commit decoding. Begin Prepare ('b')
// and Rollback Prepared ('r') carry the transaction identity for the frames
// that follow them; without handling those frames, subsequent changes would
// inherit the XID, LSN and commit timestamp of the previous transaction, and
// changes belonging to a rolled-back prepared transaction would be emitted with
// no corresponding retraction. An earlier revision of this test asserted that
// 'P' and 'K' were silently dropped, which locked in that data-correctness
// defect. The parser now fails closed on every two-phase frame, and
// CDCOptions.Validate rejects TwoPhaseCommit so the frames cannot be requested
// in the first place.
func TestUpstreamTwoPhaseCommitFramesAreRejected(t *testing.T) {
	// Begin Prepare ('b')
	var beginPrepBuf bytes.Buffer
	beginPrepBuf.WriteByte('b')
	_ = binary.Write(&beginPrepBuf, binary.BigEndian, uint64(3990))
	_ = binary.Write(&beginPrepBuf, binary.BigEndian, uint64(4000))
	_ = binary.Write(&beginPrepBuf, binary.BigEndian, int64(700000000))
	_ = binary.Write(&beginPrepBuf, binary.BigEndian, uint32(6001))
	beginPrepBuf.WriteString("beam_tx_gid_1\000")

	// Prepare ('P')
	var prepBuf bytes.Buffer
	prepBuf.WriteByte('P')
	_ = binary.Write(&prepBuf, binary.BigEndian, uint64(4000))
	_ = binary.Write(&prepBuf, binary.BigEndian, uint64(4010))
	_ = binary.Write(&prepBuf, binary.BigEndian, int64(700000000))
	_ = binary.Write(&prepBuf, binary.BigEndian, uint32(6001))
	prepBuf.WriteString("beam_tx_gid_1\000")

	// Commit Prepared ('K')
	var commitPrepBuf bytes.Buffer
	commitPrepBuf.WriteByte('K')
	commitPrepBuf.WriteByte(0)
	_ = binary.Write(&commitPrepBuf, binary.BigEndian, uint64(4020))
	_ = binary.Write(&commitPrepBuf, binary.BigEndian, uint64(4025))
	_ = binary.Write(&commitPrepBuf, binary.BigEndian, int64(700000000))
	_ = binary.Write(&commitPrepBuf, binary.BigEndian, uint32(6001))
	commitPrepBuf.WriteString("beam_tx_gid_1\000")

	// Rollback Prepared ('r')
	var rollbackPrepBuf bytes.Buffer
	rollbackPrepBuf.WriteByte('r')
	rollbackPrepBuf.WriteByte(0)
	_ = binary.Write(&rollbackPrepBuf, binary.BigEndian, uint64(4030))
	_ = binary.Write(&rollbackPrepBuf, binary.BigEndian, uint64(4035))
	_ = binary.Write(&rollbackPrepBuf, binary.BigEndian, int64(700000000))
	_ = binary.Write(&rollbackPrepBuf, binary.BigEndian, int64(700000001))
	_ = binary.Write(&rollbackPrepBuf, binary.BigEndian, uint32(6001))
	rollbackPrepBuf.WriteString("beam_tx_gid_1\000")

	// Stream Prepare ('p')
	var streamPrepBuf bytes.Buffer
	streamPrepBuf.WriteByte('p')
	_ = binary.Write(&streamPrepBuf, binary.BigEndian, uint64(4040))
	_ = binary.Write(&streamPrepBuf, binary.BigEndian, uint64(4045))
	_ = binary.Write(&streamPrepBuf, binary.BigEndian, int64(700000000))
	_ = binary.Write(&streamPrepBuf, binary.BigEndian, uint32(6001))
	streamPrepBuf.WriteString("beam_tx_gid_1\000")

	tests := []struct {
		name  string
		frame []byte
	}{
		{"BeginPrepare", beginPrepBuf.Bytes()},
		{"Prepare", prepBuf.Bytes()},
		{"CommitPrepared", commitPrepBuf.Bytes()},
		{"RollbackPrepared", rollbackPrepBuf.Bytes()},
		{"StreamPrepare", streamPrepBuf.Bytes()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewPgOutputParser()
			event, err := parser.ParseMessage(tt.frame)
			if err == nil {
				t.Fatalf("expected an error for two-phase frame %q, got event: %v", tt.name, event)
			}
			if event != nil {
				t.Errorf("expected no event alongside the error, got: %v", event)
			}
			if !strings.Contains(err.Error(), "two-phase") {
				t.Errorf("error should identify two-phase commit as the cause, got: %v", err)
			}
		})
	}
}

// CDCOptions must reject two-phase commit before a slot is ever created with
// two_phase enabled, so operators get a configuration-time failure rather than
// a stream that silently mis-attributes rows.
func TestTwoPhaseCommitRejectedByValidate(t *testing.T) {
	opts := NewCDCOptions(
		WithCDCSlotName("beam_slot"),
		WithCDCPublication("beam_pub"),
	)
	opts.TwoPhaseCommit = true
	err := opts.Validate()
	if err == nil {
		t.Fatal("expected Validate to reject TwoPhaseCommit")
	}
	if !strings.Contains(err.Error(), "two-phase") && !strings.Contains(err.Error(), "TwoPhaseCommit") {
		t.Errorf("error should identify two-phase commit as the cause, got: %v", err)
	}
}

// Replicates upstream PostgreSQL messages.sql generic logical messages.
func TestUpstreamLogicalDecodingMessages(t *testing.T) {
	parser := NewPgOutputParser()
	var msgBuf bytes.Buffer
	msgBuf.WriteByte('M')
	_ = binary.Write(&msgBuf, binary.BigEndian, uint32(0)) // xid
	msgBuf.WriteByte(0)                                    // non-transactional
	_ = binary.Write(&msgBuf, binary.BigEndian, uint64(5000))
	msgBuf.WriteString("beam_heartbeat\000")
	payload := []byte("2026-09-05T18:00:00Z")
	_ = binary.Write(&msgBuf, binary.BigEndian, uint32(len(payload)))
	msgBuf.Write(payload)

	event, err := parser.ParseMessage(msgBuf.Bytes())
	if err != nil || event != nil {
		t.Errorf("expected nil event for Logical Message, got: %v, err: %v", event, err)
	}
}

// Replicates origin message decoding for bidirectional replication loop prevention.
func TestUpstreamOriginMessage(t *testing.T) {
	parser := NewPgOutputParser()
	cols := []ColumnDef{{Flags: 1, Name: "id", TypeOID: 23, TypeModifier: -1}}
	relPayload := buildMockRelationPayload(16500, "public", "node_sync", 'd', cols)
	_, _ = parser.ParseMessage(relPayload)

	// Origin message setting origin to "remote_node_east"
	var origBuf bytes.Buffer
	origBuf.WriteByte('O')
	_ = binary.Write(&origBuf, binary.BigEndian, uint64(6000))
	origBuf.WriteString("remote_node_east\000")
	_, err := parser.ParseMessage(origBuf.Bytes())
	if err != nil {
		t.Fatalf("unexpected error parsing Origin: %v", err)
	}

	// Next insert should inherit the origin
	insertPayload := buildMockInsertPayload(16500, []string{"99"})
	event, err := parser.ParseMessage(insertPayload)
	if err != nil {
		t.Fatalf("unexpected error parsing Insert: %v", err)
	}
	if event == nil || event.Origin != "remote_node_east" {
		t.Errorf("expected event Origin 'remote_node_east', got %q", event.Origin)
	}
}

// Replicates streaming in-flight transactions with uncommitted mutations discarded on Stream Abort ('A').
func TestUpstreamInFlightStreamingSpoolAndAbort(t *testing.T) {
	parser := NewPgOutputParser()
	cols := []ColumnDef{{Flags: 1, Name: "id", TypeOID: 23, TypeModifier: -1}}
	relPayload := buildMockRelationPayload(16600, "public", "streamed_orders", 'd', cols)
	_, _ = parser.ParseMessage(relPayload)

	// Stream Start (xid = 6001)
	var streamStart bytes.Buffer
	streamStart.WriteByte('S')
	_ = binary.Write(&streamStart, binary.BigEndian, uint32(6001))
	streamStart.WriteByte(1)
	_, _ = parser.ParseMessage(streamStart.Bytes())

	// Insert while in stream - should be buffered (returns nil)
	insertPayload := buildMockInsertPayload(16600, []string{"101"})
	ev, err := parser.ParseMessage(insertPayload)
	if err != nil || ev != nil {
		t.Fatalf("expected nil event for in-stream insert, got ev=%v, err=%v", ev, err)
	}

	// Stream Stop
	_, _ = parser.ParseMessage([]byte{'E'})

	// Stream Abort ('A')
	var streamAbort bytes.Buffer
	streamAbort.WriteByte('A')
	_ = binary.Write(&streamAbort, binary.BigEndian, uint32(6001))
	_ = binary.Write(&streamAbort, binary.BigEndian, uint32(0))
	events, err := parser.ParseMessages(streamAbort.Bytes())
	if err != nil {
		t.Fatalf("unexpected error parsing Stream Abort: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("expected 0 events on abort, got %d", len(events))
	}
}

// Replicates streaming in-flight transactions with mutations emitted on Stream Commit ('c').
func TestUpstreamInFlightStreamingSpoolAndCommit(t *testing.T) {
	parser := NewPgOutputParser()
	cols := []ColumnDef{{Flags: 1, Name: "id", TypeOID: 23, TypeModifier: -1}}
	relPayload := buildMockRelationPayload(16700, "public", "streamed_orders", 'd', cols)
	_, _ = parser.ParseMessage(relPayload)

	// Stream Start (xid = 7001)
	var streamStart bytes.Buffer
	streamStart.WriteByte('S')
	_ = binary.Write(&streamStart, binary.BigEndian, uint32(7001))
	streamStart.WriteByte(1)
	_, _ = parser.ParseMessage(streamStart.Bytes())

	// Insert 1
	insert1 := buildMockInsertPayload(16700, []string{"201"})
	ev1, err := parser.ParseMessage(insert1)
	if err != nil || ev1 != nil {
		t.Fatalf("expected nil event for in-stream insert1, got ev=%v, err=%v", ev1, err)
	}

	// Insert 2
	insert2 := buildMockInsertPayload(16700, []string{"202"})
	ev2, err := parser.ParseMessage(insert2)
	if err != nil || ev2 != nil {
		t.Fatalf("expected nil event for in-stream insert2, got ev=%v, err=%v", ev2, err)
	}

	// Stream Stop
	_, _ = parser.ParseMessage([]byte{'E'})

	// Stream Commit ('c')
	var streamCommit bytes.Buffer
	streamCommit.WriteByte('c')
	_ = binary.Write(&streamCommit, binary.BigEndian, uint32(7001))
	streamCommit.WriteByte(0)
	_ = binary.Write(&streamCommit, binary.BigEndian, uint64(5000))
	_ = binary.Write(&streamCommit, binary.BigEndian, uint64(5050))
	events, err := parser.ParseMessages(streamCommit.Bytes())
	if err != nil {
		t.Fatalf("unexpected error parsing Stream Commit: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 committed events, got %d", len(events))
	}
	if fmt.Sprint(events[0].After["id"]) != "201" || fmt.Sprint(events[1].After["id"]) != "202" {
		t.Errorf("unexpected event payloads: %+v, %+v", events[0], events[1])
	}
	if events[0].LSN != 5000 || events[1].LSN != 5000 {
		t.Errorf("expected commit LSN 5000, got %d, %d", events[0].LSN, events[1].LSN)
	}
}
