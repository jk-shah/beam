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
	"testing"
	"time"
)

func buildMockRelationPayload(relID uint32, schema, table string, replicaIdent byte, cols []ColumnDef) []byte {
	var buf bytes.Buffer
	buf.WriteByte('R')
	_ = binary.Write(&buf, binary.BigEndian, relID)
	buf.WriteString(schema)
	buf.WriteByte(0)
	buf.WriteString(table)
	buf.WriteByte(0)
	buf.WriteByte(replicaIdent)
	_ = binary.Write(&buf, binary.BigEndian, int16(len(cols)))
	for _, col := range cols {
		buf.WriteByte(col.Flags)
		buf.WriteString(col.Name)
		buf.WriteByte(0)
		_ = binary.Write(&buf, binary.BigEndian, col.TypeOID)
		_ = binary.Write(&buf, binary.BigEndian, col.TypeModifier)
	}
	return buf.Bytes()
}

func buildMockBeginPayload(finalLSN uint64, commitTime time.Time, xid uint32) []byte {
	var buf bytes.Buffer
	buf.WriteByte('B')
	_ = binary.Write(&buf, binary.BigEndian, finalLSN)
	_ = binary.Write(&buf, binary.BigEndian, GoTimeToPg(commitTime))
	_ = binary.Write(&buf, binary.BigEndian, xid)
	return buf.Bytes()
}

func buildMockCommitPayload(commitLSN, endLSN uint64, commitTime time.Time) []byte {
	var buf bytes.Buffer
	buf.WriteByte('C')
	buf.WriteByte(0) // flags
	_ = binary.Write(&buf, binary.BigEndian, commitLSN)
	_ = binary.Write(&buf, binary.BigEndian, endLSN)
	_ = binary.Write(&buf, binary.BigEndian, GoTimeToPg(commitTime))
	return buf.Bytes()
}

func buildMockInsertPayload(relID uint32, textVals []string) []byte {
	var buf bytes.Buffer
	buf.WriteByte('I')
	_ = binary.Write(&buf, binary.BigEndian, relID)
	buf.WriteByte('N')
	_ = binary.Write(&buf, binary.BigEndian, int16(len(textVals)))
	for _, v := range textVals {
		if v == "<null>" {
			buf.WriteByte('n')
		} else if v == "<unchanged>" {
			buf.WriteByte('u')
		} else {
			buf.WriteByte('t')
			_ = binary.Write(&buf, binary.BigEndian, int32(len(v)))
			buf.WriteString(v)
		}
	}
	return buf.Bytes()
}

func buildMockUpdatePayload(relID uint32, oldVals, newVals []string) []byte {
	var buf bytes.Buffer
	buf.WriteByte('U')
	_ = binary.Write(&buf, binary.BigEndian, relID)
	if len(oldVals) > 0 {
		buf.WriteByte('K') // Key tuple
		_ = binary.Write(&buf, binary.BigEndian, int16(len(oldVals)))
		for _, v := range oldVals {
			buf.WriteByte('t')
			_ = binary.Write(&buf, binary.BigEndian, int32(len(v)))
			buf.WriteString(v)
		}
	}
	buf.WriteByte('N') // New tuple
	_ = binary.Write(&buf, binary.BigEndian, int16(len(newVals)))
	for _, v := range newVals {
		if v == "<unchanged>" {
			buf.WriteByte('u')
		} else if v == "<null>" {
			buf.WriteByte('n')
		} else {
			buf.WriteByte('t')
			_ = binary.Write(&buf, binary.BigEndian, int32(len(v)))
			buf.WriteString(v)
		}
	}
	return buf.Bytes()
}

func buildMockDeletePayload(relID uint32, keyVals []string) []byte {
	var buf bytes.Buffer
	buf.WriteByte('D')
	_ = binary.Write(&buf, binary.BigEndian, relID)
	buf.WriteByte('K')
	_ = binary.Write(&buf, binary.BigEndian, int16(len(keyVals)))
	for _, v := range keyVals {
		buf.WriteByte('t')
		_ = binary.Write(&buf, binary.BigEndian, int32(len(v)))
		buf.WriteString(v)
	}
	return buf.Bytes()
}

func TestPgOutputParserLifecycle(t *testing.T) {
	parser := NewPgOutputParser()
	fixedTime := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

	// 1. Begin message
	beginPayload := buildMockBeginPayload(10050, fixedTime, 42)
	event, err := parser.ParseMessage(beginPayload)
	if err != nil {
		t.Fatalf("unexpected error parsing Begin: %v", err)
	}
	if event != nil {
		t.Fatalf("expected nil event for Begin, got %v", event)
	}

	// 2. Relation message
	cols := []ColumnDef{
		{Flags: 1, Name: "id", TypeOID: 20, TypeModifier: -1},
		{Flags: 0, Name: "username", TypeOID: 25, TypeModifier: -1},
		{Flags: 0, Name: "bio", TypeOID: 25, TypeModifier: -1},
	}
	relPayload := buildMockRelationPayload(16384, "public", "users", 'd', cols)
	event, err = parser.ParseMessage(relPayload)
	if err != nil {
		t.Fatalf("unexpected error parsing Relation: %v", err)
	}
	if event != nil {
		t.Fatalf("expected nil event for Relation, got %v", event)
	}

	rel, ok := parser.GetRelation(16384)
	if !ok || rel.RelationName != "users" || len(rel.PrimaryKeys) != 1 || rel.PrimaryKeys[0] != "id" {
		t.Fatalf("relation not cached properly: %+v", rel)
	}

	// 3. Insert message
	insertPayload := buildMockInsertPayload(16384, []string{"101", "alice", "Developer"})
	event, err = parser.ParseMessage(insertPayload)
	if err != nil {
		t.Fatalf("unexpected error parsing Insert: %v", err)
	}
	if event == nil {
		t.Fatalf("expected non-nil event for Insert")
	}
	if event.Operation != OpInsert || event.Table != "users" || event.Schema != "public" {
		t.Errorf("unexpected event metadata: %+v", event)
	}
	if event.After["id"] != int64(101) || event.After["username"] != "alice" || event.After["bio"] != "Developer" {
		t.Errorf("unexpected event payload: %+v", event.After)
	}

	// 4. Update message with unchanged TOAST column ('u')
	updatePayload := buildMockUpdatePayload(16384, []string{"101"}, []string{"101", "alice_new", "<unchanged>"})
	event, err = parser.ParseMessage(updatePayload)
	if err != nil {
		t.Fatalf("unexpected error parsing Update: %v", err)
	}
	if event == nil {
		t.Fatalf("expected non-nil event for Update")
	}
	if event.Operation != OpUpdate {
		t.Errorf("expected OpUpdate, got %s", event.Operation)
	}
	if event.After["username"] != "alice_new" {
		t.Errorf("expected username to be alice_new, got %v", event.After["username"])
	}
	if event.After["bio"] != unchangedToastMarker {
		t.Errorf("expected unchanged toast marker for bio, got %v", event.After["bio"])
	}

	// 5. Delete message
	deletePayload := buildMockDeletePayload(16384, []string{"101"})
	event, err = parser.ParseMessage(deletePayload)
	if err != nil {
		t.Fatalf("unexpected error parsing Delete: %v", err)
	}
	if event == nil || event.Operation != OpDelete {
		t.Fatalf("expected OpDelete event, got %+v", event)
	}
	if event.Before["id"] != int64(101) {
		t.Errorf("expected deleted id 101, got %v", event.Before["id"])
	}

	// 6. Commit message
	commitPayload := buildMockCommitPayload(10050, 10055, fixedTime)
	event, err = parser.ParseMessage(commitPayload)
	if err != nil {
		t.Fatalf("unexpected error parsing Commit: %v", err)
	}
	if event != nil {
		t.Fatalf("expected nil event for Commit")
	}
}

func TestKeepAliveAndStatusUpdateEncoding(t *testing.T) {
	// Test Keepalive parser
	var buf bytes.Buffer
	buf.WriteByte('k')
	_ = binary.Write(&buf, binary.BigEndian, uint64(50000))
	now := time.Now().UTC().Truncate(time.Microsecond)
	_ = binary.Write(&buf, binary.BigEndian, GoTimeToPg(now))
	buf.WriteByte(1) // reply requested

	endWAL, serverTime, replyReq, err := ParseKeepAlive(buf.Bytes())
	if err != nil {
		t.Fatalf("failed to parse Keepalive: %v", err)
	}
	if endWAL != 50000 {
		t.Errorf("expected endWAL 50000, got %d", endWAL)
	}
	if !replyReq {
		t.Errorf("expected replyReq to be true")
	}
	if serverTime.Sub(now).Abs() > time.Millisecond {
		t.Errorf("expected server time %v, got %v", now, serverTime)
	}

	// Test StandbyStatusUpdate encoding
	statusBytes := FormatStandbyStatusUpdate(50000, 48000, 48000, now, false)
	if len(statusBytes) != 34 || statusBytes[0] != 'r' {
		t.Fatalf("invalid StandbyStatusUpdate format: len=%d, byte0=%c", len(statusBytes), statusBytes[0])
	}
	wLSN := binary.BigEndian.Uint64(statusBytes[1:9])
	fLSN := binary.BigEndian.Uint64(statusBytes[9:17])
	aLSN := binary.BigEndian.Uint64(statusBytes[17:25])
	flag := statusBytes[33]

	if wLSN != 50000 || fLSN != 48000 || aLSN != 48000 || flag != 0 {
		t.Errorf("unexpected status fields: write=%d, flush=%d, apply=%d, flag=%d", wLSN, fLSN, aLSN, flag)
	}
}

func TestTimeConversions(t *testing.T) {
	orig := time.Date(2026, 9, 5, 15, 30, 45, 123456000, time.UTC)
	micros := GoTimeToPg(orig)
	reconstructed := PgTimeToGo(micros)
	if !orig.Equal(reconstructed) {
		t.Errorf("time roundtrip mismatch: orig=%v, recon=%v", orig, reconstructed)
	}
}
