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
	"strconv"
	"strings"
	"sync"
	"time"
)

// pgEpoch represents the PostgreSQL microsecond epoch: 2000-01-01 00:00:00 UTC.
var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// Buffer pool for recycling frame byte buffers to prevent GC churn.
var frameBufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, 65536)
		return &b
	},
}

// PgTimeToGo converts PostgreSQL epoch microseconds into time.Time.
func PgTimeToGo(micro int64) time.Time {
	return pgEpoch.Add(time.Duration(micro) * time.Microsecond)
}

// GoTimeToPg converts time.Time into PostgreSQL epoch microseconds.
func GoTimeToPg(t time.Time) int64 {
	return t.Sub(pgEpoch).Microseconds()
}

// RelationDef describes the schema of a table emitted by an 'R' message.
type RelationDef struct {
	RelationID      uint32
	Namespace       string
	RelationName    string
	ReplicaIdentity byte
	Columns         []ColumnDef
	PrimaryKeys     []string
}

// ColumnDef describes a single column within a RelationDef.
type ColumnDef struct {
	Flags        uint8
	Name         string
	TypeOID      uint32
	TypeModifier int32
}

// PgOutputParser parses binary pgoutput protocol messages into ChangeEvent objects.
type PgOutputParser struct {
	relations           map[uint32]*RelationDef
	currentXID          uint32
	currentLSN          uint64
	currentTxTime       time.Time
	currentOrigin       string
	inStream            bool
	spooledTransactions map[uint32][]*ChangeEvent
	mu                  sync.RWMutex
}

// NewPgOutputParser initializes an empty protocol parser.
func NewPgOutputParser() *PgOutputParser {
	return &PgOutputParser{
		relations:           make(map[uint32]*RelationDef),
		spooledTransactions: make(map[uint32][]*ChangeEvent),
	}
}

// RegisterRelation explicitly stores a relation definition in the schema cache.
func (p *PgOutputParser) RegisterRelation(rel *RelationDef) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.relations[rel.RelationID] = rel
}

// GetRelation retrieves a cached relation definition.
func (p *PgOutputParser) GetRelation(relID uint32) (*RelationDef, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	rel, ok := p.relations[relID]
	return rel, ok
}

// ParseMessage decodes a single pgoutput frame payload.
// If the message is part of a streamed transaction, mutations are buffered until
// Stream Commit ('c'). Use ParseMessages for full multi-event streaming support.
func (p *PgOutputParser) ParseMessage(data []byte) (*ChangeEvent, error) {
	events, err := p.ParseMessages(data)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, nil
	}
	return events[0], nil
}

// ParseMessages decodes a single pgoutput frame payload and returns zero or more ChangeEvents.
// Returns buffered events upon Stream Commit ('c') or a single event for standard DML ('I', 'U', 'D', 'T').
func (p *PgOutputParser) ParseMessages(data []byte) ([]*ChangeEvent, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty pgoutput payload")
	}

	msgType := data[0]
	r := bytes.NewReader(data[1:])

	switch msgType {
	case 'B': // Begin
		var finalLSN uint64
		var commitTimeMicros int64
		var xid uint32
		if err := binary.Read(r, binary.BigEndian, &finalLSN); err != nil {
			return nil, fmt.Errorf("failed to parse Begin finalLSN: %w", err)
		}
		if err := binary.Read(r, binary.BigEndian, &commitTimeMicros); err != nil {
			return nil, fmt.Errorf("failed to parse Begin commitTime: %w", err)
		}
		if err := binary.Read(r, binary.BigEndian, &xid); err != nil {
			return nil, fmt.Errorf("failed to parse Begin xid: %w", err)
		}
		p.currentXID = xid
		p.currentLSN = finalLSN
		p.currentTxTime = PgTimeToGo(commitTimeMicros)
		return nil, nil

	case 'C': // Commit
		var flags uint8
		var commitLSN, endLSN uint64
		var commitTimeMicros int64
		if err := binary.Read(r, binary.BigEndian, &flags); err != nil {
			return nil, fmt.Errorf("failed to parse Commit flags: %w", err)
		}
		if err := binary.Read(r, binary.BigEndian, &commitLSN); err != nil {
			return nil, fmt.Errorf("failed to parse Commit commitLSN: %w", err)
		}
		if err := binary.Read(r, binary.BigEndian, &endLSN); err != nil {
			return nil, fmt.Errorf("failed to parse Commit endLSN: %w", err)
		}
		if err := binary.Read(r, binary.BigEndian, &commitTimeMicros); err != nil {
			return nil, fmt.Errorf("failed to parse Commit commitTime: %w", err)
		}
		p.currentLSN = endLSN
		return nil, nil

	case 'R': // Relation
		rel, err := parseRelation(r)
		if err != nil {
			return nil, fmt.Errorf("failed to parse Relation message: %w", err)
		}
		p.RegisterRelation(rel)
		return nil, nil

	case 'I': // Insert
		var relID uint32
		if err := binary.Read(r, binary.BigEndian, &relID); err != nil {
			return nil, fmt.Errorf("failed to parse Insert relID: %w", err)
		}
		rel, ok := p.GetRelation(relID)
		if !ok {
			return nil, fmt.Errorf("unknown relation ID %d for Insert", relID)
		}
		tupleType, err := r.ReadByte()
		if err != nil || tupleType != 'N' {
			return nil, fmt.Errorf("expected 'N' tuple type for Insert, got %c: %w", tupleType, err)
		}
		values, err := parseTupleData(r, rel.Columns)
		if err != nil {
			return nil, fmt.Errorf("failed to parse Insert tuple: %w", err)
		}
		ev := &ChangeEvent{
			Operation:     OpInsert,
			Schema:        rel.Namespace,
			Table:         rel.RelationName,
			CommitTime:    p.currentTxTime,
			LSN:           p.currentLSN,
			TransactionID: p.currentXID,
			PrimaryKeys:   rel.PrimaryKeys,
			Origin:        p.currentOrigin,
			After:         valuesToMap(values),
			ColumnTypes:   valuesToColumnTypes(values),
		}
		return p.emitOrSpool(ev), nil

	case 'U': // Update
		var relID uint32
		if err := binary.Read(r, binary.BigEndian, &relID); err != nil {
			return nil, fmt.Errorf("failed to parse Update relID: %w", err)
		}
		rel, ok := p.GetRelation(relID)
		if !ok {
			return nil, fmt.Errorf("unknown relation ID %d for Update", relID)
		}

		subType, err := r.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("failed to read Update sub-type: %w", err)
		}

		var beforeVals []ColumnValue
		if subType == 'K' || subType == 'O' {
			beforeVals, err = parseTupleData(r, rel.Columns)
			if err != nil {
				return nil, fmt.Errorf("failed to parse Update before tuple: %w", err)
			}
			subType, err = r.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("failed to read next tuple indicator: %w", err)
			}
		}

		if subType != 'N' {
			return nil, fmt.Errorf("expected 'N' tuple type in Update, got %c", subType)
		}
		afterVals, err := parseTupleData(r, rel.Columns)
		if err != nil {
			return nil, fmt.Errorf("failed to parse Update after tuple: %w", err)
		}

		var beforeMap map[string]any
		if len(beforeVals) > 0 {
			beforeMap = valuesToMap(beforeVals)
		}

		ev := &ChangeEvent{
			Operation:     OpUpdate,
			Schema:        rel.Namespace,
			Table:         rel.RelationName,
			CommitTime:    p.currentTxTime,
			LSN:           p.currentLSN,
			TransactionID: p.currentXID,
			PrimaryKeys:   rel.PrimaryKeys,
			Origin:        p.currentOrigin,
			Before:        beforeMap,
			After:         valuesToMap(afterVals),
			ColumnTypes:   valuesToColumnTypes(afterVals),
		}
		return p.emitOrSpool(ev), nil

	case 'D': // Delete
		var relID uint32
		if err := binary.Read(r, binary.BigEndian, &relID); err != nil {
			return nil, fmt.Errorf("failed to parse Delete relID: %w", err)
		}
		rel, ok := p.GetRelation(relID)
		if !ok {
			return nil, fmt.Errorf("unknown relation ID %d for Delete", relID)
		}
		subType, err := r.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("failed to read Delete sub-type: %w", err)
		}
		if subType != 'K' && subType != 'O' {
			return nil, fmt.Errorf("expected 'K' or 'O' tuple type for Delete, got %c", subType)
		}
		values, err := parseTupleData(r, rel.Columns)
		if err != nil {
			return nil, fmt.Errorf("failed to parse Delete tuple: %w", err)
		}
		ev := &ChangeEvent{
			Operation:     OpDelete,
			Schema:        rel.Namespace,
			Table:         rel.RelationName,
			CommitTime:    p.currentTxTime,
			LSN:           p.currentLSN,
			TransactionID: p.currentXID,
			PrimaryKeys:   rel.PrimaryKeys,
			Origin:        p.currentOrigin,
			Before:        valuesToMap(values),
			ColumnTypes:   valuesToColumnTypes(values),
		}
		return p.emitOrSpool(ev), nil

	case 'T': // Truncate
		var numRelations uint32
		var options uint8
		if err := binary.Read(r, binary.BigEndian, &numRelations); err != nil {
			return nil, fmt.Errorf("failed to parse Truncate numRelations: %w", err)
		}
		if err := binary.Read(r, binary.BigEndian, &options); err != nil {
			return nil, fmt.Errorf("failed to parse Truncate options: %w", err)
		}
		relIDs := make([]uint32, numRelations)
		for i := 0; i < int(numRelations); i++ {
			if err := binary.Read(r, binary.BigEndian, &relIDs[i]); err != nil {
				return nil, fmt.Errorf("failed to parse Truncate relation ID %d: %w", i, err)
			}
		}
		var tableName, schemaName string
		if len(relIDs) > 0 {
			if rel, ok := p.GetRelation(relIDs[0]); ok {
				tableName = rel.RelationName
				schemaName = rel.Namespace
			}
		}
		ev := &ChangeEvent{
			Operation:     OpTruncate,
			Schema:        schemaName,
			Table:         tableName,
			CommitTime:    p.currentTxTime,
			LSN:           p.currentLSN,
			TransactionID: p.currentXID,
			Origin:        p.currentOrigin,
		}
		return p.emitOrSpool(ev), nil

	case 'O': // Origin (Replication loop prevention)
		var lsn uint64
		_ = binary.Read(r, binary.BigEndian, &lsn)
		origin, _ := readNullTerminatedString(r)
		p.currentOrigin = origin
		return nil, nil

	case 'M': // Generic logical message
		return nil, nil

	case 'S': // Stream Start
		var xid uint32
		var firstSegment uint8
		_ = binary.Read(r, binary.BigEndian, &xid)
		_ = binary.Read(r, binary.BigEndian, &firstSegment)
		p.inStream = true
		p.currentXID = xid
		return nil, nil

	case 'E': // Stream Stop
		p.inStream = false
		return nil, nil

	case 'c': // Stream Commit
		var xid uint32
		var flags uint8
		var commitLSN, endLSN uint64
		_ = binary.Read(r, binary.BigEndian, &xid)
		_ = binary.Read(r, binary.BigEndian, &flags)
		_ = binary.Read(r, binary.BigEndian, &commitLSN)
		_ = binary.Read(r, binary.BigEndian, &endLSN)
		p.currentLSN = endLSN
		spooled := p.spooledTransactions[xid]
		delete(p.spooledTransactions, xid)
		for _, ev := range spooled {
			if ev.LSN == 0 {
				ev.LSN = commitLSN
			}
		}
		return spooled, nil

	case 'A': // Stream Abort
		var xid, subxid uint32
		_ = binary.Read(r, binary.BigEndian, &xid)
		_ = binary.Read(r, binary.BigEndian, &subxid)
		delete(p.spooledTransactions, xid)
		return nil, nil

	case 'P', 'K': // 2PC Prepare / Commit Prepared
		return nil, nil

	default:
		// Unsupported or extension message, skip gracefully
		return nil, nil
	}
}

func (p *PgOutputParser) emitOrSpool(ev *ChangeEvent) []*ChangeEvent {
	if p.inStream {
		p.spooledTransactions[p.currentXID] = append(p.spooledTransactions[p.currentXID], ev)
		return nil
	}
	return []*ChangeEvent{ev}
}

// ParseKeepAlive decodes a Primary Keepalive ('k') message.
func ParseKeepAlive(data []byte) (endWAL uint64, serverTime time.Time, replyRequested bool, err error) {
	if len(data) < 18 || data[0] != 'k' {
		return 0, time.Time{}, false, fmt.Errorf("invalid Keepalive message")
	}
	r := bytes.NewReader(data[1:])
	var serverMicros int64
	var replyFlag uint8
	if err := binary.Read(r, binary.BigEndian, &endWAL); err != nil {
		return 0, time.Time{}, false, err
	}
	if err := binary.Read(r, binary.BigEndian, &serverMicros); err != nil {
		return 0, time.Time{}, false, err
	}
	if err := binary.Read(r, binary.BigEndian, &replyFlag); err != nil {
		return 0, time.Time{}, false, err
	}
	return endWAL, PgTimeToGo(serverMicros), replyFlag == 1, nil
}

// FormatStandbyStatusUpdate encodes a client status update ('r') message.
func FormatStandbyStatusUpdate(writeLSN, flushLSN, applyLSN uint64, clientTime time.Time, replyRequested bool) []byte {
	buf := make([]byte, 34)
	buf[0] = 'r'
	binary.BigEndian.PutUint64(buf[1:9], writeLSN)
	binary.BigEndian.PutUint64(buf[9:17], flushLSN)
	binary.BigEndian.PutUint64(buf[17:25], applyLSN)
	binary.BigEndian.PutUint64(buf[25:33], uint64(GoTimeToPg(clientTime)))
	if replyRequested {
		buf[33] = 1
	} else {
		buf[33] = 0
	}
	return buf
}

func parseRelation(r *bytes.Reader) (*RelationDef, error) {
	var relID uint32
	if err := binary.Read(r, binary.BigEndian, &relID); err != nil {
		return nil, err
	}
	ns, err := readNullTerminatedString(r)
	if err != nil {
		return nil, err
	}
	relName, err := readNullTerminatedString(r)
	if err != nil {
		return nil, err
	}
	repIdent, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	var numCols int16
	if err := binary.Read(r, binary.BigEndian, &numCols); err != nil {
		return nil, err
	}

	cols := make([]ColumnDef, numCols)
	var pks []string
	for i := 0; i < int(numCols); i++ {
		var flags uint8
		if err := binary.Read(r, binary.BigEndian, &flags); err != nil {
			return nil, err
		}
		cName, err := readNullTerminatedString(r)
		if err != nil {
			return nil, err
		}
		var typeOID uint32
		var typeMod int32
		if err := binary.Read(r, binary.BigEndian, &typeOID); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.BigEndian, &typeMod); err != nil {
			return nil, err
		}
		cols[i] = ColumnDef{
			Flags:        flags,
			Name:         cName,
			TypeOID:      typeOID,
			TypeModifier: typeMod,
		}
		if flags&1 != 0 {
			pks = append(pks, cName)
		}
	}

	return &RelationDef{
		RelationID:      relID,
		Namespace:       ns,
		RelationName:    relName,
		ReplicaIdentity: repIdent,
		Columns:         cols,
		PrimaryKeys:     pks,
	}, nil
}

func parseTupleData(r *bytes.Reader, cols []ColumnDef) ([]ColumnValue, error) {
	var numCols int16
	if err := binary.Read(r, binary.BigEndian, &numCols); err != nil {
		return nil, err
	}

	values := make([]ColumnValue, numCols)
	for i := 0; i < int(numCols); i++ {
		var colName string
		var colType uint32
		if i < len(cols) {
			colName = cols[i].Name
			colType = cols[i].TypeOID
		} else {
			colName = fmt.Sprintf("col_%d", i)
		}

		kind, err := r.ReadByte()
		if err != nil {
			return nil, err
		}

		cv := ColumnValue{
			Name:    colName,
			TypeOID: colType,
		}

		switch kind {
		case 'n': // NULL
			cv.IsNull = true
			cv.Value = nil
		case 'u': // Unchanged TOAST datum
			cv.IsToastUnchanged = true
			cv.Value = nil
		case 't': // Text formatted value
			var length int32
			if err := binary.Read(r, binary.BigEndian, &length); err != nil {
				return nil, err
			}
			valBytes := make([]byte, length)
			if _, err := r.Read(valBytes); err != nil {
				return nil, err
			}
			cv.Value = parseTextValue(colType, string(valBytes))
		case 'b': // Binary formatted value
			var length int32
			if err := binary.Read(r, binary.BigEndian, &length); err != nil {
				return nil, err
			}
			valBytes := make([]byte, length)
			if _, err := r.Read(valBytes); err != nil {
				return nil, err
			}
			if colType == 3802 { // JSONB
				if str, err := DecodeBinaryJSONB(valBytes); err == nil {
					cv.Value = str
				} else {
					cv.Value = valBytes
				}
			} else if isArrayOID(colType) {
				if arr, err := DecodeBinaryArray(valBytes); err == nil {
					cv.Value = arr
				} else {
					cv.Value = valBytes
				}
			} else {
				cv.Value = valBytes
			}
		default:
			return nil, fmt.Errorf("unknown column datum kind %c at index %d", kind, i)
		}
		values[i] = cv
	}
	return values, nil
}

func readNullTerminatedString(r *bytes.Reader) (string, error) {
	var b []byte
	for {
		c, err := r.ReadByte()
		if err != nil {
			return "", err
		}
		if c == 0 {
			break
		}
		b = append(b, c)
	}
	return string(b), nil
}

func valuesToMap(values []ColumnValue) map[string]any {
	m := make(map[string]any, len(values))
	for _, v := range values {
		if v.IsNull {
			m[v.Name] = nil
		} else if v.IsToastUnchanged {
			m[v.Name] = "<unchanged_toast>"
		} else {
			m[v.Name] = v.Value
		}
	}
	return m
}

func valuesToColumnTypes(values []ColumnValue) map[string]uint32 {
	m := make(map[string]uint32, len(values))
	for _, v := range values {
		m[v.Name] = v.TypeOID
	}
	return m
}

func isArrayOID(typeOID uint32) bool {
	switch typeOID {
	case 1000, 1005, 1007, 1016, 1021, 1022, 1009, 1015, 199, 3807:
		return true
	}
	return false
}

func isRangeOID(typeOID uint32) bool {
	switch typeOID {
	case 3904, 3906, 3908, 3910, 3912, 3926:
		return true
	}
	return false
}

func parseTextValue(typeOID uint32, s string) any {
	if typeOID == 114 || typeOID == 3802 { // JSON and JSONB
		return s
	}
	if isArrayOID(typeOID) || (strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}") && !strings.Contains(s, ":")) {
		if arr, err := DecodeTextArray(s); err == nil {
			return arr
		}
	}
	if typeOID == 600 || (strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") && strings.Contains(s, ",") && !isRangeOID(typeOID)) {
		if pt, err := DecodePoint(s); err == nil {
			return pt
		}
	}
	if isRangeOID(typeOID) || strings.HasPrefix(s, "[") || strings.HasPrefix(s, "(") {
		if rng, err := DecodeRange(s); err == nil {
			return rng
		}
	}

	switch typeOID {
	case 16: // bool
		return s == "t" || s == "true" || s == "1"
	case 20: // int8 (bigint)
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			return v
		}
	case 21: // int2 (smallint)
		if v, err := strconv.ParseInt(s, 10, 16); err == nil {
			return int16(v)
		}
	case 23: // int4 (integer)
		if v, err := strconv.ParseInt(s, 10, 32); err == nil {
			return int32(v)
		}
	case 700: // float4
		if v, err := strconv.ParseFloat(s, 32); err == nil {
			return float32(v)
		}
	case 701: // float8
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			return v
		}
	case 1114, 1184: // timestamp, timestamptz
		layouts := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05.999999999-07",
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05",
		}
		for _, layout := range layouts {
			if t, err := time.Parse(layout, s); err == nil {
				return t
			}
		}
	}
	return s
}
