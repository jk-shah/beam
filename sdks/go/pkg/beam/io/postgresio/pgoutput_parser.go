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
	"io"
	"math"
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

// RelationMessage is an alias for RelationDef representing a relation schema message.
type RelationMessage = RelationDef

// ColumnDef describes a single column within a RelationDef.
type ColumnDef struct {
	Flags        uint8
	Name         string
	TypeOID      uint32
	TypeModifier int32
}

// PgOutputParser parses binary pgoutput protocol messages into ChangeEvent objects.
type PgOutputParser struct {
	relations     map[uint32]*RelationDef
	currentXID    uint32
	currentLSN    uint64
	currentTxTime time.Time
	currentOrigin string
	// inTransaction is true between a Begin ('B') and its matching Commit
	// ('C') for an ordinary, non-streamed transaction.
	//
	// This is distinct from inStream, which covers streamed (in-progress)
	// transactions only. Both conditions mean the parser has seen part of a
	// transaction and not all of it, which is what determines whether the
	// stream position is safe to acknowledge to the server.
	inTransaction       bool
	inStream            bool
	spooledTransactions map[uint32][]*ChangeEvent
	mu                  sync.RWMutex

	// txSeqByXID counts row changes within each in-progress transaction. It is
	// keyed by XID rather than being a single counter because streamed
	// transactions interleave on the wire: a transaction's changes arrive in
	// several StreamStart/StreamStop segments with other transactions'
	// segments in between. A single counter would restart numbering on each
	// resumed segment and assign the same sequence twice within a transaction.
	//
	// The counter supplies the intra-transaction ordering that LSN cannot,
	// because pgoutput reuses the BEGIN record's LSN for every change in the
	// transaction.
	txSeqByXID map[uint32]uint32

	// spooledBytes and spooledBytesByXID bound how much a streamed transaction
	// may buffer before decoding fails.
	//
	// PostgreSQL streams an in-progress transaction precisely so that neither
	// side has to hold all of it at once. Re-accumulating it here to emit
	// atomically at Stream Commit gives back that property, so without a limit
	// one large transaction sizes worker memory and a 50M-row update is an OOM
	// rather than an error. Failing the bundle is recoverable; the worker being
	// killed mid-bundle is less so.
	//
	// The per-XID total is tracked separately because streamed transactions
	// interleave, so the running total cannot be reconstructed from one
	// transaction's slice at commit or abort time.
	spooledBytes      int64
	spooledBytesByXID map[uint32]int64
	maxSpooledBytes   int64
	oversizedPolicy   OversizedTxnPolicy
	oversizedXIDs     map[uint32]bool
	lastCommittedLSN  uint64
}

// OversizedTxnPolicy controls how in-progress streamed transactions exceeding
// MaxSpooledBytes are handled.
type OversizedTxnPolicy int

const (
	// OversizedTxnFailPipeline fails decoding with an explicit error when a transaction
	// exceeds MaxSpooledBytes. This is the default.
	OversizedTxnFailPipeline OversizedTxnPolicy = iota

	// OversizedTxnSkip drops oversized transactions without failing the pipeline,
	// immediately freeing memory and advancing the replication slot on commit to
	// prevent infinite crash loops on bulk DML.
	OversizedTxnSkip
)

// DefaultMaxSpooledTransactionBytes bounds, by default, the total size of
// streamed in-progress transactions the parser will buffer before failing.
//
// It applies only when streaming is enabled, since a non-streamed transaction
// is delivered whole at commit and never spooled.
const DefaultMaxSpooledTransactionBytes int64 = 256 << 20 // 256 MiB

// NewPgOutputParser initializes an empty protocol parser.
func NewPgOutputParser() *PgOutputParser {
	return &PgOutputParser{
		relations:           make(map[uint32]*RelationDef),
		spooledTransactions: make(map[uint32][]*ChangeEvent),
		txSeqByXID:          make(map[uint32]uint32),
		spooledBytesByXID:   make(map[uint32]int64),
		maxSpooledBytes:     DefaultMaxSpooledTransactionBytes,
		oversizedPolicy:     OversizedTxnFailPipeline,
		oversizedXIDs:       make(map[uint32]bool),
	}
}

// SetMaxSpooledBytes overrides the streamed-transaction buffer limit. A value
// of zero or less disables the limit, which restores the previous unbounded
// behaviour and should only be used where transaction size is known to be
// bounded by other means.
func (p *PgOutputParser) SetMaxSpooledBytes(n int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.maxSpooledBytes = n
}

// SetOversizedTxnPolicy configures the policy for handling transactions exceeding
// MaxSpooledBytes.
func (p *PgOutputParser) SetOversizedTxnPolicy(policy OversizedTxnPolicy) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.oversizedPolicy = policy
}

// RegisterRelation explicitly stores a relation definition in the schema cache.
func (p *PgOutputParser) RegisterRelation(rel *RelationDef) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.registerRelationLocked(rel)
}

func (p *PgOutputParser) registerRelationLocked(rel *RelationDef) {
	if rel != nil {
		p.relations[rel.RelationID] = rel
	}
}

// SetRelation stores or updates a relation definition by relation ID.
func (p *PgOutputParser) SetRelation(relID uint32, rel *RelationDef) {
	if rel != nil {
		rel.RelationID = relID
		p.RegisterRelation(rel)
	}
}

// GetRelation retrieves a cached relation definition.
func (p *PgOutputParser) GetRelation(relID uint32) (*RelationDef, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.getRelationLocked(relID)
}

func (p *PgOutputParser) getRelationLocked(relID uint32) (*RelationDef, bool) {
	rel, ok := p.relations[relID]
	return rel, ok
}

// LastCommittedLSN returns the latest transaction commit LSN seen by the parser.
func (p *PgOutputParser) LastCommittedLSN() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.lastCommittedLSN
}

// HasBufferedTransaction reports whether the parser has seen part of a
// transaction but not all of it.
//
// Three cases qualify: an ordinary transaction whose Begin has arrived but
// whose Commit has not, a streamed transaction that is open ('S' seen, no
// commit or abort yet), and spooled events held for streamed transactions
// that have not committed.
//
// The source consults this before treating a server keepalive's WAL position
// as acknowledgeable. pgoutput transmits a non-streamed transaction only after
// it commits, as Begin, changes, Commit, and the walsender can interleave a
// keepalive between those frames. The keepalive's position is at or beyond the
// transaction's commit LSN, so acknowledging it mid-transaction would tell the
// server the transaction is consumed. If the worker then restarts before
// receiving the remaining changes, the server does not resend them and those
// rows are lost.
func (p *PgOutputParser) HasBufferedTransaction() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.inTransaction || p.inStream || len(p.spooledTransactions) > 0 || len(p.oversizedXIDs) > 0
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

	p.mu.Lock()
	defer p.mu.Unlock()

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
		p.txSeqByXID[xid] = 0
		// The transaction is now partially received. Nothing between here and
		// the matching Commit may be acknowledged to the server.
		p.inTransaction = true
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
		p.lastCommittedLSN = endLSN
		// Release the counter; a long-lived stream would otherwise accumulate
		// one map entry per transaction.
		delete(p.txSeqByXID, p.currentXID)
		p.inTransaction = false
		return nil, nil

	case 'R': // Relation
		rel, err := parseRelation(r)
		if err != nil {
			return nil, fmt.Errorf("failed to parse Relation message: %w", err)
		}
		p.registerRelationLocked(rel)
		return nil, nil

	case 'I': // Insert
		var relID uint32
		if err := binary.Read(r, binary.BigEndian, &relID); err != nil {
			return nil, fmt.Errorf("failed to parse Insert relID: %w", err)
		}
		rel, ok := p.getRelationLocked(relID)
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
		afterMap, unchanged := valuesToMap(values)
		ev := &ChangeEvent{
			Operation:        OpInsert,
			Schema:           rel.Namespace,
			Table:            rel.RelationName,
			CommitTime:       p.currentTxTime,
			LSN:              p.currentLSN,
			TransactionID:    p.currentXID,
			PrimaryKeys:      rel.PrimaryKeys,
			Origin:           p.currentOrigin,
			After:            afterMap,
			UnchangedColumns: unchanged,
			ColumnTypes:      valuesToColumnTypes(values),
		}

		return p.emitOrSpool(ev)

	case 'U': // Update
		var relID uint32
		if err := binary.Read(r, binary.BigEndian, &relID); err != nil {
			return nil, fmt.Errorf("failed to parse Update relID: %w", err)
		}
		rel, ok := p.getRelationLocked(relID)
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
			beforeMap, _ = valuesToMap(beforeVals)
		}

		afterMap, unchanged := valuesToMap(afterVals)
		ev := &ChangeEvent{
			Operation:        OpUpdate,
			Schema:           rel.Namespace,
			Table:            rel.RelationName,
			CommitTime:       p.currentTxTime,
			LSN:              p.currentLSN,
			TransactionID:    p.currentXID,
			PrimaryKeys:      rel.PrimaryKeys,
			Origin:           p.currentOrigin,
			Before:           beforeMap,
			After:            afterMap,
			UnchangedColumns: unchanged,
			ColumnTypes:      valuesToColumnTypes(afterVals),
		}

		return p.emitOrSpool(ev)

	case 'D': // Delete
		var relID uint32
		if err := binary.Read(r, binary.BigEndian, &relID); err != nil {
			return nil, fmt.Errorf("failed to parse Delete relID: %w", err)
		}
		rel, ok := p.getRelationLocked(relID)
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
		beforeMap, _ := valuesToMap(values)
		ev := &ChangeEvent{

			Operation:     OpDelete,
			Schema:        rel.Namespace,
			Table:         rel.RelationName,
			CommitTime:    p.currentTxTime,
			LSN:           p.currentLSN,
			TransactionID: p.currentXID,
			PrimaryKeys:   rel.PrimaryKeys,
			Origin:        p.currentOrigin,
			Before:        beforeMap,
			ColumnTypes:   valuesToColumnTypes(values),
		}
		return p.emitOrSpool(ev)

	case 'T': // Truncate
		var numRelations uint32
		var options uint8
		if err := binary.Read(r, binary.BigEndian, &numRelations); err != nil {
			return nil, fmt.Errorf("failed to parse Truncate numRelations: %w", err)
		}
		if err := binary.Read(r, binary.BigEndian, &options); err != nil {
			return nil, fmt.Errorf("failed to parse Truncate options: %w", err)
		}
		// numRelations sizes an allocation from a wire-supplied uint32, so a
		// corrupted frame could otherwise reserve tens of gigabytes. Each
		// relation ID occupies 4 bytes, so the frame's remaining length is an
		// exact bound.
		if int64(numRelations)*4 > int64(r.Len()) {
			return nil, fmt.Errorf("truncate relation count %d exceeds %d bytes remaining in frame", numRelations, r.Len())
		}
		relIDs := make([]uint32, numRelations)
		for i := 0; i < int(numRelations); i++ {
			if err := binary.Read(r, binary.BigEndian, &relIDs[i]); err != nil {
				return nil, fmt.Errorf("failed to parse Truncate relation ID %d: %w", i, err)
			}
		}
		var truncEvents []*ChangeEvent
		for _, relID := range relIDs {
			var tableName, schemaName string
			if rel, ok := p.getRelationLocked(relID); ok {
				tableName = rel.RelationName
				schemaName = rel.Namespace
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
			emitted, err := p.emitOrSpool(ev)
			if err != nil {
				return nil, err
			}
			truncEvents = append(truncEvents, emitted...)
		}
		return truncEvents, nil

	case 'O': // Origin (Replication loop prevention)
		var lsn uint64
		if err := binary.Read(r, binary.BigEndian, &lsn); err != nil {
			return nil, fmt.Errorf("failed to parse Origin LSN: %w", err)
		}
		origin, err := readNullTerminatedString(r)
		if err != nil {
			return nil, fmt.Errorf("failed to parse Origin name: %w", err)
		}
		p.currentOrigin = origin
		return nil, nil

	case 'M': // Generic logical message
		return nil, nil

	// The stream-control frames below set the transaction identity that every
	// subsequent event is stamped with. A discarded read error here leaves the
	// target at zero and decoding continues, so events are attributed to XID 0
	// or to the previous transaction's LSN. That is not recoverable downstream
	// because nothing marks the values as untrustworthy, so each read is
	// checked and a short frame fails the message instead.
	case 'S': // Stream Start
		var xid uint32
		var firstSegment uint8
		if err := binary.Read(r, binary.BigEndian, &xid); err != nil {
			return nil, fmt.Errorf("failed to parse Stream Start XID: %w", err)
		}
		if err := binary.Read(r, binary.BigEndian, &firstSegment); err != nil {
			return nil, fmt.Errorf("failed to parse Stream Start segment flag: %w", err)
		}
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
		if err := binary.Read(r, binary.BigEndian, &xid); err != nil {
			return nil, fmt.Errorf("failed to parse Stream Commit XID: %w", err)
		}
		if err := binary.Read(r, binary.BigEndian, &flags); err != nil {
			return nil, fmt.Errorf("failed to parse Stream Commit flags: %w", err)
		}
		if err := binary.Read(r, binary.BigEndian, &commitLSN); err != nil {
			return nil, fmt.Errorf("failed to parse Stream Commit LSN: %w", err)
		}
		if err := binary.Read(r, binary.BigEndian, &endLSN); err != nil {
			return nil, fmt.Errorf("failed to parse Stream Commit end LSN: %w", err)
		}
		p.currentLSN = endLSN
		p.lastCommittedLSN = endLSN
		if p.oversizedXIDs[xid] {
			delete(p.oversizedXIDs, xid)
			delete(p.txSeqByXID, xid)
			return nil, nil
		}
		spooled := p.spooledTransactions[xid]
		delete(p.spooledTransactions, xid)
		delete(p.txSeqByXID, xid)
		p.spooledBytes -= p.spooledBytesByXID[xid]
		if p.spooledBytes < 0 {
			p.spooledBytes = 0
		}
		delete(p.spooledBytesByXID, xid)

		// Assign the commit LSN unconditionally. A streamed transaction has no
		// BEGIN record, so these events were stamped with whatever LSN the
		// previously decoded transaction left in currentLSN. That value is
		// stale and non-zero, so a zero-check would never correct it and the
		// events would sort into the wrong transaction.
		for _, ev := range spooled {
			if ev != nil {
				ev.LSN = commitLSN
			}
		}
		return spooled, nil

	case 'A': // Stream Abort
		var xid, subxid uint32
		if err := binary.Read(r, binary.BigEndian, &xid); err != nil {
			return nil, fmt.Errorf("failed to parse Stream Abort XID: %w", err)
		}
		if err := binary.Read(r, binary.BigEndian, &subxid); err != nil {
			return nil, fmt.Errorf("failed to parse Stream Abort sub-XID: %w", err)
		}
		if subxid == 0 || subxid == xid {
			delete(p.oversizedXIDs, xid)
			delete(p.spooledTransactions, xid)
			delete(p.txSeqByXID, xid)
			p.spooledBytes -= p.spooledBytesByXID[xid]
			if p.spooledBytes < 0 {
				p.spooledBytes = 0
			}
			delete(p.spooledBytesByXID, xid)
		}
		return nil, nil

	case 'b', 'P', 'K', 'r', 'p':
		// Two-phase commit frames: Begin Prepare, Prepare, Commit Prepared,
		// Rollback Prepared, Stream Prepare.
		//
		// These are refused rather than skipped. Skipping them is not
		// equivalent to not receiving them: without Begin Prepare the changes
		// that follow inherit the previous transaction's XID, LSN and commit
		// timestamp, and without Rollback Prepared the changes of an aborted
		// prepared transaction stay downstream permanently. Both corrupt the
		// output quietly.
		//
		// CDCOptions.Validate refuses two_phase, so the server is not asked to
		// send these. Reaching this point means that guard was bypassed.
		return nil, fmt.Errorf("postgresio: two-phase commit message %q received but 2PC decoding is not supported", msgType)

	default:
		// Unsupported or extension message, skip gracefully
		return nil, nil
	}
}

// estimateEventBytes approximates the retained heap size of a spooled event.
//
// It is deliberately cheap and approximate: it is used to decide when to stop
// buffering, not to report memory. Exact accounting would mean walking every
// value on the hot path for a number that only needs to be the right order of
// magnitude.
func estimateEventBytes(ev *ChangeEvent) int64 {
	if ev == nil {
		return 0
	}
	// Base covers the struct itself plus the short fixed strings on it.
	size := int64(256 + len(ev.Schema) + len(ev.Table) + len(ev.Origin) + len(ev.EventID))
	for _, m := range []map[string]any{ev.Before, ev.After} {
		for k, v := range m {
			// Per-entry map overhead plus the key, then the value.
			size += int64(48 + len(k))
			switch t := v.(type) {
			case string:
				size += int64(len(t))
			case []byte:
				size += int64(len(t))
			default:
				size += 16
			}
		}
	}
	return size
}

// emitOrSpool stamps the intra-transaction sequence number and either returns
// the event or holds it until the streamed transaction commits.
//
// The sequence is assigned here because this is the one path every change
// event takes, and it must be set before PopulateEventID since the event ID is
// derived from it.
func (p *PgOutputParser) emitOrSpool(ev *ChangeEvent) ([]*ChangeEvent, error) {
	if ev != nil {
		ev.TxSeq = p.txSeqByXID[p.currentXID]
		p.txSeqByXID[p.currentXID]++
		ev.PopulateEventID()
	}
	if p.inStream {
		if p.oversizedXIDs[p.currentXID] {
			return nil, nil
		}
		// Charge the event before buffering it so the limit is enforced on the
		// allocation that is about to be retained, not the previous one.
		sz := estimateEventBytes(ev)
		if p.maxSpooledBytes > 0 && p.spooledBytes+sz > p.maxSpooledBytes {
			// Immediately free all previously spooled events for this transaction to reclaim worker heap
			p.spooledBytes -= p.spooledBytesByXID[p.currentXID]
			if p.spooledBytes < 0 {
				p.spooledBytes = 0
			}
			delete(p.spooledBytesByXID, p.currentXID)
			delete(p.spooledTransactions, p.currentXID)
			p.oversizedXIDs[p.currentXID] = true

			if p.oversizedPolicy == OversizedTxnSkip {
				return nil, nil
			}
			return nil, fmt.Errorf(
				"postgresio: streamed transaction buffer would exceed %d bytes (xid %d, %d bytes already buffered): "+
					"the in-progress transaction is too large to reassemble in worker memory; "+
					"raise the limit with SetMaxSpooledBytes or set WithCDCOversizedTxnPolicy(OversizedTxnSkip)",
				p.maxSpooledBytes, p.currentXID, p.spooledBytes)
		}
		p.spooledBytes += sz
		p.spooledBytesByXID[p.currentXID] += sz
		p.spooledTransactions[p.currentXID] = append(p.spooledTransactions[p.currentXID], ev)
		return nil, nil
	}
	return []*ChangeEvent{ev}, nil
}

// ParseXLogData decodes a WAL data ('w') message envelope.
// In the PostgreSQL streaming replication protocol, CopyData ('d') messages carrying
// WAL data begin with Byte1('w'), followed by startLSN (8 bytes), endWAL (8 bytes),
// serverTime (8 bytes), and the inner pgoutput stream message payload.
func ParseXLogData(data []byte) (startLSN, endWAL uint64, serverTime time.Time, walData []byte, err error) {
	if len(data) < 25 || data[0] != 'w' {
		return 0, 0, time.Time{}, nil, fmt.Errorf("invalid XLogData message")
	}
	r := bytes.NewReader(data[1:])
	var serverMicros int64
	if err := binary.Read(r, binary.BigEndian, &startLSN); err != nil {
		return 0, 0, time.Time{}, nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &endWAL); err != nil {
		return 0, 0, time.Time{}, nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &serverMicros); err != nil {
		return 0, 0, time.Time{}, nil, err
	}
	return startLSN, endWAL, PgTimeToGo(serverMicros), data[25:], nil
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
	// numCols is signed on the wire and sizes an allocation, so a negative
	// value panics make() before any read can fail. Each column contributes at
	// least a flags byte, a NUL terminator, and 8 bytes of type information, so
	// the bytes remaining in the frame are an exact upper bound on how many
	// columns can actually follow.
	if numCols < 0 {
		return nil, fmt.Errorf("negative column count %d in Relation message", numCols)
	}
	if int(numCols) > r.Len() {
		return nil, fmt.Errorf("Relation column count %d exceeds %d bytes remaining in frame", numCols, r.Len())
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

// readTupleField reads one length-prefixed column datum from r.
//
// The declared length is validated before anything is allocated. pgoutput
// encodes it as a signed int32, so a corrupted or truncated frame can present a
// negative value, which panics make with "len out of range", or a large
// positive value, which reserves up to 2GB for a frame that cannot contain it.
// Either one kills the worker, and because Beam retries a failed bundle against
// the same bytes the crash repeats instead of clearing.
//
// r holds the entire frame in memory, so r.Len() is an exact upper bound on any
// field within it and no arbitrary cap is needed. Using io.ReadFull rather than
// r.Read matters for the same reason: a short read would otherwise leave the
// tail of the buffer as zeros and decode as a valid, wrong value.
func readTupleField(r *bytes.Reader) ([]byte, error) {
	var length int32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, fmt.Errorf("negative field length %d", length)
	}
	if int(length) > r.Len() {
		return nil, fmt.Errorf("field length %d exceeds %d bytes remaining in frame", length, r.Len())
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func parseTupleData(r *bytes.Reader, cols []ColumnDef) ([]ColumnValue, error) {
	var numCols int16
	if err := binary.Read(r, binary.BigEndian, &numCols); err != nil {
		return nil, err
	}
	// Same exposure as the field length above: numCols is signed and sizes an
	// allocation. Every column costs at least its one-byte kind tag, so a count
	// larger than the bytes left in the frame cannot be honest.
	if numCols < 0 {
		return nil, fmt.Errorf("negative column count %d", numCols)
	}
	if int(numCols) > r.Len() {
		return nil, fmt.Errorf("column count %d exceeds %d bytes remaining in frame", numCols, r.Len())
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
			valBytes, err := readTupleField(r)
			if err != nil {
				return nil, fmt.Errorf("column %d (%s): %w", i, colName, err)
			}
			cv.Value = parseTextValue(colType, string(valBytes))
		case 'b': // Binary formatted value
			valBytes, err := readTupleField(r)
			if err != nil {
				return nil, fmt.Errorf("column %d (%s): %w", i, colName, err)
			}
			cv.Value = parseBinaryValue(colType, valBytes)
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

// valuesToMap converts decoded column values into a map.
//
// Columns whose TOASTed value was not modified by the UPDATE are omitted
// entirely rather than given a placeholder. The server does not transmit
// these values at all, and inventing one is unsafe: a sentinel string is a
// type violation for a JSONB, NUMERIC or BIGINT column, while NULL would
// instruct the sink to erase a value the source still holds. Callers learn
// which columns were withheld from the second return value.
func valuesToMap(values []ColumnValue) (map[string]any, []string) {
	m := make(map[string]any, len(values))
	var unchanged []string

	for _, v := range values {
		switch {
		case v.IsToastUnchanged:
			unchanged = append(unchanged, v.Name)
		case v.IsNull:
			m[v.Name] = nil
		default:
			m[v.Name] = v.Value
		}
	}
	return m, unchanged
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

func parseBinaryValue(typeOID uint32, valBytes []byte) any {
	switch typeOID {
	case 16: // bool
		if len(valBytes) >= 1 {
			return valBytes[0] != 0
		}
		return false
	case 20: // int8 (bigint)
		if len(valBytes) >= 8 {
			return int64(binary.BigEndian.Uint64(valBytes))
		}
	case 21: // int2 (smallint)
		if len(valBytes) >= 2 {
			return int16(binary.BigEndian.Uint16(valBytes))
		}
	case 23: // int4 (integer)
		if len(valBytes) >= 4 {
			return int32(binary.BigEndian.Uint32(valBytes))
		}
	case 700: // float4
		if len(valBytes) >= 4 {
			bits := binary.BigEndian.Uint32(valBytes)
			return float64(math.Float32frombits(bits))
		}
	case 701: // float8
		if len(valBytes) >= 8 {
			bits := binary.BigEndian.Uint64(valBytes)
			return math.Float64frombits(bits)
		}
	case 25, 1043, 1042: // text, varchar, bpchar
		return string(valBytes)
	case 17: // bytea
		return valBytes
	case 114: // json
		return string(valBytes)
	case 3802: // jsonb
		if str, err := DecodeBinaryJSONB(valBytes); err == nil {
			return str
		}
		return string(valBytes)
	case 1114, 1184: // timestamp, timestamptz
		if len(valBytes) >= 8 {
			micros := int64(binary.BigEndian.Uint64(valBytes))
			return pgEpoch.Add(time.Duration(micros) * time.Microsecond)
		}
	case 1082: // date
		if len(valBytes) >= 4 {
			days := int32(binary.BigEndian.Uint32(valBytes))
			return pgEpoch.AddDate(0, 0, int(days))
		}
	case 1700: // numeric
		// Arbitrary-precision decimal. Falling through to the default branch
		// returned an opaque []byte, which silently corrupts monetary data.
		if s, err := decodeBinaryNumeric(valBytes); err == nil {
			return s
		}
	case 2950: // uuid
		if s, err := decodeBinaryUUID(valBytes); err == nil {
			return s
		}
	case 1186: // interval
		if iv, err := decodeBinaryInterval(valBytes); err == nil {
			return iv
		}
	case 600: // point
		if len(valBytes) >= 16 {
			xBits := binary.BigEndian.Uint64(valBytes[0:8])
			yBits := binary.BigEndian.Uint64(valBytes[8:16])
			return PgPoint{
				X: math.Float64frombits(xBits),
				Y: math.Float64frombits(yBits),
			}
		}

	default:
		if isArrayOID(typeOID) {
			if arr, err := DecodeBinaryArray(valBytes); err == nil {
				return arr
			}
		}
	}
	return valBytes
}
