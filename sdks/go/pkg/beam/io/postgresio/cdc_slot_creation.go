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
	"encoding/binary"
	"fmt"
	"strings"
)

// SlotCreationResult describes the outcome of CREATE_REPLICATION_SLOT.
//
// SnapshotName is the key field: it identifies a consistent snapshot taken at
// exactly the slot's starting LSN. A backfill query run inside a transaction
// that adopts this snapshot sees the database as of that LSN, and the
// replication stream then delivers every change after it. That is the only
// race-free way to load pre-existing rows: without it a pipeline either
// misses changes made during the backfill or double-counts them.
type SlotCreationResult struct {
	SlotName       string
	ConsistentLSN  string
	SnapshotName   string
	OutputPlugin   string
	AlreadyExisted bool
}

// SnapshotIsolationStatements returns the statements a backfill connection
// must run, in order, to read at the slot's consistent snapshot.
//
// The transaction must be REPEATABLE READ or SERIALIZABLE; PostgreSQL rejects
// SET TRANSACTION SNAPSHOT under READ COMMITTED because the snapshot would be
// discarded at the first statement boundary.
func (r SlotCreationResult) SnapshotIsolationStatements() []string {
	if r.SnapshotName == "" {
		return nil
	}
	return []string{
		"BEGIN ISOLATION LEVEL REPEATABLE READ",
		fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", escapeSQLLiteral(r.SnapshotName)),
	}
}

// buildCreateSlotQuery returns the CREATE_REPLICATION_SLOT command.
//
// EXPORT_SNAPSHOT asks the server to retain a snapshot at the slot's starting
// LSN so an initial backfill can read a consistent view of the tables. The
// TWO_PHASE option is only emitted when requested, because it requires
// PostgreSQL 15 or newer.
func buildCreateSlotQuery(slotName, plugin string, exportSnapshot, twoPhase bool) (string, error) {
	if !validSlotRegex.MatchString(slotName) {
		return "", fmt.Errorf("invalid replication slot name %q: must match ^[a-z0-9_]{1,63}$", slotName)
	}
	if plugin == "" {
		plugin = "pgoutput"
	}
	if _, err := SanitizeIdentifier(plugin); err != nil {
		return "", fmt.Errorf("invalid output plugin %q: %w", plugin, err)
	}

	var sb strings.Builder
	sb.WriteString("CREATE_REPLICATION_SLOT ")
	sb.WriteString(slotName)
	sb.WriteString(" LOGICAL ")
	sb.WriteString(plugin)

	if twoPhase {
		sb.WriteString(" TWO_PHASE")
	}
	if exportSnapshot {
		sb.WriteString(" EXPORT_SNAPSHOT")
	} else {
		sb.WriteString(" NOEXPORT_SNAPSHOT")
	}

	return sb.String(), nil
}

// buildDropSlotQuery returns a DROP_REPLICATION_SLOT command.
func buildDropSlotQuery(slotName string, wait bool) (string, error) {
	if !validSlotRegex.MatchString(slotName) {
		return "", fmt.Errorf("invalid replication slot name %q: must match ^[a-z0-9_]{1,63}$", slotName)
	}
	if wait {
		return fmt.Sprintf("DROP_REPLICATION_SLOT %s WAIT", slotName), nil
	}
	return fmt.Sprintf("DROP_REPLICATION_SLOT %s", slotName), nil
}

// parseCreateSlotResponse extracts the slot metadata from the single result
// row returned by CREATE_REPLICATION_SLOT.
//
// The row carries four text columns: slot_name, consistent_point,
// snapshot_name and output_plugin. snapshot_name is empty when the slot was
// created with NOEXPORT_SNAPSHOT.
func parseCreateSlotResponse(fields []string) (SlotCreationResult, error) {
	if len(fields) < 2 {
		return SlotCreationResult{}, fmt.Errorf("CREATE_REPLICATION_SLOT returned %d fields, expected at least 2", len(fields))
	}

	res := SlotCreationResult{
		SlotName:      fields[0],
		ConsistentLSN: fields[1],
	}
	if len(fields) > 2 {
		res.SnapshotName = fields[2]
	}
	if len(fields) > 3 {
		res.OutputPlugin = fields[3]
	}
	return res, nil
}

// isDuplicateSlotError reports whether an error is PostgreSQL's
// "replication slot already exists" (SQLSTATE 42710, duplicate_object).
//
// Treated as success by the create-if-missing path: another worker or a
// previous run may have created the slot, and failing in that case would make
// the option unusable.
func isDuplicateSlotError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "42710") ||
		(strings.Contains(msg, "already exists") && strings.Contains(msg, "slot"))
}

// escapeSQLLiteral escapes single quotes for interpolation into a SQL string
// literal.
func escapeSQLLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// parseDataRow decodes a PostgreSQL DataRow ('D') message body into its text
// field values.
//
// Layout: int16 field count, then per field an int32 length followed by that
// many bytes. A length of -1 means SQL NULL, which is reported as an empty
// string; CREATE_REPLICATION_SLOT returns NULL for snapshot_name when the slot
// was created with NOEXPORT_SNAPSHOT, and an empty string is the correct
// representation for that caller.
func parseDataRow(payload []byte) ([]string, error) {
	if len(payload) < 2 {
		return nil, fmt.Errorf("DataRow too short: %d bytes", len(payload))
	}
	fieldCount := int(binary.BigEndian.Uint16(payload[0:2]))
	offset := 2

	fields := make([]string, 0, fieldCount)
	for i := 0; i < fieldCount; i++ {
		if offset+4 > len(payload) {
			return nil, fmt.Errorf("DataRow truncated reading length of field %d", i)
		}
		length := int32(binary.BigEndian.Uint32(payload[offset : offset+4]))
		offset += 4

		if length == -1 {
			fields = append(fields, "")
			continue
		}
		if length < -1 {
			return nil, fmt.Errorf("DataRow field %d has invalid length %d", i, length)
		}
		if offset+int(length) > len(payload) {
			return nil, fmt.Errorf("DataRow truncated reading value of field %d", i)
		}
		fields = append(fields, string(payload[offset:offset+int(length)]))
		offset += int(length)
	}
	return fields, nil
}
