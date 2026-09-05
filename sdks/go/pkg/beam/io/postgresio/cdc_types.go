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
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*ChangeEvent)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*ColumnValue)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*StandbyStatus)(nil)).Elem())

	beam.RegisterCoder(
		reflect.TypeOf((*ChangeEvent)(nil)).Elem(),
		encodeChangeEvent,
		decodeChangeEvent,
	)
}

func encodeChangeEvent(in ChangeEvent) ([]byte, error) {
	return json.Marshal(in)
}

func decodeChangeEvent(in []byte) (ChangeEvent, error) {
	var out ChangeEvent
	err := json.Unmarshal(in, &out)
	return out, err
}

// OpType represents the mutation operation type captured by PostgreSQL CDC.
type OpType string

const (
	// OpInsert indicates a new record inserted into the relation.
	OpInsert OpType = "INSERT"
	// OpUpdate indicates an existing record modified in the relation.
	OpUpdate OpType = "UPDATE"
	// OpDelete indicates a record removed from the relation.
	OpDelete OpType = "DELETE"
	// OpTruncate indicates a table truncation event.
	OpTruncate OpType = "TRUNCATE"
)

// ColumnValue captures a single attribute datum within a CDC change tuple.
type ColumnValue struct {
	Name             string `beam:"name" json:"name"`
	TypeOID          uint32 `beam:"type_oid" json:"type_oid"`
	Value            any    `beam:"value" json:"value"`
	IsNull           bool   `beam:"is_null" json:"is_null"`
	IsToastUnchanged bool   `beam:"is_toast_unchanged" json:"is_toast_unchanged"`
}

// ChangeEvent represents an immutable Change Data Capture event decoded from
// PostgreSQL's logical replication stream (pgoutput).
type ChangeEvent struct {
	Operation     OpType         `beam:"operation" json:"operation"`
	Schema        string         `beam:"schema" json:"schema"`
	Table         string         `beam:"table" json:"table"`
	CommitTime    time.Time      `beam:"commit_time" json:"commit_time"`
	LSN           uint64         `beam:"lsn" json:"lsn"`
	TransactionID uint32         `beam:"transaction_id" json:"transaction_id"`
	PrimaryKeys   []string       `beam:"primary_keys" json:"primary_keys"`
	Before        map[string]any `beam:"before" json:"before,omitempty"`
	After         map[string]any `beam:"after" json:"after,omitempty"`
}

// FullTableName returns the fully qualified schema.table name.
func (e ChangeEvent) FullTableName() string {
	if e.Schema == "" {
		return e.Table
	}
	return fmt.Sprintf("%s.%s", e.Schema, e.Table)
}

// PrimaryKeyString returns a deterministic string representation of the composite
// primary key values used for downstream Beam KeyBy and Reshuffle partitioning.
func (e ChangeEvent) PrimaryKeyString() string {
	source := e.After
	if len(source) == 0 {
		source = e.Before
	}
	if len(e.PrimaryKeys) == 0 || len(source) == 0 {
		return fmt.Sprintf("%s:%d", e.FullTableName(), e.LSN)
	}

	parts := make([]string, len(e.PrimaryKeys))
	for i, pk := range e.PrimaryKeys {
		if val, exists := source[pk]; exists {
			parts[i] = fmt.Sprintf("%v", val)
		} else {
			parts[i] = "null"
		}
	}
	return fmt.Sprintf("%s:%s", e.FullTableName(), strings.Join(parts, "|"))
}

// StandbyStatus encapsulates client replication position acknowledged to PostgreSQL.
type StandbyStatus struct {
	WriteLSN       uint64    `beam:"write_lsn" json:"write_lsn"`
	FlushLSN       uint64    `beam:"flush_lsn" json:"flush_lsn"`
	ApplyLSN       uint64    `beam:"apply_lsn" json:"apply_lsn"`
	ClientTime     time.Time `beam:"client_time" json:"client_time"`
	ReplyRequested bool      `beam:"reply_requested" json:"reply_requested"`
}

// TokenProvider defines an interface for fetching and renewing dynamic database
// authentication credentials (e.g., Cloud SQL IAM, AlloyDB, AWS RDS 15-minute IAM tokens).
type TokenProvider interface {
	GetPassword(ctx context.Context) (string, error)
}

// StaticTokenProvider provides a fixed database password.
type StaticTokenProvider struct {
	Password string
}

// NewStaticTokenProvider creates a TokenProvider returning a static password.
func NewStaticTokenProvider(password string) *StaticTokenProvider {
	return &StaticTokenProvider{Password: password}
}

// GetPassword returns the static password.
func (p *StaticTokenProvider) GetPassword(ctx context.Context) (string, error) {
	return p.Password, nil
}
