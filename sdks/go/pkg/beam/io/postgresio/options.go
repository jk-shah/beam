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
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"
)

// WriteMode defines the write mutation strategy against the target PostgreSQL table.
type WriteMode int

const (
	// WriteModeInsert executes standard parameterized INSERT statements.
	WriteModeInsert WriteMode = iota
	// WriteModeUpsert executes idempotent INSERT ... ON CONFLICT (pks) DO UPDATE statements.
	WriteModeUpsert
	// WriteModeUpdate executes bulk UPDATE statements matched on primary key columns.
	WriteModeUpdate
)

var identifierRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_$]*$`)

// WriteMethod determines the bulk loading mechanism used to insert or upsert rows.
type WriteMethod int

const (
	// WriteMethodStagedCopy utilizes PostgreSQL COPY into a session temporary table
	// followed by an atomic set-based INSERT ... SELECT ... ON CONFLICT DO UPDATE.
	// Recommended for maximum throughput (>100,000 rows/sec).
	WriteMethodStagedCopy WriteMethod = iota

	// WriteMethodUnnest utilizes parameterized UNNEST($1, $2, ...) upsert queries.
	WriteMethodUnnest
)

// DialFunc defines a pluggable network dialer interface for connecting to PostgreSQL.
// Used by cloud-specific dialers (e.g., Google Cloud SQL, AlloyDB) to establish
// authenticated mTLS socket connections.
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// WriteOptions configures connection pooling, write mutation modes, batch thresholds,
// and safety guards for writing to PostgreSQL.
type WriteOptions struct {
	Host               string
	Port               int
	Database           string
	Username           string
	Password           string `beam:"-" json:"-"`
	SSLMode            string
	WriteMode          WriteMode
	WriteMethod        WriteMethod
	PrimaryKeyCols     []string
	BatchSize          int
	MaxBatchBytes      int
	FlushInterval      time.Duration
	MaxConnections        int
	UsePgBouncer          bool
	ConnectionInitSQL     string
	ReplicationOriginName string
	DialFunc              DialFunc `beam:"-" json:"-"`
}

// Option represents a functional option for configuring WriteOptions.
type Option func(*WriteOptions)

// NewWriteOptions creates a WriteOptions struct initialized with production defaults.
func NewWriteOptions(opts ...Option) WriteOptions {
	wo := WriteOptions{
		Port:           5432,
		SSLMode:        "disable",
		WriteMode:      WriteModeUpsert,
		WriteMethod:    WriteMethodStagedCopy,
		BatchSize:      5000,
		MaxBatchBytes:  8 * 1024 * 1024, // 8 MB
		FlushInterval:  1 * time.Second,
		MaxConnections: 2,
	}
	for _, opt := range opts {
		opt(&wo)
	}
	return wo
}


// WithHost sets the target PostgreSQL server hostname or IP address.
func WithHost(host string) Option {
	return func(o *WriteOptions) {
		o.Host = host
	}
}

// WithPort sets the target PostgreSQL server TCP port.
func WithPort(port int) Option {
	return func(o *WriteOptions) {
		o.Port = port
	}
}

// WithDatabase sets the target database name.
func WithDatabase(db string) Option {
	return func(o *WriteOptions) {
		o.Database = db
	}
}

// WithUsername sets the database authentication username.
func WithUsername(user string) Option {
	return func(o *WriteOptions) {
		o.Username = user
	}
}

// WithPassword sets the database authentication password.
func WithPassword(pass string) Option {
	return func(o *WriteOptions) {
		o.Password = pass
	}
}

// WithSSLMode sets the SSL/TLS connection mode (disable, require, verify-ca, verify-full).
func WithSSLMode(sslMode string) Option {
	return func(o *WriteOptions) {
		o.SSLMode = sslMode
	}
}

// WithWriteMethod sets the bulk write method (WriteMethodStagedCopy or WriteMethodUnnest).
func WithWriteMethod(method WriteMethod) Option {
	return func(o *WriteOptions) {
		o.WriteMethod = method
	}
}

// WithWriteMode configures the write strategy (Insert, Upsert, or Update).
func WithWriteMode(mode WriteMode) Option {
	return func(o *WriteOptions) {
		o.WriteMode = mode
	}
}

// WithPrimaryKeyColumns specifies primary key column names required for upserts and deadlock sorting.
func WithPrimaryKeyColumns(cols ...string) Option {
	return func(o *WriteOptions) {
		o.PrimaryKeyCols = cols
	}
}

// WithBatchSize sets the maximum number of rows buffered before issuing a batch write.
func WithBatchSize(size int) Option {
	return func(o *WriteOptions) {
		o.BatchSize = size
	}
}

// WithMaxBatchBytes sets the maximum payload byte volume buffered before flushing.
func WithMaxBatchBytes(bytes int) Option {
	return func(o *WriteOptions) {
		o.MaxBatchBytes = bytes
	}
}

// WithFlushInterval sets the maximum latency before buffering rows are flushed.
func WithFlushInterval(interval time.Duration) Option {
	return func(o *WriteOptions) {
		o.FlushInterval = interval
	}
}

// WithMaxConnections clamps the worker connection pool size.
func WithMaxConnections(maxConns int) Option {
	return func(o *WriteOptions) {
		o.MaxConnections = maxConns
	}
}

// WithPgBouncer enables compatibility flags for PgBouncer in transaction pooling mode.
func WithPgBouncer(usePgBouncer bool) Option {
	return func(o *WriteOptions) {
		o.UsePgBouncer = usePgBouncer
	}
}

// WithDialFunc injects a custom network dialer for Cloud SQL, AlloyDB, or proxy tunnels.
func WithDialFunc(dialFunc DialFunc) Option {
	return func(o *WriteOptions) {
		o.DialFunc = dialFunc
	}
}

var originNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_]{1,64}$`)

// WithReplicationOriginName configures a replication origin identifier for the sink.
// When specified, write transactions are tagged with this origin to prevent bidirectional
// replication loops when replicating between active-active PostgreSQL databases.
func WithReplicationOriginName(originName string) Option {
	return func(o *WriteOptions) {
		if originName != "" && !originNameRegex.MatchString(originName) {
			panic(fmt.Sprintf("postgresio: invalid replication origin name %q (must match ^[a-zA-Z0-9_]{1,64}$)", originName))
		}
		o.ReplicationOriginName = originName
	}
}

// SanitizeIdentifier validates that an identifier conforms to PostgreSQL naming rules,
// rejects null bytes (\0) and quotation marks to prevent SQL injection, and wraps the
// identifier in double quotes.
func SanitizeIdentifier(ident string) (string, error) {
	if strings.TrimSpace(ident) == "" {
		return "", fmt.Errorf("postgresio: identifier cannot be empty")
	}
	if strings.Contains(ident, "\x00") {
		return "", fmt.Errorf("postgresio: identifier contains illegal null byte: %q", ident)
	}
	if strings.Contains(ident, "\"") {
		return "", fmt.Errorf("postgresio: identifier contains illegal quotation mark: %q", ident)
	}
	if !identifierRegex.MatchString(ident) {
		return "", fmt.Errorf("postgresio: identifier %q contains invalid characters (must match ^[a-zA-Z_][a-zA-Z0-9_$]*$)", ident)
	}
	return `"` + ident + `"`, nil
}

// SanitizeTableIdentifier validates and escapes a qualified table identifier (e.g. "public.orders"
// or "orders"), ensuring all schema and table components are safely enclosed in double quotes.
func SanitizeTableIdentifier(table string) (string, error) {
	if strings.TrimSpace(table) == "" {
		return "", fmt.Errorf("postgresio: table name cannot be empty")
	}
	parts := strings.Split(table, ".")
	if len(parts) > 2 {
		return "", fmt.Errorf("postgresio: invalid table identifier with more than 2 parts: %q", table)
	}
	sanitizedParts := make([]string, len(parts))
	for i, part := range parts {
		sanitized, err := SanitizeIdentifier(part)
		if err != nil {
			return "", err
		}
		sanitizedParts[i] = sanitized
	}
	return strings.Join(sanitizedParts, "."), nil
}
