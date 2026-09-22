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
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/schematransform"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

const (
	// WriteSchemaTransformURN is the canonical URN for PostgreSQL Write SchemaTransform.
	WriteSchemaTransformURN = "beam:schematransform:org.apache.beam:postgres_write:v1"

	// ReadSchemaTransformURN is the standard URN for PostgreSQL SchemaTransform Bounded Batch Read.
	ReadSchemaTransformURN = "beam:schematransform:org.apache.beam:postgres_read:v1"

	// ReadCDCSchemaTransformURN is the canonical URN for PostgreSQL ReadCDC SchemaTransform.
	ReadCDCSchemaTransformURN = "beam:schematransform:org.apache.beam:postgres_read_cdc:v1"
)

// FailedMutation represents a dead-letter queue record with portable Beam Schema.
type FailedMutation struct {
	RowPayload   string `beam:"row_payload"`
	ErrorMessage string `beam:"error_message"`
	SqlState     string `beam:"sql_state"`
}

func formatFailedRowFn(f FailedRow) FailedMutation {
	rowBytes, _ := json.Marshal(f.Row)
	return FailedMutation{
		RowPayload:   string(rowBytes),
		ErrorMessage: f.ErrorMessage,
		SqlState:     f.SqlState,
	}
}

func init() {
	beam.RegisterFunction(formatFailedRowFn)
	schema.RegisterType(reflect.TypeOf((*FailedMutation)(nil)).Elem())
	schematransform.GlobalRegisterTyped[PostgreSqlWriteConfig](&postgreSqlWriteProvider{})
	schematransform.GlobalRegisterTyped[PostgreSqlReadConfig](&postgreSqlReadProvider{})
}

// PostgreSqlWriteConfig defines the configuration parameters for the PostgreSQL Write SchemaTransform.
type PostgreSqlWriteConfig struct {
	Host              string   `beam:"host" doc:"PostgreSQL database server hostname or IP address."`
	Port              int32    `beam:"port" doc:"PostgreSQL database server port (default: 5432)."`
	Database          string   `beam:"database" doc:"Target PostgreSQL database name."`
	Table             string   `beam:"table" doc:"Target PostgreSQL table, schema-qualified (for example public.orders)."`
	Username          string   `beam:"username" doc:"Authentication username."`
	Password          string   `beam:"password,secret" doc:"Authentication password."`
	PasswordEnvVar    string   `beam:"password_env_var" doc:"Environment variable name on the worker containing the authentication password."`
	SSLMode           string   `beam:"sslmode" doc:"SSL mode (e.g. disable, require, verify-ca, verify-full)."`
	SSLRootCert       string   `beam:"sslrootcert" doc:"Path to SSL root certificate file (PEM format) for verify-ca/verify-full."`
	ConflictKeys      []string `beam:"conflict_keys" doc:"Columns used as primary or unique key conflict targets for UPSERT."`
	UpdateFields      []string `beam:"update_fields" doc:"Columns to update ON CONFLICT DO UPDATE. If empty, uses DO NOTHING."`
	MaxBatchRows      int32    `beam:"max_batch_rows" doc:"Maximum rows per batch UNNEST statement (default: 5000)."`
	MaxBatchBytes     int32    `beam:"max_batch_bytes" doc:"Maximum byte buffer threshold before flushing (default: 8MB)."`
	UsePgBouncer      bool     `beam:"use_pgbouncer" doc:"Enable single-statement transaction pooling for PgBouncer compatibility."`
	ReplicationOrigin string   `beam:"replication_origin" doc:"Replication origin name to tag write transactions to prevent cyclic loops."`
	WriteMode         string   `beam:"write_mode" doc:"Write mutation mode (INSERT, UPSERT, UPDATE, MERGE). Default UPSERT."`
	OpColumn          string   `beam:"op_column" doc:"Column name containing CDC operation type for MERGE mode."`
	DeleteOpValue     string   `beam:"delete_op_value" doc:"Value in op_column that indicates a DELETE in MERGE mode."`
	ExplainAnalyze    bool     `beam:"explain_analyze" doc:"Enable in-band EXPLAIN (ANALYZE, BUFFERS) query plan sampling on sink batches."`
}

// Validate checks configuration invariants before pipeline graph expansion.
func (c PostgreSqlWriteConfig) Validate() error {
	if c.Host == "" {
		return errors.New("host cannot be empty")
	}
	if c.Database == "" {
		return errors.New("database cannot be empty")
	}
	if c.Table == "" {
		return errors.New("table cannot be empty")
	}
	// The sink pins search_path to pg_catalog,pg_temp on every pooled
	// connection, so an unqualified name cannot resolve to a user table.
	// Reporting it here returns a structured error to the calling SDK rather
	// than letting the native Write panic during expansion.
	if !strings.Contains(c.Table, ".") {
		return fmt.Errorf("table %q must be schema-qualified, for example %q", c.Table, "public."+c.Table)
	}
	if c.Username == "" {
		return errors.New("username cannot be empty")
	}
	return nil
}

type postgreSqlWriteTransform struct {
	cfg PostgreSqlWriteConfig
}

// BuildTransform translates the SchemaTransform configuration into native postgresio.Write pipeline steps.
func (t *postgreSqlWriteTransform) BuildTransform(s beam.Scope, inputs map[string]beam.PCollection) (map[string]beam.PCollection, error) {
	in, ok := inputs[schematransform.MainInputTag]
	if !ok {
		in, ok = inputs["input"]
		if !ok {
			if len(inputs) == 1 {
				for _, col := range inputs {
					in = col
					ok = true
					break
				}
			}
		}
	}
	if !ok {
		return nil, fmt.Errorf("postgres_write requires an input PCollection tagged %q", schematransform.MainInputTag)
	}

	opts := WriteOptions{
		Host:                  t.cfg.Host,
		Port:                  int(t.cfg.Port),
		Database:              t.cfg.Database,
		Username:              t.cfg.Username,
		Password:              t.cfg.Password,
		SSLMode:               t.cfg.SSLMode,
		SSLRootCert:           t.cfg.SSLRootCert,
		WriteMethod:           WriteMethodStagedCopy,
		PrimaryKeyCols:        t.cfg.ConflictKeys,
		BatchSize:             int(t.cfg.MaxBatchRows),
		MaxBatchBytes:         int(t.cfg.MaxBatchBytes),
		UsePgBouncer:          t.cfg.UsePgBouncer,
		ReplicationOriginName: t.cfg.ReplicationOrigin,
		PasswordEnvVar:        t.cfg.PasswordEnvVar,
		OpColumn:              t.cfg.OpColumn,
		DeleteOpValue:         t.cfg.DeleteOpValue,
		ExplainAnalyze:        t.cfg.ExplainAnalyze,
	}
	if opts.Port <= 0 {
		opts.Port = 5432
	}
	switch strings.ToUpper(t.cfg.WriteMode) {
	case "INSERT":
		opts.WriteMode = WriteModeInsert
	case "UPDATE":
		opts.WriteMode = WriteModeUpdate
	case "MERGE":
		opts.WriteMode = WriteModeMerge
	default:
		if len(opts.PrimaryKeyCols) > 0 {
			opts.WriteMode = WriteModeUpsert
		} else {
			opts.WriteMode = WriteModeInsert
		}
	}

	res := Write(s, t.cfg.Table, opts, in)
	errorCol := beam.ParDo(s, formatFailedRowFn, res.FailedRows)
	return map[string]beam.PCollection{
		schematransform.MainOutputTag:  res.SuccessfulRows,
		schematransform.ErrorOutputTag: errorCol,
	}, nil
}

type postgreSqlWriteProvider struct{}

func (p *postgreSqlWriteProvider) Identifier() string {
	return WriteSchemaTransformURN
}

func (p *postgreSqlWriteProvider) Description() string {
	return "High-performance PostgreSQL Write transform using parameterized UNNEST upserts, buffer compaction, and DLQ routing."
}

func (p *postgreSqlWriteProvider) InputCollectionNames() []string {
	return []string{schematransform.MainInputTag}
}

func (p *postgreSqlWriteProvider) OutputCollectionNames() []string {
	return []string{schematransform.MainOutputTag, schematransform.ErrorOutputTag}
}

func (p *postgreSqlWriteProvider) CreateTransform(cfg PostgreSqlWriteConfig) (schematransform.SchemaTransform, error) {
	return &postgreSqlWriteTransform{cfg: cfg}, nil
}

// PostgreSqlReadConfig defines the configuration parameters for the PostgreSQL Read SchemaTransform.
type PostgreSqlReadConfig struct {
	// Canonical Java / Beam YAML compatibility aliases
	Location  string `beam:"location" doc:"Target PostgreSQL table name (canonical alias for table)."`
	ReadQuery string `beam:"read_query" doc:"Custom SQL query (canonical alias for query)."`
	JdbcUrl   string `beam:"jdbc_url" doc:"PostgreSQL JDBC connection URL (e.g. jdbc:postgresql://host:5432/db)."`

	// Native Go properties
	Host            string `beam:"host" doc:"PostgreSQL database server hostname or IP address."`
	Port            int32  `beam:"port" doc:"PostgreSQL database server port (default: 5432)."`
	Database        string `beam:"database" doc:"Target PostgreSQL database name."`
	Table           string `beam:"table" doc:"Target PostgreSQL table, schema-qualified (for example public.orders)."`
	Query           string `beam:"query" doc:"Custom SQL query to execute instead of reading a full table."`
	Username        string `beam:"username" doc:"Authentication username."`
	Password        string `beam:"password,secret" doc:"Authentication password."`
	PasswordEnvVar  string `beam:"password_env_var" doc:"Environment variable name on the worker containing the authentication password."`
	SSLMode         string `beam:"sslmode" doc:"SSL mode (e.g. disable, require, verify-ca, verify-full)."`
	SSLRootCert     string `beam:"sslrootcert" doc:"Path to SSL root certificate file (PEM format) for verify-ca/verify-full."`
	FetchSize       int32  `beam:"fetch_size" doc:"Server-side cursor fetch chunk size (default: 5000)."`
	PartitionColumn string `beam:"partition_column" doc:"Numeric column name used for parallel partitioned reads."`
	NumPartitions   int32  `beam:"num_partitions" doc:"Number of parallel partitions to split the table read into."`
	LowerBound      *int64 `beam:"lower_bound" doc:"Lower bound value (inclusive) for the partition column."`
	UpperBound      *int64 `beam:"upper_bound" doc:"Upper bound value (inclusive) for the partition column."`
}

// parseJdbcURL parses a PostgreSQL JDBC URL into host, port, database, and sslmode parameters.
func parseJdbcURL(raw string) (host string, port int32, db string, sslMode string, user string, password string, err error) {
	if !strings.HasPrefix(raw, "jdbc:postgresql://") && !strings.HasPrefix(raw, "postgresql://") && !strings.HasPrefix(raw, "postgres://") {
		return "", 0, "", "", "", "", fmt.Errorf("invalid PostgreSQL JDBC URL %q: must start with jdbc:postgresql://", raw)
	}
	cleanURL := strings.TrimPrefix(raw, "jdbc:")
	u, err := url.Parse(cleanURL)
	if err != nil {
		return "", 0, "", "", "", "", fmt.Errorf("failed to parse JDBC URL: %w", err)
	}

	host = u.Hostname()
	if pStr := u.Port(); pStr != "" {
		p, err := strconv.Atoi(pStr)
		if err == nil && p > 0 {
			port = int32(p)
		}
	}
	if port == 0 {
		port = 5432
	}

	db = strings.TrimPrefix(u.Path, "/")
	if u.User != nil {
		user = u.User.Username()
		if pwd, ok := u.User.Password(); ok {
			password = pwd
		}
	}

	q := u.Query()
	if sm := q.Get("sslmode"); sm != "" {
		sslMode = sm
	}
	if uVal := q.Get("user"); uVal != "" && user == "" {
		user = uVal
	}
	if pVal := q.Get("password"); pVal != "" && password == "" {
		password = pVal
	}

	return host, port, db, sslMode, user, password, nil
}

func (c PostgreSqlReadConfig) normalize() (PostgreSqlReadConfig, error) {
	if c.JdbcUrl != "" {
		host, port, db, ssl, user, pwd, err := parseJdbcURL(c.JdbcUrl)
		if err != nil {
			return c, err
		}
		if c.Host == "" {
			c.Host = host
		}
		if c.Port <= 0 {
			c.Port = port
		}
		if c.Database == "" {
			c.Database = db
		}
		if c.SSLMode == "" && ssl != "" {
			c.SSLMode = ssl
		}
		if c.Username == "" && user != "" {
			c.Username = user
		}
		if c.Password == "" && pwd != "" {
			c.Password = pwd
		}
	}

	if c.Table == "" && c.Location != "" {
		c.Table = c.Location
	}
	if c.Query == "" && c.ReadQuery != "" {
		c.Query = c.ReadQuery
	}

	if c.Port <= 0 {
		c.Port = 5432
	}
	if c.FetchSize <= 0 {
		c.FetchSize = 5000
	}
	if c.SSLMode == "" {
		c.SSLMode = DefaultSSLMode
	}
	return c, nil
}

// Validate checks configuration invariants before pipeline graph expansion.
func (c PostgreSqlReadConfig) Validate() error {
	norm, err := c.normalize()
	if err != nil {
		return err
	}
	if norm.Table == "" && norm.Query == "" {
		return errors.New("either table (or location) or query (or read_query) must be specified")
	}
	if norm.Table != "" && norm.Query != "" {
		return errors.New("cannot specify both table and query; choose one")
	}
	if norm.Host == "" {
		return errors.New("host cannot be empty (specify host or jdbc_url)")
	}
	if norm.Database == "" {
		return errors.New("database cannot be empty (specify database or jdbc_url)")
	}
	if norm.Username == "" {
		return errors.New("username cannot be empty")
	}
	if norm.Table != "" && !strings.Contains(norm.Table, ".") {
		return fmt.Errorf("table %q must be schema-qualified, for example %q", norm.Table, "public."+norm.Table)
	}
	if norm.NumPartitions > 0 {
		if norm.Query != "" {
			return errors.New("partitioning is supported only for table reads, not arbitrary queries")
		}
		if strings.TrimSpace(norm.PartitionColumn) == "" {
			return errors.New("partition_column cannot be empty when num_partitions > 0")
		}
		if norm.LowerBound != nil && norm.UpperBound != nil && *norm.LowerBound >= *norm.UpperBound {
			return fmt.Errorf("lower_bound (%d) must be less than upper_bound (%d)", *norm.LowerBound, *norm.UpperBound)
		}
	}
	return nil
}

type postgreSqlReadTransform struct {
	cfg PostgreSqlReadConfig
}

// BuildTransform translates the SchemaTransform configuration into native postgresio.ReadRows / QueryRows pipeline steps.
func (t *postgreSqlReadTransform) BuildTransform(s beam.Scope, _ map[string]beam.PCollection) (map[string]beam.PCollection, error) {
	opts := []ReadOption{
		WithReadHost(t.cfg.Host),
		WithReadPort(int(t.cfg.Port)),
		WithReadDatabase(t.cfg.Database),
		WithReadUsername(t.cfg.Username),
		WithReadPassword(t.cfg.Password),
		WithReadPasswordEnvVar(t.cfg.PasswordEnvVar),
		WithReadSSLMode(t.cfg.SSLMode),
		WithReadSSLRootCert(t.cfg.SSLRootCert),
		WithReadFetchSize(int(t.cfg.FetchSize)),
	}
	if t.cfg.NumPartitions > 0 && t.cfg.PartitionColumn != "" && t.cfg.LowerBound != nil && t.cfg.UpperBound != nil {
		opts = append(opts, WithReadPartitions(t.cfg.PartitionColumn, *t.cfg.LowerBound, *t.cfg.UpperBound, int(t.cfg.NumPartitions)))
	}
	readOpts := NewReadOptions(opts...)

	var col beam.PCollection
	if t.cfg.Table != "" {
		col = ReadRows(s, t.cfg.Table, readOpts)
	} else {
		col = QueryRows(s, t.cfg.Query, readOpts)
	}

	return map[string]beam.PCollection{
		schematransform.MainOutputTag: col,
	}, nil
}

type postgreSqlReadProvider struct{}

func (p *postgreSqlReadProvider) Identifier() string {
	return ReadSchemaTransformURN
}

func (p *postgreSqlReadProvider) Description() string {
	return "High-performance bounded PostgreSQL Read transform with server-side cursors, divide-first parallel range partitioning, and multi-language schema compatibility."
}

func (p *postgreSqlReadProvider) InputCollectionNames() []string {
	return nil
}

func (p *postgreSqlReadProvider) OutputCollectionNames() []string {
	return []string{schematransform.MainOutputTag}
}

func (p *postgreSqlReadProvider) CreateTransform(cfg PostgreSqlReadConfig) (schematransform.SchemaTransform, error) {
	norm, err := cfg.normalize()
	if err != nil {
		return nil, err
	}
	return &postgreSqlReadTransform{cfg: norm}, nil
}

// formatFieldType returns a clean human-readable representation of a SchemaTransform field type.
func formatFieldType(ft *pipepb.FieldType) string {
	if ft == nil {
		return "unknown"
	}
	if arr := ft.GetArrayType(); arr != nil {
		return "list[" + formatFieldType(arr.GetElementType()) + "]"
	}
	if at := ft.GetAtomicType(); at != pipepb.AtomicType_UNSPECIFIED {
		switch at {
		case pipepb.AtomicType_STRING:
			return "string"
		case pipepb.AtomicType_INT32:
			return "integer (int32)"
		case pipepb.AtomicType_INT64:
			return "integer (int64)"
		case pipepb.AtomicType_BOOLEAN:
			return "boolean"
		case pipepb.AtomicType_FLOAT:
			return "float"
		case pipepb.AtomicType_DOUBLE:
			return "double"
		default:
			return strings.ToLower(at.String())
		}
	}
	return "object"
}

const yamlReferenceLicenseHeader = `<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

`

// GenerateYAMLReference generates GitHub-flavored Markdown reference documentation
// for all registered PostgreSQL SchemaTransforms.
func GenerateYAMLReference() string {
	var b strings.Builder
	b.WriteString(yamlReferenceLicenseHeader)
	b.WriteString("# PostgreSQL YAML Schema Reference\n\n")
	b.WriteString("This reference is programmatically generated from the registered Apache Beam Go SchemaTransform providers.\n")
	b.WriteString("Do not edit this document manually; update the struct tags in the `schematransform*.go` sources and regenerate.\n\n")

	reg := schematransform.DefaultRegistry()
	entries := []struct {
		name string
		urn  string
		kind string
	}{
		{name: "WriteToPostgres", urn: WriteSchemaTransformURN, kind: "Sink"},
		{name: "ReadFromPostgres", urn: ReadSchemaTransformURN, kind: "Source"},
		{name: "ReadFromPostgresCDC", urn: ReadCDCSchemaTransformURN, kind: "Source"},
	}

	for i, entry := range entries {
		p, schema, ok := reg.Get(entry.urn)
		if !ok || schema == nil {
			continue
		}
		if i > 0 {
			b.WriteString("\n")
		}
		fmt.Fprintf(&b, "## %s\n\n", entry.name)
		fmt.Fprintf(&b, "* **URN:** `%s`\n", entry.urn)
		fmt.Fprintf(&b, "* **Description:** %s\n", p.Description())
		fmt.Fprintf(&b, "* **Type:** %s\n\n", entry.kind)
		b.WriteString("### Configuration Parameters\n\n")
		b.WriteString("| Field | Type | Secret | Description |\n")
		b.WriteString("| :--- | :--- | :--- | :--- |\n")

		for _, f := range schema.GetFields() {
			isSecret := "No"
			for _, opt := range f.GetOptions() {
				if opt.GetName() == "beam:schema:option:secret:v1" {
					isSecret = "Yes"
					break
				}
			}
			fmt.Fprintf(&b, "| `%s` | %s | %s | %s |\n",
				f.GetName(),
				formatFieldType(f.GetType()),
				isSecret,
				f.GetDescription(),
			)
		}
	}

	return b.String()
}
