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
	"errors"
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/schematransform"
)

const (
	// WriteSchemaTransformURN is the standard URN for PostgreSQL SchemaTransform Write.
	WriteSchemaTransformURN = "beam:schematransform:org.apache.beam:postgres_write:v1"

	// ReadCDCSchemaTransformURN is the standard URN for PostgreSQL SchemaTransform ReadCDC.
	ReadCDCSchemaTransformURN = "beam:schematransform:org.apache.beam:postgres_read_cdc:v1"
)

func init() {
	schematransform.GlobalRegisterTyped[PostgreSqlWriteConfig](&postgreSqlWriteProvider{})
	schematransform.GlobalRegisterTyped[PostgreSqlReadCDCConfig](&postgreSqlReadCDCProvider{})
}

// PostgreSqlWriteConfig defines the configuration parameters for the PostgreSQL Write SchemaTransform.
type PostgreSqlWriteConfig struct {
	Host          string   `beam:"host" doc:"PostgreSQL database server hostname or IP address."`
	Port          int32    `beam:"port" doc:"PostgreSQL database server port (default: 5432)."`
	Database      string   `beam:"database" doc:"Target PostgreSQL database name."`
	Table         string   `beam:"table" doc:"Target PostgreSQL table name."`
	Username      string   `beam:"username" doc:"Authentication username."`
	Password      string   `beam:"password,secret" doc:"Authentication password."`
	SSLMode       string   `beam:"sslmode" doc:"SSL mode (e.g. disable, require, verify-ca, verify-full)."`
	ConflictKeys  []string `beam:"conflict_keys" doc:"Columns used as primary or unique key conflict targets for UPSERT."`
	UpdateFields  []string `beam:"update_fields" doc:"Columns to update ON CONFLICT DO UPDATE. If empty, uses DO NOTHING."`
	MaxBatchRows  int32    `beam:"max_batch_rows" doc:"Maximum rows per batch UNNEST statement (default: 5000)."`
	MaxBatchBytes     int32    `beam:"max_batch_bytes" doc:"Maximum byte buffer threshold before flushing (default: 8MB)."`
	UsePgBouncer      bool     `beam:"use_pgbouncer" doc:"Enable single-statement transaction pooling for PgBouncer compatibility."`
	ReplicationOrigin string   `beam:"replication_origin" doc:"Replication origin name to tag write transactions to prevent cyclic loops."`
}

// Validate checks configuration invariants before pipeline graph expansion.
func (c PostgreSqlWriteConfig) Validate() error {
	if c.Host == "" {
		return errors.New("host cannot be empty")
	}
	if c.Port <= 0 {
		c.Port = 5432
	}
	if c.Database == "" {
		return errors.New("database cannot be empty")
	}
	if c.Table == "" {
		return errors.New("table cannot be empty")
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
		Host:           t.cfg.Host,
		Port:           int(t.cfg.Port),
		Database:       t.cfg.Database,
		Username:       t.cfg.Username,
		Password:       t.cfg.Password,
		SSLMode:        t.cfg.SSLMode,
		WriteMethod:    WriteMethodStagedCopy,
		PrimaryKeyCols: t.cfg.ConflictKeys,
		BatchSize:             int(t.cfg.MaxBatchRows),
		MaxBatchBytes:         int(t.cfg.MaxBatchBytes),
		UsePgBouncer:          t.cfg.UsePgBouncer,
		ReplicationOriginName: t.cfg.ReplicationOrigin,
	}
	if opts.Port <= 0 {
		opts.Port = 5432
	}
	if len(opts.PrimaryKeyCols) > 0 {
		opts.WriteMode = WriteModeUpsert
	} else {
		opts.WriteMode = WriteModeInsert
	}

	res := Write(s, t.cfg.Table, opts, in)
	return map[string]beam.PCollection{
		schematransform.MainOutputTag:  res.SuccessfulRows,
		schematransform.ErrorOutputTag: res.FailedRows,
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

// PostgreSqlReadCDCConfig defines the configuration parameters for PostgreSQL ReadCDC SchemaTransform.
type PostgreSqlReadCDCConfig struct {
	Host           string   `beam:"host" doc:"PostgreSQL database server hostname or IP address."`
	Port           int32    `beam:"port" doc:"PostgreSQL database server port (default: 5432)."`
	Database       string   `beam:"database" doc:"Source PostgreSQL database name."`
	SlotName       string   `beam:"slot_name" doc:"Logical replication slot name."`
	Publication    string   `beam:"publication" doc:"PostgreSQL publication name to capture."`
	Username       string   `beam:"username" doc:"Replication user name."`
	Password       string   `beam:"password,secret" doc:"Replication user password."`
	SSLMode        string   `beam:"sslmode" doc:"SSL mode (e.g. disable, require, verify-ca, verify-full)."`
	Tables         []string `beam:"tables" doc:"Optional list of tables to capture (empty captures all in publication)."`
	OriginFilter   string   `beam:"origin_filter" doc:"Replication origin filter: 'all' (default) or 'none'."`
	OutputFormat   string   `beam:"output_format" doc:"Output format: 'row' (default) or 'arrow'."`
	ArrowBatchRows int32    `beam:"arrow_batch_rows" doc:"Maximum rows per Arrow batch when output_format='arrow' (default: 4096)."`
	ProtoVersion   int32    `beam:"proto_version" doc:"pgoutput protocol version (0 for auto-negotiation: 4 on PG >= 19, 1 on older)."`
	BinaryMode     *bool    `beam:"binary_mode" doc:"Whether column values are streamed in binary format (auto: true on PG >= 19)."`
	StreamingMode  string   `beam:"streaming_mode" doc:"In-progress transaction streaming mode (auto: 'parallel' on PG >= 19)."`
}

// Validate checks configuration invariants before pipeline graph expansion.
func (c PostgreSqlReadCDCConfig) Validate() error {
	if c.Host == "" {
		return errors.New("host cannot be empty")
	}
	if c.Port <= 0 {
		c.Port = 5432
	}
	if c.Database == "" {
		return errors.New("database cannot be empty")
	}
	if c.SlotName == "" {
		return errors.New("slot_name cannot be empty")
	}
	if c.Publication == "" {
		return errors.New("publication cannot be empty")
	}
	if c.Username == "" {
		return errors.New("username cannot be empty")
	}
	if c.OriginFilter != "" && c.OriginFilter != "all" && c.OriginFilter != "none" {
		return fmt.Errorf("origin_filter must be 'all' or 'none', got %q", c.OriginFilter)
	}
	if c.OutputFormat != "" && c.OutputFormat != "row" && c.OutputFormat != "arrow" {
		return fmt.Errorf("output_format must be 'row' or 'arrow', got %q", c.OutputFormat)
	}
	return nil
}

type postgreSqlReadCDCTransform struct {
	cfg PostgreSqlReadCDCConfig
}

// BuildTransform translates the SchemaTransform configuration into native postgresio.ReadCDC pipeline steps.
func (t *postgreSqlReadCDCTransform) BuildTransform(s beam.Scope, _ map[string]beam.PCollection) (map[string]beam.PCollection, error) {
	opts := []CDCOption{
		WithCDCHost(t.cfg.Host),
		WithCDCPort(int(t.cfg.Port)),
		WithCDCDatabase(t.cfg.Database),
		WithCDCUsername(t.cfg.Username),
		WithCDCPassword(t.cfg.Password),
		WithCDCSlotName(t.cfg.SlotName),
		WithCDCPublication(t.cfg.Publication),
		WithCDCSSLMode(t.cfg.SSLMode),
		WithCDCOriginFilter(t.cfg.OriginFilter),
	}
	if t.cfg.ProtoVersion > 0 {
		opts = append(opts, WithCDCProtoVersion(int(t.cfg.ProtoVersion)))
	}
	if t.cfg.BinaryMode != nil {
		opts = append(opts, WithCDCBinaryMode(*t.cfg.BinaryMode))
	}
	if t.cfg.StreamingMode != "" {
		opts = append(opts, WithCDCStreamingMode(t.cfg.StreamingMode))
	}

	cdcCol := ReadCDC(s, opts...)

	if t.cfg.OutputFormat == "arrow" {
		batchRows := 4096
		if t.cfg.ArrowBatchRows > 0 {
			batchRows = int(t.cfg.ArrowBatchRows)
		}
		arrowCol := ToArrowBatches(s, cdcCol, WithArrowMaxBatchRows(batchRows))
		return map[string]beam.PCollection{
			schematransform.MainOutputTag: arrowCol,
		}, nil
	}

	return map[string]beam.PCollection{
		schematransform.MainOutputTag: cdcCol,
	}, nil
}

type postgreSqlReadCDCProvider struct{}

func (p *postgreSqlReadCDCProvider) Identifier() string {
	return ReadCDCSchemaTransformURN
}

func (p *postgreSqlReadCDCProvider) Description() string {
	return "Continuous PostgreSQL Change Data Capture (CDC) streaming source using logical replication and pgoutput with optional Apache Arrow micro-batching."
}

func (p *postgreSqlReadCDCProvider) InputCollectionNames() []string {
	return nil
}

func (p *postgreSqlReadCDCProvider) OutputCollectionNames() []string {
	return []string{schematransform.MainOutputTag}
}

func (p *postgreSqlReadCDCProvider) CreateTransform(cfg PostgreSqlReadCDCConfig) (schematransform.SchemaTransform, error) {
	return &postgreSqlReadCDCTransform{cfg: cfg}, nil
}
