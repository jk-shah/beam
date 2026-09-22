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
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/schematransform"
)

// CDCRecord represents a portable Change Data Capture record conforming to Beam Row Schema.
type CDCRecord struct {
	EventID       string `beam:"event_id"`
	Operation     string `beam:"operation"`
	Schema        string `beam:"schema"`
	Table         string `beam:"table"`
	LSN           string `beam:"lsn"`
	CommitTime    string `beam:"commit_time"`
	BeforeJSON    string `beam:"before_json"`
	AfterJSON     string `beam:"after_json"`
	TransactionID string `beam:"transaction_id"`
}

func changeEventToCDCRecordFn(ev ChangeEvent) CDCRecord {
	var beforeStr, afterStr string
	if ev.Before != nil {
		b, _ := json.Marshal(ev.Before)
		beforeStr = string(b)
	}
	if ev.After != nil {
		b, _ := json.Marshal(ev.After)
		afterStr = string(b)
	}
	return CDCRecord{
		EventID:       ev.EventID,
		Operation:     string(ev.Operation),
		Schema:        ev.Schema,
		Table:         ev.Table,
		LSN:           fmt.Sprintf("%X", ev.LSN),
		CommitTime:    ev.CommitTime.Format(time.RFC3339Nano),
		BeforeJSON:    beforeStr,
		AfterJSON:     afterStr,
		TransactionID: fmt.Sprintf("%d", ev.TransactionID),
	}
}

// PostgreSqlReadCDCConfig defines the configuration parameters for PostgreSQL ReadCDC SchemaTransform.
type PostgreSqlReadCDCConfig struct {
	Host              string                   `beam:"host" doc:"PostgreSQL database server hostname or IP address."`
	Port              int32                    `beam:"port" doc:"PostgreSQL database server port (default: 5432)."`
	Database          string                   `beam:"database" doc:"Source PostgreSQL database name."`
	SlotName          string                   `beam:"slot_name" doc:"Logical replication slot name."`
	Publication       string                   `beam:"publication" doc:"PostgreSQL publication name to capture."`
	Username          string                   `beam:"username" doc:"Replication user name."`
	Password          string                   `beam:"password,secret" doc:"Replication user password."`
	PasswordEnvVar    string                   `beam:"password_env_var" doc:"Environment variable name on the worker containing the replication password."`
	SSLMode           string                   `beam:"sslmode" doc:"SSL mode (e.g. disable, require, verify-ca, verify-full)."`
	Tables            []string                 `beam:"tables" doc:"Optional list of tables to capture (empty captures all in publication)."`
	OriginFilter      string                   `beam:"origin_filter" doc:"Replication origin filter: 'all' (default) or 'none'."`
	OutputFormat      string                   `beam:"output_format" doc:"Output format: 'row' (default) or 'arrow'."`
	ArrowBatchRows    int32                    `beam:"arrow_batch_rows" doc:"Maximum rows per Arrow batch when output_format='arrow' (default: 4096)."`
	ProtoVersion      int32                    `beam:"proto_version" doc:"pgoutput protocol version (0 for auto-negotiation: 4 on PG >= 19, 1 on older)."`
	BinaryMode        *bool                    `beam:"binary_mode" doc:"Whether column values are streamed in binary format (auto: true on PG >= 19)."`
	StreamingMode     string                   `beam:"streaming_mode" doc:"In-progress transaction streaming mode (auto: 'parallel' on PG >= 19)."`
	FailoverSlot      bool                     `beam:"failover_slot" doc:"Whether to create replication slot with FAILOVER option (PostgreSQL 17+)."`
	PublicationTables []PublicationTableConfig `beam:"publication_tables" doc:"Optional list of table-specific column lists and row filters for publication (PostgreSQL 15+)."`
}

// Validate checks configuration invariants before pipeline graph expansion.
func (c PostgreSqlReadCDCConfig) Validate() error {
	if c.Host == "" {
		return errors.New("host cannot be empty")
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
	// Validate cannot apply this default: it has a value receiver, so any
	// assignment to c.Port is discarded. WithCDCPort assigns unconditionally,
	// so an unset port would otherwise clobber the NewCDCOptions default.
	port := int(t.cfg.Port)
	if port <= 0 {
		port = 5432
	}
	opts := []CDCOption{
		WithCDCHost(t.cfg.Host),
		WithCDCPort(port),
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
	if t.cfg.PasswordEnvVar != "" {
		opts = append(opts, WithCDCPasswordEnvVar(t.cfg.PasswordEnvVar))
	}
	if t.cfg.FailoverSlot {
		opts = append(opts, WithCDCFailoverSlot(true))
	}
	if len(t.cfg.PublicationTables) > 0 {
		opts = append(opts, WithCDCPublicationTables(t.cfg.PublicationTables...))
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

	recordCol := beam.ParDo(s, changeEventToCDCRecordFn, cdcCol)
	return map[string]beam.PCollection{
		schematransform.MainOutputTag: recordCol,
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

func init() {
	beam.RegisterFunction(changeEventToCDCRecordFn)
	schema.RegisterType(reflect.TypeOf((*CDCRecord)(nil)).Elem())
	schematransform.GlobalRegisterTyped[PostgreSqlReadCDCConfig](&postgreSqlReadCDCProvider{})
}
