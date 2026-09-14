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
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand"

	"reflect"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/lib/pq"
)

var (
	sinkWrittenRows     = beam.NewCounter("postgresio", "sink_written_rows")
	sinkFailedRows      = beam.NewCounter("postgresio", "sink_failed_rows")
	sinkDeadlockRetries = beam.NewCounter("postgresio", "sink_deadlock_retries")
)

func init() {
	beam.RegisterDoFn(&writeFn{})
	beam.RegisterCoder(
		reflect.TypeOf((*FailedRow)(nil)).Elem(),
		encodeFailedRow,
		decodeFailedRow,
	)
}

func encodeFailedRow(in FailedRow) ([]byte, error) {
	return json.Marshal(in)
}

func decodeFailedRow(in []byte) (FailedRow, error) {
	var out FailedRow
	err := json.Unmarshal(in, &out)
	return out, err
}

// FailedRow captures a rejected record with sanitized diagnostic error information
// for routing to a Dead-Letter Queue (DLQ).
type FailedRow struct {
	Row          any    `json:"row"`
	ErrorMessage string `json:"error_message"`
	SqlState     string `json:"sql_state"`
}

// WriteResult encapsulates the output PCollections produced by the Write transform:
// SuccessfulRows contains all committed records; FailedRows contains dead-letter records.
type WriteResult struct {
	SuccessfulRows beam.PCollection
	FailedRows     beam.PCollection
}

// Write writes elements from an input PCollection to a target PostgreSQL table using
// parameterized array upserts, in-memory deduplication, and deadlock prevention sorting.
func Write(s beam.Scope, table string, opts WriteOptions, col beam.PCollection) WriteResult {
	s = s.Scope("postgresio.Write")

	// The write path pins search_path to pg_catalog,pg_temp on every pooled
	// connection to close CVE-2018-1058, so an unqualified name cannot resolve
	// to a user table. Rejecting it here turns a runtime "relation does not
	// exist" surfacing from a distributed worker into a pipeline construction
	// error that names the fix.
	if !strings.Contains(table, ".") {
		panic(fmt.Sprintf("postgresio.Write: table name %q must be schema-qualified, for example %q. "+
			"Every connection in the write pool pins search_path to pg_catalog,pg_temp to close "+
			"CVE-2018-1058, so an unqualified name does not resolve to a user table.",
			table, "public."+table))
	}

	sanitizedTable, err := SanitizeTableIdentifier(table)
	if err != nil {
		panic(fmt.Sprintf("postgresio.Write: invalid table name %q: %v", table, err))
	}

	elemType := col.Type().Type()
	fn := &writeFn{
		Table:          sanitizedTable,
		Options:        opts,
		Type:           beam.EncodedType{T: elemType},
		PrimaryKeyCols: opts.PrimaryKeyCols,
	}

	success, failed := beam.ParDo2(s, fn, col)

	return WriteResult{
		SuccessfulRows: success,
		FailedRows:     failed,
	}
}

// buildWriteDSN renders the libpq keyword/value connection string for the sink.
//
// search_path is pinned in the DSN rather than issued as a later SET so that
// every connection the pool opens is isolated from CVE-2018-1058, including
// connections created when a pool member is recycled mid-bundle. Its value is
// a compile-time literal rather than configuration, so it is left unquoted.
//
// Every value that can originate from configuration is quoted, because an
// unquoted value ends at the first space and libpq reads the remainder as
// further keywords. Quoting keeps a credential that legitimately contains a
// space intact, and stops any configured value from introducing a keyword the
// caller never set.
//
// Keyword order is load-bearing as a second layer of defence. libpq lets a
// later duplicate keyword win, so the two settings that carry the security
// properties, sslmode and the pinned search_path, are emitted last and cannot
// be displaced by anything injected from an earlier value.
func buildWriteDSN(host string, port int, database, username, password, sslMode string) string {
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s search_path=pg_catalog,pg_temp",
		quoteDSNValue(host), port, quoteDSNValue(database),
		quoteDSNValue(username), quoteDSNValue(password), quoteDSNValue(sslMode))
}

type writeFn struct {
	Table          string           `json:"table"`
	Options        WriteOptions     `json:"options"`
	Type           beam.EncodedType `json:"type"`
	PrimaryKeyCols []string         `json:"primary_key_cols"`

	db        *sql.DB
	compactor *BatchCompactor
	columns   []string
}

func (fn *writeFn) Setup(ctx context.Context) error {
	sslMode := fn.Options.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	password := fn.Options.ResolvePassword()
	dsn := buildWriteDSN(fn.Options.Host, fn.Options.Port, fn.Options.Database,
		fn.Options.Username, password, sslMode)

	var db *sql.DB
	var err error
	if fn.Options.DialFunc != nil {
		db = sql.OpenDB(&pqConnector{dialer: &pqDialerAdapter{dialFunc: fn.Options.DialFunc}, dsn: dsn})
	} else {
		db, err = sql.Open("postgres", dsn)
		if err != nil {
			return fmt.Errorf("postgresio: failed to open connection pool: %w", err)
		}
	}

	// Clamp worker pool connections to prevent connection storms across distributed workers
	maxConns := fn.Options.MaxConnections
	if maxConns <= 0 {
		maxConns = 2
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxLifetime(30 * time.Minute)

	if fn.Options.ConnectionInitSQL != "" {
		if _, err := db.ExecContext(ctx, fn.Options.ConnectionInitSQL); err != nil {
			log.Warnf(ctx, "postgresio: connection init SQL failed: %v", err)
		}
	}

	fn.db = db
	fn.compactor = NewBatchCompactor(fn.Options.BatchSize, fn.Options.MaxBatchBytes, fn.Options.FlushInterval)
	fn.inspectColumns(fn.Type.T)

	return nil
}

func (fn *writeFn) inspectColumns(t reflect.Type) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() == reflect.Struct {
		fn.columns = make([]string, 0, t.NumField())
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if !field.IsExported() {
				continue
			}
			colName := field.Tag.Get("db")
			if colName == "" {
				colName = field.Tag.Get("beam")
			}
			if colName == "" {
				colName = field.Tag.Get("json")
			}
			if colName == "" {
				colName = strings.ToLower(field.Name)
			}
			fn.columns = append(fn.columns, colName)
		}
	}
}

func (fn *writeFn) StartBundle(ctx context.Context, emitSuccess func(beam.X), emitFailed func(FailedRow)) error {
	fn.compactor = NewBatchCompactor(fn.Options.BatchSize, fn.Options.MaxBatchBytes, fn.Options.FlushInterval)
	return nil
}

func (fn *writeFn) ProcessElement(ctx context.Context, elem beam.X, emitSuccess func(beam.X), emitFailed func(FailedRow)) error {
	entityKey, sortKey := ExtractPrimaryKeys(elem, fn.PrimaryKeyCols)
	fn.compactor.Add(entityKey, sortKey, elem, estimateElementSize(elem))

	if fn.compactor.ShouldFlush() {
		return fn.flushBatch(ctx, emitSuccess, emitFailed)
	}
	return nil
}

func (fn *writeFn) FinishBundle(ctx context.Context, emitSuccess func(beam.X), emitFailed func(FailedRow)) error {
	if fn.compactor.Len() > 0 {
		return fn.flushBatch(ctx, emitSuccess, emitFailed)
	}
	return nil
}

func (fn *writeFn) Teardown() {
	if fn.db != nil {
		_ = fn.db.Close()
	}
}

func (fn *writeFn) flushBatch(ctx context.Context, emitSuccess func(beam.X), emitFailed func(FailedRow)) error {
	batch := fn.compactor.CompactAndSort()
	if len(batch) == 0 {
		return nil
	}

	// Rows in one batch may carry different column sets: a CDC UPDATE omits
	// columns whose TOASTed values the server did not retransmit. A single
	// statement can only name one column list, so the batch is grouped by the
	// set of columns present and each group written separately.
	for _, part := range partitionBatchByUnchangedColumns(batch, fn.columns) {
		if err := fn.writePartition(ctx, part, emitSuccess, emitFailed); err != nil {
			return err
		}
	}
	return nil
}

func (fn *writeFn) writePartition(ctx context.Context, part writePartition, emitSuccess func(beam.X), emitFailed func(FailedRow)) error {
	batch := part.Rows
	if len(batch) == 0 {
		return nil
	}

	// High-Throughput Fast Path: Staged COPY Upsert (>100,000 rows/sec).
	//
	// Only a complete partition is eligible. The staging table is created LIKE
	// the target, so it carries every column; streaming a partial row into it
	// would materialize a zero value for the omitted columns and the merge
	// would then write that over the stored value. Partial partitions fall
	// through to the parameterized path, which can name an arbitrary column
	// subset. This is a rare case, so the hot path is unaffected.
	if fn.Options.WriteMethod == WriteMethodStagedCopy && part.IsComplete() {
		copyErr := fn.executeStagedCopy(ctx, batch)
		if copyErr == nil {
			sinkWrittenRows.Inc(ctx, int64(len(batch)))
			for _, item := range batch {
				emitSuccess(item)
			}
			return nil
		}
		log.Warnf(ctx, "postgresio: staged COPY failed (%v), falling back to parameterized UNNEST", copyErr)
	}

	query, args, err := fn.buildUnnestQueryForColumns(batch, part.Columns)
	if err != nil {
		sinkFailedRows.Inc(ctx, int64(len(batch)))
		for _, item := range batch {
			emitFailed(FailedRow{
				Row:          item,
				ErrorMessage: SanitizeErrorMessage(err),
				SqlState:     "XX000",
			})
		}
		return nil
	}

	// Retry loop for SQLState 40P01 deadlock detected with full-jitter exponential backoff
	maxRetries := 5
	backoff := 50 * time.Millisecond

	for attempt := 0; attempt <= maxRetries; attempt++ {
		_, execErr := fn.db.ExecContext(ctx, query, args...)
		if execErr == nil {
			sinkWrittenRows.Inc(ctx, int64(len(batch)))
			for _, item := range batch {
				emitSuccess(item)
			}
			return nil
		}

		sqlState := extractSqlState(execErr)
		if sqlState == "40P01" && attempt < maxRetries {
			sinkDeadlockRetries.Inc(ctx, 1)
			jitter := time.Duration(rand.Int63n(int64(backoff)))
			time.Sleep(backoff + jitter)
			backoff *= 2
			continue
		}

		// Permanent failure: route batch to Dead-Letter Queue (DLQ)
		sinkFailedRows.Inc(ctx, int64(len(batch)))
		sanitizedMsg := SanitizeErrorMessage(execErr)
		log.Errorf(ctx, "postgresio: write batch failed with sqlstate %s: %s", sqlState, sanitizedMsg)
		for _, item := range batch {
			emitFailed(FailedRow{
				Row:          item,
				ErrorMessage: sanitizedMsg,
				SqlState:     sqlState,
			})
		}
		break
	}

	return nil
}

// buildCopyStatement returns a COPY ... FROM STDIN statement.
//
// This replaces the deprecated lib/pq copy-statement helper, which quoted the
// table name itself. Because the caller has already quoted the identifier via
// SanitizeTableIdentifier, that helper produced a doubly-quoted name such as
// COPY """public"".""orders""", which PostgreSQL reads as a single table
// literally named `"public"."orders"`.
//
// table must already be a quoted identifier; columns must already be quoted.
func buildCopyStatement(table string, columns []string) (string, error) {
	if table == "" {
		return "", fmt.Errorf("postgresio: COPY target table must not be empty")
	}
	if len(columns) == 0 {
		return "", fmt.Errorf("postgresio: COPY requires at least one column")
	}
	return fmt.Sprintf("COPY %s (%s) FROM STDIN", table, strings.Join(columns, ", ")), nil
}

// stagingTableName derives a stable per-target staging table name.
//
// The name must be deterministic so the table can be reused across batches on
// the same session, and distinct per target table so that a worker writing to
// several tables does not reuse a staging table with the wrong column layout.
func stagingTableName(qualifiedTable string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(qualifiedTable))
	return fmt.Sprintf("beam_stage_%016x", h.Sum64())
}

// setupReplicationOrigin tags the transaction with a replication origin.
//
// The error is deliberately not discarded. Any failed statement inside a
// transaction puts it into an aborted state (SQLSTATE 25P02), so swallowing
// this error causes the *next* statement to fail with an unrelated message.
// It also silently disables bidirectional loop prevention, which is the only
// reason the origin is being set.
func (fn *writeFn) setupReplicationOrigin(ctx context.Context, txn *sql.Tx) error {
	if fn.Options.ReplicationOriginName == "" {
		return nil
	}

	escapedOrigin := strings.ReplaceAll(fn.Options.ReplicationOriginName, "'", "''")
	_, err := txn.ExecContext(ctx, fmt.Sprintf("SELECT pg_replication_origin_xact_setup('%s', '0/0')", escapedOrigin))
	if err != nil {
		return fmt.Errorf("postgresio: failed to set replication origin %q: %w\n"+
			"pg_replication_origin_xact_setup requires superuser or membership in pg_checkpoint, "+
			"and the origin must already exist (SELECT pg_replication_origin_create('%s')). "+
			"Continuing without the origin would disable bidirectional loop prevention",
			fn.Options.ReplicationOriginName, err, fn.Options.ReplicationOriginName)
	}
	return nil
}

// executeStagedCopy executes a two-phase bulk upsert: a PostgreSQL COPY into a
// session-scoped staging table followed by an atomic set-based ON CONFLICT merge.
func (fn *writeFn) executeStagedCopy(ctx context.Context, batch []any) error {
	if len(fn.columns) == 0 {
		return fmt.Errorf("postgresio: no columns discovered for type %v", fn.Type.T)
	}

	sanitizedCols := make([]string, len(fn.columns))
	for i, col := range fn.columns {
		san, err := SanitizeIdentifier(col)
		if err != nil {
			return err
		}
		sanitizedCols[i] = san
	}

	txn, err := fn.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer txn.Rollback()

	if err := fn.setupReplicationOrigin(ctx, txn); err != nil {
		return err
	}

	// Append-only fast path
	if fn.Options.WriteMode == WriteModeInsert && len(fn.PrimaryKeyCols) == 0 {
		copyStmt, err := buildCopyStatement(fn.Table, sanitizedCols)
		if err != nil {
			return err
		}
		stmt, err := txn.PrepareContext(ctx, copyStmt)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, item := range batch {
			rowVals := fn.extractRowValues(item)
			if _, err := stmt.ExecContext(ctx, rowVals...); err != nil {
				return err
			}
		}
		if _, err := stmt.ExecContext(ctx); err != nil {
			return err
		}
		if err := stmt.Close(); err != nil {
			return err
		}
		return txn.Commit()
	}

	// Upsert path via a session-scoped staging table.
	//
	// The staging table is created once per session under a stable name and
	// reused. Creating and dropping a temporary table on every micro-batch
	// inserts and deletes rows in pg_class, pg_attribute, pg_type and
	// pg_depend at the flush rate, which autovacuum on the catalogs cannot
	// keep up with; catalog bloat then slows query planning for every session
	// on the instance, not just this pipeline.
	//
	// ON COMMIT DELETE ROWS empties the table at each commit while keeping the
	// definition, so steady-state flushes perform no catalog DDL at all.
	tempTable := stagingTableName(fn.Table)
	createSQL := fmt.Sprintf("CREATE TEMP TABLE IF NOT EXISTS %s (LIKE %s INCLUDING DEFAULTS) ON COMMIT DELETE ROWS", tempTable, fn.Table)
	if _, err := txn.ExecContext(ctx, createSQL); err != nil {
		return err
	}
	// Defensive: a prior transaction on this connection may have left rows
	// behind if it did not reach commit.
	if _, err := txn.ExecContext(ctx, fmt.Sprintf("TRUNCATE %s", tempTable)); err != nil {
		return err
	}

	copyStmt, err := buildCopyStatement(tempTable, sanitizedCols)
	if err != nil {
		return err
	}
	stmt, err := txn.PrepareContext(ctx, copyStmt)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, item := range batch {
		rowVals := fn.extractRowValues(item)
		if _, err := stmt.ExecContext(ctx, rowVals...); err != nil {
			return err
		}
	}
	if _, err := stmt.ExecContext(ctx); err != nil {
		return err
	}
	if err := stmt.Close(); err != nil {
		return err
	}

	pkSet := make(map[string]bool)
	sanitizedPks := make([]string, len(fn.PrimaryKeyCols))
	for i, pk := range fn.PrimaryKeyCols {
		san, err := SanitizeIdentifier(pk)
		if err != nil {
			return err
		}
		sanitizedPks[i] = san
		pkSet[pk] = true
	}

	updateClauses := make([]string, 0, len(fn.columns))
	for _, col := range fn.columns {
		if !pkSet[col] {
			san, err := SanitizeIdentifier(col)
			if err != nil {
				return err
			}
			updateClauses = append(updateClauses, fmt.Sprintf("%s = EXCLUDED.%s", san, san))
		}
	}

	colList := strings.Join(sanitizedCols, ", ")
	var mergeSql string
	if fn.Options.WriteMode == WriteModeMerge {
		if len(batch) >= 1000 {
			// Pre-analyze staging table to prevent optimizer from selecting sequential scans on large targets
			_, _ = txn.ExecContext(ctx, fmt.Sprintf("ANALYZE %s", tempTable))
		}
		var err error
		mergeSql, err = buildMergeQuery(fn.Table, tempTable, fn.columns, fn.PrimaryKeyCols, fn.Options.OpColumn, fn.Options.DeleteOpValue)
		if err != nil {
			return err
		}
	} else if len(sanitizedPks) == 0 {
		mergeSql = fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT DO NOTHING",
			fn.Table, colList, colList, tempTable)
	} else if len(updateClauses) > 0 {
		mergeSql = fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) DO UPDATE SET %s",
			fn.Table, colList, colList, tempTable, strings.Join(sanitizedPks, ", "), strings.Join(updateClauses, ", "))
	} else {
		mergeSql = fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) DO NOTHING",
			fn.Table, colList, colList, tempTable, strings.Join(sanitizedPks, ", "))
	}

	if fn.Options.ExplainAnalyze && shouldSampleExplain(fn.Options.ExplainSampleRate) {
		explainSql := fmt.Sprintf("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) %s", mergeSql)
		var planJSON []byte
		if err := txn.QueryRowContext(ctx, explainSql).Scan(&planJSON); err != nil {
			return err
		}
		recordExplainTelemetry(planJSON, fn.Table)
	} else {
		if _, err := txn.ExecContext(ctx, mergeSql); err != nil {
			return err
		}
	}

	return txn.Commit()
}

func (fn *writeFn) extractRowValues(item any) []any {
	v := reflect.ValueOf(item)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	vals := make([]any, len(fn.columns))
	t := v.Type()
	for i, colName := range fn.columns {
		f := v.FieldByName(colName)
		if !f.IsValid() {
			for j := 0; j < t.NumField(); j++ {
				fld := t.Field(j)
				if strings.EqualFold(fld.Name, colName) || fld.Tag.Get("db") == colName || fld.Tag.Get("beam") == colName || fld.Tag.Get("json") == colName {
					f = v.Field(j)
					break
				}
			}
		}
		if f.IsValid() {
			val := f.Interface()
			if vec, ok := val.(Vector); ok {
				vals[i] = FormatVectorLiteral(vec)
			} else if f32s, ok := val.([]float32); ok {
				vals[i] = FormatVectorLiteral(f32s)
			} else {
				vals[i] = val
			}
		} else {
			vals[i] = nil
		}
	}
	return vals
}

func estimateElementSize(elem any) int {
	if elem == nil {
		return 64
	}
	v := reflect.ValueOf(elem)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return 64
		}
		v = v.Elem()
	}
	if v.Kind() == reflect.Struct {
		sz := int(v.Type().Size())
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			if f.Kind() == reflect.String {
				sz += f.Len()
			} else if f.Kind() == reflect.Slice {
				sz += f.Len()
			}
		}
		if sz < 64 {
			return 64
		}
		return sz
	}
	return 64
}

func (fn *writeFn) buildUnnestQuery(batch []any) (string, []any, error) {
	return fn.buildUnnestQueryForColumns(batch, fn.columns)
}

// buildUnnestQueryForColumns builds the parameterized write for a specific set
// of columns.
//
// The column set is a parameter rather than always being fn.columns because a
// CDC UPDATE may omit columns whose TOASTed values the server did not
// retransmit. Such a column must be absent from both the INSERT column list and
// the DO UPDATE SET clause: including it would write a zero value over the
// stored one, which is the very data loss the structural TOAST representation
// exists to prevent. Omitting it from the INSERT list means a row that does not
// yet exist in the target takes the column's default, which is the best
// available answer when the source did not send a value.
func (fn *writeFn) buildUnnestQueryForColumns(batch []any, columns []string) (string, []any, error) {
	if len(columns) == 0 {
		return "", nil, fmt.Errorf("no columns discovered for type %v", fn.Type.T)
	}

	sanitizedCols := make([]string, len(columns))
	for i, col := range columns {
		san, err := SanitizeIdentifier(col)
		if err != nil {
			return "", nil, err
		}
		sanitizedCols[i] = san
	}

	columnArrays := make([][]any, len(columns))
	for i := range columnArrays {
		columnArrays[i] = make([]any, len(batch))
	}

	columnTypes := make([]reflect.Type, len(columns))
	for rowIdx, item := range batch {
		v := reflect.ValueOf(item)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
		for colIdx, colName := range columns {
			f := v.FieldByName(colName)
			if !f.IsValid() {
				t := v.Type()
				for j := 0; j < t.NumField(); j++ {
					fld := t.Field(j)
					if strings.EqualFold(fld.Name, colName) || fld.Tag.Get("db") == colName || fld.Tag.Get("beam") == colName || fld.Tag.Get("json") == colName {
						f = v.Field(j)
						columnTypes[colIdx] = fld.Type
						break
					}
				}
			} else {
				columnTypes[colIdx] = f.Type()
			}
			if f.IsValid() {
				columnArrays[colIdx][rowIdx] = f.Interface()
			} else {
				columnArrays[colIdx][rowIdx] = nil
			}
		}
	}

	unnestPlaceholders := make([]string, len(columns))
	args := make([]any, len(columns))
	for i := range columns {
		cast := goTypeToPgArrayType(columnTypes[i])
		unnestPlaceholders[i] = fmt.Sprintf("$%d::%s", i+1, cast)
		args[i] = pq.Array(columnArrays[i])
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO %s (%s) SELECT * FROM UNNEST(%s)",
		fn.Table,
		strings.Join(sanitizedCols, ", "),
		strings.Join(unnestPlaceholders, ", ")))

	if fn.Options.WriteMode == WriteModeUpsert {
		if len(fn.PrimaryKeyCols) == 0 {
			sb.WriteString(" ON CONFLICT DO NOTHING")
		} else {
			sanitizedPks := make([]string, len(fn.PrimaryKeyCols))
			pkSet := make(map[string]bool)
			for i, pk := range fn.PrimaryKeyCols {
				san, err := SanitizeIdentifier(pk)
				if err != nil {
					return "", nil, err
				}
				sanitizedPks[i] = san
				pkSet[pk] = true
			}

			updateClauses := make([]string, 0, len(columns))
			for _, col := range columns {
				if !pkSet[col] {
					san, err := SanitizeIdentifier(col)
					if err != nil {
						return "", nil, err
					}
					updateClauses = append(updateClauses, fmt.Sprintf("%s = EXCLUDED.%s", san, san))
				}
			}

			if len(updateClauses) > 0 {
				sb.WriteString(fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s",
					strings.Join(sanitizedPks, ", "),
					strings.Join(updateClauses, ", ")))
			} else {
				sb.WriteString(fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING",
					strings.Join(sanitizedPks, ", ")))
			}
		}
	}

	return sb.String(), args, nil
}

func extractSqlState(err error) string {
	if err == nil {
		return ""
	}
	if pqErr, ok := err.(*pq.Error); ok {
		return string(pqErr.Code)
	}
	return "UNKNOWN"
}

func goTypeToPgArrayType(t reflect.Type) string {
	if t == nil {
		return "text[]"
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Int64:
		return "bigint[]"
	case reflect.Int, reflect.Int32:
		return "integer[]"
	case reflect.Int16:
		return "smallint[]"
	case reflect.Float64:
		return "double precision[]"
	case reflect.Float32:
		return "real[]"
	case reflect.Bool:
		return "boolean[]"
	case reflect.String:
		return "text[]"
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "bytea[]"
		}
		return "text[]"
	default:
		if t.String() == "time.Time" {
			return "timestamptz[]"
		}
		return "text[]"
	}
}

// ExplainPlanResult captures PostgreSQL JSON plan metrics from EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON).
type ExplainPlanResult struct {
	PlanningTime  float64 `json:"Planning Time"`
	ExecutionTime float64 `json:"Execution Time"`
	Plan          struct {
		NodeType         string `json:"Node Type"`
		RelationName     string `json:"Relation Name"`
		SharedHitBlocks  int64  `json:"Shared Hit Blocks"`
		SharedReadBlocks int64  `json:"Shared Read Blocks"`
		ActualRows       int64  `json:"Actual Rows"`
	} `json:"Plan"`
}

func shouldSampleExplain(rate float64) bool {
	if rate <= 0 {
		rate = 0.001 // 0.1% default (1 in 1000 batches)
	}
	return rand.Float64() < rate
}

func recordExplainTelemetry(planJSON []byte, targetTable string) {
	var results []ExplainPlanResult
	if err := json.Unmarshal(planJSON, &results); err != nil || len(results) == 0 {
		return
	}
	res := results[0]
	if res.ExecutionTime > 250.0 {
		log.Warnf(context.Background(),
			"[POSTGRES_SINK_PLAN_ALERT] table=%s execution_time=%.2fms planning_time=%.2fms node=%s hit_blocks=%d read_blocks=%d",
			targetTable, res.ExecutionTime, res.PlanningTime, res.Plan.NodeType,
			res.Plan.SharedHitBlocks, res.Plan.SharedReadBlocks)
	}
}

// buildMergeQuery generates an SQL-standard MERGE statement for PostgreSQL 15+.
func buildMergeQuery(targetTable, sourceTable string, columns, pkCols []string, opCol, deleteOpVal string) (string, error) {
	if len(pkCols) == 0 {
		return "", fmt.Errorf("postgresio: MERGE mode requires at least one primary key column")
	}

	sanitizedTarget, err := SanitizeTableIdentifier(targetTable)
	if err != nil {
		return "", err
	}
	sanitizedSource, err := SanitizeTableIdentifier(sourceTable)
	if err != nil {
		return "", err
	}

	pkSet := make(map[string]bool)
	var onConditions []string
	for _, pk := range pkCols {
		san, err := SanitizeIdentifier(pk)
		if err != nil {
			return "", err
		}
		pkSet[pk] = true
		onConditions = append(onConditions, fmt.Sprintf("target.%s = source.%s", san, san))
	}

	var updateClauses []string
	var insertCols []string
	var insertVals []string

	for _, col := range columns {
		san, err := SanitizeIdentifier(col)
		if err != nil {
			return "", err
		}
		if col != opCol {
			insertCols = append(insertCols, san)
			insertVals = append(insertVals, fmt.Sprintf("source.%s", san))
			if !pkSet[col] {
				updateClauses = append(updateClauses, fmt.Sprintf("%s = source.%s", san, san))
			}
		}
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("MERGE INTO %s AS target\n", sanitizedTarget))
	sb.WriteString(fmt.Sprintf("USING %s AS source\n", sanitizedSource))
	sb.WriteString(fmt.Sprintf("ON %s\n", strings.Join(onConditions, " AND ")))

	hasOp := opCol != ""
	var sanOp string
	if hasOp {
		var err error
		sanOp, err = SanitizeIdentifier(opCol)
		if err != nil {
			return "", err
		}
		delValEscaped := strings.ReplaceAll(deleteOpVal, "'", "''")
		sb.WriteString(fmt.Sprintf("WHEN MATCHED AND source.%s = '%s' THEN\n  DELETE\n", sanOp, delValEscaped))
	}

	if len(updateClauses) > 0 {
		sb.WriteString(fmt.Sprintf("WHEN MATCHED THEN\n  UPDATE SET\n    %s\n", strings.Join(updateClauses, ",\n    ")))
	}

	if hasOp {
		delValEscaped := strings.ReplaceAll(deleteOpVal, "'", "''")
		sb.WriteString(fmt.Sprintf("WHEN NOT MATCHED AND source.%s <> '%s' THEN\n", sanOp, delValEscaped))
	} else {
		sb.WriteString("WHEN NOT MATCHED THEN\n")
	}
	sb.WriteString(fmt.Sprintf("  INSERT (%s)\n  VALUES (%s)", strings.Join(insertCols, ", "), strings.Join(insertVals, ", ")))

	return sb.String(), nil
}

