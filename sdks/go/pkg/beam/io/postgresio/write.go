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
	// Pin search_path directly in DSN so EVERY connection in the pool is isolated (CVE-2018-1058)
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s search_path=pg_catalog,pg_temp",
		fn.Options.Host, fn.Options.Port, fn.Options.Database, fn.Options.Username, fn.Options.Password, sslMode)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("postgresio: failed to open connection pool: %w", err)
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

	// High-Throughput Fast Path: Staged COPY Upsert (>100,000 rows/sec)
	if fn.Options.WriteMethod == WriteMethodStagedCopy {
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

	query, args, err := fn.buildUnnestQuery(batch)
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

// executeStagedCopy executes two-phase bulk upsert utilizing PostgreSQL COPY
// into a temporary unlogged staging table followed by an atomic set-based ON CONFLICT merge.
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

	if fn.Options.ReplicationOriginName != "" {
		escapedOrigin := strings.ReplaceAll(fn.Options.ReplicationOriginName, "'", "''")
		_, _ = txn.ExecContext(ctx, fmt.Sprintf("SELECT pg_replication_origin_xact_setup('%s', '0/0')", escapedOrigin))
	}

	// Append-only fast path
	if fn.Options.WriteMode == WriteModeInsert && len(fn.PrimaryKeyCols) == 0 {
		stmt, err := txn.PrepareContext(ctx, pq.CopyIn(fn.Table, fn.columns...))
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

	// Upsert fast path via temp table staging
	tempTable := fmt.Sprintf("stage_%d", rand.Int63()&0x7FFFFFFFFFFFFFFF)
	createSql := fmt.Sprintf("CREATE TEMP TABLE %s (LIKE %s INCLUDING DEFAULTS) ON COMMIT DROP", tempTable, fn.Table)
	if _, err := txn.ExecContext(ctx, createSql); err != nil {
		return err
	}

	stmt, err := txn.PrepareContext(ctx, pq.CopyIn(tempTable, fn.columns...))
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
	if len(updateClauses) > 0 {
		mergeSql = fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) DO UPDATE SET %s",
			fn.Table, colList, colList, tempTable, strings.Join(sanitizedPks, ", "), strings.Join(updateClauses, ", "))
	} else {
		mergeSql = fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) DO NOTHING",
			fn.Table, colList, colList, tempTable, strings.Join(sanitizedPks, ", "))
	}

	if _, err := txn.ExecContext(ctx, mergeSql); err != nil {
		return err
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
			vals[i] = f.Interface()
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
	if len(fn.columns) == 0 {
		return "", nil, fmt.Errorf("no columns discovered for type %v", fn.Type.T)
	}

	sanitizedCols := make([]string, len(fn.columns))
	for i, col := range fn.columns {
		san, err := SanitizeIdentifier(col)
		if err != nil {
			return "", nil, err
		}
		sanitizedCols[i] = san
	}

	columnArrays := make([][]any, len(fn.columns))
	for i := range columnArrays {
		columnArrays[i] = make([]any, len(batch))
	}

	columnTypes := make([]reflect.Type, len(fn.columns))
	for rowIdx, item := range batch {
		v := reflect.ValueOf(item)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
		for colIdx, colName := range fn.columns {
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

	unnestPlaceholders := make([]string, len(fn.columns))
	args := make([]any, len(fn.columns))
	for i := range fn.columns {
		cast := goTypeToPgArrayType(columnTypes[i])
		unnestPlaceholders[i] = fmt.Sprintf("$%d::%s", i+1, cast)
		args[i] = pq.Array(columnArrays[i])
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO %s (%s) SELECT * FROM UNNEST(%s)",
		fn.Table,
		strings.Join(sanitizedCols, ", "),
		strings.Join(unnestPlaceholders, ", ")))

	if fn.Options.WriteMode == WriteModeUpsert && len(fn.PrimaryKeyCols) > 0 {
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

		updateClauses := make([]string, 0, len(fn.columns))
		for _, col := range fn.columns {
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
