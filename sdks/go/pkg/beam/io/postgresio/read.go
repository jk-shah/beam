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
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/lib/pq"
)

var (
	readRecordsTotal     = beam.NewCounter("postgresio", "postgres_read_records_total")
	readBytesTotal       = beam.NewCounter("postgresio", "postgres_read_bytes_total")
	readQueryErrorsTotal = beam.NewCounter("postgresio", "postgres_read_query_errors_total")
	readLatencyMs        = beam.NewDistribution("postgresio", "postgres_read_latency_ms")
)

func init() {
	beam.RegisterDoFn(&singleReadFn{})
	beam.RegisterDoFn(&partitionedReadFn{})
	beam.RegisterType(reflect.TypeOf((*PartitionRange)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*PostgresRow)(nil)).Elem())
}

// PostgresRow represents a dynamic schema row mapping column names to stringified values.
type PostgresRow struct {
	Columns []string          `beam:"columns" json:"columns"`
	Values  map[string]string `beam:"values" json:"values"`
}

// PartitionRange describes a closed-open integer range for parallel distributed reading.
type PartitionRange struct {
	Index  int   `json:"index"`
	Start  int64 `json:"start"`
	End    int64 `json:"end"`
	IsLast bool  `json:"is_last"`
}

// planPartitions calculates disjoint, closed-open partition ranges with an overflow-safe stride.
func planPartitions(lower, upper int64, numPartitions int) ([]PartitionRange, error) {
	if numPartitions <= 0 {
		return nil, fmt.Errorf("postgresio: num_partitions must be > 0, got %d", numPartitions)
	}
	if lower >= upper {
		return nil, fmt.Errorf("postgresio: lower_bound (%d) must be < upper_bound (%d)", lower, upper)
	}

	// Divide-first arithmetic avoids integer overflow across signed int64 bounds (parity with Java JdbcUtil).
	stride := (upper/int64(numPartitions) - lower/int64(numPartitions)) + 1
	if stride <= 0 {
		stride = 1
	}

	ranges := make([]PartitionRange, 0, numPartitions)
	curr := lower

	for i := 0; i < numPartitions-1 && curr < upper; i++ {
		next := curr + stride
		if next > upper {
			next = upper
		}
		ranges = append(ranges, PartitionRange{
			Index:  i,
			Start:  curr,
			End:    next,
			IsLast: false,
		})
		curr = next
	}

	if curr <= upper {
		ranges = append(ranges, PartitionRange{
			Index:  len(ranges),
			Start:  curr,
			End:    upper,
			IsLast: true,
		})
	}
	return ranges, nil
}

// buildPartitionQuery constructs a sanitized parameter-bound SQL query for a partition slice.
func buildPartitionQuery(table, partitionCol string, start, end int64, isLast bool) (string, []any, error) {
	sanitizedTable, err := SanitizeTableIdentifier(table)
	if err != nil {
		return "", nil, err
	}
	sanitizedCol, err := SanitizeIdentifier(partitionCol)
	if err != nil {
		return "", nil, err
	}

	if isLast {
		query := fmt.Sprintf("SELECT * FROM %s WHERE %s >= $1 AND %s <= $2", sanitizedTable, sanitizedCol, sanitizedCol)
		return query, []any{start, end}, nil
	}
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s >= $1 AND %s < $2", sanitizedTable, sanitizedCol, sanitizedCol)
	return query, []any{start, end}, nil
}

// Read reads all rows from the specified schema-qualified table into PCollection<t>.
func Read(s beam.Scope, table string, t reflect.Type, opts ReadOptions) beam.PCollection {
	s = s.Scope("postgresio.Read")
	if err := opts.Validate(); err != nil {
		panic(fmt.Sprintf("postgresio.Read: invalid options: %v", err))
	}
	if strings.TrimSpace(table) == "" {
		panic("postgresio.Read: table cannot be empty")
	}
	if !strings.Contains(table, ".") {
		panic(fmt.Sprintf("postgresio.Read: table name %q must be schema-qualified, for example %q. "+
			"Every connection in the read pool pins search_path to pg_catalog,pg_temp to close "+
			"CVE-2018-1058, so an unqualified name does not resolve to a user table.",
			table, "public."+table))
	}
	if t == nil {
		panic("postgresio.Read: record type t cannot be nil")
	}

	// Recorded while the closure is still observable; see DialFunc.
	opts.RequiresDialFunc = opts.DialFunc != nil

	if opts.NumPartitions > 1 && opts.PartitionColumn != "" {
		ranges, err := planPartitions(opts.LowerBound, opts.UpperBound, opts.NumPartitions)
		if err != nil {
			panic(fmt.Sprintf("postgresio.Read: partition planning failed: %v", err))
		}
		pCol := beam.CreateList(s, ranges)
		// Crucial: beam.Reshuffle breaks runner fusion, distributing partitions across worker nodes.
		shuffled := beam.Reshuffle(s, pCol)
		return beam.ParDo(s, &partitionedReadFn{
			Table:   table,
			Options: opts,
			Type:    beam.EncodedType{T: t},
		}, shuffled, beam.TypeDefinition{Var: beam.XType, T: t})
	}

	imp := beam.Impulse(s)
	return beam.ParDo(s, &singleReadFn{
		Table:   table,
		Options: opts,
		Type:    beam.EncodedType{T: t},
	}, imp, beam.TypeDefinition{Var: beam.XType, T: t})
}

// Query executes a query and projects results into PCollection<t>.
// Partitioning is disallowed on Query to prevent subquery re-execution amplification.
func Query(s beam.Scope, query string, t reflect.Type, opts ReadOptions) beam.PCollection {
	s = s.Scope("postgresio.Query")
	if err := opts.Validate(); err != nil {
		panic(fmt.Sprintf("postgresio.Query: invalid options: %v", err))
	}
	if strings.TrimSpace(query) == "" {
		panic("postgresio.Query: query cannot be empty")
	}
	if opts.NumPartitions > 1 {
		panic("postgresio.Query: partitioning is supported only for table reads, not arbitrary queries")
	}
	if t == nil {
		panic("postgresio.Query: record type t cannot be nil")
	}

	// Recorded while the closure is still observable; see DialFunc.
	opts.RequiresDialFunc = opts.DialFunc != nil

	imp := beam.Impulse(s)
	return beam.ParDo(s, &singleReadFn{
		Query:   query,
		Options: opts,
		Type:    beam.EncodedType{T: t},
	}, imp, beam.TypeDefinition{Var: beam.XType, T: t})
}

// ReadRows reads all rows from the specified schema-qualified table into PCollection<PostgresRow>.
func ReadRows(s beam.Scope, table string, opts ReadOptions) beam.PCollection {
	s = s.Scope("postgresio.ReadRows")
	return Read(s, table, reflect.TypeOf((*PostgresRow)(nil)).Elem(), opts)
}

// QueryRows executes a custom query and projects rows into PCollection<PostgresRow>.
func QueryRows(s beam.Scope, query string, opts ReadOptions) beam.PCollection {
	s = s.Scope("postgresio.QueryRows")
	return Query(s, query, reflect.TypeOf((*PostgresRow)(nil)).Elem(), opts)
}

// singleReadFn executes a non-partitioned bounded query or full table read.
type singleReadFn struct {
	Table   string           `json:"table,omitempty"`
	Query   string           `json:"query,omitempty"`
	Options ReadOptions      `json:"options"`
	Type    beam.EncodedType `json:"type"`

	workerID string
	db       *sql.DB
}

func (fn *singleReadFn) Setup(ctx context.Context) error {
	var rawID [8]byte
	_, _ = rand.Read(rawID[:])
	fn.workerID = hex.EncodeToString(rawID[:])

	// A configured dialer that arrives nil was dropped crossing the
	// serialization boundary.
	if fn.Options.RequiresDialFunc && fn.Options.DialFunc == nil {
		return errDialFuncLost()
	}

	dsn := buildWriteDSN(fn.Options.Host, fn.Options.Port, fn.Options.Database,
		fn.Options.Username, fn.Options.ResolvePassword(), fn.Options.SSLMode)

	var db *sql.DB
	var err error
	if fn.Options.DialFunc != nil {
		db = sql.OpenDB(&pqConnector{dialer: &pqDialerAdapter{dialFunc: fn.Options.DialFunc}, dsn: dsn})
	} else {
		db, err = sql.Open("postgres", dsn)
	}
	if err != nil {
		return fmt.Errorf("postgresio: failed to open database: %w", err)
	}

	maxConns := fn.Options.MaxConnections
	if maxConns <= 0 {
		maxConns = 2
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	fn.db = db
	return nil
}

func (fn *singleReadFn) Teardown() error {
	if fn.db != nil {
		return fn.db.Close()
	}
	return nil
}

func (fn *singleReadFn) ProcessElement(ctx context.Context, _ []byte, emit func(beam.X)) error {
	var query string
	if fn.Query != "" {
		query = fn.Query
	} else {
		sanitizedTable, err := SanitizeTableIdentifier(fn.Table)
		if err != nil {
			return err
		}
		query = fmt.Sprintf("SELECT * FROM %s", sanitizedTable)
	}

	return executeCursorRead(ctx, fn.db, fn.workerID, query, nil, fn.Options.FetchSize, fn.Options.QueryTimeout, fn.Type.T, emit)
}

// partitionedReadFn executes one partition range of a partitioned table read.
type partitionedReadFn struct {
	Table   string           `json:"table"`
	Options ReadOptions      `json:"options"`
	Type    beam.EncodedType `json:"type"`

	workerID string
	db       *sql.DB
}

func (fn *partitionedReadFn) Setup(ctx context.Context) error {
	var rawID [8]byte
	_, _ = rand.Read(rawID[:])
	fn.workerID = hex.EncodeToString(rawID[:])

	// A configured dialer that arrives nil was dropped crossing the
	// serialization boundary.
	if fn.Options.RequiresDialFunc && fn.Options.DialFunc == nil {
		return errDialFuncLost()
	}

	dsn := buildWriteDSN(fn.Options.Host, fn.Options.Port, fn.Options.Database,
		fn.Options.Username, fn.Options.ResolvePassword(), fn.Options.SSLMode)

	var db *sql.DB
	var err error
	if fn.Options.DialFunc != nil {
		db = sql.OpenDB(&pqConnector{dialer: &pqDialerAdapter{dialFunc: fn.Options.DialFunc}, dsn: dsn})
	} else {
		db, err = sql.Open("postgres", dsn)
	}
	if err != nil {
		return fmt.Errorf("postgresio: failed to open database: %w", err)
	}

	maxConns := fn.Options.MaxConnections
	if maxConns <= 0 {
		maxConns = 2
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	fn.db = db
	return nil
}

func (fn *partitionedReadFn) Teardown() error {
	if fn.db != nil {
		return fn.db.Close()
	}
	return nil
}

func (fn *partitionedReadFn) ProcessElement(ctx context.Context, p PartitionRange, emit func(beam.X)) error {
	query, args, err := buildPartitionQuery(fn.Table, fn.Options.PartitionColumn, p.Start, p.End, p.IsLast)
	if err != nil {
		return err
	}
	return executeCursorRead(ctx, fn.db, fn.workerID, query, args, fn.Options.FetchSize, fn.Options.QueryTimeout, fn.Type.T, emit)
}

// executeCursorRead coordinates a transaction-pinned server-side cursor to stream rows safely.
func executeCursorRead(ctx context.Context, db *sql.DB, workerID, query string, args []any, fetchSize int, queryTimeout time.Duration, recordType reflect.Type, emit func(beam.X)) error {
	if fetchSize <= 0 {
		fetchSize = 5000
	}
	if queryTimeout <= 0 {
		queryTimeout = 30 * time.Minute
	}

	startTime := time.Now()

	// 1. Begin ReadOnly transaction to pin physical connection for the cursor's entire lifetime.
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		readQueryErrorsTotal.Inc(ctx, 1)
		return fmt.Errorf("postgresio: failed to start read transaction: %w", err)
	}
	defer tx.Rollback()

	// 2. Set statement timeout to prevent runaway queries and database xmin horizon pinning.
	timeoutSQL := fmt.Sprintf("SET LOCAL statement_timeout = '%dms'", queryTimeout.Milliseconds())
	if _, err := tx.ExecContext(ctx, timeoutSQL); err != nil {
		readQueryErrorsTotal.Inc(ctx, 1)
		return fmt.Errorf("postgresio: failed to set statement timeout: %w", err)
	}

	// 3. Declare cursor with unique worker and timestamp identifier.
	cursorName := fmt.Sprintf("beam_cursor_%s_%d", workerID, time.Now().UnixNano())
	declareSQL := fmt.Sprintf("DECLARE %s NO SCROLL CURSOR FOR %s", cursorName, query)
	if _, err := tx.ExecContext(ctx, declareSQL, args...); err != nil {
		readQueryErrorsTotal.Inc(ctx, 1)
		return fmt.Errorf("postgresio: failed to declare server-side cursor: %w", err)
	}
	defer tx.ExecContext(context.Background(), fmt.Sprintf("CLOSE %s", cursorName))

	// 4. Cursor fetch iteration.
	fetchSQL := fmt.Sprintf("FETCH FORWARD %d FROM %s", fetchSize, cursorName)
	var mapper *structRowMapper

	for {
		select {
		case <-ctx.Done():
			readQueryErrorsTotal.Inc(ctx, 1)
			return ctx.Err()
		default:
		}

		rows, err := tx.QueryContext(ctx, fetchSQL)
		if err != nil {
			readQueryErrorsTotal.Inc(ctx, 1)
			return fmt.Errorf("postgresio: cursor fetch failed: %w", err)
		}

		if mapper == nil {
			cols, err := rows.Columns()
			if err != nil {
				rows.Close()
				readQueryErrorsTotal.Inc(ctx, 1)
				return fmt.Errorf("postgresio: failed to read columns: %w", err)
			}
			colTypes, _ := rows.ColumnTypes()
			m, err := newStructRowMapper(cols, colTypes, recordType)
			if err != nil {
				rows.Close()
				readQueryErrorsTotal.Inc(ctx, 1)
				return err
			}
			mapper = m
		}

		rowCount := 0
		for rows.Next() {
			rowCount++
			record, err := mapper.scan(rows)
			if err != nil {
				rows.Close()
				readQueryErrorsTotal.Inc(ctx, 1)
				return err
			}
			emit(record)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			readQueryErrorsTotal.Inc(ctx, 1)
			return fmt.Errorf("postgresio: rows iteration error: %w", err)
		}
		rows.Close()

		if rowCount > 0 {
			readRecordsTotal.Inc(ctx, int64(rowCount))
		}

		if rowCount < fetchSize {
			break
		}
	}

	if err := tx.Commit(); err != nil {
		readQueryErrorsTotal.Inc(ctx, 1)
		return fmt.Errorf("postgresio: failed to commit read transaction: %w", err)
	}
	readLatencyMs.Update(ctx, time.Since(startTime).Milliseconds())
	return nil
}

// structRowMapper manages pre-compiled field mapping and pointer buffers for row scanning.
type structRowMapper struct {
	recordType    reflect.Type
	isPostgresRow bool
	columns       []string
	fieldIndices  []int
	scanBuffers   []any
}

func newStructRowMapper(columns []string, colTypes []*sql.ColumnType, t reflect.Type) (*structRowMapper, error) {
	if t == nil {
		return nil, fmt.Errorf("postgresio: recordType cannot be nil")
	}

	if t == reflect.TypeOf((*PostgresRow)(nil)).Elem() {
		return &structRowMapper{
			recordType:    t,
			isPostgresRow: true,
			columns:       columns,
		}, nil
	}

	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("postgresio: recordType must be a struct or PostgresRow, got %v", t.Kind())
	}

	fieldMap := make(map[string]int)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" { // unexported
			continue
		}
		name := f.Name
		fieldMap[name] = i
		fieldMap[strings.ToLower(name)] = i
		fieldMap[strings.ReplaceAll(strings.ToLower(name), "_", "")] = i

		if tag := f.Tag.Get("beam"); tag != "" {
			tag = strings.Split(tag, ",")[0]
			fieldMap[tag] = i
			fieldMap[strings.ToLower(tag)] = i
		}
		if tag := f.Tag.Get("db"); tag != "" {
			tag = strings.Split(tag, ",")[0]
			fieldMap[tag] = i
			fieldMap[strings.ToLower(tag)] = i
		}
		if tag := f.Tag.Get("column"); tag != "" {
			tag = strings.Split(tag, ",")[0]
			fieldMap[tag] = i
			fieldMap[strings.ToLower(tag)] = i
		}
	}

	indices := make([]int, len(columns))
	for i, col := range columns {
		idx, ok := fieldMap[col]
		if !ok {
			idx, ok = fieldMap[strings.ToLower(col)]
		}
		if !ok {
			idx, ok = fieldMap[strings.ReplaceAll(strings.ToLower(col), "_", "")]
		}
		if !ok {
			indices[i] = -1 // Unmatched column (projected out)
		} else {
			indices[i] = idx
		}
	}

	return &structRowMapper{
		recordType:   t,
		columns:      columns,
		fieldIndices: indices,
		scanBuffers:  make([]any, len(columns)),
	}, nil
}

func (m *structRowMapper) scan(rows *sql.Rows) (any, error) {
	if m.isPostgresRow {
		rawValues := make([]any, len(m.columns))
		scanTargets := make([]any, len(m.columns))
		for i := range rawValues {
			scanTargets[i] = &rawValues[i]
		}
		if err := rows.Scan(scanTargets...); err != nil {
			return nil, fmt.Errorf("postgresio: scan failed: %w", err)
		}

		valMap := make(map[string]string, len(m.columns))
		for i, col := range m.columns {
			v := rawValues[i]
			if v == nil {
				continue
			}
			switch val := v.(type) {
			case []byte:
				valMap[col] = string(val)
			case time.Time:
				valMap[col] = val.Format(time.RFC3339Nano)
			default:
				valMap[col] = fmt.Sprintf("%v", val)
			}
		}
		return PostgresRow{
			Columns: m.columns,
			Values:  valMap,
		}, nil
	}

	valPtr := reflect.New(m.recordType)
	elem := valPtr.Elem()

	for i, fieldIdx := range m.fieldIndices {
		if fieldIdx >= 0 {
			m.scanBuffers[i] = elem.Field(fieldIdx).Addr().Interface()
		} else {
			var discard any
			m.scanBuffers[i] = &discard
		}
	}

	if err := rows.Scan(m.scanBuffers...); err != nil {
		return nil, fmt.Errorf("postgresio: scan failed: %w", err)
	}

	return elem.Interface(), nil
}
