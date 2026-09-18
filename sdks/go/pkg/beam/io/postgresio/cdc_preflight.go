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
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
)

// preflightTimeout bounds the whole preflight sequence.
//
// Preflight is a diagnostic, and a diagnostic must not become an availability
// dependency: the catalog reads below can block behind unrelated DDL holding a
// lock. On timeout the remaining checks report as indeterminate and the
// pipeline proceeds. Declared as a var so tests can shorten it.
var preflightTimeout = 15 * time.Second

// preflightStatus classifies one check.
//
// The four values exist so each check is independently observable. An
// absence-only signal -- "no error was returned" -- is inert when several
// different outcomes produce the same absence, so a check reports which of
// them occurred.
type preflightStatus int

const (
	// preflightPassed means the server is configured as the connector
	// requires.
	preflightPassed preflightStatus = iota

	// preflightFailed means the server is definitively misconfigured and
	// replication cannot work. This fails the pipeline, early and with a
	// message that names the setting.
	preflightFailed

	// preflightWarned means the configuration is legal but will surprise
	// somebody. Advisory only.
	preflightWarned

	// preflightIndeterminate means the check itself could not be completed --
	// the query errored, timed out, or the role could not read the catalog.
	// Advisory only: a preflight that cannot see the server must not be the
	// reason a working pipeline refuses to start.
	preflightIndeterminate
)

func (s preflightStatus) String() string {
	switch s {
	case preflightPassed:
		return "PASS"
	case preflightFailed:
		return "FAIL"
	case preflightWarned:
		return "WARN"
	case preflightIndeterminate:
		return "UNKNOWN"
	default:
		return fmt.Sprintf("preflightStatus(%d)", int(s))
	}
}

// preflightResult is one check and what it found.
type preflightResult struct {
	Check  string
	Status preflightStatus
	Detail string
}

// Check names. Stable strings: they appear in logs and in the failure message,
// and tests assert on them.
const (
	preflightCheckWALLevel        = "wal_level"
	preflightCheckSlotHeadroom    = "replication_slot_headroom"
	preflightCheckPublication     = "publication_exists"
	preflightCheckPublishedTables = "published_tables"
	preflightCheckReplicaIdentity = "replica_identity"
)

// publishedTable is one table a publication carries, with the catalog facts
// needed to judge whether UPDATE and DELETE can be decoded from it.
type publishedTable struct {
	Schema string
	Table  string

	// ReplicaIdentity is pg_class.relreplident: d (default), n (nothing),
	// f (full) or i (index).
	ReplicaIdentity string

	// HasPrimaryKey reports whether a primary key exists, which is what
	// makes relreplident = 'd' sufficient.
	HasPrimaryKey bool
}

func (t publishedTable) qualifiedName() string {
	return t.Schema + "." + t.Table
}

// hasUsableReplicaIdentity reports whether the server can construct an old
// tuple for UPDATE and DELETE on this table. Without one, PostgreSQL rejects
// those statements outright on a published table.
func (t publishedTable) hasUsableReplicaIdentity() bool {
	switch t.ReplicaIdentity {
	case "f", "i":
		return true
	case "d":
		return t.HasPrimaryKey
	default:
		// "n" is REPLICA IDENTITY NOTHING. An unrecognized value is treated
		// as unusable rather than assumed benign.
		return false
	}
}

// slotCapacity is the cluster's replication slot budget at one instant.
type slotCapacity struct {
	Used   int
	Limit  int
	Exists bool
}

// preflightQuerier reads the catalog facts preflight needs. It is an interface
// so tests can drive every branch without a database, in the same way
// slotRetentionQuerier, StreamFactory and DialFunc are used elsewhere in this
// package.
type preflightQuerier interface {
	WALLevel(ctx context.Context) (string, error)
	SlotCapacity(ctx context.Context, slotName string) (slotCapacity, error)
	PublicationExists(ctx context.Context, publication string) (bool, error)
	PublishedTables(ctx context.Context, publication string) ([]publishedTable, error)
	Close() error
}

// publishedTablesQuery resolves the publication's table list and, for each
// table, whether the server can build an old tuple for it.
//
// pg_publication_tables already expands FOR ALL TABLES and FOR TABLES IN
// SCHEMA, so this covers every publication form without special-casing. The
// join is on the schema and table name pair rather than on relname alone,
// which would match same-named tables in other schemas.
const publishedTablesQuery = `
SELECT
    t.schemaname,
    t.tablename,
    c.relreplident::text,
    EXISTS (
        SELECT 1 FROM pg_catalog.pg_index i
        WHERE i.indrelid = c.oid AND i.indisprimary
    ) AS has_primary_key
FROM pg_catalog.pg_publication_tables t
JOIN pg_catalog.pg_namespace n ON n.nspname = t.schemaname
JOIN pg_catalog.pg_class c ON c.relname = t.tablename AND c.relnamespace = n.oid
WHERE t.pubname = $1`

// slotCapacityQuery reads the cluster-wide slot count and limit, and whether
// this pipeline's slot is already among them.
//
// pg_replication_slots is cluster-wide and max_replication_slots is a
// cluster-wide limit, so the two are directly comparable.
const slotCapacityQuery = `
SELECT
    (SELECT count(*) FROM pg_catalog.pg_replication_slots)::int AS used,
    (SELECT COALESCE(setting, '0')::int FROM pg_catalog.pg_settings
      WHERE name = 'max_replication_slots') AS slot_limit,
    EXISTS (
        SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1
    ) AS slot_exists`

// sqlPreflightQuerier is the production preflightQuerier. It holds an ordinary
// SQL connection, not a replication connection: these are catalog reads, and
// the replication protocol cannot serve them.
type sqlPreflightQuerier struct {
	db *sql.DB
}

func newSQLPreflightQuerier(opts CDCOptions) (*sqlPreflightQuerier, error) {
	sslMode := opts.SSLMode
	if sslMode == "" {
		sslMode = DefaultSSLMode
	}
	// Reuses the sink's DSN builder so this connection inherits the same
	// value escaping and the same pinned search_path.
	dsn := buildWriteDSN(opts.Host, opts.Port, opts.Database, opts.Username, opts.ResolvePassword(), sslMode)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("postgresio: failed to open preflight connection: %w", err)
	}
	// Preflight is a handful of catalog reads that run once and then the
	// connection is closed, so one is enough.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return &sqlPreflightQuerier{db: db}, nil
}

func (q *sqlPreflightQuerier) WALLevel(ctx context.Context) (string, error) {
	var level string
	err := q.db.QueryRowContext(ctx,
		`SELECT setting FROM pg_catalog.pg_settings WHERE name = 'wal_level'`).Scan(&level)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(level), nil
}

func (q *sqlPreflightQuerier) SlotCapacity(ctx context.Context, slotName string) (slotCapacity, error) {
	var out slotCapacity
	err := q.db.QueryRowContext(ctx, slotCapacityQuery, slotName).
		Scan(&out.Used, &out.Limit, &out.Exists)
	if err != nil {
		return slotCapacity{}, err
	}
	return out, nil
}

func (q *sqlPreflightQuerier) PublicationExists(ctx context.Context, publication string) (bool, error) {
	var exists bool
	err := q.db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = $1)`,
		publication).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (q *sqlPreflightQuerier) PublishedTables(ctx context.Context, publication string) ([]publishedTable, error) {
	rows, err := q.db.QueryContext(ctx, publishedTablesQuery, publication)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []publishedTable
	for rows.Next() {
		var t publishedTable
		if err := rows.Scan(&t.Schema, &t.Table, &t.ReplicaIdentity, &t.HasPrimaryKey); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (q *sqlPreflightQuerier) Close() error {
	return q.db.Close()
}

// runPreflight executes the checks against an already-open querier and returns
// one result per check attempted.
//
// It does not log and does not decide the pipeline's fate; callers do both.
// Keeping the checks pure makes each branch assertable from a table test.
func runPreflight(ctx context.Context, q preflightQuerier, opts CDCOptions) []preflightResult {
	ctx, cancel := context.WithTimeout(ctx, preflightTimeout)
	defer cancel()

	results := []preflightResult{
		checkWALLevel(ctx, q),
		checkSlotHeadroom(ctx, q, opts.SlotName),
	}

	publication := checkPublication(ctx, q, opts.Publication)
	results = append(results, publication)
	if publication.Status != preflightPassed {
		// Every remaining check reads through the publication. Reporting them
		// as failures would multiply one root cause into three.
		return results
	}

	tables, err := q.PublishedTables(ctx, opts.Publication)
	if err != nil {
		return append(results, preflightResult{
			Check:  preflightCheckPublishedTables,
			Status: preflightIndeterminate,
			Detail: fmt.Sprintf("could not list the tables published by %q: %v", opts.Publication, err),
		})
	}
	return append(results,
		checkPublishedTables(opts.Publication, tables),
		checkReplicaIdentity(tables))
}

func checkWALLevel(ctx context.Context, q preflightQuerier) preflightResult {
	level, err := q.WALLevel(ctx)
	if err != nil {
		return preflightResult{
			Check:  preflightCheckWALLevel,
			Status: preflightIndeterminate,
			Detail: fmt.Sprintf("could not read wal_level: %v", err),
		}
	}
	if level != "logical" {
		return preflightResult{
			Check:  preflightCheckWALLevel,
			Status: preflightFailed,
			Detail: fmt.Sprintf("wal_level is %q, but logical decoding requires wal_level=logical; "+
				"set it in postgresql.conf and restart the server", level),
		}
	}
	return preflightResult{
		Check:  preflightCheckWALLevel,
		Status: preflightPassed,
		Detail: "wal_level=logical",
	}
}

func checkSlotHeadroom(ctx context.Context, q preflightQuerier, slotName string) preflightResult {
	capacity, err := q.SlotCapacity(ctx, slotName)
	if err != nil {
		return preflightResult{
			Check:  preflightCheckSlotHeadroom,
			Status: preflightIndeterminate,
			Detail: fmt.Sprintf("could not read replication slot capacity: %v", err),
		}
	}
	if capacity.Exists {
		// An existing slot consumes no additional headroom, so the limit is
		// irrelevant to this pipeline.
		return preflightResult{
			Check:  preflightCheckSlotHeadroom,
			Status: preflightPassed,
			Detail: fmt.Sprintf("replication slot %q already exists", slotName),
		}
	}
	if capacity.Used >= capacity.Limit {
		return preflightResult{
			Check:  preflightCheckSlotHeadroom,
			Status: preflightFailed,
			Detail: fmt.Sprintf("the server holds %d replication slots and max_replication_slots is %d, "+
				"so slot %q cannot be created; drop an unused slot or raise max_replication_slots, "+
				"which requires a server restart", capacity.Used, capacity.Limit, slotName),
		}
	}
	return preflightResult{
		Check:  preflightCheckSlotHeadroom,
		Status: preflightPassed,
		Detail: fmt.Sprintf("%d of %d replication slots in use", capacity.Used, capacity.Limit),
	}
}

func checkPublication(ctx context.Context, q preflightQuerier, publication string) preflightResult {
	exists, err := q.PublicationExists(ctx, publication)
	if err != nil {
		return preflightResult{
			Check:  preflightCheckPublication,
			Status: preflightIndeterminate,
			Detail: fmt.Sprintf("could not look up publication %q: %v", publication, err),
		}
	}
	if !exists {
		return preflightResult{
			Check:  preflightCheckPublication,
			Status: preflightFailed,
			Detail: fmt.Sprintf("publication %q does not exist; create it with "+
				"CREATE PUBLICATION %s FOR TABLE ...", publication, publication),
		}
	}
	return preflightResult{
		Check:  preflightCheckPublication,
		Status: preflightPassed,
		Detail: fmt.Sprintf("publication %q exists", publication),
	}
}

func checkPublishedTables(publication string, tables []publishedTable) preflightResult {
	if len(tables) == 0 {
		// Legal, and legitimate for a FOR ALL TABLES publication on a schema
		// that has not been created yet, so this warns rather than fails.
		return preflightResult{
			Check:  preflightCheckPublishedTables,
			Status: preflightWarned,
			Detail: fmt.Sprintf("publication %q currently publishes no tables, "+
				"so the pipeline will emit no change events until tables are added to it", publication),
		}
	}
	return preflightResult{
		Check:  preflightCheckPublishedTables,
		Status: preflightPassed,
		Detail: fmt.Sprintf("publication %q publishes %d table(s)", publication, len(tables)),
	}
}

func checkReplicaIdentity(tables []publishedTable) preflightResult {
	var inadequate []string
	for _, t := range tables {
		if !t.hasUsableReplicaIdentity() {
			inadequate = append(inadequate, t.qualifiedName())
		}
	}
	if len(inadequate) == 0 {
		return preflightResult{
			Check:  preflightCheckReplicaIdentity,
			Status: preflightPassed,
			Detail: fmt.Sprintf("all %d published table(s) have a usable replica identity", len(tables)),
		}
	}
	sort.Strings(inadequate)
	// A warning, not a failure: replica identity only constrains UPDATE and
	// DELETE, and the connector cannot know whether the pipeline's publication
	// carries them or whether the workload issues them.
	return preflightResult{
		Check:  preflightCheckReplicaIdentity,
		Status: preflightWarned,
		Detail: fmt.Sprintf("these published tables have no usable replica identity: %s; "+
			"PostgreSQL will reject UPDATE and DELETE against them while they are published. "+
			"Add a primary key or set REPLICA IDENTITY FULL", strings.Join(inadequate, ", ")),
	}
}

// logPreflight emits one line per check, passing or not.
//
// A silent preflight is indistinguishable from a preflight that did not run,
// which is the state an operator most wants to rule out when replication does
// not start.
func logPreflight(ctx context.Context, results []preflightResult) {
	for _, r := range results {
		switch r.Status {
		case preflightPassed:
			log.Infof(ctx, "postgresio: preflight %s: PASS: %s", r.Check, r.Detail)
		case preflightWarned:
			log.Warnf(ctx, "postgresio: preflight %s: WARN: %s", r.Check, r.Detail)
		case preflightIndeterminate:
			log.Warnf(ctx, "postgresio: preflight %s: UNKNOWN: %s", r.Check, r.Detail)
		default:
			log.Errorf(ctx, "postgresio: preflight %s: FAIL: %s", r.Check, r.Detail)
		}
	}
}

// preflightError folds the failing checks into one error, or returns nil.
//
// Only preflightFailed contributes. Warnings and indeterminate results are
// reported through the log and do not stop the pipeline.
func preflightError(results []preflightResult) error {
	var failures []string
	for _, r := range results {
		if r.Status == preflightFailed {
			failures = append(failures, fmt.Sprintf("%s: %s", r.Check, r.Detail))
		}
	}
	if len(failures) == 0 {
		return nil
	}
	return fmt.Errorf("postgresio: preflight validation failed: %s", strings.Join(failures, "; "))
}

// Preflight validates that the server is configured for this pipeline and
// returns an error naming every setting that is wrong.
//
// It opens its own short-lived ordinary connection and closes it before
// returning, so it is safe to call from a driver program, an operator tool or a
// test. The source runs the same checks on the worker that opens the
// replication stream; calling this directly is for getting the answer earlier.
//
// Conditions that make replication impossible -- wal_level, slot headroom, a
// missing publication -- return an error. Conditions that are merely
// surprising, and checks that could not be completed, are logged instead: a
// diagnostic that cannot reach the catalog must not be the reason a working
// pipeline refuses to start.
func Preflight(ctx context.Context, opts CDCOptions) error {
	querier, err := newSQLPreflightQuerier(opts)
	if err != nil {
		return err
	}
	defer querier.Close()

	results := runPreflight(ctx, querier, opts)
	logPreflight(ctx, results)
	return preflightError(results)
}

// ValidatePublicationProjections validates that table-specific publication column lists include
// all primary key / replica identity attributes, and that row filters are syntactically safe.
// PostgreSQL halts replication if a publication column projection omits any replica identity attribute.
func ValidatePublicationProjections(configs []PublicationTableConfig, pkMap map[string][]string) error {
	for _, cfg := range configs {
		if err := SanitizeRowFilter(cfg.RowFilter); err != nil {
			return err
		}
		if len(cfg.Columns) > 0 {
			pks, ok := pkMap[cfg.TableName]
			if ok && len(pks) > 0 {
				colSet := make(map[string]bool, len(cfg.Columns))
				for _, col := range cfg.Columns {
					colSet[col] = true
				}
				for _, pk := range pks {
					if !colSet[pk] {
						return fmt.Errorf("postgresio: table %q publication column list %v omits replica identity primary key column %q; PostgreSQL will reject UPDATE and DELETE operations",
							cfg.TableName, cfg.Columns, pk)
					}
				}
			}
		}
	}
	return nil
}
