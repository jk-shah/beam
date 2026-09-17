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
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakePreflightQuerier drives preflight without a database. Every field is a
// scripted answer for one check, so a test can make exactly one check fail and
// leave the rest healthy.
type fakePreflightQuerier struct {
	mu sync.Mutex

	walLevel    string
	walLevelErr error

	capacity    slotCapacity
	capacityErr error

	publicationExists bool
	publicationErr    error

	tables    []publishedTable
	tablesErr error

	// block, when set, makes every call wait for that long or until the
	// context is cancelled, whichever comes first. It models a catalog read
	// stuck behind a lock.
	block time.Duration

	calls  int
	closed bool
}

// healthyPreflightQuerier is a server on which every check passes.
func healthyPreflightQuerier() *fakePreflightQuerier {
	return &fakePreflightQuerier{
		walLevel:          "logical",
		capacity:          slotCapacity{Used: 3, Limit: 10},
		publicationExists: true,
		tables: []publishedTable{
			{Schema: "public", Table: "orders", ReplicaIdentity: "d", HasPrimaryKey: true},
		},
	}
}

func (f *fakePreflightQuerier) wait(ctx context.Context) error {
	f.mu.Lock()
	f.calls++
	block := f.block
	f.mu.Unlock()
	if block <= 0 {
		return nil
	}
	select {
	case <-time.After(block):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *fakePreflightQuerier) WALLevel(ctx context.Context) (string, error) {
	if err := f.wait(ctx); err != nil {
		return "", err
	}
	return f.walLevel, f.walLevelErr
}

func (f *fakePreflightQuerier) SlotCapacity(ctx context.Context, _ string) (slotCapacity, error) {
	if err := f.wait(ctx); err != nil {
		return slotCapacity{}, err
	}
	return f.capacity, f.capacityErr
}

func (f *fakePreflightQuerier) PublicationExists(ctx context.Context, _ string) (bool, error) {
	if err := f.wait(ctx); err != nil {
		return false, err
	}
	return f.publicationExists, f.publicationErr
}

func (f *fakePreflightQuerier) PublishedTables(ctx context.Context, _ string) ([]publishedTable, error) {
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	return f.tables, f.tablesErr
}

func (f *fakePreflightQuerier) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakePreflightQuerier) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakePreflightQuerier) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func preflightOptions() CDCOptions {
	return NewCDCOptions(
		WithCDCSlotName("test_slot"),
		WithCDCPublication("test_pub"),
	)
}

// resultFor returns the result for one check by name.
func resultFor(t *testing.T, results []preflightResult, check string) preflightResult {
	t.Helper()
	for _, r := range results {
		if r.Check == check {
			return r
		}
	}
	t.Fatalf("preflight produced no result for check %q; got %v", check, checkNames(results))
	return preflightResult{}
}

func checkNames(results []preflightResult) []string {
	names := make([]string, 0, len(results))
	for _, r := range results {
		names = append(names, r.Check+"="+r.Status.String())
	}
	return names
}

// requireStatus asserts one check reached one specific classification.
//
// Asserting the classification rather than the absence of an error is
// deliberate: several different outcomes produce no error, so an absence-only
// assertion would pass whichever of them occurred.
func requireStatus(t *testing.T, results []preflightResult, check string, want preflightStatus) preflightResult {
	t.Helper()
	got := resultFor(t, results, check)
	if got.Status != want {
		t.Errorf("check %q reported %s, want %s: %s", check, got.Status, want, got.Detail)
	}
	return got
}

// --- wal_level ---

func TestPreflightPassesWhenWALLevelIsLogical(t *testing.T) {
	results := runPreflight(context.Background(), healthyPreflightQuerier(), preflightOptions())
	requireStatus(t, results, preflightCheckWALLevel, preflightPassed)
	if err := preflightError(results); err != nil {
		t.Errorf("a fully healthy server produced a preflight error: %v", err)
	}
}

// TestPreflightFailsWhenWALLevelIsNotLogical also pins the message content. A
// failure that says "invalid configuration" costs the operator the lookup that
// the connector already did.
func TestPreflightFailsWhenWALLevelIsNotLogical(t *testing.T) {
	q := healthyPreflightQuerier()
	q.walLevel = "replica"

	results := runPreflight(context.Background(), q, preflightOptions())
	got := requireStatus(t, results, preflightCheckWALLevel, preflightFailed)

	for _, want := range []string{"replica", "wal_level=logical", "restart"} {
		if !strings.Contains(got.Detail, want) {
			t.Errorf("wal_level failure detail omits %q: %s", want, got.Detail)
		}
	}

	err := preflightError(results)
	if err == nil {
		t.Fatal("a server with wal_level=replica passed preflight; logical decoding cannot work on it")
	}
	if !strings.Contains(err.Error(), preflightCheckWALLevel) {
		t.Errorf("aggregated error does not name the failing check: %v", err)
	}
}

func TestPreflightIsIndeterminateWhenWALLevelCannotBeRead(t *testing.T) {
	q := healthyPreflightQuerier()
	q.walLevelErr = errors.New("permission denied for view pg_settings")

	results := runPreflight(context.Background(), q, preflightOptions())
	requireStatus(t, results, preflightCheckWALLevel, preflightIndeterminate)

	if err := preflightError(results); err != nil {
		t.Errorf("a check that could not run stopped the pipeline: %v", err)
	}
}

// --- replication slot headroom ---

func TestPreflightFailsWithoutReplicationSlotHeadroom(t *testing.T) {
	q := healthyPreflightQuerier()
	q.capacity = slotCapacity{Used: 10, Limit: 10}

	results := runPreflight(context.Background(), q, preflightOptions())
	got := requireStatus(t, results, preflightCheckSlotHeadroom, preflightFailed)

	for _, want := range []string{"max_replication_slots", "10", "test_slot"} {
		if !strings.Contains(got.Detail, want) {
			t.Errorf("slot headroom failure detail omits %q: %s", want, got.Detail)
		}
	}
	if preflightError(results) == nil {
		t.Error("a full slot table passed preflight; slot creation will fail at connection time")
	}
}

// TestPreflightAllowsAFullSlotTableWhenTheSlotAlreadyExists is the resumption
// case, which is the common one. An existing slot consumes no headroom, so the
// limit does not apply to this pipeline.
func TestPreflightAllowsAFullSlotTableWhenTheSlotAlreadyExists(t *testing.T) {
	q := healthyPreflightQuerier()
	q.capacity = slotCapacity{Used: 10, Limit: 10, Exists: true}

	results := runPreflight(context.Background(), q, preflightOptions())
	got := requireStatus(t, results, preflightCheckSlotHeadroom, preflightPassed)
	if !strings.Contains(got.Detail, "test_slot") {
		t.Errorf("slot headroom detail does not name the slot: %s", got.Detail)
	}
	if err := preflightError(results); err != nil {
		t.Errorf("resuming an existing slot failed preflight: %v", err)
	}
}

func TestPreflightIsIndeterminateWhenSlotCapacityCannotBeRead(t *testing.T) {
	q := healthyPreflightQuerier()
	q.capacityErr = errors.New("connection reset by peer")

	results := runPreflight(context.Background(), q, preflightOptions())
	requireStatus(t, results, preflightCheckSlotHeadroom, preflightIndeterminate)
	if err := preflightError(results); err != nil {
		t.Errorf("an unreadable slot count stopped the pipeline: %v", err)
	}
}

// --- publication ---

func TestPreflightFailsWhenThePublicationIsMissing(t *testing.T) {
	q := healthyPreflightQuerier()
	q.publicationExists = false

	results := runPreflight(context.Background(), q, preflightOptions())
	got := requireStatus(t, results, preflightCheckPublication, preflightFailed)

	if !strings.Contains(got.Detail, "test_pub") {
		t.Errorf("publication failure detail does not name the publication: %s", got.Detail)
	}
	if !strings.Contains(got.Detail, "CREATE PUBLICATION") {
		t.Errorf("publication failure detail does not say how to fix it: %s", got.Detail)
	}
	if preflightError(results) == nil {
		t.Error("a missing publication passed preflight")
	}
}

// TestPreflightSkipsTableChecksWhenThePublicationIsMissing keeps one root cause
// from being reported as three separate failures.
func TestPreflightSkipsTableChecksWhenThePublicationIsMissing(t *testing.T) {
	q := healthyPreflightQuerier()
	q.publicationExists = false

	results := runPreflight(context.Background(), q, preflightOptions())
	for _, r := range results {
		if r.Check == preflightCheckPublishedTables || r.Check == preflightCheckReplicaIdentity {
			t.Errorf("check %q ran even though the publication does not exist: %s", r.Check, r.Detail)
		}
	}
}

func TestPreflightIsIndeterminateWhenThePublicationLookupFails(t *testing.T) {
	q := healthyPreflightQuerier()
	q.publicationErr = errors.New("canceling statement due to statement timeout")

	results := runPreflight(context.Background(), q, preflightOptions())
	requireStatus(t, results, preflightCheckPublication, preflightIndeterminate)
	if err := preflightError(results); err != nil {
		t.Errorf("an unreadable publication catalog stopped the pipeline: %v", err)
	}
}

// --- published tables ---

// TestPreflightWarnsWhenThePublicationIsEmpty stays a warning: a FOR ALL TABLES
// publication on a schema whose tables are created later is legitimate.
func TestPreflightWarnsWhenThePublicationIsEmpty(t *testing.T) {
	q := healthyPreflightQuerier()
	q.tables = nil

	results := runPreflight(context.Background(), q, preflightOptions())
	requireStatus(t, results, preflightCheckPublishedTables, preflightWarned)
	if err := preflightError(results); err != nil {
		t.Errorf("an empty publication stopped the pipeline: %v", err)
	}
}

func TestPreflightIsIndeterminateWhenTheTableListFails(t *testing.T) {
	q := healthyPreflightQuerier()
	q.tablesErr = errors.New("permission denied for view pg_publication_tables")

	results := runPreflight(context.Background(), q, preflightOptions())
	requireStatus(t, results, preflightCheckPublishedTables, preflightIndeterminate)
	if err := preflightError(results); err != nil {
		t.Errorf("an unreadable table list stopped the pipeline: %v", err)
	}
}

// --- replica identity ---

func TestReplicaIdentityAdequacy(t *testing.T) {
	cases := []struct {
		name    string
		table   publishedTable
		usable  bool
		because string
	}{
		{"default with a primary key", publishedTable{ReplicaIdentity: "d", HasPrimaryKey: true}, true,
			"the primary key supplies the old tuple"},
		{"default without a primary key", publishedTable{ReplicaIdentity: "d"}, false,
			"there is no key to identify the old row"},
		{"full", publishedTable{ReplicaIdentity: "f"}, true, "the whole old row is logged"},
		{"index", publishedTable{ReplicaIdentity: "i"}, true, "a nominated unique index supplies the old tuple"},
		{"nothing", publishedTable{ReplicaIdentity: "n"}, false, "no old tuple is logged at all"},
		{"unrecognized", publishedTable{ReplicaIdentity: "?"}, false, "an unknown value must not be assumed benign"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.table.hasUsableReplicaIdentity(); got != tc.usable {
				t.Errorf("relreplident %q with primary key=%v reported usable=%v, want %v because %s",
					tc.table.ReplicaIdentity, tc.table.HasPrimaryKey, got, tc.usable, tc.because)
			}
		})
	}
}

// TestPreflightWarnsAboutTablesWithoutAUsableReplicaIdentity asserts both the
// classification and that it is not fatal. The connector cannot know whether
// the pipeline consumes UPDATE or DELETE, so it must not refuse to start.
func TestPreflightWarnsAboutTablesWithoutAUsableReplicaIdentity(t *testing.T) {
	q := healthyPreflightQuerier()
	q.tables = []publishedTable{
		{Schema: "public", Table: "orders", ReplicaIdentity: "d", HasPrimaryKey: true},
		{Schema: "public", Table: "audit_log", ReplicaIdentity: "n"},
		{Schema: "sales", Table: "events", ReplicaIdentity: "d"},
	}

	results := runPreflight(context.Background(), q, preflightOptions())
	got := requireStatus(t, results, preflightCheckReplicaIdentity, preflightWarned)

	for _, want := range []string{"public.audit_log", "sales.events", "REPLICA IDENTITY FULL"} {
		if !strings.Contains(got.Detail, want) {
			t.Errorf("replica identity warning omits %q: %s", want, got.Detail)
		}
	}
	if strings.Contains(got.Detail, "public.orders") {
		t.Errorf("replica identity warning names a table that is correctly configured: %s", got.Detail)
	}
	if err := preflightError(results); err != nil {
		t.Errorf("an inadequate replica identity stopped the pipeline: %v", err)
	}
}

// --- reporting ---

// TestPreflightReportsEveryCheckOnAHealthyServer keeps a passing preflight
// visible. A silent preflight cannot be distinguished from one that never ran,
// which is the state an operator most wants to rule out.
func TestPreflightReportsEveryCheckOnAHealthyServer(t *testing.T) {
	results := runPreflight(context.Background(), healthyPreflightQuerier(), preflightOptions())

	wanted := []string{
		preflightCheckWALLevel,
		preflightCheckSlotHeadroom,
		preflightCheckPublication,
		preflightCheckPublishedTables,
		preflightCheckReplicaIdentity,
	}
	if len(results) != len(wanted) {
		t.Fatalf("preflight produced %d results, want %d: %v", len(results), len(wanted), checkNames(results))
	}
	for _, check := range wanted {
		requireStatus(t, results, check, preflightPassed)
	}
}

func TestPreflightErrorNamesEveryFailingCheck(t *testing.T) {
	q := healthyPreflightQuerier()
	q.walLevel = "replica"
	q.capacity = slotCapacity{Used: 10, Limit: 10}

	err := preflightError(runPreflight(context.Background(), q, preflightOptions()))
	if err == nil {
		t.Fatal("two definitive misconfigurations passed preflight")
	}
	for _, want := range []string{preflightCheckWALLevel, preflightCheckSlotHeadroom} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("aggregated error omits failing check %q: %v", want, err)
		}
	}
}

// TestPreflightTimeoutDoesNotStopThePipeline covers the availability rule: a
// diagnostic that cannot complete must degrade, not block. A catalog read
// stuck behind unrelated DDL should not prevent a pipeline that would
// otherwise run.
func TestPreflightTimeoutDoesNotStopThePipeline(t *testing.T) {
	previous := preflightTimeout
	preflightTimeout = 50 * time.Millisecond
	t.Cleanup(func() { preflightTimeout = previous })

	q := healthyPreflightQuerier()
	q.block = 10 * time.Second

	start := time.Now()
	results := runPreflight(context.Background(), q, preflightOptions())
	elapsed := time.Since(start)

	if elapsed > 5*time.Second {
		t.Fatalf("preflight took %s; the timeout did not bound it", elapsed)
	}
	requireStatus(t, results, preflightCheckWALLevel, preflightIndeterminate)
	if err := preflightError(results); err != nil {
		t.Errorf("a timed-out preflight stopped the pipeline: %v", err)
	}
}

// --- placement and lifecycle ---

// newPreflightSourceFn builds a source whose preflight and replication stream
// are both fakes.
func newPreflightSourceFn(t *testing.T, q *fakePreflightQuerier) (*cdcSourceFn, *int32Counter) {
	t.Helper()

	dialled := &int32Counter{}
	opts := NewCDCOptions(
		WithCDCSlotName("test_slot"),
		WithCDCPublication("test_pub"),
		WithCDCHeartbeatInterval(30*time.Second),
		WithCDCSlotMonitoring(false),
		WithCDCStreamFactory(func(context.Context, CDCOptions) (ReplicationStream, error) {
			dialled.inc()
			return NewMockReplicationStream(), nil
		}),
	)
	fn := newCDCSourceFn(opts)
	fn.newPreflightQuerier = func(CDCOptions) (preflightQuerier, error) { return q, nil }
	t.Cleanup(func() { _ = fn.Teardown() })
	return fn, dialled
}

// TestPreflightDoesNotRunInSetup is the connection-storm guard.
//
// Setup executes on every worker the runner initializes. Validating there
// would open one connection per worker against the primary at the same
// instant, which can exhaust max_connections. Moving preflight back into Setup
// must fail this test.
func TestPreflightDoesNotRunInSetup(t *testing.T) {
	q := healthyPreflightQuerier()
	fn, _ := newPreflightSourceFn(t, q)

	if err := fn.Setup(context.Background()); err != nil {
		t.Fatalf("Setup returned an error: %v", err)
	}
	if got := q.callCount(); got != 0 {
		t.Errorf("Setup issued %d catalog queries; preflight must not open a connection on every worker", got)
	}
}

// TestPreflightRunsOncePerWorker checks that reopening a session after the
// connection is dropped does not re-query the catalog.
func TestPreflightRunsOncePerWorker(t *testing.T) {
	q := healthyPreflightQuerier()
	fn, _ := newPreflightSourceFn(t, q)
	ctx := context.Background()

	session, err := fn.ensureSession(ctx, 0)
	if err != nil {
		t.Fatalf("first ensureSession failed: %v", err)
	}
	afterFirst := q.callCount()
	if afterFirst == 0 {
		t.Fatal("opening the first session ran no preflight checks")
	}
	if !q.isClosed() {
		t.Error("the preflight connection was left open; it is used once and must not persist")
	}

	// A dropped session is the retry path, and it must not re-validate.
	fn.dropSession(session)
	if _, err := fn.ensureSession(ctx, 0); err != nil {
		t.Fatalf("second ensureSession failed: %v", err)
	}
	if got := q.callCount(); got != afterFirst {
		t.Errorf("reopening the session issued %d more catalog queries; preflight must run once per worker",
			got-afterFirst)
	}
}

// TestPreflightFailureStopsBeforeTheReplicationConnection asserts ordering: the
// point of preflight is that the operator reads a named setting rather than a
// protocol error from the replication handshake.
func TestPreflightFailureStopsBeforeTheReplicationConnection(t *testing.T) {
	q := healthyPreflightQuerier()
	q.walLevel = "replica"
	fn, dialled := newPreflightSourceFn(t, q)

	_, err := fn.ensureSession(context.Background(), 0)
	if err == nil {
		t.Fatal("ensureSession succeeded against a server with wal_level=replica")
	}
	if !strings.Contains(err.Error(), "wal_level") {
		t.Errorf("the error does not name the setting at fault: %v", err)
	}
	if dialled.value() != 0 {
		t.Errorf("the replication connection was opened %d time(s) despite a failed preflight", dialled.value())
	}
}

// TestPreflightRetriesAfterAFailure covers the operator fixing the setting: the
// next bundle must re-check rather than reuse the earlier verdict.
func TestPreflightRetriesAfterAFailure(t *testing.T) {
	q := healthyPreflightQuerier()
	q.walLevel = "replica"
	fn, _ := newPreflightSourceFn(t, q)
	ctx := context.Background()

	if _, err := fn.ensureSession(ctx, 0); err == nil {
		t.Fatal("ensureSession succeeded against a server with wal_level=replica")
	}
	afterFailure := q.callCount()

	q.mu.Lock()
	q.walLevel = "logical"
	q.mu.Unlock()

	if _, err := fn.ensureSession(ctx, 0); err != nil {
		t.Fatalf("ensureSession failed after the server was corrected: %v", err)
	}
	if q.callCount() <= afterFailure {
		t.Error("preflight was not re-run after a failure; a corrected server would never be noticed")
	}
}

// TestPreflightSurvivesAnUnopenableConnection keeps the diagnostic from
// becoming an availability dependency of its own.
func TestPreflightSurvivesAnUnopenableConnection(t *testing.T) {
	dialled := &int32Counter{}
	opts := NewCDCOptions(
		WithCDCSlotName("test_slot"),
		WithCDCPublication("test_pub"),
		WithCDCHeartbeatInterval(30*time.Second),
		WithCDCSlotMonitoring(false),
		WithCDCStreamFactory(func(context.Context, CDCOptions) (ReplicationStream, error) {
			dialled.inc()
			return NewMockReplicationStream(), nil
		}),
	)
	fn := newCDCSourceFn(opts)
	fn.newPreflightQuerier = func(CDCOptions) (preflightQuerier, error) {
		return nil, errors.New("dial tcp: connection refused")
	}
	t.Cleanup(func() { _ = fn.Teardown() })

	if _, err := fn.ensureSession(context.Background(), 0); err != nil {
		t.Fatalf("a preflight connection that could not be opened stopped the pipeline: %v", err)
	}
	if dialled.value() != 1 {
		t.Errorf("the replication connection was opened %d time(s), want 1", dialled.value())
	}
}

// --- options ---

func TestConstructionTimePreflightIsOffByDefault(t *testing.T) {
	if NewCDCOptions().PreflightAtConstruction {
		t.Error("construction-time preflight is on by default; that breaks Flex Templates and TokenProvider, " +
			"neither of which can reach the database from the submitting machine")
	}
}

func TestWithCDCPreflightReadsInThePositive(t *testing.T) {
	if !NewCDCOptions(WithCDCPreflight(true)).PreflightAtConstruction {
		t.Error("WithCDCPreflight(true) did not enable construction-time preflight")
	}
	if NewCDCOptions(WithCDCPreflight(false)).PreflightAtConstruction {
		t.Error("WithCDCPreflight(false) enabled construction-time preflight")
	}
}

func TestValidatePublicationProjections(t *testing.T) {
	pkMap := map[string][]string{
		"public.orders":   {"order_id"},
		"public.payments": {"account_id", "seq_no"},
	}

	t.Run("valid projection containing all primary keys", func(t *testing.T) {
		configs := []PublicationTableConfig{
			{
				TableName: "public.orders",
				Columns:   []string{"order_id", "amount", "status"},
				RowFilter: "amount > 50",
			},
			{
				TableName: "public.payments",
				Columns:   []string{"account_id", "seq_no", "amount"},
			},
		}
		if err := ValidatePublicationProjections(configs, pkMap); err != nil {
			t.Fatalf("unexpected error for valid projections: %v", err)
		}
	})

	t.Run("projection omitting primary key is rejected", func(t *testing.T) {
		configs := []PublicationTableConfig{
			{
				TableName: "public.orders",
				Columns:   []string{"amount", "status"}, // omits order_id!
			},
		}
		err := ValidatePublicationProjections(configs, pkMap)
		if err == nil {
			t.Fatalf("expected error when publication column list omits primary key")
		}
		if !strings.Contains(err.Error(), "omits replica identity primary key column") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("projection with dangerous row filter is rejected", func(t *testing.T) {
		configs := []PublicationTableConfig{
			{
				TableName: "public.orders",
				Columns:   []string{"order_id", "amount"},
				RowFilter: "amount > 0; DROP TABLE users",
			},
		}
		if err := ValidatePublicationProjections(configs, pkMap); err == nil {
			t.Fatalf("expected error for SQL injection in row filter")
		}
	})
}
