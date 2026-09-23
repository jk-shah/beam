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

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

// fakeSlotQuerier drives the monitor without a database.
type fakeSlotQuerier struct {
	mu sync.Mutex

	// responses are returned in order; the last one repeats.
	responses []fakeSlotResponse
	calls     int

	serverLimit    int64
	serverLimitErr error

	closed bool

	// queried is signalled after every QuerySlotRetention call, so a test can
	// wait for a measurement instead of sleeping.
	queried chan struct{}
}

type fakeSlotResponse struct {
	retention slotRetention
	err       error
}

func newFakeSlotQuerier(responses ...fakeSlotResponse) *fakeSlotQuerier {
	return &fakeSlotQuerier{
		responses:   responses,
		serverLimit: -1,
		queried:     make(chan struct{}, 64),
	}
}

func (f *fakeSlotQuerier) QuerySlotRetention(_ context.Context, _ string) (slotRetention, error) {
	f.mu.Lock()
	idx := f.calls
	if idx >= len(f.responses) {
		idx = len(f.responses) - 1
	}
	f.calls++
	resp := f.responses[idx]
	f.mu.Unlock()

	select {
	case f.queried <- struct{}{}:
	default:
	}
	return resp.retention, resp.err
}

func (f *fakeSlotQuerier) MaxSlotWALKeepSizeBytes(context.Context) (int64, error) {
	return f.serverLimit, f.serverLimitErr
}

func (f *fakeSlotQuerier) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeSlotQuerier) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeSlotQuerier) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// waitForChecks blocks until the monitor has completed at least n measurements.
func (f *fakeSlotQuerier) waitForChecks(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-f.queried:
		case <-deadline:
			t.Fatalf("timed out waiting for measurement %d of %d", i+1, n)
		}
	}
}

func reserved(bytes int64) slotRetention {
	return slotRetention{RetainedBytes: bytes, WALStatus: "reserved", Active: true}
}

// startTestMonitor builds a monitor over a fake querier and starts it.
func startTestMonitor(t *testing.T, opts CDCOptions, q *fakeSlotQuerier) (*slotMonitor, *int32Counter) {
	t.Helper()
	severed := &int32Counter{}
	m := newSlotMonitor(opts, q, severed.inc)
	m.start(context.Background())
	t.Cleanup(func() { _ = m.close() })
	return m, severed
}

type int32Counter struct {
	mu sync.Mutex
	n  int
}

func (c *int32Counter) inc() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
}

func (c *int32Counter) value() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func breakerOptions(budget uint64, policy SlotLagPolicy) CDCOptions {
	return NewCDCOptions(
		WithCDCSlotName("test_slot"),
		WithCDCPublication("test_pub"),
		WithCDCMaxSlotLagBytes(budget),
		WithCDCSlotLagPolicy(policy),
		WithCDCSlotLagCheckInterval(minSlotLagCheckInterval),
	)
}

// --- Measurement and policy ---

// TestSlotMonitorDoesNotBreachUnderBudget is the baseline: a healthy slot must
// never be reported.
func TestSlotMonitorDoesNotBreachUnderBudget(t *testing.T) {
	q := newFakeSlotQuerier(fakeSlotResponse{retention: reserved(int64(4 * MiB))})
	m, severed := startTestMonitor(t, breakerOptions(8*MiB, SlotLagFailPipeline), q)

	q.waitForChecks(t, 2)

	if b := m.breached(); b != nil {
		t.Errorf("monitor latched a breach at 4 MiB against an 8 MiB budget: %v", b)
	}
	if severed.value() != 0 {
		t.Errorf("replication session was severed %d time(s) while under budget", severed.value())
	}
}

// TestSlotMonitorBreachesOverBudgetAndSeversTheSession covers the default
// policy.
//
// Severing the session is the corrective action that does not depend on
// ProcessElement being scheduled. A pipeline that is drained or suspended never
// reaches the read loop, so a latch alone would never be observed.
func TestSlotMonitorBreachesOverBudgetAndSeversTheSession(t *testing.T) {
	q := newFakeSlotQuerier(fakeSlotResponse{retention: reserved(int64(16 * MiB))})
	m, severed := startTestMonitor(t, breakerOptions(8*MiB, SlotLagFailPipeline), q)

	q.waitForChecks(t, 1)
	waitFor(t, func() bool { return m.breached() != nil }, "monitor did not latch a breach at 16 MiB against an 8 MiB budget")

	breach := m.breached()
	msg := breach.Error()
	for _, want := range []string{"test_slot", "16777216", "8388608"} {
		if !strings.Contains(msg, want) {
			t.Errorf("breach message omits %q, so an operator cannot act on it: %s", want, msg)
		}
	}
	waitFor(t, func() bool { return severed.value() > 0 }, "breach did not sever the replication session")
}

// TestSlotMonitorLogOnlyDoesNotInterfere checks that the opt-out really opts
// out: no latch, and the session is left alone.
func TestSlotMonitorLogOnlyDoesNotInterfere(t *testing.T) {
	q := newFakeSlotQuerier(fakeSlotResponse{retention: reserved(int64(16 * MiB))})
	m, severed := startTestMonitor(t, breakerOptions(8*MiB, SlotLagLogOnly), q)

	q.waitForChecks(t, 2)

	if b := m.breached(); b != nil {
		t.Errorf("LogOnly latched a breach: %v", b)
	}
	if severed.value() != 0 {
		t.Errorf("LogOnly severed the replication session %d time(s)", severed.value())
	}
}

// TestSlotMonitorBreachClearsWhenBacklogDrains ensures a transient backlog does
// not fail the pipeline permanently.
func TestSlotMonitorBreachClearsWhenBacklogDrains(t *testing.T) {
	q := newFakeSlotQuerier(
		fakeSlotResponse{retention: reserved(int64(16 * MiB))},
		fakeSlotResponse{retention: reserved(int64(1 * MiB))},
	)
	m, _ := startTestMonitor(t, breakerOptions(8*MiB, SlotLagFailPipeline), q)

	q.waitForChecks(t, 1)
	waitFor(t, func() bool { return m.breached() != nil }, "monitor did not latch the initial breach")
	waitFor(t, func() bool { return m.breached() == nil }, "latch did not clear after retention fell back under budget")
}

// --- Failure modes: the breaker must never become the outage ---

// TestSlotMonitorQueryFailureDoesNotBreach is the design's central safety
// property. A monitoring bug or a permissions error must not fail a pipeline
// that is otherwise healthy.
func TestSlotMonitorQueryFailureDoesNotBreach(t *testing.T) {
	q := newFakeSlotQuerier(fakeSlotResponse{err: errors.New("permission denied for view pg_replication_slots")})
	m, severed := startTestMonitor(t, breakerOptions(8*MiB, SlotLagFailPipeline), q)

	q.waitForChecks(t, 2)

	if b := m.breached(); b != nil {
		t.Errorf("a failed measurement produced a breach: %v", b)
	}
	if severed.value() != 0 {
		t.Errorf("a failed measurement severed the replication session %d time(s)", severed.value())
	}
}

// TestSlotMonitorMissingSlotDoesNotBreach separates "cannot measure" from
// "over budget". A slot that is absent is a configuration problem, not a
// retention problem, and must not be reported as one.
func TestSlotMonitorMissingSlotDoesNotBreach(t *testing.T) {
	q := newFakeSlotQuerier(fakeSlotResponse{err: errSlotNotFound})
	m, severed := startTestMonitor(t, breakerOptions(8*MiB, SlotLagFailPipeline), q)

	q.waitForChecks(t, 2)

	if m.breached() != nil || severed.value() != 0 {
		t.Error("a missing slot was treated as a retention breach")
	}
}

// TestSlotMonitorClassifiesEachNonBreachingCondition covers the conditions that
// correctly produce no breach.
//
// They are asserted through the returned outcome rather than only through the
// absence of a latch. Several different guards decline to breach, and "no
// latch" is satisfied if any one of them fires, so an absence-only assertion
// stays green when a guard is deleted. Mutation testing demonstrated exactly
// that: removing either the lost or the unreserved guard left both tests
// passing, because the other one absorbed the case.
func TestSlotMonitorClassifiesEachNonBreachingCondition(t *testing.T) {
	cases := []struct {
		name      string
		response  fakeSlotResponse
		want      slotCheckOutcome
		wantLatch bool
	}{
		{
			// Invalidation also nulls restart_lsn, so this must be recognised
			// as lost rather than as a slot that never reserved anything. The
			// distinction matters: unreserved is benign and clears itself,
			// lost is terminal and cannot clear by waiting.
			name:      "lost slot",
			response:  fakeSlotResponse{retention: slotRetention{RestartLSNUnset: true, WALStatus: "lost"}},
			want:      slotCheckLost,
			wantLatch: true,
		},
		{
			name:     "slot has reserved no WAL",
			response: fakeSlotResponse{retention: slotRetention{RestartLSNUnset: true, WALStatus: "reserved"}},
			want:     slotCheckUnreserved,
		},
		{
			// The byte count in this state is the COALESCE default rather than
			// a measurement, so the guard must not defer to it even when it
			// reads high.
			name: "unreserved slot reporting a high byte count",
			response: fakeSlotResponse{
				retention: slotRetention{RetainedBytes: int64(64 * MiB), RestartLSNUnset: true, WALStatus: "reserved"},
			},
			want: slotCheckUnreserved,
		},
		{
			name:     "within budget",
			response: fakeSlotResponse{retention: reserved(int64(4 * MiB))},
			want:     slotCheckUnderBudget,
		},
		{
			name:     "measurement failed",
			response: fakeSlotResponse{err: errors.New("permission denied")},
			want:     slotCheckMeasureFailed,
		},
		{
			name:     "slot missing",
			response: fakeSlotResponse{err: errSlotNotFound},
			want:     slotCheckMeasureFailed,
		},
		{
			name:      "over budget",
			response:  fakeSlotResponse{retention: reserved(int64(64 * MiB))},
			want:      slotCheckBreached,
			wantLatch: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := newFakeSlotQuerier(tc.response)
			severed := &int32Counter{}
			m := newSlotMonitor(breakerOptions(8*MiB, SlotLagFailPipeline), q, severed.inc)
			t.Cleanup(func() { _ = m.close() })

			if got := m.check(context.Background()); got != tc.want {
				t.Errorf("check classified this as %v, want %v", got, tc.want)
			}
			if latched := m.breached() != nil; latched != tc.wantLatch {
				t.Errorf("breach latched = %v, want %v", latched, tc.wantLatch)
			}
			if wantSevered := tc.wantLatch; (severed.value() > 0) != wantSevered {
				t.Errorf("session severed = %v, want %v", severed.value() > 0, wantSevered)
			}
		})
	}
}

// TestSlotMonitorTolueratesUnreadableServerLimit ensures the startup comparison
// against max_slot_wal_keep_size cannot stop the monitor from running.
func TestSlotMonitorToleratesUnreadableServerLimit(t *testing.T) {
	q := newFakeSlotQuerier(fakeSlotResponse{retention: reserved(int64(1 * MiB))})
	q.serverLimitErr = errors.New("unavailable")

	_, _ = startTestMonitor(t, breakerOptions(8*MiB, SlotLagFailPipeline), q)

	// The monitor must still be measuring despite the failed startup read.
	q.waitForChecks(t, 2)
}

// TestSlotMonitorLatchesTerminalErrorOnLostSlot tests GAP-003: when wal_status is 'lost',
// the monitor latches a terminal breach, severs the replication session, produces an explicit
// diagnostic error, and does not clear the latch on subsequent under-budget checks.
func TestSlotMonitorLatchesTerminalErrorOnLostSlot(t *testing.T) {
	q := newFakeSlotQuerier(
		fakeSlotResponse{retention: slotRetention{RestartLSNUnset: true, WALStatus: "lost"}},
		fakeSlotResponse{retention: reserved(int64(2 * MiB))},
	)
	severed := &int32Counter{}
	m := newSlotMonitor(breakerOptions(8*MiB, SlotLagFailPipeline), q, severed.inc)
	t.Cleanup(func() { _ = m.close() })

	outcome := m.check(context.Background())
	if outcome != slotCheckLost {
		t.Fatalf("expected slotCheckLost, got %v", outcome)
	}

	breach := m.breached()
	if breach == nil {
		t.Fatal("expected breach to be latched for lost slot, got nil")
	}
	if breach.WALStatus != "lost" {
		t.Fatalf("expected breach.WALStatus='lost', got %q", breach.WALStatus)
	}
	if severed.value() == 0 {
		t.Fatal("expected onBreach() to sever the session on lost slot")
	}
	if !strings.Contains(breach.Error(), "wal_status=lost") {
		t.Fatalf("expected error message to mention 'wal_status=lost', got %q", breach.Error())
	}

	// Subsequent check returning under budget must NOT clear the terminal lost latch
	m.check(context.Background())
	if m.breached() == nil {
		t.Fatal("expected terminal breach latch to persist across under-budget readings")
	}
}

// --- Lifecycle ---

// TestSlotMonitorCloseIsPromptAndReleasesTheConnection guards teardown.
func TestSlotMonitorCloseIsPromptAndReleasesTheConnection(t *testing.T) {
	q := newFakeSlotQuerier(fakeSlotResponse{retention: reserved(0)})
	m := newSlotMonitor(breakerOptions(8*MiB, SlotLagFailPipeline), q, nil)
	m.start(context.Background())
	q.waitForChecks(t, 1)

	start := time.Now()
	if err := m.close(); err != nil {
		t.Fatalf("close returned an error: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("close took %s; teardown must not block on the poll loop", elapsed)
	}
	if !q.isClosed() {
		t.Error("close did not release the monitoring connection")
	}

	// Idempotent: Teardown may run after an explicit close.
	if err := m.close(); err != nil {
		t.Errorf("second close returned an error: %v", err)
	}
}

// TestSlotMonitorStopsPollingAfterClose catches a leaked goroutine that would
// keep querying a primary after the pipeline is gone.
func TestSlotMonitorStopsPollingAfterClose(t *testing.T) {
	q := newFakeSlotQuerier(fakeSlotResponse{retention: reserved(0)})
	m := newSlotMonitor(breakerOptions(8*MiB, SlotLagFailPipeline), q, nil)
	m.start(context.Background())
	q.waitForChecks(t, 1)
	_ = m.close()

	after := q.callCount()
	time.Sleep(3 * minSlotLagCheckInterval)
	if got := q.callCount(); got != after {
		t.Errorf("monitor kept polling after close: %d calls became %d", after, got)
	}
}

// TestSlotLagCheckIntervalIsClamped prevents a misconfiguration from turning the
// breaker into a hot loop against a primary that may already be struggling.
func TestSlotLagCheckIntervalIsClamped(t *testing.T) {
	opts := NewCDCOptions(
		WithCDCSlotName("test_slot"),
		WithCDCMaxSlotLagBytes(8*MiB),
		WithCDCSlotLagCheckInterval(time.Microsecond),
	)
	m := newSlotMonitor(opts, newFakeSlotQuerier(fakeSlotResponse{retention: reserved(0)}), nil)
	if m.interval < minSlotLagCheckInterval {
		t.Errorf("check interval %s is below the %s minimum", m.interval, minSlotLagCheckInterval)
	}
}

// TestSlotLagCheckIntervalDefaults confirms the documented default.
func TestSlotLagCheckIntervalDefaults(t *testing.T) {
	opts := NewCDCOptions(WithCDCSlotName("test_slot"), WithCDCMaxSlotLagBytes(8*MiB))
	m := newSlotMonitor(opts, newFakeSlotQuerier(fakeSlotResponse{retention: reserved(0)}), nil)
	if m.interval != DefaultSlotLagCheckInterval {
		t.Errorf("default check interval is %s, want %s", m.interval, DefaultSlotLagCheckInterval)
	}
}

// --- Integration with the source ---

// TestSlotMonitoringIsOnByDefault checks that retention is measured without any
// circuit breaker configuration.
//
// The monitor publishes cdc_slot_retained_bytes, which is the only accurate
// view of what the slot is costing the server: the in-process alternative was
// removed because it freezes during the stall it claims to detect. If
// monitoring were gated on the breaker, which is off by default, a default
// deployment would emit no retention signal at all.
func TestSlotMonitoringIsOnByDefault(t *testing.T) {
	h := newSDFHarness(t, true, keepalivePayload(100, false))

	q := newFakeSlotQuerier(fakeSlotResponse{retention: reserved(int64(4 * MiB))})
	created := false
	h.fn.newSlotQuerier = func(CDCOptions) (slotRetentionQuerier, error) {
		created = true
		return q, nil
	}

	h.run(context.Background(), t)

	if !created {
		t.Fatal("no slot monitor was started with default options, so the pipeline publishes no retention metric")
	}
	q.waitForChecks(t, 1)
}

// TestEnforcementIsOffWithoutABudget separates measuring from acting.
//
// Monitoring defaults on, but nothing may be enforced until the operator sets a
// budget. A slot far above any plausible threshold must still not fail a
// pipeline that never asked for a breaker.
func TestEnforcementIsOffWithoutABudget(t *testing.T) {
	h := newSDFHarness(t, true, keepalivePayload(100, false))

	q := newFakeSlotQuerier(fakeSlotResponse{retention: reserved(int64(512 * GiB))})
	h.fn.newSlotQuerier = func(CDCOptions) (slotRetentionQuerier, error) { return q, nil }

	h.run(context.Background(), t)
	q.waitForChecks(t, 1)

	if breach := h.fn.latchedBreach(); breach != nil {
		t.Errorf("a breach was latched with no budget configured: %v", breach)
	}
	if h.fn.currentSession() == nil {
		t.Error("the replication session was severed with no budget configured")
	}
}

// TestSlotMonitoringCanBeDisabled checks the opt-out, which is the only way to
// avoid the monitor's extra connection.
func TestSlotMonitoringCanBeDisabled(t *testing.T) {
	h := newSDFHarness(t, true, keepalivePayload(100, false))

	h.fn.Options.DisableSlotMonitoring = true
	created := false
	h.fn.newSlotQuerier = func(CDCOptions) (slotRetentionQuerier, error) {
		created = true
		return newFakeSlotQuerier(fakeSlotResponse{retention: reserved(0)}), nil
	}

	h.run(context.Background(), t)

	if created {
		t.Error("a slot monitor connection was opened although monitoring was disabled")
	}
}

// TestSlotMonitoringOptionReadsInThePositive guards the negated struct field.
//
// DisableSlotMonitoring is negated so the zero value leaves monitoring on. The
// option is phrased positively, so the two must not drift apart.
func TestSlotMonitoringOptionReadsInThePositive(t *testing.T) {
	if opts := NewCDCOptions(WithCDCSlotMonitoring(false)); !opts.DisableSlotMonitoring {
		t.Error("WithCDCSlotMonitoring(false) left monitoring enabled")
	}
	if opts := NewCDCOptions(WithCDCSlotMonitoring(true)); opts.DisableSlotMonitoring {
		t.Error("WithCDCSlotMonitoring(true) disabled monitoring")
	}
	if opts := NewCDCOptions(); opts.DisableSlotMonitoring {
		t.Error("monitoring is off by default; the zero value must leave it on")
	}
}

// TestProcessElementReportsALatchedBreach checks that the breach reaches the
// runner rather than only the logs.
func TestProcessElementReportsALatchedBreach(t *testing.T) {
	h := newSDFHarness(t, true, keepalivePayload(100, false))

	q := newFakeSlotQuerier(fakeSlotResponse{retention: reserved(int64(64 * MiB))})
	h.fn.Options.MaxSlotLagBytes = 8 * MiB
	h.fn.Options.SlotLagPolicy = SlotLagFailPipeline
	h.fn.Options.SlotLagCheckInterval = minSlotLagCheckInterval
	h.fn.newSlotQuerier = func(CDCOptions) (slotRetentionQuerier, error) { return q, nil }

	// Count replication connections so the test can prove the source does not
	// reconnect onto a slot that is already over budget.
	var connMu sync.Mutex
	connections := 0
	underlying := h.fn.Options.StreamFactory
	h.fn.Options.StreamFactory = func(ctx context.Context, opts CDCOptions) (ReplicationStream, error) {
		connMu.Lock()
		connections++
		connMu.Unlock()
		return underlying(ctx, opts)
	}
	connectionCount := func() int {
		connMu.Lock()
		defer connMu.Unlock()
		return connections
	}

	ctx := context.Background()

	// The first invocation starts the monitor, which measures immediately.
	_, _ = h.fn.ProcessElement(ctx, h.we, h.bf, h.rt, 1, func(beam.EventTime, ChangeEvent) {})
	q.waitForChecks(t, 1)
	waitFor(t, func() bool { return h.fn.latchedBreach() != nil }, "the monitor did not latch a breach")

	// The monitor severed the session on breach, so a reconnect is what the
	// pre-session check exists to prevent. Without it the invocation reopens
	// the replication connection and resumes pinning WAL on a slot that is
	// already over budget, before the in-loop check reports the breach.
	waitFor(t, func() bool { return h.fn.currentSession() == nil }, "the breach did not sever the replication session")
	connectionsBefore := connectionCount()

	_, err := h.fn.ProcessElement(ctx, h.we, h.bf, h.rt, 1, func(beam.EventTime, ChangeEvent) {})
	if err == nil {
		t.Fatal("ProcessElement succeeded while the slot was over its retention budget")
	}
	var breach *slotBreach
	if !errors.As(err, &breach) {
		t.Fatalf("error is not a *slotBreach, so callers cannot inspect it: %T %v", err, err)
	}
	if !strings.Contains(err.Error(), "test_slot") {
		t.Errorf("error does not name the slot: %v", err)
	}
	if got := connectionCount(); got != connectionsBefore {
		t.Errorf("a replication connection was reopened onto an over-budget slot: %d connections became %d",
			connectionsBefore, got)
	}
}

// TestTeardownStopsTheSlotMonitor guards against leaking the monitor's
// connection when the DoFn goes away.
func TestTeardownStopsTheSlotMonitor(t *testing.T) {
	h := newSDFHarness(t, true, keepalivePayload(100, false))

	q := newFakeSlotQuerier(fakeSlotResponse{retention: reserved(0)})
	h.fn.Options.MaxSlotLagBytes = 8 * MiB
	h.fn.Options.SlotLagCheckInterval = minSlotLagCheckInterval
	h.fn.newSlotQuerier = func(CDCOptions) (slotRetentionQuerier, error) { return q, nil }

	h.run(context.Background(), t)
	q.waitForChecks(t, 1)

	if err := h.fn.Teardown(); err != nil {
		t.Fatalf("Teardown returned an error: %v", err)
	}
	if !q.isClosed() {
		t.Error("Teardown left the slot monitor connection open")
	}
}

// TestMonitorStartFailureDoesNotFailThePipeline: the breaker is a safety net,
// and failing to hang the net must not stop the pipeline.
func TestMonitorStartFailureDoesNotFailThePipeline(t *testing.T) {
	h := newSDFHarness(t, true, keepalivePayload(100, false))

	h.fn.Options.MaxSlotLagBytes = 8 * MiB
	h.fn.newSlotQuerier = func(CDCOptions) (slotRetentionQuerier, error) {
		return nil, errors.New("could not connect")
	}

	// run fails the test if ProcessElement returns an error.
	h.run(context.Background(), t)
}

// --- Source-level invariants ---

// TestSlotRetentionQueryMeasuresFromRestartLSN protects the distinction between
// restart_lsn and confirmed_flush_lsn.
//
// confirmed_flush_lsn is ahead of restart_lsn, so measuring from it understates
// what the primary is retaining. That is the quantity the operator is trying to
// bound, so the substitution would quietly weaken the feature while every other
// test still passed.
func TestSlotRetentionQueryMeasuresFromRestartLSN(t *testing.T) {
	if !strings.Contains(slotRetentionQuery, "restart_lsn") {
		t.Error("the retention query does not reference restart_lsn")
	}
	if strings.Contains(slotRetentionQuery, "confirmed_flush_lsn") {
		t.Error("the retention query measures from confirmed_flush_lsn, which understates what the primary retains")
	}
}

// TestSlotRetentionQueryHandlesStandbys guards PostgreSQL 16+ logical decoding
// from a standby, where pg_current_wal_lsn raises "recovery is in progress".
func TestSlotRetentionQueryHandlesStandbys(t *testing.T) {
	for _, want := range []string{"pg_is_in_recovery", "pg_last_wal_receive_lsn"} {
		if !strings.Contains(slotRetentionQuery, want) {
			t.Errorf("the retention query does not call %s, so it fails on a standby", want)
		}
	}
}

// TestSlotRetentionQueryToleratesNullRestartLSN guards the NULL case, which
// occurs both before a slot reserves WAL and after it is invalidated.
func TestSlotRetentionQueryToleratesNullRestartLSN(t *testing.T) {
	if !strings.Contains(slotRetentionQuery, "COALESCE") {
		t.Error("the retention query does not COALESCE the diff; a NULL restart_lsn fails the scan")
	}
	if !strings.Contains(slotRetentionQuery, "restart_lsn IS NULL") {
		t.Error("the query does not report whether restart_lsn was NULL, so an unreserved slot " +
			"is indistinguishable from one retaining zero bytes")
	}
}

// TestSlotRetentionQueryIsSchemaQualified keeps the monitor consistent with the
// connector's CVE-2018-1058 posture.
func TestSlotRetentionQueryIsSchemaQualified(t *testing.T) {
	if !strings.Contains(slotRetentionQuery, "pg_catalog.pg_replication_slots") {
		t.Error("the retention query does not schema-qualify pg_replication_slots")
	}
}

// TestSlotRetentionQueryReportsTheXminHorizon covers the failure mode that is
// independent of WAL volume: a slot that stops advancing also pins
// catalog_xmin, and VACUUM cannot remove catalog tuples deleted by any later
// transaction. A database can therefore suffer catalog bloat and wraparound
// pressure from a stalled slot that has retained very little WAL.
func TestSlotRetentionQueryReportsTheXminHorizon(t *testing.T) {
	if !strings.Contains(slotRetentionQuery, "catalog_xmin") {
		t.Error("the retention query does not read catalog_xmin, so catalog bloat caused by a " +
			"stalled slot is invisible")
	}
	if !strings.Contains(slotRetentionQuery, "pg_catalog.age(") {
		t.Error("the retention query does not convert catalog_xmin to an age, so the value is a " +
			"raw transaction id rather than a distance that can be alerted on")
	}
}

// TestSlotMonitorQueriesAreBounded guards against the breaker failing open.
//
// A query with no timeout against a silently severed connection blocks forever:
// the monitor stops measuring, the failure counter never increments, and a
// breaker that is protecting nothing is indistinguishable from a healthy one.
//
// Asserted behaviourally. An earlier version of this test grepped the source
// for the timeout call, and mutation testing showed it still passed when the
// timeout was removed from one of the two query paths, because the string
// remained in the other.
func TestSlotMonitorQueriesAreBounded(t *testing.T) {
	previous := slotLagQueryTimeout
	slotLagQueryTimeout = 50 * time.Millisecond
	t.Cleanup(func() { slotLagQueryTimeout = previous })

	q := newBlockingSlotQuerier()
	m := newSlotMonitor(breakerOptions(8*MiB, SlotLagFailPipeline), q, nil)
	t.Cleanup(func() { _ = m.close() })

	done := make(chan slotCheckOutcome, 1)
	go func() { done <- m.check(context.Background()) }()

	select {
	case outcome := <-done:
		if outcome != slotCheckMeasureFailed {
			t.Errorf("a query that never returns was classified as %v, want %v", outcome, slotCheckMeasureFailed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("check did not return; a hung query leaves the breaker protecting nothing while appearing healthy")
	}

	if m.breached() != nil {
		t.Error("a timed-out measurement produced a breach")
	}
}

// blockingSlotQuerier never returns until its context is cancelled.
type blockingSlotQuerier struct {
	released chan struct{}
}

func newBlockingSlotQuerier() *blockingSlotQuerier {
	return &blockingSlotQuerier{released: make(chan struct{})}
}

func (b *blockingSlotQuerier) QuerySlotRetention(ctx context.Context, _ string) (slotRetention, error) {
	select {
	case <-ctx.Done():
		return slotRetention{}, ctx.Err()
	case <-b.released:
		return slotRetention{}, nil
	}
}

func (b *blockingSlotQuerier) MaxSlotWALKeepSizeBytes(ctx context.Context) (int64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-b.released:
		return -1, nil
	}
}

func (b *blockingSlotQuerier) Close() error {
	close(b.released)
	return nil
}

// waitFor polls until cond holds or the deadline passes.
func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(msg)
}

// TestProcessElementAbandonsTheInvocationWhenTheBreachLatchesMidRead covers the
// check inside the read loop, which is distinct from the one before the session
// is opened.
//
// The pre-session check only sees a breach that was already latched when the
// invocation began. An invocation that is midway through a long read when the
// monitor trips would, without the in-loop check, keep consuming frames until
// its checkpoint deadline and only report on the next invocation. That is the
// case this test pins.
//
// Mutation testing found this gap: deleting the in-loop check left every test
// passing, because TestProcessElementReportsALatchedBreach was satisfied by the
// pre-session check alone.
func TestProcessElementAbandonsTheInvocationWhenTheBreachLatchesMidRead(t *testing.T) {
	commitTime := time.Date(2026, 5, 4, 9, 30, 0, 0, time.UTC)

	// Three complete transactions. The breach is latched after the first
	// commit, so a correct invocation stops at that boundary and never reads
	// the other two.
	h := newSDFHarness(t, true,
		buildMockBeginPayload(3000, commitTime, 9),
		buildMockRelationPayload(42, "public", "orders", 'd', []ColumnDef{
			{Flags: 1, Name: "id", TypeOID: 20, TypeModifier: -1},
		}),
		buildMockInsertPayload(42, []string{"1"}),
		buildMockCommitPayload(3000, 3000, commitTime),

		buildMockBeginPayload(4000, commitTime, 10),
		buildMockInsertPayload(42, []string{"2"}),
		buildMockCommitPayload(4000, 4000, commitTime),

		buildMockBeginPayload(5000, commitTime, 11),
		buildMockInsertPayload(42, []string{"3"}),
		buildMockCommitPayload(5000, 5000, commitTime),
	)

	// The querier always fails, so the monitor's own goroutine never stores or
	// clears a latch. A failed measurement is not a breach, which keeps this
	// test's latch the only one in play and removes a race with the poller.
	q := newFakeSlotQuerier(fakeSlotResponse{err: errors.New("measurement unavailable")})
	h.fn.Options.MaxSlotLagBytes = 8 * MiB
	h.fn.Options.SlotLagPolicy = SlotLagFailPipeline
	h.fn.Options.SlotLagCheckInterval = minSlotLagCheckInterval
	h.fn.newSlotQuerier = func(CDCOptions) (slotRetentionQuerier, error) { return q, nil }

	// Latch after the fourth frame, which is the first transaction's Commit.
	// The next pass around the read loop is therefore at a boundary, where
	// abandoning the invocation is safe.
	latch := func() {
		h.fn.mu.Lock()
		monitor := h.fn.monitor
		h.fn.mu.Unlock()
		if monitor == nil {
			return
		}
		monitor.breach.Store(&slotBreach{
			SlotName:      "test_slot",
			RetainedBytes: int64(64 * MiB),
			BudgetBytes:   8 * MiB,
			WALStatus:     "reserved",
			At:            time.Now(),
		})
	}

	underlying := h.fn.Options.StreamFactory
	h.fn.Options.StreamFactory = func(ctx context.Context, opts CDCOptions) (ReplicationStream, error) {
		s, err := underlying(ctx, opts)
		if err != nil {
			return nil, err
		}
		return &breachMidInvocationStream{ReplicationStream: s, after: 4, latch: latch}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	emitted := 0
	_, err := h.fn.ProcessElement(ctx, h.we, h.bf, h.rt, 1, func(beam.EventTime, ChangeEvent) {
		emitted++
	})

	if err == nil {
		t.Fatal("ProcessElement returned successfully after the breaker tripped mid-invocation; " +
			"the invocation kept reading and pinning WAL on an over-budget slot")
	}
	var breach *slotBreach
	if !errors.As(err, &breach) {
		t.Fatalf("error is not a *slotBreach, so callers cannot inspect it: %T %v", err, err)
	}

	// The load-bearing assertion. Reporting the breach is not enough: the
	// invocation has to stop at the first boundary after it latched. Draining
	// the remaining transactions and reporting afterwards would look identical
	// to the caller but would keep reading well past the trip.
	if emitted != 1 {
		t.Errorf("the invocation emitted %d events, want 1; it continued reading past the boundary "+
			"at which the breaker tripped", emitted)
	}
}

// breachMidInvocationStream trips the circuit breaker once a chosen number of
// frames has been delivered, modelling the monitor latching while an invocation
// is already inside its read loop.
type breachMidInvocationStream struct {
	ReplicationStream

	after int
	latch func()

	mu     sync.Mutex
	frames int
}

func (s *breachMidInvocationStream) NextMessage(ctx context.Context) ([]byte, error) {
	payload, err := s.ReplicationStream.NextMessage(ctx)
	if err != nil {
		return payload, err
	}

	s.mu.Lock()
	s.frames++
	reached := s.frames == s.after
	s.mu.Unlock()

	if reached {
		s.latch()
	}
	return payload, nil
}
