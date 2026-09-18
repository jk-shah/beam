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
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
)

var (
	// cdcSlotRetainedBytes is the WAL the primary is holding for this slot,
	// measured from restart_lsn. This is the circuit breaker's control input
	// and the number that corresponds to disk consumption on the primary.
	cdcSlotRetainedBytes = beam.NewGauge("postgresio", "cdc_slot_retained_bytes")

	// cdcSlotWALStatus encodes pg_replication_slots.wal_status:
	// 0 reserved, 1 extended, 2 unreserved, 3 lost, -1 unknown.
	cdcSlotWALStatus = beam.NewGauge("postgresio", "cdc_slot_wal_status")

	// cdcSlotXminHorizonAge is how many transactions have elapsed since the
	// oldest catalog transaction this slot forces the server to retain.
	//
	// WAL volume is not the only cost of a stalled slot. A logical slot also
	// pins catalog_xmin, and VACUUM cannot remove catalog tuples deleted by any
	// later transaction. A slot that stops advancing therefore produces catalog
	// bloat and contributes to transaction ID wraparound pressure, on a clock
	// that is independent of how much WAL has been written. A low-traffic
	// database can be in trouble here while retained bytes still look fine.
	cdcSlotXminHorizonAge = beam.NewGauge("postgresio", "cdc_slot_xmin_horizon_age")

	// cdcSlotLagCheckFailures counts monitoring queries that failed.
	//
	// A non-zero value means the circuit breaker is not protecting anything.
	// A breaker that cannot measure looks exactly like a healthy pipeline
	// unless its own failure is visible, so this is a first-class signal
	// rather than a log line.
	cdcSlotLagCheckFailures = beam.NewCounter("postgresio", "cdc_slot_lag_check_failures")

	// cdcSlotLagBreaches counts threshold crossings.
	cdcSlotLagBreaches = beam.NewCounter("postgresio", "cdc_slot_lag_breaches")
)

// Byte-size helpers for expressing a retention budget.
const (
	MiB uint64 = 1 << 20
	GiB uint64 = 1 << 30
)

const (
	// DefaultSlotLagCheckInterval is how often retention is measured when
	// CDCOptions.SlotLagCheckInterval is unset.
	DefaultSlotLagCheckInterval = 30 * time.Second

	// minSlotLagCheckInterval clamps the poll rate. The query is cheap but not
	// free, and a misconfigured sub-second interval would put avoidable load on
	// a primary that may already be in trouble.
	minSlotLagCheckInterval = time.Second

	// slotMonitorCloseTimeout bounds teardown's wait for the monitor goroutine.
	slotMonitorCloseTimeout = 15 * time.Second
)

// slotLagQueryTimeout bounds every call the monitor makes into its querier.
//
// Without it a silently severed connection leaves the query blocked forever:
// the monitor stops measuring, the failure counter never increments, and the
// breaker fails open while continuing to look healthy.
//
// It is applied by the monitor rather than inside a querier implementation, so
// the bound holds for any implementation rather than depending on each one
// remembering to impose it.
//
// A variable rather than a constant so tests can exercise expiry without
// waiting ten seconds.
var slotLagQueryTimeout = 10 * time.Second

// SlotLagPolicy selects what happens when retention exceeds the budget.
type SlotLagPolicy int

const (
	// SlotLagFailPipeline latches a breach and closes the replication session.
	// The slot is left intact, so the pipeline resumes once the backlog is
	// addressed. This is the default.
	SlotLagFailPipeline SlotLagPolicy = iota

	// SlotLagLogOnly reports and never intervenes. For environments where
	// max_slot_wal_keep_size already bounds retention server-side.
	SlotLagLogOnly
)

// String implements fmt.Stringer.
func (p SlotLagPolicy) String() string {
	switch p {
	case SlotLagLogOnly:
		return "LogOnly"
	case SlotLagFailPipeline:
		return "FailPipeline"
	default:
		return fmt.Sprintf("SlotLagPolicy(%d)", int(p))
	}
}

// slotRetentionQuery measures what the primary is retaining for one slot.
//
// restart_lsn, not confirmed_flush_lsn, is the retention anchor: it is the
// oldest LSN the slot still requires, and it trails confirmed_flush_lsn.
// Measuring from confirmed_flush_lsn would systematically understate what the
// database is holding, which is the quantity the operator is trying to bound.
//
// pg_current_wal_lsn raises "recovery is in progress" on a standby, and
// PostgreSQL 16 and later support logical decoding from standbys, so the
// current position is selected according to recovery state.
//
// restart_lsn is NULL when the slot has never reserved WAL and again once the
// slot is invalidated. COALESCE keeps the scan total, and the flag is returned
// separately so "reserved nothing" stays distinguishable from "retaining zero".
const slotRetentionQuery = `
SELECT
    COALESCE(
        pg_catalog.pg_wal_lsn_diff(
            CASE WHEN pg_catalog.pg_is_in_recovery()
                 THEN pg_catalog.pg_last_wal_receive_lsn()
                 ELSE pg_catalog.pg_current_wal_lsn()
            END,
            restart_lsn),
        0)::bigint AS retained_bytes,
    restart_lsn IS NULL AS restart_lsn_unset,
    COALESCE(wal_status, 'unknown') AS wal_status,
    active,
    COALESCE(pg_catalog.age(catalog_xmin), 0)::bigint AS xmin_horizon_age
FROM pg_catalog.pg_replication_slots
WHERE slot_name = $1`

// slotRetention is one measurement.
type slotRetention struct {
	RetainedBytes int64
	// XminHorizonAge is transactions elapsed since catalog_xmin, or zero when
	// the slot pins no catalog transaction. Reported but not enforced: the
	// budget is expressed in bytes, and a sensible transaction-age threshold is
	// installation-specific.
	XminHorizonAge  int64
	RestartLSNUnset bool
	WALStatus       string
	Active          bool
}

// errSlotNotFound reports that the configured slot does not exist.
var errSlotNotFound = errors.New("replication slot not found")

// slotRetentionQuerier reads slot retention. It exists so tests can drive the
// monitor without a database, in the same way StreamFactory and DialFunc are
// used elsewhere in this package.
type slotRetentionQuerier interface {
	QuerySlotRetention(ctx context.Context, slotName string) (slotRetention, error)
	// MaxSlotWALKeepSizeBytes reports the server's max_slot_wal_keep_size in
	// bytes, or -1 when unlimited.
	MaxSlotWALKeepSizeBytes(ctx context.Context) (int64, error)
	Close() error
}

// sqlSlotQuerier is the production slotRetentionQuerier.
type sqlSlotQuerier struct {
	db *sql.DB
}

func newSQLSlotQuerier(opts CDCOptions) (*sqlSlotQuerier, error) {
	sslMode := opts.SSLMode
	if sslMode == "" {
		sslMode = DefaultSSLMode
	}
	// Reuses the sink's DSN builder so this connection inherits the same value
	// escaping and the same pinned search_path.
	dsn := buildWriteDSN(opts.Host, opts.Port, opts.Database, opts.Username, opts.ResolvePassword(), sslMode)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("postgresio: failed to open slot monitor connection: %w", err)
	}
	// One connection, one query per interval. The monitor must not contribute
	// meaningfully to load on a primary that may already be under pressure.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(30 * time.Minute)
	return &sqlSlotQuerier{db: db}, nil
}

func (q *sqlSlotQuerier) QuerySlotRetention(ctx context.Context, slotName string) (slotRetention, error) {
	var out slotRetention
	row := q.db.QueryRowContext(ctx, slotRetentionQuery, slotName)
	if err := row.Scan(&out.RetainedBytes, &out.RestartLSNUnset, &out.WALStatus, &out.Active, &out.XminHorizonAge); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return slotRetention{}, errSlotNotFound
		}
		return slotRetention{}, err
	}
	return out, nil
}

func (q *sqlSlotQuerier) MaxSlotWALKeepSizeBytes(ctx context.Context) (int64, error) {
	// Read the value in bytes directly rather than parsing the human-readable
	// form that SHOW returns.
	var setting string
	err := q.db.QueryRowContext(ctx,
		`SELECT COALESCE(setting, '-1') FROM pg_catalog.pg_settings WHERE name = 'max_slot_wal_keep_size'`).
		Scan(&setting)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return -1, nil
		}
		return 0, err
	}
	// pg_settings reports max_slot_wal_keep_size in megabytes.
	var mb int64
	if _, err := fmt.Sscanf(strings.TrimSpace(setting), "%d", &mb); err != nil {
		return 0, fmt.Errorf("postgresio: unparsable max_slot_wal_keep_size %q: %w", setting, err)
	}
	if mb < 0 {
		return -1, nil
	}
	return mb * int64(MiB), nil
}

func (q *sqlSlotQuerier) Close() error {
	return q.db.Close()
}

// slotCheckOutcome classifies the result of one measurement.
//
// Several outcomes correctly result in no breach, and returning them makes the
// distinction observable. Without it a test cannot tell which guard declined to
// breach, and a guard that is removed entirely looks identical to one that
// fired.
type slotCheckOutcome int

const (
	// slotCheckMeasureFailed means the measurement did not complete. The
	// breaker is protecting nothing until this clears.
	slotCheckMeasureFailed slotCheckOutcome = iota
	// slotCheckLost means wal_status is 'lost'; the slot is unrecoverable.
	slotCheckLost
	// slotCheckUnreserved means the slot has reserved no WAL.
	slotCheckUnreserved
	// slotCheckUnderBudget means retention is within the configured budget.
	slotCheckUnderBudget
	// slotCheckBreached means the budget was exceeded.
	slotCheckBreached
)

// String implements fmt.Stringer to keep test failures readable.
func (o slotCheckOutcome) String() string {
	switch o {
	case slotCheckMeasureFailed:
		return "MeasureFailed"
	case slotCheckLost:
		return "Lost"
	case slotCheckUnreserved:
		return "Unreserved"
	case slotCheckUnderBudget:
		return "UnderBudget"
	case slotCheckBreached:
		return "Breached"
	default:
		return fmt.Sprintf("slotCheckOutcome(%d)", int(o))
	}
}

// slotBreach describes a retention budget breach.
type slotBreach struct {
	SlotName      string
	RetainedBytes int64
	BudgetBytes   uint64
	WALStatus     string
	At            time.Time
}

// Error implements error so the breach can be returned directly from
// ProcessElement without reformatting at the call site.
func (b *slotBreach) Error() string {
	if b.WALStatus == "lost" {
		return fmt.Sprintf(
			"postgresio: replication slot %q has wal_status=lost (observed %s): the PostgreSQL server has discarded WAL required by the slot and it cannot be resumed without recreating the slot and taking a new snapshot",
			b.SlotName, b.At.Format(time.RFC3339))
	}
	return fmt.Sprintf(
		"postgresio: replication slot %q is retaining %d bytes of WAL, over the configured budget of %d bytes (wal_status=%s, observed %s). "+
			"The pipeline is not acknowledging fast enough to bound WAL growth on the primary. "+
			"The slot has been left in place so the pipeline can resume once the backlog clears; "+
			"set max_slot_wal_keep_size on the server as a backstop that does not depend on this client",
		b.SlotName, b.RetainedBytes, b.BudgetBytes, b.WALStatus, b.At.Format(time.RFC3339))
}

// slotMonitor measures WAL retention on a connection independent of the
// replication stream.
//
// A separate connection is required rather than convenient. The in-process
// figure derived from serverWALEnd only advances when a keepalive is read off
// the replication socket, and reads happen solely inside ProcessElement. When
// the pipeline stalls nobody reads, that value freezes, and the computed lag
// stops growing in exactly the situation the breaker exists to catch.
type slotMonitor struct {
	slotName string
	budget   uint64
	policy   SlotLagPolicy
	interval time.Duration
	querier  slotRetentionQuerier

	// onBreach severs the replication session. It is the only corrective
	// action available that does not depend on ProcessElement being scheduled.
	onBreach func()

	breach atomic.Pointer[slotBreach]

	cancel context.CancelFunc
	done   chan struct{}

	closeOnce sync.Once
}

func newSlotMonitor(opts CDCOptions, querier slotRetentionQuerier, onBreach func()) *slotMonitor {
	interval := opts.SlotLagCheckInterval
	if interval <= 0 {
		interval = DefaultSlotLagCheckInterval
	}
	if interval < minSlotLagCheckInterval {
		interval = minSlotLagCheckInterval
	}
	return &slotMonitor{
		slotName: opts.SlotName,
		budget:   opts.MaxSlotLagBytes,
		policy:   opts.SlotLagPolicy,
		interval: interval,
		querier:  querier,
		onBreach: onBreach,
		done:     make(chan struct{}),
	}
}

// start launches the polling loop.
func (m *slotMonitor) start(ctx context.Context) {
	monitorCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	m.cancel = cancel
	go m.run(monitorCtx)
}

func (m *slotMonitor) run(ctx context.Context) {
	defer close(m.done)

	m.warnIfServerLimitIsTighter(ctx)

	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	// Measure immediately. Waiting a full interval would leave a window in
	// which an already-breached slot goes unreported.
	_ = m.check(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = m.check(ctx)
		}
	}
}

// warnIfServerLimitIsTighter reports a budget the server will never let the
// breaker act on.
//
// If max_slot_wal_keep_size is smaller than the configured budget, the server
// invalidates the slot first and the breaker never fires. This is a warning
// rather than a construction error because the setting is runtime-mutable, so a
// value read once is not a sound basis for refusing to build the pipeline.
func (m *slotMonitor) warnIfServerLimitIsTighter(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, slotLagQueryTimeout)
	defer cancel()

	serverLimit, err := m.querier.MaxSlotWALKeepSizeBytes(ctx)
	if err != nil {
		log.Warnf(ctx, "postgresio: could not read max_slot_wal_keep_size: %v", err)
		return
	}
	if serverLimit >= 0 && uint64(serverLimit) < m.budget {
		log.Errorf(ctx,
			"postgresio: max_slot_wal_keep_size is %d bytes, below the configured slot lag budget of %d bytes. "+
				"The server will invalidate slot %q before the circuit breaker can act; lower the budget or raise the server setting",
			serverLimit, m.budget, m.slotName)
	}
}

// check performs one measurement.
//
// A failed measurement never breaches. The breaker may only act on a successful
// reading, because a monitoring bug that failed pipelines would be a worse
// outage than the condition it is meant to prevent.
func (m *slotMonitor) check(parent context.Context) slotCheckOutcome {
	ctx, cancel := context.WithTimeout(parent, slotLagQueryTimeout)
	defer cancel()

	retention, err := m.querier.QuerySlotRetention(ctx, m.slotName)
	if err != nil {
		if parent.Err() != nil {
			// Shutting down, not a measurement failure.
			return slotCheckMeasureFailed
		}
		cdcSlotLagCheckFailures.Inc(ctx, 1)
		log.Errorf(ctx, "postgresio: slot retention check failed for %q, the WAL circuit breaker is not protecting the database: %v",
			m.slotName, err)
		return slotCheckMeasureFailed
	}

	cdcSlotRetainedBytes.Set(ctx, retention.RetainedBytes)
	cdcSlotWALStatus.Set(ctx, walStatusCode(retention.WALStatus))
	cdcSlotXminHorizonAge.Set(ctx, retention.XminHorizonAge)

	// Checked before the unreserved case: invalidation also nulls restart_lsn,
	// so a lost slot presents as unreserved and would otherwise be reported as
	// the benign condition.
	if retention.WALStatus == "lost" {
		// Already unrecoverable. Reported distinctly: no budget comparison is
		// meaningful, and unlike a breach this cannot clear by waiting.
		cdcSlotLagCheckFailures.Inc(ctx, 1)
		log.Errorf(ctx, "postgresio: replication slot %q has wal_status=lost; the server has discarded WAL the slot required and it cannot be resumed",
			m.slotName)
		breach := &slotBreach{
			SlotName:      m.slotName,
			RetainedBytes: retention.RetainedBytes,
			BudgetBytes:   m.budget,
			WALStatus:     "lost",
			At:            time.Now(),
		}
		m.breach.Store(breach)
		if m.onBreach != nil {
			m.onBreach()
		}
		return slotCheckLost
	}

	// The slot exists but has reserved nothing, so it is retaining nothing.
	// The guard does not trust the byte count in this state: it is the
	// COALESCE default rather than a measurement, and treating a defaulted
	// zero as evidence of health only holds while the query and the scan
	// agree.
	if retention.RestartLSNUnset {
		return slotCheckUnreserved
	}

	if m.budget == 0 || retention.RetainedBytes < 0 || uint64(retention.RetainedBytes) <= m.budget {
		// Under budget. Release any previous latch so a transient backlog that
		// has since drained does not fail the pipeline forever.
		// Never clear a latched breach if the slot is permanently lost.
		if cur := m.breach.Load(); cur == nil || cur.WALStatus != "lost" {
			m.breach.Store(nil)
		}
		return slotCheckUnderBudget
	}

	breach := &slotBreach{
		SlotName:      m.slotName,
		RetainedBytes: retention.RetainedBytes,
		BudgetBytes:   m.budget,
		WALStatus:     retention.WALStatus,
		At:            time.Now(),
	}
	cdcSlotLagBreaches.Inc(ctx, 1)
	log.Errorf(ctx, "%v", breach)

	if m.policy == SlotLagLogOnly {
		return slotCheckBreached
	}

	m.breach.Store(breach)
	if m.onBreach != nil {
		// Severing the replication session stops the keepalives and releases
		// the walsender, which marks the slot inactive server-side. That does
		// not reclaim WAL, but it is the only action here that does not depend
		// on ProcessElement being scheduled, and a pipeline that is drained or
		// suspended is precisely the case where it is not.
		m.onBreach()
	}
	return slotCheckBreached
}

// breached returns the latched breach, or nil.
func (m *slotMonitor) breached() *slotBreach {
	return m.breach.Load()
}

// close stops the monitor and releases its connection.
func (m *slotMonitor) close() error {
	var err error
	m.closeOnce.Do(func() {
		if m.cancel != nil {
			m.cancel()
			select {
			case <-m.done:
			case <-time.After(slotMonitorCloseTimeout):
				// The poll is bounded by slotLagQueryTimeout, so reaching this is
				// unexpected. Closing the connection anyway is better than blocking
				// teardown indefinitely.
			}
		}
		if m.querier != nil {
			err = m.querier.Close()
		}
	})
	return err
}

// walStatusCode maps wal_status to a gauge value, since Beam gauges are numeric.
func walStatusCode(status string) int64 {
	switch status {
	case "reserved":
		return 0
	case "extended":
		return 1
	case "unreserved":
		return 2
	case "lost":
		return 3
	default:
		return -1
	}
}
