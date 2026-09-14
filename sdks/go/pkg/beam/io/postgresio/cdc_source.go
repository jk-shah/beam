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
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
)

var (
	cdcProcessedRecords = beam.NewCounter("postgresio", "cdc_processed_records")
	cdcFilteredRecords  = beam.NewCounter("postgresio", "cdc_filtered_origin_records")

	// cdcConfirmedFlushLSN is the LSN this worker has reported to the server as
	// flushed. The server will not recycle WAL beyond it. A flat line on this
	// gauge while cdcServerWALEnd climbs is the signature of the failure mode
	// that fills the primary's WAL volume.
	cdcConfirmedFlushLSN = beam.NewGauge("postgresio", "cdc_confirmed_flush_lsn")

	// cdcServerWALEnd is the primary's current WAL write position, learned from
	// server keepalives.
	//
	// It only advances when ProcessElement reads a frame off the replication
	// socket. A pipeline that is stalled, drained or suspended reads nothing, so
	// this gauge freezes. Do not use it, or any quantity derived from it, to
	// detect a stall: cdc_slot_retained_bytes is measured independently and is
	// the signal that keeps moving. See cdc_slot_monitor.go.
	cdcServerWALEnd = beam.NewGauge("postgresio", "cdc_server_wal_end_lsn")
)

const (
	// bundleFinalizationTimeout bounds how long the runner waits for the
	// acknowledgment callback to run after a bundle's output is durable.
	bundleFinalizationTimeout = 60 * time.Second

	// maxEventsPerInvocation caps how many change events one ProcessElement
	// call emits before returning.
	//
	// Returning is what lets the bundle finalize, and bundle finalization is
	// what advances confirmed_flush_lsn, so this constant is the upper bound on
	// how far acknowledgment can fall behind measured in records.
	maxEventsPerInvocation = 10000

	// DefaultCheckpointInterval caps how long one ProcessElement call waits
	// for data before returning, when CDCOptions.CheckpointInterval is unset.
	//
	// It is the upper bound on acknowledgment latency on an idle database,
	// where no volume of events would otherwise trigger a return.
	DefaultCheckpointInterval = 5 * time.Second

	// resumeDelay is the delay suggested to the runner when the stream had data
	// and more is expected immediately.
	resumeDelay = 0 * time.Second

	// reconnectDelay is the delay suggested after the replication connection
	// ends, before a new one is opened.
	reconnectDelay = 5 * time.Second

	// sessionCloseTimeout bounds how long session teardown waits for the
	// keepalive goroutine to observe cancellation.
	sessionCloseTimeout = 15 * time.Second
)

// transactionStallTimeout bounds how long the stream may be silent while a
// transaction is open.
//
// A transaction cannot be checkpointed partway through, so the checkpoint
// deadline is advisory while one is open and the invocation keeps reading until
// the Commit frame arrives. This is a stall detector rather than a size limit:
// every frame received resets it, so a transaction of any size completes as
// long as it keeps arriving, while a connection that stops delivering is
// dropped and the bundle retried from the last acknowledged boundary.
//
// A variable rather than a constant so tests can exercise the expiry without
// waiting two minutes.
var transactionStallTimeout = 2 * time.Minute

func init() {
	beam.RegisterDoFn(&cdcSourceFn{})
}

// cdcSourceFn reads a PostgreSQL logical replication slot as an unbounded
// splittable DoFn.
//
// The restriction is the half-open LSN range [resume, MaxInt64). A position is
// claimed for each distinct commit LSN before the transaction's events are
// emitted, so a checkpoint always lands on a transaction boundary.
//
// ProcessElement deliberately returns often. A bundle cannot finalize until
// ProcessElement returns, the BundleFinalization callback cannot run until the
// bundle finalizes, and the replication slot is not acknowledged until that
// callback runs. An implementation that loops forever therefore pins
// confirmed_flush_lsn at its starting value and the primary retains WAL without
// bound until its volume fills.
type cdcSourceFn struct {
	// Options must be exported. A structural DoFn is shipped to workers as JSON
	// (graphx.encodeFn calls jsonx.Marshal on the receiver), and JSON drops
	// unexported fields, so an unexported config field arrives zero-valued on
	// every runner that does not execute in the submitting process.
	Options CDCOptions `json:"options"`

	// session holds the replication connection and its keepalive loop. It is
	// runtime state, deliberately unexported so it is not serialized, and it
	// outlives a single ProcessElement call: a self-checkpointing source
	// returns frequently, and reopening a replication connection on every
	// return would make connection churn proportional to the checkpoint rate.
	session *cdcSession
	mu      sync.Mutex

	// monitor measures WAL retention on a connection independent of the
	// replication stream. It is started lazily, on the first session this
	// worker actually opens, so a worker the runner initialized but never
	// scheduled does not open a connection or poll the primary.
	//
	// It is owned here rather than by cdcSession because it must outlive one.
	// A session that is dropped and not recreated is the stalled case, and a
	// monitor tied to the session would stop measuring at the moment the slot
	// is orphaned and still retaining WAL.
	monitor *slotMonitor

	// newSlotQuerier is a test seam. Nil means the production SQL querier.
	newSlotQuerier func(CDCOptions) (slotRetentionQuerier, error)

	// newPreflightQuerier is a test seam. Nil means the production SQL querier.
	newPreflightQuerier func(CDCOptions) (preflightQuerier, error)

	// preflightDone records that preflight has already passed on this worker,
	// so a session reopened after a dropped connection does not re-query the
	// catalog. A failed preflight deliberately does not set it: an operator who
	// corrects the setting should see the retry succeed.
	preflightDone bool
}

func newCDCSourceFn(opts CDCOptions) *cdcSourceFn {
	return &cdcSourceFn{
		Options: opts,
	}
}

// --- Splittable DoFn: restriction ---

// CreateInitialRestriction returns the LSN range this source will consume.
//
// The end is math.MaxInt64, which offsetrange.GrowableTracker treats as
// "unbounded". That is what makes the resulting PCollection unbounded even
// though the impulse that drives it is bounded.
func (fn *cdcSourceFn) CreateInitialRestriction(_ int) offsetrange.Restriction {
	start := int64(0)
	if fn.Options.StartLSN <= math.MaxInt64 {
		start = int64(fn.Options.StartLSN)
	}
	return offsetrange.Restriction{Start: start, End: math.MaxInt64}
}

// SplitRestriction returns the restriction unchanged.
//
// A replication slot admits exactly one connection, so the LSN range cannot be
// divided across workers. Parallelism downstream is obtained by keying on the
// primary key, not by splitting the source.
func (fn *cdcSourceFn) SplitRestriction(_ int, rest offsetrange.Restriction) []offsetrange.Restriction {
	return []offsetrange.Restriction{rest}
}

// RestrictionSize reports the number of WAL bytes between the resume position
// and the server's current write position, which is the backlog this source
// still has to read.
func (fn *cdcSourceFn) RestrictionSize(_ int, rest offsetrange.Restriction) float64 {
	end := fn.estimateWALEnd(rest.Start)
	if end <= rest.Start {
		return 0
	}
	return float64(end - rest.Start)
}

// CreateTracker returns a growable tracker over the LSN range.
func (fn *cdcSourceFn) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
	tracker, err := offsetrange.NewGrowableTracker(rest, &walEndEstimator{fn: fn})
	if err != nil {
		// NewGrowableTracker only rejects a nil estimator, which cannot happen
		// here. Fall back to the bounded tracker rather than panicking.
		return sdf.NewLockRTracker(offsetrange.NewTracker(rest))
	}
	return sdf.NewLockRTracker(tracker)
}

// TruncateRestriction discards the remaining range so a drain terminates.
//
// The alternative, truncating to the server's current WAL end, would make a
// drain wait for a position the source may never reach if writes continue.
func (fn *cdcSourceFn) TruncateRestriction(_ *sdf.LockRTracker, _ int) offsetrange.Restriction {
	return offsetrange.Restriction{}
}

// walEndEstimator reports the primary's current WAL write position so the
// runner can size the backlog of an otherwise unbounded range.
type walEndEstimator struct {
	fn *cdcSourceFn
}

// Estimate implements offsetrange.RangeEndEstimator.
func (e *walEndEstimator) Estimate() int64 {
	return e.fn.estimateWALEnd(0)
}

func (fn *cdcSourceFn) estimateWALEnd(floor int64) int64 {
	fn.mu.Lock()
	session := fn.session
	fn.mu.Unlock()
	if session == nil {
		return floor
	}
	end := session.serverWALEnd.Load()
	if end > math.MaxInt64 {
		return math.MaxInt64
	}
	if int64(end) < floor {
		return floor
	}
	return int64(end)
}

// --- Splittable DoFn: watermark ---

// CreateWatermarkEstimator returns a manually advanced watermark estimator.
//
// The source advances it from the commit timestamp of the transactions it
// reads, and to wall-clock time when the server reports it is caught up. An
// estimator is required for downstream fixed or sliding windows to close.
func (fn *cdcSourceFn) CreateWatermarkEstimator() *sdf.ManualWatermarkEstimator {
	return &sdf.ManualWatermarkEstimator{}
}

// --- DoFn lifecycle ---

// Setup validates the configuration once per worker.
func (fn *cdcSourceFn) Setup(_ context.Context) error {
	if err := fn.Options.Validate(); err != nil {
		return fmt.Errorf("postgresio: invalid CDC options: %w", err)
	}
	return nil
}

// Teardown closes the replication connection and stops the keepalive loop and
// the slot monitor.
func (fn *cdcSourceFn) Teardown() error {
	fn.mu.Lock()
	session := fn.session
	fn.session = nil
	monitor := fn.monitor
	fn.monitor = nil
	fn.mu.Unlock()

	var err error
	if session != nil {
		err = session.close()
	}
	if monitor != nil {
		if mErr := monitor.close(); mErr != nil && err == nil {
			err = mErr
		}
	}
	return err
}

// startMonitorLocked starts the slot monitor unless monitoring is disabled and
// it is not already running. fn.mu must be held.
//
// The monitor runs independently of the circuit breaker. Enforcement is gated
// separately, on MaxSlotLagBytes, inside check: with no budget every
// measurement classifies as under budget and nothing is ever latched. Starting
// it regardless is what makes cdc_slot_retained_bytes present by default, and
// that metric is the only accurate view of what the slot is retaining.
//
// A failure to start the monitor is logged rather than returned. Monitoring is
// a safety net; refusing to run the pipeline because the net could not be hung
// would make the protective feature an availability risk in its own right.
func (fn *cdcSourceFn) startMonitorLocked(ctx context.Context) {
	if fn.Options.DisableSlotMonitoring || fn.monitor != nil {
		return
	}

	newQuerier := fn.newSlotQuerier
	if newQuerier == nil {
		newQuerier = func(opts CDCOptions) (slotRetentionQuerier, error) {
			return newSQLSlotQuerier(opts)
		}
	}
	querier, err := newQuerier(fn.Options)
	if err != nil {
		cdcSlotLagCheckFailures.Inc(ctx, 1)
		log.Errorf(ctx, "postgresio: could not start the WAL retention circuit breaker for slot %q, "+
			"the database is not protected against unbounded WAL growth by this pipeline: %v",
			fn.Options.SlotName, err)
		return
	}

	monitor := newSlotMonitor(fn.Options, querier, func() {
		// Sever the replication session. This runs on the monitor goroutine,
		// so it must not take fn.mu in a way that can deadlock against a
		// caller already holding it; dropSession takes the lock itself and the
		// monitor never holds it.
		if session := fn.currentSession(); session != nil {
			fn.dropSession(session)
		}
	})
	monitor.start(ctx)
	fn.monitor = monitor
}

// --- Splittable DoFn: processing ---

// ProcessElement consumes replication messages until it has emitted a bounded
// amount of work or waited a bounded amount of time, then returns so the bundle
// can finalize and the slot can be acknowledged.
//
// Parameter order is fixed by the Go SDK:
// context, watermark estimator, bundle finalization, restriction tracker,
// element, emitters.
func (fn *cdcSourceFn) ProcessElement(
	ctx context.Context,
	we *sdf.ManualWatermarkEstimator,
	bf beam.BundleFinalization,
	rt *sdf.LockRTracker,
	_ int,
	emit func(beam.EventTime, ChangeEvent),
) (sdf.ProcessContinuation, error) {

	rest, ok := rt.GetRestriction().(offsetrange.Restriction)
	if !ok {
		return sdf.StopProcessing(), fmt.Errorf("postgresio: unexpected restriction type %T", rt.GetRestriction())
	}

	// A breach latched by the monitor is reported before any further reading.
	// The monitor has already severed the session; continuing would reopen the
	// replication connection and resume pinning WAL on a slot that is over
	// budget.
	if breach := fn.latchedBreach(); breach != nil {
		return sdf.StopProcessing(), breach
	}

	session, err := fn.ensureSession(ctx, uint64(rest.Start))
	if err != nil {
		return sdf.StopProcessing(), fmt.Errorf("postgresio: failed to initialize replication stream: %w", err)
	}

	// ackCandidate is the highest LSN this invocation may acknowledge once its
	// output is durable. It is registered with the bundle finalization callback
	// below, never applied directly, so acknowledgment can only ever follow a
	// durable commit of the corresponding records.
	var ackCandidate uint64

	// pendingAck holds the highest LSN emitted so far, which may belong to a
	// transaction whose Commit frame has not arrived. It is promoted into
	// ackCandidate only at a transaction boundary.
	//
	// Every change in a pgoutput transaction carries the LSN of its Begin
	// record, so a half-received transaction's events already carry the commit
	// LSN. Acknowledging that LSN would tell the server the transaction is
	// consumed while part of it is still unread, and the server does not resend
	// acknowledged data.
	var pendingAck uint64

	emitted := 0
	checkpointInterval := fn.Options.CheckpointInterval
	if checkpointInterval <= 0 {
		checkpointInterval = DefaultCheckpointInterval
	}
	deadline := time.Now().Add(checkpointInterval)
	continuation := sdf.ResumeProcessingIn(resumeDelay)

	stallDeadline := time.Now().Add(transactionStallTimeout)

readLoop:
	for {
		// Returning is only safe on a transaction boundary. The restriction
		// tracker addresses positions as a single int64 LSN and cannot express
		// a position partway through a transaction, so a residual restriction
		// created mid-transaction would resume at the next commit LSN and the
		// unread remainder of the interrupted transaction would be skipped.
		atBoundary := !session.parser.HasBufferedTransaction()
		if atBoundary && pendingAck > ackCandidate {
			ackCandidate = pendingAck
		}

		if ctx.Err() != nil {
			continuation = sdf.StopProcessing()
			break
		}

		// Only at a boundary: abandoning mid-transaction would discard the
		// unread remainder, and the breach is reported on the next invocation
		// regardless.
		if atBoundary {
			if breach := fn.latchedBreach(); breach != nil {
				return sdf.StopProcessing(), breach
			}
		}

		readDeadline := deadline
		if atBoundary {
			if emitted >= maxEventsPerInvocation || !time.Now().Before(deadline) {
				break
			}
		} else {
			readDeadline = stallDeadline
		}

		payload, err := session.nextMessage(ctx, readDeadline)
		if err != nil {
			switch {
			case ctx.Err() != nil:
				continuation = sdf.StopProcessing()
				break readLoop
			case isIdleTimeout(err) && atBoundary:
				// No data within this invocation's budget. Returning here is
				// what bounds acknowledgment latency on a quiet database.
				break readLoop
			case isIdleTimeout(err):
				// Silence while a transaction is open, for longer than any
				// healthy server takes to finish sending one. Dropping the
				// connection is the only recovery: the invocation cannot
				// return here without losing the transaction's tail, and
				// nothing was acknowledged, so the server resends it in full.
				fn.dropSession(session)
				return sdf.StopProcessing(), fmt.Errorf("postgresio: replication stream silent for %s inside an open transaction: %w", transactionStallTimeout, err)
			case errors.Is(err, io.EOF):
				// The server ended the copy stream. Drop the session so the
				// next invocation reconnects from the resume position.
				fn.dropSession(session)
				continuation = sdf.ResumeProcessingIn(reconnectDelay)
				break readLoop
			default:
				fn.dropSession(session)
				log.Errorf(ctx, "postgresio: error reading replication stream: %v", err)
				return sdf.StopProcessing(), err
			}
		}

		// The stream is making progress, so restart the stall budget. This is
		// what lets a transaction larger than the checkpoint interval complete.
		stallDeadline = time.Now().Add(transactionStallTimeout)

		if len(payload) == 0 {
			continue
		}

		// Server keepalive ('k').
		if payload[0] == 'k' {
			walEnd, _, replyRequested, err := ParseKeepAlive(payload)
			if err != nil {
				continue
			}
			session.observeServerWALEnd(walEnd)
			session.observeReceived(walEnd)

			// A keepalive says the server has sent everything through walEnd.
			// If the parser holds no partially received transaction then every
			// record up to walEnd has already been emitted, so walEnd is a
			// legitimate acknowledgment candidate. Without this, a slot whose
			// publication covers only quiet tables never advances and retains
			// WAL for the whole database's write traffic.
			if !session.parser.HasBufferedTransaction() {
				if walEnd > ackCandidate {
					ackCandidate = walEnd
				}
				// Caught up: no unread data exists, so event time has reached
				// the present.
				advanceWatermark(we, time.Now())
			}

			if replyRequested {
				session.sendStatus(ctx)
			}
			continue
		}

		events, err := session.parser.ParseMessages(payload)
		if err != nil {
			log.Warnf(ctx, "postgresio: failed to parse pgoutput message: %v", err)
			continue
		}

		for _, event := range events {
			if event == nil {
				continue
			}
			if fn.Options.OriginFilter == "none" && event.Origin != "" {
				cdcFilteredRecords.Inc(ctx, 1)
				continue
			}

			// Claim the commit LSN once per transaction. Every change in a
			// pgoutput transaction carries the LSN of its BEGIN record, so
			// successive events within one transaction repeat a position that
			// has already been claimed and must not be claimed again.
			if event.LSN > session.claimedLSN {
				if event.LSN > math.MaxInt64 {
					return sdf.StopProcessing(), fmt.Errorf("postgresio: LSN %d exceeds the representable restriction range", event.LSN)
				}
				if !rt.TryClaim(int64(event.LSN)) {
					if err := rt.GetError(); err != nil {
						return sdf.StopProcessing(), err
					}
					// The runner split the restriction away from this worker.
					continuation = sdf.StopProcessing()
					break readLoop
				}
				session.claimedLSN = event.LSN
			}

			session.observeReceived(event.LSN)
			cdcProcessedRecords.Inc(ctx, 1)

			eventTime := event.CommitTime
			if eventTime.IsZero() {
				eventTime = time.Now()
			}
			emit(mtime.FromTime(eventTime), *event)
			emitted++

			if event.LSN > pendingAck {
				pendingAck = event.LSN
			}
			advanceWatermark(we, eventTime)
		}
	}

	// A transaction that completed after the last loop-head check is safe to
	// acknowledge. One that did not is deliberately left out of ackCandidate.
	if !session.parser.HasBufferedTransaction() && pendingAck > ackCandidate {
		ackCandidate = pendingAck
	}

	// Acknowledgment runs here and nowhere else.
	//
	// The callback fires only after the runner has durably committed this
	// bundle's output, so the slot never advances past data the pipeline could
	// still lose. There is deliberately no alternative path that advances the
	// confirmed LSN directly: a shortcut taken only under test would leave
	// production acknowledging nothing while the suite stayed green.
	if ackCandidate > 0 {
		bf.RegisterCallback(bundleFinalizationTimeout, func() error {
			session.confirmFlushed(ackCandidate)
			return nil
		})
	}

	session.publishMetrics(ctx)
	return continuation, nil
}

func advanceWatermark(we *sdf.ManualWatermarkEstimator, t time.Time) {
	if we == nil {
		return
	}
	// The watermark must never move backwards. Out-of-order commit timestamps
	// are possible across concurrent transactions.
	if t.After(we.CurrentWatermark()) {
		we.UpdateWatermark(t)
	}
}

// isIdleTimeout reports whether err is a deadline expiry rather than a real
// stream failure.
func isIdleTimeout(err error) bool {
	if errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// --- session ---

// cdcSession owns one replication connection, its parser, and the keepalive
// goroutine that reports acknowledged positions back to the server.
type cdcSession struct {
	stream ReplicationStream
	parser *PgOutputParser

	// claimedLSN is the highest LSN claimed from the restriction tracker. It
	// deduplicates claims across the events of a single transaction, which all
	// carry the same LSN.
	claimedLSN uint64

	// latestReceivedLSN is the highest LSN this worker has read off the wire.
	// Reported as write_lsn, which is advisory only.
	latestReceivedLSN atomic.Uint64

	// confirmedFlushLSN is the highest LSN whose records are durable in the
	// pipeline. Reported as both flush_lsn and apply_lsn, and therefore the
	// only value that lets the server recycle WAL.
	confirmedFlushLSN atomic.Uint64

	// serverWALEnd is the primary's write position as reported by keepalives.
	serverWALEnd atomic.Uint64

	cancel context.CancelFunc
	done   chan struct{}

	closeOnce sync.Once
	closeErr  error
}

// runPreflightOnce validates the server configuration the first time this
// worker successfully opens a session.
//
// It runs here, and not in Setup, deliberately. Setup executes on every worker
// the runner initializes, so validating there produces a burst of concurrent
// connections to the primary proportional to the worker count, which can
// exhaust max_connections on a server that is also serving production traffic.
// ensureSession is reached only from ProcessElement, and the LSN restriction is
// never split, so exactly one worker arrives here however wide the pipeline is.
//
// Failure to open the diagnostic connection is not itself fatal. The
// replication connection opened immediately afterwards will report the
// underlying problem with better context.
func (fn *cdcSourceFn) runPreflightOnce(ctx context.Context, opts CDCOptions) error {
	fn.mu.Lock()
	done := fn.preflightDone
	fn.mu.Unlock()
	if done {
		return nil
	}

	newQuerier := fn.newPreflightQuerier
	if newQuerier == nil {
		newQuerier = func(o CDCOptions) (preflightQuerier, error) {
			return newSQLPreflightQuerier(o)
		}
	}
	querier, err := newQuerier(opts)
	if err != nil {
		log.Warnf(ctx, "postgresio: skipping preflight validation for slot %q, "+
			"could not open a diagnostic connection: %v", opts.SlotName, err)
		return nil
	}
	defer querier.Close()

	results := runPreflight(ctx, querier, opts)
	logPreflight(ctx, results)
	if err := preflightError(results); err != nil {
		return err
	}

	fn.mu.Lock()
	fn.preflightDone = true
	fn.mu.Unlock()
	return nil
}

func (fn *cdcSourceFn) ensureSession(ctx context.Context, resumeLSN uint64) (*cdcSession, error) {
	fn.mu.Lock()
	if fn.session != nil {
		session := fn.session
		fn.mu.Unlock()
		return session, nil
	}
	fn.mu.Unlock()

	opts := fn.Options
	// Resume where the restriction says, not where the pipeline was originally
	// configured to start. On a retry after a checkpoint these differ.
	opts.StartLSN = resumeLSN

	// Validate before dialing the replication protocol, so a misconfigured
	// server is reported by name rather than as a protocol-level error.
	if err := fn.runPreflightOnce(ctx, opts); err != nil {
		return nil, err
	}

	var stream ReplicationStream
	var err error
	if opts.StreamFactory != nil {
		stream, err = opts.StreamFactory(ctx, opts)
	} else {
		stream, err = NewNativeReplicationStream(ctx, opts)
	}
	if err != nil {
		return nil, err
	}

	statusInterval := opts.StatusInterval
	if statusInterval <= 0 {
		statusInterval = opts.HeartbeatInterval
	}
	if statusInterval <= 0 {
		statusInterval = 10 * time.Second
	}

	session := &cdcSession{
		stream: stream,
		parser: NewPgOutputParser(),
		done:   make(chan struct{}),
	}
	session.latestReceivedLSN.Store(resumeLSN)
	session.confirmedFlushLSN.Store(resumeLSN)
	session.serverWALEnd.Store(resumeLSN)

	// The keepalive loop is owned by the session rather than by a single
	// ProcessElement call. It must keep running between invocations: the server
	// closes a replication connection that is silent for wal_sender_timeout.
	keepaliveCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	session.cancel = cancel
	go session.keepaliveLoop(keepaliveCtx, statusInterval)

	fn.mu.Lock()
	if fn.session != nil {
		// Another invocation won the race.
		existing := fn.session
		fn.mu.Unlock()
		_ = session.close()
		return existing, nil
	}
	fn.session = session
	fn.startMonitorLocked(ctx)
	fn.mu.Unlock()
	return session, nil
}

func (fn *cdcSourceFn) dropSession(session *cdcSession) {
	fn.mu.Lock()
	if fn.session == session {
		fn.session = nil
	}
	fn.mu.Unlock()
	_ = session.close()
}

// currentSession returns the live replication session, or nil if none is open.
func (fn *cdcSourceFn) currentSession() *cdcSession {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	return fn.session
}

// latchedBreach returns the circuit breaker's latched breach, or nil.
//
// The latch clears on its own once a later measurement comes in under budget,
// so a backlog that drains does not fail the pipeline permanently.
func (fn *cdcSourceFn) latchedBreach() *slotBreach {
	fn.mu.Lock()
	monitor := fn.monitor
	fn.mu.Unlock()
	if monitor == nil {
		return nil
	}
	return monitor.breached()
}

func (s *cdcSession) keepaliveLoop(ctx context.Context, interval time.Duration) {
	defer close(s.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sendStatus(ctx)
		}
	}
}

// sendStatus reports positions to the server. flush and apply carry only the
// confirmed LSN, which is what governs WAL retention.
func (s *cdcSession) sendStatus(ctx context.Context) {
	confirmed := s.confirmedFlushLSN.Load()
	_ = s.stream.SendStandbyStatus(ctx, StandbyStatus{
		WriteLSN:       s.latestReceivedLSN.Load(),
		FlushLSN:       confirmed,
		ApplyLSN:       confirmed,
		ClientTime:     time.Now(),
		ReplyRequested: false,
	})
}

func (s *cdcSession) nextMessage(ctx context.Context, deadline time.Time) ([]byte, error) {
	readCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	return s.stream.NextMessage(readCtx)
}

func (s *cdcSession) observeReceived(lsn uint64) {
	for {
		current := s.latestReceivedLSN.Load()
		if lsn <= current || s.latestReceivedLSN.CompareAndSwap(current, lsn) {
			return
		}
	}
}

func (s *cdcSession) observeServerWALEnd(lsn uint64) {
	for {
		current := s.serverWALEnd.Load()
		if lsn <= current || s.serverWALEnd.CompareAndSwap(current, lsn) {
			return
		}
	}
}

// confirmFlushed advances the acknowledged position. Called only from a bundle
// finalization callback.
func (s *cdcSession) confirmFlushed(lsn uint64) {
	for {
		current := s.confirmedFlushLSN.Load()
		if lsn <= current || s.confirmedFlushLSN.CompareAndSwap(current, lsn) {
			return
		}
	}
}

func (s *cdcSession) publishMetrics(ctx context.Context) {
	confirmed := s.confirmedFlushLSN.Load()
	walEnd := s.serverWALEnd.Load()
	cdcConfirmedFlushLSN.Set(ctx, int64(confirmed))
	cdcServerWALEnd.Set(ctx, int64(walEnd))
}

// close shuts the connection down before waiting for the keepalive goroutine.
//
// The order matters. That goroutine writes standby status to the socket, and
// closing the socket is what forces a blocked write to return; cancelling
// first and then waiting would deadlock against a write that has not yet
// observed cancellation. The bounded wait is a second line of defence, so a
// goroutine stuck somewhere unanticipated delays teardown rather than
// preventing it.
func (s *cdcSession) close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.stream.Close()
		if s.cancel != nil {
			s.cancel()
			select {
			case <-s.done:
			case <-time.After(sessionCloseTimeout):
			}
		}
	})
	return s.closeErr
}

// ReadCDC constructs a streaming Apache Beam source that reads continuous Change Data Capture
// events from a PostgreSQL logical replication slot.
//
// Ingestion is strictly single-consumer at the replication slot boundary: the
// restriction is never split, because PostgreSQL admits one connection per
// slot. For parallel processing across workers, downstream pipelines can apply
// PartitionByPrimaryKey.
//
// Emitted elements carry the transaction's commit timestamp as their event
// time, and the source advances a watermark from the same clock, so downstream
// fixed and sliding windows close.
func ReadCDC(s beam.Scope, opts ...CDCOption) beam.PCollection {
	s = s.Scope("postgresio.ReadCDC")
	cdcOpts := NewCDCOptions(opts...)
	if err := cdcOpts.Validate(); err != nil {
		panic(fmt.Sprintf("invalid postgresio.CDCOptions: %v", err))
	}
	if cdcOpts.PreflightAtConstruction {
		// Opt-in, and it panics for the same reason Validate does: a pipeline
		// built against a server that cannot serve it should not be submitted.
		if err := Preflight(context.Background(), cdcOpts); err != nil {
			panic(err.Error())
		}
	}

	// Singleton impulse to ensure exactly one worker connects to the replication slot
	singleton := beam.Create(s, 1)
	return beam.ParDo(s, newCDCSourceFn(cdcOpts), singleton)
}
