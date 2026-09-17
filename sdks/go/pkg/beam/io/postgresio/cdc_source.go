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
	"strings"
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
	globalCDCSessionsMu sync.Mutex
	globalCDCSessions   = make(map[string]*cdcSession)
)

func cdcSessionKey(opts CDCOptions) string {
	return fmt.Sprintf("%s:%d/%s/%s", opts.Host, opts.Port, opts.Database, opts.SlotName)
}

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

	// lastConfirmedLSN records the highest LSN confirmed flushed by this DoFn,
	// ensuring that dropping a session does not reset resumeLSN back to rest.Start.
	lastConfirmedLSN uint64
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
//
// Both releases previously ran only when Options.StreamFactory was set. That
// field is injected by tests, so on a real pipeline neither ran: the walsender
// stayed attached and the slot stayed active = true until the worker process
// exited. An active slot cannot be re-acquired, which delays restart and
// failover, and the monitor goroutine kept polling on its own connection.
//
// The session is shared between DoFn instances on a worker, so it is released
// by reference count and closed by whichever instance drops the last one. The
// monitor belongs to this DoFn and is always stopped.
func (fn *cdcSourceFn) Teardown() error {
	fn.mu.Lock()
	session := fn.session
	fn.session = nil
	monitor := fn.monitor
	fn.monitor = nil
	fn.mu.Unlock()

	var err error
	if session != nil && session.refs.Add(-1) <= 0 {
		// Last holder: unpublish before closing so a concurrent ensureSession
		// cannot adopt a session that is about to be torn down.
		key := cdcSessionKey(fn.Options)
		globalCDCSessionsMu.Lock()
		if globalCDCSessions[key] == session {
			delete(globalCDCSessions, key)
		}
		globalCDCSessionsMu.Unlock()
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
func (fn *cdcSourceFn) startMonitorLocked(ctx context.Context, session *cdcSession) {
	if fn.Options.DisableSlotMonitoring {
		return
	}
	if session.monitor != nil {
		fn.monitor = session.monitor
		return
	}
	if fn.monitor != nil {
		session.monitor = fn.monitor
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
		fn.dropSession(session)
	})
	monitor.start(ctx)
	session.monitor = monitor
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

	var ackCandidate uint64
	var pendingAck uint64
	var claimedLSN uint64

	emitted := 0
	checkpointInterval := fn.Options.CheckpointInterval
	if checkpointInterval <= 0 {
		checkpointInterval = DefaultCheckpointInterval
	}
	deadline := time.Now().Add(checkpointInterval)
	continuation := sdf.ResumeProcessingIn(resumeDelay)

	stallTimeout := fn.Options.TransactionStallTimeout
	if stallTimeout <= 0 {
		stallTimeout = transactionStallTimeout
	}
	stallDeadline := time.Now().Add(stallTimeout)

	consecutiveFailures := 0
	var firstFailure time.Time

	maxAttempts := fn.Options.MaxReconnectAttempts
	if maxAttempts <= 0 {
		maxAttempts = 5 // sane default
	}

	var session *cdcSession
	for {
		var resumeLSN uint64
		fn.mu.Lock()
		if fn.session != nil {
			resumeLSN = fn.session.confirmedFlushLSN.Load()
		}
		fn.mu.Unlock()
		if resumeLSN == 0 {
			resumeLSN = uint64(rest.Start)
		}

		var err error
		session, err = fn.ensureSession(ctx, resumeLSN)
		if err != nil {
			if isRetryableConnectionError(err) {
				consecutiveFailures++
				if consecutiveFailures == 1 {
					firstFailure = time.Now()
				}
				if consecutiveFailures > maxAttempts {
					log.Errorf(ctx, "postgresio: replication stream error %v, retry budget exhausted", err)
					return sdf.StopProcessing(), err
				}
				if fn.Options.MaxReconnectElapsed > 0 && time.Since(firstFailure) > fn.Options.MaxReconnectElapsed {
					log.Errorf(ctx, "postgresio: replication stream error %v, max elapsed retry time exhausted", err)
					return sdf.StopProcessing(), err
				}

				backoff := computeBackoff(consecutiveFailures)
				log.Infof(ctx, "postgresio: connection failure %v, attempt %d/%d, backing off for %v, resuming from LSN %X", err, consecutiveFailures, maxAttempts, backoff, resumeLSN)
				select {
				case <-ctx.Done():
					return sdf.StopProcessing(), ctx.Err()
				case <-time.After(backoff):
				}
				continue
			}
			return sdf.StopProcessing(), fmt.Errorf("postgresio: failed to initialize replication stream: %w", err)
		}

		consecutiveFailures = 0
		firstFailure = time.Time{}

	readLoop:
		for {
			atBoundary := !session.parser.HasBufferedTransaction()
			if atBoundary && pendingAck > ackCandidate {
				ackCandidate = pendingAck
			}

			if ctx.Err() != nil {
				continuation = sdf.StopProcessing()
				break readLoop
			}

			if atBoundary {
				if breach := fn.latchedBreach(); breach != nil {
					return sdf.StopProcessing(), breach
				}
			}

			readDeadline := deadline
			if atBoundary {
				if emitted >= maxEventsPerInvocation || !time.Now().Before(deadline) {
					break readLoop
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
					break readLoop
				case isIdleTimeout(err):
					fn.dropSession(session)
					return sdf.StopProcessing(), fmt.Errorf("postgresio: replication stream silent for %s inside an open transaction: %w", stallTimeout, err)
				case errors.Is(err, io.EOF):
					fn.dropSession(session)
					continuation = sdf.ResumeProcessingIn(reconnectDelay)
					break readLoop
				case isRetryableConnectionError(err):
					fn.dropSession(session)
					consecutiveFailures++
					if consecutiveFailures == 1 {
						firstFailure = time.Now()
					}
					if consecutiveFailures > maxAttempts {
						log.Errorf(ctx, "postgresio: replication stream error %v, retry budget exhausted", err)
						return sdf.StopProcessing(), err
					}
					if fn.Options.MaxReconnectElapsed > 0 && time.Since(firstFailure) > fn.Options.MaxReconnectElapsed {
						log.Errorf(ctx, "postgresio: replication stream error %v, max elapsed retry time exhausted", err)
						return sdf.StopProcessing(), err
					}

					resumeLSN = session.confirmedFlushLSN.Load()
					if resumeLSN == 0 {
						resumeLSN = atomic.LoadUint64(&fn.lastConfirmedLSN)
					}
					if resumeLSN == 0 {
						resumeLSN = uint64(rest.Start)
					}

					backoff := computeBackoff(consecutiveFailures)
					log.Infof(ctx, "postgresio: connection failure %v, attempt %d/%d, backing off for %v, resuming from LSN %X", err, consecutiveFailures, maxAttempts, backoff, resumeLSN)
					select {
					case <-ctx.Done():
						return sdf.StopProcessing(), ctx.Err()
					case <-time.After(backoff):
					}
					break readLoop
				default:
					fn.dropSession(session)
					log.Errorf(ctx, "postgresio: error reading replication stream: %v", err)
					return sdf.StopProcessing(), err
				}
			}

			stallDeadline = time.Now().Add(stallTimeout)

			if len(payload) == 0 {
				continue
			}

			if payload[0] == 'k' {
				walEnd, _, replyRequested, err := ParseKeepAlive(payload)
				if err != nil {
					continue
				}
				session.observeServerWALEnd(walEnd)
				session.observeReceived(walEnd)

				if !session.parser.HasBufferedTransaction() {
					if walEnd > ackCandidate {
						ackCandidate = walEnd
					}
					advanceWatermark(we, time.Now())
				}

				if replyRequested {
					session.sendStatus(ctx)
				}
				continue
			}

			msgPayload := payload
			if len(payload) > 0 && payload[0] == 'w' {
				startLSN, endWAL, _, walData, err := ParseXLogData(payload)
				if err != nil {
					log.Warnf(ctx, "postgresio: failed to parse XLogData envelope: %v", err)
					continue
				}
				session.observeServerWALEnd(endWAL)
				session.observeReceived(startLSN)
				msgPayload = walData
			}

			events, err := session.parser.ParseMessages(msgPayload)
			if err != nil {
				fn.dropSession(session)
				log.Errorf(ctx, "postgresio: failed to parse pgoutput message: %v", err)
				return sdf.StopProcessing(), fmt.Errorf("postgresio: failed to parse pgoutput message: %w", err)
			}

			if lastCommitted := session.parser.LastCommittedLSN(); lastCommitted > pendingAck {
				pendingAck = lastCommitted
			}

			for _, event := range events {
				if event == nil {
					continue
				}

				if event.LSN > claimedLSN {
					if event.LSN > math.MaxInt64 {
						return sdf.StopProcessing(), fmt.Errorf("postgresio: LSN %d exceeds the representable restriction range", event.LSN)
					}
					if !rt.TryClaim(int64(event.LSN)) {
						if err := rt.GetError(); err != nil {
							return sdf.StopProcessing(), err
						}
						continuation = sdf.StopProcessing()
						break readLoop
					}
					claimedLSN = event.LSN
				}

				session.observeReceived(event.LSN)

				if event.LSN > pendingAck {
					pendingAck = event.LSN
				}

				if fn.Options.OriginFilter == "none" && event.Origin != "" {
					cdcFilteredRecords.Inc(ctx, 1)
					continue
				}

				cdcProcessedRecords.Inc(ctx, 1)

				eventTime := event.CommitTime
				if eventTime.IsZero() {
					eventTime = time.Now()
				}
				emit(mtime.FromTime(eventTime), *event)
				emitted++

				advanceWatermark(we, eventTime)
			}
		}

		if consecutiveFailures > 0 {
			continue
		}
		break
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
	bf.RegisterCallback(bundleFinalizationTimeout, func() error {
		if ackCandidate > 0 {
			session.confirmFlushed(ackCandidate)
			for {
				curr := atomic.LoadUint64(&fn.lastConfirmedLSN)
				if ackCandidate <= curr || atomic.CompareAndSwapUint64(&fn.lastConfirmedLSN, curr, ackCandidate) {
					break
				}
			}
		}
		return nil
	})

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

func isRetryableConnectionError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if errors.Is(err, errStreamDesynchronized) {
		return true
	}
	// driver.ErrBadConn is typically "driver: bad connection"
	if err.Error() == "driver: bad connection" {
		return true
	}
	// pq fatal/admin-shutdown SQLSTATEs
	msg := err.Error()
	if strings.Contains(msg, "57P01") ||
		strings.Contains(msg, "57P02") ||
		strings.Contains(msg, "57P03") ||
		strings.Contains(msg, "08006") ||
		strings.Contains(msg, "08003") {
		return true
	}
	return false
}

func computeBackoff(attempt int) time.Duration {
	base := 100 * time.Millisecond
	cap := 30 * time.Second

	shift := attempt - 1
	if shift < 0 {
		shift = 0
	}
	var backoff time.Duration
	if shift >= 30 {
		backoff = cap
	} else {
		backoff = base * (1 << shift)
	}
	if backoff > cap {
		backoff = cap
	}

	// +/- 10% jitter (pseudo-random via time since we don't strictly need rand)
	// Actually, time.Now().UnixNano() is fine for jitter.
	jitterAmt := int64(backoff) / 5
	if jitterAmt == 0 {
		return backoff
	}
	jitter := time.Duration(time.Now().UnixNano()%jitterAmt) - (backoff / 10)
	return backoff + jitter
}

// --- session ---

// cdcSession owns one replication connection, its parser, and the keepalive
// goroutine that reports acknowledged positions back to the server.
type cdcSession struct {
	stream ReplicationStream
	parser *PgOutputParser

	latestReceivedLSN atomic.Uint64
	confirmedFlushLSN atomic.Uint64
	serverWALEnd      atomic.Uint64

	readMu  sync.Mutex
	monitor *slotMonitor

	cancel context.CancelFunc
	done   chan struct{}

	closeOnce sync.Once
	closeErr  error
	isClosed  atomic.Bool

	// refs counts the DoFn instances currently holding this session.
	//
	// A session is shared through globalCDCSessions so that several DoFn
	// instances on one worker reuse a single replication connection rather
	// than opening one slot consumer each. Sharing means teardown cannot
	// simply close: the first instance to finish would pull the connection out
	// from under the others. The count makes the last one out responsible for
	// closing.
	refs atomic.Int64
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
	if fn.session != nil && !fn.session.isClosed.Load() {
		session := fn.session
		fn.mu.Unlock()
		return session, nil
	}
	fn.mu.Unlock()

	opts := fn.Options
	opts.StartLSN = resumeLSN

	key := cdcSessionKey(opts)
	globalCDCSessionsMu.Lock()
	if existing, ok := globalCDCSessions[key]; ok && !existing.isClosed.Load() {
		// Take the reference while still holding the map lock, so the count
		// cannot reach zero and close the session between the lookup here and
		// the adoption below.
		existing.refs.Add(1)
		globalCDCSessionsMu.Unlock()
		fn.mu.Lock()
		fn.session = existing
		fn.startMonitorLocked(ctx, existing)
		fn.mu.Unlock()
		return existing, nil
	}

	if err := fn.runPreflightOnce(ctx, opts); err != nil {
		globalCDCSessionsMu.Unlock()
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
		globalCDCSessionsMu.Unlock()
		return nil, err
	}

	statusInterval := opts.StatusInterval
	if statusInterval <= 0 {
		statusInterval = opts.HeartbeatInterval
	}
	if statusInterval <= 0 {
		statusInterval = 10 * time.Second
	}

	parser := NewPgOutputParser()
	if opts.MaxSpooledBytes > 0 {
		parser.SetMaxSpooledBytes(opts.MaxSpooledBytes)
	}
	parser.SetOversizedTxnPolicy(opts.OversizedTxnPolicy)

	session := &cdcSession{
		stream: stream,
		parser: parser,
		done:   make(chan struct{}),
	}
	session.latestReceivedLSN.Store(resumeLSN)
	session.confirmedFlushLSN.Store(resumeLSN)
	session.serverWALEnd.Store(resumeLSN)
	// The creating DoFn holds the first reference.
	session.refs.Store(1)

	keepaliveCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	session.cancel = cancel
	go session.keepaliveLoop(keepaliveCtx, statusInterval)

	globalCDCSessions[key] = session
	globalCDCSessionsMu.Unlock()

	fn.mu.Lock()
	fn.session = session
	fn.startMonitorLocked(ctx, session)
	fn.mu.Unlock()
	return session, nil
}

func (fn *cdcSourceFn) dropSession(session *cdcSession) {
	key := cdcSessionKey(fn.Options)
	globalCDCSessionsMu.Lock()
	if globalCDCSessions[key] == session {
		delete(globalCDCSessions, key)
	}
	globalCDCSessionsMu.Unlock()

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
	s.readMu.Lock()
	defer s.readMu.Unlock()
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
		s.isClosed.Store(true)
		if s.monitor != nil {
			_ = s.monitor.close()
		}
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
