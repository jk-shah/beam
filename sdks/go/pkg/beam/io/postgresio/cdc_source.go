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
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
)

var (
	cdcProcessedRecords = beam.NewCounter("postgresio", "cdc_processed_records")
	cdcFilteredRecords  = beam.NewCounter("postgresio", "cdc_filtered_origin_records")
)

func init() {
	beam.RegisterDoFn(&cdcSourceFn{})
}

// cdcSourceFn is a single-consumer DoFn that establishes a logical replication
// stream and yields ChangeEvent records with decoupled background keepalives.
type cdcSourceFn struct {
	options CDCOptions
}

func newCDCSourceFn(opts CDCOptions) *cdcSourceFn {
	return &cdcSourceFn{
		options: opts,
	}
}

// ProcessElement connects to the PostgreSQL replication slot and emits change events.
func (fn *cdcSourceFn) ProcessElement(ctx context.Context, bf beam.BundleFinalization, _ int, emit func(ChangeEvent)) error {
	var stream ReplicationStream
	var err error

	if fn.options.StreamFactory != nil {
		stream, err = fn.options.StreamFactory(ctx, fn.options)
	} else {
		stream, err = NewNativeReplicationStream(ctx, fn.options)
	}
	if err != nil {
		return fmt.Errorf("failed to initialize replication stream: %w", err)
	}
	defer func() {
		_ = stream.Close()
	}()

	parser := NewPgOutputParser()

	var latestReceivedLSN uint64
	var confirmedCommittedLSN uint64
	var currentBundleMaxLSN uint64

	atomic.StoreUint64(&latestReceivedLSN, fn.options.StartLSN)
	atomic.StoreUint64(&confirmedCommittedLSN, fn.options.StartLSN)
	atomic.StoreUint64(&currentBundleMaxLSN, fn.options.StartLSN)

	// Decoupled background heartbeat loop
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	defer cancelHeartbeat()

	statusInterval := fn.options.StatusInterval
	if statusInterval <= 0 {
		statusInterval = fn.options.HeartbeatInterval
	}
	if statusInterval <= 0 {
		statusInterval = 10 * time.Second
	}
	heartbeatTicker := time.NewTicker(statusInterval)
	defer heartbeatTicker.Stop()

	var heartbeatWg sync.WaitGroup
	heartbeatWg.Add(1)
	go func() {
		defer heartbeatWg.Done()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-heartbeatTicker.C:
				wLSN := atomic.LoadUint64(&latestReceivedLSN)
				fLSN := atomic.LoadUint64(&confirmedCommittedLSN)
				status := StandbyStatus{
					WriteLSN:       wLSN,
					FlushLSN:       fLSN,
					ApplyLSN:       fLSN,
					ClientTime:     time.Now(),
					ReplyRequested: false,
				}
				_ = stream.SendStandbyStatus(heartbeatCtx, status)
			}
		}
	}()

	// Register bundle commit callback to safely advance confirmedCommittedLSN
	if bf != nil {
		bf.RegisterCallback(60*time.Second, func() error {
			bundleLSN := atomic.LoadUint64(&currentBundleMaxLSN)
			if bundleLSN > atomic.LoadUint64(&confirmedCommittedLSN) {
				atomic.StoreUint64(&confirmedCommittedLSN, bundleLSN)
			}
			return nil
		})
	}

	for {
		select {
		case <-ctx.Done():
			cancelHeartbeat()
			heartbeatWg.Wait()
			return ctx.Err()
		default:
		}

		payload, err := stream.NextMessage(ctx)
		if err != nil {
			if err == io.EOF || ctx.Err() != nil {
				cancelHeartbeat()
				heartbeatWg.Wait()
				return nil
			}
			log.Errorf(ctx, "postgresio: error reading replication stream: %v", err)
			cancelHeartbeat()
			heartbeatWg.Wait()
			return err
		}

		if len(payload) == 0 {
			continue
		}

		// Handle server keepalive message ('k')
		if payload[0] == 'k' {
			endWAL, _, replyReq, err := ParseKeepAlive(payload)
			if err == nil {
				if endWAL > atomic.LoadUint64(&latestReceivedLSN) {
					atomic.StoreUint64(&latestReceivedLSN, endWAL)
				}
				if replyReq {
					wLSN := atomic.LoadUint64(&latestReceivedLSN)
					fLSN := atomic.LoadUint64(&confirmedCommittedLSN)
					status := StandbyStatus{
						WriteLSN:       wLSN,
						FlushLSN:       fLSN,
						ApplyLSN:       fLSN,
						ClientTime:     time.Now(),
						ReplyRequested: false,
					}
					_ = stream.SendStandbyStatus(ctx, status)
				}
			}
			continue
		}

		events, err := parser.ParseMessages(payload)
		if err != nil {
			log.Warnf(ctx, "postgresio: failed to parse pgoutput message: %v", err)
			continue
		}

		for _, event := range events {
			if event != nil {
				if fn.options.OriginFilter == "none" && event.Origin != "" {
					cdcFilteredRecords.Inc(ctx, 1)
					continue
				}
				cdcProcessedRecords.Inc(ctx, 1)
				if event.LSN > atomic.LoadUint64(&latestReceivedLSN) {
					atomic.StoreUint64(&latestReceivedLSN, event.LSN)
				}
				if event.LSN > atomic.LoadUint64(&currentBundleMaxLSN) {
					atomic.StoreUint64(&currentBundleMaxLSN, event.LSN)
				}
				emit(*event)
				if bf == nil {
					atomic.StoreUint64(&confirmedCommittedLSN, event.LSN)
				}
			}
		}
	}
}

// ReadCDC constructs a streaming Apache Beam source that reads continuous Change Data Capture
// events from a PostgreSQL logical replication slot.
//
// Ingestion is strictly single-consumer (Parallelism = 1) at the replication slot boundary.
// For parallel processing across workers, downstream pipelines can apply PartitionByPrimaryKey.
func ReadCDC(s beam.Scope, opts ...CDCOption) beam.PCollection {
	s = s.Scope("postgresio.ReadCDC")
	cdcOpts := NewCDCOptions(opts...)
	if err := cdcOpts.Validate(); err != nil {
		panic(fmt.Sprintf("invalid postgresio.CDCOptions: %v", err))
	}

	// Singleton impulse to ensure exactly one worker connects to the replication slot
	singleton := beam.Create(s, 1)
	return beam.ParDo(s, newCDCSourceFn(cdcOpts), singleton)
}
