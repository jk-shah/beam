/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.postgres.cdc;

import org.apache.beam.sdk.io.postgres.cdc.ChangeEvent.OpType;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;

/**
 * Stateful DoFn for deduplicating lock-free watermark snapshot records against concurrent live CDC
 * change events.
 *
 * <p>For a given primary key:
 *
 * <ul>
 *   <li>If a live CDC mutation (INSERT/UPDATE/DELETE) has already been processed, incoming snapshot
 *       records (READ) are suppressed as superseded.
 *   <li>If no live CDC mutation has been observed, the snapshot record is emitted as the baseline.
 * </ul>
 */
public class WatermarkDeduplicationDoFn
    extends DoFn<KV<String, ChangeEvent<Row>>, ChangeEvent<Row>> {

  private final Counter snapshotEmittedCounter =
      Metrics.counter(WatermarkDeduplicationDoFn.class, "PostgreSQL_Snapshot_Records_Emitted");
  private final Counter snapshotSupersededCounter =
      Metrics.counter(WatermarkDeduplicationDoFn.class, "PostgreSQL_Snapshot_Records_Superseded");
  private final Counter liveMutationsEmittedCounter =
      Metrics.counter(WatermarkDeduplicationDoFn.class, "PostgreSQL_Live_Mutations_Emitted");

  @StateId("hasLiveMutation")
  private final StateSpec<ValueState<Boolean>> hasLiveMutationSpec = StateSpecs.value();

  @StateId("lastSeenLsn")
  private final StateSpec<ValueState<Long>> lastSeenLsnSpec = StateSpecs.value();

  @TimerId("stateExpiryTimer")
  private final TimerSpec stateExpiryTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  private final Duration stateTtl;

  public WatermarkDeduplicationDoFn(Duration stateTtl) {
    this.stateTtl = stateTtl;
  }

  public WatermarkDeduplicationDoFn() {
    this(Duration.standardHours(1));
  }

  @ProcessElement
  public void processElement(
      @Element KV<String, ChangeEvent<Row>> element,
      OutputReceiver<ChangeEvent<Row>> receiver,
      @StateId("hasLiveMutation") ValueState<Boolean> hasLiveMutationState,
      @StateId("lastSeenLsn") ValueState<Long> lastSeenLsnState,
      @TimerId("stateExpiryTimer") Timer stateExpiryTimer) {

    ChangeEvent<Row> event = element.getValue();
    Boolean hasLive = hasLiveMutationState.read();

    if (event.getOpType() == OpType.READ) {
      // Snapshot record
      if (Boolean.TRUE.equals(hasLive)) {
        // A more recent live CDC mutation already arrived for this key -> suppress snapshot record
        snapshotSupersededCounter.inc();
      } else {
        receiver.output(event);
        snapshotEmittedCounter.inc();
      }
    } else {
      // Live CDC mutation (INSERT / UPDATE / DELETE / TRUNCATE)
      hasLiveMutationState.write(true);
      lastSeenLsnState.write(event.getLsn());
      receiver.output(event);
      liveMutationsEmittedCounter.inc();

      // Schedule TTL state cleanup
      if (stateTtl != null) {
        stateExpiryTimer.offset(stateTtl).setRelative();
      }
    }
  }

  @OnTimer("stateExpiryTimer")
  public void onExpiry(
      @StateId("hasLiveMutation") ValueState<Boolean> hasLiveMutationState,
      @StateId("lastSeenLsn") ValueState<Long> lastSeenLsnState) {
    hasLiveMutationState.clear();
    lastSeenLsnState.clear();
  }
}
