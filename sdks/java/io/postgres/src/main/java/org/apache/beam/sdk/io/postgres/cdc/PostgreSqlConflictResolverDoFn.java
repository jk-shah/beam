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

import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;

/**
 * Stateful PTransform DoFn resolving multi-master write conflicts in bi-directional or multi-region
 * PostgreSQL replication topologies.
 *
 * <p>Implements deterministic Last-Write-Wins (LWW) resolution based on PostgreSQL commit
 * timestamps ({@code track_commit_timestamp = on}) with monotonic LSN tie-breaking.
 */
public class PostgreSqlConflictResolverDoFn<T>
    extends DoFn<KV<String, ChangeEvent<T>>, ChangeEvent<T>> {

  @StateId("latestEvent")
  private final StateSpec<ValueState<ChangeEvent<T>>> latestEventSpec = StateSpecs.value();

  @ProcessElement
  public void processElement(
      @Element KV<String, ChangeEvent<T>> element,
      OutputReceiver<ChangeEvent<T>> receiver,
      @StateId("latestEvent") ValueState<ChangeEvent<T>> latestEventState) {

    ChangeEvent<T> incoming = element.getValue();
    if (incoming == null) {
      return;
    }

    ChangeEvent<T> existing = latestEventState.read();
    if (existing == null) {
      latestEventState.write(incoming);
      receiver.output(incoming);
      return;
    }

    if (isIncomingFresher(incoming, existing)) {
      latestEventState.write(incoming);
      receiver.output(incoming);
    }
  }

  /** Evaluates whether the incoming event is strictly fresher than the existing state. */
  public static <T> boolean isIncomingFresher(ChangeEvent<T> incoming, ChangeEvent<T> existing) {
    Instant incomingTs = incoming.getCommitTimestamp();
    Instant existingTs = existing.getCommitTimestamp();

    if (incomingTs.isAfter(existingTs)) {
      return true;
    }
    if (incomingTs.isBefore(existingTs)) {
      return false;
    }

    // Tie-breaker: Monotonic LSN comparison
    return incoming.getLsn() > existing.getLsn();
  }
}
