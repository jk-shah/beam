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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.postgres.cdc.ChangeEvent.OpType;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
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
 * Stateful DoFn for reconstructing out-of-line TOAST (The Oversized-Attribute Storage Technique)
 * columns in PostgreSQL {@code UPDATE} CDC events using Beam Managed State.
 *
 * <p>Under PostgreSQL's default replica identity ({@code REPLICA IDENTITY DEFAULT}), unchanged
 * TOAST columns (such as large TEXT, JSONB, or BYTEA fields) are omitted from {@code UPDATE} change
 * events. This DoFn maintains a per-primary-key column cache in Beam managed state, filling in
 * unchanged TOAST attributes to emit complete, consistent row records downstream without issuing
 * expensive point queries back to the database.
 */
public class PostgreSqlToastReconstructionDoFn
    extends DoFn<KV<String, ChangeEvent<Row>>, ChangeEvent<Row>> {

  private final Counter toastReconstructedCounter =
      Metrics.counter(
          PostgreSqlToastReconstructionDoFn.class, "PostgreSQL_Toast_Columns_Reconstructed");
  private final Counter toastCacheHitsCounter =
      Metrics.counter(PostgreSqlToastReconstructionDoFn.class, "PostgreSQL_Toast_Cache_Hits");
  private final Counter toastCacheMissesCounter =
      Metrics.counter(PostgreSqlToastReconstructionDoFn.class, "PostgreSQL_Toast_Cache_Misses");

  @StateId("lastKnownFullRowState")
  private final StateSpec<ValueState<Row>> lastKnownRowStateSpec =
      StateSpecs.value(SerializableCoder.of(Row.class));

  @TimerId("toastExpiryTimer")
  private final TimerSpec toastExpiryTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  private final Duration stateTtl;

  public PostgreSqlToastReconstructionDoFn(Duration stateTtl) {
    this.stateTtl = stateTtl;
  }

  public PostgreSqlToastReconstructionDoFn() {
    this(Duration.standardHours(24));
  }

  @ProcessElement
  public void processElement(
      @Element KV<String, ChangeEvent<Row>> element,
      OutputReceiver<ChangeEvent<Row>> receiver,
      @StateId("lastKnownFullRowState") ValueState<Row> lastKnownRowState,
      @TimerId("toastExpiryTimer") Timer toastExpiryTimer) {

    ChangeEvent<Row> event = element.getValue();
    OpType opType = event.getOpType();

    if (opType == OpType.INSERT || opType == OpType.READ) {
      // Cache the full baseline row in state
      Row afterRow = event.getAfter();
      if (afterRow != null) {
        lastKnownRowState.write(afterRow);
        if (stateTtl != null) {
          toastExpiryTimer.offset(stateTtl).setRelative();
        }
      }
      receiver.output(event);

    } else if (opType == OpType.UPDATE) {
      Row afterRow = event.getAfter();
      Set<String> unchangedToastCols = event.getUnchangedToastColumns();

      if (afterRow != null && !unchangedToastCols.isEmpty()) {
        // Reconstruct unchanged TOAST columns from cached baseline row
        Row cachedRow = lastKnownRowState.read();
        Schema schema = afterRow.getSchema();
        List<Object> reconstructedValues = new ArrayList<>();

        for (Schema.Field field : schema.getFields()) {
          String fieldName = field.getName();
          if (unchangedToastCols.contains(fieldName)) {
            Object cachedVal = (cachedRow != null) ? cachedRow.getValue(fieldName) : null;
            if (cachedVal != null) {
              reconstructedValues.add(cachedVal);
              toastCacheHitsCounter.inc();
              toastReconstructedCounter.inc();
            } else {
              toastCacheMissesCounter.inc();
              reconstructedValues.add(null);
            }
          } else {
            reconstructedValues.add(afterRow.getValue(fieldName));
          }
        }

        Row reconstructedRow = Row.withSchema(schema).addValues(reconstructedValues).build();
        lastKnownRowState.write(reconstructedRow);

        ChangeEvent<Row> reconstructedEvent =
            new ChangeEvent<>(
                OpType.UPDATE,
                event.getSchemaName(),
                event.getTableName(),
                event.getLsn(),
                event.getTransactionId(),
                event.getCommitTimestamp(),
                event.getBefore(),
                reconstructedRow,
                Collections.emptySet());

        receiver.output(reconstructedEvent);
        if (stateTtl != null) {
          toastExpiryTimer.offset(stateTtl).setRelative();
        }
      } else {
        if (afterRow != null) {
          lastKnownRowState.write(afterRow);
          if (stateTtl != null) {
            toastExpiryTimer.offset(stateTtl).setRelative();
          }
        }
        receiver.output(event);
      }

    } else if (opType == OpType.DELETE) {
      // Row is deleted -> clear state cache
      lastKnownRowState.clear();
      receiver.output(event);

    } else {
      receiver.output(event);
    }
  }

  @OnTimer("toastExpiryTimer")
  public void onExpiry(@StateId("lastKnownFullRowState") ValueState<Row> lastKnownRowState) {
    lastKnownRowState.clear();
  }
}
