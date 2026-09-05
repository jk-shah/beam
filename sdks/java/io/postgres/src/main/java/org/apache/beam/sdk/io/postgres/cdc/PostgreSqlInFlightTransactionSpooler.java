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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * Stateful spooling engine for PostgreSQL 16+ streaming in-flight logical decoding transactions
 * ({@code streaming = parallel}, {@code two_phase = on}).
 *
 * <p>Spools partial chunks of large uncommitted transactions into Beam managed state, preventing
 * JVM Heap memory exhaustion on multi-million row batch updates. Emits all spooled mutations in
 * exact stream order upon transaction commit ({@code STREAM_COMMIT} / {@code COMMIT_PREPARED}) or
 * purges them immediately upon transaction rollback ({@code STREAM_ABORT}).
 */
public class PostgreSqlInFlightTransactionSpooler
    extends DoFn<
        KV<Long, PostgreSqlInFlightTransactionSpooler.TransactionMessage>, ChangeEvent<Row>> {

  public enum MessageType {
    DATA_MUTATION,
    STREAM_COMMIT,
    STREAM_ABORT
  }

  @DefaultCoder(SerializableCoder.class)
  public static class TransactionMessage implements Serializable {
    private final MessageType messageType;
    private final long transactionId;
    private final @Nullable ChangeEvent<Row> mutation;

    public TransactionMessage(
        MessageType messageType, long transactionId, @Nullable ChangeEvent<Row> mutation) {
      this.messageType = messageType;
      this.transactionId = transactionId;
      this.mutation = mutation;
    }

    public static TransactionMessage ofMutation(long xid, ChangeEvent<Row> mutation) {
      return new TransactionMessage(MessageType.DATA_MUTATION, xid, mutation);
    }

    public static TransactionMessage ofCommit(long xid) {
      return new TransactionMessage(MessageType.STREAM_COMMIT, xid, null);
    }

    public static TransactionMessage ofAbort(long xid) {
      return new TransactionMessage(MessageType.STREAM_ABORT, xid, null);
    }

    public MessageType getMessageType() {
      return messageType;
    }

    public long getTransactionId() {
      return transactionId;
    }

    public @Nullable ChangeEvent<Row> getMutation() {
      return mutation;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TransactionMessage)) return false;
      TransactionMessage that = (TransactionMessage) o;
      return transactionId == that.transactionId
          && messageType == that.messageType
          && Objects.equals(mutation, that.mutation);
    }

    @Override
    public int hashCode() {
      return Objects.hash(messageType, transactionId, mutation);
    }
  }

  private final Counter spooledMutationsCounter =
      Metrics.counter(
          PostgreSqlInFlightTransactionSpooler.class, "PostgreSQL_InFlight_Mutations_Spooled");
  private final Counter committedTransactionsCounter =
      Metrics.counter(
          PostgreSqlInFlightTransactionSpooler.class, "PostgreSQL_InFlight_Transactions_Committed");
  private final Counter abortedTransactionsCounter =
      Metrics.counter(
          PostgreSqlInFlightTransactionSpooler.class, "PostgreSQL_InFlight_Transactions_Aborted");

  private static final org.apache.beam.sdk.values.TypeDescriptor<ChangeEvent<Row>>
      CHANGE_EVENT_TYPE = new org.apache.beam.sdk.values.TypeDescriptor<ChangeEvent<Row>>() {};

  @StateId("spooledMutations")
  private final StateSpec<BagState<ChangeEvent<Row>>> spooledMutationsSpec =
      StateSpecs.bag(SerializableCoder.of(CHANGE_EVENT_TYPE));

  @TimerId("transactionTimeoutTimer")
  private final TimerSpec transactionTimeoutTimerSpec =
      TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  private final Duration transactionTimeout;

  public PostgreSqlInFlightTransactionSpooler(Duration transactionTimeout) {
    this.transactionTimeout = transactionTimeout;
  }

  public PostgreSqlInFlightTransactionSpooler() {
    this(Duration.standardHours(2));
  }

  @ProcessElement
  public void processElement(
      @Element KV<Long, TransactionMessage> element,
      OutputReceiver<ChangeEvent<Row>> receiver,
      @StateId("spooledMutations") BagState<ChangeEvent<Row>> spooledMutationsState,
      @TimerId("transactionTimeoutTimer") Timer transactionTimeoutTimer) {

    TransactionMessage message = element.getValue();
    MessageType msgType = message.getMessageType();

    if (msgType == MessageType.DATA_MUTATION) {
      if (message.getMutation() != null) {
        spooledMutationsState.add(message.getMutation());
        spooledMutationsCounter.inc();
        if (transactionTimeout != null) {
          transactionTimeoutTimer.offset(transactionTimeout).setRelative();
        }
      }
    } else if (msgType == MessageType.STREAM_COMMIT) {
      // Transaction committed: replay all spooled mutations downstream
      List<ChangeEvent<Row>> spooled = new ArrayList<>();
      for (ChangeEvent<Row> event : spooledMutationsState.read()) {
        spooled.add(event);
      }
      for (ChangeEvent<Row> event : spooled) {
        receiver.output(event);
      }
      spooledMutationsState.clear();
      committedTransactionsCounter.inc();
    } else if (msgType == MessageType.STREAM_ABORT) {
      // Transaction aborted: discard all spooled mutations without emitting
      spooledMutationsState.clear();
      abortedTransactionsCounter.inc();
    }
  }

  @OnTimer("transactionTimeoutTimer")
  public void onTimeout(
      @StateId("spooledMutations") BagState<ChangeEvent<Row>> spooledMutationsState) {
    // Clean up abandoned in-flight transactions
    spooledMutationsState.clear();
  }
}
