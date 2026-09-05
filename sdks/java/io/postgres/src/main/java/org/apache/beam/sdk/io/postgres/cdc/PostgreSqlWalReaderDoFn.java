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

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.io.postgres.PostgreSqlDataSourceConfiguration;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Splittable DoFn that streams continuous Write-Ahead Log (WAL) changes from a PostgreSQL logical
 * replication slot using the {@code pgoutput} protocol.
 */
@DoFn.UnboundedPerElement
public class PostgreSqlWalReaderDoFn extends DoFn<String, ChangeEvent<Row>> {

  private static final Logger LOG = LoggerFactory.getLogger(PostgreSqlWalReaderDoFn.class);

  private final PostgreSqlDataSourceConfiguration dataSourceConfig;
  private final String publicationName;
  private final String slotName;
  private final Map<String, Schema> tableSchemas;
  private final long pollTimeoutMs;

  private transient Connection replicationConnection;
  private transient PGReplicationStream replicationStream;
  private transient PgOutputParser parser;
  private transient long lastFlushedLsn = 0L;

  public PostgreSqlWalReaderDoFn(
      PostgreSqlDataSourceConfiguration dataSourceConfig,
      String publicationName,
      String slotName,
      Map<String, Schema> tableSchemas,
      long pollTimeoutMs) {
    this.dataSourceConfig = dataSourceConfig;
    this.publicationName = publicationName;
    this.slotName = slotName;
    this.tableSchemas = tableSchemas;
    this.pollTimeoutMs = pollTimeoutMs;
  }

  @GetInitialRestriction
  public LsnRange getInitialRestriction(@Element String startLsnStr) {
    long startLsn = LogSequenceNumber.valueOf(startLsnStr).asLong();
    return LsnRange.startingFrom(startLsn);
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState() {
    return Instant.now();
  }

  @GetRestrictionCoder
  public LsnRangeCoder getRestrictionCoder() {
    return LsnRangeCoder.of();
  }

  @NewWatermarkEstimator
  public ManualWatermarkEstimator<Instant> newWatermarkEstimator(
      @WatermarkEstimatorState Instant initialWatermark) {
    return new WatermarkEstimators.Manual(initialWatermark);
  }

  @Setup
  public void setup() throws Exception {
    parser = new PgOutputParser();
  }

  private void initReplicationStream(long startLsn) throws SQLException {
    Properties props = new Properties();
    props.putAll(dataSourceConfig.getConnectionProperties());
    props.setProperty(
        "user",
        dataSourceConfig.getUsername() != null ? dataSourceConfig.getUsername() : "postgres");
    if (dataSourceConfig.getPassword() != null) {
      props.setProperty("password", dataSourceConfig.getPassword());
    } else if (dataSourceConfig.getDynamicPasswordProvider() != null) {
      try {
        props.setProperty("password", dataSourceConfig.getDynamicPasswordProvider().getPassword());
      } catch (Exception e) {
        throw new SQLException("Failed to acquire dynamic IAM password for replication", e);
      }
    }
    PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "14");
    PGProperty.REWRITE_BATCHED_INSERTS.set(props, "true");

    replicationConnection = DriverManager.getConnection(dataSourceConfig.getUrl(), props);
    PGConnection pgConn = replicationConnection.unwrap(PGConnection.class);

    replicationStream =
        pgConn
            .getReplicationAPI()
            .replicationStream()
            .logical()
            .withSlotName(slotName)
            .withStartPosition(LogSequenceNumber.valueOf(startLsn))
            .withSlotOption("proto_version", "2")
            .withSlotOption("publication_names", publicationName)
            .withSlotOption("streaming", "on")
            .withStatusInterval(10, TimeUnit.SECONDS)
            .start();
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element String startLsnStr,
      RestrictionTracker<LsnRange, Long> tracker,
      OutputReceiver<ChangeEvent<Row>> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator,
      BundleFinalizer bundleFinalizer)
      throws Exception {

    long startLsn = tracker.currentRestriction().getFromLsn();
    if (replicationStream == null) {
      initReplicationStream(startLsn);
    }

    long maxElementsPerBundle = 5000L;
    long processedCount = 0;

    while (processedCount < maxElementsPerBundle) {
      ByteBuffer buffer = replicationStream.readPending();
      if (buffer == null) {
        // No pending changes, sleep briefly
        Thread.sleep(Math.min(100, pollTimeoutMs));
        buffer = replicationStream.readPending();
        if (buffer == null) {
          break;
        }
      }

      LogSequenceNumber currentLsn = replicationStream.getLastReceiveLSN();
      long lsnVal = currentLsn.asLong();

      if (!tracker.tryClaim(lsnVal)) {
        return ProcessContinuation.stop();
      }

      ChangeEvent<Row> event = parser.parseMessage(buffer, lsnVal, tableSchemas);
      if (event != null) {
        watermarkEstimator.setWatermark(event.getCommitTimestamp());
        receiver.outputWithTimestamp(event, event.getCommitTimestamp());
        processedCount++;
      }

      lastFlushedLsn = lsnVal;
    }

    // Register bundle finalizer to acknowledge LSN ONLY after runner commits bundle checkpoint
    final long confirmedLsn = lastFlushedLsn;
    bundleFinalizer.afterBundleCommit(
        Instant.now().plus(Duration.standardSeconds(30)),
        () -> {
          if (replicationStream != null && confirmedLsn > 0) {
            replicationStream.setFlushedLSN(LogSequenceNumber.valueOf(confirmedLsn));
            replicationStream.setAppliedLSN(LogSequenceNumber.valueOf(confirmedLsn));
          }
        });

    return ProcessContinuation.resume().withResumeDelay(Duration.millis(50));
  }

  @Teardown
  public void teardown() {
    try {
      if (replicationStream != null) {
        replicationStream.close();
      }
      if (replicationConnection != null) {
        replicationConnection.close();
      }
    } catch (Exception e) {
      LOG.warn("Error closing PostgreSQL replication stream", e);
    }
  }
}
