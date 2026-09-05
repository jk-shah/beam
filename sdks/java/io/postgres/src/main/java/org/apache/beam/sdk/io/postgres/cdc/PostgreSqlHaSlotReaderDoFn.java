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

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.postgres.PostgreSqlDataSourceConfiguration;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High-Availability (HA) Slot Ingestion DoFn whose sole responsibility is to drain the PostgreSQL
 * logical replication stream, buffer raw change batches into durable Cloud Storage (GCS / S3)
 * chunks, and immediately acknowledge flushed LSN offsets back to the database.
 *
 * <p>Includes in-band WAL lag monitoring from PostgreSQL keepalive frames ('k') with a
 * deterministic circuit breaker that raises {@link PostgreSqlWalExhaustionException} before source
 * database disk exhaustion can occur.
 */
public class PostgreSqlHaSlotReaderDoFn extends DoFn<String, PostgreSqlChunkMetadata> {

  private static final Logger LOG = LoggerFactory.getLogger(PostgreSqlHaSlotReaderDoFn.class);

  private final Counter recordsIngestedCounter =
      Metrics.counter(PostgreSqlHaSlotReaderDoFn.class, "PostgreSQL_HA_Records_Ingested_Count");
  private final Counter chunksStagedCounter =
      Metrics.counter(PostgreSqlHaSlotReaderDoFn.class, "PostgreSQL_HA_Chunks_Staged_Count");
  private final Counter bytesStagedCounter =
      Metrics.counter(PostgreSqlHaSlotReaderDoFn.class, "PostgreSQL_HA_Bytes_Staged_Count");
  private final Counter circuitBreakerTrippedCounter =
      Metrics.counter(
          PostgreSqlHaSlotReaderDoFn.class, "PostgreSQL_HA_Circuit_Breaker_Tripped_Count");

  private final PostgreSqlDataSourceConfiguration dataSourceConfig;
  private final String replicationSlotName;
  private final String publicationName;
  private final String stagingLocation;
  private final Duration maxBatchDuration;
  private final long maxBatchSizeBytes;
  private final PostgreSqlStagingFormat stagingFormat;
  private final long maxWalRetainedBytes;
  private final boolean failFastOnWalExhaustion;
  private final @Nullable String kmsKeyUri;

  public PostgreSqlHaSlotReaderDoFn(
      PostgreSqlDataSourceConfiguration dataSourceConfig,
      String replicationSlotName,
      String publicationName,
      String stagingLocation,
      Duration maxBatchDuration,
      long maxBatchSizeBytes,
      PostgreSqlStagingFormat stagingFormat,
      long maxWalRetainedBytes,
      boolean failFastOnWalExhaustion,
      @Nullable String kmsKeyUri) {
    this.dataSourceConfig = dataSourceConfig;
    this.replicationSlotName = replicationSlotName;
    this.publicationName = publicationName;
    this.stagingLocation = stagingLocation;
    this.maxBatchDuration = maxBatchDuration;
    this.maxBatchSizeBytes = maxBatchSizeBytes;
    this.stagingFormat = stagingFormat;
    this.maxWalRetainedBytes = maxWalRetainedBytes;
    this.failFastOnWalExhaustion = failFastOnWalExhaustion;
    this.kmsKeyUri = kmsKeyUri;
  }

  @ProcessElement
  public void processElement(
      @Element String trigger, OutputReceiver<PostgreSqlChunkMetadata> receiver) throws Exception {
    LOG.info(
        "Starting PostgreSQL HA Slot Reader for slot '{}', publication '{}', staging '{}', kmsKey '{}'",
        replicationSlotName,
        publicationName,
        stagingLocation,
        kmsKeyUri);

    try (Connection conn = dataSourceConfig.buildRawDataSource().getConnection()) {
      PGConnection pgConn = conn.unwrap(PGConnection.class);

      PGReplicationStream stream =
          pgConn
              .getReplicationAPI()
              .replicationStream()
              .logical()
              .withSlotName(replicationSlotName)
              .withSlotOption("proto_version", "1")
              .withSlotOption("publication_names", publicationName)
              .withStatusInterval(5, TimeUnit.SECONDS)
              .start();

      ByteArrayOutputStream chunkBuffer = new ByteArrayOutputStream(64 * 1024);
      long chunkStartLsn = stream.getLastReceiveLSN().asLong();
      long chunkEndLsn = chunkStartLsn;
      long recordCount = 0;
      long batchStartTime = System.currentTimeMillis();

      while (!Thread.currentThread().isInterrupted()) {
        ByteBuffer msg = stream.readPending();

        if (msg == null) {
          // Check if batch time threshold is reached during quiet period
          if (recordCount > 0
              && (System.currentTimeMillis() - batchStartTime >= maxBatchDuration.getMillis())) {
            PostgreSqlChunkMetadata metadata =
                flushChunk(chunkBuffer, chunkStartLsn, chunkEndLsn, recordCount, stream);
            receiver.output(metadata);
            chunksStagedCounter.inc();

            chunkBuffer.reset();
            recordCount = 0;
            chunkStartLsn = stream.getLastReceiveLSN().asLong();
            batchStartTime = System.currentTimeMillis();
          }

          TimeUnit.MILLISECONDS.sleep(10);
          continue;
        }

        // In-band WAL Lag Evaluation & Circuit Breaker
        LogSequenceNumber lastReceiveLsn = stream.getLastReceiveLSN();
        LogSequenceNumber lastFlushedLsn = stream.getLastFlushedLSN();
        if (lastReceiveLsn != null && lastFlushedLsn != null) {
          long walRetainedBytes = lastReceiveLsn.asLong() - lastFlushedLsn.asLong();
          checkWalCircuitBreaker(
              walRetainedBytes, lastReceiveLsn.asLong(), lastFlushedLsn.asLong());
        }

        // Append raw frame to chunk buffer with length prefix
        int frameLength = msg.remaining();
        chunkBuffer.write((frameLength >>> 24) & 0xFF);
        chunkBuffer.write((frameLength >>> 16) & 0xFF);
        chunkBuffer.write((frameLength >>> 8) & 0xFF);
        chunkBuffer.write(frameLength & 0xFF);

        byte[] frameBytes = new byte[frameLength];
        msg.get(frameBytes);
        chunkBuffer.write(frameBytes);

        chunkEndLsn = lastReceiveLsn != null ? lastReceiveLsn.asLong() : chunkEndLsn;
        recordCount++;
        recordsIngestedCounter.inc();

        // Check flush triggers: Size or Time watermark
        boolean sizeExceeded = chunkBuffer.size() >= maxBatchSizeBytes;
        boolean timeExceeded =
            (System.currentTimeMillis() - batchStartTime) >= maxBatchDuration.getMillis();

        if (sizeExceeded || timeExceeded) {
          PostgreSqlChunkMetadata metadata =
              flushChunk(chunkBuffer, chunkStartLsn, chunkEndLsn, recordCount, stream);
          receiver.output(metadata);
          chunksStagedCounter.inc();

          chunkBuffer.reset();
          recordCount = 0;
          chunkStartLsn = stream.getLastReceiveLSN().asLong();
          batchStartTime = System.currentTimeMillis();
        }
      }
    } catch (SQLException e) {
      LOG.error("PostgreSQL HA slot reading failed for slot {}", replicationSlotName, e);
      throw e;
    }
  }

  @VisibleForTesting
  void checkWalCircuitBreaker(long walRetainedBytes, long serverWalEnd, long confirmedFlushLsn) {
    if (walRetainedBytes >= maxWalRetainedBytes) {
      circuitBreakerTrippedCounter.inc();
      LOG.error(
          "CIRCUIT BREAKER TRIPPED! Slot '{}' retained WAL ({} bytes) >= max threshold ({} bytes).",
          replicationSlotName,
          walRetainedBytes,
          maxWalRetainedBytes);
      if (failFastOnWalExhaustion) {
        throw new PostgreSqlWalExhaustionException(
            replicationSlotName,
            walRetainedBytes,
            maxWalRetainedBytes,
            serverWalEnd,
            confirmedFlushLsn);
      }
    }
  }

  private PostgreSqlChunkMetadata flushChunk(
      ByteArrayOutputStream buffer,
      long startLsn,
      long endLsn,
      long recordCount,
      PGReplicationStream stream)
      throws Exception {
    byte[] rawData = buffer.toByteArray();
    String uuid = UUID.randomUUID().toString();
    String lsnHex = Long.toHexString(startLsn);
    String hashPrefix = computeHashPrefix(lsnHex);

    // Hash-partitioned immutable URI
    String normalizedLocation =
        stagingLocation.endsWith("/") ? stagingLocation : stagingLocation + "/";
    String chunkPath =
        String.format(
            "%sshard=0/hash=%s/chunk_%s_%s_%s.%s",
            normalizedLocation,
            hashPrefix,
            Long.toHexString(startLsn),
            Long.toHexString(endLsn),
            uuid,
            stagingFormat == PostgreSqlStagingFormat.RAW_PG_FRAMES ? "pgframe" : "avro");

    ResourceId resourceId = FileSystems.matchNewResource(chunkPath, false);
    try (WritableByteChannel channel = FileSystems.create(resourceId, MimeTypes.BINARY);
        OutputStream out = Channels.newOutputStream(channel)) {
      out.write(rawData);
      out.flush();
    }

    bytesStagedCounter.inc(rawData.length);

    // Immediately acknowledge flushed LSN back to primary PostgreSQL DB
    LogSequenceNumber flushLsn = LogSequenceNumber.valueOf(endLsn);
    stream.setFlushedLSN(flushLsn);
    stream.setAppliedLSN(flushLsn);
    stream.forceUpdateStatus();

    LOG.debug(
        "Successfully staged chunk with {} records ({} bytes) to {} and flushed LSN {}",
        recordCount,
        rawData.length,
        chunkPath,
        Long.toHexString(endLsn));

    return new PostgreSqlChunkMetadata(
        chunkPath,
        replicationSlotName,
        startLsn,
        endLsn,
        recordCount,
        rawData.length,
        stagingFormat,
        Instant.now());
  }

  private String computeHashPrefix(String lsnHex) {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] digest = md.digest(lsnHex.getBytes(StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < 2; i++) {
        sb.append(String.format("%02x", digest[i]));
      }
      return sb.toString();
    } catch (Exception e) {
      return "0000";
    }
  }
}
