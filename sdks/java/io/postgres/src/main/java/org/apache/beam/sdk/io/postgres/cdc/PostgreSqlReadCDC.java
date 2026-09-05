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

import com.google.auto.value.AutoValue;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.postgres.PostgreSqlDataSourceConfiguration;
import org.apache.beam.sdk.io.postgres.PostgreSqlTypeUtils;
import org.apache.beam.sdk.io.postgres.PostgreSqlUtils;
import org.apache.beam.sdk.io.postgres.cdc.PostgreSqlSnapshotSourceDoFn.SnapshotSlice;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * Top-level {@link PTransform} for Change Data Capture (CDC) from PostgreSQL tables via logical
 * replication and initial consistent snapshotting.
 */
@AutoValue
public abstract class PostgreSqlReadCDC extends PTransform<PBegin, PCollection<ChangeEvent<Row>>> {

  public enum SnapshotMode {
    INITIAL_PARALLEL,
    INCREMENTAL_WATERMARK,
    NEVER
  }

  public abstract PostgreSqlDataSourceConfiguration getDataSourceConfiguration();

  public abstract String getPublicationName();

  public abstract String getReplicationSlotName();

  public abstract SnapshotMode getSnapshotMode();

  public abstract int getSnapshotParallelism();

  public abstract List<String> getTableWhitelist();

  public abstract @Nullable Duration getHeartbeatInterval();

  public abstract @Nullable String getStartLsn();

  public abstract @Nullable String getStagingLocation();

  public abstract @Nullable Duration getStagingDuration();

  public abstract @Nullable Long getStagingMaxBytes();

  public abstract @Nullable PostgreSqlStagingFormat getStagingFormat();

  public abstract @Nullable Long getMaxWalRetainedBytes();

  public abstract @Nullable Boolean getFailFastOnWalExhaustion();

  public abstract @Nullable Boolean getAutoRepairOnStaleSlot();

  public abstract @Nullable SlotRepairPolicy getSlotRepairPolicy();

  public abstract @Nullable String getOriginFilter();

  public abstract @Nullable String getKmsKeyUri();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_PostgreSqlReadCDC.Builder()
        .setSnapshotMode(SnapshotMode.INITIAL_PARALLEL)
        .setSnapshotParallelism(8)
        .setTableWhitelist(Collections.emptyList())
        .setHeartbeatInterval(Duration.standardSeconds(10))
        .setStagingFormat(PostgreSqlStagingFormat.RAW_AVRO)
        .setStagingDuration(Duration.standardSeconds(10))
        .setStagingMaxBytes(10L * 1024 * 1024L)
        .setMaxWalRetainedBytes(20L * 1024 * 1024 * 1024L)
        .setFailFastOnWalExhaustion(true)
        .setAutoRepairOnStaleSlot(true)
        .setSlotRepairPolicy(SlotRepairPolicy.AUTOMATIC_RECREATE_AND_CATCHUP);
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setDataSourceConfiguration(PostgreSqlDataSourceConfiguration config);

    public abstract Builder setPublicationName(String publicationName);

    public abstract Builder setReplicationSlotName(String slotName);

    public abstract Builder setSnapshotMode(SnapshotMode mode);

    public abstract Builder setSnapshotParallelism(int parallelism);

    public abstract Builder setTableWhitelist(List<String> tables);

    public abstract Builder setHeartbeatInterval(@Nullable Duration interval);

    public abstract Builder setStartLsn(@Nullable String startLsn);

    public abstract Builder setStagingLocation(@Nullable String location);

    public abstract Builder setStagingDuration(@Nullable Duration duration);

    public abstract Builder setStagingMaxBytes(@Nullable Long maxBytes);

    public abstract Builder setStagingFormat(@Nullable PostgreSqlStagingFormat format);

    public abstract Builder setMaxWalRetainedBytes(@Nullable Long maxBytes);

    public abstract Builder setFailFastOnWalExhaustion(@Nullable Boolean failFast);

    public abstract Builder setAutoRepairOnStaleSlot(@Nullable Boolean autoRepair);

    public abstract Builder setSlotRepairPolicy(@Nullable SlotRepairPolicy policy);

    public abstract Builder setOriginFilter(@Nullable String originFilter);

    public abstract Builder setKmsKeyUri(@Nullable String kmsKey);

    public Builder withOriginFilter(@Nullable String originFilter) {
      return setOriginFilter(originFilter);
    }

    public Builder withAutoRepairOnStaleSlot(boolean autoRepair) {
      return setAutoRepairOnStaleSlot(autoRepair);
    }

    public Builder withSlotRepairPolicy(SlotRepairPolicy policy) {
      return setSlotRepairPolicy(policy);
    }

    public Builder withDataSourceConfiguration(PostgreSqlDataSourceConfiguration config) {
      return setDataSourceConfiguration(config);
    }

    public Builder withPublicationName(String publicationName) {
      return setPublicationName(publicationName);
    }

    public Builder withReplicationSlotName(String slotName) {
      return setReplicationSlotName(slotName);
    }

    public Builder withSnapshotMode(SnapshotMode mode) {
      return setSnapshotMode(mode);
    }

    public Builder withSnapshotParallelism(int parallelism) {
      return setSnapshotParallelism(parallelism);
    }

    public Builder withTableWhitelist(List<String> tables) {
      return setTableWhitelist(tables);
    }

    public Builder withHeartbeatInterval(@Nullable Duration interval) {
      return setHeartbeatInterval(interval);
    }

    public Builder withStartLsn(@Nullable String startLsn) {
      return setStartLsn(startLsn);
    }

    public Builder withStagingLocation(@Nullable String location) {
      return setStagingLocation(location);
    }

    public Builder withStagingFileRotation(Duration duration, long maxBytes) {
      return setStagingDuration(duration).setStagingMaxBytes(maxBytes);
    }

    public Builder withStagingFormat(@Nullable PostgreSqlStagingFormat format) {
      return setStagingFormat(format);
    }

    public Builder withMaxWalRetainedBytes(@Nullable Long maxBytes) {
      return setMaxWalRetainedBytes(maxBytes);
    }

    public Builder withFailFastOnWalExhaustion(@Nullable Boolean failFast) {
      return setFailFastOnWalExhaustion(failFast);
    }

    public Builder withKmsKeyUri(@Nullable String kmsKey) {
      return setKmsKeyUri(kmsKey);
    }

    public abstract PostgreSqlReadCDC build();
  }

  public PostgreSqlReadCDC withDataSourceConfiguration(PostgreSqlDataSourceConfiguration config) {
    return toBuilder().setDataSourceConfiguration(config).build();
  }

  public PostgreSqlReadCDC withPublicationName(String publicationName) {
    return toBuilder().setPublicationName(publicationName).build();
  }

  public PostgreSqlReadCDC withReplicationSlotName(String slotName) {
    return toBuilder().setReplicationSlotName(slotName).build();
  }

  public PostgreSqlReadCDC withSnapshotMode(SnapshotMode mode) {
    return toBuilder().setSnapshotMode(mode).build();
  }

  public PostgreSqlReadCDC withSnapshotParallelism(int parallelism) {
    return toBuilder().setSnapshotParallelism(parallelism).build();
  }

  public PostgreSqlReadCDC withTableWhitelist(List<String> tables) {
    return toBuilder().setTableWhitelist(tables).build();
  }

  public PostgreSqlReadCDC withHeartbeatInterval(Duration interval) {
    return toBuilder().setHeartbeatInterval(interval).build();
  }

  public PostgreSqlReadCDC withStartLsn(String lsn) {
    return toBuilder().setStartLsn(lsn).build();
  }

  @Override
  public PCollection<ChangeEvent<Row>> expand(PBegin input) {
    Map<String, Schema> tableSchemas = discoverSchemas();
    String startLsn = getStartLsn();

    List<SnapshotSlice> slices = new ArrayList<>();
    String snapshotId = null;

    if (startLsn == null) {
      startLsn = "0/0";
    }

    if (getSnapshotMode() == SnapshotMode.INITIAL_PARALLEL) {
      try (Connection conn = getDataSourceConfiguration().buildRawDataSource().getConnection()) {
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

        // Create logical slot if it does not exist and capture consistent snapshot
        try (Statement stmt = conn.createStatement()) {
          ResultSet slotRs =
              stmt.executeQuery(
                  String.format(
                      "SELECT lsn, snapshot_name FROM pg_create_logical_replication_slot('%s', 'pgoutput', false, false)",
                      getReplicationSlotName()));
          if (slotRs.next()) {
            startLsn = slotRs.getString(1);
            snapshotId = slotRs.getString(2);
          }
        } catch (Exception e) {
          // Slot might already exist, read current LSN
          try (Statement stmt2 = conn.createStatement();
              ResultSet rs = stmt2.executeQuery("SELECT pg_current_wal_lsn()")) {
            if (rs.next()) {
              startLsn = rs.getString(1);
            }
          }
        }

        // Build parallel range slices for each whitelisted table
        for (String table : getTableWhitelist()) {
          buildSlicesForTable(conn, table, snapshotId, startLsn, slices);
        }

        conn.commit();
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to initialize PostgreSQL CDC replication slot and snapshot", e);
      }
    }

    // Launch Phase 1 snapshot reader if slices exist
    PCollection<ChangeEvent<Row>> snapshotStream = null;
    if (!slices.isEmpty()) {
      Schema firstSchema = tableSchemas.values().iterator().next();
      snapshotStream =
          input
              .apply("CreateSnapshotSlices", Create.of(slices))
              .apply(
                  "ReadInitialSnapshot",
                  ParDo.of(
                      new PostgreSqlSnapshotSourceDoFn(getDataSourceConfiguration(), firstSchema)));
    }

    // Launch Phase 2 continuous WAL streamer (Direct or Decoupled HA Staging)
    PCollection<ChangeEvent<Row>> walStream;
    if (getStagingLocation() != null && !getStagingLocation().isEmpty()) {
      TupleTag<ChangeEvent<Row>> outputTag = new TupleTag<>("output");
      TupleTag<Row> dlqTag = new TupleTag<>("dlq");

      PCollection<PostgreSqlChunkMetadata> stagedChunks =
          input
              .apply("TriggerHAOffloader", Create.of("start"))
              .apply(
                  "HA_Slot_Draining_And_Staging",
                  ParDo.of(
                      new PostgreSqlHaSlotReaderDoFn(
                          getDataSourceConfiguration(),
                          getReplicationSlotName(),
                          getPublicationName(),
                          getStagingLocation(),
                          getStagingDuration() != null
                              ? getStagingDuration()
                              : Duration.standardSeconds(10),
                          getStagingMaxBytes() != null ? getStagingMaxBytes() : 10L * 1024 * 1024L,
                          getStagingFormat() != null
                              ? getStagingFormat()
                              : PostgreSqlStagingFormat.RAW_AVRO,
                          getMaxWalRetainedBytes() != null
                              ? getMaxWalRetainedBytes()
                              : 20L * 1024 * 1024 * 1024L,
                          getFailFastOnWalExhaustion() != null
                              ? getFailFastOnWalExhaustion()
                              : true,
                          getKmsKeyUri())));

      org.apache.beam.sdk.values.PCollectionTuple chunkRecords =
          stagedChunks.apply(
              "Parallel_Staged_Chunk_Decoding",
              ParDo.of(new PostgreSqlStagedChunkReaderDoFn(tableSchemas, outputTag, dlqTag))
                  .withOutputTags(outputTag, org.apache.beam.sdk.values.TupleTagList.of(dlqTag)));

      walStream = chunkRecords.get(outputTag);
    } else {
      walStream =
          input
              .apply("EmitStartLsn", Create.of(Collections.singletonList(startLsn)))
              .apply(
                  "StreamWAL",
                  ParDo.of(
                      new PostgreSqlWalReaderDoFn(
                          getDataSourceConfiguration(),
                          getPublicationName(),
                          getReplicationSlotName(),
                          tableSchemas,
                          100)));
    }

    PCollection<ChangeEvent<Row>> mergedStream;
    if (snapshotStream != null) {
      mergedStream =
          PCollectionList.of(snapshotStream)
              .and(walStream)
              .apply("UnionCDCStreams", Flatten.pCollections());
    } else {
      mergedStream = walStream;
    }

    return mergedStream
        .apply(
            "KeyByEntityForToast",
            ParDo.of(
                new DoFn<
                    ChangeEvent<Row>, org.apache.beam.sdk.values.KV<String, ChangeEvent<Row>>>() {
                  @ProcessElement
                  public void processElement(
                      @Element ChangeEvent<Row> event,
                      OutputReceiver<org.apache.beam.sdk.values.KV<String, ChangeEvent<Row>>>
                          receiver) {
                    String entityKey;
                    if (event.getAfter() != null) {
                      entityKey =
                          event.getSchemaName()
                              + "."
                              + event.getTableName()
                              + "#"
                              + event.getAfter().hashCode();
                    } else if (event.getBefore() != null) {
                      entityKey =
                          event.getSchemaName()
                              + "."
                              + event.getTableName()
                              + "#"
                              + event.getBefore().hashCode();
                    } else {
                      entityKey =
                          event.getSchemaName()
                              + "."
                              + event.getTableName()
                              + "#tx_"
                              + event.getTransactionId();
                    }
                    receiver.output(org.apache.beam.sdk.values.KV.of(entityKey, event));
                  }
                }))
        .apply("ReconstructTOAST", ParDo.of(new PostgreSqlToastReconstructionDoFn()))
        .apply(
            "KeyByEntityForDedup",
            ParDo.of(
                new DoFn<
                    ChangeEvent<Row>, org.apache.beam.sdk.values.KV<String, ChangeEvent<Row>>>() {
                  @ProcessElement
                  public void processElement(
                      @Element ChangeEvent<Row> event,
                      OutputReceiver<org.apache.beam.sdk.values.KV<String, ChangeEvent<Row>>>
                          receiver) {
                    String entityKey;
                    if (event.getAfter() != null) {
                      entityKey =
                          event.getSchemaName()
                              + "."
                              + event.getTableName()
                              + "#"
                              + event.getAfter().hashCode();
                    } else if (event.getBefore() != null) {
                      entityKey =
                          event.getSchemaName()
                              + "."
                              + event.getTableName()
                              + "#"
                              + event.getBefore().hashCode();
                    } else {
                      entityKey =
                          event.getSchemaName()
                              + "."
                              + event.getTableName()
                              + "#tx_"
                              + event.getTransactionId();
                    }
                    receiver.output(org.apache.beam.sdk.values.KV.of(entityKey, event));
                  }
                }))
        .apply("DeduplicateWatermark", ParDo.of(new WatermarkDeduplicationDoFn()));
  }

  private Map<String, Schema> discoverSchemas() {
    Map<String, Schema> schemas = new HashMap<>();
    try (Connection conn = getDataSourceConfiguration().buildRawDataSource().getConnection()) {
      for (String table : getTableWhitelist()) {
        String escapedTable = PostgreSqlUtils.escapeTableIdentifier(table);
        try (Statement stmt = conn.createStatement();
            ResultSet rs =
                stmt.executeQuery(String.format("SELECT * FROM %s WHERE 1=0", escapedTable))) {
          Schema schema = PostgreSqlTypeUtils.inferBeamSchema(rs.getMetaData());
          schemas.put(table, schema);
        }
      }
    } catch (Exception e) {
      // Fallback
    }
    return schemas;
  }

  private void buildSlicesForTable(
      Connection conn,
      String table,
      @Nullable String snapshotId,
      String startLsnStr,
      List<SnapshotSlice> slicesOut)
      throws Exception {
    String escapedTable = PostgreSqlUtils.escapeTableIdentifier(table);
    String pkCol = "id";

    // Query primary key column
    try (PreparedStatement pkStmt =
        conn.prepareStatement(
            "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid "
                + "AND a.attnum = ANY(i.indkey) WHERE i.indrelid = ?::regclass AND i.indisprimary")) {
      pkStmt.setString(1, table);
      try (ResultSet pkRs = pkStmt.executeQuery()) {
        if (pkRs.next()) {
          pkCol = pkRs.getString(1);
        }
      }
    }

    long minId = 0L;
    long maxId = 0L;
    try (Statement minMaxStmt = conn.createStatement();
        ResultSet rs =
            minMaxStmt.executeQuery(
                String.format(
                    "SELECT COALESCE(MIN(%s), 0), COALESCE(MAX(%s), 0) FROM %s",
                    PostgreSqlUtils.escapeIdentifier(pkCol),
                    PostgreSqlUtils.escapeIdentifier(pkCol),
                    escapedTable))) {
      if (rs.next()) {
        minId = rs.getLong(1);
        maxId = rs.getLong(2);
      }
    }

    if (maxId >= minId && maxId > 0) {
      int splits = Math.max(1, getSnapshotParallelism());
      long rangeSize = Math.max(1L, (maxId - minId + 1) / splits);
      long current = minId;
      long snapshotLsn = org.postgresql.replication.LogSequenceNumber.valueOf(startLsnStr).asLong();

      for (int i = 0; i < splits; i++) {
        long next = (i == splits - 1) ? maxId + 1 : current + rangeSize;
        slicesOut.add(new SnapshotSlice(table, pkCol, current, next, snapshotId, snapshotLsn));
        current = next;
      }
    }
  }
}
