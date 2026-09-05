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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import org.apache.beam.sdk.io.postgres.PostgreSqlDataSourceConfiguration;
import org.apache.beam.sdk.io.postgres.PostgreSqlRowMappers;
import org.apache.beam.sdk.io.postgres.PostgreSqlRowMappers.RowMapper;
import org.apache.beam.sdk.io.postgres.PostgreSqlUtils;
import org.apache.beam.sdk.io.postgres.cdc.ChangeEvent.OpType;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lock-Free Incremental Watermark Snapshot reader DoFn.
 *
 * <p>Reads a single primary-key range chunk of a table under standard {@code READ COMMITTED}
 * isolation, bracketed by low and high watermark signals emitted via PostgreSQL logical messages
 * ({@code pg_logical_emit_message}).
 */
public class WatermarkSnapshotSourceDoFn
    extends DoFn<WatermarkSnapshotChunk, KV<String, ChangeEvent<Row>>> {

  private static final Logger LOG = LoggerFactory.getLogger(WatermarkSnapshotSourceDoFn.class);

  private final Counter chunksReadCounter =
      Metrics.counter(WatermarkSnapshotSourceDoFn.class, "PostgreSQL_Snapshot_Chunks_Read");
  private final Counter rowsReadCounter =
      Metrics.counter(WatermarkSnapshotSourceDoFn.class, "PostgreSQL_Snapshot_Rows_Read");

  private final PostgreSqlDataSourceConfiguration dataSourceConfig;
  private final Schema schema;

  public WatermarkSnapshotSourceDoFn(
      PostgreSqlDataSourceConfiguration dataSourceConfig, Schema schema) {
    this.dataSourceConfig = dataSourceConfig;
    this.schema = schema;
  }

  @ProcessElement
  public void processElement(
      @Element WatermarkSnapshotChunk chunk, OutputReceiver<KV<String, ChangeEvent<Row>>> receiver)
      throws Exception {

    Instant snapshotTimestamp = Instant.now();
    RowMapper<Row> rowMapper = PostgreSqlRowMappers.forBeamSchema(schema);
    String escapedTable = PostgreSqlUtils.escapeTableIdentifier(chunk.getTableName());
    String escapedPk = PostgreSqlUtils.escapeIdentifier(chunk.getPrimaryKeyColumn());

    try (Connection conn = dataSourceConfig.buildRawDataSource().getConnection()) {
      conn.setAutoCommit(true);

      // 1. Emit Low Watermark Signal into Logical Replication Stream
      emitSignal(conn, chunk.getChunkId() + "_LOW", chunk.getTableName());

      // 2. Read Bounded Chunk under Short-Lived Query (<100ms)
      String query =
          String.format(
              "SELECT * FROM %s WHERE %s >= ? AND %s < ?", escapedTable, escapedPk, escapedPk);

      try (PreparedStatement stmt = conn.prepareStatement(query)) {
        stmt.setLong(1, chunk.getMinId());
        stmt.setLong(2, chunk.getMaxId());

        try (ResultSet rs = stmt.executeQuery()) {
          while (rs.next()) {
            Row row = rowMapper.mapRow(rs);
            Object pkVal = row.getValue(chunk.getPrimaryKeyColumn());
            String keyString = chunk.getTableName() + ":" + (pkVal != null ? pkVal.toString() : "");

            ChangeEvent<Row> event =
                new ChangeEvent<>(
                    OpType.READ,
                    "public",
                    chunk.getTableName(),
                    0L,
                    0L,
                    snapshotTimestamp,
                    null,
                    row,
                    Collections.emptySet());

            receiver.output(KV.of(keyString, event));
            rowsReadCounter.inc();
          }
        }
      }

      // 3. Emit High Watermark Signal into Logical Replication Stream
      emitSignal(conn, chunk.getChunkId() + "_HIGH", chunk.getTableName());
      chunksReadCounter.inc();
    }
  }

  private void emitSignal(Connection conn, String signalId, String tableName) {
    try (PreparedStatement stmt =
        conn.prepareStatement("SELECT pg_logical_emit_message(true, 'beam_signal', ?)")) {
      String payload =
          String.format("{\"signal_id\":\"%s\",\"table\":\"%s\"}", signalId, tableName);
      stmt.setString(1, payload);
      stmt.execute();
    } catch (Exception e) {
      LOG.warn("Failed to emit pg_logical_emit_message for signal {}. Non-fatal.", signalId, e);
    }
  }
}
