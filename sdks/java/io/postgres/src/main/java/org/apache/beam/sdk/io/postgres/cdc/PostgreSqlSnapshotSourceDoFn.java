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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.io.postgres.PostgreSqlDataSourceConfiguration;
import org.apache.beam.sdk.io.postgres.PostgreSqlRowMappers;
import org.apache.beam.sdk.io.postgres.PostgreSqlRowMappers.RowMapper;
import org.apache.beam.sdk.io.postgres.PostgreSqlUtils;
import org.apache.beam.sdk.io.postgres.cdc.ChangeEvent.OpType;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

/**
 * Reads a bounded partition slice of a table within a leased {@code REPEATABLE READ} snapshot
 * during Phase 1 of CDC bootstrapping.
 */
public class PostgreSqlSnapshotSourceDoFn
    extends DoFn<PostgreSqlSnapshotSourceDoFn.SnapshotSlice, ChangeEvent<Row>> {

  public static class SnapshotSlice implements Serializable {
    public final String tableName;
    public final String primaryKeyColumn;
    public final long minId;
    public final long maxId;
    public final String snapshotId;
    public final long snapshotLsn;

    public SnapshotSlice(
        String tableName,
        String primaryKeyColumn,
        long minId,
        long maxId,
        String snapshotId,
        long snapshotLsn) {
      this.tableName = tableName;
      this.primaryKeyColumn = primaryKeyColumn;
      this.minId = minId;
      this.maxId = maxId;
      this.snapshotId = snapshotId;
      this.snapshotLsn = snapshotLsn;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof SnapshotSlice)) return false;
      SnapshotSlice that = (SnapshotSlice) o;
      return minId == that.minId
          && maxId == that.maxId
          && snapshotLsn == that.snapshotLsn
          && Objects.equals(tableName, that.tableName)
          && Objects.equals(primaryKeyColumn, that.primaryKeyColumn)
          && Objects.equals(snapshotId, that.snapshotId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tableName, primaryKeyColumn, minId, maxId, snapshotId, snapshotLsn);
    }
  }

  private final PostgreSqlDataSourceConfiguration dataSourceConfig;
  private final Schema schema;

  public PostgreSqlSnapshotSourceDoFn(
      PostgreSqlDataSourceConfiguration dataSourceConfig, Schema schema) {
    this.dataSourceConfig = dataSourceConfig;
    this.schema = schema;
  }

  @ProcessElement
  public void processElement(
      @Element SnapshotSlice slice, OutputReceiver<ChangeEvent<Row>> receiver) throws Exception {

    Instant snapshotTimestamp = Instant.now();
    RowMapper<Row> rowMapper = PostgreSqlRowMappers.forBeamSchema(schema);

    try (Connection conn = dataSourceConfig.buildRawDataSource().getConnection()) {
      conn.setAutoCommit(false);
      conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      // Bind to exported snapshot if provided
      if (slice.snapshotId != null && !slice.snapshotId.isEmpty()) {
        try (PreparedStatement snapStmt = conn.prepareStatement("SET TRANSACTION SNAPSHOT ?")) {
          snapStmt.setString(1, slice.snapshotId);
          snapStmt.execute();
        }
      }

      String escapedTable = PostgreSqlUtils.escapeTableIdentifier(slice.tableName);
      String escapedPk = PostgreSqlUtils.escapeIdentifier(slice.primaryKeyColumn);

      String query =
          String.format(
              "SELECT * FROM %s WHERE %s >= ? AND %s < ?", escapedTable, escapedPk, escapedPk);

      try (PreparedStatement stmt = conn.prepareStatement(query)) {
        stmt.setFetchSize(2000);
        stmt.setLong(1, slice.minId);
        stmt.setLong(2, slice.maxId);

        try (ResultSet rs = stmt.executeQuery()) {
          List<String> parts =
              org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter.on('.')
                  .splitToList(slice.tableName);
          String schemaName = parts.size() == 2 ? parts.get(0) : "public";
          String tableName = parts.size() == 2 ? parts.get(1) : parts.get(0);

          while (rs.next()) {
            Row row = rowMapper.mapRow(rs);
            ChangeEvent<Row> event =
                new ChangeEvent<>(
                    OpType.READ,
                    schemaName,
                    tableName,
                    slice.snapshotLsn,
                    0L,
                    snapshotTimestamp,
                    null,
                    row,
                    null);
            receiver.outputWithTimestamp(event, snapshotTimestamp);
          }
        }
      }
      conn.commit();
    }
  }
}
