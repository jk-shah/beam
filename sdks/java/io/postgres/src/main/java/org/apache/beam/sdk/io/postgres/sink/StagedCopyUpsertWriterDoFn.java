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
package org.apache.beam.sdk.io.postgres.sink;

import com.zaxxer.hikari.HikariDataSource;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.postgres.PostgreSqlDataSourceConfiguration;
import org.apache.beam.sdk.io.postgres.PostgreSqlUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Two-phase bulk upsert writer utilizing {@code COPY} into temporary unlogged staging tables
 * followed by an atomic {@code INSERT INTO ... SELECT ... ON CONFLICT DO UPDATE}.
 */
public class StagedCopyUpsertWriterDoFn extends DoFn<KV<String, Iterable<Row>>, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(StagedCopyUpsertWriterDoFn.class);

  private final PostgreSqlDataSourceConfiguration dataSourceConfig;
  private final List<String> primaryKeyColumns;
  private final TupleTag<Row> successTag;
  private final TupleTag<PostgreSqlWriteError> dlqTag;

  private transient HikariDataSource dataSource;

  public StagedCopyUpsertWriterDoFn(
      PostgreSqlDataSourceConfiguration dataSourceConfig,
      List<String> primaryKeyColumns,
      TupleTag<Row> successTag,
      TupleTag<PostgreSqlWriteError> dlqTag) {
    this.dataSourceConfig = dataSourceConfig;
    this.primaryKeyColumns = primaryKeyColumns;
    this.successTag = successTag;
    this.dlqTag = dlqTag;
  }

  @Setup
  public void setup() {
    dataSource = ConnectionPoolManager.getDataSource(dataSourceConfig);
  }

  @ProcessElement
  public void processElement(
      @Element KV<String, Iterable<Row>> element, MultiOutputReceiver receiver) throws Exception {

    String targetTable = element.getKey();
    List<Row> rows = BatchCompactor.compactAndSort(element.getValue(), primaryKeyColumns);
    if (rows.isEmpty()) {
      return;
    }

    Schema schema = rows.get(0).getSchema();
    String escapedTargetTable = PostgreSqlUtils.escapeTableIdentifier(targetTable);
    String tempTableName = "pg_temp.stage_" + UUID.randomUUID().toString().replace("-", "");

    String columnList =
        schema.getFieldNames().stream()
            .map(PostgreSqlUtils::escapeIdentifier)
            .collect(Collectors.joining(", "));

    String conflictTarget =
        primaryKeyColumns.stream()
            .map(PostgreSqlUtils::escapeIdentifier)
            .collect(Collectors.joining(", "));

    List<String> nonPkColumns =
        schema.getFieldNames().stream()
            .filter(col -> !primaryKeyColumns.contains(col))
            .collect(Collectors.toList());

    String updateClause =
        nonPkColumns.isEmpty()
            ? "DO NOTHING"
            : "DO UPDATE SET "
                + nonPkColumns.stream()
                    .map(
                        col ->
                            PostgreSqlUtils.escapeIdentifier(col)
                                + " = EXCLUDED."
                                + PostgreSqlUtils.escapeIdentifier(col))
                    .collect(Collectors.joining(", "));

    StringBuilder textData = new StringBuilder();
    for (Row row : rows) {
      for (int i = 0; i < schema.getFieldCount(); i++) {
        if (i > 0) textData.append('\t');
        Object val = row.getValue(i);
        if (val == null) {
          textData.append("\\N");
        } else {
          String strVal =
              val.toString()
                  .replace("\\", "\\\\")
                  .replace("\t", "\\t")
                  .replace("\n", "\\n")
                  .replace("\r", "\\r");
          textData.append(strVal);
        }
      }
      textData.append('\n');
    }

    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("SET search_path = pg_catalog, pg_temp, public");
        stmt.execute(
            String.format(
                "CREATE TEMP TABLE %s (LIKE %s EXCLUDING CONSTRAINTS INCLUDING DEFAULTS) ON COMMIT DROP",
                tempTableName, escapedTargetTable));

        PGConnection pgConn = conn.unwrap(PGConnection.class);
        CopyManager copyManager = pgConn.getCopyAPI();
        String copySql =
            String.format(
                "COPY %s (%s) FROM STDIN WITH (FORMAT text, DELIMITER '\t')",
                tempTableName, columnList);
        try (StringReader reader = new StringReader(textData.toString())) {
          copyManager.copyIn(copySql, reader);
        }

        String mergeSql =
            String.format(
                "INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) %s",
                escapedTargetTable,
                columnList,
                columnList,
                tempTableName,
                conflictTarget,
                updateClause);
        stmt.execute(mergeSql);

        conn.commit();
        stmt.execute("DISCARD TEMP");

        for (Row row : rows) {
          receiver.get(successTag).output(row);
        }
      } catch (Exception e) {
        conn.rollback();
        LOG.warn(
            "StagedCopy upsert failed for table {}. Routing batch to DLQ. Error: {}",
            targetTable,
            SanitizingExceptionTransformer.sanitizeErrorMessage(e));
        String sqlState = SanitizingExceptionTransformer.extractSqlState(e);
        String cleanMsg = SanitizingExceptionTransformer.sanitizeErrorMessage(e);
        for (Row row : rows) {
          receiver
              .get(dlqTag)
              .output(
                  new PostgreSqlWriteError(row, sqlState, cleanMsg, targetTable, Instant.now()));
        }
      }
    }
  }
}
