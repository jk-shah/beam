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
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.postgres.PostgreSqlDataSourceConfiguration;
import org.apache.beam.sdk.io.postgres.PostgreSqlUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High-performance streaming sink {@link DoFn} that executes atomic multi-row upserts via
 * parameterized PostgreSQL {@code UNNEST} arrays.
 */
public class UnnestArrayUpsertWriterDoFn extends DoFn<KV<String, Iterable<Row>>, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(UnnestArrayUpsertWriterDoFn.class);

  private final PostgreSqlDataSourceConfiguration dataSourceConfig;
  private final List<String> primaryKeyColumns;
  private final TupleTag<Row> successTag;
  private final TupleTag<PostgreSqlWriteError> dlqTag;

  private transient HikariDataSource dataSource;

  public UnnestArrayUpsertWriterDoFn(
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
    String sql = PostgreSqlUtils.buildUnnestUpsertQuery(targetTable, schema, primaryKeyColumns);

    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        bindArrayParameters(conn, stmt, rows, schema);
        stmt.execute();
        conn.commit();

        for (Row row : rows) {
          receiver.get(successTag).output(row);
        }
      } catch (SQLException e) {
        conn.rollback();
        LOG.warn(
            "Batch UNNEST upsert failed for table {}. Falling back to row-by-row execution. Error: {}",
            targetTable,
            SanitizingExceptionTransformer.sanitizeErrorMessage(e));
        fallbackRowByRow(conn, rows, schema, targetTable, receiver);
      }
    }
  }

  private void bindArrayParameters(
      Connection conn, PreparedStatement stmt, List<Row> rows, Schema schema) throws SQLException {
    int numRows = rows.size();
    for (int colIdx = 0; colIdx < schema.getFieldCount(); colIdx++) {
      Field field = schema.getField(colIdx);
      Object[] colValues = new Object[numRows];

      for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
        Object val = rows.get(rowIdx).getValue(colIdx);
        if (val instanceof Instant) {
          colValues[rowIdx] = new java.sql.Timestamp(((Instant) val).getMillis());
        } else {
          colValues[rowIdx] = val;
        }
      }

      String typeName = mapFieldTypeToSqlTypeName(field.getType());
      Array sqlArray = conn.createArrayOf(typeName, colValues);
      stmt.setArray(colIdx + 1, sqlArray);
    }
  }

  private String mapFieldTypeToSqlTypeName(org.apache.beam.sdk.schemas.Schema.FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case INT16:
        return "int2";
      case INT32:
        return "int4";
      case INT64:
        return "int8";
      case FLOAT:
        return "float4";
      case DOUBLE:
        return "float8";
      case DECIMAL:
        return "numeric";
      case BOOLEAN:
        return "bool";
      case BYTES:
        return "bytea";
      case DATETIME:
        return "timestamptz";
      case STRING:
      default:
        return "text";
    }
  }

  private void fallbackRowByRow(
      Connection conn,
      List<Row> rows,
      Schema schema,
      String targetTable,
      MultiOutputReceiver receiver) {

    String singleRowSql =
        PostgreSqlUtils.buildUnnestUpsertQuery(targetTable, schema, primaryKeyColumns);

    for (Row row : rows) {
      try (PreparedStatement stmt = conn.prepareStatement(singleRowSql)) {
        List<Row> singleList = new ArrayList<>(1);
        singleList.add(row);
        bindArrayParameters(conn, stmt, singleList, schema);
        stmt.execute();
        conn.commit();
        receiver.get(successTag).output(row);
      } catch (Exception rowEx) {
        try {
          conn.rollback();
        } catch (SQLException rollbackEx) {
          LOG.debug("Error rolling back failed row transaction", rollbackEx);
        }
        String sqlState = SanitizingExceptionTransformer.extractSqlState(rowEx);
        String cleanMsg = SanitizingExceptionTransformer.sanitizeErrorMessage(rowEx);
        PostgreSqlWriteError writeError =
            new PostgreSqlWriteError(row, sqlState, cleanMsg, targetTable, Instant.now());
        receiver.get(dlqTag).output(writeError);
      }
    }
  }
}
