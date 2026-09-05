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
import java.util.ArrayList;
import java.util.List;
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
 * Maximum throughput append-only sink {@link DoFn} utilizing PostgreSQL native {@link CopyManager}.
 */
public class CopyManagerBinaryWriterDoFn extends DoFn<KV<String, Iterable<Row>>, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(CopyManagerBinaryWriterDoFn.class);

  private final PostgreSqlDataSourceConfiguration dataSourceConfig;
  private final TupleTag<Row> successTag;
  private final TupleTag<PostgreSqlWriteError> dlqTag;

  private transient HikariDataSource dataSource;

  public CopyManagerBinaryWriterDoFn(
      PostgreSqlDataSourceConfiguration dataSourceConfig,
      TupleTag<Row> successTag,
      TupleTag<PostgreSqlWriteError> dlqTag) {
    this.dataSourceConfig = dataSourceConfig;
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
    List<Row> rows = new ArrayList<>();
    element.getValue().forEach(rows::add);
    if (rows.isEmpty()) {
      return;
    }

    Schema schema = rows.get(0).getSchema();
    String escapedTable = PostgreSqlUtils.escapeTableIdentifier(targetTable);
    String columnList =
        schema.getFieldNames().stream()
            .map(PostgreSqlUtils::escapeIdentifier)
            .collect(Collectors.joining(", "));

    String copySql =
        String.format(
            "COPY %s (%s) FROM STDIN WITH (FORMAT text, DELIMITER '\t')", escapedTable, columnList);

    StringBuilder textData = new StringBuilder();
    for (Row row : rows) {
      for (int i = 0; i < schema.getFieldCount(); i++) {
        if (i > 0) textData.append('\t');
        Object val = row.getValue(i);
        if (val == null) {
          textData.append("\\N");
        } else {
          String strVal =
              val.toString().replace("\\", "\\\\").replace("\t", " ").replace("\n", " ");
          textData.append(strVal);
        }
      }
      textData.append('\n');
    }

    try (Connection conn = dataSource.getConnection()) {
      PGConnection pgConn = conn.unwrap(PGConnection.class);
      CopyManager copyManager = pgConn.getCopyAPI();
      try (StringReader reader = new StringReader(textData.toString())) {
        copyManager.copyIn(copySql, reader);
      }

      for (Row row : rows) {
        receiver.get(successTag).output(row);
      }
    } catch (Exception e) {
      LOG.warn(
          "CopyManager write failed for table {}. Routing batch to DLQ. Error: {}",
          targetTable,
          SanitizingExceptionTransformer.sanitizeErrorMessage(e));
      String sqlState = SanitizingExceptionTransformer.extractSqlState(e);
      String cleanMsg = SanitizingExceptionTransformer.sanitizeErrorMessage(e);
      for (Row row : rows) {
        receiver
            .get(dlqTag)
            .output(new PostgreSqlWriteError(row, sqlState, cleanMsg, targetTable, Instant.now()));
      }
    }
  }
}
