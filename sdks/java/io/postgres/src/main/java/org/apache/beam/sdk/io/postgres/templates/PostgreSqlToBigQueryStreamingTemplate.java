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
package org.apache.beam.sdk.io.postgres.templates;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.postgres.PostgreSQLIO;
import org.apache.beam.sdk.io.postgres.PostgreSqlDataSourceConfiguration;
import org.apache.beam.sdk.io.postgres.auth.GoogleCloudSqlIamPasswordProvider;
import org.apache.beam.sdk.io.postgres.cdc.ChangeEvent;
import org.apache.beam.sdk.io.postgres.cdc.PostgreSqlReadCDC;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Production Dataflow Streaming Template for replicating PostgreSQL CDC streams into Google Cloud
 * BigQuery with support for database-wide, single-schema, or single-table scopes, Dead-Letter
 * Queues, and Dataflow streaming pause and resume.
 */
public class PostgreSqlToBigQueryStreamingTemplate {

  private static final Logger LOG =
      LoggerFactory.getLogger(PostgreSqlToBigQueryStreamingTemplate.class);

  public interface Options extends PostgreSqlTemplateOptions {

    @Description("BigQuery Dataset ID (e.g. my_project:my_dataset or my_dataset)")
    @Required
    String getBigQueryDataset();

    void setBigQueryDataset(String value);

    @Description("BigQuery Table ID when scope is TABLE (e.g. orders)")
    String getBigQueryTable();

    void setBigQueryTable(String value);

    @Description("BigQuery Write Disposition (default: WRITE_APPEND)")
    @Default.String("WRITE_APPEND")
    String getWriteDisposition();

    void setWriteDisposition(String value);

    @Description("BigQuery Create Disposition (default: CREATE_IF_NEEDED)")
    @Default.String("CREATE_IF_NEEDED")
    String getCreateDisposition();

    void setCreateDisposition(String value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setStreaming(true);

    run(options);
  }

  public static void run(Options options) {
    Pipeline pipeline = Pipeline.create(options);

    // 1. Configure PostgreSQL DataSource with Dynamic IAM if specified
    PostgreSqlDataSourceConfiguration dsConfig =
        PostgreSqlDataSourceConfiguration.create(options.getPostgresUrl())
            .withUsername(options.getPostgresUsername());

    if (options.getCloudSqlInstance() != null && !options.getCloudSqlInstance().isEmpty()) {
      dsConfig =
          dsConfig.withDynamicPasswordProvider(
              GoogleCloudSqlIamPasswordProvider.forInstance(options.getCloudSqlInstance()));
    } else if (options.getPostgresPassword() != null) {
      dsConfig = dsConfig.withPassword(options.getPostgresPassword());
    }

    // 2. Resolve Table Whitelist based on Scope (DATABASE / SCHEMA / TABLE)
    List<String> targetTables = resolveTargetTables(dsConfig, options);
    if (targetTables.isEmpty()) {
      throw new IllegalArgumentException(
          "No matching tables found for specified replication scope.");
    }
    LOG.info(
        "Configured PostgreSQL CDC streaming to BigQuery for {} tables: {}",
        targetTables.size(),
        targetTables);

    // 3. Build CDC Stream
    PostgreSqlReadCDC readTransform =
        PostgreSQLIO.readCDC()
            .withDataSourceConfiguration(dsConfig)
            .withPublicationName(options.getPublicationName())
            .withReplicationSlotName(options.getReplicationSlotName())
            .withTableWhitelist(targetTables)
            .build();

    PCollection<ChangeEvent<Row>> cdcEvents = pipeline.apply("ReadPostgresCDC", readTransform);

    // 4. Filter, Extract, and Validate Change Records
    TupleTag<Row> validRowsTag = new TupleTag<Row>("validRows") {};
    TupleTag<String> dlqTag = new TupleTag<String>("dlqRecords") {};

    PCollectionTuple processedEvents =
        cdcEvents.apply(
            "ProcessAndValidateRecords",
            ParDo.of(
                    new DoFn<ChangeEvent<Row>, Row>() {
                      @ProcessElement
                      public void processElement(
                          @Element ChangeEvent<Row> event, MultiOutputReceiver receiver) {
                        try {
                          if (event.getAfter() != null) {
                            receiver.get(validRowsTag).output(event.getAfter());
                          } else if (event.getBefore() != null) {
                            receiver.get(validRowsTag).output(event.getBefore());
                          }
                        } catch (Exception e) {
                          String errorJson =
                              String.format(
                                  "{\"table\":\"%s.%s\",\"lsn\":%d,\"error\":\"%s\"}",
                                  event.getSchemaName(),
                                  event.getTableName(),
                                  event.getLsn(),
                                  e.getMessage());
                          receiver.get(dlqTag).output(errorJson);
                        }
                      }
                    })
                .withOutputTags(validRowsTag, TupleTagList.of(dlqTag)));

    // 5. Determine BigQuery destination table spec
    String targetTableSpec =
        options.getBigQueryTable() != null
            ? (options.getBigQueryDataset() + "." + options.getBigQueryTable())
            : (options.getBigQueryDataset()
                + "."
                + (options.getTargetTable() != null
                    ? options.getTargetTable().replace(".", "_")
                    : "default_table"));

    // 6. Write to BigQuery via Managed IO
    processedEvents
        .get(validRowsTag)
        .apply(
            "WriteToBigQuery",
            Managed.write(Managed.BIGQUERY)
                .withConfig(
                    ImmutableMap.<String, Object>builder()
                        .put("table", targetTableSpec)
                        .put("write_disposition", options.getWriteDisposition())
                        .put("create_disposition", options.getCreateDisposition())
                        .build()));

    // 7. Output DLQ Records if DLQ path is specified
    if (options.getDlqPath() != null && !options.getDlqPath().isEmpty()) {
      processedEvents
          .get(dlqTag)
          .apply(
              "WriteDLQToStorage",
              TextIO.write().to(options.getDlqPath()).withWindowedWrites().withNumShards(1));
    }

    pipeline.run();
  }

  private static List<String> resolveTargetTables(
      PostgreSqlDataSourceConfiguration dsConfig, Options options) {
    if (options.getTableWhitelist() != null && !options.getTableWhitelist().isEmpty()) {
      return Arrays.asList(options.getTableWhitelist().split(","));
    }

    String scope = options.getReplicationScope().toUpperCase();
    List<String> tables = new ArrayList<>();

    try (Connection conn = dsConfig.buildRawDataSource().getConnection()) {
      String query;
      if ("TABLE".equals(scope)) {
        if (options.getTargetTable() != null && !options.getTargetTable().isEmpty()) {
          return Arrays.asList(options.getTargetTable());
        }
        throw new IllegalArgumentException(
            "--targetTable must be specified when --replicationScope=TABLE");
      } else if ("SCHEMA".equals(scope)) {
        String schema = options.getTargetSchema() != null ? options.getTargetSchema() : "public";
        query =
            "SELECT table_schema || '.' || table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'";
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
          stmt.setString(1, schema);
          try (ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
              tables.add(rs.getString(1));
            }
          }
        }
      } else { // DATABASE
        query =
            "SELECT table_schema || '.' || table_name FROM information_schema.tables "
                + "WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast') AND table_type = 'BASE TABLE'";
        try (PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery()) {
          while (rs.next()) {
            tables.add(rs.getString(1));
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to dynamically inspect PostgreSQL tables for scope {}", scope, e);
      if (options.getTargetTable() != null) {
        tables.add(options.getTargetTable());
      }
    }

    return tables;
  }
}
