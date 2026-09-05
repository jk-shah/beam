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
package org.apache.beam.sdk.io.postgres;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import org.apache.beam.sdk.io.postgres.sink.PostgreSqlWrite;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * End-to-End Integration Test demonstrating complex PostgreSQL-to-PostgreSQL pipelines in Java:
 * 1. Pure Replication
 * 2. Filtering Pipeline
 * 3. Complex Transformation & PII Masking Pipeline
 */
@RunWith(JUnit4.class)
public class PostgreSqlComplexPipelineIT implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/postgres";
  private static final String DB_USER = "beam_test";
  private static final String DB_PASS = "beam_password";

  public static final Schema SOURCE_SCHEMA =
      Schema.builder()
          .addInt64Field("order_id")
          .addStringField("customer_id")
          .addStringField("customer_email")
          .addDecimalField("amount")
          .addStringField("status")
          .addStringField("country_code")
          .addInt32Field("items_count")
          .addDateTimeField("created_at")
          .build();

  public static final Schema TRANSFORMED_SCHEMA =
      Schema.builder()
          .addInt64Field("order_id")
          .addStringField("customer_id")
          .addStringField("masked_email")
          .addDecimalField("net_amount")
          .addDecimalField("processing_fee")
          .addStringField("customer_tier")
          .addStringField("status")
          .addStringField("country_code")
          .addInt32Field("items_count")
          .addDateTimeField("processed_at")
          .build();

  @Before
  public void cleanTargetTables() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASS);
        Statement stmt = conn.createStatement()) {
      stmt.execute("TRUNCATE TABLE test_pipelines.target_orders_replicated;");
      stmt.execute("TRUNCATE TABLE test_pipelines.target_orders_filtered;");
      stmt.execute("TRUNCATE TABLE test_pipelines.target_orders_transformed;");
    }
  }

  @Test
  public void testPureReplicationPipeline() throws Exception {
    PostgreSqlDataSourceConfiguration config =
        PostgreSqlDataSourceConfiguration.create(JDBC_URL)
            .withUsername(DB_USER)
            .withPassword(DB_PASS);

    PCollection<Row> sourceRows =
        pipeline
            .apply("Trigger", Create.of(0L))
            .apply("ReadSourceOrders", ParDo.of(new ReadSourceOrdersFn(JDBC_URL, DB_USER, DB_PASS)))
            .setRowSchema(SOURCE_SCHEMA);

    sourceRows.apply(
        "WriteReplicatedOrders",
        PostgreSQLIO.write()
            .withDataSourceConfiguration(config)
            .to("test_pipelines.target_orders_replicated")
            .withPrimaryKeyColumns(Collections.singletonList("order_id"))
            .withWriteMode(PostgreSqlWrite.WriteMode.STREAMING_UPSERT_UNNEST)
            .build());

    pipeline.run().waitUntilFinish();

    try (Connection conn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASS);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM test_pipelines.target_orders_replicated;")) {
      assertTrue(rs.next());
      assertEquals(20, rs.getInt(1));
    }
  }

  @Test
  public void testFilteringPipeline() throws Exception {
    PostgreSqlDataSourceConfiguration config =
        PostgreSqlDataSourceConfiguration.create(JDBC_URL)
            .withUsername(DB_USER)
            .withPassword(DB_PASS);

    PCollection<Row> sourceRows =
        pipeline
            .apply("Trigger", Create.of(0L))
            .apply("ReadSourceOrders", ParDo.of(new ReadSourceOrdersFn(JDBC_URL, DB_USER, DB_PASS)))
            .setRowSchema(SOURCE_SCHEMA);

    PCollection<Row> filtered =
        sourceRows
            .apply(
                "FilterCompletedHighValue",
                Filter.by(
                    (Row row) -> {
                      String status = row.getString("status");
                      BigDecimal amount = row.getDecimal("amount");
                      return "COMPLETED".equals(status)
                          && amount != null
                          && amount.compareTo(new BigDecimal("100.00")) >= 0;
                    }))
            .setRowSchema(SOURCE_SCHEMA);

    filtered.apply(
        "WriteFilteredOrders",
        PostgreSQLIO.write()
            .withDataSourceConfiguration(config)
            .to("test_pipelines.target_orders_filtered")
            .withPrimaryKeyColumns(Collections.singletonList("order_id"))
            .withWriteMode(PostgreSqlWrite.WriteMode.STREAMING_UPSERT_UNNEST)
            .build());

    pipeline.run().waitUntilFinish();

    try (Connection conn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASS);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM test_pipelines.target_orders_filtered;")) {
      assertTrue(rs.next());
      assertEquals(13, rs.getInt(1));
    }
  }

  @Test
  public void testComplexTransformationPipeline() throws Exception {
    PostgreSqlDataSourceConfiguration config =
        PostgreSqlDataSourceConfiguration.create(JDBC_URL)
            .withUsername(DB_USER)
            .withPassword(DB_PASS);

    PCollection<Row> sourceRows =
        pipeline
            .apply("Trigger", Create.of(0L))
            .apply("ReadSourceOrders", ParDo.of(new ReadSourceOrdersFn(JDBC_URL, DB_USER, DB_PASS)))
            .setRowSchema(SOURCE_SCHEMA);

    PCollection<Row> transformed =
        sourceRows
            .apply("TransformAndEnrich", ParDo.of(new EnrichOrderDoFn()))
            .setRowSchema(TRANSFORMED_SCHEMA);

    transformed.apply(
        "WriteTransformedOrders",
        PostgreSQLIO.write()
            .withDataSourceConfiguration(config)
            .to("test_pipelines.target_orders_transformed")
            .withPrimaryKeyColumns(Collections.singletonList("order_id"))
            .withWriteMode(PostgreSqlWrite.WriteMode.STREAMING_UPSERT_UNNEST)
            .build());

    pipeline.run().waitUntilFinish();

    try (Connection conn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASS);
        Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM test_pipelines.target_orders_transformed;")) {
        assertTrue(rs.next());
        assertEquals(20, rs.getInt(1));
      }
      try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM test_pipelines.target_orders_transformed WHERE customer_tier = 'VIP';")) {
        assertTrue(rs.next());
        assertEquals(7, rs.getInt(1));
      }
    }
  }

  public static class ReadSourceOrdersFn extends DoFn<Long, Row> {
    private final String url;
    private final String user;
    private final String password;

    public ReadSourceOrdersFn(String url, String user, String password) {
      this.url = url;
      this.user = user;
      this.password = password;
    }

    @ProcessElement
    public void processElement(OutputReceiver<Row> out) throws Exception {
      try (Connection conn = DriverManager.getConnection(url, user, password);
          Statement stmt = conn.createStatement();
          ResultSet rs =
              stmt.executeQuery(
                  "SELECT order_id, customer_id, customer_email, amount, status, country_code, items_count, created_at FROM test_pipelines.source_orders ORDER BY order_id")) {
        while (rs.next()) {
          Row row =
              Row.withSchema(SOURCE_SCHEMA)
                  .addValue(rs.getLong("order_id"))
                  .addValue(rs.getString("customer_id"))
                  .addValue(rs.getString("customer_email"))
                  .addValue(rs.getBigDecimal("amount"))
                  .addValue(rs.getString("status"))
                  .addValue(rs.getString("country_code"))
                  .addValue(rs.getInt("items_count"))
                  .addValue(new DateTime(rs.getTimestamp("created_at").getTime(), DateTimeZone.UTC))
                  .build();
          out.output(row);
        }
      }
    }
  }

  public static class EnrichOrderDoFn extends DoFn<Row, Row> {
    @ProcessElement
    public void processElement(@Element Row in, OutputReceiver<Row> out) {
      String email = in.getString("customer_email");
      String maskedEmail = "***";
      if (email != null && email.contains("@")) {
        String[] parts = email.split("@", 2);
        String prefix = parts[0].length() >= 3 ? parts[0].substring(0, 3) : parts[0];
        maskedEmail = prefix + "***@" + parts[1];
      }

      BigDecimal amount = in.getDecimal("amount");
      if (amount == null) {
        amount = BigDecimal.ZERO;
      }
      BigDecimal feeRate = new BigDecimal("0.025");
      BigDecimal fee = amount.multiply(feeRate).setScale(2, RoundingMode.HALF_UP);
      BigDecimal net = amount.subtract(fee).setScale(2, RoundingMode.HALF_UP);

      String tier = amount.compareTo(new BigDecimal("1000.00")) >= 0 ? "VIP" : "STANDARD";

      Row enriched =
          Row.withSchema(TRANSFORMED_SCHEMA)
              .addValue(in.getInt64("order_id"))
              .addValue(in.getString("customer_id"))
              .addValue(maskedEmail)
              .addValue(net)
              .addValue(fee)
              .addValue(tier)
              .addValue(in.getString("status"))
              .addValue(in.getString("country_code"))
              .addValue(in.getInt32("items_count"))
              .addValue(DateTime.now(DateTimeZone.UTC))
              .build();
      out.output(enriched);
    }
  }
}
