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

import org.apache.beam.sdk.io.postgres.cdc.PostgreSqlReadCDC;
import org.apache.beam.sdk.io.postgres.sink.PostgreSqlWrite;
import org.apache.beam.sdk.values.Row;

/**
 * An Apache Beam connector for high-throughput, low-latency interactions with PostgreSQL database
 * systems, featuring native Change Data Capture (CDC) Splittable DoFns and streaming multi-mode
 * sink engines.
 *
 * <h3>1. Before you start</h3>
 *
 * <p>To use {@link PostgreSQLIO} for Change Data Capture (CDC), configure the PostgreSQL database:
 *
 * <pre>{@code
 * -- 1. Configure postgresql.conf:
 * -- wal_level = logical
 * -- max_replication_slots = 10
 * -- max_wal_senders = 10
 *
 * -- 2. Create the replication publication:
 * CREATE PUBLICATION beam_pub FOR TABLE public.orders, public.customers;
 *
 * -- 3. Create the logical replication slot:
 * SELECT pg_create_logical_replication_slot('beam_slot', 'pgoutput');
 *
 * -- 4. Grant replication privileges:
 * GRANT REPLICATION TO beam_user;
 * GRANT SELECT ON ALL TABLES IN SCHEMA public TO beam_user;
 * }</pre>
 *
 * <h3>2. PostgreSQLIO basics</h3>
 *
 * <p>{@link PostgreSQLIO} provides unified streaming CDC ingestion and vectorized write sinks:
 *
 * <ul>
 *   <li>{@link #readCDC()}: Ingests live database mutations using PostgreSQL's binary {@code
 *       pgoutput} protocol with lock-free watermark snapshotting and stateful TOAST reconstruction.
 *   <li>{@link #write()}: Executes high-velocity batch and streaming upserts using parameterized
 *       {@code UNNEST(ARRAY[...])} queries or binary staged {@code COPY} temp-table merges.
 * </ul>
 *
 * <h3>3. Supported Features</h3>
 *
 * <table border="1" cellpadding="4">
 *   <tr><th>Feature</th><th>Supported</th><th>Notes</th></tr>
 *   <tr><td>Change Data Capture (CDC)</td><td>Yes</td><td>Continuous WAL streaming via {@code pgoutput} wire protocol</td></tr>
 *   <tr><td>Lock-Free Watermark Snapshots</td><td>Yes</td><td>Non-blocking table chunk reads interleaved with live CDC stream</td></tr>
 *   <tr><td>Stateful TOAST Reconstruction</td><td>Yes</td><td>Merges unchanged out-of-line TOAST values via runner-managed state</td></tr>
 *   <tr><td>Dynamic Schema Evolution</td><td>Yes</td><td>Automatic projection across in-flight DDL schema mutations</td></tr>
 *   <tr><td>Multi-Mode Upsert Sinks</td><td>Yes</td><td>Parameterized {@code UNNEST} array upserts and staged binary {@code COPY}</td></tr>
 *   <tr><td>Dead-Letter Queue (DLQ)</td><td>Yes</td><td>Structured {@code PostgreSqlWriteError} routing with SQLState tagging</td></tr>
 *   <tr><td>Replication Loop Prevention</td><td>Yes</td><td>Session origin stamping via {@code pg_replication_origin_session_setup}</td></tr>
 * </table>
 *
 * <h3>4. Authentication</h3>
 *
 * <p>Supports static credentials, Google Cloud SQL IAM, Google Cloud AlloyDB IAM, AWS RDS IAM, and
 * URI-driven Google Cloud Secret Manager resolution:
 *
 * <pre>{@code
 * // Cloud SQL IAM authentication
 * PostgreSqlDataSourceConfiguration config =
 *     PostgreSqlDataSourceConfiguration.createForCloudSql("project:region:instance", "mydb")
 *         .withUsername("sa@project.iam")
 *         .withEnableIamAuth(true);
 * }</pre>
 *
 * <h3>5. Reading from PostgreSQL</h3>
 *
 * <pre>{@code
 * PCollection<ChangeEvent<Row>> changes = pipeline
 *     .apply(PostgreSQLIO.readCDC()
 *         .withDataSourceConfiguration(config)
 *         .withPublicationName("beam_pub")
 *         .withReplicationSlotName("beam_slot")
 *         .withTable("public.orders"));
 * }</pre>
 *
 * <h3>6. Writing to PostgreSQL</h3>
 *
 * <pre>{@code
 * PostgreSqlWriteResult result = rows.apply(PostgreSQLIO.write()
 *     .withDataSourceConfiguration(config)
 *     .to("public.orders_summary")
 *     .withPrimaryKeyColumns(Collections.singletonList("order_id"))
 *     .withWriteMode(PostgreSqlWrite.WriteMode.STREAMING_UPSERT_UNNEST));
 *
 * // Dead-Letter Queue (DLQ) error handling
 * result.getFailedRows().apply("HandleErrors", ParDo.of(new DeadLetterSinkDoFn()));
 * }</pre>
 *
 * <h3>7. Resource scalability</h3>
 *
 * <p>Partitioning across cluster workers is managed via deterministic key-based channel routing and
 * connection pooling with {@code HikariCP}. Worker pools use non-blocking initialization ({@code
 * initializationFailTimeout = -1}) to ensure zero graph construction deadlocks.
 *
 * <h3>8. Limitations</h3>
 *
 * <ul>
 *   <li>Requires PostgreSQL 10+ for logical replication.
 *   <li>DDL alterations require table publication refresh if publication is scoped to specific
 *       tables.
 * </ul>
 *
 * <h3>9. Reporting an Issue</h3>
 *
 * <p>To report bugs or feature requests, file a GitHub issue at <a
 * href="https://github.com/apache/beam/issues">https://github.com/apache/beam/issues</a> with the
 * label {@code [io-postgres]}.
 */
public class PostgreSQLIO {

  /** Returns a builder for reading change data capture streams from PostgreSQL. */
  public static PostgreSqlReadCDC.Builder readCDC() {
    return PostgreSqlReadCDC.builder();
  }

  /** Returns a builder for writing {@link Row} streams to PostgreSQL. */
  public static PostgreSqlWrite.Builder write() {
    return PostgreSqlWrite.builder();
  }

  private PostgreSQLIO() {}
}
