<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Product Design Document (PDD): PostgreSQLIO Developer Experience & Pipeline Workflows

This document outlines the user experience (UX), developer workflows (DevX), monitoring interfaces, and failure triage runbooks for the Apache Beam `PostgreSQLIO` connector.

---

## 1. Developer Experience (DevX) Principles

1. **Zero-Boilerplate Declarative APIs**: Developers configure complex CDC pipelines using minimal parameters (`url`, `table`, `slotName`, `publicationName`) without writing custom SQL parsers or socket code.
2. **Multi-SDK Ergonomic Parity**: Fluent, idiom-preserving APIs in Java, Python, Go, and Beam YAML.
3. **Safe-by-Default Operation**: Automated SQL identifier escaping, deadlock-prevention canonical sorting, non-blocking connection pool initialization, and automated IAM credential recycling.

---

## 2. Pipeline Authoring & Deployment Workflows

```
                          DEVELOPER PIPELINE AUTHORING
 ┌───────────────────────────┐      ┌───────────────────────────┐      ┌───────────────────────────┐
 │ Java / Python / Go Code   │  OR  │ Beam YAML Definition      │  OR  │ Flex / Classic Template   │
 └─────────────┬─────────────┘      └─────────────┬─────────────┘      └─────────────┬─────────────┘
               │                                  │                                  │
               └──────────────────────────────────┼──────────────────────────────────┘
                                                  │
                                                  ▼
                                     ┌─────────────────────────┐
                                     │ Beam Pipeline Execution │
                                     │ (Direct / Dataflow /    │
                                     │  Flink / Spark Runner)  │
                                     └────────────┬────────────┘
                                                  │
                                                  ▼
                                     ┌─────────────────────────┐
                                     │ Continuous Monitoring & │
                                     │ Live Telemetry Metrics  │
                                     └─────────────────────────┘
```

### A. Deploying via Dataflow Flex Templates
```bash
gcloud dataflow flex-template run postgres-to-bigquery-cdc-$(date +%s) \
    --template-file-gcs-location="gs://dataflow-templates/latest/flex/PostgreSql_to_BigQuery" \
    --region="us-central1" \
    --parameters \
        databaseUrl="jdbc:postgresql://10.0.0.1:5432/ecommerce",\
        tableName="public.orders",\
        slotName="orders_cdc_slot",\
        publicationName="orders_pub",\
        outputTableSpec="my-project:ecommerce.orders_cdc",\
        enableIamAuth=true,\
        cloudSqlInstanceConnectionName="my-project:us-central1:pg-instance"
```

---

## 3. Observability & Monitoring Dashboards

The connector exports standard Beam metrics and OpenTelemetry gauges visible in runner consoles (e.g. Google Cloud Dataflow Monitoring, Apache Flink Web Dashboard):

```
+---------------------------------------------------------------------------------------------------+
| PIPELINE STAGE: ReadPostgresCDC / PostgreSqlWalReaderDoFn                                          |
|                                                                                                   |
| [Throughput Meter] ─────────────────────── 620,000 ops/sec                                        |
| [Replication WAL Lag Gauge] ────────────── 0 bytes (Optimal Drained)                              |
| [Heartbeat Round-Trip Gauge] ───────────── 42 ms                                                  |
| [TOAST Cache Hit Ratio] ────────────────── 99.98% (0 point queries to database)                   |
+---------------------------------------------------------------------------------------------------+
| PIPELINE STAGE: WriteToPostgres / UnnestArrayUpsertWriterDoFn                                     |
|                                                                                                   |
| [Sink Write Throughput Meter] ──────────── 105,000 rows/sec                                       |
| [Active Connection Pool Gauge] ─────────── 4 / 8 connections (Healthy)                            |
| [Intra-Bundle Compacted Records] ───────── 1,420 records deduplicated                             |
| [Dead-Letter Queue Errors (DLQ)] ───────── 0 records                                              |
+---------------------------------------------------------------------------------------------------+
```

---

## 4. Dead-Letter Queue (DLQ) & Error Triage Runbook

When a row fails database constraint checks (e.g., foreign key violation, schema mismatch, or non-retryable SQL exception):
1. **Error Isolation**: The row is intercepted by `SanitizingExceptionTransformer`, scrubbed of sensitive connection strings or passwords, and routed to the `getFailedRows()` PCollection.
2. **Structured Error Payload (`PostgreSqlWriteError`)**:
   * `getFailedRow()`: The original input `Row` that failed insertion.
   * `getSqlState()`: The PostgreSQL standard SQLState code (e.g. `23505` for unique violation, `23503` for foreign key violation).
   * `getErrorMessage()`: Sanitized database error diagnostic message.
   * `getTimestamp()`: Timestamp of the failed attempt.
3. **Dead-Letter Storage**: Production pipelines route failed rows to GCS dead-letter files, BigQuery error tables, or Pub/Sub alerts for automated operator notification.

---

## 5. Security & Multi-Cloud Identity Workflow

* **Cloud SQL & AlloyDB (Google Cloud)**: Zero credential management via IAM database authentication (`withEnableIamAuth(true)`). OAuth2 tokens refreshed automatically via GoogleCredentials.
* **AWS RDS & Aurora (AWS)**: IAM token generation via AWS SDK v2 RDS Utilities.
* **Secret Manager**: URI-driven secret lookup (`sm://projects/.../secrets/...`) with automatic 15-minute rotation refresh.
