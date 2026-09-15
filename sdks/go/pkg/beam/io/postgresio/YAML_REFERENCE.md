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

# PostgreSQL YAML Schema Reference

This reference is programmatically generated from the registered Apache Beam Go SchemaTransform providers.
Do not edit this document manually; update the struct tags in `schematransform.go` and regenerate.

## WriteToPostgres

* **URN:** `beam:schematransform:org.apache.beam:postgres_write:v1`
* **Description:** High-performance PostgreSQL Write transform using parameterized UNNEST upserts, buffer compaction, and DLQ routing.
* **Type:** Sink

### Configuration Parameters

| Field | Type | Secret | Description |
| :--- | :--- | :--- | :--- |
| `host` | string | No | PostgreSQL database server hostname or IP address. |
| `port` | integer (int32) | No | PostgreSQL database server port (default: 5432). |
| `database` | string | No | Target PostgreSQL database name. |
| `table` | string | No | Target PostgreSQL table, schema-qualified (for example public.orders). |
| `username` | string | No | Authentication username. |
| `password` | string | Yes | Authentication password. |
| `password_env_var` | string | No | Environment variable name on the worker containing the authentication password. |
| `sslmode` | string | No | SSL mode (e.g. disable, require, verify-ca, verify-full). |
| `conflict_keys` | list[string] | No | Columns used as primary or unique key conflict targets for UPSERT. |
| `update_fields` | list[string] | No | Columns to update ON CONFLICT DO UPDATE. If empty, uses DO NOTHING. |
| `max_batch_rows` | integer (int32) | No | Maximum rows per batch UNNEST statement (default: 5000). |
| `max_batch_bytes` | integer (int32) | No | Maximum byte buffer threshold before flushing (default: 8MB). |
| `use_pgbouncer` | boolean | No | Enable single-statement transaction pooling for PgBouncer compatibility. |
| `replication_origin` | string | No | Replication origin name to tag write transactions to prevent cyclic loops. |
| `write_mode` | string | No | Write mutation mode (INSERT, UPSERT, UPDATE, MERGE). Default UPSERT. |
| `op_column` | string | No | Column name containing CDC operation type for MERGE mode. |
| `delete_op_value` | string | No | Value in op_column that indicates a DELETE in MERGE mode. |
| `explain_analyze` | boolean | No | Enable in-band EXPLAIN (ANALYZE, BUFFERS) query plan sampling on sink batches. |

## ReadFromPostgresCDC

* **URN:** `beam:schematransform:org.apache.beam:postgres_read_cdc:v1`
* **Description:** Continuous PostgreSQL Change Data Capture (CDC) streaming source using logical replication and pgoutput with optional Apache Arrow micro-batching.
* **Type:** Source

### Configuration Parameters

| Field | Type | Secret | Description |
| :--- | :--- | :--- | :--- |
| `host` | string | No | PostgreSQL database server hostname or IP address. |
| `port` | integer (int32) | No | PostgreSQL database server port (default: 5432). |
| `database` | string | No | Source PostgreSQL database name. |
| `slot_name` | string | No | Logical replication slot name. |
| `publication` | string | No | PostgreSQL publication name to capture. |
| `username` | string | No | Replication user name. |
| `password` | string | Yes | Replication user password. |
| `password_env_var` | string | No | Environment variable name on the worker containing the replication password. |
| `sslmode` | string | No | SSL mode (e.g. disable, require, verify-ca, verify-full). |
| `tables` | list[string] | No | Optional list of tables to capture (empty captures all in publication). |
| `origin_filter` | string | No | Replication origin filter: 'all' (default) or 'none'. |
| `output_format` | string | No | Output format: 'row' (default) or 'arrow'. |
| `arrow_batch_rows` | integer (int32) | No | Maximum rows per Arrow batch when output_format='arrow' (default: 4096). |
| `proto_version` | integer (int32) | No | pgoutput protocol version (0 for auto-negotiation: 4 on PG >= 19, 1 on older). |
| `binary_mode` | boolean | No | Whether column values are streamed in binary format (auto: true on PG >= 19). |
| `streaming_mode` | string | No | In-progress transaction streaming mode (auto: 'parallel' on PG >= 19). |
| `failover_slot` | boolean | No | Whether to create replication slot with FAILOVER option (PostgreSQL 17+). |
| `publication_tables` | list[object] | No | Optional list of table-specific column lists and row filters for publication (PostgreSQL 15+). |
