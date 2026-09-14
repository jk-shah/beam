// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package postgresio provides native, high-performance Apache Beam I/O transforms
// for writing to and streaming from PostgreSQL databases without Java Virtual Machine
// (JVM) dependencies.
//
// # Status
//
// This package is unreleased and under active remediation. It is not ready for
// production use. A security and correctness review identified defects that affect
// the source database rather than only the pipeline:
//
//   - The replication slot is not acknowledged in production. ProcessElement does not
//     return, so no bundle finalizes and the BundleFinalization callback that advances
//     confirmed_flush_lsn never runs. Write-ahead log accumulates on the primary until
//     its volume fills.
//   - TLS is disabled by default. An unset sslmode skips negotiation entirely, so
//     credentials are sent in cleartext, and no option exists for supplying a CA bundle.
//   - SCRAM-SHA-256 is unsupported, so a default PostgreSQL 14+ server cannot be reached.
//   - No watermark is produced, so downstream windows cannot close reliably.
//   - There is no initial snapshot, so pre-existing rows are never emitted.
//
// See the Known Limitations section of the package README for the full list and the
// current state of each item.
//
// # Key Capabilities
//
// 1. Parameterized UNNEST Array Upserts: Executes high-throughput bulk inserts
// and idempotent upserts (INSERT INTO ... ON CONFLICT DO UPDATE) using vectorized
// array parameters, eliminating system catalog lock contention (pg_class, pg_attribute).
//
// 2. Deadlock Elimination (Anti-40P01): Employs an in-memory BatchCompactor that
// deduplicates micro-batches via Last-Write-Wins (LWW) and sorts records canonically
// by composite primary key prior to database transmission, preventing PostgreSQL
// SQLState 40P01 deadlocks across distributed workers.
//
// 3. Multi-Output Dead-Letter Queue (DLQ): Returns a WriteResult struct separating
// successfully committed records from rejected records (with sanitized error messages
// and SQL states), preventing credential disclosure in logs or queues.
//
// 4. Change Data Capture Streaming (ReadCDC): Directly streams continuous change events
// (INSERT, UPDATE, DELETE, TRUNCATE) from PostgreSQL logical replication slots using a
// native binary pgoutput wire decoder, eliminating Debezium and external JVM processes.
//
// 5. Decoupled Heartbeat & Checkpointing Design: Implements an asynchronous keepalive
// goroutine sending StandbyStatusUpdate messages to prevent PostgreSQL wal_sender_timeout
// (60s) drops during downstream backpressure. Confirmed FlushLSN is coordinated strictly
// via Beam's BundleFinalizer so that no LSN is acknowledged for data Beam has not durably
// committed. Note that this is the intended design; see Status above for the defect that
// currently prevents the acknowledgment from advancing at all.
//
// 6. Stateful TOAST Reassembly (ReassembleToast): Automatically caches baseline tuples
// in Beam runner state (state.Value[ChangeEvent]) and reassembles unmodified out-of-line
// TOAST attributes ('u') on UPDATE events when REPLICA IDENTITY DEFAULT is in use.
//
// 7. Single-Consumer Ingestion with Auto-Partitioned Fanout (PartitionByPrimaryKey):
// Enforces strict single-consumer execution at the replication slot boundary (Parallelism = 1)
// while providing immediate downstream reshuffling and primary-key partitioning across cluster workers.
//
// 8. Cloud IAM & Dynamic Auth: Decoupled TokenProvider and DialFunc interfaces allowing
// dynamic credential renewal across reconnects for Google Cloud SQL, AlloyDB, and AWS RDS IAM.
package postgresio
