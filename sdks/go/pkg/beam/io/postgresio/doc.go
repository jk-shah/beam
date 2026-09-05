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
// 4. Connection Pool & PgBouncer Safety: Automatically clamps worker connection pool
// limits based on available CPU cores (NumCPU/2) and supports PgBouncer transaction
// pooling mode by enforcing simple query protocol execution to prevent SQLState 42P05
// prepared statement collisions.
//
// 5. Cloud IAM Ready: Decoupled network dialer interface (DialFunc) allowing direct,
// zero-dependency integration with Google Cloud SQL, AlloyDB, and AWS RDS IAM.
package postgresio
