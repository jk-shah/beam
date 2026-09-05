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

# Contributing to Apache Beam Go `postgresio`

Thank you for your interest in contributing to the Apache Beam Go PostgreSQL I/O connector (`postgresio`). This guide provides technical standards and conventions for contributors.

---

## 1. Development Principles

1. **Pure Go Without JNI or JVM Wrappers**:
   The Go connector is intentionally self-contained. Do not introduce CGo dependencies (`CGO_ENABLED=0` invariant) or depend on JVM expansion services for native Go execution.
2. **Zero-Allocation Hot Paths**:
   The streaming CDC decoding and Arrow micro-batching pipelines operate on continuous high-throughput streams (millions of ops/sec). Avoid allocations inside per-row processing loops. Pre-allocate slices and reuse builders where possible.
3. **Database Guardrails**:
   Protect the upstream PostgreSQL primary database:
   * Maintain a single logical replication slot consumer (`Parallelism = 1` at the slot boundary).
   * Clamp worker write connection pools to `max(1, NumCPU / 2)` to avoid saturating PostgreSQL's `max_connections`.
   * Enforce Last-Write-Wins (LWW) batch compaction and canonical primary-key ordering to eliminate `40P01` deadlocks.
4. **Factual and Objective Documentation**:
   Maintain neutral and objective engineering language in all comments, docstrings, commit messages, and PR descriptions. Avoid subjective or hyperbolic modifiers.

---

## 2. Setting Up the Local Development Environment

### Prerequisites
* Go 1.21 or higher
* Docker or native PostgreSQL 15–18
* Git

### Local PostgreSQL Setup
Start a local PostgreSQL container configured for logical replication:

```bash
docker run --name beam-postgres -p 5432:5432 \
  -e POSTGRES_USER=beam_test \
  -e POSTGRES_PASSWORD=beam_password \
  -e POSTGRES_DB=postgres \
  -d postgres:18-alpine \
  postgres -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10
```

Verify connection:
```bash
psql -h localhost -U beam_test -d postgres -c "SELECT version();"
```

---

## 3. Adding Support for a New PostgreSQL Type

When adding support for a new PostgreSQL data type (e.g. `pgvector`, custom enum, geospatial):

1. **Type OID & Decoders**:
   Add decoding logic in [`cdc_decoders.go`](cdc_decoders.go). Handle text and binary formats as emitted by `pgoutput`.
2. **Arrow Vectorized Engine**:
   Extend [`arrow_batcher.go`](arrow_batcher.go) and [`arrow_decoder.go`](arrow_decoder.go) with the corresponding Apache Arrow array type (e.g., `arrow.FixedSizeList` for vectors).
3. **Write Array Cast**:
   Update `goTypeToPgArrayType` in [`write.go`](write.go) to emit the correct PostgreSQL array type cast for parameterized `UNNEST` queries (e.g., `$1::vector[]`).
4. **Unit Tests**:
   Add test cases to [`cdc_range_decoder_test.go`](cdc_range_decoder_test.go) or create a targeted `_test.go` file.

---

## 4. Code Style & Registration Invariants

### Beam Type Registration Rules
* **Schema Registration**:
  Do **not** call `beam.RegisterType` on types that contain `any`, `interface{}`, or `func` fields. Doing so causes Beam's schema registry to fail at pipeline initialization.
* **Non-Serializable Struct Tags**:
  Always add `beam:"-" json:"-"` to any field containing interfaces, custom dialers, or closures.
* **Custom Coders**:
  For polymophic types (e.g., `ChangeEvent`), register a custom coder using `beam.RegisterCoder`:
  ```go
  beam.RegisterCoder(
      reflect.TypeOf((*MyType)(nil)).Elem(),
      encodeMyType,
      decodeMyType,
  )
  ```
* **DoFn Registration**:
  Always register DoFn structs using `beam.RegisterDoFn(&MyDoFn{})`.

---

## 5. Verification Checklist

Before submitting a pull request:

- [ ] `go test -v -race ./...` passes with zero race detector warnings.
- [ ] Code is formatted with `gofmt -s -w .`.
- [ ] No personal usernames or credentials in tests, scripts, or documentation (use role `beam_test`).
- [ ] Apache 2.0 license header is present on every new file.
- [ ] Integration tests pass against a live PostgreSQL 18 instance.
- [ ] Benchmark allocations remain at 0 allocs/op for the hot decoding path.
