# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Stage 1: Build all 22 Beam PostgreSQL example binaries
FROM golang:alpine AS builder
ENV GOTOOLCHAIN=auto
ENV CGO_ENABLED=0

WORKDIR /workspace

# Copy Go SDK module definitions and source tree
COPY sdks/go.mod sdks/go.sum /workspace/sdks/
COPY sdks/go /workspace/sdks/go

WORKDIR /workspace/sdks

# Compile batch and operational example binaries
RUN mkdir -p /out && \
    go build -o /out/top_n_ranking ./go/examples/postgres/advanced_use_cases/top_n_ranking && \
    go build -o /out/deduplication ./go/examples/postgres/deduplication && \
    go build -o /out/data_reconciliation_diff ./go/examples/postgres/advanced_use_cases/data_reconciliation_diff && \
    go build -o /out/scd_type2 ./go/examples/postgres/scd_type2 && \
    go build -o /out/multi_dimensional_olap ./go/examples/postgres/advanced_use_cases/multi_dimensional_olap && \
    go build -o /out/sessionization ./go/examples/postgres/advanced_use_cases/sessionization && \
    go build -o /out/graph_vertex_degrees ./go/examples/postgres/advanced_use_cases/graph_vertex_degrees && \
    go build -o /out/ml_feature_engineering ./go/examples/postgres/advanced_use_cases/ml_feature_engineering && \
    go build -o /out/vectorized_batch_etl ./go/examples/postgres/vectorized_batch_etl && \
    go build -o /out/partitioned_sink ./go/examples/postgres/operational_patterns/partitioned_sink && \
    go build -o /out/local_inference ./go/examples/postgres/local_inference && \
    go build -o /out/lookup_enrichment ./go/examples/postgres/lookup_enrichment && \
    go build -o /out/relational_enrichment ./go/examples/postgres/relational_enrichment && \
    go build -o /out/dead_letter_queue ./go/examples/postgres/dead_letter_queue && \
    go build -o /out/sequence_migration ./go/examples/postgres/operational_patterns/sequence_migration && \
    go build -o /out/pgbouncer_compat ./go/examples/postgres/operational_patterns/pgbouncer_compat && \
    go build -o /out/quickstart ./go/examples/postgres/quickstart && \
    go build -o /out/streaming_aggregation ./go/examples/postgres/streaming_aggregation && \
    go build -o /out/backfill_cutover ./go/examples/postgres/operational_patterns/backfill_cutover && \
    go build -o /out/schema_evolution ./go/examples/postgres/operational_patterns/schema_evolution && \
    go build -o /out/failover_recovery ./go/examples/postgres/operational_patterns/failover_recovery && \
    go build -o /out/geo_fanout_masking ./go/examples/postgres/geo_fanout_masking

# Stage 2: Runtime image equipped with compiled binaries, Go toolchain, and psql client
FROM golang:alpine

RUN apk add --no-cache ca-certificates bash curl postgresql17-client

WORKDIR /workspace/sdks

# Copy Go SDK source tree for dynamic runs and testing
COPY --from=builder /workspace/sdks /workspace/sdks

# Copy all compiled pipeline binaries
COPY --from=builder /out/* /usr/local/bin/

# Default connection environment targeting docker-compose services
ENV PGHOST=postgres \
    PGPORT=5432 \
    PGDATABASE=beammeup \
    PGUSER=beam_navigator \
    PGPASSWORD=beam_navigator \
    PGBOUNCER_HOST=pgbouncer \
    PGBOUNCER_PORT=6432

CMD ["tail", "-f", "/dev/null"]
