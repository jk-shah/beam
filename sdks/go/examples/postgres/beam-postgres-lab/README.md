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

# Beam PostgreSQL lab

A self-contained PostgreSQL environment for the `postgresio` examples and for
the connector's own integration tests.

## What it starts

| Service | Container | Default host port | Purpose |
| --- | --- | --- | --- |
| `postgres` | `beam-lab-postgres` | `55432` | PostgreSQL 17, `wal_level=logical` |
| `pgbouncer` | `beam-lab-pgbouncer` | `56432` | Transaction pooling, for the PgBouncer example |
| `airflow` | `beam-lab-airflow` | `8088` | Runs the DAGs in `dags/` |
| `beam-runner` | `beam-lab-runner` | -- | Go toolchain plus the prebuilt example binaries |

Ports are deliberately non-standard so the lab does not collide with a
PostgreSQL server you already run. Change them in `.env`.

The server is started with `password_encryption=scram-sha-256` and no trust
authentication, so every connection must present a correct password.

```bash
docker compose up -d           # everything
docker compose up -d postgres  # just the database
docker compose down -v         # stop and discard all data
```

`init-lab.sql` runs only when the data directory is empty. After changing it,
`docker compose down -v` before bringing the lab back up.

## Roles

| Role | Password | Database | Used by |
| --- | --- | --- | --- |
| `postgres` | `secret` | all | administration |
| `scotty` | `scotty_secret` | `beammeup` | CDC and replication tutorials |
| `beam_navigator` | `beam_navigator` | `beammeup`, regional DBs | sink and batch tutorials |
| `beam_transporter` | `beam_transporter_pass` | `beammeup` | PgBouncer pooled sink |
| `beam_test` | `beam_test` | `postgres` | the connector's integration tests |

Tutorial data lives in `beammeup`. The connector's test fixtures live in the
`postgres` database, in the `test_pipelines` schema, and are created by
section 8 of `init-lab.sql`.

## Running the connector integration tests

The live tests in `sdks/go/pkg/beam/io/postgresio` dial `localhost:5432` as
`beam_test`. They call `t.Skipf` when the server is unreachable, so a run
against nothing reports success without testing anything. Check for `--- PASS`,
not merely a zero exit code.

If port 5432 is free on your machine:

```bash
POSTGRES_PORT=5432 docker compose up -d postgres
cd ../../..              # to sdks/go
go test ./pkg/beam/io/postgresio/ -v -count=1 -run \
'TestStagedCopyExecutionDirect|TestPostgreSqlRead_TableIntegration|TestPostgreSqlRead_QueryIntegration|TestPostgreSqlRead_PartitionedIntegration|TestPostgreSqlRead_ReadRowsIntegration|TestGoComplexPipeline_PostgresToPostgres|TestNativeReplicationStream_LivePostgres'
```

If something already listens on 5432, publish no host port and run the tests
inside the database container's network namespace, where `localhost:5432` is
the lab:

```bash
docker compose up -d postgres
docker run --rm --network container:beam-lab-postgres \
  -v "$(git rev-parse --show-toplevel)":/workspace:ro \
  -w /workspace/sdks/go -e GOTOOLCHAIN=auto -e CGO_ENABLED=0 \
  golang:alpine \
  go test ./pkg/beam/io/postgresio/ -v -count=1 -run \
'TestStagedCopyExecutionDirect|TestPostgreSqlRead_TableIntegration|TestPostgreSqlRead_QueryIntegration|TestPostgreSqlRead_PartitionedIntegration|TestPostgreSqlRead_ReadRowsIntegration|TestGoComplexPipeline_PostgresToPostgres|TestNativeReplicationStream_LivePostgres'
```

Do not add `--user` to that command. `expansionx` resolves the home directory
in a package initializer and does not check whether the lookup succeeded, so a
UID with no `/etc/passwd` entry panics the test binary before any test runs.

Seven tests should pass. They create and drop their own replication slot,
publication and CDC table, and truncate the fixture tables, so the lab is
reusable between runs without a restart.
