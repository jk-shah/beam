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

# Apache Beam PostgreSQL I/O Developer & User Guide

This guide provides step-by-step instructions for reading Change Data Capture (CDC) streams and writing high-throughput streams to PostgreSQL, Google Cloud SQL, and AlloyDB using Java, Python, Go, and Beam YAML.

---

## 1. PostgreSQL Database Configuration Prerequisites

Before running streaming replication pipelines, configure your PostgreSQL database instance for logical replication:

```sql
-- 1. In postgresql.conf (or Cloud SQL Flags):
-- wal_level = logical
-- max_replication_slots = 10
-- max_wal_senders = 10

-- 2. Create the publication for target tables:
CREATE PUBLICATION beam_pub FOR TABLE public.orders, public.customers;

-- Or publish all tables:
-- CREATE PUBLICATION beam_pub FOR ALL TABLES;

-- 3. Create the logical replication slot:
SELECT pg_create_logical_replication_slot('beam_slot', 'pgoutput');

-- 4. Ensure the Beam replication user has REPLICATION privileges:
GRANT REPLICATION TO beam_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO beam_user;
```

---

## 2. Java SDK Pipeline Examples

### Reading CDC Streams (`PostgreSQLIO.readCDC()`)

```java
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.postgres.PostgreSQLIO;
import org.apache.beam.sdk.io.postgres.cdc.ChangeEvent;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class PostgresCdcIngestionPipeline {
  public static void main(String[] args) {
    Pipeline p = Pipeline.create();

    PCollection<ChangeEvent<Row>> cdcStream =
        p.apply(
            "ReadPostgresCDC",
            PostgreSQLIO.readCDC()
                .withUrl("jdbc:postgresql://db.example.com:5432/ecommerce")
                .withUsername("beam_user")
                .withPassword("secret_password")
                .withSlotName("beam_slot")
                .withPublicationName("beam_pub")
                .withTable("public.orders")
                .withSlotRepairPolicy(SlotRepairPolicy.RECREATE_AND_RECONCILE));

    // Process ChangeEvents (INSERT, UPDATE, DELETE, TRUNCATE)
    cdcStream.apply("LogEvents", ParDo.of(new LogChangeEventDoFn()));

    p.run();
  }
}
```

### High-Throughput Upsert Sinks (`PostgreSQLIO.write()`)

```java
import java.util.Collections;
import org.apache.beam.sdk.io.postgres.PostgreSQLIO;
import org.apache.beam.sdk.io.postgres.sink.PostgreSqlWrite.WriteMode;
import org.apache.beam.sdk.io.postgres.sink.PostgreSqlWriteResult;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class PostgresWritePipeline {
  public static void writeRows(PCollection<Row> rows) {
    PostgreSqlWriteResult writeResult =
        rows.apply(
            "WriteToPostgres",
            PostgreSQLIO.write()
                .withDataSourceConfiguration(
                    PostgreSqlDataSourceConfiguration.create("jdbc:postgresql://db.example.com:5432/ecommerce")
                        .withUsername("beam_user")
                        .withPassword("secret_password")
                        .withMaxConnections(8)
                        .withReplicationOriginName("beam_sync"))
                .to("public.orders_summary")
                .withPrimaryKeyColumns(Collections.singletonList("order_id"))
                .withWriteMode(WriteMode.STREAMING_UPSERT_UNNEST)
                .withBatchSize(2000));

    // Route Dead-Letter Queue errors
    writeResult
        .getFailedRows()
        .apply("HandleWriteErrors", ParDo.of(new DeadLetterSinkDoFn()));
  }
}
```

---

## 3. Python SDK (`beam.managed.POSTGRES`)

### Reading CDC Stream
```python
import apache_beam as beam
from apache_beam.transforms.managed import POSTGRES, Read

with beam.Pipeline() as p:
    cdc_records = p | "ReadCDC" >> Read(
        POSTGRES,
        config={
            "url": "jdbc:postgresql://db.example.com:5432/ecommerce",
            "table": "public.orders",
            "username": "beam_user",
            "password": "secret_password",
            "slot_name": "beam_slot",
            "publication_name": "beam_pub",
        },
    )
    
    cdc_records | "Print" >> beam.Map(print)
```

### Writing with Parameterized Upserts
```python
from apache_beam.transforms.managed import POSTGRES, Write

with beam.Pipeline() as p:
    _ = (
        p
        | "CreateRecords" >> beam.Create(order_rows)
        | "UpsertToPostgres" >> Write(
            POSTGRES,
            config={
                "url": "jdbc:postgresql://db.example.com:5432/ecommerce",
                "table": "public.orders_summary",
                "primary_key_columns": ["order_id"],
                "write_mode": "STREAMING_UPSERT_UNNEST",
                "batch_size": 1000,
            },
        )
    )
```

---

## 4. Beam YAML Pipelines

```yaml
pipeline:
  transforms:
    - type: ReadFromPostgres
      name: IngestPostgresCDC
      config:
        url: "jdbc:postgresql://localhost:5432/ecommerce"
        table: "public.orders"
        slotName: "orders_slot"
        publicationName: "orders_pub"

    - type: Filter
      name: FilterActiveOrders
      config:
        language: python
        expression: "status == 'CONFIRMED'"

    - type: WriteToPostgres
      name: UpsertAnalyticsSummary
      config:
        url: "jdbc:postgresql://localhost:5432/analytics"
        table: "public.active_orders_summary"
        primaryKeyColumns: ["order_id"]
        writeMode: "STREAMING_UPSERT_UNNEST"
```

---

## 5. Go SDK (`sdks/go/pkg/beam/io/xlang/postgresio`)

```go
package main

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/xlang/xlangx"
)

func main() {
	p, s := beam.NewPipelineWithRoot()

	// Read CDC stream
	cdcRows := postgresio.Read(s, postgresio.ReadConfig{
		Url:             "jdbc:postgresql://localhost:5432/mydb",
		Table:           "public.users",
		SlotName:        "go_slot",
		PublicationName: "go_pub",
	})

	// Write upsert rows
	postgresio.Write(s, cdcRows, postgresio.WriteConfig{
		Url:               "jdbc:postgresql://localhost:5432/mydb",
		Table:             "public.users_replica",
		PrimaryKeyColumns: []string{"user_id"},
		WriteMode:         "STREAMING_UPSERT_UNNEST",
	})

	xlangx.Run(p)
}
```

---

## 6. Google Cloud SQL & AlloyDB IAM Authentication

To connect using Cloud IAM database authentication without managing static passwords:

```java
PostgreSqlDataSourceConfiguration config =
    PostgreSqlDataSourceConfiguration.createForCloudSql("my-project:us-central1:my-instance", "ecommerce")
        .withUsername("beam_iam_service_account@my-project.iam")
        .withEnableIamAuth(true)
        .withIpType("PRIVATE"); // or "PUBLIC", "PSC"
```

---

## 7. Production Tuning & Best Practices

| Parameter | Default | Recommended Setting | Rationale |
| :--- | :--- | :--- | :--- |
| `maxConnections` | `4` | `4`--`8` per worker | Prevents exhausting PostgreSQL server `max_connections`. |
| `maxLifetimeMs` | `1,800,000` ($30\text{m}$) | `1,800,000` | Recycles connections prior to Cloud IAM 60-minute token expiry. |
| `batchSize` | `1,000` | `2,000`--`5,000` | Maximizes vector efficiency in `UNNEST` array upserting. |
| `writeMode` | `STREAMING_UPSERT_UNNEST` | `STREAMING_UPSERT_UNNEST` (streaming), `STAGED_COPY_UPSERT` (batch) | Balances sub-second latency vs bulk raw throughput. |
