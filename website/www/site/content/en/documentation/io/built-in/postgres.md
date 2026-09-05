---
title: "PostgreSQL I/O Connector"
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

[Built-in I/O Transforms](/documentation/io/built-in/)

# PostgreSQL I/O

The PostgreSQL I/O connector provides high-throughput reading, writing, and streaming Change Data Capture (CDC) replication for PostgreSQL databases across Apache Beam SDKs (Go, Java, Python, and Beam YAML).

## Supported Capabilities

| Capability | Go SDK | Java SDK | Python SDK (Cross-Language) | Beam YAML |
| :--- | :--- | :--- | :--- | :--- |
| **Batch Reading** | Supported | Supported | Supported | Supported |
| **Batch / Streaming Upsert** | Supported (`UNNEST` array upsert) | Supported (Staged `COPY` / `UNNEST`) | Supported | Supported |
| **Change Data Capture (CDC)** | Supported (Pure Go `pgoutput`) | Supported (`pgoutput` via JDBC) | Supported | Supported |
| **Vectorized Columnar Engine**| Supported (Apache Arrow) | Supported (SIMD VarHandle) | Supported | N/A |
| **Dead-Letter Queue (DLQ)** | Supported (`FailedRow`) | Supported (`TupleTag`) | Supported (`TaggedOutput`) | Supported |

---

## 1. Writing and Upserting Data

PostgreSQL I/O provides idempotent batch upserts using `INSERT INTO ... ON CONFLICT (primary_keys) DO UPDATE`.

### Go SDK Example
```go
import (
    "github.com/apache/beam/sdks/v2/go/pkg/beam"
    "github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
)

type Order struct {
    OrderID    int64   `beam:"order_id" db:"order_id"`
    CustomerID string  `beam:"customer_id" db:"customer_id"`
    Amount     float64 `beam:"amount" db:"amount"`
    Status     string  `beam:"status" db:"status"`
}

func WriteOrders(s beam.Scope, orders beam.PCollection) {
    opts := postgresio.NewWriteOptions(
        postgresio.WithHost("localhost"),
        postgresio.WithPort(5432),
        postgresio.WithDatabase("postgres"),
        postgresio.WithUsername("beam_test"),
        postgresio.WithPassword("beam_password"),
        postgresio.WithPrimaryKeyColumns("order_id"),
        postgresio.WithWriteMode(postgresio.WriteModeUpsert),
        postgresio.WithBatchSize(5000),
    )

    postgresio.Write(s, "public.orders", opts, orders)
}
```

### Beam YAML Example
```yaml
pipeline:
  transforms:
    - type: WriteToPostgres
      input: MyInputTransform
      config:
        url: "jdbc:postgresql://localhost:5432/postgres"
        table: "public.orders"
        username: "beam_test"
        password: "beam_password"
        primary_keys: ["order_id"]
        write_method: "UPSERT"
```

---

## 2. Change Data Capture (CDC) Streaming

The connector reads directly from PostgreSQL logical replication slots using the `pgoutput` streaming protocol without intermediate message brokers.

### Prerequisites in PostgreSQL (`postgresql.conf`)
```ini
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```

### Go SDK CDC Example
```go
changes := postgresio.ReadCDC(s,
    postgresio.WithCDCHost("localhost"),
    postgresio.WithCDCPort(5432),
    postgresio.WithCDCDatabase("postgres"),
    postgresio.WithCDCUsername("beam_test"),
    postgresio.WithCDCPassword("beam_password"),
    postgresio.WithCDCSlotName("beam_cdc_slot"),
    postgresio.WithCDCPublication("beam_orders_pub"),
    postgresio.WithCDCHeartbeatInterval(10 * time.Second),
)

// Partition downstream to scale processing workers
partitioned := postgresio.PartitionByPrimaryKey(s, changes)
```

---

## 3. Declarative Pure Replication, Filtering, and Transformation

Beam YAML pipelines support full end-to-end processing between PostgreSQL tables:

```yaml
pipeline:
  type: chain
  transforms:
    - type: ReadFromPostgres
      name: ReadSourceOrders
      config:
        url: "jdbc:postgresql://localhost:5432/postgres"
        table: "public.source_orders"
        username: "beam_test"
        password: "beam_password"

    - type: Filter
      name: FilterCompleted
      config:
        language: python
        keep: "status == 'COMPLETED' and float(amount) >= 100.0"

    - type: MapToFields
      name: EnrichAndMask
      config:
        language: python
        fields:
          order_id: "int(order_id)"
          customer_id: "str(customer_id)"
          masked_email: "customer_email[:3] + '***@' + customer_email.split('@')[1] if '@' in customer_email else '***'"
          net_amount: "round(float(amount) * 0.975, 2)"
          status: "str(status)"

    - type: WriteToPostgres
      name: WriteTransformedOrders
      config:
        url: "jdbc:postgresql://localhost:5432/postgres"
        table: "public.target_orders"
        username: "beam_test"
        password: "beam_password"
        primary_keys: ["order_id"]
        write_method: "UPSERT"
```

---

## 4. Architectural Considerations

* **Lock Contention Avoidance**: Go `postgresio.Write` uses parameterized array upserts (`UNNEST($1::type[])`), avoiding temporary table creation that locks system catalogs (`pg_class`).
* **Deadlock Prevention**: In-memory micro-batches sort records canonically by primary key prior to execution, ensuring uniform lock acquisition order across distributed runner workers.
* **Keepalive Heartbeat**: CDC consumers maintain a decoupled background keepalive goroutine sending periodic standby status updates, preventing `wal_sender_timeout` slot drops while downstream pipelines apply backpressure.
* **Replication Slot Resiliency**: Logical replication slots retain unconsumed WAL segments during worker failure. Upon restart, workers resume from the last confirmed checkpoint LSN without dropped or duplicated records.
