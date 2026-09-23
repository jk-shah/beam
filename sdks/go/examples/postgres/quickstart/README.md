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

# PostgreSQL CDC & Bulk Write Quickstart

This quickstart launches a complete, standalone Apache Beam pipeline synchronizing PostgreSQL changes in real-time.

Within minutes, you will have:
1. A PostgreSQL primary instance pre-configured with `wal_level=logical`.
2. A seeded `orders_source` table and an unseeded `orders_target` table.
3. A pre-provisioned replication role (`beam_cdc`) and publication (`beam_pub`).
4. The `beam_cdc_health` operational view deployed and readable.
5. An active Apache Beam Go pipeline streaming CDC records into `orders_target`.

---

## Running the Quickstart

### 1. Launch Services

```bash
docker compose up --build
```

Docker Compose will start the PostgreSQL instance, execute `init.sql`, wait until the database passes health checks, and then start the Beam streaming pipeline.

### 2. Observe Replicated Rows

In a separate terminal, monitor the target table:

```bash
docker compose exec postgres psql -U postgres -d quickstart -c "SELECT count(*) FROM orders_target;"
```

You will see rows appearing and synchronizing in `orders_target`.

### 3. Inspect Slot Health and Retention

Check replication slot status using the `beam_cdc_health` monitoring view:

```bash
docker compose exec postgres psql -U postgres -d quickstart -c "SELECT * FROM beam_cdc_health;"
```

Sample output:
```
 slot_name       | active | active_pid | wal_status | retained_bytes | retained_pretty | catalog_xmin_age | health
-----------------+--------+------------+------------+----------------+-----------------+------------------+---------
 quickstart_slot | t      |         42 | reserved   |          32768 | 32 kB           |                0 | healthy
```

### 4. Insert Live Mutations

Insert new records into `orders_source` and watch them replicate immediately:

```bash
docker compose exec postgres psql -U postgres -d quickstart -c "
INSERT INTO orders_source (customer_id, amount, status)
VALUES (999, 456.78, 'COMPLETED');
"
```

Verify in `orders_target`:

```bash
docker compose exec postgres psql -U postgres -d quickstart -c "
SELECT * FROM orders_target WHERE customer_id = 999;
"
```

### 5. Tear Down

Stop all containers and remove temporary volumes:

```bash
docker compose down -v
```
