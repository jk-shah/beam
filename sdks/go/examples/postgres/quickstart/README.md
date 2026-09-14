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
