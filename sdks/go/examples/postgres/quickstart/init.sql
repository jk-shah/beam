-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- 1. Create source and target tables
CREATE TABLE IF NOT EXISTS public.orders_source (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.orders_target (
    id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    status TEXT NOT NULL,
    synced_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. Seed initial data
INSERT INTO public.orders_source (customer_id, amount, status)
SELECT
    (i * 7 % 100) + 1,
    ROUND((i * 13.37 % 500)::numeric, 2),
    CASE WHEN i % 3 = 0 THEN 'COMPLETED' WHEN i % 3 = 1 THEN 'PENDING' ELSE 'PROCESSING' END
FROM generate_series(1, 100) AS s(i);

-- 3. Provisioning script matching PostgresProvisioningScript output
CREATE ROLE "beam_cdc" WITH LOGIN REPLICATION PASSWORD 'secret';
GRANT CONNECT ON DATABASE "quickstart" TO "beam_cdc";
GRANT USAGE ON SCHEMA "public" TO "beam_cdc";
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE "public"."orders_source" TO "beam_cdc";
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE "public"."orders_target" TO "beam_cdc";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA "public" TO "beam_cdc";

CREATE PUBLICATION "beam_pub" FOR TABLE "public"."orders_source";

-- 4. Deploy beam_cdc_health monitoring view
DROP VIEW IF EXISTS "public"."beam_cdc_health";
CREATE VIEW "public"."beam_cdc_health" AS
SELECT
    s.slot_name,
    s.active,
    s.active_pid,
    s.wal_status,
    calc.retained_bytes,
    CASE
        WHEN calc.retained_bytes IS NULL THEN 'N/A'
        ELSE pg_catalog.pg_size_pretty(calc.retained_bytes)
    END AS retained_pretty,
    COALESCE(pg_catalog.age(s.catalog_xmin), 0)::bigint AS catalog_xmin_age,
    CASE
        WHEN s.wal_status = 'lost' THEN 'lost'
        WHEN s.restart_lsn IS NULL THEN 'unreserved'
        WHEN NOT s.active THEN 'inactive'
        ELSE 'healthy'
    END AS health
FROM pg_catalog.pg_replication_slots s
CROSS JOIN LATERAL (
    SELECT CASE
        WHEN s.restart_lsn IS NULL THEN NULL
        WHEN pg_catalog.pg_is_in_recovery() THEN
            pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_last_wal_replay_lsn(), s.restart_lsn)
        ELSE
            pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(), s.restart_lsn)
    END AS retained_bytes
) calc
WHERE s.slot_type = 'logical';

-- Pre-create replication slot for quickstart pipeline
SELECT pg_create_logical_replication_slot('quickstart_slot', 'pgoutput')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'quickstart_slot'
);
