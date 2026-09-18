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

-- 1. Provision standard roles with LEAST PRIVILEGE (Zero SUPERUSER)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'scotty') THEN
        CREATE ROLE scotty WITH LOGIN REPLICATION PASSWORD 'scotty_secret';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'beam_navigator') THEN
        CREATE ROLE beam_navigator WITH LOGIN REPLICATION PASSWORD 'beam_navigator';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'beam_transporter') THEN
        CREATE ROLE beam_transporter WITH LOGIN PASSWORD 'beam_transporter_pass';
    END IF;
END $$;

-- 2. Create regional databases and secondary operational databases
SELECT 'CREATE DATABASE beammeup OWNER scotty' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'beammeup')\gexec
ALTER DATABASE beammeup OWNER TO scotty;
SELECT 'CREATE DATABASE us_accounts_db OWNER beam_navigator' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'us_accounts_db')\gexec
SELECT 'CREATE DATABASE eu_accounts_db OWNER beam_navigator' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'eu_accounts_db')\gexec
SELECT 'CREATE DATABASE apac_accounts_db OWNER beam_navigator' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'apac_accounts_db')\gexec
SELECT 'CREATE DATABASE quickstart OWNER scotty' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'quickstart')\gexec
SELECT 'CREATE DATABASE postgres OWNER beam_navigator' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'postgres')\gexec

-- Connect to primary beammeup database
\c beammeup

-- 3. Provision Table Schemas for all 22 Tutorials:

-- Tutorial 1 & 2: Backfill Cutover & Sequence Reconciliation
CREATE TABLE public.orders (
    order_id BIGSERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL,
    status VARCHAR(32) NOT NULL,
    order_date TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE public.orders_sequence_demo (LIKE public.orders INCLUDING ALL);

-- Tutorial 3: Partitioned Table Ingestion (Composite Primary Key)
CREATE TABLE public.orders_partitioned (
    order_id BIGINT NOT NULL,
    order_date DATE NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (order_id, order_date)
) PARTITION BY RANGE (order_date);

CREATE TABLE public.orders_2026_09_01 PARTITION OF public.orders_partitioned
    FOR VALUES FROM ('2026-09-01') TO ('2026-09-02');
CREATE TABLE public.orders_2026_09_02 PARTITION OF public.orders_partitioned
    FOR VALUES FROM ('2026-09-02') TO ('2026-09-03');
CREATE TABLE public.orders_2026_09_03 PARTITION OF public.orders_partitioned
    FOR VALUES FROM ('2026-09-03') TO ('2026-09-04');

-- Tutorial 4: PgBouncer Pooled Events Table
CREATE TABLE public.pooled_events (
    event_id BIGINT PRIMARY KEY,
    source VARCHAR(64) NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tutorial 5: Schema Evolution Target Table
CREATE TABLE public.evolved_orders (
    order_id BIGINT PRIMARY KEY,
    customer_id INT NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    loyalty_tier VARCHAR(32) NOT NULL,
    extra_fields JSONB,
    schema_ver INT NOT NULL,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tutorial 6: High Availability Replicated Table
CREATE TABLE public.ha_replicated_orders (
    record_id BIGINT PRIMARY KEY,
    payload TEXT NOT NULL,
    confirmed_lsn BIGINT NOT NULL,
    replicated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tutorial 7: Target Orders (Backfill Cutover)
DROP TABLE IF EXISTS public.target_orders;
CREATE TABLE public.target_orders (
    order_id BIGINT PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    status VARCHAR(32) NOT NULL,
    source_type VARCHAR(16) NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tutorial 8: Customer Accounts for Geo Fan-Out
CREATE TABLE public.customer_accounts (
    account_id VARCHAR(64) PRIMARY KEY,
    customer_name VARCHAR(128) NOT NULL,
    ssn VARCHAR(16) NOT NULL,
    email VARCHAR(128) NOT NULL,
    geo_region VARCHAR(16) NOT NULL,
    balance NUMERIC(14,2) NOT NULL,
    lsn BIGINT DEFAULT 0,
    _op_type VARCHAR(4) DEFAULT 'c',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tutorial 9: Dead-Letter Queue (DLQ) Tables
CREATE TABLE public.clean_payments (
    payment_id VARCHAR(64) PRIMARY KEY,
    account_id VARCHAR(64) NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    currency VARCHAR(8) NOT NULL,
    status VARCHAR(32) NOT NULL,
    cleaned_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE public.dead_letter_payments (
    payment_id VARCHAR(64) PRIMARY KEY,
    account_id VARCHAR(64) NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    error_code VARCHAR(64) NOT NULL,
    error_message TEXT NOT NULL,
    raw_payload TEXT NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tutorial 10: Deduplication Table
CREATE TABLE public.canonical_events (
    event_id VARCHAR(64) PRIMARY KEY,
    source VARCHAR(64) NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

-- Tutorial 11: Data Reconciliation Audit
CREATE TABLE public.data_reconciliation_audit (
    record_id VARCHAR(64) PRIMARY KEY,
    reconciliation_status VARCHAR(32) NOT NULL,
    source_checksum VARCHAR(64) NOT NULL,
    target_checksum VARCHAR(64) NOT NULL,
    difference_details TEXT,
    audited_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tutorial 12: SCD Type 2 History Table
CREATE TABLE public.customer_dim_history (
    customer_id VARCHAR(64) NOT NULL,
    version INT NOT NULL,
    full_name VARCHAR(128) NOT NULL,
    tier VARCHAR(32) NOT NULL,
    address TEXT NOT NULL,
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (customer_id, version)
);

-- Tutorial 13 & 14: Relational Enrichment Table
CREATE TABLE IF NOT EXISTS public.customer_profiles (
    customer_id VARCHAR(64) PRIMARY KEY,
    customer_name VARCHAR(128) NOT NULL,
    customer_email VARCHAR(128) NOT NULL,
    loyalty_tier VARCHAR(32) NOT NULL,
    credit_limit NUMERIC(12,2) NOT NULL,
    risk_score NUMERIC(5,2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE public.enriched_orders (
    order_id BIGINT PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    customer_name VARCHAR(128) NOT NULL,
    customer_email VARCHAR(128) NOT NULL,
    loyalty_tier VARCHAR(32) NOT NULL,
    credit_limit NUMERIC(12,2) DEFAULT 0.00,
    risk_score NUMERIC(5,2) DEFAULT 0.00,
    item VARCHAR(64) NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    is_approved BOOLEAN DEFAULT TRUE,
    approval_reason TEXT DEFAULT '',
    enriched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tutorial 15: Streaming Materialized View Rollup Table
CREATE TABLE public.merchant_minute_rollups (
    merchant_id VARCHAR(64) NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    transaction_count BIGINT NOT NULL,
    total_amount NUMERIC(14,2) NOT NULL,
    max_amount NUMERIC(14,2) NOT NULL,
    last_applied_lsn BIGINT NOT NULL DEFAULT 0,
    last_tx_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (merchant_id, window_start)
);

-- Tutorial 16: Multi-Dimensional OLAP Cube Table
CREATE TABLE public.olap_sales_cube (
    region VARCHAR(64) NOT NULL,
    category VARCHAR(64) NOT NULL,
    total_revenue NUMERIC(16,2) NOT NULL,
    order_count BIGINT NOT NULL,
    avg_order_value NUMERIC(14,2) NOT NULL,
    max_order_value NUMERIC(14,2) NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (region, category)
);

-- Tutorial 17: Window Top-N Leaderboard Table
CREATE TABLE public.top_products_by_category (
    category VARCHAR(64) NOT NULL,
    rank_position INT NOT NULL,
    product_id VARCHAR(64) NOT NULL,
    product_name VARCHAR(128) NOT NULL,
    sales_volume NUMERIC(14,2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (category, rank_position)
);

-- Tutorial 18: Inactivity Gap Sessionization Table
CREATE TABLE public.user_session_summaries (
    user_id VARCHAR(64) NOT NULL,
    session_start TIMESTAMPTZ NOT NULL,
    session_end TIMESTAMPTZ NOT NULL,
    duration_seconds BIGINT NOT NULL,
    event_count INT NOT NULL,
    is_bounce BOOLEAN NOT NULL,
    PRIMARY KEY (user_id, session_start)
);

-- Tutorial 19: Vectorized Batch ETL Table
CREATE TABLE public.migrated_transactions (
    id BIGINT PRIMARY KEY,
    code VARCHAR(32) NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    region VARCHAR(16) NOT NULL,
    tier VARCHAR(16) NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tutorial 20: Graph Node Centrality Table
CREATE TABLE public.graph_node_centrality (
    node_id VARCHAR(64) PRIMARY KEY,
    in_degree INT NOT NULL,
    out_degree INT NOT NULL,
    total_degree INT NOT NULL,
    avg_edge_weight NUMERIC(10,4) NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tutorial 21: Feature Store Table
CREATE TABLE public.ml_feature_store (
    entity_id VARCHAR(64) PRIMARY KEY,
    raw_income NUMERIC(14,2) NOT NULL,
    norm_income NUMERIC(14,4) NOT NULL,
    z_income NUMERIC(14,4) NOT NULL,
    raw_score NUMERIC(14,2) NOT NULL,
    norm_score NUMERIC(14,4) NOT NULL,
    engineered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tutorial 22: Local Inference Predictions Table
CREATE TABLE public.payment_fraud_predictions (
    payment_id VARCHAR(64) PRIMARY KEY,
    account_id VARCHAR(64) NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    fraud_probability NUMERIC(6,4) NOT NULL,
    risk_tier VARCHAR(16) NOT NULL,
    is_flagged BOOLEAN NOT NULL,
    model_version VARCHAR(32) NOT NULL,
    inference_latency_us BIGINT NOT NULL,
    predicted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4. Create Publications matching tutorial defaults
CREATE PUBLICATION orders_pub FOR TABLE public.orders;
CREATE PUBLICATION ha_critical_pub FOR TABLE public.orders;
CREATE PUBLICATION fraud_inference_pub FOR ALL TABLES;
CREATE PUBLICATION backfill_orders_pub FOR TABLE public.orders;
CREATE PUBLICATION beam_publication FOR TABLE public.orders, public.customer_accounts;
CREATE PUBLICATION pub_customer_accounts FOR TABLE public.customer_accounts;
CREATE PUBLICATION pub_beam_cdc FOR TABLE public.orders, public.customer_accounts;

-- 5. Seed baseline rows
INSERT INTO public.orders (customer_id, total_amount, status) VALUES
(1, 150.00, 'COMPLETED'),
(2, 45.50, 'PENDING'),
(3, 890.00, 'COMPLETED');

INSERT INTO public.customer_accounts (account_id, customer_name, ssn, email, geo_region, balance) VALUES
('ACC-001', 'Alice Smith', '123-45-6789', 'alice@example.com', 'US', 4500.00),
('ACC-002', 'Bob Jones', '987-65-4321', 'bob@example.org', 'EU', 1200.50),
('ACC-003', 'Charlie Lee', '456-78-1234', 'charlie@example.jp', 'APAC', 8900.00);

INSERT INTO public.customer_profiles (customer_id, customer_name, customer_email, loyalty_tier, credit_limit, risk_score) VALUES
('CUST-1', 'Alice Johnson', 'alice@example.com', 'PLATINUM', 5000.00, 12.5),
('CUST-2', 'Bob Smith', 'bob@example.com', 'GOLD', 2500.00, 45.0),
('CUST-3', 'Charlie Brown', 'charlie@example.com', 'SILVER', 1000.00, 78.0),
('CUST-4', 'Diana Prince', 'diana@example.com', 'PLATINUM', 10000.00, 5.0),
('CUST-5', 'Evan Wright', 'evan@example.com', 'BRONZE', 500.00, 88.5)
ON CONFLICT (customer_id) DO NOTHING;

-- 6. Deploy standardized DBA replication health monitoring view
CREATE OR REPLACE VIEW public.beam_cdc_health AS
SELECT
    slot_name,
    slot_type,
    active,
    active_pid,
    COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0) AS retained_bytes,
    pg_size_pretty(COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)) AS retained_size,
    confirmed_flush_lsn,
    CASE
        WHEN NOT active THEN 'UNCONNECTED'
        WHEN pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) > 1073741824 THEN 'WARNING_LAG_HIGH'
        ELSE 'HEALTHY'
    END AS health_status
FROM pg_replication_slots
WHERE plugin = 'pgoutput';

-- 7. Grant Least-Privilege Permissions across Roles (Zero SUPERUSER Enforcement)

-- 7.1 Permissions for "scotty" (CDC Streaming & Replication)
GRANT CONNECT ON DATABASE beammeup TO scotty;
GRANT USAGE ON SCHEMA public TO scotty;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO scotty;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO scotty;
GRANT SELECT ON public.beam_cdc_health TO scotty;
GRANT pg_read_all_stats TO scotty;
GRANT INSERT, UPDATE, DELETE ON public.ha_replicated_orders TO scotty;
GRANT INSERT, UPDATE, DELETE ON public.clean_payments, public.dead_letter_payments TO scotty;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.customer_profiles, public.enriched_orders TO scotty;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO scotty;

-- 7.2 Permissions for "beam_navigator" (Pipeline Sink, Batch Ingestion & Dynamic Tables)
GRANT CONNECT ON DATABASE beammeup TO beam_navigator;
GRANT CONNECT ON DATABASE postgres TO beam_navigator;
GRANT USAGE, CREATE ON SCHEMA public TO beam_navigator;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO beam_navigator;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO beam_navigator;
GRANT SELECT ON public.beam_cdc_health TO beam_navigator;
GRANT pg_read_all_stats TO beam_navigator;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.clean_payments TO beam_navigator;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.dead_letter_payments TO beam_navigator;
ALTER TABLE public.orders_partitioned OWNER TO beam_navigator;
ALTER TABLE public.orders_2026_09_01 OWNER TO beam_navigator;
ALTER TABLE public.orders_2026_09_02 OWNER TO beam_navigator;
ALTER TABLE public.orders_2026_09_03 OWNER TO beam_navigator;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO beam_navigator;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO beam_navigator;

-- 7.3 Permissions for "beam_transporter" (PgBouncer Pooled Sink)
GRANT CONNECT ON DATABASE beammeup TO beam_transporter;
GRANT USAGE ON SCHEMA public TO beam_transporter;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.pooled_events TO beam_transporter;

-- 7.4 Cross-Database Grants for Regional Databases (Tutorial 8 Geo Fan-Out)
\c us_accounts_db
CREATE TABLE public.customer_accounts (
    account_id VARCHAR(64) PRIMARY KEY,
    customer_name VARCHAR(128) NOT NULL,
    ssn VARCHAR(16) NOT NULL,
    email VARCHAR(128) NOT NULL,
    geo_region VARCHAR(16) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
GRANT CONNECT ON DATABASE us_accounts_db TO beam_navigator;
GRANT USAGE, CREATE ON SCHEMA public TO beam_navigator;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO beam_navigator;

\c eu_accounts_db
CREATE TABLE public.customer_accounts (
    account_id VARCHAR(64) PRIMARY KEY,
    customer_name VARCHAR(128) NOT NULL,
    ssn VARCHAR(16) NOT NULL,
    email VARCHAR(128) NOT NULL,
    geo_region VARCHAR(16) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
GRANT CONNECT ON DATABASE eu_accounts_db TO beam_navigator;
GRANT USAGE, CREATE ON SCHEMA public TO beam_navigator;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO beam_navigator;

\c apac_accounts_db
CREATE TABLE public.customer_accounts (
    account_id VARCHAR(64) PRIMARY KEY,
    customer_name VARCHAR(128) NOT NULL,
    ssn VARCHAR(16) NOT NULL,
    email VARCHAR(128) NOT NULL,
    geo_region VARCHAR(16) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
GRANT CONNECT ON DATABASE apac_accounts_db TO beam_navigator;
GRANT USAGE, CREATE ON SCHEMA public TO beam_navigator;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO beam_navigator;

\c beammeup

-- 7.5 Seed tables for batch examples (DEFECT-018)
CREATE TABLE public.legacy_transactions (
    id BIGINT PRIMARY KEY,
    raw_code VARCHAR(128) NOT NULL,
    raw_amount VARCHAR(128) NOT NULL,
    region VARCHAR(64) NOT NULL
);
INSERT INTO public.legacy_transactions (id, raw_code, raw_amount, region)
SELECT i, 'TXN-CODE-' || lpad(i::text, 6, '0'), ((i % 1000) + 10)::text || '.50',
       (ARRAY['us-central1', 'us-east1', 'europe-west1', 'asia-east1'])[ (i % 4) + 1 ]
FROM generate_series(1, 1000) AS s(i);

CREATE TABLE public.product_sales_catalog (
    product_id VARCHAR(64) PRIMARY KEY,
    product_name VARCHAR(128) NOT NULL,
    category VARCHAR(64) NOT NULL,
    sales_volume NUMERIC(14,2) NOT NULL
);
INSERT INTO public.product_sales_catalog VALUES
('PROD-101', 'Smartphone Model X', 'ELECTRONICS', 250000.00),
('PROD-102', 'Wireless Earbuds Pro', 'ELECTRONICS', 85000.00),
('PROD-103', '4K Monitor 27-inch', 'ELECTRONICS', 120000.00),
('PROD-201', 'Ergonomic Desk Chair', 'FURNITURE', 75000.00),
('PROD-202', 'Executive Leather Chair', 'FURNITURE', 145000.00),
('PROD-203', 'Filing Cabinet Metal', 'FURNITURE', 15000.00),
('PROD-204', 'Monitor Arm Dual Mount', 'FURNITURE', 35000.00);

CREATE TABLE public.olap_transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    region VARCHAR(64) NOT NULL,
    category VARCHAR(64) NOT NULL,
    amount NUMERIC(14,2) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO public.olap_transactions (transaction_id, region, category, amount) VALUES
('T-01', 'US-EAST', 'ELECTRONICS', 1200.00),
('T-02', 'US-EAST', 'ELECTRONICS', 850.00),
('T-03', 'US-EAST', 'BOOKS', 45.00),
('T-04', 'US-WEST', 'ELECTRONICS', 2100.00),
('T-05', 'US-WEST', 'HOME', 150.00),
('T-06', 'EU-WEST', 'ELECTRONICS', 3100.00),
('T-07', 'EU-WEST', 'ELECTRONICS', 900.00),
('T-08', 'EU-WEST', 'GROCERY', 45.00),
('T-09', 'EU-WEST', 'GROCERY', 65.00);
