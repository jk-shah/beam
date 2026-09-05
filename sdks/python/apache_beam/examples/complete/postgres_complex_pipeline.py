#
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
#

"""
Complex PostgreSQL-to-PostgreSQL Pipeline in Python using Apache Beam.

Demonstrates:
1. Source Reading from PostgreSQL with pg8000 connector
2. Complex DoFn with Dead-Letter-Queue (DLQ) branching via TaggedOutput
3. PII Data Masking (Email Obfuscation)
4. Financial Calculations (Net amount, tiered processing fee)
5. Customer Loyalty Tier Classification (VIP vs Standard)
6. Real-time Metric Tracking via Beam Metrics counters
7. Idempotent Upsert Sink with Parameterized Batching into PostgreSQL
"""

import sys
import datetime
from decimal import Decimal
import pg8000.native

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.metrics import Metrics
from apache_beam.pvalue import TaggedOutput


class ReadFromPostgresSource(beam.DoFn):
    """Reads batches of records from PostgreSQL table."""

    def __init__(self, host, database, user, table_name):
        self.host = host
        self.database = database
        self.user = user
        self.table_name = table_name

    def process(self, element):
        conn = pg8000.native.Connection(
            self.user,
            host=self.host,
            database=self.database
        )
        try:
            query = f"""
                SELECT order_id, customer_id, customer_email, amount, status, country_code, items_count, created_at
                FROM {self.table_name}
                ORDER BY order_id;
            """
            for row in conn.run(query):
                yield {
                    "order_id": row[0],
                    "customer_id": row[1],
                    "customer_email": row[2],
                    "amount": Decimal(str(row[3])),
                    "status": row[4],
                    "country_code": row[5],
                    "items_count": row[6],
                    "created_at": row[7]
                }
        finally:
            conn.close()


class EnrichAndValidateOrderDoFn(beam.DoFn):
    """Processes, validates, enriches orders and routes invalid elements to DLQ."""

    def __init__(self):
        self.processed_counter = Metrics.counter(self.__class__, "orders_processed")
        self.vip_counter = Metrics.counter(self.__class__, "vip_orders")
        self.standard_counter = Metrics.counter(self.__class__, "standard_orders")
        self.dlq_counter = Metrics.counter(self.__class__, "dlq_orders")

    def process(self, element):
        self.processed_counter.inc()

        # Validation rule: Amount must be strictly positive and email must contain '@'
        amount = element.get("amount", Decimal("0"))
        email = element.get("customer_email", "")
        if amount <= 0 or "@" not in email:
            self.dlq_counter.inc()
            yield TaggedOutput("dlq", {
                "element": element,
                "reason": "Invalid amount or malformed email",
                "rejected_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
            })
            return

        # PII Obfuscation: alice.smith@example.com -> ali***@example.com
        user_part, domain_part = email.split("@", 1)
        masked_user = user_part[:3] + "***" if len(user_part) >= 3 else user_part + "***"
        masked_email = f"{masked_user}@{domain_part}"

        # Financial Calculations (2.5% fee)
        fee = (amount * Decimal("0.025")).quantize(Decimal("0.01"))
        net_amount = (amount - fee).quantize(Decimal("0.01"))

        # Customer Tier Classification
        if amount >= Decimal("1000.00"):
            customer_tier = "VIP"
            self.vip_counter.inc()
        else:
            customer_tier = "STANDARD"
            self.standard_counter.inc()

        transformed = {
            "order_id": int(element["order_id"]),
            "customer_id": str(element["customer_id"]),
            "masked_email": masked_email,
            "net_amount": str(net_amount),
            "processing_fee": str(fee),
            "customer_tier": customer_tier,
            "status": str(element["status"]),
            "country_code": str(element["country_code"]),
            "items_count": int(element["items_count"]),
            "processed_at": datetime.datetime.now(datetime.timezone.utc)
        }
        yield transformed


class WriteToPostgresSink(beam.DoFn):
    """Idempotent upsert writer with connection pooling and parameterized batching."""

    def __init__(self, host, database, user, target_table):
        self.host = host
        self.database = database
        self.user = user
        self.target_table = target_table
        self._conn = None

    def setup(self):
        self._conn = pg8000.native.Connection(
            self.user,
            host=self.host,
            database=self.database
        )

    def process(self, batch):
        if not batch:
            return

        query = f"""
            INSERT INTO {self.target_table} (
                order_id, customer_id, masked_email, net_amount, processing_fee,
                customer_tier, status, country_code, items_count, processed_at
            ) VALUES (
                :order_id, :customer_id, :masked_email, :net_amount, :processing_fee,
                :customer_tier, :status, :country_code, :items_count, :processed_at
            )
            ON CONFLICT (order_id) DO UPDATE SET
                masked_email = EXCLUDED.masked_email,
                net_amount = EXCLUDED.net_amount,
                processing_fee = EXCLUDED.processing_fee,
                customer_tier = EXCLUDED.customer_tier,
                status = EXCLUDED.status,
                items_count = EXCLUDED.items_count,
                processed_at = EXCLUDED.processed_at;
        """
        for item in batch:
            self._conn.run(
                query,
                order_id=item["order_id"],
                customer_id=item["customer_id"],
                masked_email=item["masked_email"],
                net_amount=item["net_amount"],
                processing_fee=item["processing_fee"],
                customer_tier=item["customer_tier"],
                status=item["status"],
                country_code=item["country_code"],
                items_count=item["items_count"],
                processed_at=item["processed_at"]
            )

    def teardown(self):
        if self._conn:
            self._conn.close()


def run_pipeline(host="localhost", database="postgres", user="beam_test"):
    options = PipelineOptions(["--runner=DirectRunner"])
    with beam.Pipeline(options=options) as p:
        # Trigger single batch read
        raw_orders = (
            p
            | "Trigger" >> beam.Create([None])
            | "ReadFromPostgres" >> beam.ParDo(ReadFromPostgresSource(host, database, user, "test_pipelines.source_orders"))
        )

        # Apply multi-output transformation & enrichment
        results = raw_orders | "EnrichAndValidate" >> beam.ParDo(
            EnrichAndValidateOrderDoFn()
        ).with_outputs("dlq", main="valid")

        # Process valid orders into batches and upsert to Postgres
        _ = (
            results.valid
            | "BatchElements" >> beam.BatchElements(min_batch_size=5, max_batch_size=100)
            | "WriteToPostgres" >> beam.ParDo(WriteToPostgresSink(host, database, user, "test_pipelines.target_orders_transformed"))
        )

        # Log DLQ elements
        _ = (
            results.dlq
            | "FormatDLQ" >> beam.Map(lambda x: f"[DLQ REJECTED]: {x}")
            | "LogDLQ" >> beam.Map(print)
        )

    # Verification Query
    conn = pg8000.native.Connection(user, host=host, database=database)
    try:
        count = conn.run("SELECT COUNT(*) FROM test_pipelines.target_orders_transformed;")[0][0]
        vip_count = conn.run("SELECT COUNT(*) FROM test_pipelines.target_orders_transformed WHERE customer_tier = 'VIP';")[0][0]
        sample = conn.run("SELECT order_id, masked_email, net_amount, processing_fee, customer_tier FROM test_pipelines.target_orders_transformed ORDER BY order_id LIMIT 3;")
        print(f"\n=== Python Beam Pipeline Execution Verification ===")
        print(f"Total Rows Upserted: {count}")
        print(f"VIP Rows Classified: {vip_count}")
        print(f"Sample Rows:")
        for row in sample:
            print(f"  Order {row[0]}: {row[1]} | Net: {row[2]} | Fee: {row[3]} | Tier: {row[4]}")
        assert count == 20, f"Expected 20 rows, got {count}"
        assert vip_count == 7, f"Expected 7 VIP rows (amount >= 1000), got {vip_count}"
        print("PYTHON PIPELINE SUCCESS: All assertions passed!")
    finally:
        conn.close()


if __name__ == "__main__":
    run_pipeline()
