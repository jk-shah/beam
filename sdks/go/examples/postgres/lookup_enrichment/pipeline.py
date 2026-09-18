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

"""Dynamic Lookup & Dimension Enrichment in Python for PostgreSQL.

Ingests order events via PostgreSQL CDC, performs in-flight dimension lookup
with an in-memory TTL cache to reduce database round-trips, and writes
enriched orders back to PostgreSQL with idempotent ON CONFLICT upsert.

Execution:
    python3 pipeline.py --runner=DirectRunner
"""

import argparse
import datetime
import logging
import time

import apache_beam as beam
from apache_beam.io.postgres import ReadFromPostgresCDC, WriteToPostgresIO
from apache_beam.options.pipeline_options import PipelineOptions


class CachedCustomerLookupFn(beam.DoFn):
    """Enriches orders against customer dimension data with an in-worker LRU/TTL cache."""

    def __init__(self, ttl_seconds=300):
        self.ttl_seconds = ttl_seconds
        self.cache = {}

    def setup(self):
        # In a production environment, initialize a connection pool or read static side input
        logging.info("Initialized in-memory customer profile dimension cache")

    def process(self, element):
        customer_id = element.get("customer_id", "")
        now = time.time()

        # Check local worker cache
        profile = self.cache.get(customer_id)
        if not profile or (now - profile["cached_at"]) > self.ttl_seconds:
            # Emulated DB lookup or side-input fetch
            profile = {
                "customer_name": f"Customer-{customer_id}",
                "customer_email": f"{customer_id.lower()}@example.com",
                "loyalty_tier": "VIP" if float(element.get("amount", 0.0)) >= 1000.0 else "STANDARD",
                "cached_at": now,
            }
            self.cache[customer_id] = profile

        yield {
            "order_id": str(element.get("order_id")),
            "customer_id": customer_id,
            "customer_name": profile["customer_name"],
            "customer_email": profile["customer_email"],
            "loyalty_tier": profile["loyalty_tier"],
            "item": str(element.get("item", "Generic Item")),
            "amount": float(element.get("amount", 0.0)),
            "enriched_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--database", default="beammeup", help="PostgreSQL database")
    parser.add_argument("--username", default="beam_navigator", help="PostgreSQL username")
    parser.add_argument("--slot_name", default="orders_enrichment_slot", help="CDC replication slot")
    parser.add_argument("--publication", default="orders_pub", help="PostgreSQL publication")
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args, streaming=True)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadCDC"
            >> ReadFromPostgresCDC(
                host=known_args.host,
                port=known_args.port,
                database=known_args.database,
                username=known_args.username,
                password_env_var="PGPASSWORD",
                slot_name=known_args.slot_name,
                publication=known_args.publication,
            )
            | "FilterValidOrders"
            >> beam.Filter(lambda r: r.get("operation") in ("INSERT", "UPDATE") and float(r.get("amount", 0)) > 0)
            | "EnrichFromDimensionCache" >> beam.ParDo(CachedCustomerLookupFn())
            | "WriteEnrichedOrders"
            >> WriteToPostgresIO(
                host=known_args.host,
                port=known_args.port,
                database=known_args.database,
                username=known_args.username,
                password_env_var="PGPASSWORD",
                table="public.enriched_orders",
                conflict_keys=["order_id"],
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
