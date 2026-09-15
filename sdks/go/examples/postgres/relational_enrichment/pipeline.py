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

"""Customer 360 Relational Stream Join in Python for PostgreSQL.

Ingests order stream from PostgreSQL CDC, joins with customer profile dimension
data using beam.CoGroupByKey, and writes enriched orders with idempotent upsert.

Execution:
    python3 pipeline.py --runner=DirectRunner
"""

import argparse
import datetime
import logging

import apache_beam as beam
from apache_beam.io.postgres_cdc import ReadFromPostgresCDC, WriteToPostgres
from apache_beam.options.pipeline_options import PipelineOptions


class JoinCustomerAndOrderFn(beam.DoFn):
    """Performs outer relational join between orders and customer profile dimensions."""

    def process(self, element):
        customer_id, grouped = element
        orders = grouped.get("orders", [])
        customers = grouped.get("customers", [])

        customer = customers[0] if customers else {
            "customer_name": "Unknown Customer",
            "customer_email": "unknown@example.com",
            "loyalty_tier": "STANDARD",
        }

        for order in orders:
            yield {
                "order_id": str(order.get("order_id")),
                "customer_id": str(customer_id),
                "customer_name": customer["customer_name"],
                "customer_email": customer["customer_email"],
                "loyalty_tier": customer["loyalty_tier"],
                "item": str(order.get("item", "Unknown")),
                "amount": float(order.get("amount", 0.0)),
                "enriched_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--database", default="beammeup", help="PostgreSQL database")
    parser.add_argument("--username", default="beam_navigator", help="PostgreSQL username")
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        # 1. Orders stream
        orders = (
            p
            | "ReadOrdersCDC"
            >> ReadFromPostgresCDC(
                host=known_args.host,
                port=known_args.port,
                database=known_args.database,
                username=known_args.username,
                password_env_var="PGPASSWORD",
                slot_name="orders_relational_slot",
                publication="orders_pub",
            )
            | "FilterValid" >> beam.Filter(lambda o: o.get("operation") in ("INSERT", "UPDATE"))
            | "KeyOrdersByCust" >> beam.Map(lambda o: (o["customer_id"], o))
        )

        # 2. Customers dimension side collection (e.g. initial read or dimension stream)
        customers = (
            p
            | "CreateMockCustomers"
            >> beam.Create([
                ("CUST-101", {"customer_name": "Alice Smith", "customer_email": "alice@example.com", "loyalty_tier": "VIP"}),
                ("CUST-102", {"customer_name": "Bob Jones", "customer_email": "bob@example.com", "loyalty_tier": "STANDARD"}),
            ])
        )

        # 3. Relational join & write
        (
            {"orders": orders, "customers": customers}
            | "CoGroupCustomerOrders" >> beam.CoGroupByKey()
            | "JoinDimensions" >> beam.ParDo(JoinCustomerAndOrderFn())
            | "WriteEnrichedOrders"
            >> WriteToPostgres(
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
