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

"""Multi-Dimensional OLAP Sales Cube in Python for PostgreSQL.

Aggregates sales metrics across multi-attribute composite keys (Region x Category)
and writes rollups into PostgreSQL using atomic ON CONFLICT upsert.

Execution:
    python3 pipeline.py --runner=DirectRunner
"""

import argparse
import datetime
from decimal import Decimal
import logging

import apache_beam as beam
from apache_beam.io.postgres_cdc import WriteToPostgres
from apache_beam.options.pipeline_options import PipelineOptions


class SalesCubeAccumulator(beam.CombineFn):
    """Computes total revenue, order count, and max value per (region, category)."""

    def create_accumulator(self):
        return (Decimal("0.0"), 0, Decimal("0.0"))

    def add_input(self, accumulator, element):
        total, count, max_val = accumulator
        amt = Decimal(str(element.get("amount", "0.0")))
        return (total + amt, count + 1, max(max_val, amt))

    def merge_accumulators(self, accumulators):
        totals, counts, maxs = zip(*accumulators)
        return (sum(totals), sum(counts), max(maxs))

    def extract_output(self, accumulator):
        total, count, max_val = accumulator
        avg_val = float(total / count) if count > 0 else 0.0
        return {
            "total_revenue": float(total),
            "order_count": count,
            "avg_order_value": round(avg_val, 2),
            "max_order_value": float(max_val),
        }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--database", default="beammeup", help="PostgreSQL database")
    parser.add_argument("--username", default="beam_navigator", help="PostgreSQL username")
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    sample_sales = [
        {"region": "EMEA", "category": "ELECTRONICS", "amount": 250.00},
        {"region": "EMEA", "category": "ELECTRONICS", "amount": 450.00},
        {"region": "NA", "category": "BOOKS", "amount": 35.00},
        {"region": "NA", "category": "ELECTRONICS", "amount": 1200.00},
    ]

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "CreateSales" >> beam.Create(sample_sales)
            | "KeyByRegionCategory" >> beam.Map(lambda s: ((s["region"], s["category"]), s))
            | "AggregateCube" >> beam.CombinePerKey(SalesCubeAccumulator())
            | "FormatCubeRecord"
            >> beam.Map(
                lambda kv: {
                    "region": kv[0][0],
                    "category": kv[0][1],
                    "total_revenue": kv[1]["total_revenue"],
                    "order_count": kv[1]["order_count"],
                    "avg_order_value": kv[1]["avg_order_value"],
                    "max_order_value": kv[1]["max_order_value"],
                    "computed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                }
            )
            | "WriteSalesCube"
            >> WriteToPostgres(
                host=known_args.host,
                port=known_args.port,
                database=known_args.database,
                username=known_args.username,
                password_env_var="PGPASSWORD",
                table="public.olap_sales_cube",
                conflict_keys=["region", "category"],
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
