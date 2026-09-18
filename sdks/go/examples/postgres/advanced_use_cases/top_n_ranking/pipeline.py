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

"""Top-N Ranking per Category in Python for PostgreSQL.

Computes top-performing products per category partition using bounded in-memory heaps
and writes ranked results into PostgreSQL using atomic ON CONFLICT upsert.

Execution:
    python3 pipeline.py --runner=DirectRunner
"""

import argparse
import datetime
import heapq
import logging

import apache_beam as beam
from apache_beam.io.postgres import WriteToPostgresIO
from apache_beam.options.pipeline_options import PipelineOptions


class TopNPerCategoryFn(beam.DoFn):
    """Computes Top-N records per category using a min-heap to keep memory bounded."""

    def __init__(self, n=5):
        self.n = n

    def process(self, element):
        category, products = element
        # Bounded heap by sales_volume
        top_products = heapq.nlargest(
            self.n,
            products,
            key=lambda p: float(p.get("sales_volume", 0.0))
        )

        for rank, p in enumerate(top_products, start=1):
            yield {
                "category": str(category),
                "rank_position": rank,
                "product_id": str(p.get("product_id")),
                "product_name": str(p.get("product_name")),
                "sales_volume": float(p.get("sales_volume", 0.0)),
                "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--database", default="beammeup", help="PostgreSQL database")
    parser.add_argument("--username", default="beam_navigator", help="PostgreSQL username")
    parser.add_argument("--top_n", type=int, default=3, help="Top N items per partition")
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    sample_products = [
        {"category": "BOOKS", "product_id": "B-1", "product_name": "Database Internals", "sales_volume": 4500.0},
        {"category": "BOOKS", "product_id": "B-2", "product_name": "Designing Data-Intensive Apps", "sales_volume": 8900.0},
        {"category": "BOOKS", "product_id": "B-3", "product_name": "Go in Action", "sales_volume": 3200.0},
        {"category": "BOOKS", "product_id": "B-4", "product_name": "PostgreSQL Up & Running", "sales_volume": 5100.0},
        {"category": "AUDIO", "product_id": "A-1", "product_name": "Studio Monitors", "sales_volume": 12000.0},
        {"category": "AUDIO", "product_id": "A-2", "product_name": "Wireless Earbuds", "sales_volume": 18500.0},
    ]

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "CreateProducts" >> beam.Create(sample_products)
            | "KeyByCategory" >> beam.Map(lambda p: (p["category"], p))
            | "GroupByCategory" >> beam.GroupByKey()
            | "ComputeTopN" >> beam.ParDo(TopNPerCategoryFn(n=known_args.top_n))
            | "WriteTopProducts"
            >> WriteToPostgresIO(
                host=known_args.host,
                port=known_args.port,
                database=known_args.database,
                username=known_args.username,
                password_env_var="PGPASSWORD",
                table="public.top_products_by_category",
                conflict_keys=["category", "rank_position"],
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
