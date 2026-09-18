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

"""Partitioned Table Sink in Python for PostgreSQL.

Demonstrates writing to range-partitioned tables using composite conflict targets
and upstream partition localization to prevent lock contention.

Execution:
    python3 pipeline.py --runner=DirectRunner
"""

import argparse
import datetime
import logging
from typing import Any, Dict, Iterator

import apache_beam as beam
from apache_beam.io.postgres import WriteToPostgresIO
from apache_beam.options.pipeline_options import PipelineOptions


class GeneratePartitionedOrdersFn(beam.DoFn):
    """Generates synthetic orders across multiple partition date ranges."""

    def process(self, order_id: int) -> Iterator[Dict[str, Any]]:
        day = (order_id % 3) + 1
        yield {
            "order_id": order_id,
            "order_date": f"2026-09-{day:02d}",
            "customer_id": f"USER-{order_id * 7 % 1000:04d}",
            "total_amount": 49.99 * float((order_id % 5) + 1),
            "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }


def run():
    parser = argparse.ArgumentParser(description="PostgreSQL Partitioned Table Sink")
    parser.add_argument("--host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--database", default="beammeup", help="Database name")
    parser.add_argument("--username", default="beam_navigator", help="Database user")
    parser.add_argument("--password", default="beam_navigator", help="Database password")
    parser.add_argument("--table", default="public.orders_partitioned", help="Partitioned target table")

    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        orders = (
            p
            | "CreateOrderIDs" >> beam.Create([1001, 1002, 1003, 1004, 1005, 1006, 1007])
            | "GenerateOrders" >> beam.ParDo(GeneratePartitionedOrdersFn())
        )

        # Localize by partition key before batch write to avoid cross-partition locks
        localized = (
            orders
            | "KeyByDate" >> beam.Map(lambda o: (o["order_date"], o))
            | "GroupByDate" >> beam.GroupByKey()
            | "FlattenDate" >> beam.FlatMap(lambda kv: kv[1])
        )

        _ = localized | "SinkToPartitionedPostgres" >> WriteToPostgresIO(
            host=known_args.host,
            port=known_args.port,
            database=known_args.database,
            username=known_args.username,
            password=known_args.password,
            table=known_args.table,
            conflict_keys=["order_id", "order_date"],
            write_mode="UPSERT",
            max_batch_rows=1000,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
