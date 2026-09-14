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

"""Database Sequence Reconciliation in Python for PostgreSQL.

Computes the maximum primary key during database migration or CDC cutover and
generates idempotent `SELECT setval(...)` statements to prevent primary key
collision errors when application write traffic is enabled on the target.

Execution:
    python3 pipeline.py --runner=DirectRunner
"""

import argparse
import datetime
import logging
from typing import Any, Dict, Iterator

import apache_beam as beam
from apache_beam.io.postgres_cdc import WriteToPostgres
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.combiners import MaxInt64Fn


class ExtractOrderIDFn(beam.DoFn):
    """Extracts integer primary keys from order records."""

    def process(self, record: Dict[str, Any]) -> Iterator[int]:
        order_id = int(record.get("order_id", 0))
        if order_id > 0:
            yield order_id


class GenerateSetValFn(beam.DoFn):
    """Generates setval SQL statements to advance sequence past max table value."""

    def __init__(self, table: str, column: str):
        super().__init__()
        self.table = table
        self.column = column

    def process(self, max_id: int) -> Iterator[str]:
        query = (
            f"SELECT setval(pg_get_serial_sequence('{self.table}', '{self.column}'), "
            f"{max_id}, true);"
        )
        logging.info("[SEQUENCE RECONCILIATION] Target: %s, MaxID: %d, SQL: %s", self.table, max_id, query)
        yield query


def run():
    parser = argparse.ArgumentParser(description="PostgreSQL Sequence Reconciliation Pipeline")
    parser.add_argument("--host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--database", default="postgres", help="Database name")
    parser.add_argument("--username", default="beam_test", help="Database user")
    parser.add_argument("--password", default="beam_test", help="Database password")
    parser.add_argument("--table", default="public.orders_sequence_demo", help="Target table")

    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        records = p | "CreateMigratedData" >> beam.Create([
            {"order_id": 100001, "description": "order_alpha", "migrated_at": datetime.datetime.now(datetime.timezone.utc).isoformat()},
            {"order_id": 100002, "description": "order_beta", "migrated_at": datetime.datetime.now(datetime.timezone.utc).isoformat()},
            {"order_id": 100050, "description": "order_gamma", "migrated_at": datetime.datetime.now(datetime.timezone.utc).isoformat()},
        ])

        # Sink to PostgreSQL target
        _ = records | "SinkMigratedOrders" >> WriteToPostgres(
            host=known_args.host,
            port=known_args.port,
            database=known_args.database,
            username=known_args.username,
            password=known_args.password,
            table=known_args.table,
            primary_key_columns=["order_id"],
            write_mode="UPSERT",
            batch_size=1000,
        )

        # Compute max primary key and emit reconciliation SQL
        _ = (
            records
            | "ExtractIDs" >> beam.ParDo(ExtractOrderIDFn())
            | "ComputeMaxID" >> beam.CombineGlobally(MaxInt64Fn())
            | "GenerateSetVal" >> beam.ParDo(GenerateSetValFn(table=known_args.table, column="order_id"))
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
