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

"""High-Throughput Vectorized Batch ETL in Python for PostgreSQL.

Demonstrates high-speed batch transformation and bulk upsert into PostgreSQL
using staged COPY and parameterized UNNEST under the hood.

Execution:
    python3 pipeline.py --runner=DirectRunner
"""

import argparse
import datetime
import logging

import apache_beam as beam
from apache_beam.io.postgres_cdc import WriteToPostgres
from apache_beam.options.pipeline_options import PipelineOptions


class TransformTransactionFn(beam.DoFn):
    """Standardizes codes, derives regions, and calculates financial loyalty tiers."""

    def process(self, element):
        order_id = int(element.get("order_id", 0))
        country = str(element.get("country_code", "US")).upper()
        amt = float(element.get("amount", 0.0))

        region = "NORTH_AMERICA" if country in ("US", "CA") else "INTERNATIONAL"
        tier = "VIP" if amt >= 1000.0 else "STANDARD"

        yield {
            "id": order_id,
            "code": country,
            "amount": round(amt, 2),
            "region": region,
            "tier": tier,
            "loaded_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--database", default="postgres", help="PostgreSQL database")
    parser.add_argument("--username", default="beam_test", help="PostgreSQL username")
    parser.add_argument("--rows", type=int, default=5000, help="Row count for batch ETL")
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        # Generate batch records or read from source table
        records = (
            p
            | "GenerateIDs" >> beam.Create(range(1, known_args.rows + 1))
            | "CreateRawRecords"
            >> beam.Map(
                lambda idx: {
                    "order_id": idx,
                    "country_code": "US" if idx % 2 == 0 else "GB",
                    "amount": round(15.50 + (idx % 1500), 2),
                }
            )
        )

        (
            records
            | "TransformAndEnrich" >> beam.ParDo(TransformTransactionFn())
            | "BulkUpsertPostgres"
            >> WriteToPostgres(
                host=known_args.host,
                port=known_args.port,
                database=known_args.database,
                username=known_args.username,
                password_env_var="PGPASSWORD",
                table="public.migrated_transactions",
                conflict_keys=["id"],
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
