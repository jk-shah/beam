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

"""Slowly Changing Dimensions Type 2 (SCD2) in Python for PostgreSQL.

Maintains non-destructive historical versions of dimension records, generating
sequential versions and temporal validity intervals (valid_from, valid_to, is_current).

Execution:
    python3 pipeline.py --runner=DirectRunner
"""

import argparse
import datetime
import logging

import apache_beam as beam
from apache_beam.io.postgres_cdc import WriteToPostgres
from apache_beam.options.pipeline_options import PipelineOptions


class SCD2VersionGeneratorFn(beam.DoFn):
    """Sorts dimension mutations temporally and generates valid_from/valid_to intervals."""

    def process(self, element):
        customer_id, updates = element
        # Sort updates by timestamp ascending
        sorted_updates = sorted(updates, key=lambda u: u.get("updated_at", ""))
        total = len(sorted_updates)

        for idx, u in enumerate(sorted_updates):
            version = idx + 1
            is_latest = (idx == total - 1)
            valid_from = u.get("updated_at", datetime.datetime.now(datetime.timezone.utc).isoformat())

            if is_latest:
                valid_to = "9999-12-31T23:59:59Z"
                is_current = True
            else:
                valid_to = sorted_updates[idx + 1].get("updated_at", valid_from)
                is_current = False

            yield {
                "customer_id": str(customer_id),
                "version": version,
                "full_name": str(u.get("full_name", "")),
                "tier": str(u.get("tier", "STANDARD")),
                "address": str(u.get("address", "")),
                "valid_from": valid_from,
                "valid_to": valid_to,
                "is_current": is_current,
            }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--database", default="beammeup", help="PostgreSQL database")
    parser.add_argument("--username", default="beam_navigator", help="PostgreSQL username")
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    sample_dimension_updates = [
        {"customer_id": "CUST-1", "full_name": "Alice M. Smith", "tier": "SILVER", "address": "123 Main St", "updated_at": "2026-01-01T00:00:00Z"},
        {"customer_id": "CUST-1", "full_name": "Alice M. Smith", "tier": "GOLD", "address": "456 Market St", "updated_at": "2026-06-01T00:00:00Z"},
        {"customer_id": "CUST-2", "full_name": "Bob Jones", "tier": "BRONZE", "address": "789 Broadway", "updated_at": "2026-03-15T00:00:00Z"},
    ]

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "CreateUpdates" >> beam.Create(sample_dimension_updates)
            | "KeyByCustomer" >> beam.Map(lambda u: (u["customer_id"], u))
            | "GroupCustomerUpdates" >> beam.GroupByKey()
            | "GenerateSCD2Versions" >> beam.ParDo(SCD2VersionGeneratorFn())
            | "WriteCustomerHistory"
            >> WriteToPostgres(
                host=known_args.host,
                port=known_args.port,
                database=known_args.database,
                username=known_args.username,
                password_env_var="PGPASSWORD",
                table="public.customer_dim_history",
                conflict_keys=["customer_id", "version"],
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
