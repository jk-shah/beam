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

"""Schema Evolution Handling in Python for PostgreSQL.

Demonstrates continuous stream adaptation when the underlying PostgreSQL table
undergoes schema changes (e.g. column additions), using defaults for backward
compatibility and recording schema drift metrics.

Execution:
    python3 pipeline.py --runner=DirectRunner
"""

import argparse
import datetime
import json
import logging
from typing import Any, Dict, Iterator

import apache_beam as beam
from apache_beam.io.postgres import ReadFromPostgresCDC, WriteToPostgresIO
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions


class SchemaAdaptiveTransform(beam.DoFn):
    """Processes CDC change events with backward-compatible schema adaptation."""

    def __init__(self):
        super().__init__()
        self.drift_counter = Metrics.counter("postgresio_cdc", "schema_drift_detected_total")

    def process(self, event: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        op = event.get("operation")
        if op not in ("INSERT", "UPDATE"):
            return

        raw = event.get("after", {})
        if not raw:
            return

        order_id = int(raw.get("order_id", 0))
        customer_id = str(raw.get("customer_id", "UNKNOWN"))
        amount = float(raw.get("amount", 0.0))

        # Accommodate newly introduced loyalty_tier column
        loyalty_tier = raw.get("loyalty_tier")
        schema_ver = 1
        if loyalty_tier is not None:
            schema_ver = 2
            loyalty_tier_str = str(loyalty_tier)
        else:
            loyalty_tier_str = "STANDARD"

        # Capture dynamic fields
        known = {"order_id", "customer_id", "amount", "loyalty_tier"}
        extra = {}
        for k, v in raw.items():
            if k not in known:
                extra[k] = v
                self.drift_counter.inc(1)
                schema_ver = 3

        yield {
            "order_id": order_id,
            "customer_id": customer_id,
            "amount": amount,
            "loyalty_tier": loyalty_tier_str,
            "extra_fields": json.dumps(extra),
            "schema_ver": schema_ver,
            "synced_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }


def run():
    parser = argparse.ArgumentParser(description="PostgreSQL Schema Evolution Pipeline")
    parser.add_argument("--host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--database", default="beammeup", help="Database name")
    parser.add_argument("--username", default="beam_navigator", help="Database user")
    parser.add_argument("--password", default="beam_navigator", help="Database password")
    parser.add_argument("--table", default="public.evolved_orders", help="Target table")

    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        cdc_stream = p | "ReadCDC" >> ReadFromPostgresCDC(
            host=known_args.host,
            port=known_args.port,
            database=known_args.database,
            username=known_args.username,
            password=known_args.password,
            slot_name="schema_evolution_slot",
            publication="orders_pub",
        )

        evolved = cdc_stream | "AdaptSchema" >> beam.ParDo(SchemaAdaptiveTransform())

        _ = evolved | "SinkToPostgres" >> WriteToPostgresIO(
            host=known_args.host,
            port=known_args.port,
            database=known_args.database,
            username=known_args.username,
            password=known_args.password,
            table=known_args.table,
            conflict_keys=["order_id"],
            write_mode="UPSERT",
            max_batch_rows=1000,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
