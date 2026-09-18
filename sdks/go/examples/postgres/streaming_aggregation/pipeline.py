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

"""Streaming Materialized View in Python for PostgreSQL.

Ingests transaction mutations via native logical replication CDC, computes
tumbling 1-minute aggregations per merchant, and maintains an analytics
rollup table using atomic ON CONFLICT upsert.

Execution:
    python3 pipeline.py --runner=DirectRunner
"""

import argparse
import datetime
from decimal import Decimal
import logging

import apache_beam as beam
from apache_beam.io.postgres import ReadFromPostgresCDC, WriteToPostgresIO
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows


class RollupAccumulator(beam.CombineFn):
    """Computes transaction count, total amount, and max amount per merchant window."""

    def create_accumulator(self):
        return (0, Decimal("0.0"), Decimal("0.0"))

    def add_input(self, accumulator, element):
        count, total, max_val = accumulator
        amt = Decimal(str(element.get("amount", "0.0")))
        return (count + 1, total + amt, max(max_val, amt))

    def merge_accumulators(self, accumulators):
        counts, totals, maxs = zip(*accumulators)
        return (sum(counts), sum(totals), max(maxs))

    def extract_output(self, accumulator):
        count, total, max_val = accumulator
        return {
            "transaction_count": count,
            "total_amount": float(total),
            "max_amount": float(max_val),
        }


class FormatRollupRecordFn(beam.DoFn):
    """Formats aggregated metrics with explicit window boundaries for PostgreSQL upsert."""

    def process(self, element, window=beam.DoFn.WindowParam):
        merchant_id, metrics = element
        start_ts = datetime.datetime.fromtimestamp(float(window.start), tz=datetime.timezone.utc)
        end_ts = datetime.datetime.fromtimestamp(float(window.end), tz=datetime.timezone.utc)
        yield {
            "merchant_id": merchant_id,
            "window_start": start_ts.isoformat(),
            "window_end": end_ts.isoformat(),
            "transaction_count": metrics["transaction_count"],
            "total_amount": metrics["total_amount"],
            "max_amount": metrics["max_amount"],
        }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--database", default="beammeup", help="PostgreSQL database")
    parser.add_argument("--username", default="beam_navigator", help="PostgreSQL username")
    parser.add_argument("--slot_name", default="beam_mv_slot", help="CDC replication slot")
    parser.add_argument("--publication", default="transactions_pub", help="PostgreSQL publication")
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
            | "FilterMutations"
            >> beam.Filter(lambda evt: evt.get("operation") in ("INSERT", "UPDATE") and "merchant_id" in evt)
            | "FixedWindow1m" >> beam.WindowInto(FixedWindows(60))
            | "KeyByMerchant" >> beam.Map(lambda evt: (evt["merchant_id"], evt))
            | "CombineRollups" >> beam.CombinePerKey(RollupAccumulator())
            | "FormatOutput" >> beam.ParDo(FormatRollupRecordFn())
            | "WriteToPostgres"
            >> WriteToPostgresIO(
                host=known_args.host,
                port=known_args.port,
                database=known_args.database,
                username=known_args.username,
                password_env_var="PGPASSWORD",
                table="public.merchant_minute_rollups",
                conflict_keys=["merchant_id", "window_start"],
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
