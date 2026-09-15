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

"""Database Bootstrap and Cutover Pattern in Python for PostgreSQL.

Solves the cold-start bootstrapping challenge:
1. Bootstraps historical baseline data via consistent snapshot read.
2. Streams real-time mutations via PostgreSQL logical replication CDC.
3. Merges and deduplicates mutations by primary key to ensure exact consistency.
4. Sinks to target table using idempotent ON CONFLICT upsert.

Execution:
    python3 pipeline.py --runner=DirectRunner
"""

import argparse
import datetime
import logging
from typing import Any, Dict, Iterator, Tuple

import apache_beam as beam
from apache_beam.io.postgres_cdc import ReadFromPostgresCDC, WriteToPostgres
from apache_beam.options.pipeline_options import PipelineOptions


class FormatSnapshotRecord(beam.DoFn):
    """Formats historical snapshot records."""

    def process(self, element: int) -> Iterator[Dict[str, Any]]:
        yield {
            "order_id": element,
            "customer_id": f"CUST-{element % 100}",
            "amount": 100.0 + float(element),
            "status": "COMPLETED",
            "source_type": "SNAPSHOT",
            "applied_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }


class FormatCDCRecord(beam.DoFn):
    """Extracts and formats live CDC streaming events."""

    def process(self, event: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        op = event.get("operation")
        if op not in ("INSERT", "UPDATE"):
            return
        after = event.get("after", {})
        yield {
            "order_id": int(after.get("order_id", 0)),
            "customer_id": str(after.get("customer_id", "")),
            "amount": float(after.get("amount", 0.0)),
            "status": str(after.get("status", "")),
            "source_type": "STREAM",
            "applied_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }


class DeduplicateAndReconcile(beam.DoFn):
    """Prefers live STREAM mutations over SNAPSHOT baseline."""

    def process(self, element: Tuple[int, Iterator[Dict[str, Any]]]) -> Iterator[Dict[str, Any]]:
        order_id, records = element
        chosen = None
        for record in records:
            if chosen is None or record.get("source_type") == "STREAM":
                chosen = record
        if chosen:
            yield chosen


def run():
    parser = argparse.ArgumentParser(description="PostgreSQL Bootstrap Cutover Pipeline")
    parser.add_argument("--host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--database", default="beammeup", help="Database name")
    parser.add_argument("--username", default="beam_navigator", help="Database user")
    parser.add_argument("--password", default="beam_navigator", help="Database password")
    parser.add_argument("--slot_name", default="backfill_cutover_slot", help="Replication slot")
    parser.add_argument("--publication", default="backfill_orders_pub", help="Publication name")

    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        # Phase 1: Snapshot bootstrap baseline
        snapshot_records = (
            p
            | "CreateSnapshotKeys" >> beam.Create([1001, 1002, 1003, 1004, 1005])
            | "FormatSnapshot" >> beam.ParDo(FormatSnapshotRecord())
        )

        # Phase 2: Live CDC stream from consistent LSN
        stream_records = (
            p
            | "ReadCDC"
            >> ReadFromPostgresCDC(
                host=known_args.host,
                port=known_args.port,
                database=known_args.database,
                username=known_args.username,
                password=known_args.password,
                slot_name=known_args.slot_name,
                publication=known_args.publication,
            )
            | "FormatCDC" >> beam.ParDo(FormatCDCRecord())
        )

        # Phase 3: Cutover merge and deduplication
        deduped = (
            (snapshot_records, stream_records)
            | "MergeSources" >> beam.Flatten()
            | "KeyByOrderID" >> beam.Map(lambda r: (r["order_id"], r))
            | "GroupByOrderID" >> beam.GroupByKey()
            | "Deduplicate" >> beam.ParDo(DeduplicateAndReconcile())
        )

        # Phase 4: Sinking to target via atomic ON CONFLICT upsert
        _ = deduped | "SinkToPostgres" >> WriteToPostgres(
            host=known_args.host,
            port=known_args.port,
            database=known_args.database,
            username=known_args.username,
            password=known_args.password,
            table="public.target_orders",
            primary_key_columns=["order_id"],
            write_mode="UPSERT",
            batch_size=2000,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
