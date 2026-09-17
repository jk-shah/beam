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

"""High Availability Failover Resilience in Python for PostgreSQL.

Demonstrates configuring PostgreSQL 17 failover-synchronized logical replication
slots (failover_slot=True), enabling streaming pipelines to survive standby promotions
without data loss.

Execution:
    python3 pipeline.py --runner=DirectRunner
"""

import argparse
import datetime
import logging
from typing import Any, Dict, Iterator

import apache_beam as beam
from apache_beam.io.postgres import ReadFromPostgresCDC, WriteToPostgresIO
from apache_beam.options.pipeline_options import PipelineOptions


class ExtractHARecord(beam.DoFn):
    """Filters and projects mutations replicated across HA failovers."""

    def process(self, event: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        op = event.get("operation")
        if op not in ("INSERT", "UPDATE"):
            return

        after = event.get("after", {})
        yield {
            "record_id": int(after.get("record_id", 0)),
            "payload": str(after.get("payload", "")),
            "confirmed_lsn": int(event.get("lsn", 0)),
            "replicated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }


def run():
    parser = argparse.ArgumentParser(description="PostgreSQL HA Failover Recovery Pipeline")
    parser.add_argument("--cluster_endpoint", default="pg-ha.internal", help="HA cluster VIP / endpoint")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--database", default="beammeup", help="Database name")
    parser.add_argument("--username", default="scotty", help="Database user")
    parser.add_argument("--password", default="scotty_secret", help="Database password")
    parser.add_argument("--slot_name", default="ha_failover_slot", help="Failover slot name")
    parser.add_argument("--publication", default="ha_critical_pub", help="Publication name")
    parser.add_argument("--target_table", default="public.ha_replicated_orders", help="Target table")

    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        # Ingest using failover-safe replication slot (PostgreSQL 17+)
        stream = p | "ReadHAStream" >> ReadFromPostgresCDC(
            host=known_args.endpoint,
            port=known_args.port,
            database=known_args.database,
            username=known_args.username,
            password=known_args.password,
            slot_name=known_args.slot_name,
            publication=known_args.publication,
            failover_slot=True,
        )

        records = stream | "ExtractRecords" >> beam.ParDo(ExtractHARecord())

        _ = records | "SinkToHATarget" >> WriteToPostgresIO(
            host=known_args.endpoint,
            port=known_args.port,
            database=known_args.database,
            username=known_args.username,
            password=known_args.password,
            table=known_args.target_table,
            conflict_keys=["record_id"],
            write_mode="UPSERT",
            max_batch_rows=1000,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
