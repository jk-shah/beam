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

"""PgBouncer Transaction Pooling Compatibility in Python.

Demonstrates writing event streams safely through PgBouncer in transaction
pooling mode, avoiding prepared statement collisions and session state leakage.

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


class GenerateEventsFn(beam.DoFn):
    """Generates synthetic telemetry records."""

    def process(self, event_id: int) -> Iterator[Dict[str, Any]]:
        yield {
            "event_id": event_id,
            "source": f"worker-{event_id % 8}",
            "payload": f"telemetry_payload_{event_id}",
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }


def run():
    parser = argparse.ArgumentParser(description="PostgreSQL PgBouncer Compat Pipeline")
    parser.add_argument("--host", default="localhost", help="PgBouncer host")
    parser.add_argument("--port", type=int, default=6432, help="PgBouncer port")
    parser.add_argument("--database", default="beammeup", help="Database name")
    parser.add_argument("--username", default="beam_transporter", help="Database user")
    parser.add_argument("--password", default="beam_transporter_pass", help="Database password")
    parser.add_argument("--table", default="public.pooled_events", help="Target table")

    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        events = (
            p
            | "CreateEventIDs" >> beam.Create([101, 102, 103, 104, 105, 106, 107, 108])
            | "GenerateEvents" >> beam.ParDo(GenerateEventsFn())
        )

        _ = events | "SinkViaPgBouncer" >> WriteToPostgres(
            host=known_args.host,
            port=known_args.port,
            database=known_args.database,
            username=known_args.username,
            password=known_args.password,
            table=known_args.table,
            primary_key_columns=["event_id"],
            write_mode="UPSERT",
            use_pgbouncer=True,
            batch_size=1000,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
