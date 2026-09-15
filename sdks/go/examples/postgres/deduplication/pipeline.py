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

"""Stream Deduplication & Idempotent Upsert in Python for PostgreSQL.

Eliminates at-least-once stream duplicates within event-time windows and
writes canonical events into PostgreSQL using atomic ON CONFLICT upsert.

Execution:
    python3 pipeline.py --runner=DirectRunner
"""

import argparse
import datetime
import logging

import apache_beam as beam
from apache_beam.io.postgres_cdc import WriteToPostgres
from apache_beam.options.pipeline_options import PipelineOptions


class SelectLatestEventFn(beam.DoFn):
    """Picks the latest event version for each unique event ID."""

    def process(self, element):
        event_id, events = element
        # Sort by creation timestamp descending, pick latest
        latest = max(events, key=lambda e: e.get("created_at", ""))
        yield {
            "event_id": str(event_id),
            "source": str(latest.get("source", "UNKNOWN")),
            "payload": str(latest.get("payload", "")),
            "created_at": latest.get("created_at", datetime.datetime.now(datetime.timezone.utc).isoformat()),
        }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--database", default="beammeup", help="PostgreSQL database")
    parser.add_argument("--username", default="beam_navigator", help="PostgreSQL username")
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    # Sample duplicate events
    events = [
        {"event_id": "EVT-101", "source": "mobile", "payload": '{"click": 1}', "created_at": "2026-09-14T10:00:00Z"},
        {"event_id": "EVT-101", "source": "mobile", "payload": '{"click": 2}', "created_at": "2026-09-14T10:00:02Z"}, # Dupe/newer
        {"event_id": "EVT-102", "source": "web", "payload": '{"login": true}', "created_at": "2026-09-14T10:00:01Z"},
    ]

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "CreateEvents" >> beam.Create(events)
            | "KeyByEventID" >> beam.Map(lambda e: (e["event_id"], e))
            | "GroupByID" >> beam.GroupByKey()
            | "Deduplicate" >> beam.ParDo(SelectLatestEventFn())
            | "WriteCanonicalEvents"
            >> WriteToPostgres(
                host=known_args.host,
                port=known_args.port,
                database=known_args.database,
                username=known_args.username,
                password_env_var="PGPASSWORD",
                table="public.canonical_events",
                conflict_keys=["event_id"],
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
