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

"""PostgreSQL Cross-Database Replication with Geo Fan-Out & PII Masking in Python.

Demonstrates streaming cross-database replication from a centralized PostgreSQL
source to regional destination databases (US, EU, APAC) with in-flight PII masking
(SSN redaction, email obfuscation) and SQL standard MERGEupserts.

Execution:
    python3 pipeline.py --runner=DirectRunner
"""

import argparse
import datetime
import logging
from typing import Any, Dict, Iterator

import apache_beam as beam
from apache_beam.io.postgres_cdc import ReadFromPostgresCDC, WriteToPostgres
from apache_beam.options.pipeline_options import PipelineOptions


def mask_ssn(ssn_str: str) -> str:
    """Masks SSN to ***-**-NNNN with strict input length checks."""
    if not ssn_str:
        return "***-**-****"
    cleaned = "".join(c for c in ssn_str if c.isdigit())
    if len(cleaned) == 9:
        return f"***-**-{cleaned[-4:]}"
    return "***-**-****"


def mask_email(email_str: str) -> str:
    """Obfuscates email addresses while preserving domain routing."""
    if not email_str or "@" not in email_str:
        return "***@redacted.local"
    local, domain = email_str.split("@", 1)
    if len(local) <= 2:
        return f"{local[0]}***@{domain}"
    return f"{local[0]}***{local[-1]}@{domain}"


class MaskCustomerPIIDoFn(beam.DoFn):
    """Parses CDC mutations and applies in-flight PII masking."""

    def __init__(self):
        super().__init__()
        self.records_ingested = beam.metrics.Metrics.counter("postgres_fanout", "records_ingested")
        self.ssn_masked = beam.metrics.Metrics.counter("postgres_fanout", "ssn_masked_count")

    def process(self, event: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        self.records_ingested.inc()
        op = event.get("operation", "INSERT")
        data = event.get("after", {}) if op != "DELETE" else event.get("before", {})

        ssn = str(data.get("ssn", ""))
        email = str(data.get("email", ""))
        region = str(data.get("geo_region", "OTHER")).upper().strip()

        masked_ssn = mask_ssn(ssn)
        masked_email = mask_email(email)
        self.ssn_masked.inc()

        yield {
            "account_id": str(data.get("account_id", "")),
            "customer_name": str(data.get("customer_name", "")),
            "ssn": masked_ssn,
            "email": masked_email,
            "geo_region": region,
            "balance": float(data.get("balance", 0.0)),
            "lsn": int(event.get("lsn", 0)),
            "_op_type": "d" if op == "DELETE" else ("u" if op == "UPDATE" else "c"),
            "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }


def geo_partition_fn(account: Dict[str, Any], num_partitions: int) -> int:
    """Partitions accounts into US (0), EU (1), or APAC/Default (2)."""
    region = account.get("geo_region", "")
    if region in ("US", "USA", "NA", "NORTH_AMERICA"):
        return 0
    if region in ("EU", "EMEA", "EUROPE", "UK"):
        return 1
    return 2


def run(argv=None):
    parser = argparse.ArgumentParser(description="PostgreSQL Cross-Database Geo Fan-Out & Masking")
    parser.add_argument("--src_host", default="localhost", help="Source PostgreSQL host")
    parser.add_argument("--src_port", type=int, default=5432, help="Source PostgreSQL port")
    parser.add_argument("--src_database", default="central_db", help="Source database name")
    parser.add_argument("--src_username", default="beam_cdc", help="Source replication user")
    parser.add_argument("--src_slot", default="beam_geo_slot", help="Source replication slot")
    parser.add_argument("--src_pub", default="pub_customer_accounts", help="Source publication")

    parser.add_argument("--us_host", default="localhost", help="US PostgreSQL host")
    parser.add_argument("--us_database", default="us_accounts_db", help="US database")

    parser.add_argument("--eu_host", default="localhost", help="EU PostgreSQL host")
    parser.add_argument("--eu_database", default="eu_accounts_db", help="EU database")

    parser.add_argument("--apac_host", default="localhost", help="APAC PostgreSQL host")
    parser.add_argument("--apac_database", default="apac_accounts_db", help="APAC database")

    parser.add_argument("--dest_user", default="beam_writer", help="Destination username")
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        # 1. Ingest Central CDC stream
        raw_events = p | "ReadCentralCDC" >> ReadFromPostgresCDC(
            host=known_args.src_host,
            port=known_args.src_port,
            database=known_args.src_database,
            username=known_args.src_username,
            password_env_var="PGPASSWORD",
            slot_name=known_args.src_slot,
            publication=known_args.src_pub,
            failover_slot=True,
        )

        # 2. In-flight PII Masking
        masked_accounts = raw_events | "MaskPII" >> beam.ParDo(MaskCustomerPIIDoFn())

        # 3. Geographic Partitioning
        partitions = masked_accounts | "GeoPartition" >> beam.Partition(geo_partition_fn, 3)

        us_branch = partitions[0] | "ReshuffleUS" >> beam.Reshuffle()
        eu_branch = partitions[1] | "ReshuffleEU" >> beam.Reshuffle()
        apac_branch = partitions[2] | "ReshuffleAPAC" >> beam.Reshuffle()

        # 4. Regional Sinks with SQL standard MERGE (PostgreSQL 15+)
        sink_kwargs = {
            "table": "public.customer_accounts",
            "primary_key": ["account_id"],
            "write_mode": "MERGE",
            "op_column": "_op_type",
            "delete_op_value": "d",
            "use_pgbouncer": True,
            "explain_analyze": True,
        }

        _ = us_branch | "WriteUS" >> WriteToPostgres(
            host=known_args.us_host,
            database=known_args.us_database,
            username=known_args.dest_user,
            password_env_var="PGPASSWORD",
            **sink_kwargs,
        )

        _ = eu_branch | "WriteEU" >> WriteToPostgres(
            host=known_args.eu_host,
            database=known_args.eu_database,
            username=known_args.dest_user,
            password_env_var="PGPASSWORD",
            **sink_kwargs,
        )

        _ = apac_branch | "WriteAPAC" >> WriteToPostgres(
            host=known_args.apac_host,
            database=known_args.apac_database,
            username=known_args.dest_user,
            password_env_var="PGPASSWORD",
            **sink_kwargs,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
