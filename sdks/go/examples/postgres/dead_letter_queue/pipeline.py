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

"""Dead-Letter Queue (DLQ) & Anomaly Routing in Python for PostgreSQL.

Demonstrates data validation and error isolation using:
1. Beam multi-output (TaggedOutput) for validation failures.
2. Built-in `WriteToPostgres` DLQ support (`result.failed_rows`) for write failures.

Execution:
    python3 pipeline.py --runner=DirectRunner
"""

import argparse
import datetime
import json
import logging

import apache_beam as beam
from apache_beam.io.postgres_cdc import WriteToPostgres
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import TaggedOutput


class ValidatePaymentFn(beam.DoFn):
    """Sanitizes payments, separating valid records from rejected anomalies."""

    TAG_DLQ = "dlq"

    def process(self, element):
        payment_id = str(element.get("payment_id", ""))
        amount = float(element.get("amount", 0.0))
        currency = str(element.get("currency", ""))
        status = str(element.get("status", ""))

        if amount <= 0:
            yield TaggedOutput(self.TAG_DLQ, {
                "payment_id": payment_id,
                "account_id": str(element.get("account_id")),
                "amount": amount,
                "error_code": "ERR_NON_POSITIVE_AMOUNT",
                "error_message": f"Payment amount {amount} must be positive",
                "raw_payload": json.dumps(element),
                "rejected_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            })
            return

        if currency not in ("USD", "EUR", "GBP"):
            yield TaggedOutput(self.TAG_DLQ, {
                "payment_id": payment_id,
                "account_id": str(element.get("account_id")),
                "amount": amount,
                "error_code": "ERR_UNSUPPORTED_CURRENCY",
                "error_message": f"Currency '{currency}' is unsupported",
                "raw_payload": json.dumps(element),
                "rejected_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            })
            return

        yield {
            "payment_id": payment_id,
            "account_id": str(element.get("account_id")),
            "amount": amount,
            "currency": currency,
            "status": status,
            "cleaned_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--database", default="postgres", help="PostgreSQL database")
    parser.add_argument("--username", default="beam_test", help="PostgreSQL username")
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    sample_payments = [
        {"payment_id": "PAY-001", "account_id": "ACC-A", "amount": 150.00, "currency": "USD", "status": "COMPLETED"},
        {"payment_id": "PAY-002", "account_id": "ACC-B", "amount": -25.00, "currency": "USD", "status": "PENDING"},
        {"payment_id": "PAY-003", "account_id": "ACC-C", "amount": 90.00, "currency": "XYZ", "status": "COMPLETED"},
        {"payment_id": "PAY-004", "account_id": "ACC-D", "amount": 340.50, "currency": "EUR", "status": "COMPLETED"},
    ]

    with beam.Pipeline(options=pipeline_options) as p:
        validated = (
            p
            | "CreatePayments" >> beam.Create(sample_payments)
            | "ValidateAndSanitize"
            >> beam.ParDo(ValidatePaymentFn()).with_outputs(ValidatePaymentFn.TAG_DLQ, main="clean")
        )

        # 1. Clean production table sink
        clean_result = validated.clean | "SinkCleanPayments" >> WriteToPostgres(
            host=known_args.host,
            port=known_args.port,
            database=known_args.database,
            username=known_args.username,
            password_env_var="PGPASSWORD",
            table="public.clean_payments",
            conflict_keys=["payment_id"],
        )

        # 2. Validation DLQ sink
        validation_dlq = validated[ValidatePaymentFn.TAG_DLQ]
        validation_dlq | "SinkValidationDLQ" >> WriteToPostgres(
            host=known_args.host,
            port=known_args.port,
            database=known_args.database,
            username=known_args.username,
            password_env_var="PGPASSWORD",
            table="public.dead_letter_payments",
            conflict_keys=["payment_id"],
        )

        # 3. Native Write DLQ for any rejected database rows
        clean_result.failed_rows | "LogDatabaseWriteErrors" >> beam.Map(
            lambda err: logging.error(f"Database write rejected row: {err}")
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
