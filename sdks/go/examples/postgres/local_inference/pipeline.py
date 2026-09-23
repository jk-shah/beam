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

"""Real-Time Local Fraud Scoring in Python for PostgreSQL.

Ingests payment events via PostgreSQL CDC, scores risk locally in worker memory
using a logistic regression model with zero network hops, and writes predictions
back to PostgreSQL with idempotent ON CONFLICT upsert.

Execution:
    python3 pipeline.py --runner=DirectRunner
"""

import argparse
import datetime
import logging
import math

import apache_beam as beam
from apache_beam.io.postgres import ReadFromPostgresCDC, WriteToPostgresIO
from apache_beam.options.pipeline_options import PipelineOptions


class LocalFraudInferenceFn(beam.DoFn):
    """Executes embedded fraud risk scoring model in worker memory."""

    def __init__(self, intercept=-3.5, w_amount=0.002, w_distance=0.05, w_intl=1.2):
        self.intercept = intercept
        self.w_amount = w_amount
        self.w_distance = w_distance
        self.w_intl = w_intl

    def process(self, element):
        amount = float(element.get("amount", 0.0))
        distance = float(element.get("distance_from_home_km", 0.0))
        is_intl = 1.0 if element.get("is_international", False) else 0.0

        # Logit evaluation
        z = self.intercept + (self.w_amount * amount) + (self.w_distance * distance) + (self.w_intl * is_intl)
        prob = 1.0 / (1.0 + math.exp(-z))

        if prob >= 0.85:
            tier = "HIGH_RISK_BLOCK"
        elif prob >= 0.50:
            tier = "SUSPICIOUS_REVIEW"
        else:
            tier = "LOW_RISK_CLEAR"

        yield {
            "payment_id": str(element.get("payment_id")),
            "account_id": str(element.get("account_id")),
            "amount": amount,
            "fraud_probability": round(prob, 4),
            "risk_tier": tier,
            "scored_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--database", default="beammeup", help="PostgreSQL database")
    parser.add_argument("--username", default="beam_navigator", help="PostgreSQL username")
    parser.add_argument("--slot_name", default="fraud_inference_slot", help="CDC replication slot")
    parser.add_argument("--publication", default="fraud_inference_pub", help="PostgreSQL publication")
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
            | "FilterValidPayments"
            >> beam.Filter(lambda r: r.get("operation") in ("INSERT", "UPDATE") and float(r.get("amount", 0)) > 0)
            | "ScoreRisk" >> beam.ParDo(LocalFraudInferenceFn())
            | "WritePredictions"
            >> WriteToPostgresIO(
                host=known_args.host,
                port=known_args.port,
                database=known_args.database,
                username=known_args.username,
                password_env_var="PGPASSWORD",
                table="public.payment_fraud_predictions",
                conflict_keys=["payment_id"],
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
