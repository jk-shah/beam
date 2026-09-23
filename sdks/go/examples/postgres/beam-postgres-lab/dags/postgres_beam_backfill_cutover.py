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

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "dba_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="postgres_beam_backfill_cutover",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    preflight_check = PostgresOperator(
        task_id="verify_replication_prerequisites",
        postgres_conn_id="postgres_primary",
        sql="""
            SELECT 1 WHERE current_setting('wal_level') = 'logical';
            SELECT 1 FROM pg_publication WHERE pubname = 'backfill_orders_pub';
        """,
    )

    export_snapshot = BashOperator(
        task_id="export_table_snapshot",
        bash_command="""
            python3 sdks/go/examples/postgres/operational_patterns/backfill_cutover/pipeline.py                 --mode=snapshot_export                 --output_path=/tmp/orders_snapshot.parquet
        """,
    )

    load_snapshot = BashOperator(
        task_id="load_snapshot_to_target",
        bash_command="""
            python3 sdks/go/examples/postgres/operational_patterns/backfill_cutover/pipeline.py                 --mode=snapshot_load                 --input_path=/tmp/orders_snapshot.parquet
        """,
    )

    start_streaming = BashOperator(
        task_id="start_cdc_streaming",
        bash_command="""
            go run sdks/go/examples/postgres/operational_patterns/backfill_cutover/main.go                 --runner=universal                 --endpoint=localhost:8099 &
        """,
    )

    reconcile_sequences = PostgresOperator(
        task_id="reconcile_sequences",
        postgres_conn_id="postgres_primary",
        sql="""
            SELECT setval(
                pg_get_serial_sequence('public.orders', 'order_id'),
                COALESCE(MAX(order_id), 1) + 1000
            ) FROM public.orders;
        """,
    )

    preflight_check >> export_snapshot >> load_snapshot >> start_streaming >> reconcile_sequences
