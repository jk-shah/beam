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

with DAG(
    dag_id="postgres_partition_archival",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 2 1 * *",
    catchup=False,
) as dag:

    detach_partition = PostgresOperator(
        task_id="detach_old_partition",
        postgres_conn_id="postgres_primary",
        autocommit=True,
        sql="""
            ALTER TABLE public.orders_partitioned 
            DETACH PARTITION public.orders_2026_09_01 CONCURRENTLY;
        """,
    )

    archive_to_parquet = BashOperator(
        task_id="export_partition_to_parquet",
        bash_command="""
            python3 -m apache_beam.examples.postgres.archive_partition                 --table=public.orders_2026_09_01                 --output=gs://company-cold-archive/orders/2026_09_01.parquet
        """,
    )

    drop_detached_table = PostgresOperator(
        task_id="drop_detached_partition",
        postgres_conn_id="postgres_primary",
        sql="""
            DROP TABLE public.orders_2026_09_01;
        """,
    )

    detach_partition >> archive_to_parquet >> drop_detached_table
