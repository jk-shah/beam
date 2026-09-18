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
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

def audit_slot_health(**kwargs):
    hook = PostgresHook(postgres_conn_id="postgres_primary")
    records = hook.get_records("SELECT slot_name, active, retained_bytes, health_status FROM public.beam_cdc_health")
    for slot_name, active, retained_bytes, health_status in records:
        logging.info(f"Slot: {slot_name}, Active: {active}, Retained: {retained_bytes} bytes, Health: {health_status}")
        if health_status == "WARNING_LAG_HIGH":
            raise ValueError(f"CRITICAL: Replication slot {slot_name} has accumulated {retained_bytes} bytes of WAL!")
        if not active and retained_bytes > 1073741824:
            logging.warning(f"ALERT: Disconnected slot {slot_name} is holding >1GB of WAL on primary.")

with DAG(
    dag_id="postgres_slot_hygiene_audit",
    default_args={"owner": "dba_team"},
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/10 * * * *",
    catchup=False,
) as dag:

    audit_task = PythonOperator(
        task_id="audit_replication_slots",
        python_callable=audit_slot_health,
    )
