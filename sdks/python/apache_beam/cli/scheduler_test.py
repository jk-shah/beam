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

"""Tests for Enterprise Orchestration Export (Proposal 4)."""

import ast
import json
import os
import shutil
import tempfile
import unittest

from apache_beam.cli import main
from apache_beam.cli import scheduler


class SchedulerValidationTest(unittest.TestCase):

  def test_validate_cron_expression(self):
    self.assertEqual(scheduler.validate_cron_expression('0 2 * * *'), '0 2 * * *')
    self.assertEqual(scheduler.validate_cron_expression('*/15 * * * *'), '*/15 * * * *')
    self.assertEqual(scheduler.validate_cron_expression('@daily'), '@daily')
    self.assertEqual(scheduler.validate_cron_expression('@HOURLY'), '@hourly')

    with self.assertRaises(scheduler.SchedulerError):
      scheduler.validate_cron_expression('')
    with self.assertRaises(scheduler.SchedulerError):
      scheduler.validate_cron_expression('0 2 * *')  # 4 parts
    with self.assertRaises(scheduler.SchedulerError):
      scheduler.validate_cron_expression('0 2 * * * *')  # 6 parts
    with self.assertRaises(scheduler.SchedulerError):
      scheduler.validate_cron_expression('invalid_schedule')


class ComposerDagGenerationTest(unittest.TestCase):

  def test_generate_composer_dag_valid_python(self):
    code = scheduler.generate_composer_dag(
        dag_id='daily_orders_silver_pipeline',
        schedule='0 2 * * *',
        project='my-company-prod',
        location='us-central1',
        template_gcs_path='gs://my-bucket/templates/orders.json',
        job_name_pattern='orders-silver-{{ ds_nodash }}',
        parameters={'input_date': '{{ ds }}'},
        environment={'serviceAccountEmail': 'sa@proj.iam.gserviceaccount.com'},
    )

    # Must be valid, parseable Python code
    parsed = ast.parse(code)
    self.assertIsNotNone(parsed)

    self.assertIn('from airflow import DAG', code)
    self.assertIn('DataflowStartFlexTemplateOperator', code)
    self.assertIn('dag_id=\'daily_orders_silver_pipeline\'', code)
    self.assertIn('schedule_interval=\'0 2 * * *\'', code)
    self.assertIn('"containerSpecGcsPath": \'gs://my-bucket/templates/orders.json\'', code)
    self.assertIn('sa@proj.iam.gserviceaccount.com', code)


class CloudSchedulerGenerationTest(unittest.TestCase):

  def test_generate_cloud_scheduler_script(self):
    script = scheduler.generate_cloud_scheduler_script(
        job_name='orders-daily-trigger',
        schedule='0 2 * * *',
        project='my-company-prod',
        location='us-central1',
        template_gcs_path='gs://my-bucket/templates/orders.json',
        service_account_email='scheduler-sa@proj.iam.gserviceaccount.com',
        parameters={'env': 'prod'},
    )

    self.assertIn('#!/usr/bin/env bash', script)
    self.assertIn('gcloud scheduler jobs create http', script)
    self.assertIn('https://dataflow.googleapis.com/v1b3/projects/${PROJECT}/locations/${LOCATION}/flexTemplates:launch', script)
    self.assertIn('scheduler-sa@proj.iam.gserviceaccount.com', script)
    self.assertIn('gs://my-bucket/templates/orders.json', script)


class RunExportScheduleTest(unittest.TestCase):

  def setUp(self):
    self.tmp_dir = tempfile.mkdtemp()
    self.yaml_file = os.path.join(self.tmp_dir, 'pipeline.yaml')
    with open(self.yaml_file, 'w', encoding='utf-8') as f:
      f.write("""
options:
  project: config-project
  region: us-east1
  service_account_email: runner-sa@config-project.iam.gserviceaccount.com
pipeline:
  transforms: []
""")

  def tearDown(self):
    shutil.rmtree(self.tmp_dir)

  def test_export_schedule_inherits_yaml_options(self):
    out_file = os.path.join(self.tmp_dir, 'dag.py')
    report = scheduler.run_export_schedule(
        self.yaml_file,
        schedule='@daily',
        orchestrator='composer',
        to_destination=out_file,
    )
    self.assertEqual(report.orchestrator, 'composer')
    self.assertEqual(report.schedule, '@daily')
    self.assertTrue(os.path.exists(out_file))

    with open(out_file, 'r', encoding='utf-8') as f:
      content = f.read()
      # Should inherit project and region from pipeline.yaml
      self.assertIn('config-project', content)
      self.assertIn('us-east1', content)
      self.assertIn('runner-sa@config-project.iam.gserviceaccount.com', content)

  def test_export_schedule_cloud_scheduler(self):
    out_file = os.path.join(self.tmp_dir, 'deploy.sh')
    report = scheduler.run_export_schedule(
        self.yaml_file,
        schedule='0 3 * * *',
        orchestrator='cloud-scheduler',
        to_destination=out_file,
        parameters=['batch_id=123'],
    )
    self.assertEqual(report.orchestrator, 'cloud-scheduler')
    self.assertEqual(report.schedule, '0 3 * * *')
    self.assertTrue(os.path.exists(out_file))

    with open(out_file, 'r', encoding='utf-8') as f:
      content = f.read()
      self.assertIn('gcloud scheduler jobs create http', content)
      self.assertIn('batch_id', content)


class CliExportScheduleIntegrationTest(unittest.TestCase):

  def setUp(self):
    self.tmp_dir = tempfile.mkdtemp()
    self.yaml_file = os.path.join(self.tmp_dir, 'orders.yaml')
    with open(self.yaml_file, 'w', encoding='utf-8') as f:
      f.write("pipeline:\n  transforms: []\n")

  def tearDown(self):
    shutil.rmtree(self.tmp_dir)

  def test_cli_export_schedule_json(self):
    argv = [
        'export-schedule',
        self.yaml_file,
        '--schedule', '0 4 * * *',
        '--project', 'test-proj',
        '--location', 'europe-west1',
        '--template-gcs-path', 'gs://bkt/orders.json',
        '--json',
    ]
    import io
    from contextlib import redirect_stdout
    buf = io.StringIO()
    with redirect_stdout(buf):
      code = main.main(argv)
    self.assertEqual(code, 0)
    data = json.loads(buf.getvalue().strip())
    self.assertTrue(data['ok'])
    self.assertEqual(data['command'], 'export-schedule')
    self.assertEqual(data['outputs']['orchestrator'], 'composer')
    self.assertEqual(data['outputs']['schedule'], '0 4 * * *')
    self.assertIn('DataflowStartFlexTemplateOperator', data['outputs']['content'])


if __name__ == '__main__':
  unittest.main()
