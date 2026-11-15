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

import os
import shutil
import subprocess
import sys
import tempfile
import types
import unittest
from unittest import mock

from apache_beam.cli import package


class PackageFlexTemplateTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp()
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

  def test_jinja_and_python_ast_parameter_extraction_and_validators(self):
    yaml_src = """
pipeline:
  transforms:
    - type: ReadFromParquet
      config:
        path: "gs://{{ input_bucket }}/events/{{ run_date | default('2026-11-15') }}/*.parquet"
    - type: WriteToBigQuery
      input: ReadFromParquet
      config:
        table: "{{ bq_table | d('proj.ds.tbl') }}"
"""
    params = package.extract_jinja_parameters(yaml_src)
    self.assertEqual([p['name'] for p in params], ['input_bucket', 'run_date', 'bq_table'])
    self.assertFalse(params[0]['isOptional'])
    self.assertTrue(params[1]['isOptional'])
    self.assertEqual(params[1]['defaultValue'], '2026-11-15')

    with self.assertRaises(package.PackageError):
      package.extract_jinja_parameters('pipeline: {{ unclosed')

    py_src = """
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.cli

class MyOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--input-path', required=True, help='Input URI')
    parser.add_value_provider_argument('--batch-size', dest='batch_sz', default=64)
"""
    py_params = package.extract_python_pipeline_parameters(py_src)
    self.assertEqual([p['name'] for p in py_params], ['input_path', 'batch_sz'])
    self.assertFalse(py_params[0]['isOptional'])
    self.assertEqual(py_params[1]['defaultValue'], '64')
    self.assertTrue(package._py_source_references_beam_cli(py_src))

    with self.assertRaises(package.PackageError):
      package.extract_python_pipeline_parameters('def broken(:')

    # Extra package detector across Enrichment / RunInference / Iceberg
    needed = package._detect_yaml_extra_packages({
        'pipeline': {
            'transforms': [
                {'type': 'CliEnrich', 'config': {'handler': 'feast', 'bigtable': True, 'redis': 'rediss://h'}},
                {'type': 'CliInfer', 'config': {'provider': 'hf'}},
                {'type': 'CliInfer', 'config': {'provider': 'vertexai'}},
                {'type': 'IcebergAddFiles', 'config': {}},
            ]
        }
    })
    for pkg_name in ('feast[gcp]', 'google-cloud-bigtable', 'redis', 'transformers', 'google-cloud-aiplatform', 'pyiceberg'):
      self.assertIn(pkg_name, needed)

  def test_yaml_packaging_airgapped_gcs_upload_and_security_guards(self):
    yaml_text = """pipeline:
  transforms:
    - type: ReadFromText
      config:
        path: "gs://{{ bucket }}/in/*.txt"
    - type: WriteToText
      input: ReadFromText
      config:
        path: "gs://{{ bucket }}/out/res"
"""
    uploaded: dict[str, bytes] = {}
    fake_storage = types.ModuleType('google.cloud.storage')

    class FakeBlob:
      def __init__(self, name):
        self.name = name

      def upload_from_string(self, data, content_type=None):
        del content_type
        uploaded[self.name] = data

      def download_as_bytes(self, start=0, end=None):
        del start, end
        return yaml_text.encode('utf-8')

    class FakeBucket:
      def blob(self, name):
        return FakeBlob(name)

    fake_storage.Client = lambda project=None: types.SimpleNamespace(
        bucket=lambda b: FakeBucket()
    )
    fake_gc = types.ModuleType('google.cloud')
    fake_gc.storage = fake_storage

    out_dir = os.path.join(self.tmp, 'yaml_out')
    with (
        mock.patch.dict(
            sys.modules,
            {'google.cloud': fake_gc, 'google.cloud.storage': fake_storage},
        ),
        mock.patch.object(
            package,
            'resolve_yaml_template_image',
            return_value=('gcr.io/dataflow-templates/2026/yaml-template', []),
        ),
    ):
      res = package.run_package(
          'gs://src-bkt/pipelines/orders.yaml',
          to_location='gs://dst-bkt/templates/orders.json',
          output_dir=out_dir,
          project='my-gcp-proj',
          region='us-central1',
          no_use_public_ips=True,
          subnetwork='regions/us-central1/subnetworks/priv',
          max_workers=8,
          enable_streaming_engine=True,
      )
    self.assertEqual(res['sdk_language'], 'YAML')
    self.assertIn('requirements_file: null', res['container_spec']['metadata']['yamlDefinition'])
    self.assertEqual(
        res['container_spec']['defaultEnvironment']['ipConfiguration'],
        'WORKER_IP_PRIVATE',
    )
    self.assertIn('templates/orders.json', uploaded)

    # Reject inline secrets or literal secret keys
    with self.assertRaisesRegex(package.PackageError, 'refusing to bake'):
      package.run_package(
          '-',
          stdin_text='pipeline:\n  transforms:\n    - type: ReadFromText\n      config:\n        redis_password: "plaintext-secret"\n',
          dry_run=True,
      )

    # Custom Python provider rejected when --sdk=yaml explicitly forced
    custom_yaml = """pipeline:
  transforms:
    - type: MyCustom
      config: {}
providers:
  - type: python
    config: {}
    transforms:
      MyCustom: apache_beam.cli.dlq.cli_count_rows
"""
    with self.assertRaisesRegex(package.PackageError, 'custom Python providers'):
      package.run_package('-', stdin_text=custom_yaml, sdk='yaml', dry_run=True)

  def test_python_and_auto_yaml_bundle_packaging_and_cloud_build(self):
    py_file = os.path.join(self.tmp, 'wordcount_job.py')
    req_file = os.path.join(self.tmp, 'requirements.txt')
    with open(py_file, 'w', encoding='utf-8') as f:
      f.write(
          'from apache_beam.options.pipeline_options import PipelineOptions\n'
          'import apache_beam.cli\n'
          'class Opts(PipelineOptions):\n'
          '  @classmethod\n'
          '  def _add_argparse_args(cls, parser):\n'
          '    parser.add_argument("--input-uri", required=True)\n'
      )
    with open(req_file, 'w', encoding='utf-8') as f:
      f.write('pyarrow>=16.0.0\n')

    bundle_dir = os.path.join(self.tmp, 'py_bundle')
    with (
        mock.patch.dict(
            os.environ, {'BEAM_CACHE_DIR': os.path.join(self.tmp, 'cache')}, clear=False
        ),
        mock.patch('shutil.which', return_value='/usr/bin/gcloud'),
        mock.patch(
            'subprocess.run',
            return_value=subprocess.CompletedProcess(args=[], returncode=0, stdout='ok', stderr=''),
        ),
    ):
      res = package.run_package(
          py_file,
          output_dir=bundle_dir,
          to_location=os.path.join(self.tmp, 'spec.json'),
          image='us-central1-docker.pkg.dev/my-gcp-proj/df/wc:v1',
          project='my-gcp-proj',
          region='us-central1',
          requirements_file=req_file,
          build_image=True,
      )
    self.assertEqual(res['sdk_language'], 'PYTHON')
    self.assertTrue(res['image_built'])
    self.assertTrue(os.path.isfile(os.path.join(bundle_dir, 'Dockerfile')))
    self.assertTrue(os.path.isfile(os.path.join(bundle_dir, 'metadata.json')))
    self.assertIn('requirements.txt', res['dockerfile'])

    # Cloud Build failure surfaces exit code 1
    with (
        mock.patch('shutil.which', return_value='/usr/bin/gcloud'),
        mock.patch(
            'subprocess.run',
            return_value=subprocess.CompletedProcess(
                args=[], returncode=1, stdout='', stderr='build quota exceeded'
            ),
        ),
    ):
      with self.assertRaisesRegex(package.PackageError, 'Cloud Build failed') as ctx:
        package.run_package(
            py_file,
            to_location=os.path.join(self.tmp, 'spec2.json'),
            image='us-central1-docker.pkg.dev/my-gcp-proj/df/wc:v1',
            project='my-gcp-proj',
            build_image=True,
        )
      self.assertEqual(ctx.exception.exit_code, 1)


if __name__ == '__main__':
  unittest.main()
