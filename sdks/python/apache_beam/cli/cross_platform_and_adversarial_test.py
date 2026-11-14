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

"""Cross-platform portability (Linux/Windows/macOS) & round-2 adversarial tests."""

from __future__ import annotations

import ast
import io
import json
import os
import tempfile
import unittest
from unittest import mock

from apache_beam.cli import check_cmd
from apache_beam.cli import enrich
from apache_beam.cli import expect
from apache_beam.cli import fast_local
from apache_beam.cli import lakehouse
from apache_beam.cli import output
from apache_beam.cli import pipe_sql
from apache_beam.cli import scheduler
from apache_beam.cli import uris


class ExpectAndFastLocalAstHardeningTest(unittest.TestCase):
  """Verifies AST sandboxing across CliExpect, fast_local, and check_cmd."""
  def test_expect_blocks_dunder_comprehension_and_pow_dos(self):
    bad_rules = [
        'pwn: ().__class__.__base__.__subclasses__()',
        'amount._private > 0',
        '__import__("os").system("id")',
        'amount ** 999999 > 0',
        '1 << 1000000 > 0',
        '[x for x in [1, 2, 3]]',
        '(lambda x: x)(amount) > 0',
    ]
    for spec in bad_rules:
      with self.assertRaises(ValueError, msg=spec):
        expect.parse_expectation_rule(spec)

  def test_expect_allows_safe_sql_and_string_methods(self):
    r = expect.parse_expectation_rule(
        'valid_email: email IS NOT NULL AND email.endswith("@example.com") '
        'AND coalesce(amount, 0) >= 0 -> dlq')
    code = expect.compile_expectation_expression(r.expression)
    self.assertTrue(
        expect.evaluate_record(
            code, {
                'email': 'a@example.com',
                'amount': 10,
            }))
    self.assertFalse(
        expect.evaluate_record(code, {
            'email': 'a@evil.com',
            'amount': 10,
        }))

  def test_check_cmd_rejects_malicious_cli_expect_statically(self):
    with tempfile.NamedTemporaryFile('w',
                                     suffix='.yaml',
                                     delete=False,
                                     encoding='utf-8') as f:
      f.write(
          'pipeline:\n'
          '  transforms:\n'
          '    - type: Create\n'
          '      config:\n'
          '        elements: [{amount: 10}]\n'
          '    - type: CliExpect\n'
          '      input: Create\n'
          '      config:\n'
          '        rules:\n'
          '          - "().__class__.__base__"\n')
      yaml_path = f.name
    try:
      with self.assertRaises(check_cmd.CheckError):
        check_cmd.run_check(yaml_path, expand_plan=False)
    finally:
      os.unlink(yaml_path)

  def test_fast_local_blocks_unsafe_callable_and_expression(self):
    with self.assertRaises(fast_local.FastPipeError):
      fast_local._compile_callable('def bad(r):\n  return ().__class__\n')
    with self.assertRaises(fast_local.FastPipeError):
      fast_local._compile_expression('().__class__.__mro__')


class LakehouseAndSchedulerAdversarialTest(unittest.TestCase):
  """Verifies SSRF Google ADC token guards and Composer/Scheduler escaping."""
  def test_lakehouse_blocks_google_adc_exfiltration_to_non_googleapis_hosts(
      self):
    with mock.patch('urllib.request.build_opener') as mock_opener:
      with self.assertRaises(lakehouse.LakehouseError):
        lakehouse._resolve_via_biglake_catalog(
            'ns.tbl',
            {
                'uri': 'https://evil.example.com/biglake.googleapis.com',
                'rest.auth.type': 'google',
            },
        )
      with self.assertRaises(lakehouse.LakehouseError):
        lakehouse._resolve_via_generic_rest_catalog(
            'ns.tbl',
            {
                'uri': 'https://evil.example.com/iceberg',
                'rest.auth.type': 'google',
            },
        )
      with self.assertRaises(lakehouse.LakehouseError):
        lakehouse.register_iceberg_catalog_table(
            'ns.tbl',
            '/tmp/tbl',
            'gs://bkt/tbl/metadata/v1.metadata.json',
            catalog_name='biglake',
            catalog_properties={
                'uri': 'https://evil.example.com/biglake.googleapis.com',
            },
        )
      mock_opener.assert_not_called()

  def test_composer_dag_emits_valid_python_for_bool_and_none_env(self):
    dag_src = scheduler.generate_composer_dag(
        dag_id='nightly_sync',
        schedule='0 2 * * *',
        project='my-proj',
        location='us-central1',
        template_gcs_path='gs://my-proj/templates/sync.json',
        job_name_pattern='nightly-sync-{{ ds_nodash }}',
        parameters={'win_path': r'C:\Users\alice\input.parquet'},
        environment={
            'enableStreamingEngine': True,
            'subnetwork': None,
            'maxWorkers': 8,
        },
    )
    tree = ast.parse(dag_src)
    self.assertIsInstance(tree, ast.Module)
    self.assertIn('True', dag_src)
    self.assertIn('None', dag_src)

  def test_cloud_scheduler_script_quotes_shell_metachars_and_heredoc(self):
    script = scheduler.generate_cloud_scheduler_script(
        job_name='sync$(touch /tmp/pwn)',
        schedule='0 * * * *',
        project='proj"bad',
        location='us-central1',
        template_gcs_path='gs://bkt/t.json',
        service_account_email='sa@proj.iam.gserviceaccount.com"; id; #',
        parameters={'note': 'hello\n__BEAM_PAYLOAD_EOF__\nworld'},
    )
    self.assertNotIn('/tmp/launch_payload.json', script)
    self.assertIn('mktemp', script)
    self.assertIn("'sync$(touch /tmp/pwn)'", script)
    self.assertIn('__BEAM_PAYLOAD_EOF___X', script)


class CrossPlatformPortabilityTest(unittest.TestCase):
  """Verifies Windows (`nt`), Linux, and macOS path & FD portability."""
  def test_windows_drive_letter_endpoints_and_sql_sources(self):
    ep_csv = uris.parse_endpoint(r'C:\Users\alice\orders.csv', is_read=True)
    self.assertEqual(ep_csv.kind, 'file')
    self.assertEqual(ep_csv.scheme, 'local')
    self.assertEqual(ep_csv.format, 'csv')

    ep_pq = uris.parse_endpoint('D:/lakehouse/events.parquet', is_read=False)
    self.assertEqual(ep_pq.kind, 'file')
    self.assertEqual(ep_pq.format, 'parquet')

    # Alias=WindowsPath in SQL multi-source parsing
    sources = pipe_sql.parse_named_sources(
        [r'orders=C:\Users\alice\orders.parquet'], [])
    self.assertEqual(len(sources), 1)
    self.assertEqual(sources[0].alias, 'orders')
    self.assertEqual(sources[0].uri, r'C:\Users\alice\orders.parquet')

  def test_windows_iceberg_file_uri_roundtrip(self):
    # RFC-8089 Windows `file:///C:/...` -> `'C:/...'` without leading `/`!
    self.assertEqual(
        lakehouse._from_iceberg_uri('file:///C:/Users/alice/t/part-0.parquet'),
        'C:/Users/alice/t/part-0.parquet',
    )
    # Linux/macOS `file:///var/data/...` preserves leading `/`!
    self.assertEqual(
        lakehouse._from_iceberg_uri('file:///var/data/t/part-0.parquet'),
        '/var/data/t/part-0.parquet',
    )
    # Simulated Windows `abspath` returning `C:\Users\alice\p.parquet`
    with mock.patch(
        'os.path.abspath', return_value=r'C:\Users\alice\p.parquet'), \
        mock.patch.dict(os.environ, {'BEAM_ICEBERG_FILE_URI': ''}, clear=False):
      self.assertEqual(
          lakehouse._to_iceberg_uri('p.parquet'), 'C:/Users/alice/p.parquet')

  def test_enrich_naked_windows_drive_path_gives_clear_guidance(self):
    with self.assertRaises(ValueError) as ctx:
      enrich.parse_enrich_spec_str(r'C:\Users\alice\dims.jsonl')
    self.assertIn('file:path=', str(ctx.exception))
    self.assertNotIn("unknown --enrich handler 'c'", str(ctx.exception))

  def test_headless_windows_os_dup_oserror_falls_back_cleanly(self):
    buf_out = io.StringIO()
    buf_err = io.StringIO()
    fake_out = mock.MagicMock(wraps=buf_out)
    fake_out.fileno.return_value = 1
    fake_err = mock.MagicMock(wraps=buf_err)
    fake_err.fileno.return_value = 2
    with mock.patch('sys.stdout', fake_out), \
        mock.patch('sys.stderr', fake_err), \
        mock.patch('os.dup', side_effect=OSError('WinError 6 invalid handle')):
      with output.JsonOutputSession(enabled=True) as sess:
        sess.emit({'schema_version': '1.0', 'ok': True, 'exit_code': 0})
    parsed = json.loads(buf_out.getvalue().strip())
    self.assertTrue(parsed['ok'])


if __name__ == '__main__':
  unittest.main()
