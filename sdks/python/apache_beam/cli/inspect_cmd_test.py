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

"""Unit tests for `apache_beam.cli.inspect_cmd` (`beam inspect`)."""

import contextlib
import gzip
import io
import json
import os
import shutil
import tempfile
import unittest
from unittest import mock

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq

from apache_beam.cli import inspect_cmd
from apache_beam.cli import main as cli


def _run_json(argv):
  out_buf, err_buf = io.StringIO(), io.StringIO()
  with mock.patch.dict(os.environ, {'BEAM_NO_CONFIG': '1'}, clear=False):
    with contextlib.redirect_stdout(out_buf), contextlib.redirect_stderr(
        err_buf):
      rc = cli.main(['--json'] + list(argv))
  return rc, json.loads(out_buf.getvalue().strip()), err_buf.getvalue()


class InspectCmdTest(unittest.TestCase):

  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-cli-inspect-')

  def tearDown(self):
    shutil.rmtree(self.tmp, ignore_errors=True)

  def test_inspect_avro_search_stats_and_scan_budget_truncation(self):
    avro_path = os.path.join(self.tmp, 'events.avro')
    schema = fastavro.parse_schema({
        'type': 'record',
        'name': 'Event',
        'fields': [
            {'name': 'id', 'type': 'long'},
            {'name': 'region', 'type': ['null', 'string'], 'default': None},
        ],
    })
    records = [
        {'id': 1, 'region': 'us-west'},
        {'id': 2, 'region': None},
        {'id': 3, 'region': 'eu-central'},
        {'id': 4, 'region': 'ap-south'},
    ]
    with open(avro_path, 'wb') as f:
      fastavro.writer(f, schema, records)

    res = inspect_cmd.run_inspect(
        avro_path, sample_count=5, include_stats=True, search_term='EU-')
    self.assertEqual(res['format'], 'avro')
    self.assertEqual(len(res['schema']), 2)
    self.assertFalse(res['schema'][0]['nullable'])
    self.assertTrue(res['schema'][1]['nullable'])
    rows = res['untrusted_data']['rows']
    self.assertEqual(rows, [{'id': 3, 'region': 'eu-central'}])
    self.assertEqual(res['stats']['sampled_rows'], 1)

    res_all = inspect_cmd.run_inspect(
        avro_path, sample_count=5, include_stats=True)
    self.assertEqual(res_all['stats']['null_counts_in_sample']['region'], 1)

    res_trunc = inspect_cmd.run_inspect(
        avro_path, sample_count=10, max_scanned_bytes='1B')
    self.assertTrue(res_trunc['untrusted_data']['truncated'])

  def test_inspect_parquet_multishard_search_and_zero_sample(self):
    p1 = os.path.join(self.tmp, 'part-0.parquet')
    p2 = os.path.join(self.tmp, 'part-1.parquet')
    pq.write_table(
        pa.Table.from_pylist([
            {'id': 1, 'city': 'Seattle'},
            {'id': 2, 'city': 'Austin'},
        ]),
        p1)
    pq.write_table(
        pa.Table.from_pylist([
            {'id': 3, 'city': 'Boston'},
        ]),
        p2)
    glob_pat = os.path.join(self.tmp, 'part-*.parquet')

    res = inspect_cmd.run_inspect(glob_pat, sample_count=2, search_term='ost')
    self.assertEqual(res['file_count'], 2)
    self.assertEqual(res['row_count'], 3)
    self.assertEqual(
        res['untrusted_data']['rows'], [{'id': 3, 'city': 'Boston'}])

    res_zero = inspect_cmd.run_inspect(glob_pat, sample_count=0)
    self.assertEqual(res_zero['row_count'], 3)
    self.assertIsNone(res_zero['untrusted_data'])

  def test_inspect_csv_dirty_outliers_malformed_rows_gzip_and_delimiter(self):
    s1 = os.path.join(self.tmp, 'dirty-0.csv.gz')
    s2 = os.path.join(self.tmp, 'dirty-1.csv.gz')
    with gzip.open(s1, 'wt', encoding='utf-8') as f:
      f.write('id;amount;note\n')
      f.write('1;10.5;alpha\n')
      f.write('2;20.0;beta\n')
      f.write('3;30.25;\n')
      f.write('4;40.0;delta\n')
      f.write('5;not_a_float;outlier\n')
      f.write('6;only_two_cols\n')
    with gzip.open(s2, 'wt', encoding='utf-8') as f:
      # Second shard has no header (data row on line 0 with wrong column count)
      f.write('extra_bad_header_line\n')
      f.write('7;55.0;omega\n')

    res = inspect_cmd.run_inspect(
        os.path.join(self.tmp, 'dirty-*.csv.gz'),
        explicit_format='csv',
        sample_count=10,
        include_stats=True,
        from_opts=['delimiter=;'])
    self.assertEqual(res['format'], 'csv')
    by_name = {f['name']: f['type'] for f in res['schema']}
    self.assertEqual(by_name['amount'], 'float')
    self.assertTrue(
        any('dirty type-outlier' in w and 'malformed CSV' in w
            for w in res['warnings']),
        res['warnings'])
    self.assertGreaterEqual(res['stats']['malformed_rows_in_sample'], 2)
    self.assertEqual(res['stats']['null_counts_in_sample']['note'], 1)

  def test_inspect_json_array_jsonl_malformed_and_plain_text(self):
    arr_path = os.path.join(self.tmp, 'items.json')
    with open(arr_path, 'w', encoding='utf-8') as f:
      json.dump([
          {'id': 1, 'score': 10},
          {'id': 2, 'score': 12.5, 'tag': 'pro'},
      ], f)
    res_arr = inspect_cmd.run_inspect(arr_path, sample_count=5)
    types_arr = {f['name']: f['type'] for f in res_arr['schema']}
    self.assertEqual(types_arr['score'], 'float')
    self.assertEqual(types_arr['tag'], 'str')
    self.assertEqual(len(res_arr['untrusted_data']['rows']), 2)

    jl_path = os.path.join(self.tmp, 'mixed.jsonl')
    with open(jl_path, 'w', encoding='utf-8') as f:
      f.write('[{"k": 1}, {"k": 2}]\n')
      f.write('{corrupt json\n')
      f.write('42\n')
    res_jl = inspect_cmd.run_inspect(jl_path, sample_count=10)
    self.assertTrue(
        any('malformed JSON' in w for w in res_jl['warnings']),
        res_jl['warnings'])
    self.assertEqual(len(res_jl['untrusted_data']['rows']), 3)

    txt_path = os.path.join(self.tmp, 'notes.txt')
    with open(txt_path, 'w', encoding='utf-8') as f:
      f.write('hello world\nerror timeout\n')
    res_txt = inspect_cmd.run_inspect(
        txt_path, sample_count=5, search_term='timeout')
    self.assertEqual(res_txt['format'], 'text')
    self.assertEqual(
        res_txt['untrusted_data']['rows'], [{'line': 'error timeout'}])

  def test_inspect_non_file_endpoint_redacts_secret_options(self):
    opt_file = os.path.join(self.tmp, 'db_opts.yaml')
    with open(opt_file, 'w', encoding='utf-8') as f:
      f.write('username: reader\npassword: placeholder_value\n')
    os.chmod(opt_file, 0o600)
    res = inspect_cmd.run_inspect(
        'postgres://db.internal:5432/app?table=orders',
        from_opt_file=opt_file)
    self.assertEqual(res['kind'], 'postgres')
    self.assertEqual(res['config_preview']['username'], 'reader')
    self.assertEqual(res['config_preview']['password'], '<redacted>')
    self.assertIsNone(res['untrusted_data'])

  def test_inspect_bigquery_table_and_view_zero_cost_paths(self):
    class _FakeField:
      def __init__(self, name, field_type, mode='NULLABLE'):
        self.name = name
        self.field_type = field_type
        self.mode = mode

    fake_table = mock.MagicMock()
    fake_table.table_type = 'TABLE'
    fake_table.num_rows = 250
    fake_table.num_bytes = 8192
    fake_table.time_partitioning = object()
    fake_table.clustering_fields = ['region']
    fake_table.schema = [
        _FakeField('id', 'INT64', 'REQUIRED'),
        _FakeField('region', 'STRING', 'NULLABLE'),
    ]

    fake_client = mock.MagicMock()
    fake_client.project = 'demo-proj'
    fake_client.get_table.return_value = fake_table
    fake_client.list_rows.return_value = [
        {'id': 1, 'region': 'us-east'},
        {'id': 2, 'region': 'eu-west'},
    ]

    fake_bq_mod = mock.MagicMock()
    fake_bq_mod.Client.return_value = fake_client
    with mock.patch.dict(
        'sys.modules',
        {
            'google': mock.MagicMock(cloud=mock.MagicMock(bigquery=fake_bq_mod)),
            'google.cloud': mock.MagicMock(bigquery=fake_bq_mod),
            'google.cloud.bigquery': fake_bq_mod,
        }):
      res_tbl = inspect_cmd.run_inspect(
          'bigquery://demo-proj.analytics.events',
          sample_count=5,
          search_term='eu-')
      self.assertEqual(res_tbl['table_type'], 'TABLE')
      self.assertTrue(res_tbl['partitioning'])
      self.assertEqual(res_tbl['clustering_fields'], ['region'])
      self.assertFalse(res_tbl['schema'][0]['nullable'])
      self.assertEqual(
          res_tbl['untrusted_data']['rows'], [{'id': 2, 'region': 'eu-west'}])
      fake_client.list_rows.assert_called_once()

      # VIEW must NOT call list_rows (which fails on BigQuery views).
      fake_client.list_rows.reset_mock()
      fake_table.table_type = 'VIEW'
      fake_table.num_rows = None
      fake_job = mock.MagicMock()
      fake_job.total_bytes_processed = 4096
      fake_client.query.return_value = fake_job

      res_view = inspect_cmd.run_inspect(
          'bigquery://demo-proj.analytics.events_v', sample_count=5)
      self.assertEqual(res_view['table_type'], 'VIEW')
      self.assertEqual(res_view['estimated_bytes'], 4096)
      self.assertEqual(res_view['untrusted_data']['rows'], [])
      fake_client.list_rows.assert_not_called()

  def test_cli_inspect_missing_file_returns_json_error(self):
    rc, env, _ = _run_json(['inspect', os.path.join(self.tmp, 'missing.csv')])
    self.assertNotEqual(rc, cli.EXIT_OK)
    self.assertFalse(env['ok'])
    self.assertIsNotNone(env['error'])


if __name__ == '__main__':
  unittest.main()
