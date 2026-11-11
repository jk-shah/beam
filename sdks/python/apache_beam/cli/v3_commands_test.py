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

"""End-to-end functional & pipeline tests for extended `beam` CLI commands."""

import contextlib
import csv
import decimal
import glob
import io
import json
import math
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

from apache_beam.cli import diff
from apache_beam.cli import jobs
from apache_beam.cli import main as cli


def _run_json(argv, stdin_text=None, extra_env=None):
  out_buf = io.StringIO()
  err_buf = io.StringIO()
  env = {'BEAM_NO_CONFIG': '1'}
  if extra_env:
    env.update(extra_env)
  full_argv = list(argv) + (['--json'] if '--json' not in argv else [])
  with mock.patch.dict(os.environ, env, clear=False):
    if stdin_text is not None:
      with mock.patch('sys.stdin', io.StringIO(stdin_text)):
        with contextlib.redirect_stdout(out_buf), contextlib.redirect_stderr(
            err_buf):
          rc = cli.main(full_argv)
    else:
      with contextlib.redirect_stdout(out_buf), contextlib.redirect_stderr(
          err_buf):
        rc = cli.main(full_argv)
  raw_out = out_buf.getvalue().strip()
  payload = json.loads(raw_out)
  return rc, payload, err_buf.getvalue()


class PipeV3IntegrationTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-v3-test-')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

  def test_sharded_multiline_csv_dlq_where_select_assert_counts_and_parquet(self):
    shard_dir = os.path.join(self.tmp, 'raw')
    os.makedirs(shard_dir)
    # Shard 1: quoted multiline note + 1 bad column count + 1 negative amount
    with open(os.path.join(shard_dir, 'part-0.csv'), 'w', encoding='utf-8') as f:
      f.write('id,email,amount,note\n')
      f.write('1,ADA@EXAMPLE.COM,125.50,"line one\nline two"\n')
      f.write('2,corrupt_only_two_cols\n')
      f.write('3,bob@example.com,-10.00,"refund"\n')
    # Shard 2: repeated header + 1 bad float cell + 1 valid row
    with open(os.path.join(shard_dir, 'part-1.csv'), 'w', encoding='utf-8') as f:
      f.write('id,email,amount,note\n')
      f.write('4,GRACE@EXAMPLE.COM,250.00,"clean"\n')
      f.write('5,badnum@example.com,NOT_A_FLOAT,"oops"\n')

    out_pq = os.path.join(self.tmp, 'clean', 'orders.parquet')
    dlq_json = os.path.join(self.tmp, 'dlq', 'bad_orders.json')

    rc, env, err = _run_json([
        'pipe',
        '--from',
        os.path.join(shard_dir, 'part-*.csv'),
        '--to',
        out_pq,
        '--where',
        'amount > 0 AND id IS NOT NULL',
        '--select',
        'id, lower(email) AS email, round(amount, 2) AS amount, note',
        '--dlq',
        dlq_json,
        '--assert-counts',
        '--runner=FnApiRunner',
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertTrue(env['ok'])
    # Total rows across both shards (excluding shard headers): 5
    # - 2 corrupt source rows -> dlq_rows = 2
    # - 1 filtered out (`amount = -10.00`) -> rows_filtered_out = 1
    # - 2 written (`id=1`, `id=4`) -> rows_written = 2
    self.assertEqual(env['metrics']['rows_read'], 5)
    self.assertEqual(env['metrics']['dlq_rows'], 2)
    self.assertEqual(env['metrics']['rows_filtered_out'], 1)
    self.assertEqual(env['metrics']['rows_written'], 2)

    written_shards = sorted(glob.glob(out_pq + '*'))
    self.assertTrue(written_shards)
    rows = []
    for s in written_shards:
      rows.extend(pq.read_table(s).to_pylist())
    rows.sort(key=lambda r: r['id'])
    self.assertEqual(
        rows,
        [
            {
                'id': 1,
                'email': 'ada@example.com',
                'amount': 125.5,
                'note': 'line one\nline two',
            },
            {
                'id': 4,
                'email': 'grace@example.com',
                'amount': 250.0,
                'note': 'clean',
            },
        ])

    dlq_records = []
    for p in glob.glob(dlq_json + '*'):
      with open(p, encoding='utf-8') as f:
        for line in f:
          if line.strip():
            dlq_records.append(json.loads(line))
    self.assertEqual(len(dlq_records), 2)
    self.assertTrue(all(r['step'] == 'ReadCsvWithDlq' for r in dlq_records))

  def test_pipe_verify_and_if_exists_replace(self):
    src_csv = os.path.join(self.tmp, 'items.csv')
    with open(src_csv, 'w', encoding='utf-8') as f:
      f.write('sku,qty\nA1,10\nB2,20\n')
    dst_pq = os.path.join(self.tmp, 'out', 'items.parquet')

    rc, env, err = _run_json([
        'pipe',
        '--from',
        src_csv,
        '--to',
        dst_pq,
        '--verify',
        '--verify-key',
        'sku',
        '--runner=FnApiRunner',
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertTrue(env['outputs']['verify']['match'])

    # Re-running with `--if-exists=replace` succeeds cleanly.
    rc2, env2, err2 = _run_json([
        'pipe',
        '--from',
        src_csv,
        '--to',
        dst_pq,
        '--if-exists',
        'replace',
        '--runner=FnApiRunner',
    ])
    self.assertEqual(rc2, cli.EXIT_OK, err2)
    self.assertTrue(env2['ok'])


class NestedJsonPipeTest(unittest.TestCase):
  """Nested JSONL objects (Round 3 agent task T1)."""
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-nested-json-test-')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    ev = lambda i, t, u, amt: json.dumps({
        'event_id': i, 'type': t, 'user': {'id': u, 'country': 'US'},
        'props': {} if amt is None else {'amount': amt}})
    self.src = os.path.join(self.tmp, 'events.jsonl')
    with open(self.src, 'w', encoding='utf-8') as f:
      f.write('\n'.join([
          ev(1, 'purchase', 'u1', 12.5),
          ev(2, 'click', 'u2', None),
          ev(3, 'purchase', 'u3', 8.0),
          '{"event_id": 4, "type": "purchase", "user": {',
          ev(5, 'purchase', 'u4', 99.99),
          ev(6, 'purchase', 'u5', 'abc'),
          ev(7, 'purchase', 'u6', 10),
      ]) + '\n')

  def test_nested_fields_where_select_and_dlq(self):
    out_pq = os.path.join(self.tmp, 'purchases.parquet')
    dlq = os.path.join(self.tmp, 'dlq.jsonl')
    rc, env, err = _run_json([
        'pipe', '--from', self.src, '--to', out_pq,
        '--where', "type == 'purchase' and props.amount >= 10",
        '--select', 'event_id, user.id AS user_id, props.amount AS amount',
        '--dlq', dlq, '--assert-counts', '--runner=FnApiRunner'])
    self.assertEqual(rc, cli.EXIT_OK, err)
    # 7 lines: malformed line 4 and non-numeric amount 6 -> DLQ; 2 and 3 are
    # filtered out; 1, 5 and 7 (int 10 widened to float) are written.
    self.assertEqual(env['metrics']['rows_read'], 7)
    self.assertEqual(env['metrics']['dlq_rows'], 2)
    self.assertEqual(env['metrics']['rows_filtered_out'], 2)
    self.assertEqual(env['metrics']['rows_written'], 3)
    rows = sorted(pq.read_table(out_pq).to_pylist(), key=lambda r: r['event_id'])
    self.assertEqual(rows, [
        {'event_id': 1, 'user_id': 'u1', 'amount': 12.5},
        {'event_id': 5, 'user_id': 'u4', 'amount': 99.99},
        {'event_id': 7, 'user_id': 'u6', 'amount': 10.0},
    ])
    with open(dlq, encoding='utf-8') as f:
      self.assertEqual(len([l for l in f if l.strip()]), 2)

  def test_bad_line_without_dlq_fails_with_hint(self):
    rc, env, _ = _run_json([
        'pipe', '--from', self.src, '--to',
        os.path.join(self.tmp, 'all.parquet'), '--runner=FnApiRunner'])
    self.assertNotEqual(rc, cli.EXIT_OK)
    self.assertIn('--dlq', env['error']['message'])

  def test_nested_objects_written_as_json_objects(self):
    with open(self.src, 'w', encoding='utf-8') as f:
      f.write('{"id": 1, "user": {"id": "u1"}}\n{"id": 2, "user": {"id": "u2"}}\n')
    out_json = os.path.join(self.tmp, 'users.json')
    rc, _, err = _run_json([
        'pipe', '--from', self.src, '--to', out_json, '--runner=FnApiRunner'])
    self.assertEqual(rc, cli.EXIT_OK, err)
    with open(out_json, encoding='utf-8') as f:
      first = json.loads(f.readline())
    self.assertEqual(first['user'], {'id': 'u1'})

  def test_kind_inference_and_coercion_survive_pickling(self):
    from apache_beam.cli import dlq
    from apache_beam.internal import pickler
    kinds = dlq._infer_json_kind([
        {'n': 1, 'x': 1.5, 'tags': ['a'], 'u': {'id': 'a'}},
        {'n': 2, 'x': 2, 'tags': [], 'u': {'id': 'b'}},
        {'n': 3, 'x': 'oops', 'u': {}},
        {'n': 4, 'x': 3.0},
    ])
    self.assertEqual(
        kinds, {'n': 'int', 'x': 'float', 'tags': 'json', 'u': {'id': 'str'}})
    kinds = pickler.loads(pickler.dumps(kinds))
    self.assertEqual(dlq._coerce_json_value(2, kinds['x'], 'x'), 2.0)
    self.assertEqual(dlq._coerce_json_value(['a'], kinds['tags'], 'tags'), '["a"]')
    self.assertEqual(dlq._coerce_json_value({'id': 'z'}, kinds['u'], 'u').id, 'z')
    with self.assertRaisesRegex(ValueError, "field 'x'"):
      dlq._coerce_json_value('oops', kinds['x'], 'x')
    with self.assertRaisesRegex(ValueError, 'expected a JSON object'):
      dlq._coerce_json_value('flat', kinds['u'], 'u')


class CdcIntoFilesTest(unittest.TestCase):
  """--cdc into plain files writes the latest live row per key."""
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-cdc-files-test-')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

  def _pipe(self, src, *extra):
    out = os.path.join(self.tmp, 'latest.parquet')
    rc, env, err = _run_json([
        'pipe', '--from', src, '--to', out, '--cdc', '--primary-key', 'id',
        '--assert-counts', '--runner=FnApiRunner', *extra])
    return rc, env, err, out

  def test_flat_csv_keeps_latest_version_and_updated_at(self):
    src = os.path.join(self.tmp, 'updates.csv')
    with open(src, 'w', encoding='utf-8') as f:
      f.write('id,status,updated_at\n'
              '1,active,2026-11-01T10:00:00\n'
              '1,suspended,2026-11-03T08:00:00\n'
              '2,active,2026-11-01T09:00:00\n'
              '1,active,2026-11-02T15:00:00\n')
    expected = [
        {'id': 1, 'status': 'suspended', 'updated_at': '2026-11-03T08:00:00'},
        {'id': 2, 'status': 'active', 'updated_at': '2026-11-01T09:00:00'},
    ]
    # Round 7 H1: naming the data column with --cdc-seq-col must not drop it.
    for extra in ((), ('--cdc-seq-col', 'updated_at')):
      if os.path.exists(os.path.join(self.tmp, 'latest.parquet')):
        os.remove(os.path.join(self.tmp, 'latest.parquet'))
      rc, env, err, out = self._pipe(src, *extra)
      self.assertEqual(rc, cli.EXIT_OK, err)
      self.assertEqual(env['metrics']['rows_written'], 2)
      self.assertEqual(env['metrics']['cdc_superseded_rows'], 2)
      rows = sorted(pq.read_table(out).to_pylist(), key=lambda r: r['id'])
      self.assertEqual(rows, expected, extra)

  def test_debezium_delete_removes_key(self):
    src = os.path.join(self.tmp, 'cdc.jsonl')
    events = [
        {'op': 'c', 'source': {'lsn': 1}, 'after': {'id': 1, 'v': 'a'}},
        {'op': 'c', 'source': {'lsn': 2}, 'after': {'id': 2, 'v': 'b'}},
        {'op': 'u', 'source': {'lsn': 4}, 'after': {'id': 1, 'v': 'c'}},
        {'op': 'u', 'source': {'lsn': 3}, 'after': {'id': 1, 'v': 'stale'}},
        {'op': 'd', 'source': {'lsn': 5}, 'before': {'id': 2, 'v': 'b'}},
    ]
    with open(src, 'w', encoding='utf-8') as f:
      f.write('\n'.join(json.dumps(e) for e in events) + '\n')
    rc, env, err, out = self._pipe(src)
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(pq.read_table(out).to_pylist(), [{'id': 1, 'v': 'c'}])
    self.assertEqual(env['metrics']['rows_written'], 1)

  def test_unorderable_changes_fail_with_hint(self):
    src = os.path.join(self.tmp, 'noseq.csv')
    with open(src, 'w', encoding='utf-8') as f:
      f.write('id,status\n1,a\n1,b\n')
    rc, env, _, _ = self._pipe(src)
    self.assertNotEqual(rc, cli.EXIT_OK)
    self.assertIn('--cdc-seq-col', env['error']['message'])


class OfflineRobustnessTest(unittest.TestCase):
  """check --plan without network, and the Prism exec probe."""
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-offline-test-')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

  def test_plan_falls_back_to_static_on_remote_access_error(self):
    from apache_beam.cli import check_cmd
    self.assertTrue(check_cmd._is_remote_access_error(
        RuntimeError("List operation failed for 'gs://bucket/x/'")))
    self.assertFalse(check_cmd._is_remote_access_error(
        ValueError('Unknown type or missing provider for type Foo')))
    path = os.path.join(self.tmp, 'p.yaml')
    with open(path, 'w', encoding='utf-8') as f:
      f.write('pipeline:\n  type: chain\n  transforms:\n'
              '  - type: Create\n    config:\n      elements: [1]\n'
              '  - type: LogForTesting\n')
    with mock.patch(
        'apache_beam.yaml.main.build_pipeline_components_from_yaml',
        side_effect=None) as build:
      build.return_value = (None, mock.Mock(
          side_effect=OSError('Operation not permitted: gs://acme/raw')))
      rc, env, _ = _run_json(['check', path, '--plan'])
    self.assertEqual(rc, cli.EXIT_OK)
    self.assertEqual(env['outputs']['mode'], 'offline_static')
    self.assertIn('remote storage', env['warnings'][0])

  def test_plan_hides_auth_log_noise(self):
    import logging
    path = os.path.join(self.tmp, 'p.yaml')
    with open(path, 'w', encoding='utf-8') as f:
      f.write('pipeline:\n  type: chain\n  transforms:\n'
              '  - type: Create\n    config:\n      elements: [1]\n'
              '  - type: LogForTesting\n')

    def noisy_constructor(_):
      logging.getLogger('google.auth').error(
          'Traceback: MutualTLSChannelError noise')
      sys.stderr.write('RefreshError traceback noise\n')
      raise RuntimeError('RefreshError: Reauthentication is needed.')

    with mock.patch(
        'apache_beam.yaml.main.build_pipeline_components_from_yaml',
        return_value=(None, noisy_constructor)):
      rc, env, err = _run_json(['check', path, '--plan'])
    self.assertEqual(rc, cli.EXIT_OK)
    self.assertEqual(env['outputs']['mode'], 'offline_static')
    self.assertIn('RefreshError', env['outputs']['plan_skipped_detail'])
    self.assertTrue(env['outputs']['plan_skipped_reason'].startswith('check passed'))
    self.assertNotIn('noise', err)
    # Logging is restored afterwards.
    self.assertEqual(logging.root.manager.disable, logging.NOTSET)

  def test_emit_yaml_explains_bigquery_write_empty(self):
    src = os.path.join(self.tmp, 'in.csv')
    with open(src, 'w', encoding='utf-8') as f:
      f.write('id,name\n1,a\n')
    argv = ['pipe', '--from', src, '--to', 'bigquery://p.ds.t', '--emit-yaml']
    rc, env, _ = _run_json(argv)
    self.assertEqual(rc, cli.EXIT_OK)
    self.assertIn('write_disposition: WRITE_EMPTY', env['outputs']['yaml'])
    self.assertTrue(any('--if-exists append' in w for w in env['warnings']))
    rc, env, _ = _run_json(argv + ['--if-exists', 'append'])
    self.assertEqual(rc, cli.EXIT_OK)
    self.assertFalse(any('WRITE_EMPTY' in w for w in env['warnings']))

  def test_prism_probe_requires_a_runnable_binary(self):
    ok = os.path.join(self.tmp, 'prism')
    with open(ok, 'w', encoding='utf-8') as f:
      f.write('#!/bin/sh\nexit 0\n')
    os.chmod(ok, 0o755)
    self.assertTrue(cli._prism_binary_runs(ok))
    self.assertFalse(cli._prism_binary_runs(os.path.join(self.tmp, 'missing')))

  def test_io_describe_covers_yaml_transforms(self):
    rc, env, _ = _run_json(['io', 'describe', 'Filter'])
    self.assertEqual(rc, cli.EXIT_OK)
    info = env['outputs']['connector']
    self.assertEqual(info['kind'], 'yaml_transform')
    self.assertIn('keep', info['config_fields'])
    rc, env, _ = _run_json(['io', 'describe', 'no_such_thing'])
    self.assertEqual(rc, cli.EXIT_USAGE)
    self.assertIn('transforms:', env['error']['message'])


class LocalSqlDialectTest(unittest.TestCase):
  """Local --sql statistics and dialect hints (Round 3 task T2)."""
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-sql-dialect-test-')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.src = os.path.join(self.tmp, 'latency.parquet')
    pq.write_table(
        pa.table({
            'kind': ['a'] * 5,
            'ms': [10.0, 20.0, 30.0, 40.0, 100.0],
        }),
        self.src)

  def _sql(self, query):
    return _run_json(['pipe', '--from', f'l={self.src}', '--sql', query])

  def test_exact_percentiles_match_numpy_linear(self):
    rc, env, err = self._sql(
        'SELECT median(ms) AS med, percentile(ms, 95) AS p95, '
        'quantile(ms, 0.25) AS q25, stddev(ms) AS sd FROM l')
    self.assertEqual(rc, cli.EXIT_OK, err)
    row = env['untrusted_data']['rows'][0]
    self.assertAlmostEqual(row['med'], 30.0)
    self.assertAlmostEqual(row['p95'], 88.0)  # numpy.percentile(..., 95)
    self.assertAlmostEqual(row['q25'], 20.0)
    self.assertAlmostEqual(row['sd'], 35.355339, places=5)

  def test_regression_and_bivariate_aggregates(self):
    pts_file = os.path.join(self.tmp, 'ad_spend.csv')
    with open(pts_file, 'w', encoding='utf-8') as f:
      f.write('ad_spend,revenue\n')
      f.write('10,25.0\n20,41.0\n30,62.0\n40,79.0\n50,98.0\n'
              '60,122.0\n70,139.0\n80,160.0\n90,175.0\n100,201.0\n')

    # Test on default SQLite engine
    rc, env, err = _run_json([
        'pipe', '--from', f'a={pts_file}', '--sql',
        'SELECT '
        'regr_slope(revenue, ad_spend) AS slope, '
        'regr_intercept(revenue, ad_spend) AS intercept, '
        'regr_r2(revenue, ad_spend) AS r2, '
        'corr(revenue, ad_spend) AS r, '
        'covar_pop(revenue, ad_spend) AS covar_p, '
        'regr_slope(revenue, ad_spend) * 150 + regr_intercept(revenue, ad_spend) AS pred '
        'FROM a'
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    row = env['untrusted_data']['rows'][0]
    self.assertAlmostEqual(row['slope'], 1.9490909, places=5)
    self.assertAlmostEqual(row['intercept'], 3.0, places=5)
    self.assertAlmostEqual(row['r2'], 0.9985911, places=5)
    self.assertAlmostEqual(row['r'], 0.9992953, places=5)
    self.assertAlmostEqual(row['pred'], 295.363636, places=4)

    # Test on DuckDB engine
    rc_duck, env_duck, err_duck = _run_json([
        'pipe', '--from', f'a={pts_file}', '--sql-engine', 'duckdb', '--sql',
        'SELECT '
        'regr_slope(revenue, ad_spend) AS slope, '
        'regr_intercept(revenue, ad_spend) AS intercept, '
        'regr_r2(revenue, ad_spend) AS r2, '
        'corr(revenue, ad_spend) AS r '
        'FROM a'
    ])
    self.assertEqual(rc_duck, cli.EXIT_OK, err_duck)
    row_duck = env_duck['untrusted_data']['rows'][0]
    self.assertAlmostEqual(row_duck['slope'], 1.9490909, places=5)
    self.assertAlmostEqual(row_duck['intercept'], 3.0, places=5)
    self.assertAlmostEqual(row_duck['r2'], 0.9985911, places=5)
    self.assertAlmostEqual(row_duck['r'], 0.9992953, places=5)

  def test_other_dialect_percentiles_get_a_hint(self):
    for query in (
        'SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ms) FROM l',
        'SELECT APPROX_QUANTILES(ms, 100) FROM l'):
      rc, env, _ = self._sql(query)
      self.assertEqual(rc, cli.EXIT_USAGE)
      self.assertIn('percentile(col, 95)', env['error']['message'])

  def test_unknown_table_hint_mentions_alias(self):
    rc, env, _ = _run_json(['pipe', '--from', self.src, '--sql', 'SELECT * FROM t'])
    self.assertNotEqual(rc, cli.EXIT_OK)
    self.assertIn('--from ALIAS=PATH', env['error']['message'])
    self.assertIn('available tables: latency', env['error']['message'])

  def test_local_only_function_detection(self):
    from apache_beam.cli import pipe_sql
    self.assertEqual(
        pipe_sql.local_only_sql_functions(
            'SELECT median(x), percentile (y, 9), COUNT(*) FROM t'),
        ['median', 'percentile'])
    self.assertEqual(pipe_sql.local_only_sql_functions('SELECT SUM(x) FROM t'), [])


class NestedParquetPipeTest(unittest.TestCase):
  """Struct columns in --where/--select (works around apache/beam#41100)."""
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-nested-parquet-test-')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.src = os.path.join(self.tmp, 'people.parquet')
    pq.write_table(
        pa.table({
            'name': ['a', 'b', 'c'],
            'address': [
                {'city': 'SF', 'zip': 1},
                {'city': 'NY', 'zip': 2},
                None,
            ],
        }),
        self.src)

  def _check(self, *extra):
    out = os.path.join(self.tmp, 'out.parquet')
    rc, env, err = _run_json([
        'pipe', '--from', self.src, '--to', out,
        '--where', 'address is not None and address.zip >= 2',
        '--select', 'name, address.city AS city',
        '--assert-counts', '--runner=FnApiRunner', *extra])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(pq.read_table(out).to_pylist(), [{'name': 'b', 'city': 'NY'}])
    self.assertEqual(env['metrics']['rows_written'], 1)

  def test_nested_struct_fields(self):
    self._check()

  def test_nested_struct_fields_with_dlq(self):
    self._check('--dlq', os.path.join(self.tmp, 'dlq.json'))


class SchemaDriftDiffTest(unittest.TestCase):
  """beam diff separates schema drift from row differences (Round 3 T5)."""
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-diff-drift-test-')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.v1 = os.path.join(self.tmp, 'v1.parquet')
    self.v2 = os.path.join(self.tmp, 'v2.parquet')
    pq.write_table(
        pa.table({
            'sku': ['A1', 'A2', 'A3'],
            'price': pa.array([10, 20, 30], pa.int64()),
        }),
        self.v1)
    pq.write_table(
        pa.table({
            'sku': ['A1', 'A2', 'A4'],
            'price': pa.array([10, 25, 40], pa.int64()),
            'category': ['tools', 'tools', 'toys'],
        }),
        self.v2)

  def _diff(self, *extra):
    return _run_json(
        ['diff', self.v1, self.v2, '--key', 'sku', '--runner=FnApiRunner',
         *extra])

  def test_shared_columns_by_default(self):
    rc, env, _ = self._diff()
    self.assertEqual(rc, cli.EXIT_DIFF_MISMATCH)
    self.assertEqual(env['metrics']['matched_rows'], 1)
    self.assertEqual(env['metrics']['value_mismatches'], 1)
    self.assertEqual(env['outputs']['schema_diff'], {
        'columns_added': ['category'],
        'columns_removed': [],
        'type_changes': {},
        'compared_columns': 'shared',
    })

  def test_all_columns_treats_missing_as_null(self):
    rc, env, _ = self._diff('--columns', 'all')
    self.assertEqual(rc, cli.EXIT_DIFF_MISMATCH)
    self.assertEqual(env['metrics']['matched_rows'], 0)
    self.assertEqual(env['metrics']['value_mismatches'], 2)

  def test_type_change_and_identical_data_still_differs(self):
    pq.write_table(
        pa.table({
            'sku': ['A1', 'A2', 'A3'],
            'price': pa.array([10.0, 20.0, 30.0], pa.float64()),
        }),
        self.v2)
    rc, env, _ = self._diff()
    self.assertEqual(rc, cli.EXIT_DIFF_MISMATCH)
    self.assertFalse(env['outputs']['match'])
    self.assertEqual(env['metrics']['value_mismatches'], 0)
    self.assertEqual(
        env['outputs']['schema_diff']['type_changes'],
        {'price': {'left': 'int', 'right': 'float'}})

  def test_differing_keys_are_listed(self):
    rc, env, _ = self._diff()
    self.assertEqual(rc, cli.EXIT_DIFF_MISMATCH)
    self.assertEqual(
        env['outputs']['differing_keys'], {
            'only_in_left': ['A3'],
            'only_in_right': ['A4'],
            'value_mismatches': ['A2'],
            'duplicate_keys': [],
            'limit': 100,
            'truncated': False,
            'values_are_untrusted': True,
        })

  def test_differing_keys_are_capped(self):
    from apache_beam.cli import diff as diff_mod
    with mock.patch.object(diff_mod, 'KEY_LIST_LIMIT', 1):
      pq.write_table(
          pa.table({
              'sku': ['B1', 'B2', 'B3'],
              'price': pa.array([1, 2, 3], pa.int64()),
          }),
          self.v2)
      rc, env, _ = self._diff()
    self.assertEqual(rc, cli.EXIT_DIFF_MISMATCH)
    keys = env['outputs']['differing_keys']
    self.assertEqual(env['metrics']['only_in_left'], 3)
    self.assertEqual(len(keys['only_in_left']), 1)
    self.assertTrue(keys['truncated'])

  def test_no_key_lists_without_keys(self):
    rc, env, _ = _run_json(
        ['diff', self.v1, self.v2, '--runner=FnApiRunner'])
    self.assertEqual(rc, cli.EXIT_DIFF_MISMATCH)
    self.assertIsNone(env['outputs']['differing_keys'])


class DiffV3ReconciliationTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-diff-test-')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

  def _write_parquet(self, name, table):
    path = os.path.join(self.tmp, name)
    pq.write_table(table, path)
    return path

  def test_exact_match_nan_null_keys_decimal_and_extensionless_sniffing(self):
    dec_type = pa.decimal128(10, 2)
    left_tbl = pa.table({
        'k1': pa.array([1, None, 3], type=pa.int64()),
        'score': pa.array([float('nan'), 1.5, 2.0], type=pa.float64()),
        'amt': pa.array(
            [decimal.Decimal('10.50'), decimal.Decimal('0.00'), decimal.Decimal('3.20')],
            type=dec_type),
    })
    # Write right file WITHOUT `.parquet` extension to test `PAR1` magic sniffing!
    left_path = self._write_parquet('left.parquet', left_tbl)
    right_path = self._write_parquet('right_shard_00000_of_00001', left_tbl)

    rc, env, err = _run_json(['diff', left_path, right_path, '--key', 'k1'])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertTrue(env['outputs']['match'])
    self.assertEqual(env['metrics']['matched_rows'], 3)

  def test_value_mismatch_and_rtol_tolerance(self):
    left_p = self._write_parquet(
        'l.parquet', pa.table({'id': [1, 2], 'val': [100.0, 200.0]}))
    right_p = self._write_parquet(
        'r.parquet', pa.table({'id': [1, 2], 'val': [100.0, 200.0001]}))

    # Strict diff exits with 6 (`EXIT_DIFF_MISMATCH`).
    rc, env, _ = _run_json(['diff', left_p, right_p, '--key', 'id'])
    self.assertEqual(rc, diff.EXIT_DIFF_MISMATCH)
    self.assertFalse(env['ok'])
    self.assertEqual(env['metrics']['value_mismatches'], 1)
    self.assertIsNotNone(env.get('untrusted_data'))

    # With `--rtol 1e-4`, floats match within tolerance -> exit 0!
    rc_tol, env_tol, err_tol = _run_json(
        ['diff', left_p, right_p, '--key', 'id', '--rtol', '1e-4'])
    self.assertEqual(rc_tol, cli.EXIT_OK, err_tol)
    self.assertTrue(env_tol['outputs']['match'])

  def test_duplicate_primary_keys_detected(self):
    left_p = self._write_parquet(
        'dup_l.parquet', pa.table({'id': [1, 1], 'v': ['a', 'b']}))
    right_same = self._write_parquet(
        'dup_r_same.parquet', pa.table({'id': [1, 1], 'v': ['b', 'a']}))
    right_diff = self._write_parquet(
        'dup_r_diff.parquet', pa.table({'id': [1], 'v': ['a']}))

    rc_ok, _, _ = _run_json(['diff', left_p, right_same, '--key', 'id'])
    self.assertEqual(rc_ok, cli.EXIT_OK)

    rc_bad, env_bad, _ = _run_json(['diff', left_p, right_diff, '--key', 'id'])
    self.assertEqual(rc_bad, diff.EXIT_DIFF_MISMATCH)
    self.assertEqual(env_bad['metrics']['duplicate_key_mismatches'], 1)


class InspectCheckIoDoctorTailJobsTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-cmds-test-')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.beam_home = os.path.join(self.tmp, '.beam')

  def test_inspect_parquet_and_search(self):
    p_path = os.path.join(self.tmp, 'events.parquet')
    pq.write_table(
        pa.table({
            'event_id': [10, 20, 30],
            'kind': ['click', 'checkout_failed', 'view'],
        }),
        p_path)
    rc, env, err = _run_json(
        ['inspect', p_path, '--stats', '--search', 'CHECKOUT'])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(env['outputs']['row_count'], 3)
    rows = env['untrusted_data']['rows']
    self.assertEqual(len(rows), 1)
    self.assertEqual(rows[0]['event_id'], 20)

  def test_check_stdin_and_expanded_plan(self):
    yaml_doc = (
        'pipeline:\n'
        '  type: chain\n'
        '  transforms:\n'
        '    - type: Create\n'
        '      config:\n'
        '        elements: [{x: 1}, {x: 2}]\n')
    rc, env, err = _run_json(['check', '-', '--plan'], stdin_text=yaml_doc)
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(env['outputs']['mode'], 'expanded_plan')
    self.assertTrue(env['outputs']['plan_hash'].startswith('sha256:'))

  def test_io_doctor_version_and_llms_txt(self):
    rc1, env1, _ = _run_json(['io', 'list'])
    self.assertEqual(rc1, cli.EXIT_OK)
    names = {c['name'] for c in env1['outputs']['connectors']}
    self.assertIn('bigquery', names)
    self.assertIn('pubsub', names)

    rc2, env2, _ = _run_json(['doctor'])
    self.assertEqual(rc2, cli.EXIT_OK)
    self.assertTrue(env2['outputs']['extras']['yaml'])
    self.assertTrue(env2['outputs']['extras']['pyarrow'])

    rc3, env3, _ = _run_json(['--llms-txt'])
    self.assertEqual(rc3, cli.EXIT_OK)
    self.assertIn('untrusted_data', env3['outputs']['llms_txt'])

  def test_tail_pubsub_nack_peek_never_acks(self):
    fake_msg = mock.Mock()
    fake_msg.message_id = 'm-1'
    fake_msg.data = b'{"click_id": 99}'
    fake_msg.attributes = {'env': 'prod'}
    fake_msg.publish_time = None
    fake_rm = mock.Mock()
    fake_rm.ack_id = 'ack-xyz'
    fake_rm.message = fake_msg

    fake_sub_client = mock.MagicMock()
    fake_sub_client.pull.return_value = mock.Mock(received_messages=[fake_rm])

    with mock.patch(
        'google.cloud.pubsub_v1.SubscriberClient',
        return_value=fake_sub_client):
      rc, env, err = _run_json([
          'tail',
          'pubsub://projects/p1/subscriptions/s1',
          '--limit',
          '1',
          '--duration',
          '1s',
      ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(env['outputs']['sampled_count'], 1)
    self.assertEqual(env['untrusted_data']['rows'][0]['payload'], {'click_id': 99})
    # CRITICAL: `acknowledge` MUST NEVER be called, and `modify_ack_deadline`
    # must release the message with `ack_deadline_seconds=0`!
    fake_sub_client.acknowledge.assert_not_called()
    fake_sub_client.modify_ack_deadline.assert_called_once()
    call_req = fake_sub_client.modify_ack_deadline.call_args.kwargs['request']
    self.assertEqual(call_req['ack_deadline_seconds'], 0)

  def test_jobs_idempotency_wait_and_cancel(self):
    env_vars = {'BEAM_HOME': self.beam_home}
    with mock.patch.dict(os.environ, env_vars, clear=False):
      idem = jobs.compute_idempotency_key(['pipe', '--from', 'a'], 'sha256:123')
      jobs.record_job_submission(
          '2026-11-07_job-abc',
          runner='DataflowRunner',
          state='JOB_STATE_RUNNING',
          command='pipe',
          project='my-proj',
          region='us-central1',
          idem_key=idem,
          post_checks={'assert_counts': True})
      found = jobs.lookup_recent_submission(idem)
      self.assertIsNotNone(found)
      self.assertEqual(found['job_id'], '2026-11-07_job-abc')

      # Poll `wait_for_job` with mocked Dataflow query returning DONE + balanced counters.
      code, rec = jobs.wait_for_job(
          '2026-11-07_job-abc',
          query_fn=lambda jid, p, r: (
              'JOB_STATE_DONE',
              {'rows_read': 42, 'rows_written': 40, 'dlq_rows': 2},
          ))
      self.assertEqual(code, cli.EXIT_OK)
      self.assertEqual(rec['metrics']['rows_written'], 40)

      # Cancel with `--drain`.
      cancelled = jobs.cancel_job(
          '2026-11-07_job-abc',
          drain=True,
          cancel_fn=lambda jid, p, r, st: st == 'JOB_STATE_DRAINING')
      self.assertEqual(cancelled['requested_state'], 'JOB_STATE_DRAINING')

  def test_dogfood_polish_shard_prefix_emit_yaml_plan_hash_and_dirty_warnings(self):
    dirty_csv = os.path.join(self.tmp, 'dirty.csv')
    with open(dirty_csv, 'w', encoding='utf-8') as f:
      f.write('id,amt\n1,10.5\n2,20.0\n3,30.0\n4,ERR\n5\n')

    # 1. `inspect` warns on malformed/dirty CSV rows in sample!
    rc_i, env_i, _ = _run_json(['inspect', dirty_csv, '--stats'])
    self.assertEqual(rc_i, cli.EXIT_OK)
    self.assertTrue(env_i['warnings'])
    self.assertEqual(env_i['outputs']['stats']['malformed_rows_in_sample'], 1)

    # 2. Write sharded Parquet, then read & diff WITHOUT trailing `*`!
    out_pq = os.path.join(self.tmp, 'sharded', 'clean.parquet')
    dlq_p = os.path.join(self.tmp, 'sharded', 'dlq.json')
    rc_p, _, err_p = _run_json([
        'pipe',
        '--from', dirty_csv,
        '--to', out_pq,
        '--dlq', dlq_p,
        '--runner=FnApiRunner',
    ])
    self.assertEqual(rc_p, cli.EXIT_OK, err_p)

    out_jsonl = os.path.join(self.tmp, 'sharded', 'copy.jsonl')
    rc_p2, _, err_p2 = _run_json([
        'pipe',
        '--from', out_pq,  # no `*`!
        '--to', out_jsonl,
        '--runner=FnApiRunner',
    ])
    self.assertEqual(rc_p2, cli.EXIT_OK, err_p2)

    rc_d, env_d, err_d = _run_json([
        'diff', out_pq, out_jsonl, '--key', 'id', '--runner=FnApiRunner'
    ])
    self.assertEqual(rc_d, cli.EXIT_OK, err_d)
    self.assertEqual(env_d['runner'], 'FnApiRunner')

    # 3. `--emit-yaml` succeeds even though `out_pq` already exists, and matches
    # `beam check` `plan_hash` + expands `--plan` without `--trust-providers`!
    rc_ey, env_ey, err_ey = _run_json([
        'pipe',
        '--from', dirty_csv,
        '--to', out_pq,
        '--dlq', dlq_p,
        '--emit-yaml',
    ])
    self.assertEqual(rc_ey, cli.EXIT_OK, err_ey)
    yaml_file = os.path.join(self.tmp, 'exported.yaml')
    with open(yaml_file, 'w', encoding='utf-8') as f:
      f.write(env_ey['outputs']['yaml'])

    rc_chk, env_chk, err_chk = _run_json(['check', '--plan', yaml_file])
    self.assertEqual(rc_chk, cli.EXIT_OK, err_chk)
    self.assertEqual(
        env_ey['outputs']['plan_hash'], env_chk['outputs']['plan_hash'])

  def test_preview_wheel_staging_and_worker_unpickle_roundtrip(self):
    import subprocess
    import sys
    import zipfile
    from apache_beam.internal import pickler

    preview_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', '..', 'cli-preview'))
    if preview_root not in sys.path:
      sys.path.insert(0, preview_root)
    from apache_beam_cli_preview import launcher

    cache_dir = os.path.join(self.tmp, 'wheel_cache')
    with mock.patch.dict(os.environ, {'BEAM_CACHE_DIR': cache_dir}):
      whl_path = launcher.ensure_preview_wheel()
      self.assertTrue(os.path.isfile(whl_path))
      with zipfile.ZipFile(whl_path, 'r') as zf:
        names = set(zf.namelist())
      self.assertIn('apache_beam_cli_preview.pth', names)
      self.assertIn('apache_beam_cli_preview/_worker_bootstrap.py', names)
      self.assertIn('apache_beam_cli_preview/_bundled_cli/main.py', names)
      self.assertIn('apache_beam_cli_preview/_bundled_cli/dlq.py', names)
      self.assertIn('apache_beam_cli_preview/_bundled_cli/diff.py', names)

      injected = launcher.maybe_inject_dataflow_worker_args(
          ['--runner=DataflowRunner', '--project=p'])
      self.assertIn(f'--extra_package={whl_path}', injected)
      self.assertIn(
          '--beam_plugin=apache_beam_cli_preview._worker_bootstrap', injected)
      self.assertEqual(
          injected, launcher.maybe_inject_dataflow_worker_args(injected))

      # Simulate a remote Dataflow worker container (without repo `sdks/python`
      # `apache_beam/cli` if extracted into an isolated site dir) unpickling
      # DoFns/PTransforms from both `apache_beam.cli.*` and `_apache_beam_cli_pkg.*`!
      from apache_beam.cli import dlq
      csv_sample = os.path.join(self.tmp, 's.csv')
      with open(csv_sample, 'w', encoding='utf-8') as f:
        f.write('id,v\n1,10\n')
      payload = pickler.dumps((
          dlq.read_csv_with_dlq(csv_sample),
          dlq.cli_count_rows('rows_written'),
          dlq.cli_write_to_bigquery('p:d.t'),
      ))
      payload_file = os.path.join(self.tmp, 'pickled.bin')
      with open(payload_file, 'wb') as f:
        f.write(payload)

      worker_site = os.path.join(self.tmp, 'worker_site')
      with zipfile.ZipFile(whl_path, 'r') as zf:
        zf.extractall(worker_site)

      check_code = (
          'import sys; '
          f'sys.path.insert(0, {worker_site!r}); '
          'import apache_beam_cli_preview._worker_bootstrap; '
          'from apache_beam.internal import pickler; '
          'import _apache_beam_cli_pkg.diff as fast_diff; '
          'import apache_beam.typehints.arrow_type_compatibility as atc; '
          'import apache_beam.yaml.yaml_mapping as ym; '
          f'objs = pickler.loads(open({payload_file!r}, "rb").read()); '
          'assert len(objs) == 3; '
          'assert getattr(atc.PyarrowBatchConverter, "_cli_preview_patched", False); '
          'assert getattr(ym.validate_generic_expression, "_cli_preview_patched", False); '
          'assert callable(fast_diff.run_diff)'
      )
      proc = subprocess.run(
          [sys.executable, '-c', check_code],
          capture_output=True,
          text=True,
          check=False)
      self.assertEqual(proc.returncode, 0, proc.stderr)

  def test_remote_diff_requires_temp_location_and_wait_resolves_output_files(self):
    from apache_beam.cli import jobs
    left = os.path.join(self.tmp, 'l.csv')
    right = os.path.join(self.tmp, 'r.csv')
    with open(left, 'w', encoding='utf-8') as f:
      f.write('id,val\n1,a\n2,b\n')
    with open(right, 'w', encoding='utf-8') as f:
      f.write('id,val\n1,a\n2,b\n')

    # 1. `beam diff --runner=DataflowRunner` without `--temp_location` fails fast.
    rc_d, env_d, _ = _run_json([
        'diff', left, right, '--key', 'id', '--runner=DataflowRunner'
    ])
    self.assertEqual(rc_d, cli.EXIT_USAGE)
    self.assertIn('--temp_location', env_d['error']['message'])

    # 2. `jobs.wait_for_job` resolves sharded `output_prefix` & `verify_diff`.
    shard = os.path.join(self.tmp, 'out.csv-00000-of-00001')
    with open(shard, 'w', encoding='utf-8') as f:
      f.write('id,val\n1,a\n2,b\n')
    dlq_shard = os.path.join(self.tmp, 'dlq.json-00000-of-00001')
    with open(dlq_shard, 'w', encoding='utf-8') as f:
      f.write('')

    with mock.patch.dict(os.environ, {'BEAM_HOME': self.tmp}):
      jobs.record_job_submission(
          '2026-11-08_job-verify',
          runner='DataflowRunner',
          state='JOB_STATE_RUNNING',
          command='pipe',
          project='demo-proj',
          region='us-central1',
          post_checks={
              'assert_counts': True,
              'verify_diff': {
                  'left': left,
                  'right': os.path.join(self.tmp, 'out.csv'),
                  'keys': ['id'],
              },
          },
          outputs={
              'output_prefix': os.path.join(self.tmp, 'out.csv'),
              'dlq_prefix': os.path.join(self.tmp, 'dlq.json'),
          })
      code, rec = jobs.wait_for_job(
          '2026-11-08_job-verify',
          poll_interval=0.01,
          query_fn=lambda jid, p, r: (
              'JOB_STATE_DONE',
              {
                  'rows_read': 2,
                  'rows_filtered_out': 0,
                  'dlq_rows': 0,
                  'rows_written': 2,
              }))
      self.assertEqual(code, cli.EXIT_OK)
      self.assertEqual(rec['outputs']['file_count'], 1)
      self.assertEqual(len(rec['outputs']['dlq_files']), 1)
      self.assertTrue(rec['outputs']['verify']['match'])

  def test_package_yaml_zero_docker_and_stdin(self):
    yaml_path = os.path.join(self.tmp, 'orders_ingest.yaml')
    yaml_content = (
        'pipeline:\n'
        '  type: chain\n'
        '  transforms:\n'
        '    - type: ReadFromCsv\n'
        '      config:\n'
        '        path: "{{ input_csv }}"\n'
        '    - type: Filter\n'
        '      config:\n'
        '        language: generic\n'
        '        keep: "amount >= {{ min_amount | default(100) }}"\n'
        '    - type: WriteToParquet\n'
        '      config:\n'
        '        path: "{{ output_parquet }}"\n')
    with open(yaml_path, 'w', encoding='utf-8') as f:
      f.write(yaml_content)

    spec_out = os.path.join(self.tmp, 'orders.template.json')
    rc, env, _ = _run_json([
        'package',
        yaml_path,
        '--to',
        spec_out,
        '--name',
        'orders-ingest',
        '--image',
        'gcr.io/dataflow-templates/yaml-template:test',
        '--project',
        'demo-proj',
        '--region',
        'us-central1',
        '--subnetwork',
        'regions/us-central1/subnetworks/default',
        '--no-use-public-ips',
        '--temp-location',
        'gs://demo-bucket/tmp',
    ])
    self.assertEqual(rc, cli.EXIT_OK)
    out = env['outputs']
    self.assertEqual(out['sdk_language'], 'YAML')
    self.assertFalse(out['docker_build_required'])
    params = {p['name']: p for p in out['parameters']}
    self.assertEqual(list(params.keys()), ['input_csv', 'min_amount', 'output_parquet'])
    self.assertFalse(params['input_csv']['isOptional'])
    self.assertTrue(params['min_amount']['isOptional'])
    self.assertEqual(params['min_amount']['defaultValue'], '100')
    self.assertFalse(params['output_parquet']['isOptional'])

    with open(spec_out, encoding='utf-8') as f:
      spec = json.load(f)
    self.assertEqual(spec['sdkInfo']['language'], 'YAML')
    self.assertTrue(
        spec['metadata']['yamlDefinition'].startswith(yaml_content.rstrip('\n')))
    self.assertIn('requirements_file: null', spec['metadata']['yamlDefinition'])
    self.assertEqual(
        spec['defaultEnvironment']['ipConfiguration'], 'WORKER_IP_PRIVATE')
    self.assertIn('gcloud dataflow flex-template run', out['launch']['gcloud'])

    # Also verify `beam package -` reading YAML from stdin!
    stdin_spec = os.path.join(self.tmp, 'stdin.template.json')
    with mock.patch('sys.stdin', io.StringIO(yaml_content)):
      rc2, env2, _ = _run_json([
          'package',
          '-',
          '--to',
          stdin_spec,
          '--image',
          'gcr.io/dataflow-templates/yaml-template:test',
      ])
    self.assertEqual(rc2, cli.EXIT_OK)
    self.assertTrue(os.path.isfile(stdin_spec))
    self.assertEqual(env2['outputs']['sdk_language'], 'YAML')

  def test_package_python_ast_and_custom_provider_yaml_bundle(self):
    # 1. Custom Python pipeline (.py) -> AST extracts PipelineOptions without executing!
    py_file = os.path.join(self.tmp, 'custom_etl.py')
    with open(py_file, 'w', encoding='utf-8') as f:
      f.write(
          'import apache_beam as beam\n'
          'from apache_beam.options.pipeline_options import PipelineOptions\n'
          'raise RuntimeError("MUST NOT BE EXECUTED DURING PACKAGE")\n'
          'class CustomOptions(PipelineOptions):\n'
          '  @classmethod\n'
          '  def _add_argparse_args(cls, parser):\n'
          '    parser.add_argument("--input-table", required=True, help="Source table")\n'
          '    parser.add_value_provider_argument("--batch_size", default=500)\n')
    req_file = os.path.join(self.tmp, 'requirements.txt')
    with open(req_file, 'w', encoding='utf-8') as f:
      f.write('pyarrow>=14.0.0\n')
    bundle_dir = os.path.join(self.tmp, 'py_bundle')
    rc, env, _ = _run_json([
        'package',
        py_file,
        '--output-dir',
        bundle_dir,
        '--requirements',
        req_file,
        '--image',
        'us-central1-docker.pkg.dev/demo/df/custom-etl:v1',
    ])
    self.assertEqual(rc, cli.EXIT_OK)
    out = env['outputs']
    self.assertEqual(out['sdk_language'], 'PYTHON')
    self.assertTrue(out['docker_build_required'])
    params = {p['name']: p for p in out['parameters']}
    self.assertIn('input_table', params)
    self.assertFalse(params['input_table']['isOptional'])
    self.assertEqual(params['input_table']['helpText'], 'Source table')
    self.assertIn('batch_size', params)
    self.assertTrue(params['batch_size']['isOptional'])
    self.assertEqual(params['batch_size']['defaultValue'], '500')
    self.assertTrue(os.path.isfile(os.path.join(bundle_dir, 'Dockerfile')))
    self.assertTrue(os.path.isfile(os.path.join(bundle_dir, 'metadata.json')))
    self.assertTrue(os.path.isfile(os.path.join(bundle_dir, 'requirements.txt')))

    # 2. YAML with custom Python provider (`apache_beam.cli.dlq`) gates `--sdk=yaml`
    # and succeeds cleanly with `--sdk=python`!
    dlq_yaml = os.path.join(self.tmp, 'dlq_pipe.yaml')
    with open(dlq_yaml, 'w', encoding='utf-8') as f:
      f.write(
          'providers:\n'
          '  - type: python\n'
          '    config: {}\n'
          '    transforms:\n'
          '      ReadCsvWithDlq: "apache_beam.cli.dlq:ReadCsvWithDlq"\n'
          'pipeline:\n'
          '  type: chain\n'
          '  transforms:\n'
          '    - type: ReadCsvWithDlq\n'
          '      config:\n'
          '        path: "{{ src }}"\n')
    rc_gate, env_gate, _ = _run_json([
        'package', dlq_yaml, '--sdk', 'yaml', '--dry-run'
    ])
    self.assertEqual(rc_gate, cli.EXIT_USAGE)
    self.assertIn('--sdk=python', env_gate['error']['message'])

    dlq_bundle = os.path.join(self.tmp, 'dlq_bundle')
    rc_py, env_py, _ = _run_json([
        'package',
        dlq_yaml,
        '--sdk',
        'python',
        '--output-dir',
        dlq_bundle,
        '--image',
        'us-central1-docker.pkg.dev/demo/df/dlq-pipe:v1',
    ])
    self.assertEqual(rc_py, cli.EXIT_OK)
    self.assertEqual(env_py['outputs']['sdk_language'], 'PYTHON')
    ep_code = Path(dlq_bundle, 'flex_yaml_entrypoint.py').read_text(encoding='utf-8')
    self.assertIn("'src'", ep_code)
    self.assertIn('--jinja_variables=', ep_code)

  def test_tail_follow_live_file_and_pubsub_ephemeral_vs_nack(self):
    import threading
    import time as time_mod

    # 1. Live file `-f` (`--follow`) with NDJSON streaming & ANSI sanitization.
    live_jsonl = os.path.join(self.tmp, 'stream.jsonl')
    with open(live_jsonl, 'w', encoding='utf-8') as f:
      f.write('{"id": 0, "note": "existing_before_follow"}\n')

    def _writer():
      time_mod.sleep(0.15)
      with open(live_jsonl, 'a', encoding='utf-8') as wf:
        wf.write('{"id": 1, "msg": "hello \\u001b[31mRED\\u001b[0m"}\n')
        wf.flush()
      time_mod.sleep(0.1)
      with open(live_jsonl, 'a', encoding='utf-8') as wf:
        wf.write('{"id": 2, "msg": "second"}\n')
        wf.flush()

    t = threading.Thread(target=_writer, daemon=True)
    t.start()
    out_buf = io.StringIO()
    err_buf = io.StringIO()
    with contextlib.redirect_stdout(out_buf), contextlib.redirect_stderr(err_buf):
      rc = cli.main([
          'tail', '-f', '-n', '2', '--poll-interval', '0.05', '--duration', '5s',
          live_jsonl, '--json'
      ])
    t.join(timeout=2.0)
    self.assertEqual(rc, cli.EXIT_OK)
    ndjson_lines = [
        json.loads(ln) for ln in out_buf.getvalue().strip().splitlines() if ln.strip()
    ]
    # 2 live record lines + 1 final summary envelope!
    self.assertEqual(len(ndjson_lines), 3)
    self.assertEqual(ndjson_lines[0]['event'], 'record')
    self.assertEqual(ndjson_lines[0]['record']['id'], 1)
    self.assertNotIn('\x1b', ndjson_lines[0]['record']['msg'])
    self.assertEqual(ndjson_lines[1]['record']['id'], 2)
    self.assertTrue(ndjson_lines[2]['ok'])
    self.assertEqual(ndjson_lines[2]['outputs']['sampled_count'], 2)

    # 2. Pub/Sub `-f` on existing subscription: deduplicates redeliveries and NEVER acks!
    from types import SimpleNamespace
    fake_msg_1 = SimpleNamespace(
        ack_id='ack-1',
        message=SimpleNamespace(
            message_id='m-1',
            data=b'{"event":"click"}',
            publish_time=None,
            attributes={}))
    fake_msg_dup = SimpleNamespace(
        ack_id='ack-1b',
        message=SimpleNamespace(
            message_id='m-1',
            data=b'{"event":"click"}',
            publish_time=None,
            attributes={}))
    fake_msg_2 = SimpleNamespace(
        ack_id='ack-2',
        message=SimpleNamespace(
            message_id='m-2',
            data=b'{"event":"buy"}',
            publish_time=None,
            attributes={}))

    fake_sub_client = mock.MagicMock()
    fake_sub_client.pull.side_effect = [
        SimpleNamespace(received_messages=[fake_msg_1]),
        SimpleNamespace(received_messages=[fake_msg_dup, fake_msg_2]),
    ]
    with mock.patch(
        'google.cloud.pubsub_v1.SubscriberClient',
        return_value=fake_sub_client):
      from apache_beam.cli import tail as tail_mod
      streamed = []
      res = tail_mod.run_tail(
          'pubsub://projects/p/subscriptions/s',
          limit=2,
          duration='3s',
          follow=True,
          poll_interval=0.05,
          on_record=lambda r, i: streamed.append(r))
      self.assertEqual(res['sampled_count'], 2)
      self.assertEqual(len(streamed), 2)
      fake_sub_client.acknowledge.assert_not_called()
      self.assertTrue(fake_sub_client.modify_ack_deadline.called)

    # 3. Pub/Sub `-f` on topic: ACKs private ephemeral sub AND deletes it on Ctrl-C!
    fake_topic_sub = mock.MagicMock()
    fake_topic_sub.pull.side_effect = [
        SimpleNamespace(received_messages=[fake_msg_1]),
        KeyboardInterrupt(),
    ]
    fake_pub_client = mock.MagicMock()
    fake_pub_client.list_topic_subscriptions.return_value = []
    with (
        mock.patch(
            'google.cloud.pubsub_v1.SubscriberClient',
            return_value=fake_topic_sub),
        mock.patch(
            'google.cloud.pubsub_v1.PublisherClient',
            return_value=fake_pub_client),
    ):
      res_t = tail_mod.run_tail(
          'pubsub://projects/p/topics/t',
          limit=None,
          duration=None,
          follow=True,
          poll_interval=0.05)
      self.assertTrue(res_t['interrupted'])
      self.assertEqual(res_t['sampled_count'], 1)
      fake_topic_sub.acknowledge.assert_called_once()
      fake_topic_sub.delete_subscription.assert_called_once()

  def test_delta_and_iceberg_zero_copy_inspect_cost_guard_and_diff(self):
    import pyarrow as pa
    import pyarrow.parquet as pq

    delta_dir = os.path.join(self.tmp, 'delta_orders')
    log_dir = os.path.join(delta_dir, '_delta_log')
    os.makedirs(log_dir, exist_ok=True)

    # Write 2 Parquet files in commit 0, then replace part-0 with part-2 in commit 1!
    p0 = os.path.join(delta_dir, 'part-0.parquet')
    p1 = os.path.join(delta_dir, 'part-1.parquet')
    p2 = os.path.join(delta_dir, 'part-2.parquet')
    pq.write_table(
        pa.table({
            'id': pa.array([1, 2], type=pa.int64()),
            'customer': pa.array(['Ada', 'Grace'], type=pa.string()),
            'amount': pa.array([10.5, 20.0], type=pa.float64()),
        }),
        p0)
    pq.write_table(
        pa.table({
            'id': pa.array([3], type=pa.int64()),
            'customer': pa.array(['Linus'], type=pa.string()),
            'amount': pa.array([30.25], type=pa.float64()),
        }),
        p1)
    pq.write_table(
        pa.table({
            'id': pa.array([1, 2], type=pa.int64()),
            'customer': pa.array(['Ada', 'Grace'], type=pa.string()),
            'amount': pa.array([15.0, 25.5], type=pa.float64()),
        }),
        p2)

    schema_str = json.dumps({
        'type': 'struct',
        'fields': [
            {'name': 'id', 'type': 'long', 'nullable': False, 'metadata': {}},
            {'name': 'customer', 'type': 'string', 'nullable': True, 'metadata': {}},
            {'name': 'amount', 'type': 'double', 'nullable': True, 'metadata': {}},
        ],
    })
    with open(os.path.join(log_dir, '00000000000000000000.json'), 'w', encoding='utf-8') as f:
      f.write(json.dumps({'protocol': {'minReaderVersion': 1, 'minWriterVersion': 2}}) + '\n')
      f.write(json.dumps({
          'metaData': {
              'id': 'delta-tbl-1',
              'schemaString': schema_str,
              'partitionColumns': [],
              'configuration': {},
          }
      }) + '\n')
      f.write(json.dumps({
          'add': {
              'path': 'part-0.parquet',
              'size': os.path.getsize(p0),
              'modificationTime': 1700000000000,
              'dataChange': True,
              'stats': json.dumps({'numRecords': 2}),
          }
      }) + '\n')
      f.write(json.dumps({
          'add': {
              'path': 'part-1.parquet',
              'size': os.path.getsize(p1),
              'modificationTime': 1700000000000,
              'dataChange': True,
              'stats': json.dumps({'numRecords': 1}),
          }
      }) + '\n')

    with open(os.path.join(log_dir, '00000000000000000001.json'), 'w', encoding='utf-8') as f:
      f.write(json.dumps({
          'remove': {
              'path': 'part-0.parquet',
              'deletionTimestamp': 1700000001000,
              'dataChange': True,
          }
      }) + '\n')
      f.write(json.dumps({
          'add': {
              'path': 'part-2.parquet',
              'size': os.path.getsize(p2),
              'modificationTime': 1700000001000,
              'dataChange': True,
              'stats': json.dumps({'numRecords': 2}),
          }
      }) + '\n')

    delta_uri = f'delta://{delta_dir}'

    # 1. Inspect latest Delta version (1) and time-travel version (0).
    rc_i, env_i, _ = _run_json(['inspect', delta_uri])
    self.assertEqual(rc_i, cli.EXIT_OK)
    self.assertEqual(env_i['outputs']['kind'], 'delta')
    self.assertEqual(env_i['outputs']['snapshot_version'], 1)
    self.assertEqual(env_i['outputs']['row_count'], 3)
    self.assertEqual(env_i['outputs']['file_count'], 2)
    rows_v1 = sorted(env_i['untrusted_data']['rows'], key=lambda r: r['id'])
    self.assertEqual([r['amount'] for r in rows_v1], [15.0, 25.5, 30.25])

    rc_v0, env_v0, _ = _run_json(['inspect', f'{delta_uri}?version=0'])
    self.assertEqual(rc_v0, cli.EXIT_OK)
    rows_v0 = sorted(env_v0['untrusted_data']['rows'], key=lambda r: r['id'])
    self.assertEqual([r['amount'] for r in rows_v0], [10.5, 20.0, 30.25])

    # 2. Cost guard `--max-bytes 10B` blocks before reading/copying (`EXIT_COST_LIMIT = 8`).
    wh_dir = os.path.join(self.tmp, 'iceberg_wh')
    iceberg_uri = f'iceberg://lake.orders_zc?warehouse={wh_dir}'
    rc_cg, env_cg, _ = _run_json([
        'pipe', '--from', delta_uri, '--to', iceberg_uri, '--zero-copy',
        '--max-bytes', '10B'
    ])
    self.assertEqual(rc_cg, cli.EXIT_COST_LIMIT)
    self.assertGreater(env_cg['estimated_bytes'], 10)

    # 3. Zero-copy Delta -> Iceberg v2 registration + `--verify`!
    rc_zc, env_zc, _ = _run_json([
        'pipe', '--from', delta_uri, '--to', iceberg_uri, '--zero-copy',
        '--verify', '--verify-key', 'id'
    ])
    self.assertEqual(rc_zc, cli.EXIT_OK)
    self.assertEqual(env_zc['runner'], 'zero_copy_metadata')
    self.assertEqual(env_zc['outputs']['bytes_copied'], 0)
    self.assertEqual(env_zc['outputs']['rows_registered'], 3)
    self.assertTrue(env_zc['outputs']['verify']['match'])
    with open(env_zc['outputs']['metadata_path'], encoding='utf-8') as mf:
      meta_json = json.load(mf)
    self.assertEqual(meta_json['format-version'], 2)
    self.assertIn('schema.name-mapping.default', meta_json['properties'])

    # 4. Inspect & diff the resulting Iceberg v2 table directly!
    rc_ii, env_ii, _ = _run_json(['inspect', iceberg_uri])
    self.assertEqual(rc_ii, cli.EXIT_OK)
    self.assertEqual(env_ii['outputs']['kind'], 'iceberg')
    self.assertEqual(env_ii['outputs']['row_count'], 3)

    rc_df, env_df, _ = _run_json(['diff', delta_uri, iceberg_uri, '--key', 'id'])
    self.assertEqual(rc_df, cli.EXIT_OK)
    self.assertTrue(env_df['outputs']['match'])

  def test_cross_engine_cdc_debezium_dlq_and_iceberg_merge_on_read(self):
    cdc_jsonl = os.path.join(self.tmp, 'debezium_orders.jsonl')
    events = [
        json.dumps({
            'op': 'c',
            'ts_ms': 1001,
            'source': {'lsn': 101},
            'after': {'id': 1, 'customer': 'Ada', 'amount': 10.5},
        }),
        json.dumps({
            'op': 'c',
            'ts_ms': 1002,
            'source': {'lsn': 102},
            'after': {'id': 2, 'customer': 'Grace', 'amount': 20.0},
        }),
        json.dumps({
            'op': 'c',
            'ts_ms': 1003,
            'source': {'lsn': 103},
            'after': {'id': 3, 'customer': 'Linus', 'amount': 30.0},
        }),
        '{corrupt_json_poison_pill',
        json.dumps({
            'op': 'c',
            'ts_ms': 1004,
            'after': {'id': None, 'customer': 'MissingPK', 'amount': 5.0},
        }),
        json.dumps({
            'op': 'u',
            'ts_ms': 1005,
            'source': {'lsn': 105},
            'before': {'id': 2, 'customer': 'Grace', 'amount': 20.0},
            'after': {'id': 2, 'customer': 'Grace', 'amount': 99.5},
        }),
        json.dumps({
            'op': 'd',
            'ts_ms': 1006,
            'source': {'lsn': 106},
            'before': {'id': 3, 'customer': 'Linus', 'amount': 30.0},
            'after': None,
        }),
    ]
    with open(cdc_jsonl, 'w', encoding='utf-8') as f:
      f.write('\n'.join(events) + '\n')

    wh_dir = os.path.join(self.tmp, 'wh_cdc')
    iceberg_cdc_uri = f'iceberg://lake.orders_cdc?warehouse={wh_dir}'
    dlq_path = os.path.join(self.tmp, 'cdc_dlq.json')

    rc, env, err = _run_json([
        'pipe',
        '--from',
        cdc_jsonl,
        '--to',
        iceberg_cdc_uri,
        '--cdc',
        '--primary-key',
        'id',
        '--dlq',
        dlq_path,
        '--assert-counts',
        '--runner=FnApiRunner',
    ])
    self.assertEqual(rc, cli.EXIT_OK, f'err={err} env={env}')
    self.assertEqual(env['metrics']['rows_read'], 7)
    self.assertEqual(env['metrics']['dlq_rows'], 2)
    self.assertEqual(env['metrics']['rows_written'], 5)

    # Verify Iceberg v2 Merge-on-Read state: 2 live rows (id=1 @ 10.5, id=2 @ 99.5; id=3 deleted!).
    rc_insp, env_insp, _ = _run_json(['inspect', iceberg_cdc_uri])
    self.assertEqual(rc_insp, cli.EXIT_OK)
    self.assertEqual(env_insp['outputs']['row_count'], 2)
    self.assertGreaterEqual(env_insp['outputs']['delete_file_count'], 1)
    self.assertEqual(env_insp['outputs']['identifier_columns'], ['id'])
    live_rows = sorted(env_insp['untrusted_data']['rows'], key=lambda r: r['id'])
    self.assertEqual(
        live_rows,
        [
            {'id': 1, 'customer': 'Ada', 'amount': 10.5},
            {'id': 2, 'customer': 'Grace', 'amount': 99.5},
        ])

    # Reconcile against golden expected JSONL via `beam diff`!
    expected_jsonl = os.path.join(self.tmp, 'expected_post_cdc.jsonl')
    with open(expected_jsonl, 'w', encoding='utf-8') as f:
      f.write(json.dumps({'id': 1, 'customer': 'Ada', 'amount': 10.5}) + '\n')
      f.write(json.dumps({'id': 2, 'customer': 'Grace', 'amount': 99.5}) + '\n')
    rc_d, env_d, _ = _run_json([
        'diff', iceberg_cdc_uri, expected_jsonl, '--key', 'id'
    ])
    self.assertEqual(rc_d, cli.EXIT_OK)
    self.assertTrue(env_d['outputs']['match'])

    # Append a second CDC batch (delete id=1, insert id=4) with `--if-exists=append`!
    cdc_batch_2 = os.path.join(self.tmp, 'debezium_orders_2.jsonl')
    with open(cdc_batch_2, 'w', encoding='utf-8') as f:
      f.write(json.dumps({
          'op': 'd',
          'ts_ms': 2001,
          'before': {'id': 1, 'customer': 'Ada', 'amount': 10.5},
          'after': None,
      }) + '\n')
      f.write(json.dumps({
          'op': 'c',
          'ts_ms': 2002,
          'after': {'id': 4, 'customer': 'Margaret', 'amount': 42.0},
      }) + '\n')
    rc2, env2, err2 = _run_json([
        'pipe',
        '--from',
        cdc_batch_2,
        '--to',
        iceberg_cdc_uri,
        '--cdc',
        '--primary-key',
        'id',
        '--if-exists=append',
        '--assert-counts',
        '--runner=FnApiRunner',
    ])
    self.assertEqual(rc2, cli.EXIT_OK, f'err={err2} env={env2}')
    rc_i2, env_i2, _ = _run_json(['inspect', iceberg_cdc_uri])
    self.assertEqual(rc_i2, cli.EXIT_OK)
    live_rows_2 = sorted(env_i2['untrusted_data']['rows'], key=lambda r: r['id'])
    self.assertEqual(
        live_rows_2,
        [
            {'id': 2, 'customer': 'Grace', 'amount': 99.5},
            {'id': 4, 'customer': 'Margaret', 'amount': 42.0},
        ])

  def test_delta_cdf_to_iceberg_cdc_and_live_lakehouse_tail_follow(self):
    import threading
    import time as time_mod
    import pyarrow as pa
    import pyarrow.parquet as pq
    from apache_beam.cli import lakehouse

    # 1. Delta Change Data Feed (`cdc` action with `insert`, `update_preimage`,
    #    `update_postimage`, `delete`) -> Iceberg v2 Merge-on-Read!
    cdf_root = os.path.join(self.tmp, 'delta_cdf_tbl')
    cdf_log = os.path.join(cdf_root, '_delta_log')
    os.makedirs(cdf_log, exist_ok=True)
    cdf_file = os.path.join(cdf_root, 'cdf-0.parquet')
    pq.write_table(
        pa.table({
            'id': pa.array([10, 20, 20, 30, 30], type=pa.int64()),
            'name': pa.array(['A', 'B_old', 'B_new', 'C', 'C'], type=pa.string()),
            '_change_type': pa.array(
                ['insert', 'update_preimage', 'update_postimage', 'insert', 'delete'],
                type=pa.string()),
            '_commit_version': pa.array([1, 2, 2, 1, 3], type=pa.int64()),
        }),
        cdf_file)
    schema_str = json.dumps({
        'type': 'struct',
        'fields': [
            {'name': 'id', 'type': 'long', 'nullable': False, 'metadata': {}},
            {'name': 'name', 'type': 'string', 'nullable': True, 'metadata': {}},
        ],
    })
    with open(os.path.join(cdf_log, '00000000000000000000.json'), 'w', encoding='utf-8') as f:
      f.write(json.dumps({
          'metaData': {
              'id': 'cdf-1',
              'schemaString': schema_str,
              'partitionColumns': [],
              'configuration': {'delta.enableChangeDataFeed': 'true'},
          }
      }) + '\n')
      f.write(json.dumps({
          'cdc': {
              'path': 'cdf-0.parquet',
              'size': os.path.getsize(cdf_file),
              'dataChange': False,
          }
      }) + '\n')

    wh_dir = os.path.join(self.tmp, 'wh_from_cdf')
    ice_uri = f'iceberg://lake.from_cdf?warehouse={wh_dir}'
    rc, env, err = _run_json([
        'pipe',
        '--from',
        f'delta://{cdf_root}',
        '--to',
        ice_uri,
        '--cdc',
        '--primary-key',
        'id',
        '--assert-counts',
        '--runner=FnApiRunner',
    ])
    self.assertEqual(rc, cli.EXIT_OK, f'err={err} env={env}')
    # 5 raw CDF rows -> 1 `update_preimage` filtered out -> 4 CDC ops written!
    self.assertEqual(env['metrics']['rows_read'], 5)
    self.assertEqual(env['metrics']['rows_filtered_out'], 1)
    self.assertEqual(env['metrics']['rows_written'], 4)

    rc_i, env_i, _ = _run_json(['inspect', ice_uri])
    self.assertEqual(rc_i, cli.EXIT_OK)
    rows_live = sorted(env_i['untrusted_data']['rows'], key=lambda r: r['id'])
    self.assertEqual(rows_live, [{'id': 10, 'name': 'A'}, {'id': 20, 'name': 'B_new'}])

    # 2. Live `beam tail -f` following new Iceberg v2 snapshot commits!
    tdir = os.path.join(wh_dir, 'lake', 'from_cdf')

    def _commit_later():
      time_mod.sleep(0.15)
      new_pq = os.path.join(tdir, 'data', 'live-append.parquet')
      df_entry = lakehouse.write_parquet_with_field_ids(
          new_pq,
          [{'id': 50, 'name': 'LiveCommit'}],
          [
              {'id': 1, 'name': 'id', 'type': 'long', 'nullable': False},
              {'id': 2, 'name': 'name', 'type': 'string', 'nullable': True},
          ])
      lakehouse.commit_iceberg_snapshot(
          tdir,
          iceberg_fields=[
              {'id': 1, 'name': 'id', 'type': 'long', 'nullable': False},
              {'id': 2, 'name': 'name', 'type': 'string', 'nullable': True},
          ],
          identifier_columns=['id'],
          new_data_files=[df_entry])

    t = threading.Thread(target=_commit_later, daemon=True)
    t.start()
    out_buf = io.StringIO()
    err_buf = io.StringIO()
    with contextlib.redirect_stdout(out_buf), contextlib.redirect_stderr(err_buf):
      rc_tail = cli.main([
          'tail', '-f', '-n', '1', '--poll-interval', '0.05', '--duration', '5s',
          ice_uri, '--json'
      ])
    t.join(timeout=2.0)
    self.assertEqual(rc_tail, cli.EXIT_OK)
    lines = [json.loads(ln) for ln in out_buf.getvalue().strip().splitlines() if ln.strip()]
    self.assertEqual(len(lines), 2)
    self.assertEqual(lines[0]['event'], 'record')
    self.assertEqual(lines[0]['record']['id'], 50)
    self.assertEqual(lines[0]['record']['name'], 'LiveCommit')
    self.assertEqual(lines[1]['outputs']['mode'], 'lakehouse_commit_follow')

  def test_bigquery_cdc_sequence_normalization_and_storage_write_sink(self):
    import re
    from types import SimpleNamespace
    from apache_beam.cli import cdc as cdc_mod

    bq_seq_re = re.compile(r'^[0-9A-Fa-f]{1,16}(?:/[0-9A-Fa-f]{1,16}){0,3}$')
    _, hex_1 = cdc_mod.to_bq_hex_sequence(1)
    _, hex_2 = cdc_mod.to_bq_hex_sequence(2)
    _, hex_100 = cdc_mod.to_bq_hex_sequence(100)
    self.assertRegex(hex_1, bq_seq_re)
    self.assertLess(hex_1, hex_2)
    self.assertLess(hex_2, hex_100)

    # Postgres WAL LSNs (`0/16B3748` < `0/16B3750` < `1/0000001`).
    _, lsn_a = cdc_mod.to_bq_hex_sequence('0/16B3748')
    _, lsn_b = cdc_mod.to_bq_hex_sequence('0/16B3750')
    _, lsn_c = cdc_mod.to_bq_hex_sequence('1/0000001')
    for lsn in (lsn_a, lsn_b, lsn_c):
      self.assertRegex(lsn, bq_seq_re)
    self.assertLess(lsn_a, lsn_b)
    self.assertLess(lsn_b, lsn_c)

    # ISO-8601 timestamps preserve lexicographical order.
    _, ts_a = cdc_mod.to_bq_hex_sequence('2026-11-08T10:00:00Z')
    _, ts_b = cdc_mod.to_bq_hex_sequence('2026-11-08T10:00:01Z')
    self.assertRegex(ts_a, bq_seq_re)
    self.assertLess(ts_a, ts_b)

    # Mock BigQuery DDL client + BigQueryWriteClient (`append_rows`) + row_errors DLQ!
    cdc_file = os.path.join(self.tmp, 'bq_cdc_in.jsonl')
    with open(cdc_file, 'w', encoding='utf-8') as f:
      f.write(json.dumps({
          'op': 'c',
          'source': {'lsn': '0/16B3748'},
          'after': {'id': 1, 'email': 'ada@example.com'},
      }) + '\n')
      f.write(json.dumps({
          'op': 'd',
          'source': {'lsn': '0/16B3750'},
          'before': {'id': 2, 'email': 'old@example.com'},
          'after': None,
      }) + '\n')
      f.write(json.dumps({
          'op': 'u',
          'source': {'lsn': '0/16B3760'},
          'after': {'id': 999, 'email': 'reject_me@example.com'},
      }) + '\n')

    from google.api_core import exceptions as gcp_exceptions
    fake_bq_client = mock.MagicMock()
    fake_bq_client.project = 'demo-proj'
    fake_bq_client.get_table.side_effect = gcp_exceptions.NotFound('no table')

    captured_requests: list[Any] = []

    def _fake_append_rows(req_iter):
      reqs = list(req_iter)
      captured_requests.extend(reqs)
      # Simulate BigQuery rejecting `id == 999` (`reject_me@example.com`) via `row_errors`!
      r_errs = []
      for idx, ser_row in enumerate(reqs[0].proto_rows.rows.serialized_rows):
        if b'reject_me@example.com' in ser_row:
          r_errs.append(
              SimpleNamespace(
                  index=idx, message='simulated row constraint error'))
      return iter([
          SimpleNamespace(
              row_errors=r_errs, error=SimpleNamespace(code=0, message=''))
      ])

    fake_write_client = mock.MagicMock()
    fake_write_client.append_rows.side_effect = _fake_append_rows
    bq_dlq = os.path.join(self.tmp, 'bq_cdc_dlq.json')

    with (
        mock.patch('google.cloud.bigquery.Client', return_value=fake_bq_client),
        mock.patch(
            'google.cloud.bigquery_storage_v1.BigQueryWriteClient',
            return_value=fake_write_client),
    ):
      rc, env, err = _run_json([
          'pipe',
          '--from',
          cdc_file,
          '--to',
          'bigquery://demo-proj.analytics.users_cdc',
          '--cdc',
          '--primary-key',
          'id',
          '--dlq',
          bq_dlq,
          '--assert-counts',
          '--runner=FnApiRunner',
      ])
    self.assertEqual(rc, cli.EXIT_OK, f'err={err} env={env}')
    ddl_sql = fake_bq_client.query.call_args.args[0]
    self.assertIn('PRIMARY KEY (`id`) NOT ENFORCED', ddl_sql)
    self.assertGreaterEqual(len(captured_requests), 1)
    self.assertEqual(
        captured_requests[0].write_stream,
        'projects/demo-proj/datasets/analytics/tables/users_cdc/streams/_default')
    self.assertEqual(env['metrics']['rows_read'], 3)
    self.assertEqual(env['metrics']['dlq_rows'], 1)
    self.assertEqual(env['metrics']['rows_written'], 2)

  def test_cdc_and_zero_copy_emit_yaml_package_and_worker_wheel(self):
    # 1. Postgres `--cdc` -> BigQuery `--cdc` portable YAML export (`--emit-yaml`),
    # including when a local JDK is present (`_find_java` succeeds).
    with mock.patch('apache_beam.cli.sql._find_java', return_value='/usr/bin/java'):
      rc_pg, env_pg, err_pg = _run_json([
          'pipe',
          '--from',
          'postgres://db.internal:5432/shop?table=public.orders',
          '--to',
          'bigquery://demo-proj.analytics.orders',
          '--cdc',
          '--primary-key',
          'id',
          '--emit-yaml',
      ])
    self.assertEqual(rc_pg, cli.EXIT_OK, err_pg)
    pg_yaml = env_pg['outputs']['yaml']
    self.assertIn('ReadFromDebezium', pg_yaml)
    self.assertIn('WriteToBigQueryCDC', pg_yaml)

    # Package exported cross-engine CDC YAML as a zero-Docker Dataflow Flex Template!
    cdc_tpl = os.path.join(self.tmp, 'cdc_pg_to_bq.template.json')
    with mock.patch('sys.stdin', io.StringIO(pg_yaml)):
      rc_pkg, env_pkg, err_pkg = _run_json([
          'package',
          '-',
          '--to',
          cdc_tpl,
          '--image',
          'gcr.io/dataflow-templates/yaml-template:test',
      ])
    self.assertEqual(rc_pkg, cli.EXIT_OK, err_pkg)
    self.assertFalse(env_pkg['outputs']['docker_build_required'])

    # 2. Spanner Change Stream -> Iceberg Merge-on-Read `--emit-yaml`.
    rc_sp, env_sp, err_sp = _run_json([
        'pipe',
        '--from',
        'spanner://demo-proj/prod-inst/shop-db?change_stream=orders_cs',
        '--to',
        'iceberg://lake.orders?warehouse=gs://demo-bkt/wh',
        '--cdc',
        '--primary-key',
        'id',
        '--emit-yaml',
    ])
    self.assertEqual(rc_sp, cli.EXIT_OK, err_sp)
    sp_yaml = env_sp['outputs']['yaml']
    self.assertIn('ReadFromSpannerCDC', sp_yaml)
    self.assertIn('WriteToIceberg', sp_yaml)
    self.assertIn('merge-on-read', sp_yaml)

    # 3. Parquet -> Iceberg `--zero-copy --emit-yaml` emits `IcebergAddFiles`.
    rc_zc, env_zc, err_zc = _run_json([
        'pipe',
        '--from',
        'gs://demo-bkt/raw/*.parquet',
        '--to',
        'iceberg://lake.orders_zc?warehouse=gs://demo-bkt/wh',
        '--zero-copy',
        '--emit-yaml',
    ])
    self.assertEqual(rc_zc, cli.EXIT_OK, err_zc)
    self.assertIn('IcebergAddFiles', env_zc['outputs']['yaml'])

    # 4. Verify `beam io list --json` and preview worker wheel bundling (`lakehouse.py`, `cdc.py`).
    rc_io, env_io, _ = _run_json(['io', 'list'])
    self.assertEqual(rc_io, cli.EXIT_OK)
    conn_names = {c['name'] for c in env_io['outputs']['connectors']}
    self.assertTrue({'delta', 'iceberg', 'mysql', 'spanner', 'bigquery'}.issubset(conn_names))

    import zipfile
    preview_launcher = sys.modules.get('apache_beam_cli_preview.launcher')
    if preview_launcher is not None:
      whl_path = preview_launcher.ensure_preview_wheel()
      with zipfile.ZipFile(whl_path, 'r') as zf:
        names = set(zf.namelist())
      self.assertTrue(any(n.endswith('_bundled_cli/lakehouse.py') for n in names))
      self.assertTrue(any(n.endswith('_bundled_cli/cdc.py') for n in names))
      self.assertTrue(any(n.endswith('_bundled_cli/pipe_sql.py') for n in names))

  def test_doctor_without_google_namespace_package(self):
    import importlib.util
    orig_find_spec = importlib.util.find_spec

    def _no_google_find_spec(name, *args, **kwargs):
      if name == 'google' or name.startswith('google.'):
        raise ModuleNotFoundError("No module named 'google'")
      return orig_find_spec(name, *args, **kwargs)

    with mock.patch('importlib.util.find_spec', side_effect=_no_google_find_spec):
      rc, env, err = _run_json(['doctor'])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertTrue(env['outputs']['healthy'])
    self.assertFalse(env['outputs']['extras']['gcp_bigquery'])
    self.assertFalse(env['outputs']['gcp_adc']['ok'])

  def test_keyword_column_csv_sql_syntax_and_exact_local_parquet(self):
    import pyarrow as pa
    import pyarrow.parquet as pq

    raw_csv = os.path.join(self.tmp, 'orders_raw.csv')
    with open(raw_csv, 'w', encoding='utf-8') as f:
      f.write('order_id,customer_id,country,status,class,amount,email\n')
      f.write('101,C1,US,PAID,enterprise,250.5,Alice@Example.com\n')
      f.write('102,C2,CA,PAID,starter,80.0,Bob@Example.com\n')
      f.write('103,C99,US,PAID,pro,120.25,Unmapped@Example.com\n')
      f.write('104,C1,US,REFUNDED,enterprise,50.0,Alice@Example.com\n')
      f.write('105,C3,US,PAID,starter,-10.0,Neg@Example.com\n')
      f.write('106,C3,US,PAID,starter,not_a_float,BadNum@Example.com\n')
      f.write('107,BROKEN_FEW_COLS\n')

    clean_pq = os.path.join(self.tmp, 'out', 'clean_orders.parquet')
    dlq_jsonl = os.path.join(self.tmp, 'out', 'orders_dlq.jsonl')

    rc, env, err = _run_json([
        'pipe',
        '--from',
        raw_csv,
        '--to',
        clean_pq,
        '--dlq',
        dlq_jsonl,
        '--where',
        "status = 'PAID' AND amount > 0 AND country <> 'CA'",
        '--select',
        (
            'CAST(order_id AS INT) AS order_id, customer_id, country, class, '
            'round(amount, 2) AS amount, lower(email) AS email'
        ),
        '--assert-counts',
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(env['outputs']['files'], [clean_pq])
    self.assertEqual(env['outputs']['dlq_files'], [dlq_jsonl])
    self.assertTrue(os.path.isfile(clean_pq))
    self.assertTrue(os.path.isfile(dlq_jsonl))

    tbl = pq.read_table(clean_pq)
    self.assertTrue(pa.types.is_integer(tbl.schema.field('order_id').type))
    rows = sorted(tbl.to_pylist(), key=lambda r: r['order_id'])
    self.assertEqual(
        rows,
        [
            {
                'order_id': 101,
                'customer_id': 'C1',
                'country': 'US',
                'class': 'enterprise',
                'amount': 250.5,
                'email': 'alice@example.com',
            },
            {
                'order_id': 103,
                'customer_id': 'C99',
                'country': 'US',
                'class': 'pro',
                'amount': 120.25,
                'email': 'unmapped@example.com',
            },
        ])
    m = env['metrics']
    self.assertEqual(m['rows_read'], 7)
    self.assertEqual(m['rows_written'], 2)
    self.assertEqual(m['dlq_rows'], 2)
    self.assertEqual(m['rows_filtered_out'], 3)

  def test_iceberg_path_style_absolute_uri_matches_warehouse_query(self):
    import pyarrow as pa
    import pyarrow.parquet as pq

    delta_dir = os.path.join(self.tmp, 'delta_events')
    os.makedirs(os.path.join(delta_dir, '_delta_log'), exist_ok=True)
    pq_file = os.path.join(delta_dir, 'part-00000.parquet')
    pq.write_table(
        pa.Table.from_pylist([
            {'event_id': 1, 'kind': 'click'},
            {'event_id': 2, 'kind': 'purchase'},
        ]),
        pq_file)
    commit_0 = [
        {
            'metaData': {
                'id': 'evt-1',
                'schemaString': json.dumps({
                    'type': 'struct',
                    'fields': [
                        {'name': 'event_id', 'type': 'long', 'nullable': True},
                        {'name': 'kind', 'type': 'string', 'nullable': True},
                    ],
                }),
            }
        },
        {
            'add': {
                'path': 'part-00000.parquet',
                'size': os.path.getsize(pq_file),
                'modificationTime': 1700000000000,
                'dataChange': True,
            }
        },
    ]
    with open(
        os.path.join(delta_dir, '_delta_log', '00000000000000000000.json'),
        'w',
        encoding='utf-8') as f:
      for entry in commit_0:
        f.write(json.dumps(entry) + '\n')

    wh_dir = os.path.join(self.tmp, 'iceberg_wh')
    path_style_uri = f'iceberg://{wh_dir}/analytics.events'
    query_style_uri = f'iceberg://analytics.events?warehouse={wh_dir}'

    rc, env, err = _run_json([
        'pipe',
        '--from',
        f'delta://{delta_dir}',
        '--to',
        path_style_uri,
        '--zero-copy',
        '--verify',
        '--verify-key',
        'event_id',
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    expected_meta = os.path.join(
        wh_dir, 'analytics', 'events', 'metadata', 'v1.metadata.json')
    self.assertTrue(os.path.isfile(expected_meta))
    self.assertEqual(env['outputs']['metadata_path'], expected_meta)

    # Both path-style and `?warehouse=` URIs resolve the exact same Iceberg table!
    rc_i1, env_i1, err_i1 = _run_json(['inspect', query_style_uri])
    self.assertEqual(rc_i1, cli.EXIT_OK, err_i1)
    self.assertEqual(env_i1['outputs']['row_count'], 2)

    rc_d, env_d, err_d = _run_json(
        ['diff', path_style_uri, query_style_uri, '--key', 'event_id'])
    self.assertEqual(rc_d, cli.EXIT_OK, err_d)
    self.assertTrue(env_d['outputs']['match'])

  def test_pipe_multi_input_sql_left_join_group_by_and_authorizer_sandbox(self):
    import pyarrow as pa
    import pyarrow.parquet as pq

    orders_pq = os.path.join(self.tmp, 'clean_orders.parquet')
    pq.write_table(
        pa.Table.from_pylist([
            {'order_id': 1, 'customer_id': 'C1', 'amount': 100.0},
            {'order_id': 2, 'customer_id': 'C1', 'amount': 50.25},
            {'order_id': 3, 'customer_id': 'C2', 'amount': 40.0},
            {'order_id': 4, 'customer_id': 'C_MISSING', 'amount': 25.5},
        ]),
        orders_pq)
    customers_csv = os.path.join(self.tmp, 'customers.csv')
    with open(customers_csv, 'w', encoding='utf-8') as f:
      f.write('customer_id,tier\nC1,GOLD\nC2,SILVER\n')

    out_json = os.path.join(self.tmp, 'out', 'revenue_by_tier.json')
    sql_q = (
        "SELECT COALESCE(c.tier, 'UNMAPPED') AS tier, "
        'COUNT(*) AS order_count, '
        'ROUND(SUM(o.amount), 2) AS total_revenue '
        'FROM o LEFT JOIN c ON o.customer_id = c.customer_id '
        'GROUP BY 1 ORDER BY total_revenue DESC'
    )

    rc, env, err = _run_json([
        'pipe',
        '--from',
        f'o={orders_pq}',
        '--join',
        f'c={customers_csv}',
        '--to',
        out_json,
        '--sql',
        sql_q,
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(env['runner'], 'local_sql')
    self.assertEqual(env['outputs']['files'], [out_json])
    self.assertTrue(os.path.isfile(out_json))
    expected_agg = [
        {'tier': 'GOLD', 'order_count': 2, 'total_revenue': 150.25},
        {'tier': 'SILVER', 'order_count': 1, 'total_revenue': 40.0},
        {'tier': 'UNMAPPED', 'order_count': 1, 'total_revenue': 25.5},
    ]
    self.assertIsNone(env.get('untrusted_data'))
    with open(out_json, encoding='utf-8') as f:
      written_rows = [json.loads(line) for line in f if line.strip()]
    self.assertEqual(written_rows, expected_agg)

    # Repeatable `--from` (without `--join`) + `--preview` emits inline preview!
    rc_p, env_p, err_p = _run_json([
        'pipe',
        '--from',
        f'o={orders_pq}',
        '--from',
        f'c={customers_csv}',
        '--to',
        out_json,
        '--if-exists',
        'replace',
        '--preview',
        '--sql',
        sql_q,
    ])
    self.assertEqual(rc_p, cli.EXIT_OK, err_p)
    self.assertEqual(env_p['untrusted_data']['rows'], expected_agg)

    # Query-only mode (`--sql` without `--to`) returns rows directly in JSON!
    rc_q, env_q, err_q = _run_json([
        'pipe',
        '--from',
        f'o={orders_pq}',
        '--sql',
        'SELECT COUNT(*) AS n FROM o',
    ])
    self.assertEqual(rc_q, cli.EXIT_OK, err_q)
    self.assertEqual(env_q['untrusted_data']['rows'], [{'n': 4}])

    # Portable `--emit-yaml` compiles `type: Sql` with named inputs.
    rc_y, env_y, err_y = _run_json([
        'pipe',
        '--from',
        f'o={orders_pq}',
        '--join',
        f'c={customers_csv}',
        '--to',
        out_json,
        '--sql',
        sql_q,
        '--emit-yaml',
    ])
    self.assertEqual(rc_y, cli.EXIT_OK, err_y)
    self.assertIn('type: Sql', env_y['outputs']['yaml'])
    self.assertIn('Read_o', env_y['outputs']['yaml'])
    self.assertIn('Read_c', env_y['outputs']['yaml'])

    # SQLite authorizer & read-only guardrails reject ATTACH, PRAGMA, and DML!
    for bad_sql in (
        "ATTACH DATABASE ':memory:' AS evil",
        'PRAGMA database_list',
        'DELETE FROM o',
        'SELECT 1; DROP TABLE o',
    ):
      rc_bad, _, _ = _run_json([
          'pipe', '--from', f'o={orders_pq}', '--sql', bad_sql
      ])
      self.assertEqual(rc_bad, cli.EXIT_USAGE, f'expected EXIT_USAGE for {bad_sql}')

  def test_offline_emit_yaml_succeeds_before_local_source_exists(self):
    missing_csv = os.path.join(self.tmp, 'not_created_yet', 'raw.csv')
    future_pq = os.path.join(self.tmp, 'out', 'clean.parquet')
    rc, env, err = _run_json([
        'pipe',
        '--from',
        missing_csv,
        '--to',
        future_pq,
        '--where',
        'amount > 0',
        '--emit-yaml',
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertIn('ReadFromCsv', env['outputs']['yaml'])
    self.assertIn('WriteToParquet', env['outputs']['yaml'])
    self.assertTrue(
        any('do not exist yet' in w for w in env.get('warnings', [])))

  def test_emit_yaml_with_jinja_placeholders_and_package_roundtrip(self):
    # Exact Round 2 simulation command: Jinja URIs + Jinja default() in --where.
    rc, env, err = _run_json([
        'pipe',
        '--from',
        '{{ input_csv }}',
        '--to',
        '{{ output_parquet }}',
        '--where',
        'amount >= {{ min_amount | default(50) }}',
        '--select',
        'order_id, amount * {{ rate | default(1.0) }} AS final_amount',
        '--emit-yaml',
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    emitted_yaml = env['outputs']['yaml']
    self.assertIn('ReadFromCsv', emitted_yaml)
    self.assertIn('WriteToParquet', emitted_yaml)
    self.assertIn('{{ input_csv }}', emitted_yaml)
    self.assertIn('{{ output_parquet }}', emitted_yaml)
    self.assertIn('{{ min_amount | default(50) }}', emitted_yaml)
    self.assertIn('{{ rate | default(1.0) }}', emitted_yaml)

    # 1. Passes `beam check -` offline without executing Jinja!
    with mock.patch('sys.stdin', io.StringIO(emitted_yaml)):
      rc_chk, env_chk, err_chk = _run_json(['check', '-'])
    self.assertEqual(rc_chk, cli.EXIT_OK, err_chk)
    self.assertTrue(env_chk['outputs']['valid'])

    # 2. Pipes directly into `beam package - --to spec.json` extracting all params!
    spec_json = os.path.join(self.tmp, 'orders_etl.template.json')
    with mock.patch('sys.stdin', io.StringIO(emitted_yaml)):
      rc_pkg, env_pkg, err_pkg = _run_json([
          'package',
          '-',
          '--to',
          spec_json,
          '--image',
          'gcr.io/dataflow-templates/yaml-template:test',
      ])
    self.assertEqual(rc_pkg, cli.EXIT_OK, err_pkg)
    params = {p['name']: p for p in env_pkg['outputs']['parameters']}
    self.assertEqual(
        set(params.keys()),
        {'input_csv', 'output_parquet', 'min_amount', 'rate'})
    self.assertFalse(params['input_csv']['isOptional'])
    self.assertFalse(params['output_parquet']['isOptional'])
    self.assertTrue(params['min_amount']['isOptional'])
    self.assertEqual(params['min_amount']['defaultValue'], '50')
    self.assertTrue(params['rate']['isOptional'])
    self.assertEqual(params['rate']['defaultValue'], '1.0')

    # 3. Security regression: live `beam pipe` (without --emit-yaml) rejects Jinja!
    csv_in = os.path.join(self.tmp, 'live.csv')
    with open(csv_in, 'w', encoding='utf-8') as f:
      f.write('order_id,amount\n1,100\n')
    rc_live_uri, env_live_uri, _ = _run_json([
        'pipe', '--from', '{{ input_csv }}', '--to', os.path.join(self.tmp, 'x.parquet')
    ])
    self.assertEqual(rc_live_uri, cli.EXIT_USAGE)
    self.assertIn('--emit-yaml', env_live_uri['error']['message'])

    rc_live_where, _, _ = _run_json([
        'pipe',
        '--from',
        csv_in,
        '--to',
        os.path.join(self.tmp, 'x.parquet'),
        '--where',
        'amount >= {{ min_amount | default(50) }}',
    ])
    self.assertEqual(rc_live_where, cli.EXIT_USAGE)

    # 4. Malformed Jinja in `--emit-yaml` fails fast with EXIT_USAGE.
    rc_bad_j, _, _ = _run_json([
        'pipe',
        '--from',
        '{{ input_csv }}',
        '--to',
        '{{ output_parquet }}',
        '--where',
        'amount >= {{ unclosed',
        '--emit-yaml',
    ])
    self.assertEqual(rc_bad_j, cli.EXIT_USAGE)

  def test_local_pipe_leaves_zero_empty_beam_temp_directories(self):
    raw_csv = os.path.join(self.tmp, 'orders.csv')
    with open(raw_csv, 'w', encoding='utf-8') as f:
      f.write('order_id,amount\n1,75.0\nbad_row\n2,-5.0\n')

    out_dir = os.path.join(self.tmp, 'clean_out')
    os.makedirs(os.path.join(out_dir, '.temp-stale-empty', 'sub'), exist_ok=True)
    concurrent_dir = os.path.join(out_dir, '.temp-concurrent-active')
    os.makedirs(concurrent_dir, exist_ok=True)
    active_part = os.path.join(concurrent_dir, 'in_flight.part')
    with open(active_part, 'w', encoding='utf-8') as f:
      f.write('in-progress')

    out_pq = os.path.join(out_dir, 'clean_orders.parquet')
    dlq_json = os.path.join(out_dir, 'orders_dlq.json')
    rc, env, err = _run_json([
        'pipe',
        '--from',
        raw_csv,
        '--to',
        out_pq,
        '--dlq',
        dlq_json,
        '--where',
        'amount > 0',
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(env['outputs']['files'], [out_pq])
    entries = sorted(os.listdir(out_dir))
    # Empty `.temp*` directories (both Beam's FileBasedSink temp dir and
    # `.temp-stale-empty`) are pruned, while non-empty `.temp-concurrent-active`
    # is preserved untouched!
    self.assertEqual(
        entries,
        ['.temp-concurrent-active', 'clean_orders.parquet', 'orders_dlq.json'])
    self.assertTrue(os.path.isfile(active_part))


class FastLocalPipeTest(unittest.TestCase):
  """Small local file-to-file pipes run in-process (Round 4 finding R1)."""
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-fast-local-test-')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.csv = os.path.join(self.tmp, 'orders.csv')
    with open(self.csv, 'w', encoding='utf-8') as f:
      f.write('id,customer,amount,zip\n')
      f.write('1,alice,120.5,00501\n')
      f.write('2,bob,abc,94107\n')
      f.write('3,carol,,10115\n')
      f.write('4,dave,300,02139,extra\n')
      f.write('5,erin,99.99,60601\n')
      f.write('6,frank,7,30301\n')
    self.jsonl = os.path.join(self.tmp, 'events.jsonl')
    with open(self.jsonl, 'w', encoding='utf-8') as f:
      for r in [
          {'id': 1, 'user': {'name': 'a', 'tier': 'gold'}, 'amount': 10.5},
          {'id': 2, 'user': {'name': 'b', 'tier': None}, 'amount': 200},
          {'id': 3, 'user': None, 'amount': None},
          {'id': 4, 'user': {'name': 'd', 'tier': 'gold'}, 'amount': 0},
      ]:
        f.write(json.dumps(r) + '\n')

  def _pipe(self, argv, fast=True):
    env = {} if fast else {'BEAM_CLI_NO_FAST_PATH': '1'}
    return _run_json(['pipe', '--from'] + list(argv), extra_env=env)

  def test_csv_with_dlq_runs_in_process(self):
    out = os.path.join(self.tmp, 'out.parquet')
    dlq = os.path.join(self.tmp, 'dlq.jsonl')
    rc, env, err = self._pipe([
        self.csv, '--to', out, '--where', 'amount > 50', '--select',
        'id, customer, zip', '--dlq', dlq, '--assert-counts'
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(env['runner'], 'local_fast')
    self.assertEqual(env['outputs']['files'], [out])
    self.assertEqual(env['outputs']['dlq_files'], [dlq])
    # 6 records: bad float (2) and extra column (4) -> DLQ; NULL amount (3)
    # and 7 (6) filtered out; 1 and 5 written.
    self.assertEqual(
        env['metrics'], {
            'rows_read': 6, 'rows_written': 2, 'rows_filtered_out': 2,
            'dlq_rows': 2
        })
    table = pq.read_table(out)
    self.assertEqual(
        table.to_pylist(), [
            {'id': 1, 'customer': 'alice', 'zip': '00501'},
            {'id': 5, 'customer': 'erin', 'zip': '60601'},
        ])
    self.assertEqual(str(table.schema.field('id').type), 'int64')
    with open(dlq, encoding='utf-8') as f:
      errs = [json.loads(l) for l in f if l.strip()]
    self.assertEqual(
        sorted(e['error_type'] for e in errs),
        ['ColumnCountMismatch', 'ValueError'])

  def test_matches_runner_path(self):
    cases = [
        [
            self.csv, '--to', 'OUT.jsonl', '--where', 'amount > 50',
            '--select', 'id, upper(customer) AS c, amount * 2 AS twice',
            '--dlq', 'DLQ.jsonl'
        ],
        [
            self.jsonl, '--to', 'OUT.csv', '--where',
            "user.tier == 'gold' or amount > 100", '--select',
            'id, coalesce(amount, 0) AS amount'
        ],
    ]
    for i, case in enumerate(cases):
      results = []
      for fast in (True, False):
        tag = f'{i}-{"fast" if fast else "beam"}'
        argv = [
            a.replace('OUT', os.path.join(self.tmp, f'out-{tag}')).replace(
                'DLQ', os.path.join(self.tmp, f'dlq-{tag}')) for a in case
        ]
        rc, env, err = self._pipe(argv, fast=fast)
        self.assertEqual(rc, cli.EXIT_OK, err)
        files = env['outputs']['files'] + env['outputs'].get('dlq_files', [])
        contents = []
        for path in files:
          with open(path, encoding='utf-8') as f:
            body = f.read().splitlines()
          if path.endswith('.csv'):
            contents.append([body[0]] + sorted(body[1:]))
          else:
            parsed = [json.loads(l) for l in body if l.strip()]
            for rec in parsed:
              rec.pop('raw_payload', None)
            contents.append(sorted(json.dumps(r, sort_keys=True) for r in parsed))
        results.append((env['runner'], env['metrics'], contents))
      (fast_runner, fast_metrics, fast_rows), (beam_runner, beam_metrics,
                                               beam_rows) = results
      self.assertEqual(fast_runner, 'local_fast')
      self.assertNotEqual(beam_runner, 'local_fast')
      self.assertEqual(fast_metrics, beam_metrics, case)
      self.assertEqual(fast_rows, beam_rows, case)

  def test_runner_is_used_when_not_eligible(self):
    out = os.path.join(self.tmp, 'o.jsonl')
    for extra, env_vars in (
        (['--runner=FnApiRunner'], {}),
        ([], {'BEAM_CLI_NO_FAST_PATH': '1'}),
    ):
      if os.path.exists(out):
        os.remove(out)
      rc, env, err = _run_json(
          ['pipe', '--from', self.jsonl, '--to', out] + extra,
          extra_env=env_vars)
      self.assertEqual(rc, cli.EXIT_OK, err)
      self.assertNotEqual(env['runner'], 'local_fast')
    # Sharded output (no file extension) keeps Beam's shard naming.
    rc, env, err = self._pipe(
        [self.jsonl, '--to', os.path.join(self.tmp, 'shards'), '--to-format', 'json'])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertNotEqual(env['runner'], 'local_fast')

  def test_size_cap_falls_back_to_runner(self):
    from apache_beam.cli import fast_local
    with mock.patch.object(fast_local, 'MAX_INPUT_BYTES', 10):
      rc, env, err = self._pipe(
          [self.jsonl, '--to', os.path.join(self.tmp, 'o.jsonl')])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertNotEqual(env['runner'], 'local_fast')

  def test_bad_record_without_dlq_fails_with_hint(self):
    rc, env, _ = self._pipe([self.csv, '--to', os.path.join(self.tmp, 'o.csv')])
    self.assertEqual(rc, cli.EXIT_PIPELINE_FAILED)
    self.assertIn('--dlq', env['error']['message'])
    self.assertIn('ReadCsvWithDlq', env['error']['message'])

  def test_expression_error_without_dlq_fails(self):
    rc, env, _ = self._pipe([
        self.jsonl, '--to', os.path.join(self.tmp, 'o.jsonl'), '--select',
        'id, 10 / amount AS inv'
    ])
    self.assertEqual(rc, cli.EXIT_PIPELINE_FAILED)
    self.assertIn('ZeroDivisionError', env['error']['message'])
    self.assertIn("'MapToFields'", env['error']['message'])

  def test_existing_output_is_refused_and_replace_overwrites(self):
    out = os.path.join(self.tmp, 'o.jsonl')
    with open(out, 'w', encoding='utf-8') as f:
      f.write('{"old": 1}\n')
    rc, env, _ = self._pipe([self.jsonl, '--to', out])
    self.assertEqual(rc, cli.EXIT_USAGE)
    rc, env, err = self._pipe([self.jsonl, '--to', out, '--if-exists', 'replace'])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(env['runner'], 'local_fast')
    with open(out, encoding='utf-8') as f:
      self.assertEqual(len(f.read().splitlines()), 4)

  def test_nested_struct_to_csv_is_json_text(self):
    out = os.path.join(self.tmp, 'o.csv')
    rc, env, err = self._pipe([self.jsonl, '--to', out])
    self.assertEqual(rc, cli.EXIT_OK, err)
    with open(out, encoding='utf-8', newline='') as f:
      rows = list(csv.reader(f))
    self.assertEqual(rows[0], ['id', 'user', 'amount'])
    self.assertEqual(json.loads(rows[1][1]), {'name': 'a', 'tier': 'gold'})
    self.assertEqual(rows[3][1], '')


class NullSafeExpressionTest(unittest.TestCase):
  """NULL operands follow SQL semantics instead of raising."""
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-null-safe-test-')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.src = os.path.join(self.tmp, 'in.jsonl')
    with open(self.src, 'w', encoding='utf-8') as f:
      for r in [
          {'id': 1, 'amount': 120.0, 'user': {'tier': 'gold'}, 'name': 'Ann'},
          {'id': 2, 'amount': None, 'user': None, 'name': None},
          {'id': 3, 'amount': 5.0, 'user': {'tier': None}, 'name': 'Bo'},
      ]:
        f.write(json.dumps(r) + '\n')

  def _ids(self, where, fast):
    out = os.path.join(self.tmp, f'o-{fast}-{abs(hash(where))}.jsonl')
    env_vars = {} if fast else {'BEAM_CLI_NO_FAST_PATH': '1'}
    rc, env, err = _run_json(
        ['pipe', '--from', self.src, '--to', out, '--where', where],
        extra_env=env_vars)
    self.assertEqual(rc, cli.EXIT_OK, err)
    with open(out, encoding='utf-8') as f:
      return sorted(json.loads(l)['id'] for l in f if l.strip())

  def test_where_drops_null_comparisons(self):
    for fast in (True, False):
      self.assertEqual(self._ids('amount > 10', fast), [1])
      self.assertEqual(self._ids('not (amount > 10)', fast), [3])
      self.assertEqual(self._ids("user.tier == 'gold'", fast), [1])
      self.assertEqual(self._ids("upper(name) == 'BO'", fast), [3])
      self.assertEqual(self._ids('amount * 2 >= 10', fast), [1, 3])
      self.assertEqual(self._ids('amount IS NULL', fast), [2])

  def test_compiled_code_keeps_type_inference_for_nested_paths(self):
    from apache_beam.cli import expressions
    cfg, _ = expressions.compile_select('id, user.tier AS tier, amount * 2 AS a')
    self.assertEqual(cfg['fields']['id'], 'id')
    self.assertIn(
        "getattr(__row__, 'user').tier", cfg['fields']['tier']['callable'])
    self.assertIn('is None', cfg['fields']['a']['callable'])


class NullMatrixPipeTest(unittest.TestCase):
  """NULL x operator x format, end to end on both paths (Round 5 F1-F3).

  SQLite is the oracle for which rows a SQL WHERE keeps. CSV has no NULL
  literal, so its empty cells are NULL (as in core ReadFromCsv) and the
  oracle loads them that way.
  """
  ROWS = [
      {'id': 1, 'state': 'CA', 'amount': 120.0, 'qty': 2},
      {'id': 2, 'state': None, 'amount': 50.0, 'qty': 0},
      {'id': 3, 'state': 'NY', 'amount': None, 'qty': 3},
      {'id': 4, 'state': 'C', 'amount': 0.0, 'qty': None},
      {'id': 5, 'state': '', 'amount': 300.0, 'qty': 1},
  ]
  FAST_EXPRESSIONS = [
      "state != 'CA'",
      "state IN ('CA')",
      "state NOT IN ('CA', 'NY')",
      'NOT (amount > 100)',
      'amount > 100 OR qty = 0',
      'NOT (amount > 100 OR qty = 0)',
      'state IS NULL',
  ]
  RUNNER_CASES = [
      ('csv', "state != 'CA'"),
      ('jsonl', "state IN ('CA')"),
      ('parquet', 'NOT (amount > 100 OR qty = 0)'),
  ]

  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-null-matrix-test-')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.paths = {
        'csv': os.path.join(self.tmp, 'in.csv'),
        'jsonl': os.path.join(self.tmp, 'in.jsonl'),
        'parquet': os.path.join(self.tmp, 'in.parquet'),
    }
    with open(self.paths['csv'], 'w', encoding='utf-8') as f:
      f.write('id,state,amount,qty\n')
      for r in self.ROWS:
        f.write(','.join('' if r[c] is None else str(r[c])
                         for c in ('id', 'state', 'amount', 'qty')) + '\n')
    with open(self.paths['jsonl'], 'w', encoding='utf-8') as f:
      for r in self.ROWS:
        f.write(json.dumps(r) + '\n')
    pq.write_table(pa.Table.from_pylist(self.ROWS), self.paths['parquet'])

  def _expected(self, fmt, expr):
    import sqlite3
    conn = sqlite3.connect(':memory:')
    conn.execute('CREATE TABLE t (id INT, state TEXT, amount REAL, qty INT)')
    rows = [
        dict(r, state=None if (fmt == 'csv' and r['state'] == '') else
             r['state']) for r in self.ROWS
    ]
    conn.executemany('INSERT INTO t VALUES (:id, :state, :amount, :qty)', rows)
    return sorted(i for (i, ) in conn.execute(f'SELECT id FROM t WHERE {expr}'))

  def _ids(self, fmt, expr, fast):
    out = os.path.join(
        self.tmp, f'out-{fmt}-{int(fast)}-{abs(hash(expr))}.jsonl')
    rc, env, err = _run_json(
        ['pipe', '--from', self.paths[fmt], '--to', out, '--where', expr],
        extra_env={} if fast else {'BEAM_CLI_NO_FAST_PATH': '1'})
    self.assertEqual(rc, cli.EXIT_OK, f'{fmt} {expr}: {err}')
    self.assertEqual(env['runner'] == 'local_fast', fast)
    if not os.path.exists(out):
      return []  # the Beam runner writes no file when no rows are kept
    with open(out, encoding='utf-8') as f:
      return sorted(json.loads(l)['id'] for l in f if l.strip())

  def test_fast_path_matches_sqlite(self):
    for fmt in self.paths:
      for expr in self.FAST_EXPRESSIONS:
        self.assertEqual(
            self._ids(fmt, expr, fast=True), self._expected(fmt, expr),
            f'{fmt}: {expr}')

  def test_runner_path_matches_sqlite(self):
    for fmt, expr in self.RUNNER_CASES:
      self.assertEqual(
          self._ids(fmt, expr, fast=False), self._expected(fmt, expr),
          f'{fmt}: {expr}')

  def test_null_values_option(self):
    src = os.path.join(self.tmp, 'na.csv')
    with open(src, 'w', encoding='utf-8') as f:
      f.write('id,region,amount\n1,NA,10\n2,EU,NULL\n3,,5\n4,US,7\n')
    out = os.path.join(self.tmp, 'na.jsonl')
    rc, env, err = _run_json([
        'pipe', '--from', src, '--to', out, '--null-values', 'NA,NULL',
        '--where', 'region IS NULL OR amount IS NULL'
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    with open(out, encoding='utf-8') as f:
      rows = [json.loads(l) for l in f if l.strip()]
    self.assertEqual([r['id'] for r in rows], [1, 2, 3])
    # amount stays numeric: NULL is not a dirty value.
    self.assertIsNone(rows[1]['amount'])
    self.assertEqual(rows[0]['amount'], 10)
    # Exported YAML keeps the same markers for pandas.
    rc, env, _ = _run_json([
        'pipe', '--from', src, '--to', out + '.x.jsonl', '--null-values',
        'NA,NULL', '--emit-yaml'
    ])
    self.assertEqual(rc, cli.EXIT_OK)
    self.assertIn('keep_default_na: false', env['outputs']['yaml'])


def _has_duckdb() -> bool:
  import importlib.util
  return importlib.util.find_spec('duckdb') is not None


class DuckDbSqlEngineTest(unittest.TestCase):
  """`beam pipe --sql --sql-engine duckdb` (optional local SQL engine)."""
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-duckdb-test-')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.csv = os.path.join(self.tmp, 'o.csv')
    with open(self.csv, 'w', encoding='utf-8') as f:
      f.write('id,region,amount\n1,US,10\n2,EU,\n3,US,30\n4,,40\n')
    self.pq = os.path.join(self.tmp, 'u.parquet')
    pq.write_table(
        pa.Table.from_pylist([
            {'id': 1, 'user': {'tier': 'gold'}},
            {'id': 3, 'user': None},
        ]),
        self.pq)

  def _sql(self, query, *extra, engine='duckdb', inputs=None):
    argv = ['pipe']
    for alias, path in (inputs or [('o', self.csv)]):
      argv += ['--from' if argv == ['pipe'] else '--join', f'{alias}={path}']
    argv += ['--sql', query, '--sql-engine', engine, *extra]
    return _run_json(argv)

  @unittest.skipUnless(_has_duckdb(), 'duckdb not installed')
  def test_ansi_analytics_and_nulls(self):
    rc, env, err = self._sql(
        'SELECT region, percentile_cont(0.5) WITHIN GROUP (ORDER BY amount) '
        'AS p50, count(*) AS n FROM o GROUP BY region '
        'ORDER BY region NULLS LAST')
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(env['outputs']['sql_engine'], 'duckdb')
    self.assertEqual(
        env['untrusted_data']['rows'], [
            {'region': 'EU', 'p50': None, 'n': 1},
            {'region': 'US', 'p50': 20.0, 'n': 2},
            {'region': None, 'p50': 40.0, 'n': 1},
        ])

  @unittest.skipUnless(_has_duckdb(), 'duckdb not installed')
  def test_join_with_nested_struct_and_file_output(self):
    out = os.path.join(self.tmp, 'out.parquet')
    rc, env, err = self._sql(
        'SELECT o.id, u."user".tier AS tier FROM o '
        'LEFT JOIN u ON o.id = u.id ORDER BY o.id', '--to', out,
        inputs=[('o', self.csv), ('u', self.pq)])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(
        pq.read_table(out).to_pylist(), [
            {'id': 1, 'tier': 'gold'},
            {'id': 2, 'tier': None},
            {'id': 3, 'tier': None},
            {'id': 4, 'tier': None},
        ])

  @unittest.skipUnless(_has_duckdb(), 'duckdb not installed')
  def test_same_answer_as_sqlite(self):
    query = ('SELECT region, COUNT(*) AS n, SUM(amount) AS total FROM o '
             "WHERE region IS NOT NULL AND region != 'XX' GROUP BY region "
             'ORDER BY region')
    results = {}
    for engine in ('sqlite', 'duckdb'):
      rc, env, err = self._sql(query, engine=engine)
      self.assertEqual(rc, cli.EXIT_OK, err)
      results[engine] = env['untrusted_data']['rows']
    self.assertEqual(results['sqlite'], results['duckdb'])

  @unittest.skipUnless(_has_duckdb(), 'duckdb not installed')
  def test_sandbox_blocks_files_and_multiple_statements(self):
    for query in ("SELECT * FROM read_csv('/etc/passwd')",
                  f"SELECT * FROM '{self.csv}'",
                  "SELECT 1; SELECT 2"):
      rc, env, _ = self._sql(query)
      self.assertEqual(rc, cli.EXIT_USAGE, query)
      self.assertNotIn('root:', json.dumps(env))

  def test_missing_duckdb_points_to_install_and_doctor(self):
    with mock.patch.dict(sys.modules, {'duckdb': None}):
      rc, env, _ = self._sql('SELECT * FROM o')
    self.assertEqual(rc, cli.EXIT_MISSING_PACKAGE)
    self.assertIn('pip install', env['error']['message'])
    self.assertIn('beam doctor', env['error']['message'])

  def test_sqlite_within_group_error_suggests_duckdb(self):
    rc, env, _ = self._sql(
        'SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY amount) FROM o',
        engine='sqlite')
    self.assertEqual(rc, cli.EXIT_USAGE)
    self.assertIn('--sql-engine duckdb', env['error']['message'])

  def test_doctor_reports_sql_engines(self):
    rc, env, _ = _run_json(['doctor'])
    engines = env['outputs']['sql_engines']
    self.assertEqual(engines['default'], 'sqlite')
    self.assertTrue(engines['sqlite']['available'])
    self.assertEqual(engines['duckdb']['available'], _has_duckdb())
    if not _has_duckdb():
      self.assertIn('pip install', engines['duckdb']['install'])


class Round6FixesTest(unittest.TestCase):
  """Mixed-format globs, explicit types, static check keys, SQL --dlq."""
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-r6-test-')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.mixed = os.path.join(self.tmp, 'mixed')
    os.makedirs(self.mixed)
    with open(os.path.join(self.mixed, 'a.jsonl'), 'w', encoding='utf-8') as f:
      f.write('{"order_id": 1, "region": "EU", "amount": 10.5}\n')
      f.write('{"order_id": 2, "region": "US", "amount": "abc"}\n')
      f.write('{"order_id": 3, "region": "US", "amount": 0.0}\n')
      f.write('{"order_id": 4, "region": "EU", "amount": 7.25}\n')
    with open(os.path.join(self.mixed, 'b.csv'), 'w', encoding='utf-8') as f:
      # Different column order than the JSONL files, and dirty values.
      f.write('region,order_id,amount\nUS,9,100.0\nEU,10,\nAPAC,11,oops\n')

  def _path(self, name):
    return os.path.join(self.tmp, name)

  def _rows(self, path):
    with open(path, encoding='utf-8') as f:
      return [json.loads(l) for l in f if l.strip()]

  def test_mixed_glob_reads_each_file_with_its_format(self):
    out, bad = self._path('out.jsonl'), self._path('bad.jsonl')
    rc, env, err = _run_json([
        'pipe', '--from', os.path.join(self.mixed, '*'), '--to', out, '--dlq',
        bad, '--where', 'amount > 0', '--assert-counts'
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(env['runner'], 'local_fast')
    self.assertEqual(
        env['metrics'], {
            'rows_read': 7, 'rows_written': 3, 'rows_filtered_out': 2,
            'dlq_rows': 2
        })
    self.assertEqual(
        self._rows(out), [
            {'order_id': 1, 'region': 'EU', 'amount': 10.5},
            {'order_id': 4, 'region': 'EU', 'amount': 7.25},
            {'order_id': 9, 'region': 'US', 'amount': 100.0},
        ])
    self.assertEqual(
        sorted(r['step'] for r in self._rows(bad)),
        ['ReadCsvWithDlq', 'ReadJsonWithDlq'])

  def test_mixed_glob_outside_fast_path_fails_clearly(self):
    rc, env, _ = _run_json([
        'pipe', '--from', os.path.join(self.mixed, '*'), '--to',
        self._path('x.jsonl'), '--emit-yaml'
    ])
    self.assertEqual(rc, cli.EXIT_USAGE)
    self.assertIn('several formats (1 json, 1 csv)', env['error']['message'])
    self.assertIn('UNION ALL', env['error']['message'])

  def test_csv_files_with_reordered_headers_match_by_name(self):
    d = os.path.join(self.tmp, 'csvs')
    os.makedirs(d)
    with open(os.path.join(d, '1.csv'), 'w', encoding='utf-8') as f:
      f.write('id,name,qty\n1,a,5\n')
    with open(os.path.join(d, '2.csv'), 'w', encoding='utf-8') as f:
      f.write('qty,id,name\n7,2,b\n')
    for env_vars in ({}, {'BEAM_CLI_NO_FAST_PATH': '1'}):
      out = self._path(f'csvs-{len(env_vars)}.jsonl')
      rc, env, err = _run_json(
          ['pipe', '--from', os.path.join(d, '*.csv'), '--to', out],
          extra_env=env_vars)
      self.assertEqual(rc, cli.EXIT_OK, err)
      self.assertEqual(
          sorted(self._rows(out), key=lambda r: r['id']), [
              {'id': 1, 'name': 'a', 'qty': 5},
              {'id': 2, 'name': 'b', 'qty': 7},
          ], env_vars)

  def test_explicit_csv_types_both_paths(self):
    src = self._path('t.csv')
    with open(src, 'w', encoding='utf-8') as f:
      f.write('id,amount\n1,100.0\n2,oops\n')
    for env_vars in ({}, {'BEAM_CLI_NO_FAST_PATH': '1'}):
      out = self._path(f't-{len(env_vars)}.jsonl')
      bad = self._path(f't-{len(env_vars)}-bad.jsonl')
      rc, env, err = _run_json([
          'pipe', '--from', src, '--to', out, '--dlq', bad, '--from-opt',
          'fields=amount:float', '--where', 'amount > 0'
      ],
                               extra_env=env_vars)
      self.assertEqual(rc, cli.EXIT_OK, err)
      self.assertEqual(self._rows(out), [{'id': 1, 'amount': 100.0}])
      self.assertEqual(len(self._rows(bad)), 1)
    rc, env, _ = _run_json([
        'pipe', '--from', src, '--to', self._path('u.jsonl'), '--from-opt',
        'fields=nope:int'
    ])
    self.assertNotEqual(rc, cli.EXIT_OK)
    self.assertIn("column 'nope' is not in", env['error']['message'])

  def test_static_check_rejects_params_outside_config(self):
    path = self._path('bad.yaml')
    with open(path, 'w', encoding='utf-8') as f:
      f.write('pipeline:\n  type: chain\n  transforms:\n'
              '  - type: ReadFromCsv\n    path: in.csv\n'
              '  - type: LogForTesting\n')
    rc, env, _ = _run_json(['check', path])
    self.assertEqual(rc, cli.EXIT_USAGE)
    self.assertIn('path outside config:', env['error']['message'])

  def test_stdout_sink_is_rejected(self):
    src = self._path('s.csv')
    with open(src, 'w', encoding='utf-8') as f:
      f.write('id\n1\n')
    rc, env, _ = _run_json(['pipe', '--from', src, '--to', '-'])
    self.assertEqual(rc, cli.EXIT_USAGE)
    self.assertIn('standard output', env['error']['message'])
    self.assertFalse(os.path.exists('--00000-of-00001'))

  def test_sql_honors_dlq_and_does_not_suggest_it_again(self):
    out, bad = self._path('sql.parquet'), self._path('sql-bad.jsonl')
    rc, env, err = _run_json([
        'pipe', '--from', 'j=' + os.path.join(self.mixed, '*.jsonl'),
        '--join', 'c=' + os.path.join(self.mixed, '*.csv'), '--sql',
        'SELECT order_id, amount FROM j UNION ALL '
        'SELECT order_id, amount FROM c', '--to', out, '--dlq', bad
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(env['metrics']['dlq_rows'], 1)  # amount "abc"
    # The CSV amount column mixes numbers and text: written as text, warned.
    self.assertTrue(any("'amount' mixes numbers" in w for w in env['warnings']))
    # Without --dlq the bad JSON value fails, with a --dlq hint; with --dlq
    # given, a failure never suggests passing it.
    rc, env, _ = _run_json([
        'pipe', '--from', 'j=' + os.path.join(self.mixed, '*.jsonl'), '--sql',
        'SELECT * FROM j', '--to', self._path('sql2.jsonl')
    ])
    self.assertEqual(rc, cli.EXIT_USAGE)
    self.assertIn('pass --dlq', env['error']['message'])
    from apache_beam.cli import output as output_mod
    _, before = output_mod.classify_error(
        'pipeline failed: boom', cli.EXIT_PIPELINE_FAILED, 'pipe')
    self.assertTrue(any('--dlq' in a['command'] for a in before))
    output_mod.DLQ_REQUESTED = True
    try:
      _, after = output_mod.classify_error(
          'pipeline failed: boom', cli.EXIT_PIPELINE_FAILED, 'pipe')
    finally:
      output_mod.DLQ_REQUESTED = False
    self.assertFalse(any('--dlq' in a['command'] for a in after))


class Round7FixesTest(unittest.TestCase):
  """Keyed JSON documents and the text-vs-number filter hint."""
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-r7-test-')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.events = os.path.join(self.tmp, 'events.parquet')
    pq.write_table(
        pa.Table.from_pylist([
            {'store_id': 'S01', 'amount': 10.0},
            {'store_id': 'S01', 'amount': 30.0},
            {'store_id': 'S02', 'amount': 5.0},
            {'store_id': 'S09', 'amount': 7.0},
        ]),
        self.events)
    self.stores = os.path.join(self.tmp, 'stores.csv')
    with open(self.stores, 'w', encoding='utf-8') as f:
      f.write('store_id,region\nS01,West\nS02,East\n')

  def _path(self, name):
    return os.path.join(self.tmp, name)

  def _load(self, path):
    with open(path, encoding='utf-8') as f:
      return json.load(f)

  def test_sql_keyed_document(self):
    out = self._path('answer.json')
    rc, env, err = _run_json([
        'pipe', '--from', 's=' + self.events, '--join', 'st=' + self.stores,
        '--sql',
        "SELECT COALESCE(st.region, 'UNMAPPED') AS region, COUNT(*) AS events, "
        'median(s.amount) AS median FROM s LEFT JOIN st USING (store_id) '
        'GROUP BY 1', '--to', out, '--to-opt', 'key=region'
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(
        self._load(out), {
            'West': {'events': 2, 'median': 20.0},
            'East': {'events': 1, 'median': 5.0},
            'UNMAPPED': {'events': 1, 'median': 7.0},
        })

  def test_sql_keyed_value_embeds_json_arrays(self):
    out = self._path('lists.json')
    rc, env, err = _run_json([
        'pipe', '--from', 's=' + self.events, '--sql',
        "SELECT 'big' AS k, json_group_array(store_id) AS ids FROM "
        "(SELECT DISTINCT store_id FROM s WHERE amount >= 10 ORDER BY 1) "
        "UNION ALL SELECT 'small', json_group_array(store_id) FROM "
        '(SELECT DISTINCT store_id FROM s WHERE amount < 10 ORDER BY 1)',
        '--to', out, '--to-opt', 'key=k', '--to-opt', 'value=ids'
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(self._load(out), {'big': ['S01'], 'small': ['S02', 'S09']})

  def test_pipe_keyed_document_on_fast_path(self):
    out = self._path('stores.json')
    rc, env, err = _run_json(
        ['pipe', '--from', self.stores, '--to', out, '--to-opt', 'key=store_id'])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(env['runner'], 'local_fast')
    self.assertEqual(
        self._load(out), {'S01': {'region': 'West'}, 'S02': {'region': 'East'}})

  def test_keyed_document_errors(self):
    cases = [
        # Duplicate key values.
        (['pipe', '--from', self.events, '--to', self._path('d.json'),
          '--to-opt', 'key=store_id'], 'appears more than once'),
        # Not a .json file.
        (['pipe', '--from', 's=' + self.events, '--sql', 'SELECT * FROM s',
          '--to', self._path('d.jsonl'), '--to-opt', 'key=store_id'],
         'must be a local .json file'),
        # Runner path.
        (['pipe', '--from', self.stores, '--to', self._path('r.json'),
          '--to-opt', 'key=store_id', '--runner=FnApiRunner'],
         'small local runs'),
    ]
    for argv, needle in cases:
      rc, env, _ = _run_json(argv)
      self.assertNotEqual(rc, cli.EXIT_OK, argv)
      self.assertIn(needle, env['error']['message'], argv)

  def test_text_number_filter_hint(self):
    src = self._path('t.csv')
    with open(src, 'w', encoding='utf-8') as f:
      f.write('id,amount\n1,100.0\n2,oops\n')
    rc, env, err = _run_json([
        'pipe', '--from', src, '--to', self._path('t.jsonl'), '--dlq',
        self._path('t-bad.jsonl'), '--where', 'amount > 0'
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertTrue(
        any('--from-opt fields=amount:float' in w for w in env['warnings']),
        env['warnings'])


class Round8FixesTest(unittest.TestCase):
  """Late leading-zero CSV values and readable scrubbed errors."""
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-r8-test-')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.src = os.path.join(self.tmp, 'late.csv')
    with open(self.src, 'w', encoding='utf-8') as f:
      f.write('id,zip\n')
      for i in range(1, 152):
        f.write(f'{i},{"00123" if i == 120 else 10000 + i}\n')

  def _path(self, name):
    return os.path.join(self.tmp, name)

  def test_late_leading_zero_kept_on_fast_and_sql_paths(self):
    out = self._path('fast.jsonl')
    rc, env, err = _run_json(
        ['pipe', '--from', self.src, '--to', out, '--where', 'id = 120'])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(env['runner'], 'local_fast')
    with open(out, encoding='utf-8') as f:
      self.assertEqual(json.loads(f.readline())['zip'], '00123')
    rc, env, err = _run_json([
        'pipe', '--from', 't=' + self.src, '--sql',
        'SELECT zip FROM t WHERE id = 120'
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(env['untrusted_data']['rows'], [{'zip': '00123'}])

  def test_late_leading_zero_never_cast_on_runner_path(self):
    no_fast = {'BEAM_CLI_NO_FAST_PATH': '1'}
    rc, env, _ = _run_json(
        ['pipe', '--from', self.src, '--to', self._path('r.jsonl')],
        extra_env=no_fast)
    self.assertEqual(rc, cli.EXIT_PIPELINE_FAILED)
    self.assertIn('leading zero', env['error']['message'])
    self.assertIn('fields=COLUMN:str', env['error']['message'])
    out = self._path('r2.jsonl')
    rc, env, err = _run_json([
        'pipe', '--from', self.src, '--to', out, '--from-opt', 'fields=zip:str',
        '--where', 'id = 120'
    ],
                             extra_env=no_fast)
    self.assertEqual(rc, cli.EXIT_OK, err)
    with open(out, encoding='utf-8') as f:
      self.assertEqual(json.loads(f.readline())['zip'], '00123')

  def test_offline_dry_run_falls_back_to_static_check(self):
    path = self._path('gcs.yaml')
    with open(path, 'w', encoding='utf-8') as f:
      f.write('pipeline:\n  type: chain\n  transforms:\n'
              '  - type: ReadFromCsv\n    config:\n'
              '      path: gs://bucket/orders/*.csv\n'
              '  - type: WriteToJson\n    config:\n      path: out.json\n')

    class RefreshError(Exception):
      pass

    with mock.patch(
        'apache_beam.yaml.main.build_pipeline_components_from_argv',
        side_effect=RefreshError('Reauthentication is needed')):
      rc, env, err = _run_json(['run', path, '--dry-run'])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(env['state'], 'DRY_RUN_VALIDATED')
    self.assertTrue(env['warnings'][0].startswith('dry run passed'))
    # A real YAML error is still an error.
    with open(path, 'a', encoding='utf-8') as f:
      f.write('  - type: Filter\n    keep: x > 1\n')
    with mock.patch(
        'apache_beam.yaml.main.build_pipeline_components_from_argv',
        side_effect=RefreshError('Reauthentication is needed')):
      rc, env, _ = _run_json(['run', path, '--dry-run'])
    self.assertEqual(rc, cli.EXIT_USAGE)

  def test_scrubber_keeps_text_between_short_literals(self):
    from apache_beam.cli import output as output_mod
    msg = ("column 'zip': value 00123 has a leading zero and should be kept "
           "as text; see 'docs'")
    self.assertEqual(output_mod.scrub_error_message(msg), msg)
    self.assertIn(
        '<redacted-value>',
        output_mod.scrub_error_message(
            "token 'abcdefghijklmnopqrstuvwxyz0123456789' leaked"))


class Round9FixesTest(unittest.TestCase):
  """CDC ties at the latest sequence value; readable step labels/paths."""
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-r9-test-')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

  def _cdc(self, body, *extra):
    src = os.path.join(self.tmp, f'u{len(os.listdir(self.tmp))}.csv')
    with open(src, 'w', encoding='utf-8') as f:
      f.write('id,status,updated_at\n' + body)
    out = os.path.join(self.tmp, f'o{len(os.listdir(self.tmp))}.jsonl')
    rc, env, err = _run_json([
        'pipe', '--from', src, '--to', out, '--cdc', '--primary-key', 'id',
        '--cdc-seq-col', 'updated_at', '--runner=FnApiRunner', *extra
    ])
    return rc, env, err, out

  def test_exact_duplicate_changes_collapse(self):
    rc, env, err, out = self._cdc('1,active,2026-11-01T10:00:00\n'
                                  '1,active,2026-11-01T10:00:00\n'
                                  '2,closed,2026-11-02T10:00:00\n')
    self.assertEqual(rc, cli.EXIT_OK, err)
    with open(out, encoding='utf-8') as f:
      rows = sorted((json.loads(l) for l in f), key=lambda r: r['id'])
    self.assertEqual([r['status'] for r in rows], ['active', 'closed'])

  def test_conflicting_tie_at_latest_fails_with_correct_hint(self):
    # Older version first, then two different changes at the same latest
    # timestamp: before, one was picked arbitrarily.
    rc, env, _, _ = self._cdc('1,new,2026-11-01T09:00:00\n'
                              '1,active,2026-11-01T10:00:00\n'
                              '1,closed,2026-11-01T10:00:00\n')
    self.assertEqual(rc, cli.EXIT_PIPELINE_FAILED)
    msg = env['error']['message']
    self.assertIn('2 different changes', msg)
    self.assertIn('same updated_at value', msg)
    self.assertNotIn('pass --cdc-seq-col', msg)

  def test_scrubber_keeps_step_labels_and_existing_paths(self):
    from apache_beam.cli import output as output_mod
    path = os.path.join(self.tmp, 'a_rather_long_input_file_name.csv')
    open(path, 'w').close()
    msg = (f"could not read '{path}' [while running "
           "'ReadCsvWithDlq/ParseShards/ParDo(_ParseCsvShardDoFn)']")
    self.assertEqual(output_mod.scrub_error_message(msg), msg)
    self.assertIn(
        '<redacted-value>',
        output_mod.scrub_error_message(
            "password='hunter2-correct-horse-battery-staple' rejected"))
    missing = os.path.join(self.tmp, 'missing_long_input_dataset_2026.csv')
    miss_msg = f"no such file: '{missing}'"
    self.assertEqual(output_mod.scrub_error_message(miss_msg), miss_msg)

  def test_conflicting_tie_does_not_suggest_dlq(self):
    rc, env, _, _ = self._cdc('1,active,2026-11-01T10:00:00\n'
                              '1,closed,2026-11-01T10:00:00\n')
    self.assertEqual(rc, cli.EXIT_PIPELINE_FAILED)
    self.assertFalse(
        any('--dlq' in a['command'] for a in env.get('next_actions') or []))

  def test_to_utc_normalizes_mixed_timestamps_in_sqlite_and_duckdb(self):
    ev = os.path.join(self.tmp, 'events.parquet')
    pq.write_table(
        pa.Table.from_pylist([
            {'id': 1, 'ts': '2026-11-01T10:00:00Z', 'amt': 10.0},
            {'id': 2, 'ts': '2026-11-01 12:30:00+02:00', 'amt': 20.0},
            {'id': 2, 'ts': '2026-11-01T11:45:00+00:00', 'amt': 25.0},
            {'id': 3, 'ts': '2026-11-01T05:00:00-05:00', 'amt': 30.0},
            {'id': 4, 'ts': '1762000800', 'amt': 40.0},
            {'id': 5, 'ts': None, 'amt': 50.0},
        ]),
        ev)
    query = (
        'SELECT id, ts_utc, amt FROM ('
        'SELECT id, to_utc(ts) AS ts_utc, amt, '
        'ROW_NUMBER() OVER (PARTITION BY id ORDER BY to_utc(ts) DESC) AS rn '
        'FROM e) WHERE rn = 1 ORDER BY id')
    expected = [
        {'id': 1, 'ts_utc': '2026-11-01T10:00:00Z', 'amt': 10.0},
        {'id': 2, 'ts_utc': '2026-11-01T11:45:00Z', 'amt': 25.0},
        {'id': 3, 'ts_utc': '2026-11-01T10:00:00Z', 'amt': 30.0},
        {'id': 4, 'ts_utc': '2025-11-01T12:40:00Z', 'amt': 40.0},
        {'id': 5, 'ts_utc': None, 'amt': 50.0},
    ]
    engines = ('sqlite', 'duckdb') if _has_duckdb() else ('sqlite', )
    for eng in engines:
      rc, env, err = _run_json([
          'pipe', '--from', f'e={ev}', '--sql', query, '--sql-engine', eng
      ])
      self.assertEqual(rc, cli.EXIT_OK, (eng, err))
      self.assertEqual(env['untrusted_data']['rows'], expected, eng)

  def test_diff_to_file_and_exit_zero(self):
    left = os.path.join(self.tmp, 'left.jsonl')
    right = os.path.join(self.tmp, 'right.jsonl')
    with open(left, 'w', encoding='utf-8') as f:
      f.write('{"id": 1, "v": 10}\n{"id": 2, "v": 20}\n')
    with open(right, 'w', encoding='utf-8') as f:
      f.write('{"id": 1, "v": 10}\n{"id": 3, "v": 30}\n')
    rep = os.path.join(self.tmp, 'diff_report.json')
    rc, env, err = _run_json([
        'diff', left, right, '--key', 'id', '--to', rep, '--exit-zero'
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertTrue(env['ok'])
    self.assertFalse(env['outputs']['match'])
    self.assertEqual(env['outputs']['file'], rep)
    with open(rep, encoding='utf-8') as f:
      saved = json.load(f)
    self.assertFalse(saved['match'])
    self.assertEqual(saved['differing_keys']['only_in_left'], [2])
    self.assertEqual(saved['differing_keys']['only_in_right'], [3])
    # Without --exit-zero, drift still exits 6 while writing --to.
    rep2 = os.path.join(self.tmp, 'diff_report2.json')
    rc2, _, _ = _run_json(['diff', left, right, '--key', 'id', '--to', rep2])
    self.assertEqual(rc2, cli.EXIT_DIFF_MISMATCH)
    self.assertTrue(os.path.isfile(rep2))

  def test_in_place_replace_preserves_inputs_and_siblings(self):
    target_pq = os.path.join(self.tmp, 'customers.parquet')
    bak = target_pq + '.bak'
    open(bak, 'w').close()
    pq.write_table(pa.Table.from_pylist([{'id': 1, 'name': 'Alice'}]), target_pq)
    batch2 = os.path.join(self.tmp, 'batch2.jsonl')
    with open(batch2, 'w', encoding='utf-8') as f:
      f.write('{"id": 2, "full_name": "Bob", "tier": "gold"}\n')
    rc, env, err = _run_json([
        'pipe', '--from', f't={target_pq}', '--join', f'b={batch2}', '--sql',
        'SELECT id, name, CAST(NULL AS TEXT) AS tier FROM t UNION ALL '
        'SELECT id, full_name AS name, tier FROM b ORDER BY id',
        '--to', target_pq, '--if-exists', 'replace'
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertTrue(os.path.isfile(bak))
    self.assertEqual(
        pq.read_table(target_pq).to_pylist(), [
            {'id': 1, 'name': 'Alice', 'tier': None},
            {'id': 2, 'name': 'Bob', 'tier': 'gold'},
        ])
    # Fast-path in-place filter also reads before replacing.
    rc, env, err = _run_json([
        'pipe', '--from', target_pq, '--to', target_pq, '--where', 'id > 1',
        '--if-exists', 'replace'
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(
        pq.read_table(target_pq).to_pylist(),
        [{'id': 2, 'name': 'Bob', 'tier': 'gold'}])
    # Runner path also stages local replace before swapping, so in-place works!
    rc, env, err = _run_json(
        [
            'pipe', '--from', target_pq, '--to', target_pq, '--if-exists',
            'replace'
        ],
        extra_env={'BEAM_CLI_NO_FAST_PATH': '1'})
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(
        pq.read_table(target_pq).to_pylist(),
        [{'id': 2, 'name': 'Bob', 'tier': 'gold'}])


class UserFeedbackFixesTest(unittest.TestCase):
  """Regression tests for safe --if-exists replace and >100-row JSON drift."""

  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-cli-feedback-')

  def tearDown(self):
    shutil.rmtree(self.tmp, ignore_errors=True)

  def test_failed_replace_preserves_previous_output_across_all_paths(self):
    out_pq = os.path.join(self.tmp, 'report.parquet')
    expected_rows = [{'id': 1, 'amount': 42.5}, {'id': 2, 'amount': 99.0}]
    pq.write_table(pa.Table.from_pylist(expected_rows), out_pq)
    with open(out_pq, 'rb') as f:
      original_bytes = f.read()

    bad_csv = os.path.join(self.tmp, 'bad.csv')
    with open(bad_csv, 'w', encoding='utf-8') as f:
      f.write('id,amount\n10,12.5\n11,15.0\n12,NOT_A_FLOAT\n13,20.0\n')

    # 1. Fast local path fails on bad CSV row -> previous output stays intact.
    rc, env, _ = _run_json([
        'pipe', '--from', bad_csv, '--to', out_pq, '--from-opt',
        'fields=id:int,amount:float', '--if-exists', 'replace'
    ])
    self.assertEqual(rc, cli.EXIT_PIPELINE_FAILED)
    with open(out_pq, 'rb') as f:
      self.assertEqual(f.read(), original_bytes)

    # 2. Beam runner path fails mid-run -> staged dir discarded, old output intact.
    rc, env, _ = _run_json(
        [
            'pipe', '--from', bad_csv, '--to', out_pq, '--from-opt',
            'fields=id:int,amount:float', '--if-exists', 'replace'
        ],
        extra_env={'BEAM_CLI_NO_FAST_PATH': '1'})
    self.assertEqual(rc, cli.EXIT_PIPELINE_FAILED)
    with open(out_pq, 'rb') as f:
      self.assertEqual(f.read(), original_bytes)

    # 3. Beam runner path failing post-run --verify (--where drops a row so
    #    output != source) ALSO preserves the previous output!
    good_csv = os.path.join(self.tmp, 'good.csv')
    with open(good_csv, 'w', encoding='utf-8') as f:
      f.write('id,amount\n10,12.5\n11,50.0\n')
    rc, env, _ = _run_json([
        'pipe', '--from', good_csv, '--to', out_pq, '--where', 'amount > 20',
        '--verify', '--verify-key', 'id', '--if-exists', 'replace'
    ])
    self.assertEqual(rc, cli.EXIT_DIFF_MISMATCH)
    with open(out_pq, 'rb') as f:
      self.assertEqual(f.read(), original_bytes)

    # 4. Local --sql failing mid-query preserves previous output.
    rc, env, _ = _run_json([
        'pipe', '--from', good_csv, '--sql', 'SELECT nonexistent_col FROM source',
        '--to', out_pq, '--if-exists', 'replace'
    ])
    self.assertEqual(rc, cli.EXIT_USAGE)
    with open(out_pq, 'rb') as f:
      self.assertEqual(f.read(), original_bytes)
    self.assertEqual(pq.read_table(out_pq).to_pylist(), expected_rows)

    # 5. Clean --if-exists replace succeeds and swaps in the new output cleanly.
    rc, env, err = _run_json(
        ['pipe', '--from', good_csv, '--to', out_pq, '--if-exists', 'replace'],
        extra_env={'BEAM_CLI_NO_FAST_PATH': '1'})
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(
        sorted(pq.read_table(out_pq).to_pylist(), key=lambda r: r['id']),
        [{'id': 10, 'amount': 12.5}, {'id': 11, 'amount': 50.0}])
    # No leftover .beam_replace_* staging directories.
    leftovers = [
        n for n in os.listdir(self.tmp) if n.startswith('.beam_replace_')
    ]
    self.assertEqual(leftovers, [])

  def test_late_json_fields_after_100_records_never_dropped_silently(self):
    src_jsonl = os.path.join(self.tmp, 'events.jsonl')
    with open(src_jsonl, 'w', encoding='utf-8') as f:
      for i in range(1, 121):
        rec = {'id': i, 'user': {'uid': f'u{i}'}}
        if i == 105:
          rec['coupon'] = 'SAVE10'
        elif i == 106:
          rec['user']['badge'] = 'vip'
        f.write(json.dumps(rec) + '\n')

    # 0. Small local JSONL (<= 64 MB) auto-scans all rows out of the box (K1):
    #    both `coupon` and `user.badge` are preserved with zero flags!
    for idx, env_vars in enumerate(({}, {'BEAM_CLI_NO_FAST_PATH': '1'})):
      out_auto = os.path.join(self.tmp, f'auto_{idx}.jsonl')
      rc, env, err = _run_json(
          ['pipe', '--from', src_jsonl, '--to', out_auto, '--assert-counts'],
          extra_env=env_vars)
      self.assertEqual(rc, cli.EXIT_OK, err)
      self.assertEqual(env['metrics']['rows_written'], 120)
      with open(out_auto, 'r', encoding='utf-8') as f:
        by_id = {r['id']: r for r in (json.loads(line) for line in f if line)}
      self.assertEqual(by_id[105]['coupon'], 'SAVE10')
      self.assertEqual(by_id[106]['user']['badge'], 'vip')
      self.assertIsNone(by_id[1]['coupon'])

    # Simulate large (>64 MB) / remote bounded-sample JSONL via env cap.
    capped_fast = {'BEAM_CLI_JSON_AUTO_SCAN_MAX_BYTES': '100'}
    capped_runner = {
        'BEAM_CLI_JSON_AUTO_SCAN_MAX_BYTES': '100',
        'BEAM_CLI_NO_FAST_PATH': '1',
    }

    # 1. Under bounded sampling without --dlq: fails fast naming the late field
    #    across fast_local, Beam runner, and --sql!
    for idx, env_vars in enumerate((capped_fast, capped_runner)):
      out_f = os.path.join(self.tmp, f'fail_{idx}.jsonl')
      rc, env, _ = _run_json(['pipe', '--from', src_jsonl, '--to', out_f],
                             extra_env=env_vars)
      self.assertEqual(rc, cli.EXIT_PIPELINE_FAILED)
      self.assertIn('SchemaDriftError', env['error']['message'])
      self.assertIn('coupon', env['error']['message'])
      self.assertFalse(os.path.exists(out_f))

    rc_sql, env_sql, _ = _run_json(
        [
            'pipe', '--from', src_jsonl, '--sql', 'SELECT id FROM source',
            '--to', os.path.join(self.tmp, 'fail_sql.jsonl')
        ],
        extra_env=capped_fast)
    self.assertEqual(rc_sql, cli.EXIT_USAGE)
    self.assertIn('SchemaDriftError', env_sql['error']['message'])
    self.assertIn('coupon', env_sql['error']['message'])

    # 2. With --dlq under bounded sampling: quarantines rows 105 & 106 with
    #    full raw_payload AND flags warnings so an agent never misses it!
    for idx, env_vars in enumerate((capped_fast, capped_runner)):
      out_ok = os.path.join(self.tmp, f'ok_{idx}.jsonl')
      dlq_ok = os.path.join(self.tmp, f'dlq_{idx}.jsonl')
      rc, env, err = _run_json(
          [
              'pipe', '--from', src_jsonl, '--to', out_ok, '--dlq', dlq_ok,
              '--assert-counts'
          ],
          extra_env=env_vars)
      self.assertEqual(rc, cli.EXIT_OK, err)
      self.assertEqual(env['metrics']['rows_read'], 120)
      self.assertEqual(env['metrics']['rows_written'], 118)
      self.assertEqual(env['metrics']['dlq_rows'], 2)
      self.assertTrue(
          any('quarantined to --dlq' in w for w in env['warnings']),
          env['warnings'])
      with open(dlq_ok, 'r', encoding='utf-8') as f:
        dlq_rows = [json.loads(line) for line in f if line.strip()]
      self.assertEqual(len(dlq_rows), 2)
      self.assertTrue(
          all(r['error_type'] == 'SchemaDriftError' for r in dlq_rows))
      payloads = ' '.join(r['raw_payload'] for r in dlq_rows)
      self.assertIn('SAVE10', payloads)
      self.assertIn('vip', payloads)

    # Also verify --sql with --dlq quarantines & warns on late JSON fields.
    out_sql_ok = os.path.join(self.tmp, 'sql_ok.jsonl')
    dlq_sql_ok = os.path.join(self.tmp, 'sql_dlq.jsonl')
    rc, env, err = _run_json(
        [
            'pipe', '--from', src_jsonl, '--sql', 'SELECT id FROM source',
            '--to', out_sql_ok, '--dlq', dlq_sql_ok
        ],
        extra_env=capped_fast)
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(env['metrics']['rows_written'], 118)
    self.assertEqual(env['metrics']['dlq_rows'], 2)
    self.assertTrue(
        any('quarantined to --dlq' in w for w in env['warnings']),
        env['warnings'])

    # 3. --from-opt sample_rows=all overrides the cap and preserves all 120 rows!
    for idx, env_vars in enumerate((capped_fast, capped_runner)):
      out_all = os.path.join(self.tmp, f'all_{idx}.jsonl')
      rc, env, err = _run_json(
          [
              'pipe', '--from', src_jsonl, '--to', out_all, '--from-opt',
              'sample_rows=all', '--assert-counts'
          ],
          extra_env=env_vars)
      self.assertEqual(rc, cli.EXIT_OK, err)
      self.assertEqual(env['metrics']['rows_written'], 120)
      with open(out_all, 'r', encoding='utf-8') as f:
        by_id = {r['id']: r for r in (json.loads(line) for line in f if line)}
      self.assertEqual(by_id[105]['coupon'], 'SAVE10')
      self.assertEqual(by_id[106]['user']['badge'], 'vip')
      self.assertIsNone(by_id[1]['coupon'])

    # 4. --from-opt on_unknown_fields=ignore intentionally drops unknown fields.
    out_ign = os.path.join(self.tmp, 'ign.jsonl')
    rc, env, err = _run_json(
        [
            'pipe', '--from', src_jsonl, '--to', out_ign, '--from-opt',
            'on_unknown_fields=ignore', '--assert-counts'
        ],
        extra_env=capped_fast)
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(env['metrics']['rows_written'], 120)
    self.assertEqual(env['warnings'], [])


class Round10FixesTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-cli-r10-')

  def tearDown(self):
    shutil.rmtree(self.tmp, ignore_errors=True)

  def test_local_fast_cdc_with_seq_ts_autodetect_and_select(self):
    """K2: `--cdc` runs on `local_fast`, auto-detects `seq_ts`, & composes with `--select`."""
    import pyarrow.parquet as pq

    cdc_jsonl = os.path.join(self.tmp, 'sub_cdc.jsonl')
    lines = [
        json.dumps({'sub_id': 1, 'plan': 'basic', 'mrr': 19.0, 'op': 'UPSERT', 'seq_ts': '2026-11-01T10:00:00Z'}),
        json.dumps({'sub_id': 1, 'plan': 'pro', 'mrr': 49.0, 'op': 'UPSERT', 'seq_ts': '2026-11-02T10:00:00Z'}),
        json.dumps({'sub_id': 2, 'plan': 'team', 'mrr': 99.0, 'op': 'UPSERT', 'seq_ts': '2026-11-03T08:00:00Z'}),
        json.dumps({'sub_id': 2, 'plan': 'team', 'mrr': 99.0, 'op': 'UPSERT', 'seq_ts': '2026-11-03T08:00:00Z'}),
        json.dumps({'sub_id': 3, 'plan': 'basic', 'mrr': 19.0, 'op': 'UPSERT', 'seq_ts': '2026-11-01T08:00:00Z'}),
        json.dumps({'sub_id': 3, 'plan': 'basic', 'mrr': 19.0, 'op': 'DELETE', 'seq_ts': '2026-11-02T08:00:00Z'}),
        '{"sub_id": 99, "broken": ',
    ]
    with open(cdc_jsonl, 'w', encoding='utf-8') as f:
      f.write('\n'.join(lines) + '\n')

    for idx, env_vars in enumerate(({}, {'BEAM_CLI_NO_FAST_PATH': '1'})):
      out_pq = os.path.join(self.tmp, f'live_{idx}.parquet')
      dlq_jl = os.path.join(self.tmp, f'dlq_{idx}.jsonl')
      rc, env, err = _run_json(
          [
              'pipe', '--from', cdc_jsonl, '--to', out_pq, '--dlq', dlq_jl,
              '--cdc', '--primary-key', 'sub_id', '--select',
              'sub_id, plan, mrr', '--assert-counts'
          ],
          extra_env=env_vars)
      self.assertEqual(rc, cli.EXIT_OK, err)
      if not env_vars:
        self.assertEqual(env['runner'], 'local_fast')
      self.assertEqual(env['metrics']['rows_read'], 7)
      self.assertEqual(env['metrics']['rows_written'], 2)
      self.assertEqual(env['metrics']['rows_filtered_out'], 4)
      self.assertEqual(env['metrics']['dlq_rows'], 1)
      rows = sorted(pq.read_table(out_pq).to_pylist(), key=lambda r: r['sub_id'])
      self.assertEqual(
          rows,
          [
              {'sub_id': 1, 'plan': 'pro', 'mrr': 49.0},
              {'sub_id': 2, 'plan': 'team', 'mrr': 99.0},
          ])

  def test_comma_from_hint_and_sql_exact_percentile_and_duckdb_path_hint(self):
    """K3 + K5: comma `--from` & DuckDB file-path hints; exact percentiles."""
    a_csv = os.path.join(self.tmp, 'a.csv')
    b_csv = os.path.join(self.tmp, 'b.csv')
    with open(a_csv, 'w', encoding='utf-8') as f:
      f.write('latency_ms\n10\n12\n15\n20\n25\n30\n45\n60\n80\n200\n')
    with open(b_csv, 'w', encoding='utf-8') as f:
      f.write('latency_ms\n50\n50\n60\n200\n')

    # Comma-separated --from exits 2 cleanly without YAML line numbers.
    rc, env, _ = _run_json([
        'pipe', '--from', f'{a_csv},{b_csv}', '--to',
        os.path.join(self.tmp, 'out.csv')
    ])
    self.assertEqual(rc, cli.EXIT_USAGE)
    self.assertIn('comma-separated paths in --from', env['error']['message'])
    self.assertIn('UNION ALL', env['error']['message'])

    # `FROM source` works AND percentile returns exact 146.0 (no 145.9999999999999).
    rc_a, env_a, err_a = _run_json([
        'pipe', '--from', a_csv, '--sql',
        'SELECT median(latency_ms) AS med, percentile(latency_ms, 95) AS p95 FROM source'
    ])
    self.assertEqual(rc_a, cli.EXIT_OK, err_a)
    row_a = env_a['untrusted_data']['rows'][0]
    self.assertEqual(row_a['med'], 27.5)
    self.assertEqual(row_a['p95'], 146.0)

    rc_b, env_b, err_b = _run_json([
        'pipe', '--from', b_csv, '--sql',
        'SELECT percentile(latency_ms, 90) AS p90 FROM source'
    ])
    self.assertEqual(rc_b, cli.EXIT_OK, err_b)
    self.assertEqual(env_b['untrusted_data']['rows'][0]['p90'], 158.0)

  def test_diff_explicit_column_subset(self):
    """K4: `beam diff --columns col1,col2` compares only the requested subset."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    left_pq = os.path.join(self.tmp, 'left.parquet')
    right_pq = os.path.join(self.tmp, 'right.parquet')
    pq.write_table(
        pa.Table.from_pylist([
            {'item_id': 'I1', 'qty': 10, 'price': 5.0, 'updated_at': 'old'},
            {'item_id': 'I2', 'qty': 3, 'price': 2.0, 'updated_at': 'old'},
        ]),
        left_pq)
    pq.write_table(
        pa.Table.from_pylist([
            {'item_id': 'I1', 'qty': 10, 'price': 5.0, 'updated_at': 'new', 'extra': 1},
            {'item_id': 'I2', 'qty': 3, 'price': 2.0, 'updated_at': 'new', 'extra': 2},
        ]),
        right_pq)

    # Comparing only `qty,price` ignores `updated_at` and `extra` -> match!
    rc, env, err = _run_json([
        'diff', left_pq, right_pq, '--key', 'item_id', '--columns',
        'qty,price'
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertTrue(env['outputs']['match'])
    self.assertEqual(env['outputs']['summary']['matched_rows'], 2)
    self.assertEqual(
        env['outputs']['schema_diff']['compared_columns'], ['qty', 'price'])

    # Unknown column in --columns exits 2 cleanly.
    rc_bad, env_bad, _ = _run_json([
        'diff', left_pq, right_pq, '--key', 'item_id', '--columns',
        'qty,no_such_col'
    ])
    self.assertEqual(rc_bad, cli.EXIT_USAGE)
    self.assertIn('no_such_col', env_bad['error']['message'])


class Round11FixesTest(unittest.TestCase):
  """Regression tests for Round 11 simulation findings (L1-L3)."""

  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-r11-test-')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

  def test_sqlite_cast_as_string_preserves_text_values(self):
    """L1: `CAST(col AS STRING)` maps to SQLite `TEXT` instead of `NUMERIC` 0."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    src = os.path.join(self.tmp, 'users.parquet')
    out = os.path.join(self.tmp, 'users_out.parquet')
    pq.write_table(
        pa.Table.from_pylist([
            {'id': 1, 'user': 'u1', 'note': 'keep CAST(x AS STRING) literal'},
            {'id': 2, 'user': 'u2', 'note': 'ok'},
        ]),
        src)
    rc, env, err = _run_json([
        'pipe',
        '--from',
        f't={src}',
        '--sql',
        (
            "SELECT CAST(id AS INT64) AS id, CAST(user AS STRING) AS user, "
            "CAST(NULL AS STRING) AS tier, 'AS STRING)' AS lit "
            'FROM t ORDER BY id'
        ),
        '--to',
        out,
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    rows = pq.read_table(out).to_pylist()
    self.assertEqual(rows, [
        {'id': 1, 'user': 'u1', 'tier': None, 'lit': 'AS STRING)'},
        {'id': 2, 'user': 'u2', 'tier': None, 'lit': 'AS STRING)'},
    ])

  def test_sql_on_delta_and_iceberg_and_quoted_uri_table_refs(self):
    """L2: `beam pipe --sql` reads `delta://` & `iceberg://` and quoted URI refs."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    ddir = os.path.join(self.tmp, 'wh_delta')
    log_dir = os.path.join(ddir, '_delta_log')
    os.makedirs(log_dir, exist_ok=True)
    p0 = os.path.join(ddir, 'part-0.parquet')
    pq.write_table(
        pa.Table.from_pylist([
            {'sku': 'S1', 'qty': 4, 'cost': 10.0},
            {'sku': 'S2', 'qty': 2, 'cost': 25.0},
        ]),
        p0)
    delta_schema = {
        'type': 'struct',
        'fields': [
            {'name': 'sku', 'type': 'string', 'nullable': False, 'metadata': {}},
            {'name': 'qty', 'type': 'long', 'nullable': True, 'metadata': {}},
            {'name': 'cost', 'type': 'double', 'nullable': True, 'metadata': {}},
        ],
    }
    with open(os.path.join(log_dir, '00000000000000000000.json'), 'w', encoding='utf-8') as f:
      f.write(json.dumps({'protocol': {'minReaderVersion': 1, 'minWriterVersion': 2}}) + '\n')
      f.write(json.dumps({
          'metaData': {
              'id': 'd1',
              'format': {'provider': 'parquet', 'options': {}},
              'schemaString': json.dumps(delta_schema),
              'partitionColumns': [],
          }
      }) + '\n')
      f.write(json.dumps({
          'add': {'path': 'part-0.parquet', 'size': os.path.getsize(p0), 'modificationTime': 1, 'dataChange': True}
      }) + '\n')

    delta_uri = f'delta://{ddir}'
    rc, env, err = _run_json([
        'pipe',
        '--from',
        delta_uri,
        '--sql',
        f'SELECT COUNT(*) AS n, SUM(qty * cost) AS val FROM "{delta_uri}"',
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(env['untrusted_data']['rows'], [{'n': 2, 'val': 90.0}])

    # Register into Iceberg v2 and query Iceberg directly with --sql too!
    wh = os.path.join(self.tmp, 'ice_wh')
    ice_uri = f'iceberg://ops.inv?warehouse={wh}'
    rc_zc, _, err_zc = _run_json(
        ['pipe', '--from', delta_uri, '--to', ice_uri, '--zero-copy'])
    self.assertEqual(rc_zc, cli.EXIT_OK, err_zc)

    rc_ice, env_ice, err_ice = _run_json([
        'pipe',
        '--from',
        ice_uri,
        '--sql',
        f'SELECT COUNT(*) AS n, SUM(qty * cost) AS val FROM "{ice_uri}"',
    ])
    self.assertEqual(rc_ice, cli.EXIT_OK, err_ice)
    self.assertEqual(env_ice['untrusted_data']['rows'], [{'n': 2, 'val': 90.0}])

  def test_diff_key_limit_all_and_regr_slope_duckdb_hint(self):
    """L3: `beam diff --limit all` returns >100 differing keys; `regr_slope` hints DuckDB."""
    left_csv = os.path.join(self.tmp, 'l.csv')
    right_csv = os.path.join(self.tmp, 'r.csv')
    with open(left_csv, 'w', encoding='utf-8') as f:
      f.write('id,v\n')
      for i in range(135):
        f.write(f'K{i:03d},1\n')
    with open(right_csv, 'w', encoding='utf-8') as f:
      f.write('id,v\n')

    rc_def, env_def, _ = _run_json(
        ['diff', left_csv, right_csv, '--key', 'id', '--exit-zero'])
    self.assertEqual(rc_def, cli.EXIT_OK)
    self.assertEqual(len(env_def['outputs']['differing_keys']['only_in_left']), 100)
    self.assertTrue(env_def['outputs']['differing_keys']['truncated'])

    rc_all, env_all, err_all = _run_json([
        'diff', left_csv, right_csv, '--key', 'id', '--limit', 'all', '--exit-zero'
    ])
    self.assertEqual(rc_all, cli.EXIT_OK, err_all)
    self.assertEqual(len(env_all['outputs']['differing_keys']['only_in_left']), 135)
    self.assertFalse(env_all['outputs']['differing_keys']['truncated'])

    # regr_slope is now natively supported on default SQLite:
    rc_regr, env_regr, err_regr = _run_json([
        'pipe', '--from', left_csv, '--sql', 'SELECT regr_slope(v, v) FROM source'
    ])
    self.assertEqual(rc_regr, cli.EXIT_OK, err_regr)

    # Advanced warehouse aggregates like arg_max still hint DuckDB:
    rc_argmax, env_argmax, _ = _run_json([
        'pipe', '--from', left_csv, '--sql', 'SELECT arg_max(v, v) FROM source'
    ])
    self.assertEqual(rc_argmax, cli.EXIT_USAGE)
    self.assertIn('--sql-engine duckdb', env_argmax['error']['message'])


class Round12FixesTest(unittest.TestCase):
  """Regression tests for Round 12 findings (M1-M3)."""

  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-cli-r12-')

  def tearDown(self):
    shutil.rmtree(self.tmp, ignore_errors=True)

  def test_diff_local_fast_matches_fnapi_runner_and_warns_on_truncation(self):
    """M1 + M2: local_fast diff matches FnApiRunner and warns when differing_keys truncates."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    left_pq = os.path.join(self.tmp, 'left.parquet')
    right_jsonl = os.path.join(self.tmp, 'right.jsonl')
    pq.write_table(
        pa.Table.from_pylist([
            {'id': 'I1', 'qty': 10, 'loc': None},
            {'id': 'I2', 'qty': 5, 'loc': 'A'},
            {'id': 'I3', 'qty': 7, 'loc': 'B'},
        ]),
        left_pq)
    with open(right_jsonl, 'w', encoding='utf-8') as f:
      f.write(json.dumps({'id': 'I1', 'qty': 10}) + '\n')  # missing loc == None
      f.write(json.dumps({'id': 'I2', 'qty': 9, 'loc': 'A'}) + '\n')
      f.write(json.dumps({'id': 'I4', 'qty': 1, 'loc': 'C'}) + '\n')

    rc_fast, env_fast, _ = _run_json(['diff', left_pq, right_jsonl, '--key', 'id'])
    with mock.patch.dict(os.environ, {'BEAM_CLI_NO_FAST_PATH': '1'}):
      rc_runner, env_runner, _ = _run_json(
          ['diff', left_pq, right_jsonl, '--key', 'id'])
    self.assertEqual(rc_fast, cli.EXIT_DIFF_MISMATCH)
    self.assertEqual(rc_runner, cli.EXIT_DIFF_MISMATCH)
    self.assertEqual(env_fast['runner'], 'local_fast')
    self.assertEqual(env_runner['runner'], 'FnApiRunner')
    self.assertEqual(env_fast['outputs']['summary'], env_runner['outputs']['summary'])
    self.assertEqual(
        env_fast['outputs']['differing_keys'],
        env_runner['outputs']['differing_keys'])

    # >100 churned keys triggers explicit truncation warning until --limit all
    big_left = os.path.join(self.tmp, 'big_l.csv')
    big_right = os.path.join(self.tmp, 'big_r.csv')
    with open(big_left, 'w', encoding='utf-8') as f:
      f.write('id,val\n')
      for i in range(120):
        f.write(f'ACC-{i:04d},1\n')
    with open(big_right, 'w', encoding='utf-8') as f:
      f.write('id,val\n')
    rc_trunc, env_trunc, _ = _run_json(
        ['diff', big_left, big_right, '--key', 'id', '--exit-zero'])
    self.assertEqual(rc_trunc, cli.EXIT_OK)
    self.assertTrue(env_trunc['outputs']['differing_keys']['truncated'])
    self.assertTrue(
        any('--limit all' in w for w in env_trunc['warnings']),
        msg=env_trunc['warnings'])

    rc_full, env_full, _ = _run_json([
        'diff', big_left, big_right, '--key', 'id', '--limit', 'all', '--exit-zero'
    ])
    self.assertEqual(rc_full, cli.EXIT_OK)
    self.assertFalse(env_full['outputs']['differing_keys']['truncated'])
    self.assertEqual(env_full['warnings'], [])

  def test_single_input_sql_accepts_common_table_views(self):
    """M3: Single-input --sql accepts FROM data / FROM tbl / FROM input."""
    csv_path = os.path.join(self.tmp, 'items.csv')
    with open(csv_path, 'w', encoding='utf-8') as f:
      f.write('sku,qty\nA,2\nB,3\n')
    for view_name in ('source', 'data', 'tbl', 'input'):
      rc, env, err = _run_json([
          'pipe', '--from', csv_path, '--sql', f'SELECT SUM(qty) AS s FROM {view_name}'
      ])
      self.assertEqual(rc, cli.EXIT_OK, err)
      self.assertEqual(env['untrusted_data']['rows'], [{'s': 5}])

  def test_mixed_format_local_glob_sql(self):
    """Enhancement 1: Mixed-format local globs in pipe_sql."""
    mixed_dir = os.path.join(self.tmp, 'mixed_shards')
    os.makedirs(mixed_dir, exist_ok=True)
    with open(os.path.join(mixed_dir, 'shard1.csv'), 'w', encoding='utf-8') as f:
      f.write('id,category,amount\n1,shoes,25.5\n2,hat,15.0\n')
    with open(os.path.join(mixed_dir, 'shard2.jsonl'), 'w', encoding='utf-8') as f:
      f.write(json.dumps({'id': 3, 'category': 'shoes', 'amount': 40.0}) + '\n')
      f.write(json.dumps({'id': 4, 'category': 'coat', 'amount': 120.0}) + '\n')

    rc, env, err = _run_json([
        'pipe', '--from', f'{mixed_dir}/*',
        '--sql', 'SELECT category, SUM(amount) AS tot FROM PCOLLECTION GROUP BY category ORDER BY tot DESC'
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    rows = env['untrusted_data']['rows']
    self.assertEqual(len(rows), 3)
    self.assertEqual(rows[0], {'category': 'coat', 'tot': 120.0})
    self.assertEqual(rows[1], {'category': 'shoes', 'tot': 65.5})
    self.assertEqual(rows[2], {'category': 'hat', 'tot': 15.0})

    # Test --emit-yaml for mixed formats emits Flatten
    out_buf = io.StringIO()
    with contextlib.redirect_stdout(out_buf):
      rc_yaml = cli.main([
          'pipe', '--from', f'{mixed_dir}/*',
          '--sql', 'SELECT * FROM PCOLLECTION',
          '--emit-yaml'
      ])
    self.assertEqual(rc_yaml, cli.EXIT_OK)
    self.assertIn('Flatten', out_buf.getvalue())

  def test_pipe_windowing_and_timestamp_col(self):
    """Enhancement 2: --window + --timestamp-col on beam pipe & --emit-yaml."""
    out_buf = io.StringIO()
    with contextlib.redirect_stdout(out_buf):
      rc = cli.main([
          'pipe',
          '--from', 'pubsub://projects/p/topics/t',
          '--to', 'bigquery://p:d.t',
          '--timestamp-col', 'event_ts',
          '--window', 'sliding:5m,1m',
          '--emit-yaml'
      ])
    self.assertEqual(rc, cli.EXIT_OK)
    yaml_text = out_buf.getvalue()
    self.assertIn('AssignTimestamps', yaml_text)
    self.assertIn('event_ts', yaml_text)
    self.assertIn('WindowInto', yaml_text)
    self.assertIn('sliding', yaml_text)
    self.assertIn('5m', yaml_text)

  def test_check_warns_untyped_python_map_to_parquet(self):
    """Enhancement 3: Offline warning in beam check for untyped Python preceding WriteToParquet."""
    pipeline_yaml = (
        'pipeline:\n'
        '  transforms:\n'
        '    - type: ReadFromCsv\n'
        '      config: {path: in.csv}\n'
        '    - type: MapToFields\n'
        '      config:\n'
        '        language: python\n'
        '        fields:\n'
        '          val2: "r.val * 2"\n'
        '    - type: WriteToParquet\n'
        '      config: {path: out.parquet}\n'
    )
    p_path = os.path.join(self.tmp, 'untyped_pipe.yaml')
    with open(p_path, 'w', encoding='utf-8') as f:
      f.write(pipeline_yaml)

    rc, env, _ = _run_json(['check', p_path])
    self.assertEqual(rc, cli.EXIT_OK)
    warnings = env.get('warnings') or []
    self.assertTrue(
        any('untyped Python expression' in w and 'WriteToParquet' in w for w in warnings),
        msg=warnings)

  def test_package_flag_aliases_and_case_insensitive_sdk(self):
    """Enhancement 4: Package flag aliases in package.py and main.py."""
    pipeline_yaml = (
        'pipeline:\n'
        '  transforms:\n'
        '    - type: Create\n'
        '      config: {elements: [1, 2, 3]}\n'
    )
    p_path = os.path.join(self.tmp, 'pkg_pipe.yaml')
    with open(p_path, 'w', encoding='utf-8') as f:
      f.write(pipeline_yaml)

    # 1. --sdk YAML with --to
    rc1, env1, err1 = _run_json([
        'package', p_path, '--sdk', 'YAML', '--to', 'gs://bucket/spec.json', '--dry-run'
    ])
    self.assertEqual(rc1, cli.EXIT_OK, err1)

    # 2. --sdk yaml with -o
    rc2, env2, err2 = _run_json([
        'package', p_path, '--sdk', 'yaml', '-o', 'gs://bucket/spec.json', '--dry-run'
    ])
    self.assertEqual(rc2, cli.EXIT_OK, err2)

    # 3. --sdk auto with --output
    rc3, env3, err3 = _run_json([
        'package', p_path, '--sdk', 'auto', '--output', 'gs://bucket/spec.json', '--dry-run'
    ])
    self.assertEqual(rc3, cli.EXIT_OK, err3)

  def test_scrub_error_message_preserves_dataflow_job_id(self):
    from apache_beam.cli import output as out_mod
    df_id = '2026-11-10_14_34_36-14034864922449419400'
    msg = (
        f"pipeline still running after --wait-timeout (5.0s); "
        f"detached remote job '{df_id}' with token "
        f"'super_secret_long_random_credential_string_xyz'")
    scrubbed = out_mod.scrub_error_message(msg)
    self.assertIn(df_id, scrubbed)
    self.assertNotIn('super_secret_long_random_credential_string_xyz', scrubbed)
    self.assertIn('<redacted-value>', scrubbed)

  def test_dataflow_rest_poller_console_url_and_wait_progress(self):
    from unittest import mock
    from apache_beam.cli import jobs as jobs_mod

    df_id = '2026-11-10_14_34_36-14034864922449419400'
    url = jobs_mod.dataflow_console_url(df_id, 'my-proj', 'us-central1')
    self.assertEqual(
        url,
        f'https://console.cloud.google.com/dataflow/jobs/us-central1/{df_id}'
        '?project=my-proj')

    poller = jobs_mod._DataflowPoller('my-proj', 'us-central1')
    responses = [
        {'currentState': 'JOB_STATE_RUNNING'},
        {
            'metrics': [{
                'name': {
                    'origin': 'user',
                    'name': 'rows_read',
                    'context': {'namespace': 'beam_cli'},
                },
                'scalar': {'integerValue': '42'},
            }]
        },
        {'currentState': 'JOB_STATE_DONE'},
        {
            'metrics': [
                {
                    'name': {
                        'origin': 'user',
                        'name': 'rows_read',
                        'context': {'namespace': 'beam_cli'},
                    },
                    'scalar': {'integerValue': '100'},
                },
                {
                    'name': {
                        'origin': 'user',
                        'name': 'pre_sink_rows',
                        'context': {'namespace': 'beam_cli'},
                    },
                    'scalar': {'integerValue': '95'},
                },
            ]
        },
    ]
    with mock.patch.object(
        poller, '_ensure_rest_creds', return_value='fake-tok'), mock.patch.object(
            poller, '_rest_json', side_effect=responses):
      st1, m1 = poller.query(df_id, 'my-proj', 'us-central1')
      self.assertEqual(st1, 'JOB_STATE_RUNNING')
      self.assertEqual(m1['rows_read'], 42)
      st2, m2 = poller.query(df_id, 'my-proj', 'us-central1')
      self.assertEqual(st2, 'JOB_STATE_DONE')
      self.assertEqual(m2['rows_read'], 100)
      self.assertEqual(m2['rows_written'], 95)

    # Verify `wait_for_job(..., show_progress=True)` emits console_url and stderr heartbeats.
    with mock.patch.dict(
        os.environ, {'BEAM_HOME': os.path.join(self.tmp, 'beam_home')}):
      jobs_mod.record_job_submission(
          df_id,
          runner='DataflowRunner',
          state='JOB_STATE_RUNNING',
          command='pipe',
          project='my-proj',
          region='us-central1',
          outputs={'destination': 'bigquery://my-proj.ds.tbl'})
      poll_states = iter([
          (
              'JOB_STATE_RUNNING',
              {
                  'rows_read': 10,
                  'rows_written': 0,
                  'rows_filtered_out': 0,
                  'dlq_rows': 0,
              },
          ),
          (
              'JOB_STATE_DONE',
              {
                  'rows_read': 10,
                  'rows_written': 10,
                  'rows_filtered_out': 0,
                  'dlq_rows': 0,
              },
          ),
      ])
      err_buf = io.StringIO()
      with contextlib.redirect_stderr(err_buf):
        rc, rec = jobs_mod.wait_for_job(
            df_id,
            poll_interval=0.01,
            query_fn=lambda *_a: next(poll_states),
            show_progress=True)
    self.assertEqual(rc, cli.EXIT_OK)
    self.assertEqual(rec['outputs']['destination'], 'bigquery://my-proj.ds.tbl')
    self.assertIn('console.cloud.google.com/dataflow/jobs', rec['outputs']['console_url'])
    self.assertIn('[beam] Job console:', err_buf.getvalue())
    self.assertIn('JOB_STATE_DONE', err_buf.getvalue())

  def test_io_describe_text_and_json_key_option(self):
    rc_t, env_t, err_t = _run_json(['io', 'describe', 'text'])
    self.assertEqual(rc_t, cli.EXIT_OK, err_t)
    conn_t = env_t['outputs']['connector']
    self.assertEqual(conn_t['read_transform'], 'ReadFromText')
    self.assertEqual(conn_t['write_transform'], 'WriteToText')

    rc_j, env_j, err_j = _run_json(['io', 'describe', 'json'])
    self.assertEqual(rc_j, cli.EXIT_OK, err_j)
    conn_j = env_j['outputs']['connector']
    self.assertIn('key', conn_j['options'])

  def test_sqlite_boolean_string_literal_comparison_parity(self):
    shipments = os.path.join(self.tmp, 'shipments.csv')
    with open(shipments, 'w', encoding='utf-8') as f:
      f.write(
          'id,express,status\n'
          '1,true,true\n'
          '2,false,active\n'
          '3,TRUE,false\n')
    rc, env, err = _run_json([
        'pipe',
        '--from',
        f's={shipments}',
        '--sql',
        (
            "SELECT s.id, CASE WHEN s.express = 'true' THEN 10 ELSE 0 END AS fee "
            "FROM s WHERE express != 'false' AND status != 'active' ORDER BY s.id"
        ),
    ])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(
        env['untrusted_data']['rows'],
        [{'id': 1, 'fee': 10}, {'id': 3, 'fee': 10}])

  def test_portable_flag_and_portable_to_runner_metadata(self):
    src = os.path.join(self.tmp, 'sales.csv')
    with open(src, 'w', encoding='utf-8') as f:
      f.write('region,amount\nUS,10.0\nUS,30.0\nEU,20.0\n')

    # 1. Local-only `quantile()` runs locally with `portable_to_runner == False`.
    rc1, env1, err1 = _run_json([
        'pipe',
        '--from',
        f's={src}',
        '--sql',
        'SELECT quantile(amount, 0.5) AS med FROM s',
    ])
    self.assertEqual(rc1, cli.EXIT_OK, err1)
    self.assertFalse(env1['outputs']['portable_to_runner'])
    self.assertIn('quantile()', env1['outputs']['non_portable_constructs'])

    # 2. `--portable` rejects local-only SQL functions with EXIT_USAGE.
    rc2, env2, _ = _run_json([
        'pipe',
        '--from',
        f's={src}',
        '--sql',
        'SELECT quantile(amount, 0.5) AS med FROM s',
        '--portable',
    ])
    self.assertEqual(rc2, cli.EXIT_USAGE)
    self.assertIn('quantile()', env2['error']['message'])

    # 3. Standard Calcite-compatible SQL passes `--portable` (and ignores string literals).
    rc3, env3, err3 = _run_json([
        'pipe',
        '--from',
        f's={src}',
        '--sql',
        "SELECT region, COUNT(*) AS n, 'quantile(x)' AS note FROM s GROUP BY 1 ORDER BY 1",
        '--portable',
    ])
    self.assertEqual(rc3, cli.EXIT_OK, err3)
    self.assertTrue(env3['outputs']['portable_to_runner'])

    # 4. `--portable` rejects local-only `--to-opt key=` on non-SQL pipe.
    out_json = os.path.join(self.tmp, 'keyed.json')
    rc4, _, _ = _run_json([
        'pipe',
        '--from',
        src,
        '--to',
        out_json,
        '--to-opt',
        'key=region',
        '--portable',
    ])
    self.assertEqual(rc4, cli.EXIT_USAGE)

  def test_doctor_detects_beam_path_collision_and_beam_client(self):
    from unittest import mock
    from apache_beam.cli import doctor as doc_mod

    shadow_dir = os.path.join(self.tmp, 'shadow_bin')
    os.makedirs(shadow_dir, exist_ok=True)
    shadow_beam = os.path.join(shadow_dir, 'beam')
    with open(shadow_beam, 'w', encoding='utf-8') as f:
      f.write('#!/bin/sh\necho beam-cloud\n')
    os.chmod(shadow_beam, 0o755)

    new_path = shadow_dir + os.pathsep + os.environ.get('PATH', '')
    with mock.patch.dict(
        os.environ, {
            'PATH': new_path, 'BEAM_HOME': os.path.join(self.tmp, 'bh')
        }), mock.patch.object(
            doc_mod,
            '_package_version',
            side_effect=lambda n: '0.9.1' if n == 'beam-client' else None):
      rc, env, err = _run_json(['doctor'])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertTrue(env['outputs']['binary_resolution']['beam_client_installed'])
    self.assertIn('deltalake', env['outputs']['extras'])
    self.assertIn('pyiceberg', env['outputs']['extras'])
    warns = ' | '.join(env.get('warnings') or [])
    self.assertIn('beam-client', warns)
    self.assertIn('apache-beam', warns)
    self.assertEqual(
        env['next_actions'][0]['command'], 'apache-beam --version --json')

  def test_lakehouse_delta_v3_and_iceberg_v3_protocol_guards(self):
    from unittest import mock
    import pyarrow as pa
    import pyarrow.parquet as pq
    import types
    from apache_beam.cli import lakehouse as lh

    # 1. Delta protocol v3 (`deletionVectors`): refuses raw Parquet read when
    # `deltalake` is absent, and delegates cleanly when `deltalake` is installed.
    delta_dir = os.path.join(self.tmp, 'delta_dv')
    os.makedirs(os.path.join(delta_dir, '_delta_log'), exist_ok=True)
    pq_file = os.path.join(delta_dir, 'part-00000.parquet')
    pq.write_table(
        pa.Table.from_pylist([{'id': 1, 'val': 'live'}, {'id': 2, 'val': 'deleted_by_dv'}]),
        pq_file)
    schema_json = json.dumps({
        'type': 'struct',
        'fields': [
            {'name': 'id', 'type': 'long', 'nullable': True, 'metadata': {}},
            {'name': 'val', 'type': 'string', 'nullable': True, 'metadata': {}},
        ],
    })
    commit0 = os.path.join(delta_dir, '_delta_log', '00000000000000000000.json')
    with open(commit0, 'w', encoding='utf-8') as f:
      f.write(
          json.dumps({
              'protocol': {
                  'minReaderVersion': 3,
                  'minWriterVersion': 7,
                  'readerFeatures': ['deletionVectors'],
                  'writerFeatures': ['deletionVectors'],
              }
          }) + '\n')
      f.write(
          json.dumps({
              'metaData': {
                  'id': 't1',
                  'schemaString': schema_json,
                  'partitionColumns': [],
                  'configuration': {'delta.enableDeletionVectors': 'true'},
              }
          }) + '\n')
      f.write(
          json.dumps({
              'add': {
                  'path': 'part-00000.parquet',
                  'size': os.path.getsize(pq_file),
                  'modificationTime': 1700000000000,
                  'dataChange': True,
              }
          }) + '\n')

    with mock.patch.dict(sys.modules, {'deltalake': None}):
      rc_d, env_d, _ = _run_json(['inspect', f'delta://{delta_dir}'])
    self.assertEqual(rc_d, cli.EXIT_MISSING_PACKAGE)
    self.assertIn('deletionVectors', env_d['error']['message'])
    self.assertIn('deltalake', env_d['error']['message'])

    fake_dt_mod = types.ModuleType('deltalake')

    class _FakeDeltaTable:
      def __init__(self, root, **_kw):
        self.root = root

      def to_pyarrow_table(self):
        return pa.Table.from_pylist([{'id': 1, 'val': 'live'}])

    fake_dt_mod.DeltaTable = _FakeDeltaTable
    with mock.patch.dict(sys.modules, {'deltalake': fake_dt_mod}):
      rc_ok, env_ok, err_ok = _run_json(['inspect', f'delta://{delta_dir}'])
    self.assertEqual(rc_ok, cli.EXIT_OK, err_ok)
    self.assertEqual(env_ok['outputs']['row_count'], 1)
    self.assertEqual(env_ok['untrusted_data']['rows'], [{'id': 1, 'val': 'live'}])

    # 2. Iceberg format-version 3 fails closed with EXIT_MISSING_PACKAGE.
    ice3_dir = os.path.join(self.tmp, 'wh', 'ns', 'ice_v3')
    os.makedirs(os.path.join(ice3_dir, 'metadata'), exist_ok=True)
    with open(
        os.path.join(ice3_dir, 'metadata', 'v1.metadata.json'),
        'w',
        encoding='utf-8') as f:
      json.dump({
          'format-version': 3,
          'table-uuid': 'u-3',
          'location': ice3_dir,
          'current-schema-id': 0,
          'schemas': [{
              'type': 'struct',
              'schema-id': 0,
              'fields': [{'id': 1, 'name': 'id', 'required': False, 'type': 'long'}],
          }],
      }, f)
    rc_i3, env_i3, _ = _run_json([
        'inspect', f'iceberg://ns.ice_v3?warehouse={os.path.join(self.tmp, "wh")}'
    ])
    self.assertEqual(rc_i3, cli.EXIT_MISSING_PACKAGE)
    self.assertIn('format-version 3', env_i3['error']['message'])
    self.assertIn('pyiceberg', env_i3['error']['message'])

    # 3. Partitioned existing Iceberg v2 table refuses unpartitioned local mutation.
    ice_part_dir = os.path.join(self.tmp, 'wh', 'ns', 'ice_part')
    os.makedirs(os.path.join(ice_part_dir, 'metadata'), exist_ok=True)
    with open(
        os.path.join(ice_part_dir, 'metadata', 'v1.metadata.json'),
        'w',
        encoding='utf-8') as f:
      json.dump({
          'format-version': 2,
          'table-uuid': 'u-part',
          'location': ice_part_dir,
          'last-sequence-number': 0,
          'current-schema-id': 0,
          'schemas': [{
              'type': 'struct',
              'schema-id': 0,
              'fields': [{'id': 1, 'name': 'id', 'required': False, 'type': 'long'}],
          }],
          'default-spec-id': 0,
          'partition-specs': [{
              'spec-id': 0,
              'fields': [{
                  'source-id': 1,
                  'field-id': 1000,
                  'name': 'id_bucket',
                  'transform': 'bucket[16]',
              }],
          }],
      }, f)
    with self.assertRaises(lh.LakehousePackageMissingError):
      lh.commit_iceberg_snapshot(
          ice_part_dir,
          iceberg_fields=[{'id': 1, 'name': 'id', 'type': 'long', 'nullable': True}],
          new_data_files=[])

  def test_launcher_skips_wheel_staging_on_custom_container_or_flag(self):
    from unittest import mock
    import apache_beam_cli_preview.launcher as launcher

    base_args = ['--runner=DataflowRunner', '--project=p']
    custom_img = base_args + ['--sdk_container_image=gcr.io/p/custom:latest']
    self.assertEqual(
        launcher.maybe_inject_dataflow_worker_args(custom_img), custom_img)

    no_stage = base_args + ['--no-stage-cli-wheel']
    self.assertEqual(
        launcher.maybe_inject_dataflow_worker_args(no_stage), base_args)

    with mock.patch.dict(os.environ, {'BEAM_NO_STAGE_CLI_WHEEL': '1'}):
      self.assertEqual(
          launcher.maybe_inject_dataflow_worker_args(base_args), base_args)


if __name__ == '__main__':
  unittest.main()

