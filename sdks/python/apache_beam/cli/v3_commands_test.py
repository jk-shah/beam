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
    rc, env, err, out = self._pipe(src)
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertEqual(env['metrics']['rows_written'], 2)
    self.assertEqual(env['metrics']['cdc_superseded_rows'], 2)
    rows = sorted(pq.read_table(out).to_pylist(), key=lambda r: r['id'])
    self.assertEqual(rows, [
        {'id': 1, 'status': 'suspended', 'updated_at': '2026-11-03T08:00:00'},
        {'id': 2, 'status': 'active', 'updated_at': '2026-11-01T09:00:00'},
    ])

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
    self.assertEqual(env['untrusted_data']['rows'], expected_agg)
    with open(out_json, encoding='utf-8') as f:
      written_rows = [json.loads(line) for line in f if line.strip()]
    self.assertEqual(written_rows, expected_agg)

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


if __name__ == '__main__':
  unittest.main()


