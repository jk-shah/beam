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

"""Hermetic tests for B2.2 Zero-JVM Pure-Python SQL Lowering."""

from __future__ import annotations

import glob
import io
import json
import os
import shutil
import tempfile
import unittest
from contextlib import redirect_stderr
from contextlib import redirect_stdout
from unittest import mock

import yaml

from apache_beam.cli import main as cli
from apache_beam.cli import pipe_sql
from apache_beam.cli import sql as sql_mod


def _run_cli(argv: list[str]) -> tuple[int, dict, str]:
  out = io.StringIO()
  err = io.StringIO()
  with redirect_stdout(out), redirect_stderr(err):
    rc = cli.main(argv + ['--json'])
  raw = out.getvalue().strip()
  env = json.loads(raw) if raw else {}
  return rc, env, err.getvalue()


def _read_jsonl_dir_or_prefix(prefix_or_file: str) -> list[dict]:
  paths = sorted(glob.glob(prefix_or_file + '*'))
  if not paths and os.path.isfile(prefix_or_file):
    paths = [prefix_or_file]
  rows: list[dict] = []
  for p in paths:
    if os.path.isdir(p):
      continue
    with open(p, encoding='utf-8') as f:
      for line in f:
        line = line.strip()
        if line:
          rows.append(json.loads(line))
  return rows


class PythonSqlLoweringUnitTest(unittest.TestCase):
  """Unit tests for AST lowering into pure-Python Beam YAML transforms."""

  def test_tumble_hop_session_tvf_plan_shapes(self):
    tumble_plan = pipe_sql.try_lower_sql_to_python_yaml(
        "SELECT window_start, window_end, region, COUNT(*) AS cnt, "
        "SUM(amount) AS total "
        "FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(event_ts), "
        "INTERVAL '5' MINUTE)) "
        "WHERE amount > 0 "
        "GROUP BY window_start, window_end, region "
        "HAVING COUNT(*) >= 2",
        ['events'],
        strict=True,
    )
    self.assertIsNotNone(tumble_plan)
    types = [t['type'] for t in tumble_plan.transforms]
    self.assertEqual(
        types,
        [
            'CliAssignTimestamps',
            'Filter',
            'WindowInto',
            'Combine',
            'CliExtractPaneInfo',
            'Filter',
            'MapToFields',
        ],
    )
    win_cfg = tumble_plan.transforms[2]['config']['windowing']
    self.assertEqual(win_cfg, {'type': 'fixed', 'size': '300s'})

    hop_plan = pipe_sql.try_lower_sql_to_python_yaml(
        "SELECT region, SUM(amount) AS total "
        "FROM TABLE(HOP(TABLE events, DESCRIPTOR(ts), "
        "INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)) "
        "GROUP BY window_start, window_end, region",
        ['events'],
        strict=True,
    )
    self.assertIsNotNone(hop_plan)
    win_hop = [
        t for t in hop_plan.transforms if t['type'] == 'WindowInto'
    ][0]['config']['windowing']
    self.assertEqual(
        win_hop, {'type': 'sliding', 'period': '60s', 'size': '300s'})

    sess_plan = pipe_sql.try_lower_sql_to_python_yaml(
        "SELECT user_id, COUNT(*) AS clicks "
        "FROM TABLE(SESSION(TABLE events, DESCRIPTOR(ts), "
        "INTERVAL '30' SECOND)) "
        "GROUP BY user_id, window_start, window_end",
        ['events'],
        strict=True,
    )
    self.assertIsNotNone(sess_plan)
    win_sess = [
        t for t in sess_plan.transforms if t['type'] == 'WindowInto'
    ][0]['config']['windowing']
    self.assertEqual(win_sess, {'type': 'sessions', 'gap': '30s'})

  def test_legacy_tumble_group_by_function_lowering(self):
    plan = pipe_sql.try_lower_sql_to_python_yaml(
        "SELECT TUMBLE_START(event_ts, INTERVAL '10' MINUTE) AS ws, "
        "TUMBLE_END(event_ts, INTERVAL '10' MINUTE) AS we, "
        "region, COUNT(*) AS n "
        "FROM events "
        "GROUP BY TUMBLE(event_ts, INTERVAL '10' MINUTE), region",
        ['events'],
        strict=True,
    )
    self.assertIsNotNone(plan)
    win_stage = [t for t in plan.transforms if t['type'] == 'WindowInto'][0]
    self.assertEqual(
        win_stage['config']['windowing'], {'type': 'fixed', 'size': '600s'})

  def test_strict_rejects_unsupported_sql_while_non_strict_returns_none(self):
    limit_plan = pipe_sql.try_lower_sql_to_python_yaml(
        "SELECT region FROM events LIMIT 5", ['events'], strict=True)
    self.assertIsNotNone(limit_plan)
    self.assertTrue(
        any(t['type'] == 'CliLimitRows' for t in limit_plan.transforms))

    unsupported_queries = [
        "SELECT region, ROW_NUMBER() OVER (PARTITION BY region ORDER BY ts) "
        "FROM events",
        "SELECT * FROM events ORDER BY amount DESC",
        "SELECT * FROM events LIMIT 5 OFFSET 2",
        "SELECT * FROM events UNION ALL SELECT * FROM events",
        "SELECT DISTINCT * FROM events",
        "SELECT 1; SELECT 2",
    ]
    for query in unsupported_queries:
      self.assertIsNone(
          pipe_sql.try_lower_sql_to_python_yaml(
              query, ['events'], strict=False),
          query,
      )
      with self.assertRaises(ValueError, msg=query):
        pipe_sql.try_lower_sql_to_python_yaml(query, ['events'], strict=True)


class PythonSqlLoweringEndToEndTest(unittest.TestCase):
  """End-to-end CLI tests verifying zero-JVM SQL execution."""

  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-py-sql-')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.events_path = os.path.join(self.tmp, 'events.jsonl')
    rows = [
        {
            'event_ts': '2026-05-01T12:01:00Z',
            'region': 'US',
            'user_id': 'u1',
            'status': 'OK',
            'amount': 10.0,
        },
        {
            'event_ts': '2026-05-01T12:02:30Z',
            'region': 'US',
            'user_id': 'u2',
            'status': 'OK',
            'amount': 20.0,
        },
        {
            'event_ts': '2026-05-01T12:03:00Z',
            'region': 'US',
            'user_id': 'u1',
            'status': 'BAD',
            'amount': 999.0,
        },
        {
            'event_ts': '2026-05-01T12:04:00Z',
            'region': 'EU',
            'user_id': 'u3',
            'status': 'OK',
            'amount': 5.0,
        },
        {
            'event_ts': '2026-05-01T12:06:15Z',
            'region': 'EU',
            'user_id': 'u4',
            'status': 'OK',
            'amount': 40.0,
        },
        {
            'event_ts': '2026-05-01T12:07:45Z',
            'region': 'EU',
            'user_id': 'u4',
            'status': 'OK',
            'amount': 60.0,
        },
    ]
    with open(self.events_path, 'w', encoding='utf-8') as f:
      for r in rows:
        f.write(json.dumps(r) + '\n')

  def test_zero_jvm_tumble_tvf_executes_without_java(self):
    out_path = os.path.join(self.tmp, 'tumble_out.jsonl')
    query = (
        "SELECT window_start, window_end, region, "
        "COUNT(*) AS cnt, SUM(amount) AS rev, AVG(amount) AS avg_amt "
        "FROM TABLE(TUMBLE(TABLE s, DESCRIPTOR(event_ts), "
        "INTERVAL '5' MINUTE)) "
        "WHERE status = 'OK' "
        "GROUP BY window_start, window_end, region "
        "HAVING COUNT(*) >= 2"
    )
    # Simulate a machine with NO Java installed at all.
    with mock.patch.object(
        sql_mod,
        '_find_java',
        side_effect=sql_mod.SqlError('No Java found', cli.EXIT_MISSING_PACKAGE)):
      rc, env, stderr = _run_cli([
          'pipe',
          '--from',
          f's={self.events_path}',
          '--to',
          out_path,
          '--sql',
          query,
      ])
    self.assertEqual(rc, cli.EXIT_OK, f'env={env} stderr={stderr}')
    self.assertEqual(env['outputs']['sql_engine'], 'python')
    self.assertEqual(env['outputs']['execution_path'], 'beam_runner')

    written = _read_jsonl_dir_or_prefix(out_path)
    written.sort(key=lambda r: (r['window_start'], r['region']))
    # EU [12:00, 12:05) had only 1 OK row so HAVING COUNT(*) >= 2 filtered it.
    self.assertEqual(
        written,
        [
            {
                'window_start': '2026-05-01T12:00:00Z',
                'window_end': '2026-05-01T12:05:00Z',
                'region': 'US',
                'cnt': 2,
                'rev': 30.0,
                'avg_amt': 15.0,
            },
            {
                'window_start': '2026-05-01T12:05:00Z',
                'window_end': '2026-05-01T12:10:00Z',
                'region': 'EU',
                'cnt': 2,
                'rev': 100.0,
                'avg_amt': 50.0,
            },
        ],
    )

  def test_zero_jvm_hop_and_session_tvf_execution(self):
    hop_out = os.path.join(self.tmp, 'hop_out.jsonl')
    hop_query = (
        "SELECT window_start, window_end, SUM(amount) AS rev "
        "FROM TABLE(HOP(TABLE s, DESCRIPTOR(event_ts), "
        "INTERVAL '5' MINUTE, INTERVAL '10' MINUTE)) "
        "WHERE status = 'OK' AND region = 'US' "
        "GROUP BY window_start, window_end"
    )
    with mock.patch.object(
        sql_mod,
        '_find_java',
        side_effect=sql_mod.SqlError('No Java', cli.EXIT_MISSING_PACKAGE)):
      rc, env, stderr = _run_cli([
          'pipe',
          '--from',
          f's={self.events_path}',
          '--to',
          hop_out,
          '--sql',
          hop_query,
      ])
    self.assertEqual(rc, cli.EXIT_OK, f'env={env} stderr={stderr}')
    hop_rows = sorted(
        _read_jsonl_dir_or_prefix(hop_out), key=lambda r: r['window_start'])
    # US events at 12:01 and 12:02:30 fall in both [11:55, 12:05) and
    # [12:00, 12:10).
    self.assertEqual(
        hop_rows,
        [
            {
                'window_start': '2026-05-01T11:55:00Z',
                'window_end': '2026-05-01T12:05:00Z',
                'rev': 30.0,
            },
            {
                'window_start': '2026-05-01T12:00:00Z',
                'window_end': '2026-05-01T12:10:00Z',
                'rev': 30.0,
            },
        ],
    )

    sess_out = os.path.join(self.tmp, 'sess_out.jsonl')
    sess_query = (
        "SELECT region, window_start, window_end, COUNT(*) AS n "
        "FROM TABLE(SESSION(TABLE s, DESCRIPTOR(event_ts), "
        "INTERVAL '2' MINUTE)) "
        "WHERE status = 'OK' AND region = 'EU' "
        "GROUP BY region, window_start, window_end"
    )
    with mock.patch.object(
        sql_mod,
        '_find_java',
        side_effect=sql_mod.SqlError('No Java', cli.EXIT_MISSING_PACKAGE)):
      rc, env, stderr = _run_cli([
          'pipe',
          '--from',
          f's={self.events_path}',
          '--to',
          sess_out,
          '--sql',
          sess_query,
      ])
    self.assertEqual(rc, cli.EXIT_OK, f'env={env} stderr={stderr}')
    sess_rows = sorted(
        _read_jsonl_dir_or_prefix(sess_out), key=lambda r: r['window_start'])
    # EU events: 12:04:00 (session [12:04:00, 12:06:00)) and
    # 12:06:15 + 12:07:45 (gap 1m30s < 2m -> merged [12:06:15, 12:09:45)).
    self.assertEqual(
        sess_rows,
        [
            {
                'region': 'EU',
                'window_start': '2026-05-01T12:04:00Z',
                'window_end': '2026-05-01T12:06:00Z',
                'n': 1,
            },
            {
                'region': 'EU',
                'window_start': '2026-05-01T12:06:15Z',
                'window_end': '2026-05-01T12:09:45Z',
                'n': 2,
            },
        ],
    )

  def test_sql_engine_python_scalar_case_dlq_and_distinct_agg(self):
    bad_src = os.path.join(self.tmp, 'dirty.jsonl')
    with open(self.events_path, encoding='utf-8') as src_f:
      good_lines = src_f.read()
    with open(bad_src, 'w', encoding='utf-8') as f:
      f.write(good_lines)
      f.write('{corrupt json line\n')

    dlq_path = os.path.join(self.tmp, 'dlq.jsonl')
    out_scalar = os.path.join(self.tmp, 'scalar_out.jsonl')
    scalar_sql = (
        "SELECT LOWER(region) AS reg, user_id, "
        "CASE WHEN amount >= 50 THEN 'HIGH' ELSE 'LOW' END AS tier, "
        "COALESCE(amount, 0.0) * 2 AS doubled "
        "FROM s "
        "WHERE status = 'OK' AND region IN ('US', 'EU')"
    )
    rc, env, stderr = _run_cli([
        'pipe',
        '--from',
        f's={bad_src}',
        '--to',
        out_scalar,
        '--dlq',
        dlq_path,
        '--sql',
        scalar_sql,
        '--sql-engine',
        'python',
    ])
    self.assertEqual(rc, cli.EXIT_OK, f'env={env} stderr={stderr}')
    self.assertEqual(env['outputs']['sql_engine'], 'python')
    self.assertEqual(env['outputs']['execution_path'], 'beam_runner')
    scalar_rows = _read_jsonl_dir_or_prefix(out_scalar)
    self.assertEqual(len(scalar_rows), 5)
    high_rows = [r for r in scalar_rows if r['tier'] == 'HIGH']
    self.assertEqual(len(high_rows), 1)
    self.assertEqual(high_rows[0]['doubled'], 120.0)
    dlq_rows = _read_jsonl_dir_or_prefix(dlq_path)
    self.assertEqual(len(dlq_rows), 1)

    # Grouped query with COUNT(DISTINCT user_id) and post-agg ratio.
    out_agg = os.path.join(self.tmp, 'agg_out.jsonl')
    agg_sql = (
        "SELECT region, COUNT(*) AS cnt, "
        "COUNT(DISTINCT user_id) AS uniq_users, "
        "SUM(amount) / COUNT(*) AS mean_calc "
        "FROM source "
        "WHERE status = 'OK' "
        "GROUP BY 1 "
        "HAVING SUM(amount) > 25"
    )
    rc2, env2, stderr2 = _run_cli([
        'pipe',
        '--from',
        self.events_path,
        '--to',
        out_agg,
        '--sql',
        agg_sql,
        '--sql-engine',
        'python',
    ])
    self.assertEqual(rc2, cli.EXIT_OK, f'env={env2} stderr={stderr2}')
    agg_rows = sorted(
        _read_jsonl_dir_or_prefix(out_agg), key=lambda r: r['region'])
    self.assertEqual(
        agg_rows,
        [
            {
                'region': 'EU',
                'cnt': 3,
                'uniq_users': 2,
                'mean_calc': 35.0,
            },
            {
                'region': 'US',
                'cnt': 2,
                'uniq_users': 2,
                'mean_calc': 15.0,
            },
        ],
    )

  def test_dry_run_yaml_passes_beam_check_and_strict_error_exits_usage(self):
    plan_file = os.path.join(self.tmp, 'tumble_plan.yaml')
    rc, env, stderr = _run_cli([
        'pipe',
        '--from',
        f's={self.events_path}',
        '--to',
        os.path.join(self.tmp, 'unused.jsonl'),
        '--sql',
        "SELECT window_start, region, COUNT(*) AS n "
        "FROM TABLE(TUMBLE(TABLE s, DESCRIPTOR(event_ts), "
        "INTERVAL '5' MINUTE)) "
        "GROUP BY window_start, region",
        '--dry-run',
    ])
    self.assertEqual(rc, cli.EXIT_OK, stderr)
    yaml_text = env['outputs']['yaml']
    self.assertNotIn('type: Sql', yaml_text)
    self.assertIn('WindowInto', yaml_text)
    parsed = yaml.safe_load(yaml_text)
    self.assertIsInstance(parsed, dict)
    with open(plan_file, 'w', encoding='utf-8') as f:
      f.write(yaml_text)

    rc_chk, env_chk, stderr_chk = _run_cli(['check', '--plan', plan_file])
    self.assertEqual(rc_chk, cli.EXIT_OK, f'chk={env_chk} err={stderr_chk}')

    # Strict --sql-engine=python rejects analytic OVER(...) with EXIT_USAGE.
    rc_bad, env_bad, _ = _run_cli([
        'pipe',
        '--from',
        self.events_path,
        '--to',
        os.path.join(self.tmp, 'bad.jsonl'),
        '--sql',
        'SELECT region, ROW_NUMBER() OVER (PARTITION BY region) FROM source',
        '--sql-engine',
        'python',
    ])
    self.assertEqual(rc_bad, cli.EXIT_USAGE)
    self.assertIn('OVER', env_bad['error']['message'])

  def test_doctor_reports_python_zero_jvm_engine(self):
    rc, env, _ = _run_cli(['doctor'])
    self.assertEqual(rc, cli.EXIT_OK)
    self.assertEqual(
        env['outputs']['sql_engines']['python'],
        {'available': True, 'zero_jvm': True},
    )

  def test_tumble_count_distinct_and_stddev_write_to_parquet(self):
    out_pq = os.path.join(self.tmp, 'tumble_stats.parquet')
    sql = (
        "SELECT region, window_start, window_end, "
        "COUNT(*) AS n, COUNT(DISTINCT user_id) AS uniq_users, "
        "STDDEV_POP(amount) AS sd, ROUND(AVG(amount), 2) AS avg_amt "
        "FROM TABLE(TUMBLE(TABLE source, DESCRIPTOR(event_ts), "
        "INTERVAL '5' MINUTE)) "
        "WHERE status = 'OK' "
        "GROUP BY region, window_start, window_end "
        "HAVING COUNT(*) >= 1"
    )
    rc, env, stderr = _run_cli([
        'pipe',
        self.events_path,
        out_pq,
        '--sql-engine',
        'python',
        '--sql',
        sql,
    ])
    self.assertEqual(rc, cli.EXIT_OK, f'env={env} stderr={stderr}')
    rc_ins, env_ins, _ = _run_cli(['inspect', out_pq, '-n', '10'])
    self.assertEqual(rc_ins, cli.EXIT_OK)
    self.assertEqual(env_ins['outputs']['row_count'], 3)


if __name__ == '__main__':
  unittest.main()
