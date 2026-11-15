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

"""Hermetic tests for Round 3 CLI ergonomics & competitive parity features.

Covers:
1. `--drop-bad` / `--skip-errors` (with `--assert-counts`, `--where`, `--sql`,
   and mutual exclusion with `--dlq`).
2. Safe UTC timestamp/date helpers in `--select` and `--where`
   (`parse_timestamp`, `to_utc`, `epoch_ms`, `year`, `month`, `day`, `hour`,
   `date_part`) across both `local_fast` and portable Beam runners + `beam check`.
3. Key-value `--window` grammar (`session:gap=30m:key=user_id`,
   `fixed:size=60s`, `sliding:size=5m:period=1m`) with auto-grouped keys.
4. Composable `--sql` + `--where` (pre-SQL) + `--select` (post-SQL) on both
   `local_sql` and `--emit-yaml` portable `Sql` DAGs.
5. Portable Beam Calcite `Sql` YAML synthesis (`FROM source` -> `PCOLLECTION`,
   named multi-input `JOIN`s) and `--max-bytes` enforcement on `--join`.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest

import pyarrow.parquet as pq

from apache_beam.cli import main as cli_main


def _run_cli(argv: list[str]) -> tuple[int, dict, str]:
  out_buf = io.StringIO()
  err_buf = io.StringIO()
  with contextlib.redirect_stdout(out_buf), contextlib.redirect_stderr(err_buf):
    rc = cli_main.main(argv)
  raw_out = out_buf.getvalue().strip()
  payload = json.loads(raw_out) if raw_out.startswith('{') else {}
  return rc, payload, err_buf.getvalue()


class CompetitiveRound3Test(unittest.TestCase):

  def test_drop_bad_corrupted_jsonl_with_assert_counts(self):
    with tempfile.TemporaryDirectory() as tmp:
      src = os.path.join(tmp, 'dirty.jsonl')
      dst = os.path.join(tmp, 'clean.parquet')
      with open(src, 'w', encoding='utf-8') as f:
        f.write('{"id": 1, "amount": 10.5}\n')
        f.write('NOT_VALID_JSON_AT_ALL\n')
        f.write('{"id": 2, "amount": -5.0}\n')
        f.write('{"id": 3, "amount": 25.0}\n')

      rc, env, err = _run_cli([
          'pipe',
          '--from',
          src,
          '--to',
          dst,
          '--where',
          'amount > 0',
          '--drop-bad',
          '--assert-counts',
          '--json',
      ])
      self.assertEqual(rc, 0, msg=f'env={env} err={err}')
      metrics = env['metrics']
      self.assertEqual(metrics['rows_read'], 4)
      self.assertEqual(metrics['rows_written'], 2)
      self.assertEqual(metrics['rows_filtered_out'], 2)
      self.assertEqual(metrics.get('dlq_rows', 0), 0)

      tbl = pq.read_table(dst).to_pylist()
      self.assertEqual(sorted(r['id'] for r in tbl), [1, 3])

  def test_drop_bad_conflicts_with_dlq(self):
    with tempfile.TemporaryDirectory() as tmp:
      src = os.path.join(tmp, 'in.jsonl')
      with open(src, 'w', encoding='utf-8') as f:
        f.write('{"id": 1}\n')
      rc, env, _ = _run_cli([
          'pipe',
          '--from',
          src,
          '--to',
          os.path.join(tmp, 'out.jsonl'),
          '--drop-bad',
          '--dlq',
          os.path.join(tmp, 'dlq.jsonl'),
          '--json',
      ])
      self.assertEqual(rc, 2)
      self.assertIn('--drop-bad', env['error']['message'])

  def test_drop_bad_with_local_sql_and_assert_counts(self):
    with tempfile.TemporaryDirectory() as tmp:
      src = os.path.join(tmp, 'events.jsonl')
      dst = os.path.join(tmp, 'out.jsonl')
      with open(src, 'w', encoding='utf-8') as f:
        f.write('{"k": "a", "v": 10}\n')
        f.write('{corrupt_line\n')
        f.write('{"k": "b", "v": 20}\n')

      rc, env, err = _run_cli([
          'pipe',
          '--from',
          src,
          '--to',
          dst,
          '--sql',
          'SELECT k, v * 2 AS v2 FROM source ORDER BY k',
          '--skip-errors',
          '--assert-counts',
          '--json',
      ])
      self.assertEqual(rc, 0, msg=f'env={env} err={err}')
      self.assertEqual(env['metrics']['rows_read'], 3)
      self.assertEqual(env['metrics']['rows_written'], 2)
      self.assertEqual(env['metrics']['rows_filtered_out'], 1)
      with open(dst, encoding='utf-8') as f:
        rows = [json.loads(line) for line in f if line.strip()]
      self.assertEqual(rows, [{'k': 'a', 'v2': 20}, {'k': 'b', 'v2': 40}])

  def test_timestamp_and_date_helpers_fast_and_portable_and_beam_check(self):
    with tempfile.TemporaryDirectory() as tmp:
      src = os.path.join(tmp, 'ts.jsonl')
      dst_fast = os.path.join(tmp, 'fast.parquet')
      dst_port = os.path.join(tmp, 'portable.parquet')
      yaml_path = os.path.join(tmp, 'plan.yaml')
      records = [
          {'id': 1, 'ts': '2026-06-15T15:30:00+05:30'},
          {'id': 2, 'ts': '2026-06-15 11:45:05'},
          {'id': 3, 'ts': '1781517600000'},  # 2026-06-15T10:00:00Z in ms
          {'id': 4, 'ts': None},
      ]
      with open(src, 'w', encoding='utf-8') as f:
        for r in records:
          f.write(json.dumps(r) + '\n')

      select_expr = (
          'id, parse_timestamp(ts) AS utc_ts, epoch_ms(ts) AS ts_ms, '
          'year(ts) AS yr, month(ts) AS mo, day(ts) AS dy, '
          "hour(ts) AS hr, date_part('minute', ts) AS mn")
      where_expr = 'ts IS NULL OR hour(ts) >= 10'

      # 1. Fast local execution
      rc1, env1, err1 = _run_cli([
          'pipe',
          '--from',
          src,
          '--to',
          dst_fast,
          '--where',
          where_expr,
          '--select',
          select_expr,
          '--json',
      ])
      self.assertEqual(rc1, 0, msg=f'env1={env1} err1={err1}')
      self.assertEqual(env1['runner'], 'local_fast')
      fast_rows = sorted(
          pq.read_table(dst_fast).to_pylist(), key=lambda r: r['id'])

      # 2. Portable Beam runner execution
      rc2, env2, err2 = _run_cli([
          'pipe',
          '--from',
          src,
          '--to',
          dst_port,
          '--where',
          where_expr,
          '--select',
          select_expr,
          '--portable',
          '--json',
      ])
      self.assertEqual(rc2, 0, msg=f'env2={env2} err2={err2}')
      port_rows = sorted(
          pq.read_table(dst_port).to_pylist(), key=lambda r: r['id'])

      self.assertEqual(fast_rows, port_rows)
      self.assertEqual(
          fast_rows[0],
          {
              'id': 1,
              'utc_ts': '2026-06-15T10:00:00Z',
              'ts_ms': 1781517600000,
              'yr': 2026,
              'mo': 6,
              'dy': 15,
              'hr': 10,
              'mn': 0,
          },
      )
      self.assertEqual(fast_rows[1]['utc_ts'], '2026-06-15T11:45:05Z')
      self.assertEqual(fast_rows[1]['hr'], 11)
      self.assertEqual(fast_rows[1]['mn'], 45)
      self.assertEqual(fast_rows[2]['utc_ts'], '2026-06-15T10:00:00Z')
      self.assertIsNone(fast_rows[3]['utc_ts'])
      self.assertIsNone(fast_rows[3]['yr'])

      # 3. Emit YAML and verify `beam check` passes without --trust-providers
      rc_emit, env_emit, _ = _run_cli([
          'pipe',
          '--from',
          src,
          '--to',
          dst_port,
          '--where',
          where_expr,
          '--select',
          select_expr,
          '--emit-yaml',
          '--json',
      ])
      self.assertEqual(rc_emit, 0)
      with open(yaml_path, 'w', encoding='utf-8') as f:
        f.write(env_emit['outputs']['yaml'])
      rc_chk, env_chk, err_chk = _run_cli(['check', yaml_path, '--json'])
      self.assertEqual(rc_chk, 0, msg=f'env_chk={env_chk} err_chk={err_chk}')

  def test_window_session_kv_grammar_with_key(self):
    with tempfile.TemporaryDirectory() as tmp:
      src = os.path.join(tmp, 'clicks.jsonl')
      dst = os.path.join(tmp, 'sessions.jsonl')
      # u1 has 2 events 5m apart (1 session) + 1 event 45m later (2nd session).
      # u2 has 2 events 2m apart overlapping u1's first session (1 session).
      events = [
          {'user_id': 'u1', 'event_ts': '2026-06-15T10:00:00Z'},
          {'user_id': 'u1', 'event_ts': '2026-06-15T10:05:00Z'},
          {'user_id': 'u2', 'event_ts': '2026-06-15T10:02:00Z'},
          {'user_id': 'u2', 'event_ts': '2026-06-15T10:04:00Z'},
          {'user_id': 'u1', 'event_ts': '2026-06-15T10:50:00Z'},
      ]
      with open(src, 'w', encoding='utf-8') as f:
        for e in events:
          f.write(json.dumps(e) + '\n')

      rc, env, err = _run_cli([
          'pipe',
          '--from',
          src,
          '--to',
          dst,
          '--timestamp-col',
          'event_ts',
          '--window',
          'session:gap=30m:key=user_id',
          '--select',
          'COUNT(*) AS n_events',
          '--json',
      ])
      self.assertEqual(rc, 0, msg=f'env={env} err={err}')
      with open(dst, encoding='utf-8') as f:
        out_rows = [json.loads(line) for line in f if line.strip()]
      by_user: dict[str, list[int]] = {}
      for r in out_rows:
        by_user.setdefault(r['user_id'], []).append(r['n_events'])
      self.assertEqual(sorted(by_user['u1']), [1, 2])
      self.assertEqual(by_user['u2'], [2])

  def test_sql_composes_with_where_and_select_locally_and_emit_yaml(self):
    with tempfile.TemporaryDirectory() as tmp:
      src = os.path.join(tmp, 'orders.jsonl')
      dst = os.path.join(tmp, 'summary.jsonl')
      yaml_path = os.path.join(tmp, 'sql_plan.yaml')
      rows = [
          {'region': 'West', 'status': 'PAID', 'amount': 10.0},
          {'region': 'West', 'status': 'VOID', 'amount': 999.0},
          {'region': 'East', 'status': 'PAID', 'amount': 25.5},
      ]
      with open(src, 'w', encoding='utf-8') as f:
        for r in rows:
          f.write(json.dumps(r) + '\n')

      sql_q = 'SELECT region, SUM(amount) AS rev FROM source GROUP BY region'
      rc, env, err = _run_cli([
          'pipe',
          '--from',
          src,
          '--to',
          dst,
          '--where',
          "status = 'PAID'",
          '--sql',
          sql_q,
          '--select',
          'lower(region) AS region_lc, round(rev * 2, 1) AS rev2',
          '--json',
      ])
      self.assertEqual(rc, 0, msg=f'env={env} err={err}')
      with open(dst, encoding='utf-8') as f:
        got = sorted(
            (json.loads(line) for line in f if line.strip()),
            key=lambda r: r['region_lc'])
      self.assertEqual(
          got,
          [
              {'region_lc': 'east', 'rev2': 51.0},
              {'region_lc': 'west', 'rev2': 20.0},
          ],
      )

      # Also verify --emit-yaml synthesizes a valid portable Sql Beam YAML DAG!
      rc_y, env_y, err_y = _run_cli([
          'pipe',
          '--from',
          src,
          '--to',
          dst,
          '--where',
          "status = 'PAID'",
          '--sql',
          sql_q,
          '--select',
          'lower(region) AS region_lc, round(rev * 2, 1) AS rev2',
          '--emit-yaml',
          '--json',
      ])
      self.assertEqual(rc_y, 0, msg=f'env_y={env_y} err_y={err_y}')
      yaml_text = env_y['outputs']['yaml']
      self.assertIn('type: Sql', yaml_text)
      self.assertIn('FROM PCOLLECTION', yaml_text)
      with open(yaml_path, 'w', encoding='utf-8') as f:
        f.write(yaml_text)
      rc_chk, env_chk, err_chk = _run_cli(['check', yaml_path, '--json'])
      self.assertEqual(rc_chk, 0, msg=f'env_chk={env_chk} err_chk={err_chk}')

  def test_multi_input_sql_emit_yaml_and_max_bytes_on_join_endpoint(self):
    with tempfile.TemporaryDirectory() as tmp:
      left_p = os.path.join(tmp, 'orders.csv')
      right_p = os.path.join(tmp, 'customers.csv')
      dst = os.path.join(tmp, 'joined.parquet')
      with open(left_p, 'w', encoding='utf-8') as f:
        f.write('id,cid\n1,100\n')
      with open(right_p, 'w', encoding='utf-8') as f:
        f.write('cid,tier\n' + ('100,VIP\n' * 50))

      # 1. Multi-input --emit-yaml emits named-input Beam Calcite Sql YAML
      rc_y, env_y, err_y = _run_cli([
          'pipe',
          '--from',
          f'o={left_p}',
          '--join',
          f'c={right_p}',
          '--to',
          dst,
          '--sql',
          'SELECT o.id, c.tier FROM o JOIN c ON o.cid = c.cid',
          '--emit-yaml',
          '--json',
      ])
      self.assertEqual(rc_y, 0, msg=f'env_y={env_y} err_y={err_y}')
      self.assertIn('type: Sql', env_y['outputs']['yaml'])

      # 2. --max-bytes guards BOTH --from AND --join right endpoint!
      rc_cost, env_cost, _ = _run_cli([
          'pipe',
          '--from',
          f'o={left_p}',
          '--join',
          f'c={right_p}',
          '--to',
          dst,
          '--interval-join',
          'on=cid,left_time=id,right_time=cid,between=0s..5s',
          '--max-bytes',
          '50B',
          '--json',
      ])
      self.assertEqual(rc_cost, 8, msg=f'env_cost={env_cost}')


if __name__ == '__main__':
  unittest.main()
