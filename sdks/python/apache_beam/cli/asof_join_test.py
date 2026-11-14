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

"""Hermetic unit, `TestStream`, CLI, and SQL tests for `B2.1` (`CliAsofJoin`)."""

from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from unittest import mock

import pyarrow.parquet as pq

import apache_beam as beam
from apache_beam.cli import asof_join
from apache_beam.cli import check_cmd
from apache_beam.cli import main as cli_main
from apache_beam.cli import package as package_mod
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.portability.fn_api_runner import fn_runner
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.window import TimestampedValue


def _run_cli(argv: list[str]) -> tuple[int, str, str]:
  out_buf = io.StringIO()
  err_buf = io.StringIO()
  with mock.patch('sys.stdout', out_buf), mock.patch('sys.stderr', err_buf):
    rc = cli_main.main(argv)
  return rc, out_buf.getvalue(), err_buf.getvalue()


class CliAsofJoinUnitAndSemanticsTest(unittest.TestCase):
  def test_config_validation_and_guards(self):
    cfg = asof_join.parse_asof_join_config({
        'key': 'sym',
        'left_time': 't_ts',
        'right_time': 'q_ts',
        'direction': 'backward',
        'strict': True,
        'tolerance': '5m',
        'type': 'left',
        'right_order': 'seq',
    })
    self.assertEqual(cfg.left_key, ('sym', ))
    self.assertEqual(cfg.tolerance, 300.0)
    self.assertTrue(cfg.strict)
    self.assertEqual(cfg.right_order, 'seq')

    for bad in [
        {'left_time': 't_ts', 'right_time': 'q_ts'},  # missing key
        {'key': 'k', 'direction': 'nearest',
         'strict': True},  # strict + nearest
        {'key': 'k', 'type': 'right'},  # right join disallowed on ASOF
        {'key': 'k', 'bogus': 1},  # unknown option
    ]:
      with self.subTest(bad=bad):
        with self.assertRaises(ValueError):
          asof_join.parse_asof_join_config(bad)

  def test_leak_free_backward_forward_nearest_and_stateful_parity(self):
    trades = [
        {
            'sym': 'AAPL', 't_ts': 100, 'qty': 10
        },
        {
            'sym': 'AAPL', 't_ts': 105, 'qty': 20
        },
        {
            'sym': 'AAPL', 't_ts': 200, 'qty': 30
        },
    ]
    quotes = [
        # Future quote at 120 must NEVER leak into t_ts=100 or 105 under backward!
        {
            'sym': 'AAPL', 'q_ts': 120, 'price': 999.0, 'seq': 1
        },
        # Tied quotes at q_ts=100: seq=2 (151.0) wins over seq=1 (150.0)
        {
            'sym': 'AAPL', 'q_ts': 100, 'price': 150.0, 'seq': 1
        },
        {
            'sym': 'AAPL', 'q_ts': 100, 'price': 151.0, 'seq': 2
        },
    ]

    # 1. Non-strict backward with tolerance=10s (both bounded and force_stateful)
    for force_st in (False, True):
      with self.subTest(mode='backward', force_stateful=force_st):
        p = beam.Pipeline(runner=fn_runner.FnApiRunner())
        l_pc = p | 'L' >> beam.Create([beam.Row(**r) for r in trades])
        r_pc = p | 'R' >> beam.Create([beam.Row(**r) for r in quotes])
        joined = {
            'left': l_pc, 'right': r_pc
        } | asof_join.CliAsofJoin(
            key='sym',
            left_time='t_ts',
            right_time='q_ts',
            direction='backward',
            tolerance='10s',
            type='left',
            right_order='seq',
            force_stateful=force_st,
        )
        assert_that(
            joined | beam.Map(lambda r: (r.t_ts, r.price)),
            equal_to([
                (100,
                 151.0),  # exact tie picked seq=2 (151.0), NOT future 999.0!
                (105, 151.0),  # within 10s tolerance
                (200, None),  # 200 - 120 = 80s > 10s tolerance -> NULL!
            ]),
        )
        p.run().wait_until_finish()

    # 2. Strict backward (`t_r < t_l`): t_ts=100 must NOT match q_ts=100!
    strict_rows, _, _ = asof_join.join_bounded_asof(
        trades,
        quotes,
        asof_join.parse_asof_join_config({
            'key': 'sym',
            'left_time': 't_ts',
            'right_time': 'q_ts',
            'direction': 'backward',
            'strict': True,
            'type': 'left',
            'right_order': 'seq',
        }),
    )
    by_ts = {r['t_ts']: r['price'] for r in strict_rows}
    self.assertIsNone(by_ts[100])
    self.assertEqual(by_ts[105], 151.0)
    self.assertEqual(by_ts[200], 999.0)

    # 3. Nearest with equal-distance causal backward tie-breaking:
    # Left at t=110 is 10s from q_ts=100 (151.0) and 10s from q_ts=120 (999.0) -> backward wins!
    near_rows, _, _ = asof_join.join_bounded_asof(
        [{'sym': 'AAPL', 't_ts': 110}, {'sym': 'AAPL', 't_ts': 115}],
        quotes,
        asof_join.parse_asof_join_config({
            'key': 'sym',
            'left_time': 't_ts',
            'right_time': 'q_ts',
            'direction': 'nearest',
            'tolerance': '20s',
            'right_order': 'seq',
        }),
    )
    near_map = {r['t_ts']: r['price'] for r in near_rows}
    self.assertEqual(near_map[110], 151.0)
    self.assertEqual(near_map[115], 999.0)


class CliAsofJoinTestStreamTest(unittest.TestCase):
  def test_teststream_out_of_order_watermark_sync_and_anchor_compaction(self):
    # Single TestStream root branched into L and R so BundleBasedDirectRunner
    # advances a deterministic synchronized watermark:
    # 1. Left trade at t_l=50 arrives BEFORE right quotes at t_r=10..45 while watermark=0.
    #    Watermark synchronization holds t_l=50 until watermark >= 50s so it
    #    matches q_ts=45 (205.0) instead of prematurely emitting NULL!
    # 2. Quotes 10, 20, 30, 45 compact to latest anchor 45, which then expires
    #    before t_l=70 (70 - 45 = 25s > 20s tolerance -> NULL).
    mux = (
        TestStream().advance_watermark_to(0).add_elements([
            TimestampedValue(('L', beam.Row(sym='GOOG', t_ts=50, qty=5)), 50),
        ]).add_elements([
            TimestampedValue(('R', beam.Row(sym='GOOG', q_ts=10, price=201.0)),
                             10),
            TimestampedValue(('R', beam.Row(sym='GOOG', q_ts=20, price=202.0)),
                             20),
            TimestampedValue(('R', beam.Row(sym='GOOG', q_ts=30, price=203.0)),
                             30),
            TimestampedValue(('R', beam.Row(sym='GOOG', q_ts=45, price=205.0)),
                             45),
        ]).advance_watermark_to(60).add_elements([
            TimestampedValue(('L', beam.Row(sym='GOOG', t_ts=70, qty=8)), 70),
        ]).advance_watermark_to(80).advance_watermark_to_infinity())

    opts = PipelineOptions(['--runner=DirectRunner', '--streaming'])
    p = beam.Pipeline(options=opts)
    stream = p | 'MuxTS' >> mux
    l_pc = (
        stream
        | 'FilterL' >> beam.Filter(lambda kv: kv[0] == 'L')
        | 'UnwrapL' >> beam.Map(lambda kv: kv[1]))
    r_pc = (
        stream
        | 'FilterR' >> beam.Filter(lambda kv: kv[0] == 'R')
        | 'UnwrapR' >> beam.Map(lambda kv: kv[1]))
    joined = {
        'left': l_pc, 'right': r_pc
    } | asof_join.CliAsofJoin(
        key='sym',
        left_time='t_ts',
        right_time='q_ts',
        direction='backward',
        tolerance='20s',
        type='left',
        force_stateful=True,
    )
    assert_that(
        joined | 'Proj' >> beam.Map(lambda r: (r.t_ts, r.price)),
        equal_to([
            (50, 205.0),  # waited for q_ts=45!
            (70, None),  # 70 - 45 = 25s > 20s tolerance -> anchor expired!
        ]),
    )
    p.run().wait_until_finish()


class CliAsofJoinCliAndSqlEndToEndTest(unittest.TestCase):
  def test_cli_pipe_asof_join_to_parquet_dlq_and_package(self):
    with tempfile.TemporaryDirectory() as tmp:
      trades_p = os.path.join(tmp, 'trades.jsonl')
      quotes_p = os.path.join(tmp, 'quotes.jsonl')
      out_p = os.path.join(tmp, 'joined.parquet')
      dlq_p = os.path.join(tmp, 'dlq.jsonl')

      trades = [
          {
              'sym': 'MSFT', 't_ts': 1000, 'qty': 2, 'secret': 's-ok'
          },
          {
              'sym': 'MSFT', 't_ts': 1040, 'qty': 4, 'secret': 's-unmatched'
          },
          # Skewed trade lagging 500s behind max quote 1000 (> max_watermark_skew=200s) -> DLQ!
          {
              'sym': 'MSFT', 't_ts': 400, 'qty': 1, 'secret': 'TOP-SECRET-PII'
          },
      ]
      quotes = [
          {
              'sym': 'MSFT', 'q_ts': 995, 'price': 410.5, 'seq': 1
          },
      ]
      for path, items in ((trades_p, trades), (quotes_p, quotes)):
        with open(path, 'w', encoding='utf-8') as f:
          for it in items:
            f.write(json.dumps(it) + '\n')

      # `--assert-counts` rejected pre-flight (`rc == 2`) on `--asof-join`
      rc_ac, _, _ = _run_cli([
          'pipe',
          '--from',
          f't={trades_p}',
          '--join',
          f'q={quotes_p}',
          '--to',
          out_p,
          '--asof-join',
          'key=sym,left_time=t_ts,right_time=q_ts,tolerance=15s',
          '--assert-counts',
          '--json',
      ])
      self.assertEqual(rc_ac, 2)

      rc, out, err = _run_cli([
          'pipe',
          '--from',
          f't={trades_p}',
          '--join',
          f'q={quotes_p}',
          '--to',
          out_p,
          '--asof-join',
          'key=sym,left_time=t_ts,right_time=q_ts,tolerance=15s,type=left,right_order=seq',
          '--max-watermark-skew',
          '200s',
          '--skew-action',
          'dlq',
          '--dlq',
          dlq_p,
          '--dlq-redact-fields',
          'secret',
          '--json',
      ])
      self.assertEqual(rc, 0, f'STDOUT: {out}\nSTDERR: {err}')
      env = json.loads(out)
      m = env['metrics']
      self.assertEqual(m.get('asof_matched'), 1)
      self.assertEqual(m.get('asof_unmatched'), 1)
      self.assertEqual(m.get('dlq_rows'), 1)

      rows = pq.read_table(out_p).to_pylist()
      by_ts = {r['t_ts']: r for r in rows}
      self.assertEqual(set(by_ts), {1000, 1040})
      self.assertAlmostEqual(by_ts[1000]['price'], 410.5)
      self.assertIsNone(by_ts[1040]['price'])

      with open(dlq_p, encoding='utf-8') as f:
        dlq_text = f.read()
      self.assertNotIn('TOP-SECRET-PII', dlq_text)
      self.assertIn('<REDACTED>', dlq_text)

      # Verify `--dry-run`, `beam check --plan`, and `beam package --sdk=auto`
      rc_dry, yaml_str, err_dry = _run_cli([
          'pipe',
          '--from',
          f't={trades_p}',
          '--join',
          f'q={quotes_p}',
          '--to',
          os.path.join(tmp, 'dry.parquet'),
          '--asof-join',
          'key=sym,left_time=t_ts,right_time=q_ts,tolerance=15s',
          '--dry-run',
      ])
      self.assertEqual(rc_dry, 0, err_dry)
      yaml_path = os.path.join(tmp, 'asof.yaml')
      with open(yaml_path, 'w', encoding='utf-8') as f:
        f.write(yaml_str)
      chk = check_cmd.run_check(
          yaml_path, expand_plan=True, trust_providers=False)
      self.assertTrue(chk['valid'])
      pkg = package_mod.run_package(
          yaml_path,
          to_location=os.path.join(tmp, 'spec.json'),
          sdk='auto',
          output_dir=os.path.join(tmp, 'bundle'),
          dry_run=True,
      )
      self.assertEqual(pkg['sdk_language'], 'PYTHON')

  def test_sql_asof_join_zero_duckdb_local_and_distributed_lowering(self):
    with tempfile.TemporaryDirectory() as tmp:
      trades_p = os.path.join(tmp, 'trades.jsonl')
      quotes_p = os.path.join(tmp, 'quotes.jsonl')
      local_out = os.path.join(tmp, 'local_sql_out.jsonl')
      sf_out = os.path.join(tmp, 'sf_sql_out.jsonl')
      dist_out = os.path.join(tmp, 'dist_sql_out.jsonl')

      with open(trades_p, 'w', encoding='utf-8') as f:
        f.write('{"sym": "NVDA", "t_ts": 100, "qty": 3}\n')
        f.write('{"sym": "NVDA", "t_ts": 200, "qty": 5}\n')
      with open(quotes_p, 'w', encoding='utf-8') as f:
        f.write('{"sym": "NVDA", "q_ts": 95, "price": 120.0}\n')
        f.write('{"sym": "NVDA", "q_ts": 150, "price": 999.0}\n')

      # 1. Zero-DuckDB local SQLite execution (`--sql-engine sqlite`) with DuckDB syntax + ORDER BY
      q_duck = (
          'SELECT t.sym, t.t_ts, q.price, t.qty * q.price AS notional '
          'FROM t ASOF LEFT JOIN q '
          "ON t.sym = q.sym AND t.t_ts >= q.q_ts AND t.t_ts - q.q_ts <= INTERVAL '20' SECOND "
          'ORDER BY t.t_ts')
      rc1, out1, err1 = _run_cli([
          'pipe',
          '--from',
          f't={trades_p}',
          '--join',
          f'q={quotes_p}',
          '--to',
          local_out,
          '--sql-engine',
          'sqlite',
          '--sql',
          q_duck,
          '--json',
      ])
      self.assertEqual(rc1, 0, f'STDOUT: {out1}\nSTDERR: {err1}')
      with open(local_out, encoding='utf-8') as f:
        res1 = [json.loads(line) for line in f if line.strip()]
      self.assertEqual(
          res1,
          [
              {
                  'sym': 'NVDA', 't_ts': 100, 'price': 120.0, 'notional': 360.0
              },
              # t_ts=200 - q_ts=150 = 50s > 20s tolerance -> NULL!
              {
                  'sym': 'NVDA', 't_ts': 200, 'price': None, 'notional': None
              },
          ],
      )

      # 2. Snowflake `MATCH_CONDITION(...)` syntax on zero-DuckDB local SQLite
      q_sf = (
          'SELECT t.sym, t.t_ts, q.price '
          'FROM t ASOF JOIN q MATCH_CONDITION (t.t_ts >= q.q_ts) '
          'ON t.sym = q.sym ORDER BY t.t_ts')
      rc2, out2, err2 = _run_cli([
          'pipe',
          '--from',
          f't={trades_p}',
          '--join',
          f'q={quotes_p}',
          '--to',
          sf_out,
          '--sql-engine',
          'sqlite',
          '--sql',
          q_sf,
          '--json',
      ])
      self.assertEqual(rc2, 0, f'STDOUT: {out2}\nSTDERR: {err2}')
      with open(sf_out, encoding='utf-8') as f:
        res2 = [json.loads(line) for line in f if line.strip()]
      self.assertEqual(
          res2,
          [
              {
                  'sym': 'NVDA', 't_ts': 100, 'price': 120.0
              },
              {
                  'sym': 'NVDA', 't_ts': 200, 'price': 999.0
              },
          ],
      )

      # 3. Distributed `--dry-run` lowers SQL `ASOF LEFT JOIN` to `CliAsofJoin` (NEVER `type: Sql`)
      q_dist = (
          'SELECT t.sym, t.t_ts, q.price '
          'FROM t ASOF LEFT JOIN q '
          "ON t.sym = q.sym AND t.t_ts >= q.q_ts AND t.t_ts - q.q_ts <= INTERVAL '20' SECOND"
      )
      rc_dry, yaml_dist, err_dry = _run_cli([
          'pipe',
          '--from',
          f't={trades_p}',
          '--join',
          f'q={quotes_p}',
          '--to',
          dist_out,
          '--sql',
          q_dist,
          '--dry-run',
      ])
      self.assertEqual(rc_dry, 0, err_dry)
      self.assertIn('CliAsofJoin', yaml_dist)
      self.assertNotIn('type: Sql', yaml_dist)

      dist_yaml_path = os.path.join(tmp, 'sql_asof.yaml')
      with open(dist_yaml_path, 'w', encoding='utf-8') as f:
        f.write(yaml_dist)
      rc_run, out_run, err_run = _run_cli(['run', dist_yaml_path, '--json'])
      self.assertEqual(rc_run, 0, f'STDOUT: {out_run}\nSTDERR: {err_run}')
      import glob as _glob
      raw_dist_rows = []
      for shard in sorted(_glob.glob(dist_out + '*')):
        with open(shard, encoding='utf-8') as f:
          raw_dist_rows.extend(json.loads(line) for line in f if line.strip())
      res_dist = sorted(raw_dist_rows, key=lambda r: r['t_ts'])
      self.assertEqual(
          res_dist,
          [
              {
                  'sym': 'NVDA', 't_ts': 100, 'price': 120.0
              },
              {
                  'sym': 'NVDA', 't_ts': 200, 'price': None
              },
          ],
      )

      # 4. Fail-closed rejection of RIGHT ASOF JOIN
      rc_bad, _, _ = _run_cli([
          'pipe',
          '--from',
          f't={trades_p}',
          '--join',
          f'q={quotes_p}',
          '--to',
          dist_out,
          '--sql',
          'SELECT * FROM t RIGHT ASOF JOIN q ON t.sym = q.sym AND t.t_ts >= q.q_ts',
          '--dry-run',
      ])
      self.assertEqual(rc_bad, 2)


if __name__ == '__main__':
  unittest.main()
