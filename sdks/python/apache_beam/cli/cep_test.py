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

"""Hermetic unit, `TestStream`, and CLI tests for `B2.2` (`CliPatternMatch` CEP)."""

from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from unittest import mock

import pyarrow.parquet as pq

import apache_beam as beam
from apache_beam.cli import cep
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


class CliPatternMatchValidationAndSemanticsTest(unittest.TestCase):
  def test_config_validation_quantifier_expansion_and_ast_safety(self):
    cfg = cep.parse_pattern_config({
        'key': 'user_id',
        'seq': 'fail_login{3} -> transfer within 2m',
        'where': {
            'fail_login': "status == 'fail'",
            'transfer': 'amount > 500',
        },
    })
    self.assertEqual(
        [s.step_alias for s in cfg.steps],
        ['fail_login_1', 'fail_login_2', 'fail_login_3', 'transfer'],
    )
    self.assertEqual(cfg.within_sec, 120.0)

    neg_cfg = cep.parse_pattern_config({
        'key': 'order_id',
        'seq': 'checkout -> !payment within 15m',
    })
    self.assertTrue(neg_cfg.steps[-1].negated)

    for bad in [
        {'seq': 'A -> B within 5m'},  # missing key
        {'key': 'u', 'seq': 'A -> B'},  # missing within
        {'key': 'u', 'seq': 'A+ -> B within 5m'},  # unbounded Kleene +
        {'key': 'u', 'seq': 'A* -> B within 5m'},  # unbounded Kleene *
        {'key': 'u', 'seq': '!A -> B within 5m'},  # leading negation
        {'key': 'u', 'seq': 'A -> !B -> C within 5m'},  # mid negation
        {'key': 'u', 'seq': 'A{17} -> B within 5m'},  # > 16 steps
        {'key': 'u', 'seq': 'A -> B within 5m', 'max_active': 2000},
        {
            'key': 'u',
            'seq': 'A -> B within 5m',
            'where': {'A': "__import__('os').system('id')"},
        },
        {
            'key': 'u',
            'seq': 'A -> B within 5m',
            'where': {'A': '2 ** 64 > 1'},
        },
    ]:
      with self.subTest(bad=bad):
        with self.assertRaises(ValueError):
          cep.parse_pattern_config(bad)

  def test_relaxed_vs_contiguous_and_skip_strategies_across_bounded_and_stateful(
      self):
    # 1. `relaxed` matches across intervening `pageview`, whereas `contiguous` aborts!
    events_mid = [
        {
            'uid': 'u1', 'ts': 10, 'event_type': 'login'
        },
        {
            'uid': 'u1', 'ts': 20, 'event_type': 'pageview'
        },
        {
            'uid': 'u1', 'ts': 30, 'event_type': 'purchase'
        },
    ]
    rel_rows, rel_cnt, _, _ = cep.match_bounded_patterns(
        events_mid,
        cep.parse_pattern_config({
            'key': 'uid',
            'timestamp_col': 'ts',
            'seq': 'login -> purchase within 60s',
            'mode': 'relaxed',
        }),
    )
    self.assertEqual(rel_cnt, 1)
    self.assertEqual(rel_rows[0]['matched_steps'], 'login,purchase')

    cont_rows, cont_cnt, cont_exp, _ = cep.match_bounded_patterns(
        events_mid,
        cep.parse_pattern_config({
            'key': 'uid',
            'timestamp_col': 'ts',
            'seq': 'login -> purchase within 60s',
            'mode': 'contiguous',
        }),
    )
    self.assertEqual(cont_cnt, 0)
    self.assertEqual(cont_rows, [])
    self.assertEqual(cont_exp, 1)

    # 2. `skip_past_last` (1 match) vs `skip_to_next` (2 matches) on both
    # bounded GBK and stateful DoFn!
    burst = [
        beam.Row(uid='u2', ts=1, event_type='fail', seq=1),
        beam.Row(uid='u2', ts=2, event_type='fail', seq=2),
        beam.Row(uid='u2', ts=3, event_type='fail', seq=3),
        beam.Row(uid='u2', ts=4, event_type='transfer', seq=4),
    ]
    for force_st in (False, True):
      for strat, expected_starts in (
          ('skip_past_last', [1]),
          ('skip_to_next', [1, 2]),
      ):
        with self.subTest(force_stateful=force_st, after_match=strat):
          p = beam.Pipeline(runner=fn_runner.FnApiRunner())
          matched = (
              p
              | 'In' >> beam.Create(burst)
              | 'CEP' >> cep.CliPatternMatch(
                  key='uid',
                  timestamp_col='ts',
                  seq='fail{2} -> transfer within 30s',
                  after_match=strat,
                  select={'first_seq': 'fail_1.seq'},
                  force_stateful=force_st,
              ))
          assert_that(
              matched | beam.Map(lambda r: r.first_seq),
              equal_to(expected_starts))
          p.run().wait_until_finish()


class CliPatternMatchTestStreamAndOverflowTest(unittest.TestCase):
  def test_teststream_out_of_order_sequence_and_negated_absence(self):
    # 1. Out-of-order positive sequence (`purchase@30` arrives before `login@10` & `mfa@20`)
    ts_pos = (
        TestStream().advance_watermark_to(0).add_elements([
            TimestampedValue(
                beam.Row(uid='u1', ts=30, event_type='purchase', amt=900), 30),
            TimestampedValue(
                beam.Row(uid='u1', ts=10, event_type='login', amt=0), 10),
            TimestampedValue(
                beam.Row(uid='u1', ts=20, event_type='mfa', amt=0), 20),
        ]).advance_watermark_to(40).advance_watermark_to_infinity())

    opts = PipelineOptions(['--runner=DirectRunner', '--streaming'])
    p1 = beam.Pipeline(options=opts)
    out1 = (
        p1
        | 'PosTS' >> ts_pos
        | 'PosCEP' >> cep.CliPatternMatch(
            key='uid',
            timestamp_col='ts',
            seq='login -> mfa -> purchase within 60s',
            select={
                'uid': 'uid',
                'amt': 'purchase.amt',
                'steps': 'matched_steps',
            },
            force_stateful=True,
        ))
    assert_that(
        out1 | beam.Map(lambda r: (r.uid, r.amt, r.steps)),
        equal_to([('u1', 900, 'login,mfa,purchase')]),
    )
    p1.run().wait_until_finish()

    # 2. Negated absence (`checkout -> !payment within 20s`):
    # - `o_paid` has checkout@10 and payment@25 (<= 30s) -> cancelled!
    # - `o_abandoned` has checkout@15 and NO payment within 20s -> emits at WM >= 35s!
    ts_neg = (
        TestStream().advance_watermark_to(0).add_elements([
            TimestampedValue(
                beam.Row(order_id='o_paid', ts=10, event_type='checkout'), 10),
            TimestampedValue(
                beam.Row(order_id='o_abandoned', ts=15, event_type='checkout'),
                15),
            TimestampedValue(
                beam.Row(order_id='o_paid', ts=25, event_type='payment'), 25),
        ]).advance_watermark_to(20).advance_watermark_to(
            45).advance_watermark_to_infinity())

    p2 = beam.Pipeline(options=opts)
    out2 = (
        p2
        | 'NegTS' >> ts_neg
        | 'NegCEP' >> cep.CliPatternMatch(
            key='order_id',
            timestamp_col='ts',
            seq='checkout -> !payment within 20s',
            force_stateful=True,
        ))
    assert_that(
        out2 | beam.Map(lambda r: (r.order_id, r.matched_steps)),
        equal_to([('o_abandoned', 'checkout,!payment')]),
    )
    p2.run().wait_until_finish()

  def test_max_active_matches_per_key_overflow_dlq_and_redaction(self):
    p = beam.Pipeline(runner=fn_runner.FnApiRunner())
    # 3 starts within window when max_active_matches_per_key=2 -> 3rd goes to DLQ!
    rows = [
        beam.Row(uid='u_dos', ts=1, event_type='start', card='CARD-1'),
        beam.Row(uid='u_dos', ts=2, event_type='start', card='CARD-2'),
        beam.Row(uid='u_dos', ts=3, event_type='start', card='TOP-SECRET-CARD'),
        beam.Row(uid='u_dos', ts=5, event_type='finish', card='NONE'),
    ]
    res = (
        p
        | beam.Create(rows)
        | cep.CliPatternMatch(
            key='uid',
            timestamp_col='ts',
            seq='start -> finish within 60s',
            max_active_matches_per_key=2,
            overflow_action='dlq',
            dlq_redact_fields=['card'],
            force_stateful=True,
            error_handling={'output': 'errors'},
        ))
    assert_that(
        res['good'] | 'GoodCnt' >> beam.Map(lambda r: r.uid),
        equal_to(['u_dos']),
        label='CheckGood',
    )
    assert_that(
        res['errors']
        | 'BadCheck' >> beam.Map(
            lambda r: (
                r.error_type,
                'TOP-SECRET-CARD' not in r.raw_payload,
                '<REDACTED>' in r.raw_payload, )),
        equal_to([('PatternStateOverflowError', True, True)]),
        label='CheckDlq',
    )
    p.run().wait_until_finish()


class CliPatternMatchCliEndToEndTest(unittest.TestCase):
  def test_cli_pipe_pattern_where_select_to_parquet_and_package(self):
    with tempfile.TemporaryDirectory() as tmp:
      in_p = os.path.join(tmp, 'events.jsonl')
      out_p = os.path.join(tmp, 'alerts.parquet')
      dlq_p = os.path.join(tmp, 'dlq.jsonl')

      events = [
          {
              'user_id': 'alice',
              'ts': '2026-06-01T12:00:00Z',
              'action': 'login',
              'status': 'fail',
              'ip': '10.0.0.1',
              'amount': 0.0,
          },
          {
              'user_id': 'alice',
              'ts': '2026-06-01T12:00:15Z',
              'action': 'login',
              'status': 'fail',
              'ip': '10.0.0.2',
              'amount': 0.0,
          },
          {
              'user_id': 'alice',
              'ts': '2026-06-01T12:00:40Z',
              'action': 'transfer',
              'status': 'ok',
              'ip': '10.0.0.2',
              'amount': 2500.0,
          },
          # Bob's transfer is only 50.0 (<= 500) -> does NOT match!
          {
              'user_id': 'bob',
              'ts': '2026-06-01T12:00:00Z',
              'action': 'login',
              'status': 'fail',
              'ip': '10.0.1.1',
              'amount': 0.0,
          },
          {
              'user_id': 'bob',
              'ts': '2026-06-01T12:00:10Z',
              'action': 'login',
              'status': 'fail',
              'ip': '10.0.1.1',
              'amount': 0.0,
          },
          {
              'user_id': 'bob',
              'ts': '2026-06-01T12:00:20Z',
              'action': 'transfer',
              'status': 'ok',
              'ip': '10.0.1.1',
              'amount': 50.0,
          },
      ]
      with open(in_p, 'w', encoding='utf-8') as f:
        for ev in events:
          f.write(json.dumps(ev) + '\n')

      pat_spec = (
          'key=user_id,seq=fail_login{2} -> high_transfer within 60s,'
          "where=[fail_login: action == 'login' AND status == 'fail', "
          "high_transfer: action == 'transfer' AND amount > 500],"
          'select=[user_id=high_transfer.user_id,ip1=fail_login_1.ip,'
          'ip2=fail_login_2.ip,amount=high_transfer.amount,'
          'start_ts=pattern_start_ts]')

      # `--assert-counts` rejected pre-flight (`rc == 2`) on `--pattern`
      rc_ac, _, _ = _run_cli([
          'pipe',
          '--from',
          in_p,
          '--to',
          out_p,
          '--timestamp-col',
          'ts',
          '--pattern',
          pat_spec,
          '--assert-counts',
          '--json',
      ])
      self.assertEqual(rc_ac, 2)

      rc, out, err = _run_cli([
          'pipe',
          '--from',
          in_p,
          '--to',
          out_p,
          '--timestamp-col',
          'ts',
          '--pattern',
          pat_spec,
          '--dlq',
          dlq_p,
          '--json',
      ])
      self.assertEqual(rc, 0, f'STDOUT: {out}\nSTDERR: {err}')
      env = json.loads(out)
      self.assertEqual(env['metrics'].get('patterns_matched'), 1)
      self.assertEqual(env['metrics'].get('rows_written'), 1)

      rows = pq.read_table(out_p).to_pylist()
      self.assertEqual(
          rows,
          [{
              'user_id': 'alice',
              'ip1': '10.0.0.1',
              'ip2': '10.0.0.2',
              'amount': 2500.0,
              'start_ts': '2026-06-01T12:00:00Z',
          }],
      )

      # Verify `--dry-run`, `beam check --plan`, and `beam package --sdk=auto`
      rc_dry, yaml_s, err_dry = _run_cli([
          'pipe',
          '--from',
          in_p,
          '--to',
          os.path.join(tmp, 'dry.parquet'),
          '--timestamp-col',
          'ts',
          '--pattern',
          pat_spec,
          '--dry-run',
      ])
      self.assertEqual(rc_dry, 0, err_dry)
      yaml_path = os.path.join(tmp, 'cep.yaml')
      with open(yaml_path, 'w', encoding='utf-8') as f:
        f.write(yaml_s)
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


if __name__ == '__main__':
  unittest.main()
