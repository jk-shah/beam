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

"""Hermetic unit, `TestStream`, and CLI E2E tests for `B2.0` & `B2.3` (`CliWindowInto`)."""

from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from unittest import mock

import pyarrow.parquet as pq

import apache_beam as beam
from apache_beam.cli import check_cmd
from apache_beam.cli import jobs
from apache_beam.cli import main as cli_main
from apache_beam.cli import package as package_mod
from apache_beam.cli import windowing
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners import pipeline_context
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import core
from apache_beam.transforms import trigger
from apache_beam.transforms.window import TimestampedValue


def _run_cli(argv: list[str]) -> tuple[int, str, str]:
  out_buf = io.StringIO()
  err_buf = io.StringIO()
  with mock.patch('sys.stdout', out_buf), mock.patch('sys.stderr', err_buf):
    rc = cli_main.main(argv)
  return rc, out_buf.getvalue(), err_buf.getvalue()


class CliWindowIntoConfigAndTriggerSafetyTest(unittest.TestCase):
  def test_trigger_parsing_proto_int64_and_gbk_may_lose_data_safety(self):
    ctx = pipeline_context.PipelineContext()
    safe_cases = [
        {
            'windowing': {
                'type': 'fixed',
                'size': '60s',
            },
            'allowed_lateness': '10m',
        },
        {
            'windowing': {
                'type': 'fixed',
                'size': '60s',
            },
            'allowed_lateness': '5m',
            'trigger': 'watermark,early=every:10s',
            'accumulation': 'accumulating',
        },
        {
            'windowing': {
                'type': 'sliding',
                'size': '5m',
                'period': '1m',
            },
            'allowed_lateness': '2m',
            'trigger': 'watermark,early=any:(every:5s,count:50),late=every:30s',
        },
        {
            'windowing': {
                'type': 'global',
            },
            'trigger': 'every:15s',
        },
        {
            'windowing': {
                'type': 'global',
            },
            'trigger': 'count:100',
        },
        {
            'windowing': {
                'type': 'global',
            },
            'trigger': 'any:(every:10s,count:25)',
        },
    ]
    for raw in safe_cases:
      with self.subTest(raw=raw):
        cfg = windowing.parse_windowing_config(raw)
        wfn = windowing.build_window_fn(cfg)
        tfn, acc = windowing.build_trigger_and_accumulation(cfg)
        w_obj = core.Windowing(
            wfn,
            triggerfn=tfn,
            accumulation_mode=acc,
            allowed_lateness=cfg.allowed_lateness_sec,
        )
        # Must serialize cleanly to Runner API protobuf (verifies int64 delay_millis)
        proto = w_obj.to_runner_api(ctx)
        self.assertIsNotNone(proto)
        # Must NEVER fail GroupByKey's `Unsafe trigger: MAY_FINISH` gate!
        self.assertEqual(
            w_obj.triggerfn.may_lose_data(w_obj),
            trigger.DataLossReason.NO_POTENTIAL_LOSS,
        )

    invalid_cases = [
        # Sub-second AfterProcessingTime rejected to protect int64 delay_millis
        {
            'windowing': {
                'type': 'fixed',
                'size': '60s',
            },
            'trigger': 'every:500ms',
        },
        {
            'windowing': {
                'type': 'fixed',
                'size': '60s',
            },
            'trigger': 'watermark,early=every:0.5s',
        },
        # Global windows incompatible with allowed_lateness > 0, watermark trigger, or late_action=dlq
        {
            'windowing': {
                'type': 'global',
            },
            'allowed_lateness': '10s',
        },
        {
            'windowing': {
                'type': 'global',
            },
            'trigger': 'watermark',
        },
        {
            'windowing': {
                'type': 'global',
            },
            'late_action': 'drop',
        },
        # Unknown options rejected
        {
            'windowing': {
                'type': 'fixed',
                'size': '60s',
            },
            'bogus_opt': True,
        },
    ]
    for bad in invalid_cases:
      with self.subTest(bad=bad):
        with self.assertRaises(ValueError):
          windowing.parse_windowing_config(bad)


class CliWindowIntoTestStreamTest(unittest.TestCase):
  def test_teststream_early_ontime_late_panes_and_droppably_late_dlq(self):
    cfg = windowing.parse_windowing_config({
        'windowing': {
            'type': 'fixed',
            'size': '60s',
        },
        'allowed_lateness': '15s',
        'trigger': 'watermark,early=count:2,late=count:1',
        'accumulation': 'accumulating',
        'late_action': 'dlq',
        'dlq_redact_fields': ['ssn'],
    })

    # Window [0, 60) closes at horizon = 60 + 15 = 75s (expires at 75s + 1us).
    ts = (
        TestStream().advance_watermark_to(0).add_elements([
            TimestampedValue(beam.Row(acct='a1', amt=10, ssn='111-11-1111'), 5),
            TimestampedValue(
                beam.Row(acct='a1', amt=20, ssn='111-11-1111'), 12),
        ])
        # Advancing watermark to 65s (> 60s end, <= 75s horizon) must fire ON_TIME
        # immediately without being held back to 75s by HORIZON_TIMER!
        .advance_watermark_to(65)
        # Within-lateness arrival at t=25s while watermark=65s <= 75s -> LATE pane!
        .add_elements([
            TimestampedValue(beam.Row(acct='a1', amt=5, ssn='111-11-1111'), 25),
        ])
        # Advance watermark past closing horizon 75s -> 80s (fires HORIZON_TIMER)
        .advance_watermark_to(80)
        # Droppably-late arrival at t=30s while watermark=80s > 75s -> DLQ!
        .add_elements([
            TimestampedValue(
                beam.Row(acct='a1', amt=999, ssn='999-SECRET-SSN'), 30),
        ]).advance_watermark_to_infinity())

    opts = PipelineOptions(['--runner=DirectRunner', '--streaming'])
    p = beam.Pipeline(options=opts)
    win_res = (
        p
        | ts
        | windowing.CliWindowInto(cfg, error_handling={'output': 'errors'}))
    panes = (
        win_res['good']
        | beam.Map(lambda r: (r.acct, r.amt))
        | beam.CombinePerKey(sum)
        | beam.Map(lambda kv: beam.Row(acct=kv[0], total=kv[1]))
        | windowing.CliExtractPaneInfo())

    # Under accumulating mode:
    # - EARLY (or ON_TIME if bundled) has <= 30, LATE pane has 35, and 999 is NEVER included!
    assert_that(
        panes
        | 'Summary' >> beam.Map(
            lambda r:
            (r.acct, r.total, r.pane_timing, r.window_start, r.window_end)),
        equal_to([
            ('a1', 30, 'EARLY', '1970-01-01T00:00:00Z', '1970-01-01T00:01:00Z'),
            (
                'a1',
                30,
                'ON_TIME',
                '1970-01-01T00:00:00Z',
                '1970-01-01T00:01:00Z'),
            ('a1', 35, 'LATE', '1970-01-01T00:00:00Z', '1970-01-01T00:01:00Z'),
        ]),
        label='CheckPanes',
    )
    assert_that(
        win_res['errors']
        | 'CheckDlq' >> beam.Map(
            lambda r: (
                r.step,
                r.error_type,
                '999-SECRET-SSN' not in r.raw_payload,
                '<REDACTED>' in r.raw_payload, )),
        equal_to([('CliWindowInto', 'LateDataError', True, True)]),
        label='CheckLateDlq',
    )
    pres = p.run()
    pres.wait_until_finish()

  def test_teststream_discarding_mode_emits_deltas_and_late_drop_counts(self):
    cfg = windowing.parse_windowing_config({
        'windowing': {
            'type': 'fixed',
            'size': '60s',
        },
        'allowed_lateness': '15s',
        'trigger': 'watermark,early=count:2,late=count:1',
        'accumulation': 'discarding',
        'late_action': 'drop',
    })
    ts = (
        TestStream().advance_watermark_to(0).add_elements([
            TimestampedValue(beam.Row(acct='a1', amt=10), 5),
            TimestampedValue(beam.Row(acct='a1', amt=20), 12),
        ]).advance_watermark_to(65).add_elements([
            TimestampedValue(beam.Row(acct='a1', amt=5), 25),
        ]).advance_watermark_to(80).add_elements([
            TimestampedValue(beam.Row(acct='a1', amt=999), 30),
        ]).advance_watermark_to_infinity())
    opts = PipelineOptions(['--runner=DirectRunner', '--streaming'])
    p = beam.Pipeline(options=opts)
    totals = (
        p
        | ts
        | windowing.CliWindowInto(cfg)
        | beam.Map(lambda r: (r.acct, r.amt))
        | beam.CombinePerKey(sum))
    # Under discarding mode: EARLY emits 30, ON_TIME emits 0 (empty delta), LATE emits delta 5!
    assert_that(totals, equal_to([('a1', 30), ('a1', 0), ('a1', 5)]))
    pres = p.run()
    pres.wait_until_finish()

    # Verify `late_rows_dropped` and `late_panes_emitted` metrics on FnApiRunner
    # (BundleBasedDirectRunner caches Bundle 1's MetricsContainer in StateSampler
    # at `runners/worker/statesampler.py:158-159`, whereas FnApiRunner/Prism/Dataflow
    # allocate fresh per-bundle containers).
    from apache_beam.runners.portability.fn_api_runner import fn_runner
    from apache_beam.transforms.window import GlobalWindow
    from apache_beam.transforms.window import WindowedValue
    from apache_beam.utils import windowed_value
    fn_p = beam.Pipeline(runner=fn_runner.FnApiRunner())
    drop_cfg = windowing.parse_windowing_config({
        'windowing': {
            'type': 'fixed',
            'size': '60s',
        },
        'allowed_lateness': '10s',
        'late_action': 'drop',
        'watermark_lag': '5s',
    })
    _ = (
        fn_p
        | beam.Create([
            TimestampedValue(beam.Row(id=1), 10),
            TimestampedValue(beam.Row(id=2), 90),
            TimestampedValue(beam.Row(id=3), 20),
        ])
        | windowing.CliWindowInto(drop_cfg))
    late_pi = windowed_value.PaneInfo(
        True, True, windowed_value.PaneInfoTiming.LATE, 0, 0)
    _ = (
        fn_p
        | 'CreateLatePane' >> beam.Create([beam.Row(v=1)])
        | beam.Map(lambda r: WindowedValue(r, 0, (GlobalWindow(), ), late_pi))
        | windowing.CliExtractPaneInfo())
    fn_res = fn_p.run()
    fn_res.wait_until_finish()
    fn_metrics = jobs.extract_beam_cli_counters(fn_res.metrics())
    self.assertEqual(fn_metrics.get('late_rows_dropped'), 1)
    self.assertEqual(fn_metrics.get('rows_filtered_out'), 1)
    self.assertEqual(fn_metrics.get('late_panes_emitted'), 1)


class CliWindowIntoEndToEndPipeTest(unittest.TestCase):
  def test_cli_pipe_window_combine_and_emit_pane_info_to_parquet(self):
    with tempfile.TemporaryDirectory() as tmp:
      src = os.path.join(tmp, 'events.jsonl')
      dst = os.path.join(tmp, 'windowed.parquet')
      rows = [
          {
              'ts': '2026-01-01T00:00:10Z', 'region': 'us', 'amount': 10.0
          },
          {
              'ts': '2026-01-01T00:00:25Z', 'region': 'us', 'amount': 15.0
          },
          {
              'ts': '2026-01-01T00:01:05Z', 'region': 'eu', 'amount': 40.0
          },
      ]
      with open(src, 'w', encoding='utf-8') as f:
        for r in rows:
          f.write(json.dumps(r) + '\n')

      rc, out, err = _run_cli([
          'pipe',
          '--from',
          src,
          '--to',
          dst,
          '--timestamp-col',
          'ts',
          '--window',
          'fixed:60s,group_by=region,total=sum(amount)',
          '--allowed-lateness',
          '10s',
          '--trigger',
          'watermark,early=count:5',
          '--emit-pane-info',
          '--select',
          'region, total, window_start, window_end, pane_timing, pane_index',
          '--json',
      ])
      self.assertEqual(rc, 0, f'STDOUT: {out}\nSTDERR: {err}')
      env = json.loads(out)
      # Omitting --accumulation with --trigger + Combine emits helpful warning
      self.assertTrue(
          any(
              '--accumulation=discarding' in w
              for w in env.get('warnings', [])),
          env.get('warnings'),
      )

      tbl = pq.read_table(dst).to_pylist()
      by_reg = {r['region']: r for r in tbl}
      self.assertEqual(set(by_reg), {'us', 'eu'})
      self.assertAlmostEqual(by_reg['us']['total'], 25.0)
      self.assertEqual(by_reg['us']['window_start'], '2026-01-01T00:00:00Z')
      self.assertEqual(by_reg['us']['window_end'], '2026-01-01T00:01:00Z')
      self.assertEqual(by_reg['eu']['total'], 40.0)
      self.assertEqual(by_reg['eu']['window_start'], '2026-01-01T00:01:00Z')

  def test_cli_late_data_dlq_and_late_drop_row_conservation(self):
    with tempfile.TemporaryDirectory() as tmp:
      src = os.path.join(tmp, 'stream.jsonl')
      dst_dlq_run = os.path.join(tmp, 'out_dlq.jsonl')
      late_dlq = os.path.join(tmp, 'late_dlq.jsonl')
      main_dlq = os.path.join(tmp, 'main_dlq.jsonl')

      # Simulate bounded stream with watermark_lag=5s:
      # - id=1 at t=10s (w=[0,60), max_ts=10, wm=5) -> good
      # - id=2 at t=90s (w=[60,120), max_ts=90, wm=85) -> good
      # - id=3 at t=20s (w=[0,60), horizon=70s <= wm=85s!) -> LATE!
      # - bad JSON line -> routed to main_dlq!
      with open(src, 'w', encoding='utf-8') as f:
        f.write('{"id": 1, "ts": 10, "ssn": "111"}\n')
        f.write('{"id": 2, "ts": 90, "ssn": "222"}\n')
        f.write('{"id": 3, "ts": 20, "ssn": "999-LATE-SECRET"}\n')
        f.write('{corrupt_json_line\n')

      # Build pipe spec with dual `--dlq` + `--late-data-dlq` + `watermark_lag='5s'`
      from apache_beam.cli import pipe_spec
      ps = pipe_spec.build_pipe_spec(
          src,
          dst_dlq_run,
          dlq_sink=main_dlq,
          late_data_dlq=late_dlq,
          timestamp_col='ts',
          window='fixed:60s',
          allowed_lateness='10s',
          watermark_lag='5s',
          dlq_redact_fields='ssn',
          include_counters=True,
      )
      yaml_path = os.path.join(tmp, 'dual_dlq.yaml')
      import yaml as pyyaml
      with open(yaml_path, 'w', encoding='utf-8') as f:
        pyyaml.safe_dump(ps.spec, f, sort_keys=False)

      rc_run, out_run, err_run = _run_cli(['run', yaml_path, '--json'])
      self.assertEqual(rc_run, 0, f'STDOUT: {out_run}\nSTDERR: {err_run}')
      env_run = json.loads(out_run)
      m = env_run['metrics']
      self.assertEqual(m['rows_read'], 4)
      self.assertEqual(m['dlq_rows'], 2)  # 1 corrupt JSON + 1 LateDataError
      self.assertEqual(m['rows_written'], 2)
      self.assertIsNone(jobs.check_assert_counts(m))

      # Also test CLI `--late-data-dlq` standalone dry-run + check + package!
      clean_src = os.path.join(tmp, 'clean_stream.jsonl')
      with open(clean_src, 'w', encoding='utf-8') as f:
        f.write('{"id": 1, "ts": 10, "ssn": "111"}\n')
        f.write('{"id": 2, "ts": 90, "ssn": "222"}\n')

      rc_dry, yaml_dry, err_dry = _run_cli([
          'pipe',
          '--from',
          clean_src,
          '--to',
          os.path.join(tmp, 'dry_out.parquet'),
          '--timestamp-col',
          'ts',
          '--window',
          'fixed:60s',
          '--allowed-lateness',
          '10s',
          '--late-data-dlq',
          os.path.join(tmp, 'standalone_late.jsonl'),
          '--emit-pane-info',
          '--dry-run',
      ])
      self.assertEqual(rc_dry, 0, err_dry)
      self.assertIn('CliWindowInto', yaml_dry)
      self.assertIn('CliExtractPaneInfo', yaml_dry)
      self.assertIn('WriteLateDlq', yaml_dry)

      dry_yaml_path = os.path.join(tmp, 'window_pipe.yaml')
      with open(dry_yaml_path, 'w', encoding='utf-8') as f:
        f.write(yaml_dry)

      chk = check_cmd.run_check(
          dry_yaml_path, expand_plan=True, trust_providers=False)
      self.assertTrue(chk['valid'])

      pkg = package_mod.run_package(
          dry_yaml_path,
          to_location=os.path.join(tmp, 'spec.json'),
          sdk='auto',
          output_dir=os.path.join(tmp, 'bundle'),
          dry_run=True,
      )
      self.assertEqual(pkg['sdk_language'], 'PYTHON')

  def test_b20_assert_counts_preflight_cardinality_guards(self):
    with tempfile.TemporaryDirectory() as tmp:
      src = os.path.join(tmp, 'in.jsonl')
      with open(src, 'w', encoding='utf-8') as f:
        f.write('{"ts": 10, "acct": "a", "amt": 5}\n')
      dst = os.path.join(tmp, 'out.jsonl')

      rejected_argvs = [
          # 1. Windowed Combine collapses rows
          [
              'pipe',
              '--from',
              src,
              '--to',
              dst,
              '--timestamp-col',
              'ts',
              '--window',
              'fixed:60s,group_by=acct,total=sum(amt)',
              '--assert-counts',
              '--json'
          ],
          # 2. Sliding window fans out 1 row to S/P windows
          [
              'pipe',
              '--from',
              src,
              '--to',
              dst,
              '--timestamp-col',
              'ts',
              '--window',
              'sliding:5m,1m',
              '--assert-counts',
              '--json'
          ],
          # 3. Session window merges windows
          [
              'pipe',
              '--from',
              src,
              '--to',
              dst,
              '--timestamp-col',
              'ts',
              '--window',
              'session:30m',
              '--assert-counts',
              '--json'
          ],
          # 4. Multi-pane trigger emits multiple panes per window
          [
              'pipe',
              '--from',
              src,
              '--to',
              dst,
              '--timestamp-col',
              'ts',
              '--window',
              'fixed:60s',
              '--trigger',
              'watermark,early=count:1',
              '--assert-counts',
              '--json'
          ],
          # 5. Standalone processing-time trigger on bounded input
          [
              'pipe',
              '--from',
              src,
              '--to',
              dst,
              '--window',
              'global',
              '--trigger',
              'every:10s',
              '--json'
          ],
      ]
      for argv in rejected_argvs:
        with self.subTest(argv=argv):
          rc, out, _ = _run_cli(argv)
          self.assertEqual(rc, 2, out)

      # Conversely, 1:1 fixed window + allowed-lateness WITHOUT Combine or
      # multi-pane triggers DOES conserve 1:1 row counts!
      rc_ok, out_ok, err_ok = _run_cli([
          'pipe',
          '--from',
          src,
          '--to',
          dst,
          '--timestamp-col',
          'ts',
          '--window',
          'fixed:60s',
          '--allowed-lateness',
          '10s',
          '--assert-counts',
          '--json',
      ])
      self.assertEqual(rc_ok, 0, f'STDOUT: {out_ok}\nSTDERR: {err_ok}')


if __name__ == '__main__':
  unittest.main()
