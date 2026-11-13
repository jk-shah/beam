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

"""Unit tests for declarative event-time interval joins (`sdks/python/apache_beam/cli/interval_join.py`)."""

from __future__ import annotations

import datetime
import json
import os
import tempfile
import unittest

from apache_beam.cli import dlq
from apache_beam.cli import interval_join
from apache_beam.cli import pipe_sql
from apache_beam.cli import uris


class IntervalDurationParseTest(unittest.TestCase):
  """Tests interval duration parsing across seconds, minutes, hours, days, ms."""

  def test_parse_valid_durations(self):
    self.assertEqual(interval_join.parse_interval_duration(0), 0.0)
    self.assertEqual(interval_join.parse_interval_duration('0s'), 0.0)
    self.assertEqual(interval_join.parse_interval_duration('15s'), 15.0)
    self.assertEqual(interval_join.parse_interval_duration('500ms'), 0.5)
    self.assertEqual(interval_join.parse_interval_duration('1m'), 60.0)
    self.assertEqual(interval_join.parse_interval_duration('15m'), 900.0)
    self.assertEqual(interval_join.parse_interval_duration('2h'), 7200.0)
    self.assertEqual(interval_join.parse_interval_duration('1d'), 86400.0)
    self.assertEqual(interval_join.parse_interval_duration('-5m'), -300.0)
    self.assertEqual(interval_join.parse_interval_duration('900'), 900.0)

  def test_parse_invalid_durations(self):
    with self.assertRaises(ValueError):
      interval_join.parse_interval_duration('')
    with self.assertRaises(ValueError):
      interval_join.parse_interval_duration('invalid')
    with self.assertRaises(ValueError):
      interval_join.parse_interval_duration('100lightyears')


class IntervalJoinConfigTest(unittest.TestCase):
  """Tests configuration parsing and validation."""

  def test_valid_config_defaults(self):
    cfg = interval_join.parse_interval_join_config({
        'key': 'user_id',
        'left_time': 'click_time',
        'right_time': 'order_time',
        'upper_bound': '15m',
    })
    self.assertEqual(cfg.left_key, ('user_id',))
    self.assertEqual(cfg.right_key, ('user_id',))
    self.assertEqual(cfg.lower_bound_sec, 0.0)
    self.assertEqual(cfg.upper_bound_sec, 900.0)
    self.assertEqual(cfg.join_type, 'inner')

  def test_valid_asymmetric_keys_and_bounds(self):
    cfg = interval_join.parse_interval_join_config({
        'left_key': 'uid',
        'right_key': 'customer_id',
        'left_time': 'created_at',
        'right_time': 'purchased_at',
        'lower_bound': '-1m',
        'upper_bound': '10m',
        'join_type': 'left',
    })
    self.assertEqual(cfg.left_key, ('uid',))
    self.assertEqual(cfg.right_key, ('customer_id',))
    self.assertEqual(cfg.lower_bound_sec, -60.0)
    self.assertEqual(cfg.upper_bound_sec, 600.0)
    self.assertEqual(cfg.join_type, 'left')

  def test_invalid_bounds_inverted(self):
    with self.assertRaises(ValueError) as ctx:
      interval_join.parse_interval_join_config({
          'key': 'user_id',
          'left_time': 'click_time',
          'right_time': 'order_time',
          'lower_bound': '15m',
          'upper_bound': '5m',
      })
    self.assertIn('lower_bound', str(ctx.exception))

  def test_missing_keys(self):
    with self.assertRaises(ValueError):
      interval_join.parse_interval_join_config({
          'left_time': 'click_time',
          'right_time': 'order_time',
          'upper_bound': '15m',
      })

  def test_missing_timestamps(self):
    with self.assertRaises(ValueError):
      interval_join.parse_interval_join_config({
          'key': 'user_id',
          'upper_bound': '15m',
      })

  def test_invalid_join_type(self):
    with self.assertRaises(ValueError):
      interval_join.parse_interval_join_config({
          'key': 'user_id',
          'left_time': 'click_time',
          'right_time': 'order_time',
          'upper_bound': '15m',
          'join_type': 'cross',
      })


class RecordExtractionTest(unittest.TestCase):
  """Tests key and timestamp extraction across rows, dicts, ISO strings, and epoch numbers."""

  def test_extract_key(self):
    d = {'user_id': 42, 'name': 'Alice'}
    self.assertEqual(interval_join.extract_record_key(d, 'user_id'), (42,))

  def test_extract_timestamp_epoch_sec(self):
    self.assertEqual(
        interval_join.extract_record_timestamp({'ts': 1700000000}, 'ts'),
        1700000000.0)

  def test_extract_timestamp_epoch_ms(self):
    self.assertEqual(
        interval_join.extract_record_timestamp({'ts': 1700000000000}, 'ts'),
        1700000000.0)

  def test_extract_timestamp_iso(self):
    iso_str = '2026-03-31T12:00:00Z'
    expected = datetime.datetime(2026, 3, 31, 12, 0, 0, tzinfo=datetime.timezone.utc).timestamp()
    self.assertEqual(
        interval_join.extract_record_timestamp({'ts': iso_str}, 'ts'),
        expected)

  def test_extract_timestamp_datetime_obj(self):
    dt = datetime.datetime(2026, 3, 31, 12, 0, 0, tzinfo=datetime.timezone.utc)
    self.assertEqual(
        interval_join.extract_record_timestamp({'ts': dt}, 'ts'),
        dt.timestamp())

  def test_extract_timestamp_non_finite_rejection(self):
    with self.assertRaises(ValueError) as ctx_nan:
      interval_join.extract_record_timestamp({'ts': float('nan')}, 'ts')
    self.assertIn('finite numeric value', str(ctx_nan.exception))

    with self.assertRaises(ValueError) as ctx_inf:
      interval_join.extract_record_timestamp({'ts': float('inf')}, 'ts')
    self.assertIn('finite numeric value', str(ctx_inf.exception))


class BoundedIntervalJoinAlgorithmTest(unittest.TestCase):
  """Tests in-memory join algorithm for inner, left, and full outer joins."""

  def setUp(self):
    self.cfg = interval_join.parse_interval_join_config({
        'key': 'user_id',
        'left_time': 'click_time',
        'right_time': 'order_time',
        'lower_bound': '0s',
        'upper_bound': '15m',  # 900s
        'join_type': 'inner',
    })

    self.left_records = [
        {'user_id': 1, 'click_time': 1000, 'campaign': 'spring_sale'},
        {'user_id': 2, 'click_time': 2000, 'campaign': 'summer_promo'},
    ]
    self.right_records = [
        {'user_id': 1, 'order_time': 1050, 'amount': 49.99},  # match click 1 (+50s <= 900s)
        {'user_id': 1, 'order_time': 2500, 'amount': 99.99},  # too late for click 1 (>900s)
        {'user_id': 2, 'order_time': 1950, 'amount': 19.99},  # before click 2 (<0s)
    ]

  def test_inner_join(self):
    joined = interval_join.join_bounded_records(
        self.left_records, self.right_records, self.cfg)
    self.assertEqual(len(joined), 1)
    res = joined[0]
    self.assertEqual(res['user_id'], 1)
    self.assertEqual(res['campaign'], 'spring_sale')
    self.assertEqual(res['amount'], 49.99)

  def test_left_join(self):
    left_cfg = interval_join.parse_interval_join_config({
        'key': 'user_id',
        'left_time': 'click_time',
        'right_time': 'order_time',
        'lower_bound': '0s',
        'upper_bound': '15m',
        'join_type': 'left',
    })
    joined = interval_join.join_bounded_records(
        self.left_records, self.right_records, left_cfg)
    self.assertEqual(len(joined), 2)
    # User 1 has match
    u1 = [r for r in joined if r['user_id'] == 1][0]
    self.assertEqual(u1['amount'], 49.99)
    # User 2 has no match in right -> amount is None
    u2 = [r for r in joined if r['user_id'] == 2][0]
    self.assertIsNone(u2.get('amount'))
    self.assertEqual(u2['campaign'], 'summer_promo')

  def test_full_join(self):
    full_cfg = interval_join.parse_interval_join_config({
        'key': 'user_id',
        'left_time': 'click_time',
        'right_time': 'order_time',
        'lower_bound': '0s',
        'upper_bound': '15m',
        'join_type': 'full',
    })
    joined = interval_join.join_bounded_records(
        self.left_records, self.right_records, full_cfg)
    # 1 matched pair + 1 unmatched left (user 2) + 2 unmatched rights (user 1 order 2500, user 2 order 1950) = 4 records
    self.assertEqual(len(joined), 4)


class BeamPipelineIntervalJoinTest(unittest.TestCase):
  """Tests end-to-end execution of CliIntervalJoin in an Apache Beam pipeline."""
  def test_beam_pipeline_execution(self):
    import apache_beam as beam
    from apache_beam.testing.test_pipeline import TestPipeline
    from apache_beam.testing.util import assert_that
    from apache_beam.testing.util import equal_to

    left_data = [
        {'uid': 'u1', 'click_ts': 1000, 'campaign': 'c1'},
        {'uid': 'u2', 'click_ts': 5000, 'campaign': 'c2'},
    ]
    right_data = [
        {'uid': 'u1', 'purchase_ts': 1200, 'rev': 50.0},
        {'uid': 'u1', 'purchase_ts': 9999, 'rev': 20.0},
    ]

    with TestPipeline(runner='DirectRunner') as p:
      left_pc = p | 'CreateLeft' >> beam.Create(left_data)
      right_pc = p | 'CreateRight' >> beam.Create(right_data)

      joined = {
          'left': left_pc, 'right': right_pc
      } | interval_join.CliIntervalJoin(
          key='uid',
          left_time='click_ts',
          right_time='purchase_ts',
          lower_bound='0s',
          upper_bound='10m',
      )

      tuples = joined | beam.Map(lambda r: (r.uid, r.campaign, r.rev))
      assert_that(tuples, equal_to([('u1', 'c1', 50.0)]))


class SqlIntervalJoinTest(unittest.TestCase):
  """Tests in-memory SQLite and Beam SQL interval join queries."""

  def test_sqlite_interval_sql_execution(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      clicks_file = os.path.join(tmpdir, 'clicks.jsonl')
      orders_file = os.path.join(tmpdir, 'orders.jsonl')

      with open(clicks_file, 'w', encoding='utf-8') as f:
        f.write(json.dumps({'user_id': 101, 'click_time': 1000, 'ad': 'search'}) + '\n')
        f.write(json.dumps({'user_id': 102, 'click_time': 2000, 'ad': 'display'}) + '\n')

      with open(orders_file, 'w', encoding='utf-8') as f:
        f.write(json.dumps({'user_id': 101, 'order_time': 1060, 'total': 88.50}) + '\n')
        f.write(json.dumps({'user_id': 101, 'order_time': 3000, 'total': 12.00}) + '\n')
        f.write(json.dumps({'user_id': 102, 'order_time': 1900, 'total': 45.00}) + '\n')

      named_sources = pipe_sql.parse_named_sources(
          [f'clicks={clicks_file}', f'orders={orders_file}'],
          raw_joins=None,
      )

      query = """
      SELECT c.user_id, c.ad, o.total, o.order_time - c.click_time AS lag_sec
      FROM clicks AS c
      JOIN orders AS o
        ON c.user_id = o.user_id
       AND o.order_time BETWEEN c.click_time AND c.click_time + INTERVAL '15' MINUTE
      """

      out_cols, out_rows, dlq_rows, total_read = pipe_sql.run_in_memory_sql(
          named_sources,
          query,
      )

      self.assertEqual(len(out_rows), 1)
      row = out_rows[0]
      self.assertEqual(row['user_id'], 101)
      self.assertEqual(row['ad'], 'search')
      self.assertEqual(row['total'], 88.50)
      self.assertEqual(row['lag_sec'], 60)

  def test_sqlite_interval_sub_execution(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      events_file = os.path.join(tmpdir, 'events.jsonl')
      alerts_file = os.path.join(tmpdir, 'alerts.jsonl')

      with open(events_file, 'w', encoding='utf-8') as f:
        f.write(json.dumps({'device_id': 'd1', 'event_time': 1000, 'val': 99.2}) + '\n')

      with open(alerts_file, 'w', encoding='utf-8') as f:
        f.write(json.dumps({'device_id': 'd1', 'alert_time': 980, 'severity': 'HIGH'}) + '\n')

      named_sources = pipe_sql.parse_named_sources(
          [f'events={events_file}', f'alerts={alerts_file}'],
          raw_joins=None,
      )

      query = """
      SELECT e.device_id, e.val, a.severity
      FROM events AS e
      JOIN alerts AS a
        ON e.device_id = a.device_id
       AND a.alert_time BETWEEN e.event_time - INTERVAL '1' MINUTE AND e.event_time + INTERVAL 30 SECOND
      """

      out_cols, out_rows, _, _ = pipe_sql.run_in_memory_sql(named_sources, query)
      self.assertEqual(len(out_rows), 1)
      self.assertEqual(out_rows[0]['device_id'], 'd1')
      self.assertEqual(out_rows[0]['severity'], 'HIGH')


class YamlProviderRegistrationTest(unittest.TestCase):
  """Tests that CliIntervalJoin is correctly registered in Beam YAML provider spec."""
  def test_yaml_provider_registration(self):
    spec = dlq.cli_yaml_provider_spec()
    transforms = spec.get('transforms', {})
    self.assertIn('CliIntervalJoin', transforms)
    self.assertIn('IntervalJoin', transforms)
    self.assertEqual(
        transforms['CliIntervalJoin'],
        'apache_beam.cli.interval_join.cli_interval_join')
    self.assertEqual(
        transforms['IntervalJoin'],
        'apache_beam.cli.interval_join.cli_interval_join')


class WatermarkSkewCircuitBreakerTest(unittest.TestCase):
  """Tests watermark skew circuit breaker to prevent unbounded buffer growth."""
  def test_skew_config_parsing(self):
    cfg = interval_join.parse_interval_join_config({
        'key': 'user_id',
        'left_time': 'click_time',
        'right_time': 'order_time',
        'upper_bound': '15m',
        'max_watermark_skew': '1h',
        'skew_action': 'drop',
    })
    self.assertEqual(cfg.max_watermark_skew, 3600.0)
    self.assertEqual(cfg.skew_action, 'drop')

    cfg2 = interval_join.parse_interval_join_config({
        'key': 'user_id',
        'left_time': 'click_time',
        'right_time': 'order_time',
        'upper_bound': '15m',
        'max_skew': 600,
        'skew_action': 'warn',
    })
    self.assertEqual(cfg2.max_watermark_skew, 600.0)
    self.assertEqual(cfg2.skew_action, 'warn')

    with self.assertRaises(ValueError):
      interval_join.parse_interval_join_config({
          'key': 'user_id',
          'left_time': 'click_time',
          'right_time': 'order_time',
          'upper_bound': '15m',
          'max_watermark_skew': '-5m',
      })

    with self.assertRaises(ValueError):
      interval_join.parse_interval_join_config({
          'key': 'user_id',
          'left_time': 'click_time',
          'right_time': 'order_time',
          'upper_bound': '15m',
          'skew_action': 'invalid_action',
      })

  def test_bounded_skew_filtering(self):
    cfg = interval_join.parse_interval_join_config({
        'key': 'user_id',
        'left_time': 'click_time',
        'right_time': 'order_time',
        'lower_bound': '0s',
        'upper_bound': '2h',
        'max_watermark_skew': '15m',
    })
    left_records = [
        {'user_id': 1, 'click_time': 1000.0, 'ad': 'valid'},
        {'user_id': 1, 'click_time': 2000.0, 'ad': 'valid2'},
    ]
    right_records = [
        {'user_id': 1, 'order_time': 1060.0, 'total': 10.0},    # Skew = 60s <= 900s -> match
        {'user_id': 1, 'order_time': 5000.0, 'total': 99.0},    # Skew = 3000s > 900s -> pruned by breaker
    ]
    results = interval_join.join_bounded_records(left_records, right_records, cfg)
    self.assertEqual(len(results), 1)
    self.assertEqual(results[0]['total'], 10.0)
    self.assertEqual(results[0]['ad'], 'valid')

  def test_stateful_streaming_skew_circuit_breaker(self):
    import apache_beam as beam
    from apache_beam.testing.util import assert_that
    from apache_beam.testing.util import equal_to

    cfg = interval_join.parse_interval_join_config({
        'key': 'user_id',
        'left_time': 'click_time',
        'right_time': 'order_time',
        'lower_bound': '0s',
        'upper_bound': '10m',
        'max_watermark_skew': '60s',
        'skew_action': 'drop',
    })

    # Keyed stream elements: (key, (tag, timestamp, payload))
    stream_events = [
        (('u1',), ('L', 1000.0, {'user_id': 'u1', 'click_time': 1000.0, 'ad': 'valid'})),
        (('u1',), ('R', 1030.0, {'user_id': 'u1', 'order_time': 1030.0, 'total': 42.0})),
        # Excessively late event on L: arrives with t=500, but R watermark is already at 1030 (skew = 530s > 60s)
        (('u1',), ('L', 500.0, {'user_id': 'u1', 'click_time': 500.0, 'ad': 'severely_late'})),
    ]

    with beam.Pipeline() as p:
      results = (
          p
          | beam.Create(stream_events)
          | beam.ParDo(interval_join.IntervalJoinStatefulDoFn(cfg))
          | beam.Map(lambda r: (r.user_id, r.ad, r.total))
      )
      assert_that(results, equal_to([('u1', 'valid', 42.0)]))

  def test_stateful_streaming_non_finite_timestamp_dropped(self):
    import apache_beam as beam
    from apache_beam.testing.util import assert_that
    from apache_beam.testing.util import equal_to

    cfg = interval_join.parse_interval_join_config({
        'key': 'user_id',
        'left_time': 'click_time',
        'right_time': 'order_time',
        'lower_bound': '0s',
        'upper_bound': '10m',
    })

    # Keyed stream elements with NaN and inf timestamps mixed in with valid records
    stream_events = [
        (('u1',), ('L', 1000.0, {'user_id': 'u1', 'click_time': 1000.0, 'ad': 'valid'})),
        (('u1',), ('L', float('nan'), {'user_id': 'u1', 'click_time': float('nan'), 'ad': 'nan_ts'})),
        (('u1',), ('R', float('inf'), {'user_id': 'u1', 'order_time': float('inf'), 'total': -1.0})),
        (('u1',), ('R', 1030.0, {'user_id': 'u1', 'order_time': 1030.0, 'total': 42.0})),
    ]

    with beam.Pipeline() as p:
      results = (
          p
          | beam.Create(stream_events)
          | beam.ParDo(interval_join.IntervalJoinStatefulDoFn(cfg))
          | beam.Map(lambda r: (r.user_id, r.ad, r.total)))
      assert_that(results, equal_to([('u1', 'valid', 42.0)]))

  def test_select_safe_ast_projection_and_rce_rejection(self):
    cfg = interval_join.parse_interval_join_config({
        'key': 'user_id',
        'left_alias': 'c',
        'right_alias': 'o',
        'left_time': 'click_time',
        'right_time': 'order_time',
        'lower_bound': '0s',
        'upper_bound': '10m',
        'join_type': 'left',
        'select': {
            'uid': 'c.user_id',
            'lag': 'o.order_time - c.click_time',
            'net': 'coalesce(o.total, 0.0) - cost',
        },
    })
    matched = interval_join.project_joined_row(
        {
            'user_id': 'u1', 'click_time': 100, 'cost': 10.0
        }, {
            'user_id': 'u1', 'order_time': 145, 'total': 50.0
        },
        cfg)
    self.assertEqual(matched, {'uid': 'u1', 'lag': 45, 'net': 40.0})

    unmatched = interval_join.project_joined_row({
        'user_id': 'u2', 'click_time': 200, 'cost': 5.0
    },
                                                 None,
                                                 cfg)
    self.assertEqual(unmatched, {'uid': 'u2', 'lag': None, 'net': -5.0})

    for malicious_expr in (
        '().__class__.__bases__[0].__subclasses__()',
        '__import__("os").system("id")',
        '2 ** 64',
        '(lambda x: x)(1)',
    ):
      with self.subTest(expr=malicious_expr):
        with self.assertRaises(ValueError):
          interval_join.parse_interval_join_config({
              'key': 'user_id',
              'left_time': 'click_time',
              'right_time': 'order_time',
              'upper_bound': '5m',
              'select': {
                  'out_col': malicious_expr
              },
          })


class StatefulHardeningAndTimerTest(unittest.TestCase):
  """Tests multi-record GC re-arming, TimestampedValue, state drain, & skew DLQ."""
  def test_multi_record_gc_rearm_and_timestamped_outer_emission(self):
    import apache_beam as beam
    from apache_beam.runners.direct.direct_runner import BundleBasedDirectRunner
    from apache_beam.testing.test_stream import TestStream
    from apache_beam.testing.util import assert_that
    from apache_beam.testing.util import equal_to
    from apache_beam.transforms.window import TimestampedValue

    # Two unmatched left records for the SAME key ('k1') at t=10 and t=100
    # with upper_bound=20s:
    # - t=10 sets GC timer to 31s; t=100 has deadline 121s (> 31s).
    # - When watermark reaches 50s, on_gc emits t=10 (preserving ts=10!) AND
    #   must re-arm NEXT_GC_STATE/GC_TIMER to 121s so t=100 emits when WM=130s!
    # Single TestStream root branched into L and R so BundleBasedDirectRunner
    # advances both stream watermarks deterministically in lockstep:
    ts = (
        TestStream().advance_watermark_to(0).add_elements([
            TimestampedValue(
                ('L', {
                    'id': 'k1', 'l_ts': 10.0, 'l_val': 'first'
                }), 10),
            TimestampedValue(
                ('L', {
                    'id': 'k1', 'l_ts': 100.0, 'l_val': 'second'
                }), 100),
            TimestampedValue(('R', {
                'id': 'other', 'r_ts': 15.0, 'r_val': 9
            }),
                             15),
        ]).advance_watermark_to(50).advance_watermark_to(
            130).advance_watermark_to_infinity())

    with beam.Pipeline(runner=BundleBasedDirectRunner()) as p:
      raw = p | 'Stream' >> ts
      left_pc = (
          raw
          | 'FilterL' >> beam.Filter(lambda x: x[0] == 'L')
          | 'UnpackL' >> beam.Map(lambda x: x[1]))
      right_pc = (
          raw
          | 'FilterR' >> beam.Filter(lambda x: x[0] == 'R')
          | 'UnpackR' >> beam.Map(lambda x: x[1]))
      joined = {
          'left': left_pc,
          'right': right_pc,
      } | interval_join.CliIntervalJoin(
          key='id',
          left_time='l_ts',
          right_time='r_ts',
          lower_bound='0s',
          upper_bound='20s',
          join_type='left',
          right_fields=['id', 'r_ts', 'r_val'],
      )
      inspected = joined | beam.ParDo(_CaptureRowAndTsDoFn())
      assert_that(
          inspected,
          equal_to([
              ('k1', 'first', None, 10.0),
              ('k1', 'second', None, 100.0),
          ]))

  def test_right_outer_join_and_cross_window_global_rewindowing(self):
    import apache_beam as beam
    from apache_beam.runners.direct.direct_runner import BundleBasedDirectRunner
    from apache_beam.testing.test_stream import TestStream
    from apache_beam.testing.util import assert_that
    from apache_beam.testing.util import equal_to
    from apache_beam.transforms.window import FixedWindows
    from apache_beam.transforms.window import TimestampedValue

    # Left at t=9 (FixedWindow [0,10)) matches Right at t=12 (FixedWindow [10,20)),
    # plus unmatched Right 'k2' at t=30 emits right-outer with None-padded left cols!
    ts = (
        TestStream().advance_watermark_to(0).add_elements([
            TimestampedValue(('L', {
                'id': 'k1', 'l_ts': 9.0, 'l_name': 'nine'
            }),
                             9),
            TimestampedValue(('R', {
                'id': 'k1', 'r_ts': 12.0, 'r_amt': 55.0
            }),
                             12),
            TimestampedValue(('R', {
                'id': 'k2', 'r_ts': 30.0, 'r_amt': 77.0
            }),
                             30),
        ]).advance_watermark_to_infinity())

    with beam.Pipeline(runner=BundleBasedDirectRunner()) as p:
      raw = p | 'Stream' >> ts
      left_pc = (
          raw
          | 'FilterL' >> beam.Filter(lambda x: x[0] == 'L')
          | 'UnpackL' >> beam.Map(lambda x: x[1])
          | 'WinL' >> beam.WindowInto(FixedWindows(10)))
      right_pc = (
          raw
          | 'FilterR' >> beam.Filter(lambda x: x[0] == 'R')
          | 'UnpackR' >> beam.Map(lambda x: x[1])
          | 'WinR' >> beam.WindowInto(FixedWindows(10)))
      joined = {
          'left': left_pc,
          'right': right_pc,
      } | interval_join.CliIntervalJoin(
          key='id',
          left_time='l_ts',
          right_time='r_ts',
          lower_bound='0s',
          upper_bound='10s',
          join_type='right',
          left_fields=['id', 'l_ts', 'l_name'],
          right_fields=['id', 'r_ts', 'r_amt'],
      )
      out = joined | beam.ParDo(_CaptureRightJoinDoFn())
      assert_that(
          out,
          equal_to([
              ('k1', 'nine', 55.0, 9.0),
              ('k2', None, 77.0, 30.0),
          ]))

  def test_zero_epoch_watermark_warn_counter_and_skew_dlq(self):
    import apache_beam as beam
    from apache_beam.cli import jobs
    from apache_beam.testing.util import assert_that
    from apache_beam.testing.util import equal_to

    # 1. Verify skew_action='warn' at epoch 0.0 keeps max=0.0 (not overwritten by
    # negative -100.0) and NEVER increments beam_cli:skew_dropped!
    warn_cfg = interval_join.parse_interval_join_config({
        'key': 'id',
        'left_time': 'l_ts',
        'right_time': 'r_ts',
        'lower_bound': '0s',
        'upper_bound': '10s',
        'max_watermark_skew': '30s',
        'skew_action': 'warn',
    })
    events_warn = [
        (b'k1', ('R', 0.0, {
            'id': 'k1', 'r_ts': 0.0, 'v': 1
        })),
        (b'k1', ('R', -10.0, {
            'id': 'k1', 'r_ts': -10.0, 'v': 2
        })),
        # Lag relative to R max (0.0) is 45s > 30s (would be 35s if 0.0 had been
        # clobbered by -10.0!). Under 'warn' it still joins/processes without
        # bumping skew_dropped!
        (b'k1', ('L', -45.0, {
            'id': 'k1', 'l_ts': -45.0, 'a': 'x'
        })),
    ]
    p_warn = beam.Pipeline(runner='DirectRunner')
    _ = (
        p_warn
        | beam.Create(events_warn)
        | beam.ParDo(interval_join.IntervalJoinStatefulDoFn(warn_cfg)))
    res_warn = p_warn.run()
    res_warn.wait_until_finish()
    ctrs = jobs.extract_beam_cli_counters(res_warn)
    self.assertEqual(ctrs.get('skew_dropped', 0), 0)

    # 2. Verify skew_action='dlq' routes skewed records to errors PCollection
    dlq_cfg = interval_join.parse_interval_join_config({
        'key': 'id',
        'left_time': 'l_ts',
        'right_time': 'r_ts',
        'lower_bound': '0s',
        'upper_bound': '10s',
        'max_watermark_skew': '30s',
        'skew_action': 'dlq',
    })
    events_dlq = [
        (b'k1', ('R', 100.0, {
            'id': 'k1', 'r_ts': 100.0, 'v': 1
        })),
        (b'k1', ('L', 20.0, {
            'id': 'k1', 'l_ts': 20.0, 'a': 'late_skew'
        })),
    ]
    with beam.Pipeline(runner='DirectRunner') as p_dlq:
      out_dlq = (
          p_dlq
          | beam.Create(events_dlq)
          | beam.ParDo(
              interval_join.IntervalJoinStatefulDoFn(
                  dlq_cfg, err_tag='errors', route_errors=True)).with_outputs(
                      'errors', main='good'))
      err_types = out_dlq['errors'] | beam.Map(lambda r: r.error_type)
      assert_that(err_types, equal_to(['WatermarkSkewError']))


class _CaptureRowAndTsDoFn(interval_join.beam.DoFn):
  def process(self, element, ts=interval_join.beam.DoFn.TimestampParam):
    yield (
        element.id, element.l_val, getattr(element, 'r_val', None), float(ts))


class _CaptureRightJoinDoFn(interval_join.beam.DoFn):
  def process(self, element, ts=interval_join.beam.DoFn.TimestampParam):
    yield (
        element.id,
        getattr(element, 'l_name', None),
        element.r_amt,
        float(ts),
    )


class CliPipeAndSqlIntervalJoinLoweringTest(unittest.TestCase):
  """Tests `beam pipe --interval-join` E2E and fail-closed SQL lowering."""
  def test_cli_pipe_interval_join_e2e_with_dlq_and_parquet(self):
    from apache_beam.cli import main as cli_main
    import pyarrow.parquet as pq

    with tempfile.TemporaryDirectory() as tmpdir:
      orders_p = os.path.join(tmpdir, 'orders.jsonl')
      ships_p = os.path.join(tmpdir, 'ships.jsonl')
      out_p = os.path.join(tmpdir, 'joined.parquet')
      dlq_p = os.path.join(tmpdir, 'dlq.jsonl')

      with open(orders_p, 'w', encoding='utf-8') as f:
        f.write(
            json.dumps({
                'tenant': 't1',
                'oid': 1,
                'o_ts': '2026-05-01T00:00:00Z',
                'amt': 120.0
            }) + '\n')
        f.write(
            json.dumps({
                'tenant': 't1',
                'oid': 2,
                'o_ts': '2026-05-01T00:10:00Z',
                'amt': 40.0
            }) + '\n')
        f.write(
            json.dumps({
                'tenant': 't1',
                'oid': 3,
                'o_ts': 'NOT_A_TIMESTAMP',
                'amt': 99.0
            }) + '\n')

      with open(ships_p, 'w', encoding='utf-8') as f:
        f.write(
            json.dumps({
                'tenant': 't1',
                'oid': 1,
                's_ts': '2026-05-01T00:05:00Z',
                'carrier': 'UPS'
            }) + '\n')

      rc = cli_main.main([
          'pipe',
          '--from',
          f'o={orders_p}',
          '--join',
          f's={ships_p}',
          '--to',
          out_p,
          '--dlq',
          dlq_p,
          '--interval-join',
          'key=[tenant,oid],left_time=o_ts,right_time=s_ts,lower=0s,upper=15m,type=left',
          '--select',
          'oid, amt, carrier',
          '--json',
      ])
      self.assertEqual(rc, 0)
      tbl = pq.read_table(out_p)
      rows = sorted(tbl.to_pylist(), key=lambda r: r['oid'])
      self.assertEqual(
          rows,
          [
              {
                  'oid': 1, 'amt': 120.0, 'carrier': 'UPS'
              },
              {
                  'oid': 2, 'amt': 40.0, 'carrier': None
              },
          ])
      with open(dlq_p, encoding='utf-8') as df:
        dlq_rows = [json.loads(line) for line in df if line.strip()]
      self.assertEqual(len(dlq_rows), 1)
      self.assertIn('NOT_A_TIMESTAMP', dlq_rows[0]['error'])

  def test_cli_interval_join_guards(self):
    from apache_beam.cli import main as cli_main

    # 1. --key collision guard without explicit --cdc
    rc_key = cli_main.main([
        'pipe',
        '--from',
        'a.jsonl',
        '--join',
        'b.jsonl',
        '--to',
        'out.jsonl',
        '--interval-join',
        'key=id,left_time=t1,right_time=t2,upper=5m',
        '--key',
        'id',
        '--dry-run',
        '--json',
    ])
    self.assertEqual(rc_key, cli_main.EXIT_USAGE)

    # 2. --assert-counts rejected on --interval-join
    rc_ac = cli_main.main([
        'pipe',
        '--from',
        'a.jsonl',
        '--join',
        'b.jsonl',
        '--to',
        'out.jsonl',
        '--interval-join',
        'key=id,left_time=t1,right_time=t2,upper=5m',
        '--assert-counts',
        '--dry-run',
        '--json',
    ])
    self.assertEqual(rc_ac, cli_main.EXIT_USAGE)

  def test_sql_lowering_swapped_bounds_streaming_and_fail_closed(self):
    from apache_beam.cli import main as cli_main

    # 1. Swapped BETWEEN bounds (`c.ts BETWEEN o.ts - 15m AND o.ts + 1m`
    #    => lower_bound = -60s, upper_bound = 900s) + streaming Pub/Sub option!
    ns_stream = pipe_sql.parse_named_sources(
        [
            'c=pubsub://projects/p/subscriptions/clicks',
            'o=pubsub://projects/p/subscriptions/orders',
        ],
        raw_joins=None,
        allow_sniff=False,
    )
    swapped_sql = (
        'SELECT c.uid, o.amount AS rev '
        'FROM c LEFT JOIN o ON c.uid = o.uid '
        "AND c.click_ts BETWEEN o.order_ts - INTERVAL '15' MINUTE "
        "AND o.order_ts + INTERVAL '1' MINUTE "
        "WHERE c.country = 'US'")
    spec = pipe_sql.build_sql_yaml_spec(
        ns_stream,
        swapped_sql,
        uris.parse_endpoint('gs://bkt/out.parquet', is_read=False),
        max_watermark_skew='30m',
    )
    self.assertEqual(spec.get('options'), {'streaming': True})
    ij_node = next(
        t for t in spec['pipeline']['transforms']
        if t['type'] == 'CliIntervalJoin')
    self.assertEqual(ij_node['config']['lower_bound'], '-60s')
    self.assertEqual(ij_node['config']['upper_bound'], '900s')
    self.assertEqual(ij_node['config']['join_type'], 'left')
    self.assertEqual(ij_node['config']['max_watermark_skew'], '30m')

    # 2. Fail-closed rejections: CTE, strict `<` bound, outer-join non-temporal ON
    for bad_sql in (
        'WITH x AS (SELECT * FROM c) SELECT x.uid FROM x JOIN o '
        "ON x.uid = o.uid AND o.ts BETWEEN x.ts AND x.ts + INTERVAL '5' MINUTE",
        'SELECT c.uid FROM c JOIN o '
        "ON c.uid = o.uid AND o.ts >= c.ts AND o.ts < c.ts + INTERVAL '5' MINUTE",
        'SELECT c.uid FROM c LEFT JOIN o '
        "ON c.uid = o.uid AND o.ts BETWEEN c.ts AND c.ts + INTERVAL '5' MINUTE "
        "AND o.status = 'PAID'",
    ):
      with self.subTest(sql=bad_sql):
        with self.assertRaises(pipe_sql.SqlPipeError):
          pipe_sql.build_sql_yaml_spec(ns_stream, bad_sql, None)

    # 3. End-to-end `--sql ... --portable` execution through lowered CliIntervalJoin!
    with tempfile.TemporaryDirectory() as tmpdir:
      c_path = os.path.join(tmpdir, 'clicks.jsonl')
      o_path = os.path.join(tmpdir, 'orders.jsonl')
      out_path = os.path.join(tmpdir, 'out.jsonl')
      with open(c_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps({'uid': 1, 'ts': 1000, 'src': 'ads'}) + '\n')
        f.write(json.dumps({'uid': 2, 'ts': 2000, 'src': 'organic'}) + '\n')
      with open(o_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps({'uid': 1, 'ts': 1120, 'total': 75.0}) + '\n')
      rc = cli_main.main([
          'pipe',
          '--from',
          f'c={c_path}',
          '--join',
          f'o={o_path}',
          '--to',
          out_path,
          '--sql',
          'SELECT c.uid, o.total FROM c LEFT JOIN o '
          'ON c.uid = o.uid '
          "AND o.ts BETWEEN c.ts AND c.ts + INTERVAL '5' MINUTE "
          "WHERE c.src = 'ads'",
          '--portable',
          '--json',
      ])
      self.assertEqual(rc, 0)
      with open(out_path, encoding='utf-8') as f:
        got = [json.loads(line) for line in f if line.strip()]
      self.assertEqual(got, [{'uid': 1, 'total': 75.0}])


if __name__ == '__main__':
  unittest.main()
