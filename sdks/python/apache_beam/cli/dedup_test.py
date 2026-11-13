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

"""Unit and integration tests for `CliDeduplicate` & `--dedupe-by` (`B1.2`)."""

from __future__ import annotations

import decimal
import io
import json
import os
import tempfile
import unittest
from unittest import mock

import apache_beam as beam
from apache_beam.cli import check_cmd
from apache_beam.cli import dedup
from apache_beam.cli import jobs
from apache_beam.cli import main as cli_main
from apache_beam.cli import pipe_spec
from apache_beam.runners.direct.direct_runner import BundleBasedDirectRunner
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.window import TimestampedValue


class CanonicalKeyEncodingTest(unittest.TestCase):
  """Verifies collision-free deterministic `bytes` composite key encoding."""
  def test_composite_framing_and_numeric_normalization(self):
    # Length-prefixed framing prevents ('ab', 'c') == ('a', 'bc') collisions
    self.assertNotEqual(
        dedup.encode_canonical_key_values(['ab', 'c']),
        dedup.encode_canonical_key_values(['a', 'bc']),
    )
    # Type tags separate None, '', 0, False, and '0'
    distinct = {
        dedup.encode_canonical_key_values([v])
        for v in (None, '', 0, False, '0', b'0')
    }
    self.assertEqual(len(distinct), 6)

    # Numeric canonicalization: -0.0 == 0.0 == 0 == Decimal('0.00')
    z_neg = dedup.encode_canonical_key_values([-0.0])
    z_pos = dedup.encode_canonical_key_values([0.0])
    z_int = dedup.encode_canonical_key_values([0])
    z_dec = dedup.encode_canonical_key_values([decimal.Decimal('0.00')])
    self.assertEqual(z_neg, z_pos)
    self.assertEqual(z_pos, z_int)
    self.assertEqual(z_int, z_dec)

    # NaN canonicalization is deterministic
    self.assertEqual(
        dedup.encode_canonical_key_values([float('nan')]),
        dedup.encode_canonical_key_values([float('nan')]),
    )

  def test_bracket_aware_column_list_parsing(self):
    self.assertEqual(
        dedup.parse_column_list('[tenant_id, event_id]'),
        ('tenant_id', 'event_id'),
    )
    self.assertEqual(
        dedup.parse_column_list('tenant_id,event_id,tenant_id'),
        ('tenant_id', 'event_id'),
    )


class BoundedDeduplicateTest(unittest.TestCase):
  """Tests bounded `CombinePerKey` and `GroupByKey` deduplication."""
  def test_bounded_keep_first_and_last_with_order(self):
    records = [
        {
            'k': 'a', 'seq': 2, 'val': 'v2'
        },
        {
            'k': 'a', 'seq': 5, 'val': 'v5'
        },
        {
            'k': 'a', 'seq': 1, 'val': 'v1'
        },
        {
            'k': 'b', 'seq': None, 'val': 'v_null'
        },
        {
            'k': 'b', 'seq': 0, 'val': 'v0'
        },
    ]

    with beam.Pipeline(runner='DirectRunner') as p_first:
      out_first = (
          p_first
          | beam.Create(records)
          | dedup.CliDeduplicate(keys='k', keep='first', order_by='seq')
          | beam.Map(lambda d: (d['k'], d['val'])))
      # None sorts before non-nulls (`rank 0 < rank 2`), so 'first' picks v1 & v_null
      assert_that(out_first, equal_to([('a', 'v1'), ('b', 'v_null')]))

    with beam.Pipeline(runner='DirectRunner') as p_last:
      out_last = (
          p_last
          | beam.Create(records)
          | dedup.CliDeduplicate(keys='k', keep='last', order_by='seq')
          | beam.Map(lambda d: (d['k'], d['val'])))
      assert_that(out_last, equal_to([('a', 'v5'), ('b', 'v0')]))

  def test_cli_pipe_bounded_assert_counts_and_dedupe_dlq(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      src_p = os.path.join(tmpdir, 'events.jsonl')
      out1_p = os.path.join(tmpdir, 'clean1.jsonl')
      out2_p = os.path.join(tmpdir, 'clean2.jsonl')
      dlq_p = os.path.join(tmpdir, 'dupes_dlq.jsonl')

      rows = [
          {
              'tenant': 't1', 'eid': 10, 'ver': 1, 'amt': 15.0
          },
          {
              'tenant': 't1', 'eid': 10, 'ver': 2, 'amt': 25.0
          },
          {
              'tenant': 't1', 'eid': 11, 'ver': 1, 'amt': 50.0
          },
      ]
      with open(src_p, 'w', encoding='utf-8') as f:
        for r in rows:
          f.write(json.dumps(r) + '\n')

      # 1. Silent drop: duplicates_dropped=1 folded into rows_filtered_out;
      #    --assert-counts conservation holds (3 == 2 + 1 + 0)!
      buf1 = io.StringIO()
      with mock.patch('sys.stdout', buf1):
        rc1 = cli_main.main([
            'pipe',
            '--from',
            src_p,
            '--to',
            out1_p,
            '--dedupe-by',
            '[tenant,eid]',
            '--dedupe-keep',
            'last',
            '--dedupe-order',
            'ver',
            '--assert-counts',
            '--json',
        ])
      self.assertEqual(rc1, 0)
      env1 = json.loads(buf1.getvalue())
      self.assertEqual(env1['metrics']['rows_read'], 3)
      self.assertEqual(env1['metrics']['rows_written'], 2)
      self.assertEqual(env1['metrics']['duplicates_dropped'], 1)
      self.assertEqual(env1['metrics']['rows_filtered_out'], 1)

      # 2. --dedupe-dlq: duplicate routed to --dlq without double-counting
      #    duplicates_dropped; --assert-counts holds (3 == 2 + 0 + 1)!
      buf2 = io.StringIO()
      with mock.patch('sys.stdout', buf2):
        rc2 = cli_main.main([
            'pipe',
            '--from',
            src_p,
            '--to',
            out2_p,
            '--dlq',
            dlq_p,
            '--dedupe-by',
            '[tenant,eid]',
            '--dedupe-keep',
            'last',
            '--dedupe-order',
            'ver',
            '--dedupe-dlq',
            '--assert-counts',
            '--json',
        ])
      self.assertEqual(rc2, 0)
      env2 = json.loads(buf2.getvalue())
      self.assertEqual(env2['metrics']['rows_read'], 3)
      self.assertEqual(env2['metrics']['rows_written'], 2)
      self.assertEqual(env2['metrics']['dlq_rows'], 1)
      self.assertEqual(env2['metrics'].get('duplicates_dropped', 0), 0)
      with open(dlq_p, encoding='utf-8') as df:
        dlq_items = [json.loads(line) for line in df if line.strip()]
      self.assertEqual(len(dlq_items), 1)
      self.assertEqual(dlq_items[0]['error_type'], 'DuplicateKeyError')


class StatefulStreamingDeduplicateTest(unittest.TestCase):
  """Tests time-bucketed event-time and processing-time streaming deduplication."""
  def test_out_of_order_event_time_bucketing_and_ttl_expiry(self):
    # Window = 100s => buckets are [0, 100) (idx 0), [100, 200) (idx 1), [200, 300) (idx 2).
    # Arrival order for key 'e1':
    # 1. t=170 (bucket 1) -> KEPT
    # 2. t=10  (bucket 0, out-of-order!) -> KEPT (different bucket; old mutable
    #    single-expiry design falsely dropped or corrupted this!)
    # 3. t=50  (bucket 0) -> DROPPED (same bucket 0 as t=10)
    # 4. t=180 (bucket 1) -> DROPPED (same bucket 1 as t=170)
    # 5. Watermark advances to 250 (clears buckets 0 & 1 via TTL timer)
    # 6. t=260 (bucket 2) -> KEPT!
    ts_stream = (
        TestStream().advance_watermark_to(0).add_elements([
            TimestampedValue({
                'id': 'e1', 'ts': 170.0, 'seq': 1
            }, 170),
            TimestampedValue({
                'id': 'e1', 'ts': 10.0, 'seq': 2
            }, 10),
            TimestampedValue({
                'id': 'e1', 'ts': 50.0, 'seq': 3
            }, 50),
            TimestampedValue({
                'id': 'e1', 'ts': 180.0, 'seq': 4
            }, 180),
        ]).advance_watermark_to(250).add_elements([
            TimestampedValue({
                'id': 'e1', 'ts': 260.0, 'seq': 5
            }, 260),
        ]).advance_watermark_to_infinity())

    p = beam.Pipeline(runner=BundleBasedDirectRunner())
    kept = (
        p
        | ts_stream
        | dedup.CliDeduplicate(
            keys='id',
            window='100s',
            time_domain='event',
            timestamp_col='ts',
        )
        | beam.Map(lambda d: d['seq']))
    assert_that(kept, equal_to([1, 2, 5]))
    res = p.run()
    res.wait_until_finish()
    ctrs = jobs.extract_beam_cli_counters(res)
    self.assertEqual(ctrs.get('duplicates_dropped'), 2)

  def test_processing_time_stateful_deduplicate_and_dlq(self):
    ts_stream = (
        TestStream().advance_watermark_to(0).add_elements([
            {
                'id': 'k1', 'n': 1
            },
            {
                'id': 'k1', 'n': 2
            },
            {
                'id': 'k2', 'n': 3
            },
        ]).advance_watermark_to_infinity())

    with beam.Pipeline(runner=BundleBasedDirectRunner()) as p:
      out = (
          p
          | ts_stream
          | dedup.CliDeduplicate(
              keys='id',
              window='60s',
              time_domain='processing',
              dedupe_dlq=True,
              error_handling={'output': 'errors'},
          ))
      assert_that(
          out['good'] | 'GoodN' >> beam.Map(lambda d: d['n']),
          equal_to([1, 3]),
          label='CheckGood')
      assert_that(
          out['errors'] | 'BadType' >> beam.Map(lambda r: r.error_type),
          equal_to(['DuplicateKeyError']),
          label='CheckBad')


class DedupeStageOrderAndValidationGuardsTest(unittest.TestCase):
  """Tests stage ordering (`CliDeduplicate` before `WindowInto+Combine`) & CLI guards."""
  def test_dedupe_runs_before_window_combine_preventing_double_counting(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      src_p = os.path.join(tmpdir, 'payments.jsonl')
      out_p = os.path.join(tmpdir, 'windowed.jsonl')
      # Duplicate retry of event_id='p1' inside the same 60s window [1000, 1060):
      with open(src_p, 'w', encoding='utf-8') as f:
        f.write(
            json.dumps({
                'event_id': 'p1', 'acct': 'A', 'ts': 1005, 'amount': 100.0
            }) + '\n')
        f.write(
            json.dumps({
                'event_id': 'p1', 'acct': 'A', 'ts': 1005, 'amount': 100.0
            }) + '\n')
        f.write(
            json.dumps({
                'event_id': 'p2', 'acct': 'A', 'ts': 1010, 'amount': 50.0
            }) + '\n')

      # Verify YAML stage order puts CliDeduplicate BEFORE WindowInto + Combine
      spec_obj = pipe_spec.build_pipe_spec(
          src_p,
          out_p,
          dedupe_by='event_id',
          timestamp_col='ts',
          window='fixed:60s,group_by=acct,total=sum(amount)',
          prefer_python_lakehouse_and_cdc=True)
      types = [t['type'] for t in spec_obj.spec['pipeline']['transforms']]
      self.assertLess(types.index('CliDeduplicate'), types.index('WindowInto'))

      rc = cli_main.main([
          'pipe',
          '--from',
          src_p,
          '--to',
          out_p,
          '--dedupe-by',
          'event_id',
          '--timestamp-col',
          'ts',
          '--window',
          'fixed:60s,group_by=acct,total=sum(amount)',
          '--json',
      ])
      self.assertEqual(rc, 0)
      with open(out_p, encoding='utf-8') as f:
        got = [json.loads(line) for line in f if line.strip()]
      self.assertEqual(len(got), 1)
      # Must be 150.0 (100 + 50), NOT 250.0!
      self.assertEqual(got[0]['total'], 150.0)

  def test_cli_dedupe_guards_and_processing_time_warning(self):
    def _quiet_main(argv: list[str]) -> int:
      with mock.patch('sys.stdout', io.StringIO()):
        return cli_main.main(argv)

    # 1. Unbounded stream without --dedupe-window -> EXIT_USAGE
    self.assertEqual(
        _quiet_main([
            'pipe',
            '--from',
            'pubsub://projects/my-proj/subscriptions/sub',
            '--to',
            'gs://bkt/out.parquet',
            '--dedupe-by',
            'event_id',
            '--dry-run',
            '--json',
        ]),
        cli_main.EXIT_USAGE)

    # 2. Unbounded stream with --dedupe-keep=last -> EXIT_USAGE
    self.assertEqual(
        _quiet_main([
            'pipe',
            '--from',
            'pubsub://projects/my-proj/subscriptions/sub',
            '--to',
            'gs://bkt/out.parquet',
            '--dedupe-by',
            'event_id',
            '--dedupe-window',
            '10m',
            '--dedupe-keep',
            'last',
            '--dry-run',
            '--json',
        ]),
        cli_main.EXIT_USAGE)

    # 3. Bounded file with --dedupe-time-domain=processing -> EXIT_USAGE
    self.assertEqual(
        _quiet_main([
            'pipe',
            '--from',
            'events.jsonl',
            '--to',
            'out.parquet',
            '--dedupe-by',
            'event_id',
            '--dedupe-window',
            '10m',
            '--dedupe-time-domain',
            'processing',
            '--dry-run',
            '--json',
        ]),
        cli_main.EXIT_USAGE)

    # 4. --dedupe-by + --key without --cdc -> EXIT_USAGE
    self.assertEqual(
        _quiet_main([
            'pipe',
            '--from',
            'events.jsonl',
            '--to',
            'out.parquet',
            '--dedupe-by',
            'event_id',
            '--key',
            'event_id',
            '--dry-run',
            '--json',
        ]),
        cli_main.EXIT_USAGE)

    # 5. Unbounded processing-time dedup without --timestamp-col emits warning
    with tempfile.NamedTemporaryFile('w', suffix='.json', delete=False) as sf:
      json.dump({
          'schema': {
              'type': 'object',
              'properties': {
                  'event_id': {
                      'type': 'string'
                  }
              },
          }
      },
                sf)
      opt_file = sf.name
    os.chmod(opt_file, 0o600)
    try:
      buf = io.StringIO()
      with mock.patch('sys.stdout', buf):
        rc = cli_main.main([
            'pipe',
            '--from',
            'pubsub://projects/my-proj/subscriptions/sub',
            '--from-opt-file',
            opt_file,
            '--to',
            'gs://bkt/out.parquet',
            '--dedupe-by',
            'event_id',
            '--dedupe-window',
            '10m',
            '--window',
            'fixed:60s',
            '--dry-run',
            '--json',
        ])
      self.assertEqual(rc, 0)
      env = json.loads(buf.getvalue())
      self.assertTrue(
          any('processing-time TTLs' in w for w in env.get('warnings', [])),
          msg=f'expected processing-time warning in {env.get("warnings")}')
    finally:
      os.unlink(opt_file)

  def test_beam_check_validates_cli_deduplicate_config(self):
    bad_yaml = (
        'pipeline:\n'
        '  transforms:\n'
        '    - type: Create\n'
        '      config:\n'
        '        elements: [{id: 1}]\n'
        '    - type: CliDeduplicate\n'
        '      input: Create\n'
        '      config:\n'
        '        keys: [id]\n'
        '        window: "-5m"\n')
    with tempfile.NamedTemporaryFile('w', suffix='.yaml', delete=False) as tf:
      tf.write(bad_yaml)
      yaml_path = tf.name
    try:
      with self.assertRaises(check_cmd.CheckError):
        check_cmd.run_check(yaml_path)
    finally:
      os.unlink(yaml_path)


if __name__ == '__main__':
  unittest.main()
