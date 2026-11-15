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

"""Unit & parity tests for `cdc.py`, `diff.py`, and Iceberg `catalog sync`."""

import contextlib
import datetime
import decimal
import glob
import io
import json
import os
import shutil
import tempfile
import unittest
from typing import Optional
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

import apache_beam as beam
from apache_beam.cli import cdc
from apache_beam.cli import diff
from apache_beam.cli import lakehouse
from apache_beam.cli import main as cli
from apache_beam.cli import uris
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.utils.timestamp import Timestamp


def _run_cli(argv, extra_env=None):
  out_buf, err_buf = io.StringIO(), io.StringIO()
  env = {'BEAM_NO_CONFIG': '1'}
  if extra_env:
    env.update(extra_env)
  with mock.patch.dict(os.environ, env, clear=False):
    with contextlib.redirect_stdout(out_buf), contextlib.redirect_stderr(
        err_buf):
      rc = cli.main(['--json'] + list(argv))
  return rc, json.loads(out_buf.getvalue().strip()), err_buf.getvalue()


def _read_parquet_prefix(prefix: str) -> list[dict]:
  matches = (
      [prefix] if os.path.isfile(prefix) else sorted(glob.glob(prefix + '*')))
  out: list[dict] = []
  for fp in matches:
    if not os.path.isdir(fp):
      out.extend(pq.read_table(fp).to_pylist())
  return out


class CdcUnitAndParityTest(unittest.TestCase):

  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-cli-cdc-')

  def tearDown(self):
    shutil.rmtree(self.tmp, ignore_errors=True)

  def test_to_bq_hex_sequence_floats_nan_inf_lsn_bigints_and_iso(self):
    # Regression for missing `import math` in cdc.py on float / NaN / Inf!
    i_fin, h_fin = cdc.to_bq_hex_sequence(1700000000.5)
    self.assertEqual(i_fin, 1700000000500)
    self.assertEqual(len(h_fin), 16)

    for special_f in (float('nan'), float('inf'), float('-inf')):
      i_sp, h_sp = cdc.to_bq_hex_sequence(special_f, fallback_int=42)
      self.assertEqual(i_sp, 42)
      self.assertEqual(h_sp, f'{42:016X}')

    self.assertEqual(cdc.to_bq_hex_sequence(True), (1, f'{1:016X}'))
    self.assertEqual(cdc.to_bq_hex_sequence(False), (0, f'{0:016X}'))

    # >64-bit integer produces two 16-char `/`-joined uppercase hex segments
    _, h_big = cdc.to_bq_hex_sequence((1 << 68) + 0xBEEF)
    self.assertEqual(h_big, '0000000000000010/000000000000BEEF')

    # Hex prefix, Postgres WAL LSN, pre-1970 ISO, and arbitrary string
    self.assertEqual(cdc.to_bq_hex_sequence('0x1A2B')[1], f'{0x1A2B:016X}')
    _, h_lsn = cdc.to_bq_hex_sequence('1/16B3748')
    self.assertEqual(h_lsn, '0000000000000001/00000000016B3748')
    i_pre, h_pre = cdc.to_bq_hex_sequence('1969-07-20T20:17:40Z')
    i_post, h_post = cdc.to_bq_hex_sequence('2026-07-20T20:17:40Z')
    self.assertLess(i_pre, 0)
    self.assertLess(h_pre, h_post)
    _, h_arb = cdc.to_bq_hex_sequence('tx-alpha-seq-001#custom')
    self.assertTrue(cdc._HEX_SEGMENTS_RE.match(h_arb), h_arb)

  def test_parse_cdc_event_wrappers_errors_and_shorthand(self):
    wrapped = beam.Row(
        payload=b'{"op":"c","after":{"id":7,"name":"Ada"},"source":{"lsn":99}}')
    parsed = cdc.parse_cdc_event(wrapped, primary_keys=['id'])
    self.assertIsNotNone(parsed)
    self.assertEqual(parsed[0], 'UPSERT')
    self.assertEqual(parsed[1], 99)
    self.assertEqual(parsed[3], {'id': 7, 'name': 'Ada'})

    # Pre-image skipped (`None`)
    self.assertIsNone(
        cdc.parse_cdc_event(
            {'op': 'update_before', 'before': {'id': 7}},
            cdc_format='debezium'))

    for bad_ev in (
        {'op': 'd', 'after': {'id': 1}},
        {'op': 'c', 'before': {'id': 1}},
        {'op': 'bogus', 'after': {'id': 1}},
        {'id': None, 'op': 'insert'},
    ):
      with self.assertRaises(ValueError):
        cdc.parse_cdc_event(bad_ev, cdc_format='debezium' if 'after' in bad_ev or 'before' in bad_ev else 'flat', primary_keys=['id'])

    fields = cdc.parse_fields_shorthand('id:int64,amt:double,ok:boolean,raw:bytes,tag')
    self.assertEqual(
        fields,
        [
            ('id', 'int'),
            ('amt', 'float'),
            ('ok', 'bool'),
            ('raw', 'bytes'),
            ('tag', 'str'),
        ])
    with self.assertRaises(ValueError):
      cdc.parse_fields_shorthand('id:unknown_type')

  def test_bq_cdc_proto_bundle_ensure_table_and_storage_write_dlq(self):
    desc_proto, msg_cls = cdc._build_bq_cdc_proto_bundle([
        ('id', 'int'),
        ('amount', 'float'),
        ('active', 'bool'),
    ])
    fnames = [f.name for f in desc_proto.field]
    self.assertIn('_CHANGE_TYPE', fnames)
    self.assertIn('_CHANGE_SEQUENCE_NUMBER', fnames)
    m = msg_cls()
    m.id = 5
    m._CHANGE_TYPE = 'UPSERT'
    m._CHANGE_SEQUENCE_NUMBER = f'{5:016X}'
    self.assertGreater(len(m.SerializeToString()), 0)

    # Mocked BigQuery DDL + Storage Write API append_rows with row_errors -> DLQ
    fake_bq_client = mock.MagicMock()
    fake_bq_client.project = 'demo-proj'
    fake_existing = mock.MagicMock()
    fake_existing.num_rows = 0
    fake_bq_client.get_table.return_value = fake_existing

    fake_row_err = mock.MagicMock()
    fake_row_err.index = 0
    fake_row_err.message = 'constraint error'
    fake_resp = mock.MagicMock()
    fake_resp.row_errors = [fake_row_err]
    fake_resp.error = None

    fake_write_client = mock.MagicMock()
    fake_write_client.append_rows.return_value = [fake_resp]

    in_type = RowTypeConstraint.from_fields([
        ('id', Optional[int]),
        ('name', Optional[str]),
        ('beam_cdc_op', str),
        ('beam_cdc_seq', int),
        ('beam_cdc_hex_seq', str),
    ])
    with (
        mock.patch(
            'google.cloud.bigquery.Client', return_value=fake_bq_client),
        mock.patch(
            'google.cloud.bigquery_storage_v1.BigQueryWriteClient',
            return_value=fake_write_client),
    ):
      # Reject unsafe identifier
      with self.assertRaises(ValueError):
        cdc._ensure_bq_cdc_table(
            'demo-proj:ds.tbl',
            [('bad-col', 'int')],
            ['bad-col'],
            'WRITE_APPEND')

      with TestPipeline('FnApiRunner') as p:
        pcoll = p | beam.Create([
            beam.Row(
                id=1,
                name='ok',
                beam_cdc_op='UPSERT',
                beam_cdc_seq=1,
                beam_cdc_hex_seq=f'{1:016X}'),
            beam.Row(
                id=2,
                name='bad',
                beam_cdc_op='UPSERT',
                beam_cdc_seq=2,
                beam_cdc_hex_seq=f'{2:016X}'),
        ]).with_output_types(in_type)
        out = pcoll | cdc.cli_write_to_bigquery_cdc(
            'demo-proj.ds.cdc_tbl',
            ['id'],
            write_disposition='WRITE_TRUNCATE',
            error_handling={'output': 'errors'})
        self.assertIn('errors', out)

    fake_bq_client.query.assert_called_once()
    ddl_called = fake_bq_client.query.call_args[0][0]
    self.assertIn('PRIMARY KEY (`id`) NOT ENFORCED', ddl_called)
    self.assertEqual(fake_write_client.append_rows.call_count, 2)

  def test_cdc_compaction_parity_local_fast_vs_beam_runner_parquet(self):
    cdc_in = os.path.join(self.tmp, 'changes.jsonl')
    events = [
        json.dumps({'op': 'c', 'ts_ms': 100, 'after': {'id': 1, 'v': 'v1'}}),
        json.dumps({'op': 'update_before', 'ts_ms': 200, 'before': {'id': 1, 'v': 'v1'}}),
        json.dumps({'op': 'u', 'ts_ms': 200, 'after': {'id': 1, 'v': 'v2'}}),
        json.dumps({'op': 'c', 'ts_ms': 150, 'after': {'id': 2, 'v': 'gone'}}),
        json.dumps({'op': 'd', 'ts_ms': 300, 'before': {'id': 2, 'v': 'gone'}}),
        json.dumps({'op': 'c', 'ts_ms': 400, 'after': {'id': 3, 'v': 'live'}}),
        '{corrupt_cdc_json',
    ]
    with open(cdc_in, 'w', encoding='utf-8') as f:
      f.write('\n'.join(events) + '\n')

    by_mode = {}
    for mode, extra_env in (
        ('fast', None),
        ('beam', {'BEAM_CLI_NO_FAST_PATH': '1'}),
    ):
      out_pq = os.path.join(self.tmp, f'cdc_{mode}.parquet')
      dlq_jl = os.path.join(self.tmp, f'cdc_{mode}_dlq.jsonl')
      rc, env, stderr = _run_cli(
          [
              'pipe',
              cdc_in,
              out_pq,
              '--cdc',
              '--primary-key',
              'id',
              '--dlq',
              dlq_jl,
          ],
          extra_env=extra_env)
      self.assertEqual(rc, cli.EXIT_OK, f'{mode}: {env} {stderr}')
      rows = sorted(_read_parquet_prefix(out_pq), key=lambda r: r['id'])
      dlq_cnt = sum(1 for ln in open(dlq_jl, encoding='utf-8') if ln.strip())
      by_mode[mode] = (rows, dlq_cnt)

    expected = [{'id': 1, 'v': 'v2'}, {'id': 3, 'v': 'live'}]
    self.assertEqual(by_mode['fast'], (expected, 1))
    self.assertEqual(by_mode['beam'], (expected, 1))


class DiffAndIcebergSyncParityTest(unittest.TestCase):

  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-cli-diff-')

  def tearDown(self):
    shutil.rmtree(self.tmp, ignore_errors=True)

  def test_diff_numeric_temporal_edge_cases_and_bq_view_fallback(self):
    # NaN == NaN, +Inf != -Inf, -0.0 == Decimal('0.000'), 38-digit Decimal rtol
    self.assertTrue(diff._values_equal(float('nan'), decimal.Decimal('NaN'), 0, 0))
    self.assertFalse(diff._values_equal(float('inf'), float('-inf'), 0, 0))
    self.assertTrue(diff._values_equal(-0.0, decimal.Decimal('0.000'), 0, 0))

    d_a = decimal.Decimal('1234567890123456789012345678901234.5678')
    d_b = decimal.Decimal('1234567890123456789012345678901234.5679')
    self.assertFalse(diff._values_equal(d_a, d_b, 0.0, 0.0))
    self.assertTrue(diff._values_equal(d_a, d_b, 0.0, 0.001))

    # Huge exponent OverflowError fallback in float(a)
    huge_a = decimal.Decimal('1e400')
    huge_b = decimal.Decimal('1.000001e400')
    self.assertTrue(diff._values_equal(huge_a, huge_b, 1e-4, 0.0))

    # Temporal canonicalization
    ts = Timestamp(1700000000)
    dt_utc = datetime.datetime.fromtimestamp(1700000000, tz=datetime.timezone.utc)
    self.assertTrue(diff._values_equal(ts, dt_utc, 0.0, 0.0))

    # Mocked BigQuery VIEW in _read_endpoint_pcoll switches to DIRECT_READ SQL
    fake_tbl = mock.MagicMock()
    fake_tbl.table_type = 'VIEW'
    fake_tbl.table_constraints = None
    fake_client = mock.MagicMock()
    fake_client.project = 'demo-proj'
    fake_client.get_table.return_value = fake_tbl

    ep = uris.parse_endpoint(
        'bigquery://demo-proj.ds.my_view',
        is_read=True,
        options={'row_restriction': 'id > 10'})
    with mock.patch('google.cloud.bigquery.Client', return_value=fake_client):
      with mock.patch(
          'apache_beam.io.gcp.bigquery.ReadFromBigQuery',
          return_value=beam.Create([])) as mock_read_bq:
        with TestPipeline('FnApiRunner') as p:
          _ = diff._read_endpoint_pcoll(p, 'ReadView', ep)
        mock_read_bq.assert_called_once()
        q_arg = mock_read_bq.call_args.kwargs['query']
        self.assertIn('SELECT * FROM `demo-proj.ds.my_view` WHERE (id > 10)', q_arg)

  def test_diff_parity_local_fast_vs_beam_runner_keyed_and_keyless(self):
    left_p = os.path.join(self.tmp, 'left.jsonl')
    right_p = os.path.join(self.tmp, 'right.jsonl')
    with open(left_p, 'w', encoding='utf-8') as f:
      f.write('{"id": 1, "price": 10.0, "old_col": "a"}\n')
      f.write('{"id": 2, "price": 20.0, "old_col": "b"}\n')
      f.write('{"id": 3, "price": 30.0, "old_col": "c"}\n')
      f.write('{"id": 4, "price": 40.0, "old_col": "d1"}\n')
      f.write('{"id": 4, "price": 41.0, "old_col": "d2"}\n')
      f.write('{"id": 5, "price": 50.0, "old_col": "e1"}\n')
    with open(right_p, 'w', encoding='utf-8') as f:
      f.write('{"id": 1, "price": 10.0, "new_col": "x"}\n')
      f.write('{"id": 2, "price": 99.5, "new_col": "y"}\n')
      f.write('{"id": 4, "price": 40.0, "new_col": "z1"}\n')
      f.write('{"id": 4, "price": 88.0, "new_col": "z2"}\n')
      f.write('{"id": 5, "price": 50.0, "new_col": "w1"}\n')
      f.write('{"id": 5, "price": 50.0, "new_col": "w2"}\n')
      f.write('{"id": 6, "price": 60.0, "new_col": "n"}\n')

    reports = {}
    for mode, env_ov in (
        ('fast', {}),
        ('beam', {'BEAM_CLI_NO_FAST_PATH': '1'}),
    ):
      with mock.patch.dict(os.environ, env_ov, clear=False):
        code, rep = diff.run_diff(
            left_p,
            right_p,
            keys=['id'],
            columns='shared',
            key_limit=1)
      self.assertEqual(code, diff.EXIT_DIFF_MISMATCH)
      reports[mode] = rep

    self.assertEqual(reports['fast']['summary'], reports['beam']['summary'])
    self.assertEqual(
        reports['fast']['schema_diff'], reports['beam']['schema_diff'])
    self.assertEqual(reports['fast']['summary']['matched_rows'], 1)
    self.assertEqual(reports['fast']['summary']['only_in_left'], 1)
    self.assertEqual(reports['fast']['summary']['only_in_right'], 1)
    # Key 2 (single value mismatch) + Key 4 (2-row multiset mismatch) = 3
    self.assertEqual(reports['fast']['summary']['value_mismatches'], 3)
    # Key 5 (1 row left vs 2 rows right) = 1 duplicate_key_mismatch
    self.assertEqual(reports['fast']['summary']['duplicate_key_mismatches'], 1)
    self.assertTrue(reports['fast']['differing_keys']['truncated'])
    self.assertTrue(reports['beam']['differing_keys']['truncated'])

    # Empty 0-row JSONL parity across fast & Beam runners
    e1 = os.path.join(self.tmp, 'empty1.jsonl')
    e2 = os.path.join(self.tmp, 'empty2.jsonl')
    open(e1, 'w', encoding='utf-8').close()
    open(e2, 'w', encoding='utf-8').close()
    c_fast, r_fast = diff.run_diff(e1, e2)
    with mock.patch.dict(os.environ, {'BEAM_CLI_NO_FAST_PATH': '1'}, clear=False):
      c_beam, r_beam = diff.run_diff(e1, e2)
    self.assertEqual((c_fast, r_fast['match']), (diff.EXIT_OK, True))
    self.assertEqual((c_beam, r_beam['match']), (diff.EXIT_OK, True))

  def test_iceberg_catalog_sync_dry_run_same_dir_and_cross_table(self):
    # Regression for `read_iceberg_metadata` NameError & zero-copy Iceberg sync!
    wh = os.path.join(self.tmp, 'wh')
    src_pq = os.path.join(self.tmp, 'seed.parquet')
    pq.write_table(
        pa.Table.from_pylist([
            {'id': 1, 'region': 'us'},
            {'id': 2, 'region': 'eu'},
        ]),
        src_pq)
    src_uri = f'iceberg://analytics.events?warehouse={wh}'
    rc_seed, env_seed, err_seed = _run_cli([
        'pipe', src_pq, src_uri, '--zero-copy',
    ])
    self.assertEqual(rc_seed, cli.EXIT_OK, f'{env_seed} {err_seed}')

    # 1. Dry-run catalog sync from an Iceberg source
    dry = lakehouse.sync_lakehouse_catalog(
        src_uri,
        f'iceberg://analytics.events_copy?warehouse={wh}',
        dry_run=True)
    self.assertEqual(dry['status'], 'dry_run')
    self.assertEqual(dry['source_format'], 'iceberg')
    self.assertEqual(dry['files_to_sync'], 1)
    self.assertGreater(dry['total_bytes'], 0)
    self.assertEqual([f['name'] for f in dry['schema']], ['id', 'region'])

    # 2. Same-directory catalog sync does NOT duplicate data files
    same = lakehouse.sync_lakehouse_catalog(src_uri, src_uri)
    self.assertEqual(same['status'], 'synced')
    self.assertEqual(same['rows_registered'], 2)
    st_same = lakehouse.load_iceberg_state(os.path.join(wh, 'analytics', 'events'))
    self.assertEqual(st_same['raw_record_count'], 2)

    # 3. Cross-table zero-copy sync from Iceberg to a second Iceberg table
    dst_uri = f'iceberg://analytics.events_alias?warehouse={wh}'
    synced = lakehouse.sync_lakehouse_catalog(src_uri, dst_uri)
    self.assertEqual(synced['status'], 'synced')
    self.assertEqual(synced['rows_registered'], 2)
    st_dst = lakehouse.load_iceberg_state(
        os.path.join(wh, 'analytics', 'events_alias'))
    self.assertEqual(st_dst['raw_record_count'], 2)


if __name__ == '__main__':
  unittest.main()
