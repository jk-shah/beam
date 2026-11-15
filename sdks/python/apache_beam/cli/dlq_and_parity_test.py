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

"""Unit tests for `dlq.py` and `local_fast` vs Beam runner / Parquet parity."""

import bz2
import contextlib
import datetime
import decimal
import glob
import gzip
import io
import json
import os
import shutil
import tempfile
import unittest
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

import apache_beam as beam
from apache_beam.cli import dlq
from apache_beam.cli import main as cli
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
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


def _read_sink_records(prefix: str, fmt: str) -> list[dict]:
  matches = (
      [prefix] if os.path.isfile(prefix) else sorted(glob.glob(prefix + '*')))
  rows: list[dict] = []
  for fp in matches:
    if os.path.isdir(fp):
      continue
    if fmt == 'parquet':
      rows.extend(pq.read_table(fp).to_pylist())
    else:
      with open(fp, encoding='utf-8') as f:
        for ln in f:
          if ln.strip():
            rows.append(json.loads(ln))
  return rows


class DlqUnitTest(unittest.TestCase):

  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-cli-dlq-')

  def tearDown(self):
    shutil.rmtree(self.tmp, ignore_errors=True)

  def test_safe_json_dumps_and_transform_dlq_redaction(self):
    payload = {
        'raw': memoryview(b'abc'),
        'amt': decimal.Decimal('19.95'),
        'nan_v': float('nan'),
        'inf_v': float('inf'),
        'ts': Timestamp(1700000000),
        'dt': datetime.date(2026, 5, 1),
        'ssn': '123-45-6789',
        'nested': [{'ssn': '999-99-9999', 'keep': 'ok'}],
    }
    dumped = json.loads(dlq.safe_json_dumps(payload))
    self.assertEqual(dumped['raw']['_encoding'], 'base64')
    self.assertEqual(dumped['amt'], '19.95')
    self.assertEqual(dumped['nan_v'], 'nan')
    self.assertEqual(dumped['inf_v'], 'inf')

    dlq_row = dlq.make_transform_dlq_row(
        'Filter',
        'ValueError',
        "failed on ssn='123-45-6789' keep=ok",
        payload,
        redact_fields=['ssn'])
    self.assertNotIn('123-45-6789', dlq_row.error)
    self.assertNotIn('123-45-6789', dlq_row.raw_payload)
    self.assertNotIn('999-99-9999', dlq_row.raw_payload)
    self.assertIn('<REDACTED>', dlq_row.raw_payload)

  def test_csv_recovering_multiline_overflow_compressed_and_leading_zeros(self):
    # 1. Multiline CSV record exceeding max_record_bytes recovers next rows!
    raw_csv = 'id,note\n"unclosed\nline2_overflow\n2,clean_after\n'
    items = list(
        dlq.iter_csv_rows_recovering(
            io.StringIO(raw_csv), max_record_bytes=16))
    self.assertEqual(items[0][0], ['id', 'note'])
    self.assertIsNotNone(items[1][1])  # csv.Error on unclosed quote overflow
    self.assertIn(['2', 'clean_after'], [cells for cells, err, _ in items if not err])

    # 2. Leading-zero scan across compressed bz2 CSV shards & header reordering
    s1 = os.path.join(self.tmp, 'z-0.csv.bz2')
    s2 = os.path.join(self.tmp, 'z-1.csv.bz2')
    with bz2.open(s1, 'wt', encoding='utf-8') as f:
      f.write('id,zip\n1,94043\n2,10001\n3,60601\n')
    with bz2.open(s2, 'wt', encoding='utf-8') as f:
      f.write('zip,id\n00501,4\n')

    header, kinds = dlq._preflight_csv_schema(
        os.path.join(self.tmp, 'z-*.csv.bz2'),
        sample_rows=2,
        scan_all_for_leading_zeros=True)
    self.assertEqual(header, ['id', 'zip'])
    self.assertEqual(kinds['zip'], 'str')

    # Without full leading-zero scan, parsing '00501' as int raises ValueError
    with self.assertRaises(ValueError):
      dlq._parse_cell('00501', 'int')
    self.assertTrue(dlq._parse_cell('true', 'bool'))
    self.assertFalse(dlq._parse_cell('FALSE', 'bool'))
    with self.assertRaises(ValueError):
      dlq._parse_cell('maybe', 'bool')

  def test_json_options_depth_limit_and_compressed_jsonl(self):
    self.assertEqual(dlq.parse_sample_rows_opt(None), 100)
    self.assertIsNone(dlq.parse_sample_rows_opt('all'))
    self.assertEqual(dlq.parse_sample_rows_opt('25'), 25)
    for bad in (True, 0, -5, 'nope'):
      with self.assertRaises(ValueError):
        dlq.parse_sample_rows_opt(bad)

    self.assertEqual(dlq.parse_unknown_fields_opt('ignore'), 'ignore')
    with self.assertRaises(ValueError):
      dlq.parse_unknown_fields_opt('drop_all')

    warn_msg = dlq.schema_drift_warning(3, ['secret_col', 'bad;inject'])
    self.assertIn('secret_col', warn_msg)
    self.assertNotIn('bad;inject', warn_msg)

    # Exceeding MAX_JSON_NESTING_DEPTH raises ValueError cleanly
    deep_kind: dict = {'leaf': 'int'}
    deep_val: dict = {'leaf': 1}
    for _ in range(dlq.MAX_JSON_NESTING_DEPTH + 2):
      deep_kind = {'sub': deep_kind}
      deep_val = {'sub': deep_val}
    with self.assertRaises(ValueError):
      dlq._coerce_json_value(deep_val, deep_kind, 'root')

    gz_jl = os.path.join(self.tmp, 'events.jsonl.gz')
    with gzip.open(gz_jl, 'wt', encoding='utf-8') as f:
      f.write('{"id": 1, "flag": true}\n{"id": 2, "flag": false}\n')
    inferred = dlq.infer_json_kinds(gz_jl)
    self.assertEqual(inferred, {'id': 'int', 'flag': 'bool'})

  def test_cli_limit_rows_validation_and_rowify_nested_beam_pipeline(self):
    with self.assertRaises(ValueError):
      dlq.cli_limit_rows(-1)
    with self.assertRaises(ValueError):
      dlq.cli_limit_rows(True)

    nested_pq = os.path.join(self.tmp, 'nested.parquet')
    pq.write_table(
        pa.Table.from_pylist(
            [
                {'id': 1, 'addr': {'city': 'NYC'}},
                {'id': 2, 'addr': None},
                {'id': 3, 'addr': {'city': 'LA'}},
            ],
            schema=pa.schema([
                ('id', pa.int64()),
                ('addr', pa.struct([('city', pa.string())])),
            ])),
        nested_pq)
    with TestPipeline('FnApiRunner') as p:
      raw = (
          p
          | beam.io.ReadFromParquet(nested_pq, as_rows=True)
          | 'Rowify' >> dlq.cli_rowify_nested()
          | 'Cap2' >> dlq.cli_limit_rows(2)
          | 'Extract' >> beam.Map(
              lambda r: (r.id, r.addr.city if r.addr is not None else None)))
      assert_that(raw, equal_to([(1, 'NYC'), (2, None)]))

  def test_bq_schema_conversion_and_cli_write_to_bigquery_guards(self):
    row_t = RowTypeConstraint.from_fields([
        ('id', int),
        ('amt', decimal.Decimal),
        ('ts', Timestamp),
        ('tags', list[str]),
    ])
    bq_schema = dlq._beam_row_type_to_bq_schema_dict(row_t)
    by_name = {f['name']: (f['type'], f['mode']) for f in bq_schema['fields']}
    self.assertEqual(by_name['id'], ('INTEGER', 'NULLABLE'))
    self.assertEqual(by_name['amt'], ('NUMERIC', 'NULLABLE'))
    self.assertEqual(by_name['ts'], ('TIMESTAMP', 'NULLABLE'))
    self.assertEqual(by_name['tags'], ('STRING', 'REPEATED'))

    # WRITE_TRUNCATE without --temp_location fails fast at expand time
    with self.assertRaisesRegex(ValueError, 'temp_location'):
      with TestPipeline('FnApiRunner') as p:
        _ = (
            p
            | beam.Create([beam.Row(id=1)]).with_output_types(
                RowTypeConstraint.from_fields([('id', int)]))
            | dlq.cli_write_to_bigquery(
                'proj:ds.tbl', write_disposition='WRITE_TRUNCATE'))

    # WRITE_EMPTY rejects non-empty existing table; streaming error_handling
    # formats failed_rows_with_errors into 4-column BigQueryWriteError DLQ rows.
    fake_bq_client = mock.MagicMock()
    fake_bq_client.project = 'proj'
    non_empty_tbl = mock.MagicMock()
    non_empty_tbl.num_rows = 5
    fake_bq_client.get_table.return_value = non_empty_tbl
    with mock.patch('google.cloud.bigquery.Client', return_value=fake_bq_client):
      with self.assertRaisesRegex(ValueError, 'already exists and has 5 rows'):
        with TestPipeline('FnApiRunner') as p:
          _ = (
              p
              | beam.Create([beam.Row(id=1)]).with_output_types(
                  RowTypeConstraint.from_fields([('id', int)]))
              | dlq.cli_write_to_bigquery(
                  'proj:ds.tbl', write_disposition='WRITE_EMPTY'))

      empty_tbl = mock.MagicMock()
      empty_tbl.num_rows = 0
      fake_bq_client.get_table.return_value = empty_tbl

      class _FakeWriteResult:
        def __init__(self, failed_pcoll):
          self.failed_rows_with_errors = failed_pcoll

      class _FakeWriteBq(beam.PTransform):
        Method = beam.io.WriteToBigQuery.Method

        def __init__(self, table, **kwargs):
          del table, kwargs

        def expand(self, pcoll):
          failed = pcoll | 'EmitFailed' >> beam.Map(
              lambda d: ('proj:ds.tbl', d, [{'reason': 'invalid'}]))
          return _FakeWriteResult(failed)

      with mock.patch(
          'apache_beam.io.gcp.bigquery.WriteToBigQuery', _FakeWriteBq):
        with TestPipeline('FnApiRunner') as p:
          res_bq = (
              p
              | beam.Create([
                  beam.Row(
                      id=1,
                      amt=decimal.Decimal('9.99'),
                      ts=Timestamp(1700000000),
                      dt=datetime.date(2026, 5, 1))
              ]).with_output_types(
                  RowTypeConstraint.from_fields([
                      ('id', int),
                      ('amt', decimal.Decimal),
                      ('ts', Timestamp),
                      ('dt', datetime.date),
                  ]))
              | dlq.cli_write_to_bigquery(
                  'proj:ds.tbl',
                  write_disposition='WRITE_EMPTY',
                  error_handling={'output': 'errors'}))
          assert_that(
              res_bq['errors'] | beam.Map(lambda r: (r.step, r.error_type)),
              equal_to([('WriteToBigQuery', 'BigQueryWriteError')]))


class RunnerAndSinkParityTest(unittest.TestCase):
  """Verifies `local_fast` and `BEAM_CLI_NO_FAST_PATH=1` match identically."""

  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-cli-parity-')

  def tearDown(self):
    shutil.rmtree(self.tmp, ignore_errors=True)

  def test_dirty_csv_where_select_expect_dlq_redact_parity_across_runners_and_sinks(
      self):
    s1 = os.path.join(self.tmp, 'in-0.csv')
    s2 = os.path.join(self.tmp, 'in-1.csv')
    with open(s1, 'w', encoding='utf-8') as f:
      f.write(
          'id,status,amount,ssn\n'
          '1,PAID,100.0,111-11-1111\n'
          '2,PENDING,50.0,222-22-2222\n'
          '3,PAID,-5.0,333-33-3333\n'
          '4,PAID,bad_float,444-44-4444\n'
          '5,only_two_cols\n')
    with open(s2, 'w', encoding='utf-8') as f:
      # Reordered columns in shard 2!
      f.write(
          'ssn,amount,status,id\n'
          '666-66-6666,200.0,PAID,6\n')

    # Guardrail: referencing `amount` in --expect after --select dropped it
    # must fail pre-flight instead of silently quarantining every row!
    rc_drop_chk, env_drop_chk, _ = _run_cli([
        'pipe',
        s1,
        os.path.join(self.tmp, 'never.jsonl'),
        '--select',
        'id, round(amount * 1.1, 2) AS total',
        '--expect',
        'amount > 0',
        '--dlq',
        os.path.join(self.tmp, 'never_dlq.jsonl'),
    ])
    self.assertEqual(rc_drop_chk, cli.EXIT_USAGE)
    self.assertIn('dropped by --select', env_drop_chk['error']['message'])

    # 1. Cross-runner (`local_fast` vs `beam`) & sink (`.jsonl` vs `.parquet`) parity
    results = {}
    for mode_name, extra_env in (
        ('fast', None),
        ('beam', {'BEAM_CLI_NO_FAST_PATH': '1'}),
    ):
      for ext, fmt in (('.jsonl', 'json'), ('.parquet', 'parquet')):
        out_p = os.path.join(self.tmp, f'out_{mode_name}{ext}')
        dlq_p = os.path.join(self.tmp, f'dlq_{mode_name}_{fmt}.jsonl')
        rc, env, stderr = _run_cli(
            [
                'pipe',
                os.path.join(self.tmp, 'in-*.csv'),
                out_p,
                '--from-opt',
                'fields=id:int,status:str,amount:float,ssn:str',
                '--where',
                "status == 'PAID'",
                '--select',
                'id, round(amount * 1.1, 2) AS total',
                '--expect',
                'total > 0',
                '--dlq',
                dlq_p,
                '--assert-counts',
            ],
            extra_env=extra_env)
        self.assertEqual(
            rc, cli.EXIT_OK, f'{mode_name}/{fmt} failed: {env} {stderr}')
        if mode_name == 'fast':
          self.assertEqual(env['runner'], 'local_fast')
        rows = sorted(
            _read_sink_records(out_p, fmt), key=lambda r: r['id'])
        dlq_rows = _read_sink_records(dlq_p, 'json')
        results[(mode_name, fmt)] = (rows, len(dlq_rows))

    expected_rows = [
        {'id': 1, 'total': 110.0},
        {'id': 6, 'total': 220.0},
    ]
    for key, (rows, dlq_cnt) in results.items():
      self.assertEqual(rows, expected_rows, f'mismatch for {key}')
      # Row 3 (expect), Row 4 (bad_float), Row 5 (col mismatch) -> 3 DLQ rows
      self.assertEqual(dlq_cnt, 3, f'DLQ count mismatch for {key}')

    # 2. Verify PII field redaction (`--dlq-redact-fields ssn`) into Parquet sink
    red_out = os.path.join(self.tmp, 'red_out.parquet')
    red_dlq = os.path.join(self.tmp, 'red_dlq.jsonl')
    rc_r, env_r, err_r = _run_cli([
        'pipe',
        os.path.join(self.tmp, 'in-*.csv'),
        red_out,
        '--from-opt',
        'fields=id:int,status:str,amount:float,ssn:str',
        '--where',
        "status == 'PAID'",
        '--select',
        'id, round(amount * 1.1, 2) AS total',
        '--expect',
        'total > 0',
        '--dlq',
        red_dlq,
        '--dlq-redact-fields',
        'ssn',
        '--assert-counts',
    ])
    self.assertEqual(rc_r, cli.EXIT_OK, f'{env_r} {err_r}')
    dlq_blob = json.dumps(_read_sink_records(red_dlq, 'json'))
    self.assertNotIn('444-44-4444', dlq_blob)
    self.assertIn('<REDACTED>', dlq_blob)

  def test_jsonl_schema_drift_and_limit_conservation_parity(self):
    jl = os.path.join(self.tmp, 'drift.jsonl')
    with open(jl, 'w', encoding='utf-8') as f:
      f.write('{"id": 1, "v": 10}\n')
      f.write('{"id": 2, "v": 20}\n')
      f.write('{"id": 3, "v": 30, "surprise": "drift"}\n')
      f.write('{"id": 4, "v": 40}\n')

    for mode_name, extra_env in (
        ('fast', None),
        ('beam', {'BEAM_CLI_NO_FAST_PATH': '1'}),
    ):
      out_pq = os.path.join(self.tmp, f'drift_{mode_name}.parquet')
      rc, env, stderr = _run_cli(
          [
              'pipe',
              jl,
              out_pq,
              '--from-opt',
              'sample_rows=2',
              '--drop-bad',
              '-n',
              '2',
              '--assert-counts',
          ],
          extra_env=extra_env)
      self.assertEqual(rc, cli.EXIT_OK, f'{mode_name}: {env} {stderr}')
      rows = _read_sink_records(out_pq, 'parquet')
      self.assertEqual(len(rows), 2)
      self.assertEqual(env['metrics']['rows_written'], 2)


if __name__ == '__main__':
  unittest.main()
