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

"""End-to-end functional & pipeline tests for all v3.1 `beam` CLI commands."""

import contextlib
import decimal
import glob
import io
import json
import math
import os
import shutil
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


if __name__ == '__main__':
  unittest.main()
