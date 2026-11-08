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

"""End-to-end functional & pipeline tests for extended `beam` CLI commands."""

import contextlib
import decimal
import glob
import io
import json
import math
import os
from pathlib import Path
import shutil
import sys
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

  def test_dogfood_polish_shard_prefix_emit_yaml_plan_hash_and_dirty_warnings(self):
    dirty_csv = os.path.join(self.tmp, 'dirty.csv')
    with open(dirty_csv, 'w', encoding='utf-8') as f:
      f.write('id,amt\n1,10.5\n2,20.0\n3,30.0\n4,ERR\n5\n')

    # 1. `inspect` warns on malformed/dirty CSV rows in sample!
    rc_i, env_i, _ = _run_json(['inspect', dirty_csv, '--stats'])
    self.assertEqual(rc_i, cli.EXIT_OK)
    self.assertTrue(env_i['warnings'])
    self.assertEqual(env_i['outputs']['stats']['malformed_rows_in_sample'], 1)

    # 2. Write sharded Parquet, then read & diff WITHOUT trailing `*`!
    out_pq = os.path.join(self.tmp, 'sharded', 'clean.parquet')
    dlq_p = os.path.join(self.tmp, 'sharded', 'dlq.json')
    rc_p, _, err_p = _run_json([
        'pipe',
        '--from', dirty_csv,
        '--to', out_pq,
        '--dlq', dlq_p,
        '--runner=FnApiRunner',
    ])
    self.assertEqual(rc_p, cli.EXIT_OK, err_p)

    out_jsonl = os.path.join(self.tmp, 'sharded', 'copy.jsonl')
    rc_p2, _, err_p2 = _run_json([
        'pipe',
        '--from', out_pq,  # no `*`!
        '--to', out_jsonl,
        '--runner=FnApiRunner',
    ])
    self.assertEqual(rc_p2, cli.EXIT_OK, err_p2)

    rc_d, env_d, err_d = _run_json([
        'diff', out_pq, out_jsonl, '--key', 'id', '--runner=FnApiRunner'
    ])
    self.assertEqual(rc_d, cli.EXIT_OK, err_d)
    self.assertEqual(env_d['runner'], 'FnApiRunner')

    # 3. `--emit-yaml` succeeds even though `out_pq` already exists, and matches
    # `beam check` `plan_hash` + expands `--plan` without `--trust-providers`!
    rc_ey, env_ey, err_ey = _run_json([
        'pipe',
        '--from', dirty_csv,
        '--to', out_pq,
        '--dlq', dlq_p,
        '--emit-yaml',
    ])
    self.assertEqual(rc_ey, cli.EXIT_OK, err_ey)
    yaml_file = os.path.join(self.tmp, 'exported.yaml')
    with open(yaml_file, 'w', encoding='utf-8') as f:
      f.write(env_ey['outputs']['yaml'])

    rc_chk, env_chk, err_chk = _run_json(['check', '--plan', yaml_file])
    self.assertEqual(rc_chk, cli.EXIT_OK, err_chk)
    self.assertEqual(
        env_ey['outputs']['plan_hash'], env_chk['outputs']['plan_hash'])

  def test_preview_wheel_staging_and_worker_unpickle_roundtrip(self):
    import subprocess
    import sys
    import zipfile
    from apache_beam.internal import pickler

    preview_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', '..', 'cli-preview'))
    if preview_root not in sys.path:
      sys.path.insert(0, preview_root)
    from apache_beam_cli_preview import launcher

    cache_dir = os.path.join(self.tmp, 'wheel_cache')
    with mock.patch.dict(os.environ, {'BEAM_CACHE_DIR': cache_dir}):
      whl_path = launcher.ensure_preview_wheel()
      self.assertTrue(os.path.isfile(whl_path))
      with zipfile.ZipFile(whl_path, 'r') as zf:
        names = set(zf.namelist())
      self.assertIn('apache_beam_cli_preview.pth', names)
      self.assertIn('apache_beam_cli_preview/_worker_bootstrap.py', names)
      self.assertIn('apache_beam_cli_preview/_bundled_cli/main.py', names)
      self.assertIn('apache_beam_cli_preview/_bundled_cli/dlq.py', names)
      self.assertIn('apache_beam_cli_preview/_bundled_cli/diff.py', names)

      injected = launcher.maybe_inject_dataflow_worker_args(
          ['--runner=DataflowRunner', '--project=p'])
      self.assertIn(f'--extra_package={whl_path}', injected)
      self.assertIn(
          '--beam_plugin=apache_beam_cli_preview._worker_bootstrap', injected)
      self.assertEqual(
          injected, launcher.maybe_inject_dataflow_worker_args(injected))

      # Simulate a remote Dataflow worker container (without repo `sdks/python`
      # `apache_beam/cli` if extracted into an isolated site dir) unpickling
      # DoFns/PTransforms from both `apache_beam.cli.*` and `_apache_beam_cli_pkg.*`!
      from apache_beam.cli import dlq
      csv_sample = os.path.join(self.tmp, 's.csv')
      with open(csv_sample, 'w', encoding='utf-8') as f:
        f.write('id,v\n1,10\n')
      payload = pickler.dumps((
          dlq.read_csv_with_dlq(csv_sample),
          dlq.cli_count_rows('rows_written'),
          dlq.cli_write_to_bigquery('p:d.t'),
      ))
      payload_file = os.path.join(self.tmp, 'pickled.bin')
      with open(payload_file, 'wb') as f:
        f.write(payload)

      worker_site = os.path.join(self.tmp, 'worker_site')
      with zipfile.ZipFile(whl_path, 'r') as zf:
        zf.extractall(worker_site)

      check_code = (
          'import sys; '
          f'sys.path.insert(0, {worker_site!r}); '
          'import apache_beam_cli_preview._worker_bootstrap; '
          'from apache_beam.internal import pickler; '
          'import _apache_beam_cli_pkg.diff as fast_diff; '
          'import apache_beam.typehints.arrow_type_compatibility as atc; '
          'import apache_beam.yaml.yaml_mapping as ym; '
          f'objs = pickler.loads(open({payload_file!r}, "rb").read()); '
          'assert len(objs) == 3; '
          'assert getattr(atc.PyarrowBatchConverter, "_cli_preview_patched", False); '
          'assert getattr(ym.validate_generic_expression, "_cli_preview_patched", False); '
          'assert callable(fast_diff.run_diff)'
      )
      proc = subprocess.run(
          [sys.executable, '-c', check_code],
          capture_output=True,
          text=True,
          check=False)
      self.assertEqual(proc.returncode, 0, proc.stderr)

  def test_remote_diff_requires_temp_location_and_wait_resolves_output_files(self):
    from apache_beam.cli import jobs
    left = os.path.join(self.tmp, 'l.csv')
    right = os.path.join(self.tmp, 'r.csv')
    with open(left, 'w', encoding='utf-8') as f:
      f.write('id,val\n1,a\n2,b\n')
    with open(right, 'w', encoding='utf-8') as f:
      f.write('id,val\n1,a\n2,b\n')

    # 1. `beam diff --runner=DataflowRunner` without `--temp_location` fails fast.
    rc_d, env_d, _ = _run_json([
        'diff', left, right, '--key', 'id', '--runner=DataflowRunner'
    ])
    self.assertEqual(rc_d, cli.EXIT_USAGE)
    self.assertIn('--temp_location', env_d['error']['message'])

    # 2. `jobs.wait_for_job` resolves sharded `output_prefix` & `verify_diff`.
    shard = os.path.join(self.tmp, 'out.csv-00000-of-00001')
    with open(shard, 'w', encoding='utf-8') as f:
      f.write('id,val\n1,a\n2,b\n')
    dlq_shard = os.path.join(self.tmp, 'dlq.json-00000-of-00001')
    with open(dlq_shard, 'w', encoding='utf-8') as f:
      f.write('')

    with mock.patch.dict(os.environ, {'BEAM_HOME': self.tmp}):
      jobs.record_job_submission(
          '2026-11-08_job-verify',
          runner='DataflowRunner',
          state='JOB_STATE_RUNNING',
          command='pipe',
          project='demo-proj',
          region='us-central1',
          post_checks={
              'assert_counts': True,
              'verify_diff': {
                  'left': left,
                  'right': os.path.join(self.tmp, 'out.csv'),
                  'keys': ['id'],
              },
          },
          outputs={
              'output_prefix': os.path.join(self.tmp, 'out.csv'),
              'dlq_prefix': os.path.join(self.tmp, 'dlq.json'),
          })
      code, rec = jobs.wait_for_job(
          '2026-11-08_job-verify',
          poll_interval=0.01,
          query_fn=lambda jid, p, r: (
              'JOB_STATE_DONE',
              {
                  'rows_read': 2,
                  'rows_filtered_out': 0,
                  'dlq_rows': 0,
                  'rows_written': 2,
              }))
      self.assertEqual(code, cli.EXIT_OK)
      self.assertEqual(rec['outputs']['file_count'], 1)
      self.assertEqual(len(rec['outputs']['dlq_files']), 1)
      self.assertTrue(rec['outputs']['verify']['match'])

  def test_package_yaml_zero_docker_and_stdin(self):
    yaml_path = os.path.join(self.tmp, 'orders_ingest.yaml')
    yaml_content = (
        'pipeline:\n'
        '  type: chain\n'
        '  transforms:\n'
        '    - type: ReadFromCsv\n'
        '      config:\n'
        '        path: "{{ input_csv }}"\n'
        '    - type: Filter\n'
        '      config:\n'
        '        language: generic\n'
        '        keep: "amount >= {{ min_amount | default(100) }}"\n'
        '    - type: WriteToParquet\n'
        '      config:\n'
        '        path: "{{ output_parquet }}"\n')
    with open(yaml_path, 'w', encoding='utf-8') as f:
      f.write(yaml_content)

    spec_out = os.path.join(self.tmp, 'orders.template.json')
    rc, env, _ = _run_json([
        'package',
        yaml_path,
        '--to',
        spec_out,
        '--name',
        'orders-ingest',
        '--image',
        'gcr.io/dataflow-templates/yaml-template:test',
        '--project',
        'demo-proj',
        '--region',
        'us-central1',
        '--subnetwork',
        'regions/us-central1/subnetworks/default',
        '--no-use-public-ips',
        '--temp-location',
        'gs://demo-bucket/tmp',
    ])
    self.assertEqual(rc, cli.EXIT_OK)
    out = env['outputs']
    self.assertEqual(out['sdk_language'], 'YAML')
    self.assertFalse(out['docker_build_required'])
    params = {p['name']: p for p in out['parameters']}
    self.assertEqual(list(params.keys()), ['input_csv', 'min_amount', 'output_parquet'])
    self.assertFalse(params['input_csv']['isOptional'])
    self.assertTrue(params['min_amount']['isOptional'])
    self.assertEqual(params['min_amount']['defaultValue'], '100')
    self.assertFalse(params['output_parquet']['isOptional'])

    with open(spec_out, encoding='utf-8') as f:
      spec = json.load(f)
    self.assertEqual(spec['sdkInfo']['language'], 'YAML')
    self.assertTrue(
        spec['metadata']['yamlDefinition'].startswith(yaml_content.rstrip('\n')))
    self.assertIn('requirements_file: null', spec['metadata']['yamlDefinition'])
    self.assertEqual(
        spec['defaultEnvironment']['ipConfiguration'], 'WORKER_IP_PRIVATE')
    self.assertIn('gcloud dataflow flex-template run', out['launch']['gcloud'])

    # Also verify `beam package -` reading YAML from stdin!
    stdin_spec = os.path.join(self.tmp, 'stdin.template.json')
    with mock.patch('sys.stdin', io.StringIO(yaml_content)):
      rc2, env2, _ = _run_json([
          'package',
          '-',
          '--to',
          stdin_spec,
          '--image',
          'gcr.io/dataflow-templates/yaml-template:test',
      ])
    self.assertEqual(rc2, cli.EXIT_OK)
    self.assertTrue(os.path.isfile(stdin_spec))
    self.assertEqual(env2['outputs']['sdk_language'], 'YAML')

  def test_package_python_ast_and_custom_provider_yaml_bundle(self):
    # 1. Custom Python pipeline (.py) -> AST extracts PipelineOptions without executing!
    py_file = os.path.join(self.tmp, 'custom_etl.py')
    with open(py_file, 'w', encoding='utf-8') as f:
      f.write(
          'import apache_beam as beam\n'
          'from apache_beam.options.pipeline_options import PipelineOptions\n'
          'raise RuntimeError("MUST NOT BE EXECUTED DURING PACKAGE")\n'
          'class CustomOptions(PipelineOptions):\n'
          '  @classmethod\n'
          '  def _add_argparse_args(cls, parser):\n'
          '    parser.add_argument("--input-table", required=True, help="Source table")\n'
          '    parser.add_value_provider_argument("--batch_size", default=500)\n')
    req_file = os.path.join(self.tmp, 'requirements.txt')
    with open(req_file, 'w', encoding='utf-8') as f:
      f.write('pyarrow>=14.0.0\n')
    bundle_dir = os.path.join(self.tmp, 'py_bundle')
    rc, env, _ = _run_json([
        'package',
        py_file,
        '--output-dir',
        bundle_dir,
        '--requirements',
        req_file,
        '--image',
        'us-central1-docker.pkg.dev/demo/df/custom-etl:v1',
    ])
    self.assertEqual(rc, cli.EXIT_OK)
    out = env['outputs']
    self.assertEqual(out['sdk_language'], 'PYTHON')
    self.assertTrue(out['docker_build_required'])
    params = {p['name']: p for p in out['parameters']}
    self.assertIn('input_table', params)
    self.assertFalse(params['input_table']['isOptional'])
    self.assertEqual(params['input_table']['helpText'], 'Source table')
    self.assertIn('batch_size', params)
    self.assertTrue(params['batch_size']['isOptional'])
    self.assertEqual(params['batch_size']['defaultValue'], '500')
    self.assertTrue(os.path.isfile(os.path.join(bundle_dir, 'Dockerfile')))
    self.assertTrue(os.path.isfile(os.path.join(bundle_dir, 'metadata.json')))
    self.assertTrue(os.path.isfile(os.path.join(bundle_dir, 'requirements.txt')))

    # 2. YAML with custom Python provider (`apache_beam.cli.dlq`) gates `--sdk=yaml`
    # and succeeds cleanly with `--sdk=python`!
    dlq_yaml = os.path.join(self.tmp, 'dlq_pipe.yaml')
    with open(dlq_yaml, 'w', encoding='utf-8') as f:
      f.write(
          'providers:\n'
          '  - type: python\n'
          '    config: {}\n'
          '    transforms:\n'
          '      ReadCsvWithDlq: "apache_beam.cli.dlq:ReadCsvWithDlq"\n'
          'pipeline:\n'
          '  type: chain\n'
          '  transforms:\n'
          '    - type: ReadCsvWithDlq\n'
          '      config:\n'
          '        path: "{{ src }}"\n')
    rc_gate, env_gate, _ = _run_json([
        'package', dlq_yaml, '--sdk', 'yaml', '--dry-run'
    ])
    self.assertEqual(rc_gate, cli.EXIT_USAGE)
    self.assertIn('--sdk=python', env_gate['error']['message'])

    dlq_bundle = os.path.join(self.tmp, 'dlq_bundle')
    rc_py, env_py, _ = _run_json([
        'package',
        dlq_yaml,
        '--sdk',
        'python',
        '--output-dir',
        dlq_bundle,
        '--image',
        'us-central1-docker.pkg.dev/demo/df/dlq-pipe:v1',
    ])
    self.assertEqual(rc_py, cli.EXIT_OK)
    self.assertEqual(env_py['outputs']['sdk_language'], 'PYTHON')
    ep_code = Path(dlq_bundle, 'flex_yaml_entrypoint.py').read_text(encoding='utf-8')
    self.assertIn("'src'", ep_code)
    self.assertIn('--jinja_variables=', ep_code)

  def test_tail_follow_live_file_and_pubsub_ephemeral_vs_nack(self):
    import threading
    import time as time_mod

    # 1. Live file `-f` (`--follow`) with NDJSON streaming & ANSI sanitization.
    live_jsonl = os.path.join(self.tmp, 'stream.jsonl')
    with open(live_jsonl, 'w', encoding='utf-8') as f:
      f.write('{"id": 0, "note": "existing_before_follow"}\n')

    def _writer():
      time_mod.sleep(0.15)
      with open(live_jsonl, 'a', encoding='utf-8') as wf:
        wf.write('{"id": 1, "msg": "hello \\u001b[31mRED\\u001b[0m"}\n')
        wf.flush()
      time_mod.sleep(0.1)
      with open(live_jsonl, 'a', encoding='utf-8') as wf:
        wf.write('{"id": 2, "msg": "second"}\n')
        wf.flush()

    t = threading.Thread(target=_writer, daemon=True)
    t.start()
    out_buf = io.StringIO()
    err_buf = io.StringIO()
    with contextlib.redirect_stdout(out_buf), contextlib.redirect_stderr(err_buf):
      rc = cli.main([
          'tail', '-f', '-n', '2', '--poll-interval', '0.05', '--duration', '5s',
          live_jsonl, '--json'
      ])
    t.join(timeout=2.0)
    self.assertEqual(rc, cli.EXIT_OK)
    ndjson_lines = [
        json.loads(ln) for ln in out_buf.getvalue().strip().splitlines() if ln.strip()
    ]
    # 2 live record lines + 1 final summary envelope!
    self.assertEqual(len(ndjson_lines), 3)
    self.assertEqual(ndjson_lines[0]['event'], 'record')
    self.assertEqual(ndjson_lines[0]['record']['id'], 1)
    self.assertNotIn('\x1b', ndjson_lines[0]['record']['msg'])
    self.assertEqual(ndjson_lines[1]['record']['id'], 2)
    self.assertTrue(ndjson_lines[2]['ok'])
    self.assertEqual(ndjson_lines[2]['outputs']['sampled_count'], 2)

    # 2. Pub/Sub `-f` on existing subscription: deduplicates redeliveries and NEVER acks!
    from types import SimpleNamespace
    fake_msg_1 = SimpleNamespace(
        ack_id='ack-1',
        message=SimpleNamespace(
            message_id='m-1',
            data=b'{"event":"click"}',
            publish_time=None,
            attributes={}))
    fake_msg_dup = SimpleNamespace(
        ack_id='ack-1b',
        message=SimpleNamespace(
            message_id='m-1',
            data=b'{"event":"click"}',
            publish_time=None,
            attributes={}))
    fake_msg_2 = SimpleNamespace(
        ack_id='ack-2',
        message=SimpleNamespace(
            message_id='m-2',
            data=b'{"event":"buy"}',
            publish_time=None,
            attributes={}))

    fake_sub_client = mock.MagicMock()
    fake_sub_client.pull.side_effect = [
        SimpleNamespace(received_messages=[fake_msg_1]),
        SimpleNamespace(received_messages=[fake_msg_dup, fake_msg_2]),
    ]
    with mock.patch(
        'google.cloud.pubsub_v1.SubscriberClient',
        return_value=fake_sub_client):
      from apache_beam.cli import tail as tail_mod
      streamed = []
      res = tail_mod.run_tail(
          'pubsub://projects/p/subscriptions/s',
          limit=2,
          duration='3s',
          follow=True,
          poll_interval=0.05,
          on_record=lambda r, i: streamed.append(r))
      self.assertEqual(res['sampled_count'], 2)
      self.assertEqual(len(streamed), 2)
      fake_sub_client.acknowledge.assert_not_called()
      self.assertTrue(fake_sub_client.modify_ack_deadline.called)

    # 3. Pub/Sub `-f` on topic: ACKs private ephemeral sub AND deletes it on Ctrl-C!
    fake_topic_sub = mock.MagicMock()
    fake_topic_sub.pull.side_effect = [
        SimpleNamespace(received_messages=[fake_msg_1]),
        KeyboardInterrupt(),
    ]
    fake_pub_client = mock.MagicMock()
    fake_pub_client.list_topic_subscriptions.return_value = []
    with (
        mock.patch(
            'google.cloud.pubsub_v1.SubscriberClient',
            return_value=fake_topic_sub),
        mock.patch(
            'google.cloud.pubsub_v1.PublisherClient',
            return_value=fake_pub_client),
    ):
      res_t = tail_mod.run_tail(
          'pubsub://projects/p/topics/t',
          limit=None,
          duration=None,
          follow=True,
          poll_interval=0.05)
      self.assertTrue(res_t['interrupted'])
      self.assertEqual(res_t['sampled_count'], 1)
      fake_topic_sub.acknowledge.assert_called_once()
      fake_topic_sub.delete_subscription.assert_called_once()

  def test_delta_and_iceberg_zero_copy_inspect_cost_guard_and_diff(self):
    import pyarrow as pa
    import pyarrow.parquet as pq

    delta_dir = os.path.join(self.tmp, 'delta_orders')
    log_dir = os.path.join(delta_dir, '_delta_log')
    os.makedirs(log_dir, exist_ok=True)

    # Write 2 Parquet files in commit 0, then replace part-0 with part-2 in commit 1!
    p0 = os.path.join(delta_dir, 'part-0.parquet')
    p1 = os.path.join(delta_dir, 'part-1.parquet')
    p2 = os.path.join(delta_dir, 'part-2.parquet')
    pq.write_table(
        pa.table({
            'id': pa.array([1, 2], type=pa.int64()),
            'customer': pa.array(['Ada', 'Grace'], type=pa.string()),
            'amount': pa.array([10.5, 20.0], type=pa.float64()),
        }),
        p0)
    pq.write_table(
        pa.table({
            'id': pa.array([3], type=pa.int64()),
            'customer': pa.array(['Linus'], type=pa.string()),
            'amount': pa.array([30.25], type=pa.float64()),
        }),
        p1)
    pq.write_table(
        pa.table({
            'id': pa.array([1, 2], type=pa.int64()),
            'customer': pa.array(['Ada', 'Grace'], type=pa.string()),
            'amount': pa.array([15.0, 25.5], type=pa.float64()),
        }),
        p2)

    schema_str = json.dumps({
        'type': 'struct',
        'fields': [
            {'name': 'id', 'type': 'long', 'nullable': False, 'metadata': {}},
            {'name': 'customer', 'type': 'string', 'nullable': True, 'metadata': {}},
            {'name': 'amount', 'type': 'double', 'nullable': True, 'metadata': {}},
        ],
    })
    with open(os.path.join(log_dir, '00000000000000000000.json'), 'w', encoding='utf-8') as f:
      f.write(json.dumps({'protocol': {'minReaderVersion': 1, 'minWriterVersion': 2}}) + '\n')
      f.write(json.dumps({
          'metaData': {
              'id': 'delta-tbl-1',
              'schemaString': schema_str,
              'partitionColumns': [],
              'configuration': {},
          }
      }) + '\n')
      f.write(json.dumps({
          'add': {
              'path': 'part-0.parquet',
              'size': os.path.getsize(p0),
              'modificationTime': 1700000000000,
              'dataChange': True,
              'stats': json.dumps({'numRecords': 2}),
          }
      }) + '\n')
      f.write(json.dumps({
          'add': {
              'path': 'part-1.parquet',
              'size': os.path.getsize(p1),
              'modificationTime': 1700000000000,
              'dataChange': True,
              'stats': json.dumps({'numRecords': 1}),
          }
      }) + '\n')

    with open(os.path.join(log_dir, '00000000000000000001.json'), 'w', encoding='utf-8') as f:
      f.write(json.dumps({
          'remove': {
              'path': 'part-0.parquet',
              'deletionTimestamp': 1700000001000,
              'dataChange': True,
          }
      }) + '\n')
      f.write(json.dumps({
          'add': {
              'path': 'part-2.parquet',
              'size': os.path.getsize(p2),
              'modificationTime': 1700000001000,
              'dataChange': True,
              'stats': json.dumps({'numRecords': 2}),
          }
      }) + '\n')

    delta_uri = f'delta://{delta_dir}'

    # 1. Inspect latest Delta version (1) and time-travel version (0).
    rc_i, env_i, _ = _run_json(['inspect', delta_uri])
    self.assertEqual(rc_i, cli.EXIT_OK)
    self.assertEqual(env_i['outputs']['kind'], 'delta')
    self.assertEqual(env_i['outputs']['snapshot_version'], 1)
    self.assertEqual(env_i['outputs']['row_count'], 3)
    self.assertEqual(env_i['outputs']['file_count'], 2)
    rows_v1 = sorted(env_i['untrusted_data']['rows'], key=lambda r: r['id'])
    self.assertEqual([r['amount'] for r in rows_v1], [15.0, 25.5, 30.25])

    rc_v0, env_v0, _ = _run_json(['inspect', f'{delta_uri}?version=0'])
    self.assertEqual(rc_v0, cli.EXIT_OK)
    rows_v0 = sorted(env_v0['untrusted_data']['rows'], key=lambda r: r['id'])
    self.assertEqual([r['amount'] for r in rows_v0], [10.5, 20.0, 30.25])

    # 2. Cost guard `--max-bytes 10B` blocks before reading/copying (`EXIT_COST_LIMIT = 8`).
    wh_dir = os.path.join(self.tmp, 'iceberg_wh')
    iceberg_uri = f'iceberg://lake.orders_zc?warehouse={wh_dir}'
    rc_cg, env_cg, _ = _run_json([
        'pipe', '--from', delta_uri, '--to', iceberg_uri, '--zero-copy',
        '--max-bytes', '10B'
    ])
    self.assertEqual(rc_cg, cli.EXIT_COST_LIMIT)
    self.assertGreater(env_cg['estimated_bytes'], 10)

    # 3. Zero-copy Delta -> Iceberg v2 registration + `--verify`!
    rc_zc, env_zc, _ = _run_json([
        'pipe', '--from', delta_uri, '--to', iceberg_uri, '--zero-copy',
        '--verify', '--verify-key', 'id'
    ])
    self.assertEqual(rc_zc, cli.EXIT_OK)
    self.assertEqual(env_zc['runner'], 'zero_copy_metadata')
    self.assertEqual(env_zc['outputs']['bytes_copied'], 0)
    self.assertEqual(env_zc['outputs']['rows_registered'], 3)
    self.assertTrue(env_zc['outputs']['verify']['match'])
    with open(env_zc['outputs']['metadata_path'], encoding='utf-8') as mf:
      meta_json = json.load(mf)
    self.assertEqual(meta_json['format-version'], 2)
    self.assertIn('schema.name-mapping.default', meta_json['properties'])

    # 4. Inspect & diff the resulting Iceberg v2 table directly!
    rc_ii, env_ii, _ = _run_json(['inspect', iceberg_uri])
    self.assertEqual(rc_ii, cli.EXIT_OK)
    self.assertEqual(env_ii['outputs']['kind'], 'iceberg')
    self.assertEqual(env_ii['outputs']['row_count'], 3)

    rc_df, env_df, _ = _run_json(['diff', delta_uri, iceberg_uri, '--key', 'id'])
    self.assertEqual(rc_df, cli.EXIT_OK)
    self.assertTrue(env_df['outputs']['match'])

  def test_cross_engine_cdc_debezium_dlq_and_iceberg_merge_on_read(self):
    cdc_jsonl = os.path.join(self.tmp, 'debezium_orders.jsonl')
    events = [
        json.dumps({
            'op': 'c',
            'ts_ms': 1001,
            'source': {'lsn': 101},
            'after': {'id': 1, 'customer': 'Ada', 'amount': 10.5},
        }),
        json.dumps({
            'op': 'c',
            'ts_ms': 1002,
            'source': {'lsn': 102},
            'after': {'id': 2, 'customer': 'Grace', 'amount': 20.0},
        }),
        json.dumps({
            'op': 'c',
            'ts_ms': 1003,
            'source': {'lsn': 103},
            'after': {'id': 3, 'customer': 'Linus', 'amount': 30.0},
        }),
        '{corrupt_json_poison_pill',
        json.dumps({
            'op': 'c',
            'ts_ms': 1004,
            'after': {'id': None, 'customer': 'MissingPK', 'amount': 5.0},
        }),
        json.dumps({
            'op': 'u',
            'ts_ms': 1005,
            'source': {'lsn': 105},
            'before': {'id': 2, 'customer': 'Grace', 'amount': 20.0},
            'after': {'id': 2, 'customer': 'Grace', 'amount': 99.5},
        }),
        json.dumps({
            'op': 'd',
            'ts_ms': 1006,
            'source': {'lsn': 106},
            'before': {'id': 3, 'customer': 'Linus', 'amount': 30.0},
            'after': None,
        }),
    ]
    with open(cdc_jsonl, 'w', encoding='utf-8') as f:
      f.write('\n'.join(events) + '\n')

    wh_dir = os.path.join(self.tmp, 'wh_cdc')
    iceberg_cdc_uri = f'iceberg://lake.orders_cdc?warehouse={wh_dir}'
    dlq_path = os.path.join(self.tmp, 'cdc_dlq.json')

    rc, env, err = _run_json([
        'pipe',
        '--from',
        cdc_jsonl,
        '--to',
        iceberg_cdc_uri,
        '--cdc',
        '--primary-key',
        'id',
        '--dlq',
        dlq_path,
        '--assert-counts',
        '--runner=FnApiRunner',
    ])
    self.assertEqual(rc, cli.EXIT_OK, f'err={err} env={env}')
    self.assertEqual(env['metrics']['rows_read'], 7)
    self.assertEqual(env['metrics']['dlq_rows'], 2)
    self.assertEqual(env['metrics']['rows_written'], 5)

    # Verify Iceberg v2 Merge-on-Read state: 2 live rows (id=1 @ 10.5, id=2 @ 99.5; id=3 deleted!).
    rc_insp, env_insp, _ = _run_json(['inspect', iceberg_cdc_uri])
    self.assertEqual(rc_insp, cli.EXIT_OK)
    self.assertEqual(env_insp['outputs']['row_count'], 2)
    self.assertGreaterEqual(env_insp['outputs']['delete_file_count'], 1)
    self.assertEqual(env_insp['outputs']['identifier_columns'], ['id'])
    live_rows = sorted(env_insp['untrusted_data']['rows'], key=lambda r: r['id'])
    self.assertEqual(
        live_rows,
        [
            {'id': 1, 'customer': 'Ada', 'amount': 10.5},
            {'id': 2, 'customer': 'Grace', 'amount': 99.5},
        ])

    # Reconcile against golden expected JSONL via `beam diff`!
    expected_jsonl = os.path.join(self.tmp, 'expected_post_cdc.jsonl')
    with open(expected_jsonl, 'w', encoding='utf-8') as f:
      f.write(json.dumps({'id': 1, 'customer': 'Ada', 'amount': 10.5}) + '\n')
      f.write(json.dumps({'id': 2, 'customer': 'Grace', 'amount': 99.5}) + '\n')
    rc_d, env_d, _ = _run_json([
        'diff', iceberg_cdc_uri, expected_jsonl, '--key', 'id'
    ])
    self.assertEqual(rc_d, cli.EXIT_OK)
    self.assertTrue(env_d['outputs']['match'])

    # Append a second CDC batch (delete id=1, insert id=4) with `--if-exists=append`!
    cdc_batch_2 = os.path.join(self.tmp, 'debezium_orders_2.jsonl')
    with open(cdc_batch_2, 'w', encoding='utf-8') as f:
      f.write(json.dumps({
          'op': 'd',
          'ts_ms': 2001,
          'before': {'id': 1, 'customer': 'Ada', 'amount': 10.5},
          'after': None,
      }) + '\n')
      f.write(json.dumps({
          'op': 'c',
          'ts_ms': 2002,
          'after': {'id': 4, 'customer': 'Margaret', 'amount': 42.0},
      }) + '\n')
    rc2, env2, err2 = _run_json([
        'pipe',
        '--from',
        cdc_batch_2,
        '--to',
        iceberg_cdc_uri,
        '--cdc',
        '--primary-key',
        'id',
        '--if-exists=append',
        '--assert-counts',
        '--runner=FnApiRunner',
    ])
    self.assertEqual(rc2, cli.EXIT_OK, f'err={err2} env={env2}')
    rc_i2, env_i2, _ = _run_json(['inspect', iceberg_cdc_uri])
    self.assertEqual(rc_i2, cli.EXIT_OK)
    live_rows_2 = sorted(env_i2['untrusted_data']['rows'], key=lambda r: r['id'])
    self.assertEqual(
        live_rows_2,
        [
            {'id': 2, 'customer': 'Grace', 'amount': 99.5},
            {'id': 4, 'customer': 'Margaret', 'amount': 42.0},
        ])

  def test_delta_cdf_to_iceberg_cdc_and_live_lakehouse_tail_follow(self):
    import threading
    import time as time_mod
    import pyarrow as pa
    import pyarrow.parquet as pq
    from apache_beam.cli import lakehouse

    # 1. Delta Change Data Feed (`cdc` action with `insert`, `update_preimage`,
    #    `update_postimage`, `delete`) -> Iceberg v2 Merge-on-Read!
    cdf_root = os.path.join(self.tmp, 'delta_cdf_tbl')
    cdf_log = os.path.join(cdf_root, '_delta_log')
    os.makedirs(cdf_log, exist_ok=True)
    cdf_file = os.path.join(cdf_root, 'cdf-0.parquet')
    pq.write_table(
        pa.table({
            'id': pa.array([10, 20, 20, 30, 30], type=pa.int64()),
            'name': pa.array(['A', 'B_old', 'B_new', 'C', 'C'], type=pa.string()),
            '_change_type': pa.array(
                ['insert', 'update_preimage', 'update_postimage', 'insert', 'delete'],
                type=pa.string()),
            '_commit_version': pa.array([1, 2, 2, 1, 3], type=pa.int64()),
        }),
        cdf_file)
    schema_str = json.dumps({
        'type': 'struct',
        'fields': [
            {'name': 'id', 'type': 'long', 'nullable': False, 'metadata': {}},
            {'name': 'name', 'type': 'string', 'nullable': True, 'metadata': {}},
        ],
    })
    with open(os.path.join(cdf_log, '00000000000000000000.json'), 'w', encoding='utf-8') as f:
      f.write(json.dumps({
          'metaData': {
              'id': 'cdf-1',
              'schemaString': schema_str,
              'partitionColumns': [],
              'configuration': {'delta.enableChangeDataFeed': 'true'},
          }
      }) + '\n')
      f.write(json.dumps({
          'cdc': {
              'path': 'cdf-0.parquet',
              'size': os.path.getsize(cdf_file),
              'dataChange': False,
          }
      }) + '\n')

    wh_dir = os.path.join(self.tmp, 'wh_from_cdf')
    ice_uri = f'iceberg://lake.from_cdf?warehouse={wh_dir}'
    rc, env, err = _run_json([
        'pipe',
        '--from',
        f'delta://{cdf_root}',
        '--to',
        ice_uri,
        '--cdc',
        '--primary-key',
        'id',
        '--assert-counts',
        '--runner=FnApiRunner',
    ])
    self.assertEqual(rc, cli.EXIT_OK, f'err={err} env={env}')
    # 5 raw CDF rows -> 1 `update_preimage` filtered out -> 4 CDC ops written!
    self.assertEqual(env['metrics']['rows_read'], 5)
    self.assertEqual(env['metrics']['rows_filtered_out'], 1)
    self.assertEqual(env['metrics']['rows_written'], 4)

    rc_i, env_i, _ = _run_json(['inspect', ice_uri])
    self.assertEqual(rc_i, cli.EXIT_OK)
    rows_live = sorted(env_i['untrusted_data']['rows'], key=lambda r: r['id'])
    self.assertEqual(rows_live, [{'id': 10, 'name': 'A'}, {'id': 20, 'name': 'B_new'}])

    # 2. Live `beam tail -f` following new Iceberg v2 snapshot commits!
    tdir = os.path.join(wh_dir, 'lake', 'from_cdf')

    def _commit_later():
      time_mod.sleep(0.15)
      new_pq = os.path.join(tdir, 'data', 'live-append.parquet')
      df_entry = lakehouse.write_parquet_with_field_ids(
          new_pq,
          [{'id': 50, 'name': 'LiveCommit'}],
          [
              {'id': 1, 'name': 'id', 'type': 'long', 'nullable': False},
              {'id': 2, 'name': 'name', 'type': 'string', 'nullable': True},
          ])
      lakehouse.commit_iceberg_snapshot(
          tdir,
          iceberg_fields=[
              {'id': 1, 'name': 'id', 'type': 'long', 'nullable': False},
              {'id': 2, 'name': 'name', 'type': 'string', 'nullable': True},
          ],
          identifier_columns=['id'],
          new_data_files=[df_entry])

    t = threading.Thread(target=_commit_later, daemon=True)
    t.start()
    out_buf = io.StringIO()
    err_buf = io.StringIO()
    with contextlib.redirect_stdout(out_buf), contextlib.redirect_stderr(err_buf):
      rc_tail = cli.main([
          'tail', '-f', '-n', '1', '--poll-interval', '0.05', '--duration', '5s',
          ice_uri, '--json'
      ])
    t.join(timeout=2.0)
    self.assertEqual(rc_tail, cli.EXIT_OK)
    lines = [json.loads(ln) for ln in out_buf.getvalue().strip().splitlines() if ln.strip()]
    self.assertEqual(len(lines), 2)
    self.assertEqual(lines[0]['event'], 'record')
    self.assertEqual(lines[0]['record']['id'], 50)
    self.assertEqual(lines[0]['record']['name'], 'LiveCommit')
    self.assertEqual(lines[1]['outputs']['mode'], 'lakehouse_commit_follow')

  def test_bigquery_cdc_sequence_normalization_and_storage_write_sink(self):
    import re
    from types import SimpleNamespace
    from apache_beam.cli import cdc as cdc_mod

    bq_seq_re = re.compile(r'^[0-9A-Fa-f]{1,16}(?:/[0-9A-Fa-f]{1,16}){0,3}$')
    _, hex_1 = cdc_mod.to_bq_hex_sequence(1)
    _, hex_2 = cdc_mod.to_bq_hex_sequence(2)
    _, hex_100 = cdc_mod.to_bq_hex_sequence(100)
    self.assertRegex(hex_1, bq_seq_re)
    self.assertLess(hex_1, hex_2)
    self.assertLess(hex_2, hex_100)

    # Postgres WAL LSNs (`0/16B3748` < `0/16B3750` < `1/0000001`).
    _, lsn_a = cdc_mod.to_bq_hex_sequence('0/16B3748')
    _, lsn_b = cdc_mod.to_bq_hex_sequence('0/16B3750')
    _, lsn_c = cdc_mod.to_bq_hex_sequence('1/0000001')
    for lsn in (lsn_a, lsn_b, lsn_c):
      self.assertRegex(lsn, bq_seq_re)
    self.assertLess(lsn_a, lsn_b)
    self.assertLess(lsn_b, lsn_c)

    # ISO-8601 timestamps preserve lexicographical order.
    _, ts_a = cdc_mod.to_bq_hex_sequence('2026-11-08T10:00:00Z')
    _, ts_b = cdc_mod.to_bq_hex_sequence('2026-11-08T10:00:01Z')
    self.assertRegex(ts_a, bq_seq_re)
    self.assertLess(ts_a, ts_b)

    # Mock BigQuery DDL client + BigQueryWriteClient (`append_rows`) + row_errors DLQ!
    cdc_file = os.path.join(self.tmp, 'bq_cdc_in.jsonl')
    with open(cdc_file, 'w', encoding='utf-8') as f:
      f.write(json.dumps({
          'op': 'c',
          'source': {'lsn': '0/16B3748'},
          'after': {'id': 1, 'email': 'ada@example.com'},
      }) + '\n')
      f.write(json.dumps({
          'op': 'd',
          'source': {'lsn': '0/16B3750'},
          'before': {'id': 2, 'email': 'old@example.com'},
          'after': None,
      }) + '\n')
      f.write(json.dumps({
          'op': 'u',
          'source': {'lsn': '0/16B3760'},
          'after': {'id': 999, 'email': 'reject_me@example.com'},
      }) + '\n')

    from google.api_core import exceptions as gcp_exceptions
    fake_bq_client = mock.MagicMock()
    fake_bq_client.project = 'demo-proj'
    fake_bq_client.get_table.side_effect = gcp_exceptions.NotFound('no table')

    captured_requests: list[Any] = []

    def _fake_append_rows(req_iter):
      reqs = list(req_iter)
      captured_requests.extend(reqs)
      # Simulate BigQuery rejecting `id == 999` (`reject_me@example.com`) via `row_errors`!
      r_errs = []
      for idx, ser_row in enumerate(reqs[0].proto_rows.rows.serialized_rows):
        if b'reject_me@example.com' in ser_row:
          r_errs.append(
              SimpleNamespace(
                  index=idx, message='simulated row constraint error'))
      return iter([
          SimpleNamespace(
              row_errors=r_errs, error=SimpleNamespace(code=0, message=''))
      ])

    fake_write_client = mock.MagicMock()
    fake_write_client.append_rows.side_effect = _fake_append_rows
    bq_dlq = os.path.join(self.tmp, 'bq_cdc_dlq.json')

    with (
        mock.patch('google.cloud.bigquery.Client', return_value=fake_bq_client),
        mock.patch(
            'google.cloud.bigquery_storage_v1.BigQueryWriteClient',
            return_value=fake_write_client),
    ):
      rc, env, err = _run_json([
          'pipe',
          '--from',
          cdc_file,
          '--to',
          'bigquery://demo-proj.analytics.users_cdc',
          '--cdc',
          '--primary-key',
          'id',
          '--dlq',
          bq_dlq,
          '--assert-counts',
          '--runner=FnApiRunner',
      ])
    self.assertEqual(rc, cli.EXIT_OK, f'err={err} env={env}')
    ddl_sql = fake_bq_client.query.call_args.args[0]
    self.assertIn('PRIMARY KEY (`id`) NOT ENFORCED', ddl_sql)
    self.assertGreaterEqual(len(captured_requests), 1)
    self.assertEqual(
        captured_requests[0].write_stream,
        'projects/demo-proj/datasets/analytics/tables/users_cdc/streams/_default')
    self.assertEqual(env['metrics']['rows_read'], 3)
    self.assertEqual(env['metrics']['dlq_rows'], 1)
    self.assertEqual(env['metrics']['rows_written'], 2)

  def test_cdc_and_zero_copy_emit_yaml_package_and_worker_wheel(self):
    # 1. Postgres `--cdc` -> BigQuery `--cdc` portable YAML export (`--emit-yaml`),
    # including when a local JDK is present (`_find_java` succeeds).
    with mock.patch('apache_beam.cli.sql._find_java', return_value='/usr/bin/java'):
      rc_pg, env_pg, err_pg = _run_json([
          'pipe',
          '--from',
          'postgres://db.internal:5432/shop?table=public.orders',
          '--to',
          'bigquery://demo-proj.analytics.orders',
          '--cdc',
          '--primary-key',
          'id',
          '--emit-yaml',
      ])
    self.assertEqual(rc_pg, cli.EXIT_OK, err_pg)
    pg_yaml = env_pg['outputs']['yaml']
    self.assertIn('ReadFromDebezium', pg_yaml)
    self.assertIn('WriteToBigQueryCDC', pg_yaml)

    # Package exported cross-engine CDC YAML as a zero-Docker Dataflow Flex Template!
    cdc_tpl = os.path.join(self.tmp, 'cdc_pg_to_bq.template.json')
    with mock.patch('sys.stdin', io.StringIO(pg_yaml)):
      rc_pkg, env_pkg, err_pkg = _run_json([
          'package',
          '-',
          '--to',
          cdc_tpl,
          '--image',
          'gcr.io/dataflow-templates/yaml-template:test',
      ])
    self.assertEqual(rc_pkg, cli.EXIT_OK, err_pkg)
    self.assertFalse(env_pkg['outputs']['docker_build_required'])

    # 2. Spanner Change Stream -> Iceberg Merge-on-Read `--emit-yaml`.
    rc_sp, env_sp, err_sp = _run_json([
        'pipe',
        '--from',
        'spanner://demo-proj/prod-inst/shop-db?change_stream=orders_cs',
        '--to',
        'iceberg://lake.orders?warehouse=gs://demo-bkt/wh',
        '--cdc',
        '--primary-key',
        'id',
        '--emit-yaml',
    ])
    self.assertEqual(rc_sp, cli.EXIT_OK, err_sp)
    sp_yaml = env_sp['outputs']['yaml']
    self.assertIn('ReadFromSpannerCDC', sp_yaml)
    self.assertIn('WriteToIceberg', sp_yaml)
    self.assertIn('merge-on-read', sp_yaml)

    # 3. Parquet -> Iceberg `--zero-copy --emit-yaml` emits `IcebergAddFiles`.
    rc_zc, env_zc, err_zc = _run_json([
        'pipe',
        '--from',
        'gs://demo-bkt/raw/*.parquet',
        '--to',
        'iceberg://lake.orders_zc?warehouse=gs://demo-bkt/wh',
        '--zero-copy',
        '--emit-yaml',
    ])
    self.assertEqual(rc_zc, cli.EXIT_OK, err_zc)
    self.assertIn('IcebergAddFiles', env_zc['outputs']['yaml'])

    # 4. Verify `beam io list --json` and preview worker wheel bundling (`lakehouse.py`, `cdc.py`).
    rc_io, env_io, _ = _run_json(['io', 'list'])
    self.assertEqual(rc_io, cli.EXIT_OK)
    conn_names = {c['name'] for c in env_io['outputs']['connectors']}
    self.assertTrue({'delta', 'iceberg', 'mysql', 'spanner', 'bigquery'}.issubset(conn_names))

    import zipfile
    preview_launcher = sys.modules.get('apache_beam_cli_preview.launcher')
    if preview_launcher is not None:
      whl_path = preview_launcher.ensure_preview_wheel()
      with zipfile.ZipFile(whl_path, 'r') as zf:
        names = set(zf.namelist())
      self.assertTrue(any(n.endswith('_bundled_cli/lakehouse.py') for n in names))
      self.assertTrue(any(n.endswith('_bundled_cli/cdc.py') for n in names))

  def test_doctor_without_google_namespace_package(self):
    import importlib.util
    orig_find_spec = importlib.util.find_spec

    def _no_google_find_spec(name, *args, **kwargs):
      if name == 'google' or name.startswith('google.'):
        raise ModuleNotFoundError("No module named 'google'")
      return orig_find_spec(name, *args, **kwargs)

    with mock.patch('importlib.util.find_spec', side_effect=_no_google_find_spec):
      rc, env, err = _run_json(['doctor'])
    self.assertEqual(rc, cli.EXIT_OK, err)
    self.assertTrue(env['outputs']['healthy'])
    self.assertFalse(env['outputs']['extras']['gcp_bigquery'])
    self.assertFalse(env['outputs']['gcp_adc']['ok'])


if __name__ == '__main__':
  unittest.main()


