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

"""Concurrency and cross-job isolation tests for `apache_beam.cli`.

Verifies that simultaneous executions of the same or different CLI operations
across threads and OS subprocesses never clobber sibling outputs, staging
directories, `--if-exists=fail` targets, Apache Iceberg table metadata,
`~/.beam/config.yaml`, or `~/.beam/jobs/` state records.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from pathlib import Path
from unittest import mock

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

import apache_beam as beam
from apache_beam.cli import config
from apache_beam.cli import diff
from apache_beam.cli import fast_local
from apache_beam.cli import inspect_cmd
from apache_beam.cli import jobs
from apache_beam.cli import lakehouse
from apache_beam.cli import main as cli_main
from apache_beam.cli import pipe_spec
from apache_beam.cli import uris


def _cli_subprocess_env(
    beam_home: str,
    *,
    extra: dict[str, str] | None = None,
) -> dict[str, str]:
  env = dict(os.environ)
  env['BEAM_CLI_DIR'] = str(Path(__file__).resolve().parent)
  env['BEAM_HOME'] = beam_home
  env['BEAM_CONFIG'] = os.path.join(beam_home, 'config.yaml')
  env.pop('BEAM_NO_CONFIG', None)
  env.pop('BEAM_PROFILE', None)
  if extra:
    env.update(extra)
  return env


def _run_cli_subproc(
    argv: list[str],
    *,
    beam_home: str,
    cwd: str,
    extra_env: dict[str, str] | None = None,
) -> tuple[int, str, str]:
  proc = subprocess.run(
      [sys.executable, '-m', 'apache_beam.cli.main', *argv],
      cwd=cwd,
      env=_cli_subprocess_env(beam_home, extra=extra_env),
      capture_output=True,
      text=True,
      timeout=120,
      check=False)
  return proc.returncode, proc.stdout, proc.stderr


class SiblingPrefixIsolationTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam_cli_conc_sib_')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.beam_home = os.path.join(self.tmp, 'beam_home')
    os.makedirs(self.beam_home, exist_ok=True)

  def test_is_owned_by_prefix_distinguishes_shards_from_siblings(self):
    self.assertTrue(
        cli_main._is_owned_by_prefix('/dir/orders', '/dir/orders'))
    self.assertTrue(
        cli_main._is_owned_by_prefix(
            '/dir/orders-00000-of-00003', '/dir/orders'))
    self.assertTrue(
        cli_main._is_owned_by_prefix(
            '/dir/orders-00001-of-00002.parquet', '/dir/orders'))
    self.assertTrue(
        cli_main._is_owned_by_prefix(
            '/dir/orders.parquet', '/dir/orders.parquet'))
    self.assertTrue(
        cli_main._is_owned_by_prefix(
            '/dir/orders.parquet-00000-of-00001', '/dir/orders.parquet'))

    # Overlapping sibling names must NOT be claimed by `/dir/orders`!
    self.assertFalse(
        cli_main._is_owned_by_prefix('/dir/orders_daily', '/dir/orders'))
    self.assertFalse(
        cli_main._is_owned_by_prefix(
            '/dir/orders_daily-00000-of-00001', '/dir/orders'))
    self.assertFalse(
        cli_main._is_owned_by_prefix('/dir/orders.parquet', '/dir/orders'))
    self.assertFalse(
        cli_main._is_owned_by_prefix(
            '/dir/orders.parquet.bak', '/dir/orders.parquet'))
    self.assertFalse(
        cli_main._is_owned_by_prefix(
            '/dir/orders_v2.parquet', '/dir/orders.parquet'))

    # Directory and wildcard prefixes intentionally match children.
    self.assertTrue(
        cli_main._is_owned_by_prefix('/dir/out/part-0.parquet', '/dir/out/'))
    self.assertTrue(
        cli_main._is_owned_by_prefix('/dir/orders_2026.csv', '/dir/orders*'))

  def test_concurrent_sibling_jobs_in_shared_directory_zero_clobber(self):
    shared_out = os.path.join(self.tmp, 'shared_out')
    os.makedirs(shared_out, exist_ok=True)

    # Simulate an active sibling Beam job's `.temp` directory with an in-flight
    # shard (must NOT be removed), an empty `.temp` directory in the read-only
    # input folder (must NOT be touched by `beam pipe`), and an expired empty
    # `.temp` directory in `shared_out` (must be pruned).
    src_dir = os.path.join(self.tmp, 'readonly_inputs')
    os.makedirs(src_dir, exist_ok=True)
    input_temp = os.path.join(src_dir, '.temp-other-job-in-input-dir')
    os.makedirs(input_temp, exist_ok=True)

    active_temp = os.path.join(shared_out, '.temp-active-sibling-job')
    stale_temp = os.path.join(shared_out, '.temp-stale-expired-job')
    os.makedirs(active_temp, exist_ok=True)
    with open(os.path.join(active_temp, 'part-00000.tmp'), 'w', encoding='utf-8') as tf:
      tf.write('in-flight-bytes')
    os.makedirs(stale_temp, exist_ok=True)

    specs = [
        ('orders', 'json', False, {'BEAM_CLI_NO_FAST_PATH': '1'}),
        ('orders_daily', 'json', False, {'BEAM_CLI_NO_FAST_PATH': '1'}),
        ('orders.parquet', 'parquet', False, {}),
        ('orders_v2.parquet', 'parquet', True, {}),
        ('orders.jsonl', 'json', False, {}),
        ('orders.avro', 'avro', True, {}),
    ]

    jobs_to_run = []
    for job_idx, (out_name, fmt, use_sql, extra_env) in enumerate(specs):
      in_csv = os.path.join(src_dir, f'src_{job_idx}.csv')
      with open(in_csv, 'w', encoding='utf-8') as f:
        f.write('id,job_tag,amount\n')
        for k in range(8):
          f.write(f'{job_idx * 100 + k},job_{job_idx},{10 + k}\n')
      dest = os.path.join(shared_out, out_name)
      if use_sql:
        argv = [
            '--json',
            '--no-config',
            'pipe',
            '--from',
            f's={in_csv}',
            '--to',
            dest,
            '--sql',
            'SELECT CAST(id AS BIGINT) AS id, job_tag, '
            'CAST(amount AS BIGINT) AS amount FROM s ORDER BY id',
        ]
      else:
        argv = [
            '--json',
            '--no-config',
            'pipe',
            '--from',
            in_csv,
            '--to',
            dest,
        ]
        if fmt == 'json' and not out_name.endswith('.jsonl'):
          argv.extend(['--to-format', 'json'])
      jobs_to_run.append((job_idx, out_name, fmt, dest, argv, extra_env))

    results: dict[int, tuple[int, dict, str]] = {}
    with ThreadPoolExecutor(max_workers=len(jobs_to_run)) as pool:
      futs = {
          pool.submit(
              _run_cli_subproc,
              argv,
              beam_home=self.beam_home,
              cwd=self.tmp,
              extra_env=extra_env,
          ): (job_idx, out_name, fmt, dest)
          for job_idx, out_name, fmt, dest, argv, extra_env in jobs_to_run
      }
      for fut in as_completed(futs):
        job_idx, out_name, fmt, dest = futs[fut]
        rc, out, err = fut.result()
        self.assertEqual(
            rc,
            0,
            msg=f'job {job_idx} ({out_name}) failed rc={rc}: {err}\n{out}')
        env_json = json.loads(out)
        self.assertTrue(env_json['ok'])
        results[job_idx] = (rc, env_json, dest)

    # Active `.temp` dir (with in-flight shard) and read-only input `.temp` dir
    # are preserved; stale empty `.temp` dir in shared_out is pruned!
    self.assertTrue(os.path.isfile(os.path.join(active_temp, 'part-00000.tmp')))
    self.assertTrue(os.path.isdir(input_temp))
    self.assertFalse(os.path.exists(stale_temp))

    # Every job reported ONLY its own files, and every file holds pure rows!
    seen_files: set[str] = set()
    for job_idx, out_name, fmt, dest, _, _ in jobs_to_run:
      _, env_json, _ = results[job_idx]
      rep_files = [os.path.abspath(p) for p in env_json['outputs']['files']]
      self.assertGreaterEqual(len(rep_files), 1, msg=f'job {job_idx} files')
      for rf in rep_files:
        self.assertTrue(
            cli_main._is_owned_by_prefix(rf, os.path.abspath(dest)),
            msg=f'job {job_idx} stole sibling file {rf} for {dest}')
        self.assertNotIn(rf, seen_files, msg=f'file {rf} claimed twice')
        seen_files.add(rf)

      rows: list[dict] = []
      if fmt == 'parquet':
        for rf in rep_files:
          rows.extend(pq.read_table(rf).to_pylist())
      elif fmt == 'avro':
        for rf in rep_files:
          with open(rf, 'rb') as af:
            rows.extend(list(fastavro.reader(af)))
      else:
        for rf in rep_files:
          with open(rf, 'r', encoding='utf-8') as jf:
            rows.extend(json.loads(ln) for ln in jf if ln.strip())

      self.assertEqual(len(rows), 8, msg=f'row count mismatch for {out_name}')
      self.assertEqual(
          {r['job_tag'] for r in rows},
          {f'job_{job_idx}'},
          msg=f'cross-job data contamination in {out_name}: {rows}')
      self.assertEqual(
          sorted(int(r['id']) for r in rows),
          [job_idx * 100 + k for k in range(8)])


class DestinationRaceAndReplaceAtomicityTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam_cli_conc_race_')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.beam_home = os.path.join(self.tmp, 'beam_home')
    os.makedirs(self.beam_home, exist_ok=True)

  def test_same_destination_if_exists_fail_race_fast_local_threads(self):
    target_pq = os.path.join(self.tmp, 'race_out.parquet')
    target_dlq = os.path.join(self.tmp, 'race_dlq.jsonl')
    num_workers = 8
    barrier = threading.Barrier(num_workers)

    worker_inputs: list[str] = []
    for wid in range(num_workers):
      src_json = os.path.join(self.tmp, f'w_{wid}.jsonl')
      with open(src_json, 'w', encoding='utf-8') as f:
        for k in range(25):
          f.write(json.dumps({'worker_id': wid, 'seq': k, 'val': wid * 1000 + k}) + '\n')
        # Include 1 malformed JSON line routed to DLQ carrying worker_id marker!
        f.write(f'{{"broken_worker": {wid}, invalid_json\n')
      worker_inputs.append(src_json)

    def _contender(wid: int):
      pipe = pipe_spec.build_pipe_spec(
          worker_inputs[wid],
          target_pq,
          dlq_sink=target_dlq,
          if_exists='fail')
      barrier.wait(timeout=15)
      return wid, fast_local.run(
          pipe, [worker_inputs[wid]], if_exists='fail')

    winners: list[int] = []
    losers: list[int] = []
    with ThreadPoolExecutor(max_workers=num_workers) as pool:
      futs = {pool.submit(_contender, w): w for w in range(num_workers)}
      for fut in as_completed(futs):
        wid = futs[fut]
        try:
          won_id, _ = fut.result()
          winners.append(won_id)
        except FileExistsError:
          losers.append(wid)

    self.assertEqual(len(winners), 1, msg=f'expected 1 winner, got {winners}')
    self.assertEqual(len(losers), num_workers - 1)
    winner = winners[0]

    rows = pq.read_table(target_pq).to_pylist()
    self.assertEqual(len(rows), 25)
    self.assertEqual({r['worker_id'] for r in rows}, {winner})
    self.assertEqual(
        sorted(r['val'] for r in rows), [winner * 1000 + k for k in range(25)])

    # Verify losing contenders never overwrote the winning job's DLQ file!
    with open(target_dlq, 'r', encoding='utf-8') as df:
      dlq_rows = [json.loads(line) for line in df if line.strip()]
    self.assertEqual(len(dlq_rows), 1)
    dlq_payload = json.loads(dlq_rows[0]['raw_payload'])
    self.assertIn(f'"broken_worker": {winner}', dlq_payload['raw_line'])

  def test_same_destination_if_exists_fail_race_cli_subprocesses(self):
    target_pq = os.path.join(self.tmp, 'cli_subproc_race.parquet')
    num_workers = 5
    barrier = threading.Barrier(num_workers)

    src_files = []
    for wid in range(num_workers):
      p = os.path.join(self.tmp, f'sub_src_{wid}.csv')
      with open(p, 'w', encoding='utf-8') as f:
        f.write('worker_id,seq\n')
        for k in range(15):
          f.write(f'{wid},{k}\n')
      src_files.append(p)

    def _run_one(wid: int):
      if wid % 2 == 0:
        argv = [
            '--json',
            '--no-config',
            'pipe',
            '--from',
            src_files[wid],
            '--to',
            target_pq,
            '--if-exists',
            'fail',
        ]
      else:
        argv = [
            '--json',
            '--no-config',
            'pipe',
            '--from',
            f't={src_files[wid]}',
            '--to',
            target_pq,
            '--sql',
            'SELECT CAST(worker_id AS BIGINT) AS worker_id, '
            'CAST(seq AS BIGINT) AS seq FROM t',
            '--if-exists',
            'fail',
        ]
      barrier.wait(timeout=20)
      return wid, _run_cli_subproc(argv, beam_home=self.beam_home, cwd=self.tmp)

    ok_workers: list[int] = []
    refused_workers: list[int] = []
    with ThreadPoolExecutor(max_workers=num_workers) as pool:
      for wid, (rc, out, err) in pool.map(_run_one, range(num_workers)):
        env_json = json.loads(out)
        if rc == 0:
          self.assertTrue(env_json['ok'])
          ok_workers.append(wid)
        else:
          self.assertEqual(
              rc, cli_main.EXIT_USAGE, msg=f'unexpected rc={rc}: {out}\n{err}')
          self.assertIn(
              'already exists', env_json['error']['message'].lower())
          refused_workers.append(wid)

    self.assertEqual(len(ok_workers), 1, msg=f'winners={ok_workers}')
    self.assertEqual(len(refused_workers), num_workers - 1)
    win_id = ok_workers[0]
    final_rows = pq.read_table(target_pq).to_pylist()
    self.assertEqual(len(final_rows), 15)
    self.assertEqual({int(r['worker_id']) for r in final_rows}, {win_id})

  def test_concurrent_replace_writers_with_live_inspect_and_diff_readers(self):
    live_pq = os.path.join(self.tmp, 'live_target.parquet')
    valid_versions = {0: 40, 1: 60, 2: 80, 3: 100, 4: 120}

    src_by_ver: dict[int, str] = {}
    for ver, nrows in valid_versions.items():
      src_p = os.path.join(self.tmp, f'ver_{ver}.jsonl')
      with open(src_p, 'w', encoding='utf-8') as f:
        for k in range(nrows):
          f.write(json.dumps({'id': k, 'version': ver}) + '\n')
      src_by_ver[ver] = src_p

    # Seed v0 before launching concurrent writers and readers.
    seed_pipe = pipe_spec.build_pipe_spec(
        src_by_ver[0], live_pq, if_exists='replace')
    fast_local.run(seed_pipe, [src_by_ver[0]], if_exists='replace')

    stop_readers = threading.Event()
    observed_snapshots: list[tuple[int, set[int]]] = []
    reader_errors: list[BaseException] = []
    obs_lock = threading.Lock()

    def _writer(ver: int):
      for _ in range(5):
        p = pipe_spec.build_pipe_spec(
            src_by_ver[ver], live_pq, if_exists='replace')
        fast_local.run(p, [src_by_ver[ver]], if_exists='replace')

    def _reader(reader_idx: int):
      try:
        iters = 0
        while not stop_readers.is_set() or iters < 6:
          iters += 1
          info = inspect_cmd.run_inspect(live_pq, sample_count=5)
          rc_est = info['row_count']
          sample_rows = info['untrusted_data']['rows']
          vers = {int(r['version']) for r in sample_rows}
          with obs_lock:
            observed_snapshots.append((rc_est, vers))
          if reader_idx == 0 and iters % 3 == 0:
            # Self-diff against a frozen seed file also reads `live_pq` safely.
            diff_rc, _ = diff.run_diff(src_by_ver[0], live_pq, keys=['id'])
            self.assertIn(diff_rc, (0, cli_main.EXIT_DIFF_MISMATCH))
      except BaseException as exc:  # pylint: disable=broad-except
        with obs_lock:
          reader_errors.append(exc)

    with ThreadPoolExecutor(max_workers=7) as pool:
      r_futs = [pool.submit(_reader, r_i) for r_i in range(3)]
      w_futs = [pool.submit(_writer, v) for v in (1, 2, 3, 4)]
      for wf in w_futs:
        wf.result()
      stop_readers.set()
      for rf in r_futs:
        rf.result()

    self.assertEqual(reader_errors, [], msg=f'reader errors: {reader_errors}')
    self.assertGreaterEqual(len(observed_snapshots), 6)
    for row_cnt, ver_set in observed_snapshots:
      self.assertEqual(
          len(ver_set),
          1,
          msg=f'torn read mixed versions in single Parquet snapshot: {ver_set}')
      v = next(iter(ver_set))
      self.assertEqual(row_cnt, valid_versions[v])

    # Ensure zero leftover `.tmp` staging files in self.tmp.
    leftover_tmps = [
        fn for fn in os.listdir(self.tmp) if fn.endswith('.tmp')
    ]
    self.assertEqual(leftover_tmps, [])

  def test_concurrent_local_sql_append_conserves_all_rows(self):
    append_pq = os.path.join(self.tmp, 'metrics_append.parquet')
    append_jsonl = os.path.join(self.tmp, 'metrics_append.jsonl')
    num_workers = 5
    rows_per_worker = 12

    src_csvs: list[str] = []
    for wid in range(num_workers):
      p = os.path.join(self.tmp, f'app_src_{wid}.csv')
      with open(p, 'w', encoding='utf-8') as f:
        f.write('id,worker\n')
        for k in range(rows_per_worker):
          f.write(f'{wid * 100 + k},w_{wid}\n')
      src_csvs.append(p)

    for sink_path in (append_pq, append_jsonl):
      barrier = threading.Barrier(num_workers)

      def _append_worker(wid: int, target: str = sink_path):
        argv = [
            '--json',
            '--no-config',
            'pipe',
            '--from',
            f's={src_csvs[wid]}',
            '--to',
            target,
            '--sql',
            'SELECT CAST(id AS BIGINT) AS id, worker FROM s',
            '--if-exists',
            'append',
        ]
        barrier.wait(timeout=20)
        return _run_cli_subproc(argv, beam_home=self.beam_home, cwd=self.tmp)

      with ThreadPoolExecutor(max_workers=num_workers) as pool:
        for rc, out, err in pool.map(_append_worker, range(num_workers)):
          self.assertEqual(rc, 0, msg=f'append failed: {err}\n{out}')

    pq_rows = pq.read_table(append_pq).to_pylist()
    with open(append_jsonl, 'r', encoding='utf-8') as jf:
      jsonl_rows = [json.loads(ln) for ln in jf if ln.strip()]

    expected_ids = {
        wid * 100 + k
        for wid in range(num_workers)
        for k in range(rows_per_worker)
    }
    self.assertEqual(len(pq_rows), num_workers * rows_per_worker)
    self.assertEqual({int(r['id']) for r in pq_rows}, expected_ids)
    self.assertEqual(len(jsonl_rows), num_workers * rows_per_worker)
    self.assertEqual({int(r['id']) for r in jsonl_rows}, expected_ids)

  def test_concurrent_beam_runner_staged_replace_and_fail_isolation(self):
    fail_target = os.path.join(self.tmp, 'beam_runner_race.parquet')
    replace_target = os.path.join(self.tmp, 'beam_runner_replace.parquet')
    num_workers = 4

    srcs: list[str] = []
    for wid in range(num_workers):
      p = os.path.join(self.tmp, f'br_src_{wid}.csv')
      with open(p, 'w', encoding='utf-8') as f:
        f.write('worker_id,seq\n')
        for k in range(10 + wid * 5):
          f.write(f'{wid},{k}\n')
      srcs.append(p)

    # 1. Four full Beam-runner subprocesses race on `--if-exists=fail`.
    fail_barrier = threading.Barrier(num_workers)

    def _run_beam_fail(wid: int):
      argv = [
          '--json',
          '--no-config',
          'pipe',
          '--from',
          srcs[wid],
          '--to',
          fail_target,
          '--if-exists',
          'fail',
      ]
      fail_barrier.wait(timeout=20)
      return wid, _run_cli_subproc(
          argv,
          beam_home=self.beam_home,
          cwd=self.tmp,
          extra_env={'BEAM_CLI_NO_FAST_PATH': '1'})

    winners: list[int] = []
    losers: list[int] = []
    with ThreadPoolExecutor(max_workers=num_workers) as pool:
      for wid, (rc, out, err) in pool.map(_run_beam_fail, range(num_workers)):
        if rc == 0:
          winners.append(wid)
        else:
          self.assertEqual(rc, cli_main.EXIT_USAGE, msg=f'{out}\n{err}')
          losers.append(wid)

    self.assertEqual(len(winners), 1)
    self.assertEqual(len(losers), num_workers - 1)
    win = winners[0]
    fail_rows = pq.read_table(fail_target).to_pylist()
    self.assertEqual(len(fail_rows), 10 + win * 5)
    self.assertEqual({int(r['worker_id']) for r in fail_rows}, {win})

    # 2. Four full Beam-runner subprocesses race on `--if-exists=replace`.
    rep_barrier = threading.Barrier(num_workers)

    def _run_beam_replace(wid: int):
      argv = [
          '--json',
          '--no-config',
          'pipe',
          '--from',
          srcs[wid],
          '--to',
          replace_target,
          '--if-exists',
          'replace',
      ]
      rep_barrier.wait(timeout=20)
      return wid, _run_cli_subproc(
          argv,
          beam_home=self.beam_home,
          cwd=self.tmp,
          extra_env={'BEAM_CLI_NO_FAST_PATH': '1'})

    with ThreadPoolExecutor(max_workers=num_workers) as pool:
      for wid, (rc, out, err) in pool.map(
          _run_beam_replace, range(num_workers)):
        self.assertEqual(rc, 0, msg=f'replace {wid} failed: {out}\n{err}')

    rep_rows = pq.read_table(replace_target).to_pylist()
    rep_wids = {int(r['worker_id']) for r in rep_rows}
    self.assertEqual(len(rep_wids), 1)
    final_w = next(iter(rep_wids))
    self.assertEqual(len(rep_rows), 10 + final_w * 5)
    leftover_dirs = [
        fn for fn in os.listdir(self.tmp) if fn.startswith('.beam_replace_')
    ]
    self.assertEqual(leftover_dirs, [])


class ConcurrentIcebergIsolationTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam_cli_conc_ice_')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.warehouse = os.path.join(self.tmp, 'warehouse')
    os.makedirs(self.warehouse, exist_ok=True)

  def test_concurrent_iceberg_appends_and_schema_evolution_preserve_all_snapshots(
      self):
    events_uri = f'iceberg://analytics.events?warehouse={self.warehouse}'
    users_uri = f'iceberg://analytics.users?warehouse={self.warehouse}'

    # Seed `analytics.events` (`v1`, 10 rows) and sibling `analytics.users` (`v1`, 5 rows).
    seed_events_pq = os.path.join(self.tmp, 'seed_events.parquet')
    pq.write_table(
        pa.Table.from_pylist([
            {'id': i, 'writer': 'seed', 'val': i * 10} for i in range(10)
        ]),
        seed_events_pq)
    lakehouse.register_zero_copy_to_iceberg(
        uris.parse_endpoint(seed_events_pq, is_read=True),
        uris.parse_endpoint(events_uri, is_read=False),
        if_exists='fail')

    seed_users_pq = os.path.join(self.tmp, 'seed_users.parquet')
    pq.write_table(
        pa.Table.from_pylist([
            {'uid': f'u{i}', 'handle': f'user_{i}'} for i in range(5)
        ]),
        seed_users_pq)
    lakehouse.register_zero_copy_to_iceberg(
        uris.parse_endpoint(seed_users_pq, is_read=True),
        uris.parse_endpoint(users_uri, is_read=False),
        if_exists='fail')

    num_writers = 6
    rows_per_writer = 15
    writer_pqs: list[str] = []
    for w in range(num_writers):
      p = os.path.join(self.tmp, f'batch_{w}.parquet')
      batch_rows = []
      for k in range(rows_per_writer):
        rec: dict[str, object] = {
            'id': (w + 1) * 1000 + k,
            'writer': f'w_{w}',
            'val': w * 100 + k,
        }
        # Writers 3, 4, 5 concurrently evolve distinct new columns!
        if w >= 3:
          rec[f'extra_w{w}'] = f'tag_{w}_{k}'
        batch_rows.append(rec)
      pq.write_table(pa.Table.from_pylist(batch_rows), p)
      writer_pqs.append(p)

    barrier = threading.Barrier(num_writers + 2)

    def _append_events(w: int):
      src_ep = uris.parse_endpoint(writer_pqs[w], is_read=True)
      dst_ep = uris.parse_endpoint(events_uri, is_read=False)
      barrier.wait(timeout=15)
      return lakehouse.register_zero_copy_to_iceberg(
          src_ep, dst_ep, if_exists='append')

    def _sync_sibling_users():
      barrier.wait(timeout=15)
      for _ in range(4):
        res = lakehouse.sync_lakehouse_catalog(
            os.path.join(self.warehouse, 'analytics', 'users'),
            users_uri,
            mode='refresh')
        self.assertEqual(res['bytes_copied'], 0)

    def _concurrent_inspector():
      insp_ep = uris.parse_endpoint(events_uri, is_read=True)
      barrier.wait(timeout=15)
      seen_counts = []
      for _ in range(8):
        info = lakehouse.inspect_iceberg_table(insp_ep, sample_count=5)
        seen_counts.append(info['row_count'])
      return seen_counts

    with ThreadPoolExecutor(max_workers=num_writers + 2) as pool:
      w_futs = [pool.submit(_append_events, w) for w in range(num_writers)]
      sib_fut = pool.submit(_sync_sibling_users)
      insp_fut = pool.submit(_concurrent_inspector)

      commit_versions = [f.result()['metadata_version'] for f in w_futs]
      sib_fut.result()
      seen_counts = insp_fut.result()

    # Versions 2..7 must each be assigned to exactly one writer (no collisions!).
    self.assertEqual(sorted(commit_versions), [2, 3, 4, 5, 6, 7])
    for cnt in seen_counts:
      self.assertGreaterEqual(cnt, 10)
      self.assertLessEqual(cnt, 10 + num_writers * rows_per_writer)

    events_dir = os.path.join(self.warehouse, 'analytics', 'events')
    st = lakehouse.load_iceberg_state(events_dir)
    self.assertEqual(st['metadata_version'], 7)
    self.assertEqual(len(st['metadata']['snapshots']), 7)
    self.assertEqual(len(st['metadata']['snapshot-log']), 7)

    # All 3 concurrently evolved columns must be preserved with unique field IDs!
    schema_fields = st['current_schema']['fields']
    col_names = {f['name'] for f in schema_fields}
    self.assertTrue(
        {'id', 'writer', 'val', 'extra_w3', 'extra_w4', 'extra_w5'}.issubset(
            col_names),
        msg=f'missing evolved columns: {col_names}')
    field_ids = [int(f['id']) for f in schema_fields]
    self.assertEqual(
        len(field_ids),
        len(set(field_ids)),
        msg=f'duplicate Iceberg field IDs under concurrency: {schema_fields}')

    # Every single row from seed + all 6 writers must be readable!
    _, all_rows = lakehouse.read_iceberg_rows_local(st)
    expected_total = 10 + num_writers * rows_per_writer
    self.assertEqual(len(all_rows), expected_total)
    expected_ids = set(range(10)) | {
        (w + 1) * 1000 + k
        for w in range(num_writers)
        for k in range(rows_per_writer)
    }
    self.assertEqual({int(r['id']) for r in all_rows}, expected_ids)

    # Sibling table `analytics.users` must have zero metadata or row bleed!
    users_dir = os.path.join(self.warehouse, 'analytics', 'users')
    users_st = lakehouse.load_iceberg_state(users_dir)
    self.assertEqual(
        [f['name'] for f in users_st['current_schema']['fields']],
        ['uid', 'handle'])
    _, user_rows = lakehouse.read_iceberg_rows_local(users_st)
    self.assertEqual(len(user_rows), 5)

  def test_concurrent_iceberg_if_exists_fail_race_allows_single_creator(self):
    new_uri = f'iceberg://analytics.brand_new?warehouse={self.warehouse}'
    num_contenders = 6
    barrier = threading.Barrier(num_contenders)

    pqs = []
    for i in range(num_contenders):
      p = os.path.join(self.tmp, f'new_cand_{i}.parquet')
      pq.write_table(
          pa.Table.from_pylist([
              {'creator': i, 'seq': k} for k in range(6)
          ]),
          p)
      pqs.append(p)

    def _try_create(idx: int):
      src_ep = uris.parse_endpoint(pqs[idx], is_read=True)
      dst_ep = uris.parse_endpoint(new_uri, is_read=False)
      barrier.wait(timeout=15)
      return idx, lakehouse.register_zero_copy_to_iceberg(
          src_ep, dst_ep, if_exists='fail')

    winners: list[int] = []
    rejected: list[int] = []
    with ThreadPoolExecutor(max_workers=num_contenders) as pool:
      futs = {pool.submit(_try_create, i): i for i in range(num_contenders)}
      for fut in as_completed(futs):
        idx = futs[fut]
        try:
          won, _ = fut.result()
          winners.append(won)
        except lakehouse.LakehouseError as err:
          self.assertIn('already exists', str(err).lower())
          rejected.append(idx)

    self.assertEqual(len(winners), 1, msg=f'winners={winners}')
    self.assertEqual(len(rejected), num_contenders - 1)

    tdir = os.path.join(self.warehouse, 'analytics', 'brand_new')
    st = lakehouse.load_iceberg_state(tdir)
    self.assertEqual(st['metadata_version'], 1)
    self.assertFalse(
        os.path.exists(os.path.join(tdir, 'metadata', 'v2.metadata.json')))
    _, rows = lakehouse.read_iceberg_rows_local(st)
    self.assertEqual(len(rows), 6)
    self.assertEqual({int(r['creator']) for r in rows}, {winners[0]})

  def test_concurrent_iceberg_cdc_merge_on_read_preserves_upserts_and_deletes(
      self):
    orders_dir = os.path.join(self.warehouse, 'prod', 'orders')

    # Seed 20 keys (id 0..19) at sequence 1 via Iceberg merge-on-read writer.
    seed_events = [
        beam.Row(id=k, amount=100 + k, beam_cdc_op='INSERT', beam_cdc_seq=1)
        for k in range(20)
    ]
    with beam.Pipeline(runner='FnApiRunner') as p:
      _ = (
          p
          | beam.Create(seed_events)
          | lakehouse.cli_write_to_iceberg(
              table=orders_dir,
              mode='merge-on-read',
              equality_columns=['id'],
              change_type_column='beam_cdc_op',
              sequence_number_column='beam_cdc_seq',
              if_exists='replace'))

    # 4 concurrent CDC micro-batches updating 4 keys and deleting 1 key each
    # on disjoint key partitions (0..4, 5..9, 10..14, 15..19).
    barrier = threading.Barrier(4)

    def _cdc_partition(part_idx: int):
      base = part_idx * 5
      events = []
      for offset in range(4):
        k = base + offset
        events.append(
            beam.Row(
                id=k,
                amount=1000 + k,
                beam_cdc_op='UPSERT',
                beam_cdc_seq=2))
      events.append(
          beam.Row(
              id=base + 4,
              amount=0,
              beam_cdc_op='DELETE',
              beam_cdc_seq=2))
      barrier.wait(timeout=20)
      with beam.Pipeline(runner='FnApiRunner') as p:
        _ = (
            p
            | beam.Create(events)
            | lakehouse.cli_write_to_iceberg(
                table=orders_dir,
                mode='merge-on-read',
                equality_columns=['id'],
                change_type_column='beam_cdc_op',
                sequence_number_column='beam_cdc_seq',
                if_exists='append'))

    with ThreadPoolExecutor(max_workers=4) as pool:
      futs = [pool.submit(_cdc_partition, idx) for idx in range(4)]
      for f in futs:
        f.result()

    st = lakehouse.load_iceberg_state(orders_dir)
    self.assertEqual(st['metadata_version'], 5)
    _, live_rows = lakehouse.read_iceberg_rows_local(st)
    by_id = {int(r['id']): int(r['amount']) for r in live_rows}
    expected_live = {
        base + off: 1000 + base + off
        for base in (0, 5, 10, 15)
        for off in range(4)
    }
    self.assertEqual(by_id, expected_live)


class SharedBeamHomeMetadataConcurrencyTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam_cli_conc_meta_')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.beam_home = os.path.join(self.tmp, 'beam_home')
    self.cfg_path = os.path.join(self.beam_home, 'config.yaml')
    os.makedirs(self.beam_home, exist_ok=True)

  def test_concurrent_jobs_ledger_and_validation_updates_zero_corruption(self):
    num_threads = 12
    barrier = threading.Barrier(num_threads)

    with mock.patch.dict(os.environ, {'BEAM_HOME': self.beam_home}):
      # Seed one shared "hot" job record updated concurrently alongside 12 distinct jobs.
      jobs.record_job_submission(
          'shared-hot-job',
          runner='DataflowRunner',
          state='RUNNING',
          command='pipe',
          project='proj-hot',
          region='us-central1',
          idem_key='idem-hot',
          post_checks={'assert_counts': True})

      def _job_worker(idx: int):
        jid = f'job-conc-{idx:02d}'
        ikey = f'idem-conc-{idx:02d}'
        barrier.wait(timeout=15)
        jobs.record_job_submission(
            jid,
            runner='DataflowRunner',
            state='RUNNING',
            command='pipe',
            project=f'proj-{idx}',
            region='us-central1',
            idem_key=ikey,
            post_checks={'assert_counts': True},
            outputs={'destination': f'bigquery://proj-{idx}.ds.tbl_{idx}'})
        jobs.mark_job_validation(
            jid,
            passed=True,
            exit_code=0,
            state='JOB_STATE_DONE',
            metrics={'rows_read': 100 + idx, 'rows_written': 100 + idx})
        # Contend on the shared hot job record simultaneously!
        jobs.mark_job_validation(
            'shared-hot-job',
            passed=True,
            exit_code=0,
            state='JOB_STATE_DONE',
            metrics={'worker_touch': idx})
        rec = jobs.lookup_recent_submission(ikey)
        self.assertIsNotNone(rec)
        return jid, ikey

      with ThreadPoolExecutor(max_workers=num_threads) as pool:
        pairs = list(pool.map(_job_worker, range(num_threads)))

      ledger_file = Path(self.beam_home) / 'jobs' / '_idempotency_ledger.json'
      ledger = json.loads(ledger_file.read_text(encoding='utf-8'))
      self.assertIn('idem-hot', ledger)
      for jid, ikey in pairs:
        self.assertEqual(ledger[ikey]['job_id'], jid)
        loaded = jobs.load_job_record(jid)
        self.assertEqual(loaded['job_id'], jid)
        self.assertEqual(loaded['validation_state'], 'PASSED')
        self.assertEqual(
            loaded['outputs']['destination'],
            f"bigquery://{loaded['project']}.ds.tbl_{int(jid.rsplit('-', 1)[1])}")

      hot_loaded = jobs.load_job_record('shared-hot-job')
      self.assertEqual(hot_loaded['job_id'], 'shared-hot-job')
      self.assertEqual(hot_loaded['project'], 'proj-hot')
      self.assertEqual(hot_loaded['validation_state'], 'PASSED')

  def test_concurrent_config_profile_mutations_preserve_all_profiles(self):
    # Pre-warm the PipelineOptions catalog cache once so subprocesses race
    # purely on `config.yaml` read-modify-write locking.
    rc_init, _, err_init = _run_cli_subproc(
        ['--json', 'config', 'set', '--profile', 'seed', 'project', 'seed-p'],
        beam_home=self.beam_home,
        cwd=self.tmp)
    self.assertEqual(rc_init, 0, msg=err_init)

    num_subprocs = 6
    num_threads = 6
    barrier = threading.Barrier(num_subprocs + num_threads)

    def _subproc_setter(idx: int):
      barrier.wait(timeout=20)
      rc, out, err = _run_cli_subproc(
          [
              '--json',
              'config',
              'set',
              '--profile',
              f'proc_prof_{idx}',
              'project',
              f'proc-proj-{idx}',
          ],
          beam_home=self.beam_home,
          cwd=self.tmp)
      self.assertEqual(rc, 0, msg=f'subproc config set {idx} failed: {err}\n{out}')

    def _thread_setter(idx: int):
      ns = argparse.Namespace(
          config_action='set',
          profile=f'thr_prof_{idx}',
          option='project',
          value=f'thr-proj-{idx}',
          json_output=False)
      barrier.wait(timeout=20)
      rc = config.run_command(ns, config_path=self.cfg_path)
      self.assertEqual(rc, 0)

    with ThreadPoolExecutor(max_workers=num_subprocs + num_threads) as pool:
      futs = (
          [pool.submit(_subproc_setter, i) for i in range(num_subprocs)] +
          [pool.submit(_thread_setter, j) for j in range(num_threads)])
      for f in futs:
        f.result()

    self.assertFalse(os.path.exists(f'{self.cfg_path}.corrupt'))
    parsed_cfg = yaml.safe_load(Path(self.cfg_path).read_text(encoding='utf-8'))
    profiles = parsed_cfg['profiles']
    self.assertEqual(profiles['seed']['options']['project'], 'seed-p')
    for i in range(num_subprocs):
      self.assertEqual(
          profiles[f'proc_prof_{i}']['options']['project'], f'proc-proj-{i}')
    for j in range(num_threads):
      self.assertEqual(
          profiles[f'thr_prof_{j}']['options']['project'], f'thr-proj-{j}')


class MixedOperationSubprocessStormTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam_cli_conc_storm_')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.beam_home = os.path.join(self.tmp, 'beam_home')
    os.makedirs(self.beam_home, exist_ok=True)

  def test_mixed_cli_operations_storm_zero_cross_job_interference(self):
    # Prepare shared read-only fixture datasets in one workspace directory.
    orders_csv = os.path.join(self.tmp, 'orders.csv')
    with open(orders_csv, 'w', encoding='utf-8') as f:
      f.write('id,tier,amount\n1,gold,120\n2,silver,45\n3,gold,230\n')

    tiers_csv = os.path.join(self.tmp, 'tiers.csv')
    with open(tiers_csv, 'w', encoding='utf-8') as f:
      f.write('tier,mult\ngold,2\nsilver,1\n')

    seed_pq = os.path.join(self.tmp, 'seed_lake.parquet')
    pq.write_table(
        pa.Table.from_pylist([
            {'id': 1, 'tier': 'gold', 'amount': 120},
            {'id': 2, 'tier': 'silver', 'amount': 45},
            {'id': 3, 'tier': 'gold', 'amount': 230},
        ]),
        seed_pq)

    pipeline_yaml = os.path.join(self.tmp, 'valid_pipeline.yaml')
    with open(pipeline_yaml, 'w', encoding='utf-8') as f:
      yaml.safe_dump(
          {
              'pipeline': {
                  'type': 'chain',
                  'transforms': [
                      {'type': 'ReadFromCsv', 'config': {'path': orders_csv}},
                      {
                          'type': 'WriteToJson',
                          'config': {
                              'path': os.path.join(self.tmp, 'plan_only_out')
                          },
                      },
                  ],
              }
          },
          f)

    out_fast_pq = os.path.join(self.tmp, 'storm_fast.parquet')
    out_beam_jsonl = os.path.join(self.tmp, 'storm_beam.jsonl')
    out_sql_pq = os.path.join(self.tmp, 'storm_sql.parquet')
    wh_dir = os.path.join(self.tmp, 'storm_wh')
    ice_dest = f'iceberg://storm.orders?warehouse={wh_dir}'

    tasks: dict[str, tuple[list[str], dict[str, str]]] = {
        'pipe_fast': (
            [
                '--json',
                '--no-config',
                'pipe',
                '--from',
                orders_csv,
                '--to',
                out_fast_pq,
                '--where',
                'amount >= 100',
            ],
            {},
        ),
        'pipe_beam_runner': (
            [
                '--json',
                '--no-config',
                'pipe',
                '--from',
                orders_csv,
                '--to',
                out_beam_jsonl,
            ],
            {'BEAM_CLI_NO_FAST_PATH': '1'},
        ),
        'pipe_sql_join': (
            [
                '--json',
                '--no-config',
                'pipe',
                '--from',
                f'o={orders_csv}',
                '--from',
                f't={tiers_csv}',
                '--to',
                out_sql_pq,
                '--sql',
                'SELECT CAST(o.id AS BIGINT) AS id, '
                'CAST(o.amount * t.mult AS BIGINT) AS boosted '
                'FROM o JOIN t USING (tier) ORDER BY id',
            ],
            {},
        ),
        'pipe_zero_copy_iceberg': (
            [
                '--json',
                '--no-config',
                'pipe',
                '--from',
                seed_pq,
                '--to',
                ice_dest,
                '--zero-copy',
            ],
            {},
        ),
        'diff_datasets': (
            [
                '--json',
                '--no-config',
                'diff',
                orders_csv,
                seed_pq,
                '--key',
                'id',
            ],
            {},
        ),
        'inspect_parquet': (
            ['--json', '--no-config', 'inspect', seed_pq],
            {},
        ),
        'schema_check': (
            [
                '--json',
                '--no-config',
                'schema',
                'check',
                orders_csv,
                seed_pq,
            ],
            {},
        ),
        'yaml_check': (
            ['--json', '--no-config', 'check', pipeline_yaml],
            {},
        ),
    }

    barrier = threading.Barrier(len(tasks))

    def _run_named(name: str):
      argv, extra_env = tasks[name]
      barrier.wait(timeout=20)
      rc, out, err = _run_cli_subproc(
          argv, beam_home=self.beam_home, cwd=self.tmp, extra_env=extra_env)
      return name, rc, out, err

    envelopes: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
      for name, rc, out, err in pool.map(_run_named, list(tasks.keys())):
        self.assertEqual(
            rc, 0, msg=f'storm op {name!r} failed rc={rc}: {err}\n{out}')
        env_obj = json.loads(out)
        self.assertTrue(env_obj['ok'], msg=f'{name}: {env_obj}')
        envelopes[name] = env_obj

    # Validate isolated data integrity across every concurrent operation.
    fast_rows = pq.read_table(out_fast_pq).to_pylist()
    self.assertEqual(sorted(int(r['id']) for r in fast_rows), [1, 3])

    with open(out_beam_jsonl, 'r', encoding='utf-8') as bf:
      beam_rows = [json.loads(ln) for ln in bf if ln.strip()]
    self.assertEqual(sorted(int(r['id']) for r in beam_rows), [1, 2, 3])

    sql_rows = pq.read_table(out_sql_pq).to_pylist()
    self.assertEqual(
        sql_rows,
        [
            {'id': 1, 'boosted': 240},
            {'id': 2, 'boosted': 45},
            {'id': 3, 'boosted': 460},
        ])

    storm_st = lakehouse.load_iceberg_state(
        os.path.join(wh_dir, 'storm', 'orders'))
    _, ice_rows = lakehouse.read_iceberg_rows_local(storm_st)
    self.assertEqual(sorted(int(r['id']) for r in ice_rows), [1, 2, 3])

    self.assertTrue(envelopes['diff_datasets']['outputs']['match'])
    self.assertEqual(envelopes['inspect_parquet']['outputs']['row_count'], 3)

    # Zero leftover `.tmp` or `.beam_replace_*` staging artifacts!
    leftovers = [
        fn for fn in os.listdir(self.tmp)
        if fn.endswith('.tmp') or fn.startswith('.beam_replace_')
    ]
    self.assertEqual(leftovers, [])


if __name__ == '__main__':
  unittest.main()
