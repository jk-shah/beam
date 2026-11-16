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

"""Scale, memory-ceiling, FD-leak, and high-cardinality stress tests for `apache_beam.cli`.

Exercises 50k–100k row local ETL & dataset reconciliation, 500-shard fan-in,
200-column wide schemas, 60-snapshot Lakehouse histories, 15k-event stateful
streaming transforms, O(log N) ML batch poison-pill bisection, >1.4 MB payload
splitting, and 80k-row multi-engine SQL joins.
"""

from __future__ import annotations

import decimal
import gc
import json
import math
import os
import shutil
import tempfile
import time
import tracemalloc
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

import apache_beam as beam
from apache_beam.cli import asof_join
from apache_beam.cli import cep
from apache_beam.cli import cost_guard
from apache_beam.cli import dedup
from apache_beam.cli import diff
from apache_beam.cli import enrich
from apache_beam.cli import fast_local
from apache_beam.cli import infer
from apache_beam.cli import inspect_cmd
from apache_beam.cli import lakehouse
from apache_beam.cli import pipe_spec
from apache_beam.cli import pipe_sql
from apache_beam.cli import tail
from apache_beam.cli import uris
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


def _check_seq_is_two(row) -> None:
  if int(row.seq) != 2:
    raise AssertionError(f'expected seq == 2, got {row.seq!r}')


def _check_redacted_payload(row) -> None:
  if 'tok_' in str(row.raw_payload):
    raise AssertionError(f'unredacted secret in {row.raw_payload!r}')


def _open_fd_count() -> int | None:
  for fd_dir in ('/dev/fd', '/proc/self/fd'):
    if os.path.isdir(fd_dir):
      try:
        return len(os.listdir(fd_dir))
      except OSError:
        return None
  return None


class _CallCountingPoisonModelHandler(ModelHandler):
  """Fake `ModelHandler` counting `run_inference` calls on poison-pill batches."""
  def __init__(self, counter_box: list[int], poison_ids: set[int]):
    super().__init__()
    self._counter_box = counter_box
    self._poison_ids = poison_ids

  def load_model(self):
    return object()

  def run_inference(self, batch, model, inference_args=None):
    del model, inference_args
    self._counter_box[0] += 1
    for item in batch:
      rid = item['id'] if isinstance(item, dict) else getattr(item, 'id', -1)
      if int(rid) in self._poison_ids:
        raise ValueError(f'poison feature vector on id={rid}')
    return [
        PredictionResult(example=item, inference={'score': 0.95})
        for item in batch
    ]


class HighVolumeLocalEtlAndDiffStressTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam_cli_stress_etl_')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

  def test_100k_dirty_jsonl_etl_row_conservation_and_bounded_memory(self):
    src_jsonl = os.path.join(self.tmp, 'events_100k.jsonl')
    out_pq = os.path.join(self.tmp, 'clean_events.parquet')
    dlq_jsonl = os.path.join(self.tmp, 'dlq_events.jsonl')

    total_rows = 100_000
    expected_bad_json = 0
    expected_filtered = 0
    expected_expect_dlq = 0
    expected_written = 0

    with open(src_jsonl, 'w', encoding='utf-8') as f:
      for i in range(total_rows):
        if i % 25 == 0:
          f.write(f'{{"id":{i},"corrupt":tru_bad\n')
          expected_bad_json += 1
        elif i % 20 == 1:
          # Filtered out by `--where "status != 'DROP'"`
          f.write(
              f'{{"id":{i},"status":"DROP","amount":50.0,"region":"us"}}\n')
          expected_filtered += 1
        elif i % 40 == 2:
          # Quarantined by `--expect "amount > 0"`
          f.write(
              f'{{"id":{i},"status":"OK","amount":-10.5,"region":"eu"}}\n')
          expected_expect_dlq += 1
        else:
          f.write(
              f'{{"id":{i},"status":"OK","amount":125.25,"region":"us"}}\n')
          expected_written += 1

    self.assertEqual(
        expected_bad_json + expected_filtered + expected_expect_dlq +
        expected_written,
        total_rows)

    pipe = pipe_spec.build_pipe_spec(
        src_jsonl,
        out_pq,
        where="status != 'DROP'",
        select='id, round(amount * 2.0, 2) AS doubled, region',
        expect=['doubled > 0'],
        on_violation='dlq',
        dlq_sink=dlq_jsonl,
        assert_counts=True)

    gc.collect()
    tracemalloc.start()
    t0 = time.monotonic()
    try:
      res = fast_local.run(
          pipe, [src_jsonl], assert_counts=True, if_exists='replace')
      _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
      tracemalloc.stop()
    elapsed_s = time.monotonic() - t0

    self.assertEqual(res.metrics['rows_read'], total_rows)
    self.assertEqual(res.metrics['rows_written'], expected_written)
    self.assertEqual(res.metrics['rows_filtered_out'], expected_filtered)
    self.assertEqual(
        res.metrics['dlq_rows'], expected_bad_json + expected_expect_dlq)
    self.assertEqual(pq.read_metadata(out_pq).num_rows, expected_written)
    # 100k rows in Python/Arrow must stay comfortably below 350 MiB peak traced heap.
    self.assertLess(
        peak_bytes,
        350 * 1024 * 1024,
        msg=f'excessive peak memory: {peak_bytes / (1024 * 1024):.1f} MiB')
    self.assertLess(elapsed_s, 25.0, msg=f'100k ETL took {elapsed_s:.2f}s')

  def test_50k_row_keyed_diff_38_digit_decimal_and_nan_fidelity(self):
    left_pq = os.path.join(self.tmp, 'ledger_left.parquet')
    right_pq = os.path.join(self.tmp, 'ledger_right.parquet')
    dec_type = pa.decimal128(38, 18)
    schema = pa.schema([
        pa.field('id', pa.int64()),
        pa.field('balance', dec_type),
        pa.field('score', pa.float64()),
    ])

    n_rows = 50_000
    mismatch_ids = {17, 4_099, 19_999, 33_333, 49_998}
    base_dec = decimal.Decimal('12345678901234567890.123456789012345678')
    ulp_dec = decimal.Decimal('12345678901234567890.123456789012345679')

    ids = list(range(n_rows))
    left_bals = [base_dec] * n_rows
    right_bals = [
        ulp_dec if i in mismatch_ids else base_dec for i in range(n_rows)
    ]
    scores = [
        float('nan') if i % 500 == 0 else float(i % 100) * 0.5
        for i in range(n_rows)
    ]

    pq.write_table(
        pa.Table.from_arrays(
            [
                pa.array(ids, type=pa.int64()),
                pa.array(left_bals, type=dec_type),
                pa.array(scores, type=pa.float64()),
            ],
            schema=schema),
        left_pq)
    pq.write_table(
        pa.Table.from_arrays(
            [
                pa.array(ids, type=pa.int64()),
                pa.array(right_bals, type=dec_type),
                pa.array(scores, type=pa.float64()),
            ],
            schema=schema),
        right_pq)

    rc, report = diff.run_diff(left_pq, right_pq, keys=['id'], key_limit=20)
    self.assertEqual(rc, 6)
    self.assertFalse(report['match'])
    self.assertEqual(
        report['summary']['matched_rows'], n_rows - len(mismatch_ids))
    self.assertEqual(report['summary']['value_mismatches'], len(mismatch_ids))
    flagged_ids = {
        int(k['id']) if isinstance(k, dict) else (
            int(k[0]) if isinstance(k, list) else int(k))
        for k in report['differing_keys']['value_mismatches']
    }
    self.assertEqual(flagged_ids, mismatch_ids)

  def test_50k_cdc_events_compaction_and_tail_large_log_bounded_memory(self):
    cdc_jsonl = os.path.join(self.tmp, 'debezium_50k.jsonl')
    out_pq = os.path.join(self.tmp, 'cdc_materialized.parquet')

    # 5,000 keys x 10 events each = 50,000 Debezium CDC events:
    # Keys 0..999 end in DELETE ('d'); keys 1000..4999 end at seq=9 ('u').
    with open(cdc_jsonl, 'w', encoding='utf-8') as f:
      for seq in range(10):
        for k in range(5_000):
          if seq == 9 and k < 1_000:
            rec = {
                'op': 'd',
                'ts_ms': 1_700_000_000_000 + seq,
                'before': {'id': k, 'amount': k + seq - 1},
                'after': None,
            }
          else:
            rec = {
                'op': 'c' if seq == 0 else 'u',
                'ts_ms': 1_700_000_000_000 + seq,
                'before': None,
                'after': {'id': k, 'amount': k * 100 + seq},
            }
          f.write(json.dumps(rec) + '\n')

    pipe = pipe_spec.build_pipe_spec(
        cdc_jsonl,
        out_pq,
        cdc=True,
        primary_keys=['id'],
        cdc_format='debezium',
        assert_counts=True,
        prefer_python_lakehouse_and_cdc=True)
    res = fast_local.run(
        pipe, [cdc_jsonl], assert_counts=True, if_exists='replace')
    self.assertEqual(res.metrics['rows_read'], 50_000)
    self.assertEqual(res.metrics['rows_written'], 4_000)
    out_rows = pq.read_table(out_pq).to_pylist()
    self.assertEqual(len(out_rows), 4_000)
    by_id = {int(r['id']): int(r['amount']) for r in out_rows}
    self.assertNotIn(0, by_id)
    self.assertNotIn(999, by_id)
    self.assertEqual(by_id[1_000], 1_000 * 100 + 9)
    self.assertEqual(by_id[4_999], 4_999 * 100 + 9)

    # Also verify `tail.run_tail` on the multi-MB 50k-line file obeys
    # `max_scanned_bytes='64KB'` with < 16 MiB traced heap!
    gc.collect()
    tracemalloc.start()
    try:
      tail_res = tail.run_tail(
          cdc_jsonl, limit=15, follow=False, max_scanned_bytes='64KB')
      _, tail_peak = tracemalloc.get_traced_memory()
    finally:
      tracemalloc.stop()
    self.assertEqual(tail_res['sampled_count'], 15)
    self.assertLess(tail_peak, 16 * 1024 * 1024)


class ManyShardsWideSchemaAndFdLeakStressTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam_cli_stress_shards_')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

  def test_500_parquet_shards_inspect_cost_guard_zero_copy_and_fd_safety(self):
    shard_dir = os.path.join(self.tmp, 'sharded_lake')
    os.makedirs(shard_dir, exist_ok=True)

    num_shards = 500
    rows_per_shard = 20
    tbl_proto = pa.schema([
        pa.field('id', pa.int64()),
        pa.field('bucket', pa.string()),
    ])
    for s in range(num_shards):
      shard_path = os.path.join(
          shard_dir, f'events-{s:05d}-of-{num_shards:05d}.parquet')
      pq.write_table(
          pa.Table.from_pylist(
              [
                  {
                      'id': s * rows_per_shard + k, 'bucket': f'b_{s % 10}'
                  } for k in range(rows_per_shard)
              ],
              schema=tbl_proto),
          shard_path)

    # Also drop a huge unrelated sibling file starting with `events` (`events_archive.bin`)
    # to ensure prefix inspection & cost_guard on `events` never sweep siblings!
    unrelated_sibling = os.path.join(shard_dir, 'events_archive.bin')
    with open(unrelated_sibling, 'wb') as f:
      f.write(b'X' * (2 * 1024 * 1024))

    gc.collect()
    fds_before = _open_fd_count()

    glob_pat = os.path.join(shard_dir, 'events-*-of-*.parquet')
    bare_prefix = os.path.join(shard_dir, 'events')

    # 1. Full inspect across all 500 shards.
    info_full = inspect_cmd.run_inspect(
        glob_pat, sample_count=10, max_scanned_bytes='64MB')
    self.assertEqual(info_full['file_count'], num_shards)
    self.assertEqual(info_full['row_count'], num_shards * rows_per_shard)

    # 2. Tight `max_scanned_bytes` budget on 500 Parquet shards must bound I/O
    #    instead of blindly opening all 500 files!
    info_bounded = inspect_cmd.run_inspect(
        glob_pat, sample_count=5, max_scanned_bytes='4KB')
    self.assertEqual(info_bounded['file_count'], num_shards)
    self.assertTrue(info_bounded['untrusted_data']['truncated'])

    # 3. Bare prefix `events` must match `events-00000-of-00500.parquet` shards
    #    ONLY — never `events_archive.bin`!
    ep_prefix = uris.parse_endpoint(
        bare_prefix, is_read=True, explicit_format='parquet')
    est = cost_guard.estimate_endpoint_bytes(ep_prefix)
    self.assertEqual(est['file_count'], num_shards)
    self.assertLess(est['estimated_bytes'], 2 * 1024 * 1024)

    info_prefix = inspect_cmd.run_inspect(
        bare_prefix, explicit_format='parquet', sample_count=5)
    self.assertEqual(info_prefix['file_count'], num_shards)

    # 4. Zero-copy register all 500 shards into an Iceberg v2 table.
    wh_dir = os.path.join(self.tmp, 'wh')
    ice_uri = f'iceberg://scale.events?warehouse={wh_dir}'
    zc_res = lakehouse.register_zero_copy_to_iceberg(
        uris.parse_endpoint(glob_pat, is_read=True),
        uris.parse_endpoint(ice_uri, is_read=False),
        if_exists='fail')
    self.assertEqual(zc_res['bytes_copied'], 0)
    self.assertEqual(zc_res['rows_registered'], num_shards * rows_per_shard)
    self.assertEqual(len(zc_res['registered_files']), num_shards)

    gc.collect()
    fds_after = _open_fd_count()
    if fds_before is not None and fds_after is not None:
      self.assertLessEqual(
          fds_after - fds_before,
          4,
          msg=f'FD leak detected across 500-shard operations: {fds_before} -> {fds_after}')

  def test_200_column_wide_schema_with_python_keywords_etl(self):
    num_cols = 200
    col_names = ['id', 'from', 'class', 'in'] + [
        f'metric_{i:03d}' for i in range(num_cols - 4)
    ]
    src_pq = os.path.join(self.tmp, 'wide_200c.parquet')
    out_pq = os.path.join(self.tmp, 'wide_out.parquet')

    rows = []
    for r_i in range(500):
      rec: dict[str, object] = {
          'id': r_i,
          'from': 'us' if r_i % 2 == 0 else 'eu',
          'class': 'vip',
          'in': r_i * 2,
      }
      for c_i in range(num_cols - 4):
        rec[f'metric_{c_i:03d}'] = r_i + c_i
      rows.append(rec)
    pq.write_table(pa.Table.from_pylist(rows), src_pq)

    pick_cols = [0, 25, 50, 75, 100, 125, 150, 195]
    sum_terms = ' + '.join(f'metric_{i:03d}' for i in pick_cols)
    pipe = pipe_spec.build_pipe_spec(
        src_pq,
        out_pq,
        where='metric_000 >= 100 and metric_195 >= 100 and `from` == "us"',
        select=f'*, ({sum_terms}) AS rollup_8')
    res = fast_local.run(pipe, [src_pq], if_exists='replace')
    self.assertEqual(res.metrics['rows_written'], 200)
    out_tbl = pq.read_table(out_pq)
    self.assertEqual(len(out_tbl.column_names), num_cols + 1)
    first_out = out_tbl.slice(0, 1).to_pylist()[0]
    self.assertEqual(first_out['id'], 100)
    self.assertEqual(first_out['rollup_8'], sum(100 + i for i in pick_cols))


class DeepLakehouseCommitHistoryStressTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam_cli_stress_lh_')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

  def test_60_iceberg_commits_with_schema_evolution_and_mor_deletes(self):
    tdir = os.path.join(self.tmp, 'ice_60')
    data_dir = os.path.join(tdir, 'data')
    os.makedirs(data_dir, exist_ok=True)

    base_fields = [
        {'id': 1, 'name': 'id', 'type': 'long', 'nullable': False},
        {'id': 2, 'name': 'val', 'type': 'long', 'nullable': True},
    ]

    commit_times: list[float] = []
    # Commits 1..50: append 10 keys each (keys 0..499), evolving new columns every 10th commit.
    cur_fields = list(base_fields)
    next_fid = 3
    for step in range(50):
      if step > 0 and step % 10 == 0:
        cur_fields = list(cur_fields) + [{
            'id': next_fid,
            'name': f'evolved_{step}',
            'type': 'string',
            'nullable': True,
        }]
        next_fid += 1
      batch = []
      for k in range(10):
        key_id = step * 10 + k
        r: dict[str, object] = {'id': key_id, 'val': key_id}
        for ef in cur_fields[2:]:
          r[str(ef['name'])] = f'v_{step}'
        batch.append(r)
      d_path = os.path.join(data_dir, f'append-{step:03d}.parquet')
      df_entry = lakehouse.write_parquet_with_field_ids(
          d_path, batch, cur_fields)
      t0 = time.monotonic()
      lakehouse.commit_iceberg_snapshot(
          tdir,
          iceberg_fields=cur_fields,
          identifier_columns=['id'],
          new_data_files=[df_entry])
      commit_times.append(time.monotonic() - t0)

    # Commits 51..60: 10 Merge-on-Read equality-delete + upsert commits!
    # Each step deletes 5 keys (`step_idx * 5 .. step_idx * 5 + 4`) and updates 5 keys (`+10000`).
    id_only_field = [{'id': 1, 'name': 'id', 'type': 'long', 'nullable': False}]
    for m_step in range(10):
      del_keys = [{'id': m_step * 10 + k} for k in range(10)]
      upserts = [
          {'id': m_step * 10 + k, 'val': 10_000 + m_step * 10 + k}
          for k in range(5)
      ]
      del_path = os.path.join(data_dir, f'eqdel-{m_step:03d}.parquet')
      ups_path = os.path.join(data_dir, f'upsert-{m_step:03d}.parquet')
      del_entry = lakehouse.write_parquet_with_field_ids(
          del_path, del_keys, id_only_field)
      ups_entry = lakehouse.write_parquet_with_field_ids(
          ups_path, upserts, cur_fields)
      t0 = time.monotonic()
      lakehouse.commit_iceberg_snapshot(
          tdir,
          iceberg_fields=cur_fields,
          identifier_columns=['id'],
          new_data_files=[ups_entry],
          new_eq_delete_files=[del_entry])
      commit_times.append(time.monotonic() - t0)

    st = lakehouse.load_iceberg_state(tdir)
    self.assertEqual(st['metadata_version'], 60)
    self.assertEqual(len(st['snapshots']), 60)
    # Verify 60th commit stays fast (< 500 ms).
    self.assertLess(commit_times[-1], 0.5, msg=f'commit times: {commit_times[-5:]}')

    live_count, live_rows = lakehouse.read_iceberg_rows_local(st)
    # Started with 500 keys; 10 commits deleted 100 keys (0..99) and re-inserted 50 of them (5 per batch) -> 450 live!
    self.assertEqual(live_count, 450)
    self.assertEqual(len(live_rows), 450)
    by_id = {int(r['id']): int(r['val']) for r in live_rows}
    self.assertEqual(by_id[0], 10_000)
    self.assertNotIn(6, by_id)
    self.assertEqual(by_id[499], 499)

  def test_60_delta_log_commits_with_checkpoint_and_tombstones(self):
    delta_dir = os.path.join(self.tmp, 'delta_60')
    log_dir = os.path.join(delta_dir, '_delta_log')
    os.makedirs(log_dir, exist_ok=True)

    schema_json = json.dumps({
        'type': 'struct',
        'fields': [
            {'name': 'id', 'type': 'long', 'nullable': False, 'metadata': {}},
            {'name': 'v', 'type': 'string', 'nullable': True, 'metadata': {}},
        ],
    })

    # Write 60 Parquet files and 60 Delta commits; checkpoint at v39; remove files 0..19 in v40..v59!
    ckpt_entries = [
        {
            'protocol': {'minReaderVersion': 1, 'minWriterVersion': 2},
            'metaData': None,
            'add': None,
            'remove': None,
        },
        {
            'protocol': None,
            'metaData': {
                'id': 'tbl-60',
                'schemaString': schema_json,
                'partitionColumns': [],
            },
            'add': None,
            'remove': None,
        },
    ]

    for ver in range(60):
      rel_name = f'part-{ver:05d}.parquet'
      abs_pq = os.path.join(delta_dir, rel_name)
      pq.write_table(
          pa.Table.from_pylist([
              {'id': ver * 10 + k, 'v': f's_{ver}'} for k in range(10)
          ]),
          abs_pq)
      fsize = os.path.getsize(abs_pq)
      add_obj = {
          'path': rel_name,
          'size': fsize,
          'modificationTime': 1700000000000 + ver,
          'dataChange': True,
          'stats': json.dumps({'numRecords': 10}),
          'partitionValues': {},
      }
      lines = []
      if ver == 0:
        lines.append(
            json.dumps(
                {'protocol': {'minReaderVersion': 1, 'minWriterVersion': 2}}))
        lines.append(
            json.dumps({
                'metaData': {
                    'id': 'tbl-60',
                    'schemaString': schema_json,
                    'partitionColumns': [],
                }
            }))
      lines.append(json.dumps({'add': add_obj}))
      if ver >= 40:
        rem_idx = ver - 40
        lines.append(
            json.dumps({
                'remove': {
                    'path': f'part-{rem_idx:05d}.parquet',
                    'deletionTimestamp': 1700000100000 + ver,
                    'dataChange': True,
                }
            }))
      with open(
          os.path.join(log_dir, f'{ver:020d}.json'), 'w', encoding='utf-8') as lf:
        lf.write('\n'.join(lines) + '\n')

      if ver <= 39:
        ckpt_entries.append({
            'protocol': None,
            'metaData': None,
            'add': {**add_obj, 'partitionValues': []},
            'remove': None,
        })

    ckpt_schema = pa.schema([
        pa.field(
            'protocol',
            pa.struct([
                pa.field('minReaderVersion', pa.int64()),
                pa.field('minWriterVersion', pa.int64()),
            ])),
        pa.field(
            'metaData',
            pa.struct([
                pa.field('id', pa.string()),
                pa.field('schemaString', pa.string()),
                pa.field('partitionColumns', pa.list_(pa.string())),
            ])),
        pa.field(
            'add',
            pa.struct([
                pa.field('path', pa.string()),
                pa.field('size', pa.int64()),
                pa.field('modificationTime', pa.int64()),
                pa.field('dataChange', pa.bool_()),
                pa.field('stats', pa.string()),
                pa.field(
                    'partitionValues', pa.map_(pa.string(), pa.string())),
            ])),
        pa.field(
            'remove',
            pa.struct([
                pa.field('path', pa.string()),
                pa.field('deletionTimestamp', pa.int64()),
                pa.field('dataChange', pa.bool_()),
            ])),
    ])

    # Materialize v39 Parquet checkpoint + `_last_checkpoint`.
    pq.write_table(
        pa.Table.from_pylist(ckpt_entries, schema=ckpt_schema),
        os.path.join(log_dir, f'{39:020d}.checkpoint.parquet'))
    with open(
        os.path.join(log_dir, '_last_checkpoint'), 'w', encoding='utf-8') as cf:
      json.dump({'version': 39, 'size': len(ckpt_entries)}, cf)

    t0 = time.monotonic()
    snap = lakehouse.parse_delta_log(delta_dir)
    dt = time.monotonic() - t0
    self.assertEqual(snap['version'], 59)
    # 60 added - 20 removed (0..19) = 40 active files * 10 rows = 400 rows!
    self.assertEqual(len(snap['active_files']), 40)
    self.assertEqual(snap['row_count'], 400)
    self.assertLess(dt, 0.5)


class HighCardinalityStatefulStreamingStressTest(unittest.TestCase):
  def test_15k_event_dedup_asof_join_and_cep_nfa_pruning(self):
    # 1. Deduplicate 15,000 out-of-order events across 5,000 keys (`keep=last`, `order_by=seq`).
    dedup_events = [
        beam.Row(
            payment_id=f'pay_{k:05d}',
            seq=seq,
            amount=float(k * 10 + seq),
            event_ts=1_700_000_000.0 + (seq * 3.0))
        for seq in (2, 0, 1)
        for k in range(5_000)
    ]
    with beam.Pipeline(runner='FnApiRunner') as p:
      dedup_out = (
          p
          | 'CreateDedup' >> beam.Create(dedup_events)
          | 'Dedup' >> dedup.cli_deduplicate(
              keys=['payment_id'],
              keep='last',
              order_by='seq',
              time_domain='event'))
      assert_that(
          dedup_out | 'CountDedup' >> beam.combiners.Count.Globally(),
          equal_to([5_000]),
          label='AssertDedupCount')
      _ = dedup_out | 'CheckMaxSeq' >> beam.Map(_check_seq_is_two)

    # 2. Point-in-time `CliAsofJoin` across 4,000 trades and 4,000 quotes (2,000 symbols).
    trades = [
        beam.Row(
            sym=f'SYM_{s:04d}',
            trade_id=s * 2 + off,
            ts=1_700_000_100.0 + off * 10.0)
        for s in range(2_000)
        for off in (0, 1)
    ]
    quotes = [
        beam.Row(
            sym=f'SYM_{s:04d}',
            bid=100.0 + s + off * 0.5,
            ts=1_700_000_098.0 + off * 10.0)
        for s in range(2_000)
        for off in (0, 1)
    ]
    with beam.Pipeline(runner='FnApiRunner') as p:
      t_pc = p | 'Trades' >> beam.Create(trades)
      q_pc = p | 'Quotes' >> beam.Create(quotes)
      joined = (
          {'left': t_pc, 'right': q_pc}
          | 'AsofJoin' >> asof_join.cli_asof_join(
              on='sym',
              timestamp='ts',
              tolerance='5s',
              direction='backward',
              join_type='inner'))
      assert_that(
          joined | 'CountAsof' >> beam.combiners.Count.Globally(),
          equal_to([4_000]),
          label='AssertAsofCount')

    # 3. Bounded CEP (`CliPatternMatch`): 3,000 keys where 2,900 keys produce partial
    #    expired matches (testing NFA state pruning) and 100 keys complete A -> B!
    cep_events = []
    for uid in range(3_000):
      cep_events.append(
          beam.Row(uid=f'u_{uid:04d}', ev='LOGIN', ts=1_700_000_000.0))
      if uid < 100:
        cep_events.append(
            beam.Row(uid=f'u_{uid:04d}', ev='CHECKOUT', ts=1_700_000_010.0))
      else:
        # Arrives after `within='30s'` window so partial LOGIN state must expire cleanly!
        cep_events.append(
            beam.Row(uid=f'u_{uid:04d}', ev='NOOP', ts=1_700_000_120.0))

    with beam.Pipeline(runner='FnApiRunner') as p:
      matches = (
          p
          | 'CreateCep' >> beam.Create(cep_events)
          | 'PatternMatch' >> cep.cli_pattern_match(
              seq='login -> checkout within 30s',
              key='uid',
              where={
                  'login': "ev == 'LOGIN'",
                  'checkout': "ev == 'CHECKOUT'",
              },
              timestamp_col='ts',
              max_active_matches_per_key=16,
              force_stateful=True))
      assert_that(
          matches | 'CountCep' >> beam.combiners.Count.Globally(),
          equal_to([100]),
          label='AssertCep')


class BatchPayloadSplitAndPoisonPillBisectStressTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam_cli_stress_ml_')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

  def test_64_item_batch_single_poison_pill_uses_log_n_binary_bisect(self):
    call_box = [0]
    poison_set = {43}
    inner = _CallCountingPoisonModelHandler(call_box, poison_set)
    cfg = infer.parse_infer_config({'provider': 'mock', 'max_batch_size': 64})
    wrapper = infer._LazyCliModelHandler(
        cfg, catch_singleton_errors=True, handler_factory=lambda _c: inner)
    model = wrapper.load_model()

    batch = [{'id': i, 'x': float(i)} for i in range(64)]
    results = list(wrapper.run_inference(batch, model))

    self.assertEqual(len(results), 64)
    sentinels = [
        idx for idx, r in enumerate(results)
        if isinstance(r, infer._InferErrorSentinel)
    ]
    self.assertEqual(sentinels, [43])
    # Binary bisection of 1 poison item in 64 elements takes 1 + 2 * log2(64) = 13
    # calls (vs 65 calls for naive O(N) linear singleton fallback!).
    self.assertLessEqual(
        call_box[0],
        16,
        msg=f'expected O(log N) binary bisect (<=16 RPCs), got {call_box[0]} RPCs!')

  def test_vertex_1_4mb_payload_auto_split_and_oversized_dlq(self):
    cfg = infer.parse_infer_config({
        'provider': 'vertexai',
        'endpoint': 'projects/demo-proj/locations/us-central1/endpoints/12345',
        'max_batch_size': 64,
        'max_payload_bytes': 200_000,
    })
    dofn = infer._PreprocessAndBatchDoFn(cfg, has_dlq=True)
    dofn.start_bundle()

    # Feed 20 healthy 30 KB rows (~600 KB total -> must auto-split into >= 3 batches < 200 KB)
    # plus 1 oversized 250 KB row (must route directly to DLQ without crashing batcher).
    emitted_batches = []
    dlq_items = []
    from apache_beam.transforms import window as beam_window
    gwin = beam_window.GlobalWindow()

    for i in range(20):
      row = beam.Row(id=i, blob='A' * 30_000)
      for out in dofn.process(row, ts=1_700_000_000.0 + i, win=gwin):
        if hasattr(out, 'tag'):
          dlq_items.append(out.value)
        else:
          emitted_batches.append(out.value)

    huge_row = beam.Row(id=999, blob='Z' * 250_000)
    for out in dofn.process(huge_row, ts=1_700_000_100.0, win=gwin):
      if hasattr(out, 'tag'):
        dlq_items.append(out.value)
      else:
        emitted_batches.append(out.value)

    for out in dofn.finish_bundle():
      emitted_batches.append(out.value)

    self.assertEqual(len(dlq_items), 1)
    self.assertGreaterEqual(len(emitted_batches), 4)
    self.assertEqual(sum(len(b) for b in emitted_batches), 20)
    for b in emitted_batches:
      serialized = json.dumps([item[1] for item in b]).encode('utf-8')
      self.assertLess(len(serialized), 200_000)

  def test_20k_event_file_enrichment_with_miss_dlq_and_pii_redaction(self):
    dim_pq = os.path.join(self.tmp, 'dim_users.parquet')
    pq.write_table(
        pa.Table.from_pylist([
            {'uid': f'u_{i:04d}', 'tier': 'GOLD' if i % 2 == 0 else 'SILVER'}
            for i in range(2_000)
        ]),
        dim_pq)

    events = [
        beam.Row(
            event_id=i,
            uid=f'u_{i % 2_000:04d}' if i < 19_500 else f'missing_{i}',
            secret_token=f'tok_{i}')
        for i in range(20_000)
    ]
    with beam.Pipeline(runner='FnApiRunner') as p:
      res = (
          p
          | beam.Create(events)
          | enrich.cli_enrich(
              handler='file',
              path=dim_pq,
              key='uid',
              columns=['tier:str'],
              on_miss='dlq',
              dlq_redact_fields=['secret_token'],
              error_handling={'output': 'errors'}))
      assert_that(
          res['good'] | 'CountGood' >> beam.combiners.Count.Globally(),
          equal_to([19_500]),
          label='AssertGood')
      assert_that(
          res['errors'] | 'CountMiss' >> beam.combiners.Count.Globally(),
          equal_to([500]),
          label='AssertMiss')
      _ = res['errors'] | 'VerifyRedacted' >> beam.Map(_check_redacted_payload)


class AnalyticalAndWindowedSqlScaleStressTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam_cli_stress_sql_')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

  def test_80k_row_multi_source_join_and_10k_python_window_tvf(self):
    facts_pq = os.path.join(self.tmp, 'facts_80k.parquet')
    dims_pq = os.path.join(self.tmp, 'dims_1k.parquet')

    n_facts = 80_000
    pq.write_table(
        pa.table({
            'order_id': pa.array(range(n_facts), type=pa.int64()),
            'store_id': pa.array([i % 1_000 for i in range(n_facts)], type=pa.int64()),
            'user_id': pa.array([f'u_{i % 5_000}' for i in range(n_facts)], type=pa.string()),
            'amount': pa.array([10.0 + (i % 50) for i in range(n_facts)], type=pa.float64()),
        }),
        facts_pq)
    pq.write_table(
        pa.table({
            'store_id': pa.array(range(1_000), type=pa.int64()),
            'region': pa.array(
                [f'reg_{s % 5}' for s in range(1_000)], type=pa.string()),
        }),
        dims_pq)

    sources = [
        pipe_sql.NamedSource('f', facts_pq, uris.parse_endpoint(facts_pq, is_read=True)),
        pipe_sql.NamedSource('d', dims_pq, uris.parse_endpoint(dims_pq, is_read=True)),
    ]
    query = (
        'SELECT d.region, COUNT(*) AS cnt, '
        'COUNT(DISTINCT f.user_id) AS uniq_users, '
        'ROUND(SUM(f.amount), 2) AS rev '
        'FROM f JOIN d USING (store_id) '
        'GROUP BY d.region ORDER BY d.region')

    cols_sq, rows_sq, _, read_sq = pipe_sql.run_in_memory_sql(
        sources, query, engine='sqlite')
    cols_dk, rows_dk, _, read_dk = pipe_sql.run_in_memory_sql(
        sources, query, engine='duckdb')

    self.assertEqual(cols_sq, ['region', 'cnt', 'uniq_users', 'rev'])
    self.assertEqual(cols_dk, cols_sq)
    self.assertEqual(read_sq, n_facts + 1_000)
    self.assertEqual(read_dk, n_facts + 1_000)
    self.assertEqual(len(rows_sq), 5)
    self.assertEqual(rows_sq, rows_dk)
    self.assertEqual(sum(int(r['cnt']) for r in rows_sq), n_facts)

    # Also test pure-Python zero-JVM `TUMBLE` SQL lowering on 10,000 events!
    events_pq = os.path.join(self.tmp, 'stream_10k.parquet')
    out_win_pq = os.path.join(self.tmp, 'win_out.parquet')
    pq.write_table(
        pa.table({
            'dept': pa.array(
                [f'd_{i % 4}' for i in range(10_000)], type=pa.string()),
            'uid': pa.array(
                [f'u_{i % 250}' for i in range(10_000)], type=pa.string()),
            'amt': pa.array([5.0] * 10_000, type=pa.float64()),
            'ts': pa.array(
                [
                    f'2026-01-01T00:{(i // 2000) * 10:02d}:{(i % 60):02d}Z'
                    for i in range(10_000)
                ],
                type=pa.string()),
        }),
        events_pq)

    spec_dict = pipe_sql.build_sql_yaml_spec(
        [
            pipe_sql.NamedSource(
                'e', events_pq, uris.parse_endpoint(events_pq, is_read=True))
        ],
        "SELECT dept, COUNT(*) AS n, COUNT(DISTINCT uid) AS du, SUM(amt) AS s "
        "FROM TABLE(TUMBLE(TABLE e, DESCRIPTOR(ts), INTERVAL '10' MINUTE)) "
        "GROUP BY window_start, window_end, dept",
        uris.parse_endpoint(out_win_pq, is_read=False),
        prefer_python_lowering=True,
        strict_python_lowering=True)
    import yaml
    from apache_beam.yaml import main as yaml_main
    from apache_beam.cli import dlq as dlq_mod
    dlq_mod.install_keyword_safe_row_hooks()
    yaml_text = yaml.safe_dump(spec_dict, sort_keys=False)
    opts, constructor = yaml_main.build_pipeline_components_from_yaml(
        yaml_text, ['--runner=FnApiRunner'], validate_schema='generic')
    with beam.Pipeline(options=opts) as p:
      constructor(p)

    win_files = [
        os.path.join(self.tmp, fn)
        for fn in os.listdir(self.tmp)
        if fn.startswith('win_out.parquet')
    ]
    win_rows = []
    for wf in win_files:
      win_rows.extend(pq.read_table(wf).to_pylist())
    # 5 10-minute windows * 4 depts = 20 window-dept groups summing to 10,000 events!
    self.assertEqual(len(win_rows), 20)
    self.assertEqual(sum(int(r['n']) for r in win_rows), 10_000)
    self.assertTrue(
        all(math.isclose(float(r['s']), float(r['n']) * 5.0) for r in win_rows))


if __name__ == '__main__':
  unittest.main()
