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

"""Unit tests for top 5 developer experience agent wishlist features."""

import contextlib
import io
import json
import os
import shutil
import tempfile
import unittest
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

from apache_beam.cli import check_cmd
from apache_beam.cli import lakehouse
from apache_beam.cli import main as cli
from apache_beam.cli import pipe_spec


def _run_cli_json(argv):
  out_buf = io.StringIO()
  err_buf = io.StringIO()
  env = {'BEAM_NO_CONFIG': '1'}
  full_argv = list(argv) + (['--json'] if '--json' not in argv else [])
  with mock.patch.dict(os.environ, env, clear=False):
    with contextlib.redirect_stdout(out_buf), contextlib.redirect_stderr(err_buf):
      rc = cli.main(full_argv)
  raw_out = out_buf.getvalue().strip()
  payload = json.loads(raw_out) if raw_out else {}
  return rc, payload, err_buf.getvalue()


class AgentWishlistFeaturesTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='agent-wishlist-test-')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

  # -------------------------------------------------------------------------
  # Wishlist 1: Direct --select shorthand on beam pipe
  # -------------------------------------------------------------------------
  def test_direct_select_shorthand_without_to(self):
    csv_path = os.path.join(self.tmp, 'users.csv')
    with open(csv_path, 'w', encoding='utf-8') as f:
      f.write('id,name,amount\n')
      f.write('1,alice,100\n')
      f.write('2,bob,200\n')

    rc, res, _ = _run_cli_json(['pipe', '--from', csv_path, '--select', 'id, name'])
    self.assertEqual(rc, 0)
    self.assertTrue(res.get('ok'))
    self.assertEqual(res.get('runner'), 'local_sql')
    outputs = res.get('outputs', {})
    self.assertEqual(outputs.get('row_count'), 2)
    self.assertEqual(outputs.get('columns'), ['id', 'name'])
    untrusted = res.get('untrusted_data', {}).get('rows', [])
    self.assertEqual(len(untrusted), 2)
    self.assertEqual(untrusted[0]['name'], 'alice')

  def test_direct_select_shorthand_to_parquet(self):
    csv_path = os.path.join(self.tmp, 'sales.csv')
    with open(csv_path, 'w', encoding='utf-8') as f:
      f.write('id,sku,qty,price\n')
      f.write('10,item_a,2,15.5\n')
      f.write('20,item_b,1,40.0\n')

    out_pq = os.path.join(self.tmp, 'sales.parquet')
    rc, res, _ = _run_cli_json([
        'pipe', '--from', csv_path,
        '--select', 'id, qty * price AS total',
        '--to', out_pq
    ])
    self.assertEqual(rc, 0)
    self.assertTrue(os.path.exists(out_pq))
    table = pq.read_table(out_pq)
    self.assertEqual(table.column_names, ['id', 'total'])
    self.assertEqual(table.to_pylist(), [
        {'id': 10, 'total': 31.0},
        {'id': 20, 'total': 40.0},
    ])

  def test_direct_select_shorthand_preview_and_dry_run(self):
    csv_path = os.path.join(self.tmp, 'data.csv')
    with open(csv_path, 'w', encoding='utf-8') as f:
      f.write('a,b\n1,2\n')

    # preview without --to
    rc, res, _ = _run_cli_json(['pipe', '--from', csv_path, '--select', 'a', '--preview'])
    self.assertEqual(rc, 0)
    self.assertTrue(res.get('ok'))

    # dry-run without --to
    out_buf = io.StringIO()
    with contextlib.redirect_stdout(out_buf):
      rc = cli.main(['pipe', '--from', csv_path, '--select', 'a', '--dry-run'])
    self.assertEqual(rc, 0)
    yaml_text = out_buf.getvalue()
    self.assertIn('SELECT a FROM', yaml_text)

  # -------------------------------------------------------------------------
  # Wishlist 2: Database primary key aliases & auto-CDC activation
  # -------------------------------------------------------------------------
  def test_pipe_primary_key_aliases_auto_enable_cdc(self):
    parser = cli._build_parser()
    # Test --key alias
    args, _ = parser.parse_known_args(['pipe', '--from', 'src.csv', '--key', 'id'])
    self.assertEqual(args.primary_keys, ['id'])

    # Test --cdc-pk alias
    args, _ = parser.parse_known_args(['pipe', '--from', 'src.csv', '--cdc-pk', 'order_id'])
    self.assertEqual(args.primary_keys, ['order_id'])

    # Test --pk alias
    args, _ = parser.parse_known_args(['pipe', '--from', 'src.csv', '--pk', 'uid'])
    self.assertEqual(args.primary_keys, ['uid'])

  def test_diff_primary_key_aliases(self):
    parser = cli._build_parser()
    for flag in ('--key', '--primary-key', '--cdc-pk', '--pk'):
      args, _ = parser.parse_known_args(['diff', 'a.csv', 'b.csv', flag, 'user_id'])
      self.assertEqual(args.keys, ['user_id'])

  # -------------------------------------------------------------------------
  # Wishlist 3: Inline aggregations on --window
  # -------------------------------------------------------------------------
  def test_parse_window_spec_inline_aggregations(self):
    # Single aggregate
    w1 = pipe_spec.parse_window_spec('fixed:60s,sum(metric)')
    self.assertEqual(w1['type'], 'fixed')
    self.assertEqual(w1['size'], '60s')
    self.assertIn('aggregations', w1)
    aggs1 = w1['aggregations']
    self.assertEqual(aggs1['group_by'], [])
    self.assertEqual(aggs1['combine'], {'metric': {'value': 'metric', 'fn': 'sum'}})

    # Multiple aggregates with aliases and group_by
    w2 = pipe_spec.parse_window_spec(
        'sliding:5m,1m,group_by=user,total_bytes=sum(bytes),avg(latency)')
    self.assertEqual(w2['type'], 'sliding')
    self.assertEqual(w2['size'], '5m')
    self.assertEqual(w2['period'], '1m')
    aggs2 = w2['aggregations']
    self.assertEqual(aggs2['group_by'], ['user'])
    self.assertEqual(aggs2['combine'], {
        'total_bytes': {'value': 'bytes', 'fn': 'sum'},
        'latency': {'value': 'latency', 'fn': 'mean'},
    })

    # Count without arguments
    w3 = pipe_spec.parse_window_spec('session:30m,num_events=count()')
    self.assertEqual(w3['type'], 'sessions')
    self.assertEqual(w3['gap'], '30m')
    self.assertEqual(w3['aggregations']['combine'], {
        'num_events': {'value': 'num_events', 'fn': 'count'}
    })

  def test_build_pipe_spec_window_combine_transforms(self):
    spec = pipe_spec.build_pipe_spec(
        'users.csv', 'out.parquet', window='fixed:60s,sum(amount)')
    transforms = spec.spec['pipeline']['transforms']
    types = [t['type'] for t in transforms]
    self.assertIn('WindowInto', types)
    self.assertIn('Combine', types)
    combine_idx = types.index('Combine')
    self.assertEqual(transforms[combine_idx]['config']['combine'], {
        'amount': {'value': 'amount', 'fn': 'sum'}
    })

  # -------------------------------------------------------------------------
  # Wishlist 4: Pre-flight --portable validation flag
  # -------------------------------------------------------------------------
  def test_check_cmd_portable_flag_validates_sql(self):
    # Pipeline with SQLite-specific function instr()
    sqlite_yaml = os.path.join(self.tmp, 'sqlite_pipeline.yaml')
    with open(sqlite_yaml, 'w', encoding='utf-8') as f:
      f.write('''
pipeline:
  type: chain
  transforms:
    - type: ReadFromCsv
      config: {path: in.csv}
    - type: Sql
      config:
        query: "SELECT id, instr(email, '@') AS at_pos FROM in"
    - type: WriteToParquet
      config: {path: out.parquet}
''')

    # Without --portable: passes with non_portable warning
    report = check_cmd.run_check(sqlite_yaml, portable=False)
    self.assertTrue(report['valid'])
    self.assertFalse(report['portable_to_runner'])
    self.assertIn('SQL query uses instr()', report['non_portable_constructs'])

    # With --portable: raises CheckError
    with self.assertRaises(check_cmd.CheckError) as cm:
      check_cmd.run_check(sqlite_yaml, portable=True)
    self.assertIn('--portable rejected non-portable construct', str(cm.exception))

    # Clean ANSI SQL pipeline passes with portable=True
    ansi_yaml = os.path.join(self.tmp, 'ansi_pipeline.yaml')
    with open(ansi_yaml, 'w', encoding='utf-8') as f:
      f.write('''
pipeline:
  type: chain
  transforms:
    - type: ReadFromCsv
      config: {path: in.csv}
    - type: Sql
      config:
        query: "SELECT id, UPPER(email) AS upper_email FROM in"
    - type: WriteToParquet
      config: {path: out.parquet}
''')
    ansi_report = check_cmd.run_check(ansi_yaml, portable=True)
    self.assertTrue(ansi_report['valid'])
    self.assertTrue(ansi_report['portable_to_runner'])

  # -------------------------------------------------------------------------
  # Wishlist 5: Delta-to-Iceberg Schema Evolution Bridge
  # -------------------------------------------------------------------------
  def test_iceberg_schema_evolution_unions_active_parquet_files(self):
    # Create two Parquet files representing evolved schema:
    # File 1: id, name
    # File 2: id, name, email (new nullable field)
    dir_files = os.path.join(self.tmp, 'parquet_source')
    os.makedirs(dir_files)

    t1 = pa.Table.from_arrays(
        [pa.array([1, 2]), pa.array(['alice', 'bob'])],
        names=['id', 'name'])
    p1 = os.path.join(dir_files, 'part1.parquet')
    pq.write_table(t1, p1)

    t2 = pa.Table.from_arrays(
        [pa.array([3]), pa.array(['carol']), pa.array(['carol@example.com'])],
        names=['id', 'name', 'email'])
    p2 = os.path.join(dir_files, 'part2.parquet')
    pq.write_table(t2, p2)

    iceberg_dir = os.path.join(self.tmp, 'iceberg_table')
    from apache_beam.cli import uris
    src_ep = uris.parse_endpoint(os.path.join(dir_files, '*.parquet'), is_read=True)
    sink_ep = uris.parse_endpoint(f'iceberg://{iceberg_dir}', is_read=False)

    res = lakehouse.register_zero_copy_to_iceberg(src_ep, sink_ep)
    self.assertTrue(res.get('zero_copy'))
    self.assertEqual(res.get('rows_registered'), 3)

    # Verify Iceberg metadata contains unioned schema with nullable email
    state = lakehouse.load_iceberg_state(iceberg_dir)
    cur_schema = state['current_schema']
    field_names = [f['name'] for f in cur_schema['fields']]
    self.assertIn('id', field_names)
    self.assertIn('name', field_names)
    self.assertIn('email', field_names)

    email_field = next(f for f in cur_schema['fields'] if f['name'] == 'email')
    self.assertFalse(email_field['required'])  # nullable=True in Iceberg is required=False
    self.assertEqual(state['metadata']['last-column-id'], 3)

  def test_commit_iceberg_snapshot_schema_evolution_append(self):
    iceberg_dir = os.path.join(self.tmp, 'iceberg_append')
    f1 = os.path.join(self.tmp, 'f1.parquet')
    pq.write_table(pa.Table.from_arrays([pa.array([1])], names=['id']), f1)

    # Initial commit with id only
    lakehouse.commit_iceberg_snapshot(
        iceberg_dir,
        iceberg_fields=[{'id': 1, 'name': 'id', 'type': 'long', 'nullable': False}],
        new_data_files=[{'path': f1, 'record_count': 1, 'file_size_in_bytes': 100}],
    )

    state1 = lakehouse.load_iceberg_state(iceberg_dir)
    self.assertEqual(len(state1['current_schema']['fields']), 1)
    self.assertEqual(state1['metadata']['last-column-id'], 1)

    # Subsequent commit with new evolved field 'status'
    f2 = os.path.join(self.tmp, 'f2.parquet')
    pq.write_table(
        pa.Table.from_arrays([pa.array([2]), pa.array(['ACTIVE'])], names=['id', 'status']),
        f2)

    lakehouse.commit_iceberg_snapshot(
        iceberg_dir,
        iceberg_fields=[
            {'id': 1, 'name': 'id', 'type': 'long', 'nullable': False},
            {'id': 2, 'name': 'status', 'type': 'string', 'nullable': True},
        ],
        new_data_files=[{'path': f2, 'record_count': 1, 'file_size_in_bytes': 150}],
    )

    state2 = lakehouse.load_iceberg_state(iceberg_dir)
    self.assertEqual(len(state2['current_schema']['fields']), 2)
    self.assertEqual(state2['metadata']['last-column-id'], 2)
    status_f = next(f for f in state2['current_schema']['fields'] if f['name'] == 'status')
    self.assertFalse(status_f['required'])
    self.assertEqual(status_f['id'], 2)
    # Schema ID was incremented
    self.assertEqual(state2['current_schema']['schema-id'], 1)


if __name__ == '__main__':
  unittest.main()
