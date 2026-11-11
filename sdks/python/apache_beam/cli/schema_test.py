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

"""Unit tests for pre-flight schema contract guard (`beam schema diff`)."""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from apache_beam.cli import schema


class SchemaDiffTest(unittest.TestCase):

  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam_schema_test_')

  def tearDown(self):
    shutil.rmtree(self.tmp, ignore_errors=True)

  def test_normalize_type_canonical_names(self):
    self.assertEqual(schema.normalize_type('int'), 'INT32')
    self.assertEqual(schema.normalize_type('int32'), 'INT32')
    self.assertEqual(schema.normalize_type('int64'), 'INT64')
    self.assertEqual(schema.normalize_type('bigint'), 'INT64')
    self.assertEqual(schema.normalize_type('float'), 'FLOAT32')
    self.assertEqual(schema.normalize_type('double'), 'FLOAT64')
    self.assertEqual(schema.normalize_type('numeric'), 'FLOAT64')
    self.assertEqual(schema.normalize_type('string'), 'STRING')
    self.assertEqual(schema.normalize_type('varchar'), 'STRING')
    self.assertEqual(schema.normalize_type('bool'), 'BOOLEAN')
    self.assertEqual(schema.normalize_type('boolean'), 'BOOLEAN')
    self.assertEqual(schema.normalize_type('timestamp[us]'), 'TIMESTAMP')
    self.assertEqual(schema.normalize_type('date32'), 'DATE')
    self.assertEqual(schema.normalize_type('list[int32]'), 'ARRAY')
    self.assertEqual(schema.normalize_type('struct<a:int>'), 'RECORD')

  def test_identical_schemas_are_compatible(self):
    src = [
        {'name': 'id', 'type': 'int64', 'nullable': False},
        {'name': 'name', 'type': 'string', 'nullable': True},
        {'name': 'score', 'type': 'float64', 'nullable': True},
    ]
    tgt = [
        {'name': 'id', 'type': 'INT64', 'nullable': False},
        {'name': 'name', 'type': 'STRING', 'nullable': True},
        {'name': 'score', 'type': 'FLOAT64', 'nullable': True},
    ]
    res = schema.diff_schemas(src, tgt, mode='BACKWARD')
    self.assertTrue(res['compatible'])
    self.assertEqual(res['violation_count'], 0)
    self.assertEqual(len(res['violations']), 0)

  def test_safe_type_widening_allowed(self):
    src = [
        {'name': 'id', 'type': 'int32', 'nullable': False},
        {'name': 'ratio', 'type': 'float32', 'nullable': True},
    ]
    tgt = [
        {'name': 'id', 'type': 'INT64', 'nullable': False},
        {'name': 'ratio', 'type': 'FLOAT64', 'nullable': True},
    ]
    res = schema.diff_schemas(src, tgt, mode='BACKWARD', strict=False)
    self.assertTrue(res['compatible'])
    self.assertEqual(res['violation_count'], 0)
    # Check that safe widening was noted in warnings
    issues = [w['issue'] for w in res['warnings']]
    self.assertIn('SAFE_TYPE_WIDENING', issues)

  def test_safe_type_widening_rejected_under_strict(self):
    src = [{'name': 'id', 'type': 'int32', 'nullable': False}]
    tgt = [{'name': 'id', 'type': 'int64', 'nullable': False}]
    res = schema.diff_schemas(src, tgt, mode='BACKWARD', strict=True)
    self.assertFalse(res['compatible'])
    self.assertEqual(res['violation_count'], 1)
    self.assertEqual(res['violations'][0]['issue'], 'STRICT_TYPE_WIDENING_VIOLATION')

  def test_type_narrowing_string_to_int64_violation(self):
    src = [
        {'name': 'customer_id', 'type': 'string', 'nullable': True},
    ]
    tgt = [
        {'name': 'customer_id', 'type': 'int64', 'nullable': True},
    ]
    res = schema.diff_schemas(src, tgt, mode='BACKWARD')
    self.assertFalse(res['compatible'])
    self.assertEqual(res['violation_count'], 1)
    v = res['violations'][0]
    self.assertEqual(v['field'], 'customer_id')
    self.assertEqual(v['issue'], 'TYPE_NARROWING_ERROR')
    # Check that actionable next action with CAST was recommended
    self.assertEqual(len(res['next_actions']), 1)
    self.assertIn('CAST(customer_id AS INT64)', res['next_actions'][0]['command_hint'])

  def test_type_narrowing_float_to_int_violation(self):
    src = [{'name': 'amount', 'type': 'float64', 'nullable': False}]
    tgt = [{'name': 'amount', 'type': 'int64', 'nullable': False}]
    res = schema.diff_schemas(src, tgt, mode='BACKWARD')
    self.assertFalse(res['compatible'])
    self.assertEqual(res['violations'][0]['issue'], 'TYPE_NARROWING_ERROR')

  def test_dropped_required_column_violation(self):
    src = [{'name': 'order_id', 'type': 'int64', 'nullable': False}]
    tgt = [
        {'name': 'order_id', 'type': 'int64', 'nullable': False},
        {'name': 'tenant_id', 'type': 'string', 'nullable': False},  # Required
    ]
    res = schema.diff_schemas(src, tgt, mode='BACKWARD')
    self.assertFalse(res['compatible'])
    v = res['violations'][0]
    self.assertEqual(v['field'], 'tenant_id')
    self.assertEqual(v['issue'], 'DROPPED_REQUIRED_COLUMN')
    self.assertIn('tenant_id', res['diff']['columns_missing'][0])

  def test_missing_nullable_target_column_is_safe(self):
    src = [{'name': 'order_id', 'type': 'int64', 'nullable': False}]
    tgt = [
        {'name': 'order_id', 'type': 'int64', 'nullable': False},
        {'name': 'notes', 'type': 'string', 'nullable': True},  # Nullable
    ]
    res = schema.diff_schemas(src, tgt, mode='BACKWARD')
    self.assertTrue(res['compatible'])
    self.assertEqual(res['violation_count'], 0)
    self.assertIn('notes', res['diff']['columns_missing'][0])

  def test_nullability_mismatch_violation(self):
    src = [{'name': 'status', 'type': 'string', 'nullable': True}]
    tgt = [{'name': 'status', 'type': 'string', 'nullable': False}]
    res = schema.diff_schemas(src, tgt, mode='BACKWARD')
    self.assertFalse(res['compatible'])
    self.assertEqual(res['violations'][0]['issue'], 'NULLABILITY_MISMATCH')

  def test_end_to_end_file_schema_diff(self):
    # Source: Parquet with (id: int64, val: string, score: float64)
    p_src = os.path.join(self.tmp, 'src.parquet')
    tbl_src = pa.Table.from_pylist([
        {'id': 1, 'val': 'test', 'score': 10.5},
    ])
    pq.write_table(tbl_src, p_src)

    # Target: Parquet with (id: int64, val: string)
    p_tgt = os.path.join(self.tmp, 'tgt.parquet')
    tbl_tgt = pa.Table.from_pylist([
        {'id': 1, 'val': 'test'},
    ])
    pq.write_table(tbl_tgt, p_tgt)

    res = schema.run_schema_diff(p_src, p_tgt, mode='BACKWARD')
    self.assertTrue(res['compatible'])
    self.assertIn('score (FLOAT64, NULLABLE)', res['diff']['columns_added'])

  def test_end_to_end_incompatible_file_diff(self):
    # Source has customer_id as string
    p_src = os.path.join(self.tmp, 'orders_v2.parquet')
    tbl_src = pa.Table.from_pylist([
        {'customer_id': 'C100', 'amount': 50.0},
    ])
    pq.write_table(tbl_src, p_src)

    # Target has customer_id as int64
    p_tgt = os.path.join(self.tmp, 'orders_target.parquet')
    tbl_tgt = pa.Table.from_pylist([
        {'customer_id': 100, 'amount': 50.0},
    ])
    pq.write_table(tbl_tgt, p_tgt)

    res = schema.run_schema_diff(p_src, p_tgt, mode='BACKWARD')
    self.assertFalse(res['compatible'])
    self.assertEqual(res['violation_count'], 1)
    self.assertEqual(res['violations'][0]['issue'], 'TYPE_NARROWING_ERROR')


  def test_forward_mode_new_required_column_fails(self):
    # Old target does not have new required column in source
    src = [
        {'name': 'id', 'type': 'int64', 'nullable': False},
        {'name': 'tenant_id', 'type': 'string', 'nullable': False},
    ]
    tgt = [
        {'name': 'id', 'type': 'int64', 'nullable': False},
    ]
    res = schema.diff_schemas(src, tgt, mode='FORWARD')
    self.assertFalse(res['compatible'])
    self.assertEqual(res['violations'][0]['issue'], 'FORWARD_REQUIRED_COLUMN_ADDED')

  def test_full_mode_requires_bidirectional_compatibility(self):
    src = [{'name': 'id', 'type': 'int64', 'nullable': False}]
    tgt = [{'name': 'id', 'type': 'int64', 'nullable': False}]
    res = schema.diff_schemas(src, tgt, mode='FULL')
    self.assertTrue(res['compatible'])

  def test_cli_main_schema_diff_json_success(self):
    from apache_beam.cli import main
    import io
    import json
    from unittest import mock

    p_src = os.path.join(self.tmp, 'c_src.parquet')
    pq.write_table(pa.Table.from_pylist([{'id': 1}]), p_src)
    p_tgt = os.path.join(self.tmp, 'c_tgt.parquet')
    pq.write_table(pa.Table.from_pylist([{'id': 1}]), p_tgt)

    buf = io.StringIO()
    with mock.patch('sys.stdout', buf):
      rc = main.main(['schema', 'diff', p_src, p_tgt, '--json'])
    self.assertEqual(rc, 0)
    data = json.loads(buf.getvalue())
    self.assertTrue(data['ok'])
    self.assertEqual(data['command'], 'schema-diff')
    self.assertTrue(data['outputs']['compatible'])

  def test_cli_main_schema_diff_json_violation_exit_2(self):
    from apache_beam.cli import main
    import io
    import json
    from unittest import mock

    p_src = os.path.join(self.tmp, 'bad_src.parquet')
    pq.write_table(pa.Table.from_pylist([{'id': 'not_an_int'}]), p_src)
    p_tgt = os.path.join(self.tmp, 'bad_tgt.parquet')
    pq.write_table(pa.Table.from_pylist([{'id': 100}]), p_tgt)

    buf = io.StringIO()
    with mock.patch('sys.stdout', buf):
      rc = main.main(['schema', 'diff', p_src, p_tgt, '--json'])
    self.assertEqual(rc, 2)
    data = json.loads(buf.getvalue())
    self.assertFalse(data['ok'])
    self.assertEqual(data['exit_code'], 2)
    self.assertFalse(data['outputs']['compatible'])
    self.assertEqual(len(data['outputs']['violations']), 1)
    self.assertEqual(data['outputs']['violations'][0]['issue'], 'TYPE_NARROWING_ERROR')

  def test_cli_main_schema_check_alias(self):
    from apache_beam.cli import main
    import io
    import json
    from unittest import mock

    p_src = os.path.join(self.tmp, 'chk_src.parquet')
    pq.write_table(pa.Table.from_pylist([{'id': 1}]), p_src)
    p_tgt = os.path.join(self.tmp, 'chk_tgt.parquet')
    pq.write_table(pa.Table.from_pylist([{'id': 1}]), p_tgt)

    buf = io.StringIO()
    with mock.patch('sys.stdout', buf):
      rc = main.main(['schema', 'check', p_src, p_tgt, '--json'])
    self.assertEqual(rc, 0)
    data = json.loads(buf.getvalue())
    self.assertTrue(data['ok'])

  def test_cli_pipe_check_schema_incompatible_halts_fast(self):
    from apache_beam.cli import main
    import io
    import json
    from unittest import mock

    p_src = os.path.join(self.tmp, 'pipe_bad_src.parquet')
    pq.write_table(pa.Table.from_pylist([{'id': 'str_val'}]), p_src)
    p_tgt = os.path.join(self.tmp, 'pipe_bad_tgt.parquet')
    pq.write_table(pa.Table.from_pylist([{'id': 100}]), p_tgt)

    buf = io.StringIO()
    with mock.patch('sys.stdout', buf):
      rc = main.main(['pipe', '--from', p_src, '--to', p_tgt, '--check-schema', '--json'])
    self.assertEqual(rc, 2)
    data = json.loads(buf.getvalue())
    self.assertFalse(data['ok'])
    self.assertIn('Pre-flight schema contract check failed', data['error']['message'])
    self.assertTrue(len(data['next_actions']) > 0)

  def test_cli_pipe_check_schema_compatible_succeeds(self):
    from apache_beam.cli import main
    import io
    from unittest import mock

    p_src = os.path.join(self.tmp, 'pipe_ok_src.parquet')
    pq.write_table(pa.Table.from_pylist([{'id': 1}]), p_src)
    p_out = os.path.join(self.tmp, 'pipe_ok_out.parquet')

    buf = io.StringIO()
    with mock.patch('sys.stdout', buf):
      rc = main.main(['pipe', '--from', p_src, '--to', p_out, '--check-schema', '--json'])
    self.assertEqual(rc, 0)


if __name__ == '__main__':
  unittest.main()

