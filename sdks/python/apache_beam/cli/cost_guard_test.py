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

import os
import shutil
import sys
import tempfile
import types
import unittest
from unittest import mock

from apache_beam.cli import cost_guard
from apache_beam.cli import uris


def _ep(kind: str, raw: str, fmt: str | None, cfg: dict) -> uris.ParsedEndpoint:
  return uris.ParsedEndpoint(
      kind=kind,
      scheme='',
      raw=raw,
      format=fmt,
      yaml_type='Read',
      config=cfg,
  )


class CostGuardTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp()
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

  def test_parse_byte_size_and_format_bytes_across_units(self):
    self.assertEqual(cost_guard.parse_byte_size(0), 0)
    self.assertEqual(cost_guard.parse_byte_size(4096), 4096)
    self.assertEqual(cost_guard.parse_byte_size('500B'), 500)
    self.assertEqual(cost_guard.parse_byte_size('2KB'), 2000)
    self.assertEqual(cost_guard.parse_byte_size('2KiB'), 2048)
    self.assertEqual(cost_guard.parse_byte_size('1.5MB'), 1_500_000)
    self.assertEqual(cost_guard.parse_byte_size('1GiB'), 1024**3)
    self.assertEqual(cost_guard.parse_byte_size('1TB'), 1000**4)
    self.assertEqual(cost_guard.parse_byte_size('1PB'), 1000**5)

    with self.assertRaises(ValueError):
      cost_guard.parse_byte_size(-1)
    with self.assertRaises(ValueError):
      cost_guard.parse_byte_size('not-a-size')

    self.assertEqual(cost_guard.format_bytes(512), '512 B')
    self.assertEqual(cost_guard.format_bytes(2048), '2.00 KiB')
    self.assertEqual(cost_guard.format_bytes(5 * 1024**2), '5.00 MiB')
    self.assertEqual(cost_guard.format_bytes(3 * 1024**3), '3.00 GiB')
    self.assertEqual(cost_guard.format_bytes(2 * 1024**4), '2.00 TiB')

  def test_local_files_and_remote_filesystem_short_circuit(self):
    f_path = os.path.join(self.tmp, 'sample.csv')
    with open(f_path, 'wb') as f:
      f.write(b'x' * 2500)
    ep = _ep('file', f_path, 'csv', {'path': f_path})
    ok_est = cost_guard.check_max_bytes(ep, '10KB')
    self.assertEqual(ok_est['estimated_bytes'], 2500)
    self.assertEqual(ok_est['method'], 'local_stat')

    with self.assertRaises(cost_guard.CostLimitError) as ctx:
      cost_guard.check_max_bytes(ep, '1KB')
    self.assertEqual(ctx.exception.exit_code, cost_guard.EXIT_COST_LIMIT)
    self.assertEqual(ctx.exception.estimated_bytes, 2500)

    fake_matches = [
        types.SimpleNamespace(
            metadata_list=[
                types.SimpleNamespace(size_in_bytes=700),
                types.SimpleNamespace(size_in_bytes=800),
                types.SimpleNamespace(size_in_bytes=900),
            ]
        )
    ]
    gs_ep = _ep(
        'file',
        'gs://my-bkt/data/*.parquet',
        'parquet',
        {'path': 'gs://my-bkt/data/*.parquet'},
    )
    with mock.patch(
        'apache_beam.io.filesystems.FileSystems.match',
        return_value=fake_matches,
    ):
      full = cost_guard.check_max_bytes(gs_ep, '10KB')
      self.assertEqual(full['estimated_bytes'], 2400)
      self.assertFalse(full['short_circuited'])
      with self.assertRaisesRegex(cost_guard.CostLimitError, 'at least'):
        cost_guard.check_max_bytes(gs_ep, '1KB')

  def test_delta_iceberg_and_unbounded_endpoint_estimates(self):
    delta_ep = _ep(
        'delta', 'delta:///tmp/t', 'delta', {'table': '/tmp/t', 'version': '2'}
    )
    with mock.patch(
        'apache_beam.cli.lakehouse.parse_delta_log',
        return_value={
            'estimated_bytes': 8192,
            'active_files': ['a.parquet', 'b.parquet'],
            'row_count': 42,
        },
    ):
      est = cost_guard.estimate_endpoint_bytes(delta_ep)
    self.assertEqual(est['estimated_bytes'], 8192)
    self.assertEqual(est['file_count'], 2)
    self.assertEqual(est['method'], 'delta_log')

    ice_ep = _ep(
        'iceberg',
        'iceberg://ns.tbl?warehouse=/tmp/wh',
        'iceberg',
        {'table': 'ns.tbl', 'warehouse': '/tmp/wh'},
    )
    with (
        mock.patch(
            'apache_beam.cli.lakehouse.resolve_iceberg_table_dir',
            return_value='/tmp/wh/ns/tbl',
        ),
        mock.patch(
            'apache_beam.cli.lakehouse.load_iceberg_state',
            return_value={'estimated_bytes': 4096, 'data_files': ['f1']},
        ),
    ):
      ice_est = cost_guard.estimate_endpoint_bytes(ice_ep)
    self.assertEqual(ice_est['estimated_bytes'], 4096)
    self.assertEqual(ice_est['method'], 'iceberg_metadata')

    with mock.patch(
        'apache_beam.cli.lakehouse.resolve_iceberg_table_dir',
        side_effect=FileNotFoundError('no metadata'),
    ):
      ice_fallback = cost_guard.estimate_endpoint_bytes(ice_ep)
    self.assertIsNone(ice_fallback['estimated_bytes'])
    self.assertEqual(ice_fallback['method'], 'iceberg_catalog')

    pub_ep = _ep(
        'pubsub',
        'pubsub://projects/p/topics/t',
        'pubsub',
        {'topic': 'projects/p/topics/t'},
    )
    self.assertIsNone(
        cost_guard.estimate_endpoint_bytes(pub_ep)['estimated_bytes']
    )

  def test_bigquery_tables_get_and_dry_run_estimators(self):
    fake_bq = types.ModuleType('google.cloud.bigquery')
    captured_queries: list[str] = []

    class FakeQueryJobConfig:
      def __init__(self, **kwargs):
        self.kwargs = kwargs

    class FakeClient:
      def __init__(self, project=None):
        self.project = project or 'proj-a'

      def query(self, sql_text, job_config=None):
        del job_config
        captured_queries.append(sql_text)
        return types.SimpleNamespace(total_bytes_processed=123456)

      def get_table(self, fq_dot):
        if 'my_view' in fq_dot:
          return types.SimpleNamespace(
              table_type='VIEW', num_bytes=0, num_rows=0
          )
        return types.SimpleNamespace(
            table_type='TABLE', num_bytes=98765, num_rows=500
        )

    fake_bq.Client = FakeClient
    fake_bq.QueryJobConfig = FakeQueryJobConfig
    fake_gc = types.ModuleType('google.cloud')
    fake_gc.bigquery = fake_bq

    with mock.patch.dict(
        sys.modules,
        {'google.cloud': fake_gc, 'google.cloud.bigquery': fake_bq},
    ):
      ep_tbl = _ep(
          'bigquery',
          'bigquery://proj-a.ds.tbl',
          'bigquery',
          {'table': 'proj-a:ds.tbl'},
      )
      r_tbl = cost_guard.estimate_endpoint_bytes(ep_tbl)
      self.assertEqual(r_tbl['method'], 'bigquery_tables_get')
      self.assertEqual(r_tbl['estimated_bytes'], 98765)

      ep_q = _ep(
          'bigquery',
          'bigquery://proj-a.ds.tbl',
          'bigquery',
          {'query': 'SELECT id FROM `proj-a.ds.tbl`'},
      )
      r_q = cost_guard.estimate_endpoint_bytes(ep_q)
      self.assertEqual(r_q['method'], 'bigquery_dry_run')
      self.assertEqual(r_q['estimated_bytes'], 123456)

      ep_view = _ep(
          'bigquery',
          'bigquery://proj-a.ds.my_view',
          'bigquery',
          {
              'table': 'proj-a:ds.my_view',
              'fields': ['id', 'amount'],
              'row_restriction': 'amount > 10',
          },
      )
      r_v = cost_guard.estimate_endpoint_bytes(ep_view)
      self.assertEqual(r_v['method'], 'bigquery_dry_run')
      self.assertIn(
          'SELECT `id`, `amount` FROM `proj-a.ds.my_view`',
          captured_queries[-1],
      )

      ep_bad = _ep(
          'bigquery',
          'bigquery://proj-a.ds.my_view',
          'bigquery',
          {'table': 'proj-a:ds.my_view', 'fields': ['id; DROP TABLE x']},
      )
      with self.assertRaisesRegex(ValueError, 'unsafe BigQuery column'):
        cost_guard.estimate_endpoint_bytes(ep_bad)

  def test_postgres_partition_tree_and_fallbacks(self):
    ep_no_tbl = _ep(
        'postgres',
        'postgres://host:5432/db',
        'jdbc',
        {'url': 'jdbc:postgresql://host:5432/db'},
    )
    self.assertEqual(
        cost_guard.estimate_endpoint_bytes(ep_no_tbl)['method'],
        'postgres_custom_query',
    )

    ep_tbl = _ep(
        'postgres',
        'postgres://host:5432/db?table=public.orders',
        'jdbc',
        {
            'url': 'jdbc:postgresql://host:5432/db',
            'table': 'public.orders',
            'username': 'u',
            'password': 'p',
        },
    )

    class FakeCursor:
      def __enter__(self):
        return self

      def __exit__(self, *args):
        return False

      def execute(self, sql_text, params):
        assert 'pg_partition_tree' in sql_text
        assert params == ('public.orders',)

      def fetchone(self):
        return (654321,)

    class FakeConn:
      def __init__(self):
        self.closed = False

      def __enter__(self):
        return self

      def __exit__(self, *args):
        return False

      def cursor(self):
        return FakeCursor()

      def close(self):
        self.closed = True

    fake_psycopg = types.ModuleType('psycopg')
    fake_psycopg.connect = lambda dsn, **kw: FakeConn()
    with mock.patch.dict(sys.modules, {'psycopg': fake_psycopg}):
      pg_est = cost_guard.estimate_endpoint_bytes(ep_tbl)
    self.assertEqual(pg_est['method'], 'postgres_pg_partition_tree')
    self.assertEqual(pg_est['estimated_bytes'], 654321)

    fake_err_psycopg = types.ModuleType('psycopg')
    fake_err_psycopg.connect = mock.MagicMock(
        side_effect=OSError('conn refused')
    )
    with mock.patch.dict(sys.modules, {'psycopg': fake_err_psycopg}):
      pg_skip = cost_guard.estimate_endpoint_bytes(ep_tbl)
    self.assertEqual(pg_skip['method'], 'postgres_probe_skipped')


if __name__ == '__main__':
  unittest.main()
