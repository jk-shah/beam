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

"""Tests for Proposal 5: Iceberg Catalog Hierarchy and Partition-Aware Zero-Copy."""

import io
import json
import os
import shutil
import tempfile
import unittest
from unittest import mock
import urllib.error

import pyarrow as pa
import pyarrow.parquet as pq

from apache_beam.cli import lakehouse
from apache_beam.cli import pipe_spec
from apache_beam.cli import uris


class IcebergCatalogHierarchyAndPartitionTest(unittest.TestCase):

  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='lakehouse_catalog_test_')

  def tearDown(self):
    shutil.rmtree(self.tmp, ignore_errors=True)

  # -------------------------------------------------------------------------
  # 1. Catalog Hierarchy: BigQuery Managed Iceberg API First
  # -------------------------------------------------------------------------

  def test_resolve_via_bigquery_managed_api_first(self):
    """Hierarchy Level 1: BigQuery API resolves bigLakeConfiguration.storageUri."""
    mock_payload = {
        'kind': 'bigquery#table',
        'tableReference': {
            'projectId': 'prod-proj',
            'datasetId': 'analytics',
            'tableId': 'orders_iceberg',
        },
        'bigLakeConfiguration': {
            'storageUri': 'gs://prod-lakehouse-bucket/tables/orders_iceberg',
            'tableFormat': 'ICEBERG',
            'fileFormat': 'PARQUET',
        },
    }

    mock_resp = mock.MagicMock()
    mock_resp.read.return_value = json.dumps(mock_payload).encode('utf-8')
    mock_resp.__enter__.return_value = mock_resp

    with mock.patch('urllib.request.OpenerDirector.open', return_value=mock_resp) as mock_open:
      resolved_dir = lakehouse.resolve_iceberg_table_dir({
          'table': 'prod-proj.analytics.orders_iceberg',
          'catalog_properties': {'type': 'bigquery'},
      })
      self.assertEqual(resolved_dir, 'gs://prod-lakehouse-bucket/tables/orders_iceberg')
      # Verify target URL queried was BigQuery tables.get endpoint
      req = mock_open.call_args[0][0]
      self.assertIn('bigquery.googleapis.com/bigquery/v2/projects/prod-proj/datasets/analytics/tables/orders_iceberg', req.full_url)

  def test_resolve_catalog_hierarchy_fallback_to_biglake(self):
    """Hierarchy Level 2: BigQuery returns 404 -> falls through to BigLake REST catalog."""
    # BigQuery API returns 404 (Not Found)
    err_404 = urllib.error.HTTPError(
        'https://bigquery.googleapis.com/...', 404, 'Not Found', {}, io.BytesIO(b'{}'))

    # BigLake REST catalog returns table metadata location
    bl_payload = {
        'metadata-location': 'gs://bl-bucket/catalogs/c1/namespaces/analytics/tables/events/metadata/v1.metadata.json',
    }
    mock_bl_resp = mock.MagicMock()
    mock_bl_resp.read.return_value = json.dumps(bl_payload).encode('utf-8')
    mock_bl_resp.__enter__.return_value = mock_bl_resp

    def mock_opener(req, *args, **kwargs):
      if 'bigquery.googleapis.com' in req.full_url:
        raise err_404
      return mock_bl_resp

    with mock.patch('urllib.request.OpenerDirector.open', side_effect=mock_opener):
      resolved_dir = lakehouse.resolve_iceberg_table_dir({
          'table': 'analytics.events',
          'catalog_name': 'biglake',
          'catalog_properties': {
              'type': 'rest',
              'uri': 'https://biglake.googleapis.com/iceberg/v1/restcatalog',
              'warehouse': 'gs://bl-bucket/warehouse',
              'header.x-goog-user-project': 'my-proj',
          },
      })
      self.assertEqual(resolved_dir, 'gs://bl-bucket/catalogs/c1/namespaces/analytics/tables/events')

  def test_resolve_catalog_hierarchy_fallback_to_generic_rest(self):
    """Hierarchy Level 3: Generic REST catalog resolves via /v1/namespaces/{ns}/tables/{tbl}."""
    rest_payload = {
        'metadata-location': 'https://s3.external/bucket/wh/db/tbl/metadata/v2.metadata.json',
    }
    mock_resp = mock.MagicMock()
    mock_resp.read.return_value = json.dumps(rest_payload).encode('utf-8')
    mock_resp.__enter__.return_value = mock_resp

    with mock.patch('urllib.request.OpenerDirector.open', return_value=mock_resp) as mock_open:
      resolved_dir = lakehouse.resolve_iceberg_table_dir({
          'table': 'db.tbl',
          'catalog_properties': {
              'type': 'rest',
              'uri': 'https://custom-iceberg-rest.corp.internal:8443',
          },
      })
      self.assertEqual(resolved_dir, 'https://s3.external/bucket/wh/db/tbl')
      req = mock_open.call_args[0][0]
      self.assertIn('/v1/namespaces/db/tables/tbl', req.full_url)

  # -------------------------------------------------------------------------
  # 2. Catalog Table Registration / Publication
  # -------------------------------------------------------------------------

  def test_register_table_to_bigquery_managed_iceberg_catalog(self):
    """Registers zero-copy Iceberg table with BigQuery managed table API."""
    mock_resp = mock.MagicMock()
    mock_resp.read.return_value = b'{"kind": "bigquery#table"}'
    mock_resp.__enter__.return_value = mock_resp

    with mock.patch('urllib.request.OpenerDirector.open', return_value=mock_resp) as mock_open:
      res = lakehouse.register_iceberg_catalog_table(
          'prod-proj.analytics.iceberg_orders',
          'gs://prod-lakehouse/tables/iceberg_orders',
          'gs://prod-lakehouse/tables/iceberg_orders/metadata/v1.metadata.json',
          catalog_name='bigquery',
          catalog_properties={'type': 'bigquery'},
      )
      self.assertEqual(res['catalog'], 'bigquery')
      self.assertEqual(res['status'], 'registered')
      self.assertEqual(res['api'], 'bigquery.googleapis.com')
      # Inspect payload sent to BigQuery API
      req = mock_open.call_args[0][0]
      body = json.loads(req.data.decode('utf-8'))
      self.assertEqual(body['tableReference']['projectId'], 'prod-proj')
      self.assertEqual(body['tableReference']['datasetId'], 'analytics')
      self.assertEqual(body['tableReference']['tableId'], 'iceberg_orders')
      self.assertEqual(body['bigLakeConfiguration']['tableFormat'], 'ICEBERG')
      self.assertEqual(body['bigLakeConfiguration']['fileFormat'], 'PARQUET')
      self.assertEqual(body['bigLakeConfiguration']['storageUri'], 'gs://prod-lakehouse/tables/iceberg_orders')

  def test_register_table_to_bigquery_conflict_patches_table(self):
    """When BigQuery returns 409 Conflict, register falls back to PATCH."""
    err_409 = urllib.error.HTTPError(
        'https://bigquery.googleapis.com/...', 409, 'Conflict', {}, io.BytesIO(b'{}'))
    self.addCleanup(err_409.close)
    mock_patch_resp = mock.MagicMock()
    mock_patch_resp.read.return_value = b'{"kind": "bigquery#table"}'
    mock_patch_resp.__enter__.return_value = mock_patch_resp

    call_count = [0]
    def mock_opener(req, *args, **kwargs):
      call_count[0] += 1
      if call_count[0] == 1:
        raise err_409
      return mock_patch_resp

    with mock.patch('urllib.request.OpenerDirector.open', side_effect=mock_opener):
      res = lakehouse.register_iceberg_catalog_table(
          'prod-proj.analytics.iceberg_orders',
          'gs://prod-lakehouse/tables/iceberg_orders',
          'gs://prod-lakehouse/tables/iceberg_orders/metadata/v1.metadata.json',
          catalog_name='bigquery',
          catalog_properties={'type': 'bigquery'},
      )
      self.assertEqual(res['catalog'], 'bigquery')
      self.assertEqual(res['status'], 'updated')

  def test_register_table_to_biglake_catalog(self):
    """Registers table metadata location with BigLake REST catalog."""
    mock_resp = mock.MagicMock()
    mock_resp.read.return_value = b'{}'
    mock_resp.__enter__.return_value = mock_resp

    with mock.patch('urllib.request.OpenerDirector.open', return_value=mock_resp) as mock_open:
      res = lakehouse.register_iceberg_catalog_table(
          'analytics.events',
          'gs://wh-bucket/events',
          'gs://wh-bucket/events/metadata/v1.metadata.json',
          catalog_name='biglake',
          catalog_properties={
              'type': 'rest',
              'uri': 'https://biglake.googleapis.com/iceberg/v1/restcatalog',
              'warehouse': 'gs://wh-bucket/wh',
              'header.x-goog-user-project': 'my-proj',
          },
      )
      self.assertEqual(res['catalog'], 'biglake')
      self.assertEqual(res['status'], 'registered')
      req = mock_open.call_args[0][0]
      self.assertIn('/namespaces/analytics/register', req.full_url)
      payload = json.loads(req.data.decode('utf-8'))
      self.assertEqual(payload['name'], 'events')
      self.assertEqual(payload['metadata-location'], 'gs://wh-bucket/events/metadata/v1.metadata.json')

  # -------------------------------------------------------------------------
  # 3. Partition-Aware Zero-Copy Lakehouse Translation
  # -------------------------------------------------------------------------

  def test_partitioned_delta_zero_copy_to_iceberg(self):
    """Zero-copy translation of a partitioned Delta Lake table into Iceberg v2."""
    delta_dir = os.path.join(self.tmp, 'delta_partitioned')
    os.makedirs(os.path.join(delta_dir, '_delta_log'), exist_ok=True)

    # Partition 1: date=2026-11-10/region=US
    p1_dir = os.path.join(delta_dir, 'date=2026-11-10', 'region=US')
    os.makedirs(p1_dir, exist_ok=True)
    f1_path = os.path.join(p1_dir, 'part-00000.parquet')
    pq.write_table(
        pa.Table.from_pylist([
            {'order_id': 101, 'amount': 99.50},
            {'order_id': 102, 'amount': 150.00},
        ]),
        f1_path)

    # Partition 2: date=2026-11-11/region=EU
    p2_dir = os.path.join(delta_dir, 'date=2026-11-11', 'region=EU')
    os.makedirs(p2_dir, exist_ok=True)
    f2_path = os.path.join(p2_dir, 'part-00001.parquet')
    pq.write_table(
        pa.Table.from_pylist([
            {'order_id': 103, 'amount': 45.20},
            {'order_id': 104, 'amount': 210.75},
        ]),
        f2_path)

    # Delta log commit
    commit_payload = [
        {
            'metaData': {
                'id': 'tbl-part-1',
                'schemaString': json.dumps({
                    'type': 'struct',
                    'fields': [
                        {'name': 'order_id', 'type': 'long', 'nullable': True},
                        {'name': 'amount', 'type': 'double', 'nullable': True},
                        {'name': 'date', 'type': 'string', 'nullable': True},
                        {'name': 'region', 'type': 'string', 'nullable': True},
                    ],
                }),
                'partitionColumns': ['date', 'region'],
            }
        },
        {
            'add': {
                'path': 'date=2026-11-10/region=US/part-00000.parquet',
                'size': os.path.getsize(f1_path),
                'modificationTime': 1700000000000,
                'dataChange': True,
                'partitionValues': {'date': '2026-11-10', 'region': 'US'},
            }
        },
        {
            'add': {
                'path': 'date=2026-11-11/region=EU/part-00001.parquet',
                'size': os.path.getsize(f2_path),
                'modificationTime': 1700000001000,
                'dataChange': True,
                'partitionValues': {'date': '2026-11-11', 'region': 'EU'},
            }
        },
    ]
    with open(
        os.path.join(delta_dir, '_delta_log', '00000000000000000000.json'),
        'w',
        encoding='utf-8') as f:
      for entry in commit_payload:
        f.write(json.dumps(entry) + '\n')

    iceberg_dir = os.path.join(self.tmp, 'iceberg_out')
    src_ep = uris.parse_endpoint(f'delta://{delta_dir}', is_read=True)
    sink_ep = uris.parse_endpoint(f'iceberg://{iceberg_dir}', is_read=False)

    # Perform partition-aware zero copy translation
    res = lakehouse.register_zero_copy_to_iceberg(src_ep, sink_ep)

    # Verification: zero-copy invariants
    self.assertTrue(res['zero_copy'])
    self.assertEqual(res['rows_registered'], 4)
    self.assertEqual(res['bytes_copied'], 0)
    self.assertEqual(res['partition_columns'], ['date', 'region'])
    self.assertIsNotNone(res['partition_spec'])
    self.assertEqual(len(res['partition_spec']['fields']), 2)

    # Verify Iceberg metadata JSON on disk
    state = lakehouse.load_iceberg_state(iceberg_dir)
    meta = state['metadata']
    self.assertEqual(meta['default-spec-id'], 0)
    self.assertEqual(len(meta['partition-specs']), 1)
    spec_field_names = [f['name'] for f in meta['partition-specs'][0]['fields']]
    self.assertEqual(spec_field_names, ['date', 'region'])

    # Read back rows via local Iceberg reader: partition values MUST be correctly populated!
    total_live, rows = lakehouse.read_iceberg_rows_local(state)
    self.assertEqual(total_live, 4)
    self.assertEqual(len(rows), 4)

    rows_by_id = {r['order_id']: r for r in rows}
    self.assertEqual(rows_by_id[101]['date'], '2026-11-10')
    self.assertEqual(rows_by_id[101]['region'], 'US')
    self.assertEqual(rows_by_id[103]['date'], '2026-11-11')
    self.assertEqual(rows_by_id[103]['region'], 'EU')

  def test_hive_partitioned_parquet_zero_copy_to_iceberg(self):
    """Zero-copy translation of a Hive-partitioned directory of Parquet files."""
    data_dir = os.path.join(self.tmp, 'hive_parquet')
    dt_dir = os.path.join(data_dir, 'dt=2026-11-11')
    os.makedirs(dt_dir, exist_ok=True)
    fpath = os.path.join(dt_dir, 'batch-1.parquet')
    pq.write_table(
        pa.Table.from_pylist([{'device_id': 'd1', 'temp': 23.5}]), fpath)

    iceberg_dir = os.path.join(self.tmp, 'iceberg_hive')
    src_ep = uris.parse_endpoint(f'{data_dir}/**/*.parquet', is_read=True)
    sink_ep = uris.parse_endpoint(f'iceberg://{iceberg_dir}', is_read=False)

    res = lakehouse.register_zero_copy_to_iceberg(src_ep, sink_ep)
    self.assertTrue(res['zero_copy'])
    self.assertEqual(res['rows_registered'], 1)
    self.assertEqual(res['partition_columns'], ['dt'])

    state = lakehouse.load_iceberg_state(iceberg_dir)
    _, rows = lakehouse.read_iceberg_rows_local(state)
    self.assertEqual(rows[0]['device_id'], 'd1')
    self.assertEqual(rows[0]['dt'], '2026-11-11')

  # -------------------------------------------------------------------------
  # 4. URI Parsing & DataflowRunner Pipeline Generation
  # -------------------------------------------------------------------------

  def test_uris_parsing_bigquery_catalog_options(self):
    """Validates ?catalog=bigquery and auto-hierarchy URI parsing."""
    ep = uris.parse_endpoint('iceberg://my-proj.my_ds.my_tbl?catalog=bigquery', is_read=False)
    self.assertEqual(ep.kind, 'iceberg')
    self.assertEqual(ep.config['catalog_name'], 'bigquery')
    self.assertEqual(ep.config['catalog_properties']['type'], 'bigquery')
    self.assertEqual(ep.config['catalog_properties']['api'], 'https://bigquery.googleapis.com')

    # Auto-hierarchy: 3-part identifier defaults catalog_name to 'bigquery'
    ep_auto = uris.parse_endpoint('iceberg://prod-proj.analytics.sales_iceberg', is_read=False)
    self.assertEqual(ep_auto.config['catalog_name'], 'bigquery')
    self.assertEqual(ep_auto.config['catalog_properties']['type'], 'bigquery')

  def test_pipe_spec_dataflow_runner_bigquery_iceberg_yaml(self):
    """Validates DataflowRunner YAML generation for BigQuery managed Iceberg."""
    csv_src = os.path.join(self.tmp, 'data.csv')
    with open(csv_src, 'w', encoding='utf-8') as f:
      f.write('id,val\n1,test\n')

    pipe = pipe_spec.build_pipe_spec(
        csv_src,
        'iceberg://prod-proj.analytics.iceberg_tbl?catalog=bigquery',
        prefer_python_lakehouse_and_cdc=False,  # DataflowRunner uses Beam managed WriteToIceberg
    )
    sink_step = pipe.spec['pipeline']['transforms'][-1]
    self.assertEqual(sink_step['type'], 'WriteToIceberg')
    self.assertEqual(sink_step['config']['catalog_name'], 'bigquery')
    self.assertEqual(sink_step['config']['catalog_properties']['type'], 'bigquery')
    self.assertEqual(sink_step['config']['table'], 'prod-proj.analytics.iceberg_tbl')


if __name__ == '__main__':
  unittest.main()
