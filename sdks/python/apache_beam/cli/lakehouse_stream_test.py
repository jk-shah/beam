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

"""Unit tests for Turnkey Stream-to-Lakehouse Pipeline Declarations (Proposal 2).

Validates:
1. Confluent Schema Registry URL wiring into ReadFromKafka and auto-AVRO formatting.
2. Consumer group ('group.id') and auto-offset-reset configuration.
3. Automated derivation of triggering_frequency_seconds on Iceberg sinks from window duration.
4. Portable Beam YAML generation for Kafka -> Iceberg streaming pipelines.
5. Google Cloud BigLake REST Catalog integration and Dataflow streaming option injection.
"""

import os
import shutil
import tempfile
import unittest
from typing import Optional

import pyarrow.parquet as pq

import apache_beam as beam
from apache_beam.cli import lakehouse
from apache_beam.cli import main
from apache_beam.cli import pipe_spec
from apache_beam.cli import uris
from apache_beam.internal import pickler
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.typehints.row_type import RowTypeConstraint


class LakehouseStreamTest(unittest.TestCase):

  def test_kafka_uri_with_schema_registry_defaults_to_avro(self):
    ep = uris.parse_endpoint(
        'kafka://broker.prod:9092/telemetry',
        options={'schema_registry': 'http://registry.prod:8081'},
        is_read=True)
    self.assertEqual(ep.kind, 'kafka')
    self.assertEqual(ep.format, 'avro')
    self.assertEqual(
        ep.config['confluent_schema_registry_url'],
        'http://registry.prod:8081')
    self.assertEqual(ep.config['bootstrap_servers'], 'broker.prod:9092')
    self.assertEqual(ep.config['topic'], 'telemetry')
    self.assertTrue(ep.is_unbounded)

  def test_kafka_uri_with_schema_registry_url_alias_and_subject(self):
    ep = uris.parse_endpoint(
        'kafka://broker:9092/events',
        options={
            'schema_registry_url': 'https://registry.corp:8081',
            'schema_registry_subject': 'events-value',
            'format': 'JSON',
        },
        is_read=True)
    self.assertEqual(
        ep.config['confluent_schema_registry_url'],
        'https://registry.corp:8081')
    self.assertEqual(
        ep.config['confluent_schema_registry_subject'],
        'events-value')
    # Explicit user format overrides default AVRO
    self.assertEqual(ep.format, 'json')

  def test_kafka_uri_with_consumer_group_and_offset_reset(self):
    ep = uris.parse_endpoint(
        'kafka://broker:9092/orders',
        options={
            'consumer_group': 'lakehouse-ingest',
            'auto_offset_reset': 'earliest',
            'timestamp_policy': 'CreateTime',
        },
        is_read=True)
    self.assertEqual(
        ep.config['consumer_config']['group.id'],
        'lakehouse-ingest')
    self.assertEqual(ep.config['auto_offset_reset_config'], 'earliest')
    self.assertEqual(ep.config['timestamp_policy'], 'CreateTime')

  def test_kafka_uri_rejects_malformed_schema_registry_url(self):
    with self.assertRaises(uris.UriError) as ctx:
      uris.parse_endpoint(
          'kafka://broker:9092/orders',
          options={'schema_registry': 'invalid_scheme_url'},
          is_read=True)
    self.assertIn('invalid schema_registry URL', str(ctx.exception))

  def test_build_pipe_spec_kafka_to_iceberg_with_fixed_window(self):
    pipe = pipe_spec.build_pipe_spec(
        'kafka://broker.prod:9092/telemetry',
        'iceberg://analytics.telemetry?warehouse=gs://my-lakehouse-wh',
        window='fixed:60s',
        from_opts={'schema_registry': 'https://registry.prod:8081'},
        prefer_python_lakehouse_and_cdc=True)

    self.assertTrue(pipe.is_unbounded)
    spec = pipe.spec
    self.assertTrue(spec.get('options', {}).get('streaming'))

    chain = spec['pipeline']['transforms']
    # Step 1: ReadFromKafka with Schema Registry
    self.assertEqual(chain[0]['type'], 'ReadFromKafka')
    self.assertEqual(
        chain[0]['config']['confluent_schema_registry_url'],
        'https://registry.prod:8081')
    self.assertEqual(chain[0]['config']['format'], 'AVRO')

    # Step 2: WindowInto fixed:60s
    self.assertEqual(chain[1]['type'], 'WindowInto')
    self.assertEqual(chain[1]['config']['windowing']['type'], 'fixed')
    self.assertEqual(chain[1]['config']['windowing']['size'], '60s')

    # Step 3: CliWriteToIceberg with derived triggering_frequency_seconds=60
    sink = chain[-1]
    self.assertEqual(sink['type'], 'CliWriteToIceberg')
    self.assertEqual(sink['config']['triggering_frequency_seconds'], 60)
    self.assertEqual(sink['config']['table'], 'analytics.telemetry')

  def test_build_pipe_spec_kafka_to_iceberg_with_sliding_window(self):
    pipe = pipe_spec.build_pipe_spec(
        'kafka://broker:9092/metrics',
        'iceberg://analytics.metrics?warehouse=gs://my-wh',
        window='sliding:5m,30s',
        prefer_python_lakehouse_and_cdc=False)

    spec = pipe.spec
    chain = spec['pipeline']['transforms']
    sink = chain[-1]
    self.assertEqual(sink['type'], 'WriteToIceberg')
    # Derived from sliding period '30s'
    self.assertEqual(sink['config']['triggering_frequency_seconds'], 30)

  def test_build_pipe_spec_explicit_triggering_frequency_overrides(self):
    pipe = pipe_spec.build_pipe_spec(
        'kafka://broker:9092/events',
        'iceberg://analytics.events?warehouse=gs://my-wh',
        window='fixed:5m',
        to_opts={'triggering_frequency_seconds': 15})

    sink = pipe.spec['pipeline']['transforms'][-1]
    self.assertEqual(sink['config']['triggering_frequency_seconds'], 15)

  def test_iceberg_biglake_catalog_properties(self):
    ep = uris.parse_endpoint(
        'iceberg://biglake.prod_analytics.events?warehouse=gs://my-wh',
        default_project='gcp-prod-data',
        is_read=False)
    cat_props = ep.config['catalog_properties']
    self.assertEqual(cat_props['type'], 'rest')
    self.assertEqual(
        cat_props['uri'],
        'https://biglake.googleapis.com/iceberg/v1/restcatalog')
    self.assertEqual(cat_props['warehouse'], 'gs://my-wh')
    self.assertEqual(cat_props['header.x-goog-user-project'], 'gcp-prod-data')

  def test_dataflow_runner_streaming_flags_injection(self):
    resolved = ['--runner=DataflowRunner', '--project=my-proj']
    pipe = pipe_spec.build_pipe_spec(
        'kafka://broker:9092/stream',
        'iceberg://analytics.stream?warehouse=gs://wh')
    self.assertTrue(pipe.is_unbounded)

    # Simulate main.py injection logic
    runner_norm = main._runner_name(main._extract_runner_from_args(resolved))
    self.assertEqual(runner_norm, 'dataflow')
    if pipe.is_unbounded and runner_norm == 'dataflow':
      if not any(a == '--streaming' or a.startswith('--streaming=') for a in resolved):
        resolved.append('--streaming')
      if not any(a in ('--enable_streaming_engine', '--enable-streaming-engine') for a in resolved):
        resolved.append('--enable_streaming_engine')

    self.assertIn('--streaming', resolved)
    self.assertIn('--enable_streaming_engine', resolved)

  def test_merge_on_read_cross_commit_out_of_order_cdc_and_user_seq_col(self):
    tmp = tempfile.mkdtemp(prefix='iceberg_mor_cdc_')
    self.addCleanup(shutil.rmtree, tmp, True)
    tbl_dir = os.path.join(tmp, 'orders_cdc')
    row_type = RowTypeConstraint.from_fields([
        ('id', int),
        ('val', Optional[str]),
        ('op', str),
        ('updated_at', str),
    ])

    # Commit 1: id=1 @ 12:00 ('v2'), id=2 DELETED @ 12:00.
    fn_opts = PipelineOptions(['--runner=FnApiRunner'])
    with beam.Pipeline(options=fn_opts) as p1:
      _ = (
          p1
          | beam.Create([
              beam.Row(
                  id=1, val='v2', op='UPSERT', updated_at='2026-11-10T12:00:00Z'
              ),
              beam.Row(
                  id=2,
                  val='gone',
                  op='DELETE',
                  updated_at='2026-11-10T12:00:00Z'),
          ]).with_output_types(row_type)
          | lakehouse.cli_write_to_iceberg(
              tbl_dir,
              mode='merge-on-read',
              equality_columns=['id'],
              change_type_column='op',
              sequence_number_column='updated_at',
              if_exists='append'))

    # Commit 2: stale id=1 @ 11:00, stale id=2 @ 11:00, fresh id=3 @ 13:00.
    with beam.Pipeline(options=fn_opts) as p2:
      _ = (
          p2
          | beam.Create([
              beam.Row(
                  id=1,
                  val='v1_stale',
                  op='UPSERT',
                  updated_at='2026-11-10T11:00:00Z'),
              beam.Row(
                  id=2,
                  val='resurrect_stale',
                  op='UPSERT',
                  updated_at='2026-11-10T11:00:00Z'),
              beam.Row(
                  id=3,
                  val='v3_fresh',
                  op='UPSERT',
                  updated_at='2026-11-10T13:00:00Z'),
          ]).with_output_types(row_type)
          | lakehouse.cli_write_to_iceberg(
              tbl_dir,
              mode='merge-on-read',
              equality_columns=['id'],
              change_type_column='op',
              sequence_number_column='updated_at',
              if_exists='append'))

    st = lakehouse.load_iceberg_state(tbl_dir)
    col_names = [f['name'] for f in st['schema']]
    self.assertIn('updated_at', col_names)
    self.assertNotIn('op', col_names)
    _, live = lakehouse.read_iceberg_rows_local(st)
    by_id = {r['id']: r for r in live}
    self.assertEqual(sorted(by_id.keys()), [1, 3])
    self.assertEqual(by_id[1]['val'], 'v2')
    self.assertEqual(by_id[1]['updated_at'], '2026-11-10T12:00:00Z')
    self.assertEqual(by_id[3]['val'], 'v3_fresh')

  def test_merge_on_read_rejects_null_primary_key(self):
    tmp = tempfile.mkdtemp(prefix='iceberg_mor_null_pk_')
    self.addCleanup(shutil.rmtree, tmp, True)
    tbl_dir = os.path.join(tmp, 'bad_pk')
    row_type = RowTypeConstraint.from_fields([
        ('id', Optional[int]),
        ('val', Optional[str]),
        ('beam_cdc_op', str),
        ('beam_cdc_seq', int),
    ])
    fn_opts = PipelineOptions(['--runner=FnApiRunner'])
    with self.assertRaises(Exception) as ctx:
      with beam.Pipeline(options=fn_opts) as p:
        _ = (
            p
            | beam.Create([
                beam.Row(
                    id=1, val='ok', beam_cdc_op='UPSERT', beam_cdc_seq=1),
                beam.Row(
                    id=None, val='x', beam_cdc_op='UPSERT', beam_cdc_seq=2),
            ]).with_output_types(row_type)
            | lakehouse.cli_write_to_iceberg(
                tbl_dir,
                mode='merge-on-read',
                equality_columns=['id'],
                if_exists='append'))
    self.assertIn('primary key', str(ctx.exception))

  def test_schema_evolution_parquet_field_ids_match_metadata_json(self):
    tmp = tempfile.mkdtemp(prefix='iceberg_evolve_fid_')
    self.addCleanup(shutil.rmtree, tmp, True)
    tbl_dir = os.path.join(tmp, 'evolve_tbl')
    fn_opts = PipelineOptions(['--runner=FnApiRunner'])

    rt1 = RowTypeConstraint.from_fields([('id', int), ('name', str)])
    with beam.Pipeline(options=fn_opts) as p1:
      _ = (
          p1
          | beam.Create([beam.Row(id=1, name='alice')]).with_output_types(rt1)
          | lakehouse.cli_write_to_iceberg(tbl_dir, if_exists='append'))

    # Second write omits 'name' and introduces 'extra_col' at positional idx 2:
    # 'extra_col' MUST get field_id=3 in BOTH Parquet footer AND v2.metadata.json!
    rt2 = RowTypeConstraint.from_fields([('id', int), ('extra_col', str)])
    with beam.Pipeline(options=fn_opts) as p2:
      _ = (
          p2
          | beam.Create([beam.Row(id=2, extra_col='bonus')]).with_output_types(
              rt2)
          | lakehouse.cli_write_to_iceberg(tbl_dir, if_exists='append'))

    st = lakehouse.load_iceberg_state(tbl_dir)
    meta_id_by_name = {f['name']: f['id'] for f in st['schema']}
    self.assertEqual(meta_id_by_name, {'id': 1, 'name': 2, 'extra_col': 3})

    # Inspect the evolved Parquet file written in snapshot 2
    latest_df = max(st['data_files'], key=lambda d: d['sequence_number'])
    pf = pq.ParquetFile(latest_df['path'])
    pq_fids = {
        field.name: int(field.metadata[b'PARQUET:field_id'].decode('ascii'))
        for field in pf.schema_arrow
    }
    self.assertEqual(pq_fids['id'], 1)
    self.assertEqual(pq_fids['extra_col'], 3)

  def test_if_exists_replace_persists_safely_across_deserialized_bundles(self):
    tmp = tempfile.mkdtemp(prefix='iceberg_replace_bundles_')
    self.addCleanup(shutil.rmtree, tmp, True)
    tbl_dir = os.path.join(tmp, 'rep_tbl')
    fields = [{'id': 1, 'name': 'id', 'type': 'long', 'nullable': False}]
    for i in (1, 2):
      df = lakehouse.write_parquet_with_field_ids(
          os.path.join(tbl_dir, 'data', f'pre{i}.parquet'), [{'id': i}], fields)
      lakehouse.commit_iceberg_snapshot(
          tbl_dir, iceberg_fields=fields, new_data_files=[df])

    # Construct transform at v2 with if_exists='replace'
    xform = lakehouse.cli_write_to_iceberg(tbl_dir, if_exists='replace')
    rt = RowTypeConstraint.from_fields([('id', int)])
    fn_opts = PipelineOptions(['--runner=FnApiRunner'])
    with beam.Pipeline(options=fn_opts) as p:
      pcoll = p | beam.Create([beam.Row(id=10)]).with_output_types(rt)
      _ = pcoll | xform

    # After replace pipeline (writes v3), a second run / bundle at >v2 appends!
    st_after_rep = lakehouse.load_iceberg_state(tbl_dir)
    self.assertEqual(st_after_rep['metadata_version'], 3)
    _, rows1 = lakehouse.read_iceberg_rows_local(st_after_rep)
    self.assertEqual([r['id'] for r in rows1], [10])

    # Re-running a deserialized copy of the SAME xform now sees cur_ver=3 > 2,
    # so a second window/bundle appends v4 instead of wiping id=10!
    xform_copy = pickler.loads(pickler.dumps(xform))
    with beam.Pipeline(options=fn_opts) as p2:
      pcoll2 = p2 | beam.Create([beam.Row(id=20)]).with_output_types(rt)
      _ = pcoll2 | xform_copy

    st_final = lakehouse.load_iceberg_state(tbl_dir)
    self.assertEqual(st_final['metadata_version'], 4)
    _, rows2 = lakehouse.read_iceberg_rows_local(st_final)
    self.assertEqual(sorted(r['id'] for r in rows2), [10, 20])


if __name__ == '__main__':
  unittest.main()
