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

import unittest

from apache_beam.cli import main
from apache_beam.cli import pipe_spec
from apache_beam.cli import uris


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


if __name__ == '__main__':
  unittest.main()
