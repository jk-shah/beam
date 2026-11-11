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

"""Zero-SDK-import Beam YAML & URI connector catalog (`beam io`)."""

from pathlib import Path
import re
from typing import Any

_CATALOG: list[dict[str, Any]] = [
    {
        'name': 'csv',
        'uri_example': 'data/users.csv  or  gs://bucket/users.csv',
        'read_transform': 'ReadFromCsv',
        'write_transform': 'WriteToCsv',
        'runtime': 'python',
        'supports_dlq': True,
        'options': [
            'path',
            'delimiter',
            'null_values',
            'fields',
            'comment',
            'filename_column',
        ],
        'description': (
            'RFC-4180 CSV files on local or cloud filesystems '
            '(--from-opt fields=col:type overrides inferred column types).'),
    },
    {
        'name': 'json',
        'uri_example': 'data/events.jsonl  or  gs://bucket/events.json*',
        'read_transform': 'ReadFromJson',
        'write_transform': 'WriteToJson',
        'runtime': 'python',
        'supports_dlq': True,
        'options': [
            'path',
            'sample_rows',
            'fields',
            'on_unknown_fields',
            'key',
        ],
        'description': (
            'Newline-delimited JSON objects (JSONL) by default; pass '
            '--to-opt key=<col> on local runs to write a single JSON object '
            'indexed by <col>.'),
    },
    {
        'name': 'text',
        'uri_example': 'data/logs.txt  or  gs://bucket/logs/*.txt',
        'read_transform': 'ReadFromText',
        'write_transform': 'WriteToText',
        'runtime': 'python',
        'supports_dlq': False,
        'options': ['path'],
        'description': (
            'Line-delimited plain text files emitting Row(line: str); pair '
            'with --select or --sql to project structured columns into '
            'BigQuery or Parquet.'),
    },
    {
        'name': 'parquet',
        'uri_example': 'data/events.parquet  or  s3://bucket/events/*.parquet',
        'read_transform': 'ReadFromParquet',
        'write_transform': 'WriteToParquet',
        'runtime': 'python (pyarrow)',
        'supports_dlq': False,
        'options': ['path'],
        'description': 'Columnar Apache Parquet files.',
    },
    {
        'name': 'avro',
        'uri_example': 'data/events.avro',
        'read_transform': 'ReadFromAvro',
        'write_transform': 'WriteToAvro',
        'runtime': 'python (fastavro)',
        'supports_dlq': False,
        'options': ['path'],
        'description': 'Apache Avro container files.',
    },
    {
        'name': 'bigquery',
        'uri_example': 'bigquery://PROJECT.DATASET.TABLE',
        'read_transform': 'ReadFromBigQuery',
        'write_transform': 'WriteToBigQuery',
        'cdc_write_transform': 'WriteToBigQueryCDC',
        'runtime': 'python (Storage Read / Storage Write CDC / File Loads) or java xlang',
        'supports_dlq': True,
        'supports_cdc': True,
        'options': [
            'table',
            'query',
            'row_restriction',
            'fields',
            'create_disposition',
            'write_disposition',
            'primary_key',
        ],
        'description': (
            'Google Cloud BigQuery tables, views, SQL queries, and Storage '
            'Write API UPSERT/DELETE CDC (--cdc --primary-key).'),
    },
    {
        'name': 'postgres',
        'uri_example': 'postgres://HOST:5432/DB?table=public.users',
        'read_transform': 'ReadFromPostgres',
        'cdc_read_transform': 'ReadFromDebezium',
        'write_transform': 'WriteToPostgres',
        'runtime': 'java xlang (JDBC / Debezium)',
        'supports_dlq': False,
        'supports_cdc': True,
        'options': [
            'url',
            'table',
            'query',
            'partition_column',
            'num_partitions',
            'batch_size',
            'connection_properties',
        ],
        'auth': 'BEAM_POSTGRES_USER / BEAM_POSTGRES_PASSWORD or --*-opt-file',
        'description': 'PostgreSQL / Cloud SQL tables (batch JDBC & Debezium WAL --cdc).',
    },
    {
        'name': 'mysql',
        'uri_example': 'mysql://HOST:3306/DB?table=orders',
        'read_transform': 'ReadFromMySql',
        'cdc_read_transform': 'ReadFromDebezium',
        'write_transform': 'WriteToMySql',
        'runtime': 'java xlang (JDBC / Debezium)',
        'supports_dlq': False,
        'supports_cdc': True,
        'options': [
            'url',
            'table',
            'query',
            'connection_properties',
        ],
        'auth': 'BEAM_MYSQL_USER / BEAM_MYSQL_PASSWORD or --*-opt-file',
        'description': 'MySQL / Cloud SQL tables (batch JDBC & Debezium binlog --cdc).',
    },
    {
        'name': 'spanner',
        'uri_example': 'spanner://PROJECT/INSTANCE/DATABASE?change_stream=orders_cs',
        'read_transform': 'ReadFromSpanner',
        'cdc_read_transform': 'ReadFromSpannerCDC',
        'write_transform': 'WriteToSpanner',
        'runtime': 'java xlang (Spanner / Change Streams)',
        'supports_dlq': False,
        'supports_cdc': True,
        'options': [
            'project_id',
            'instance_id',
            'database_id',
            'table',
            'change_stream_name',
            'metadata_instance',
            'metadata_database',
        ],
        'description': 'Google Cloud Spanner tables and Change Streams (--cdc).',
    },
    {
        'name': 'pubsub',
        'uri_example': 'pubsub://projects/PROJECT/subscriptions/SUB',
        'read_transform': 'ReadFromPubSub',
        'write_transform': 'WriteToPubSub',
        'runtime': 'python',
        'supports_dlq': True,
        'supports_cdc': True,
        'options': [
            'topic',
            'subscription',
            'format',
            'schema',
            'attributes',
            'id_attribute',
            'timestamp_attribute',
        ],
        'description': 'Google Cloud Pub/Sub streaming topics, subscriptions & Debezium CDC.',
    },
    {
        'name': 'kafka',
        'uri_example': 'kafka://BOOTSTRAP_HOST:9092/TOPIC',
        'read_transform': 'ReadFromKafka',
        'write_transform': 'WriteToKafka',
        'runtime': 'java xlang',
        'supports_dlq': True,
        'supports_cdc': True,
        'options': [
            'bootstrap_servers',
            'topic',
            'format',
            'schema',
            'auto_offset_reset_config',
            'consumer_config',
        ],
        'description': 'Apache Kafka streaming topics & Debezium CDC envelopes.',
    },
    {
        'name': 'iceberg',
        'uri_example': 'iceberg://NAMESPACE.TABLE?warehouse=gs://BUCKET/wh',
        'read_transform': 'ReadFromIceberg',
        'cdc_read_transform': 'ReadFromIcebergCDC',
        'write_transform': 'WriteToIceberg',
        'zero_copy_transform': 'IcebergAddFiles',
        'runtime': 'python (zero-JVM v2 & zero-copy) / java xlang / BigLake REST',
        'supports_dlq': False,
        'supports_cdc': True,
        'supports_zero_copy': True,
        'options': [
            'table',
            'warehouse',
            'catalog',
            'catalog_name',
            'catalog_properties',
            'config_properties',
            'mode',
            'equality_columns',
        ],
        'description': (
            'Apache Iceberg v2 lakehouse tables (append, merge-on-read --cdc, '
            'BigLake/REST catalogs, and <100ms --zero-copy Parquet/Delta registration).'),
    },
    {
        'name': 'delta',
        'uri_example': 'delta://gs://BUCKET/tables/orders  or  delta:///path/to/table',
        'read_transform': 'ReadFromDelta',
        'cdc_read_transform': 'ReadFromDeltaCDC',
        'write_transform': 'WriteToParquet',
        'runtime': 'python (zero-JVM _delta_log + pyarrow)',
        'supports_dlq': False,
        'supports_cdc': True,
        'options': [
            'table',
            'version',
            'start_version',
            'end_version',
        ],
        'description': (
            'Delta Lake tables (snapshot reads, time-travel ?version=N, '
            'Change Data Feed --cdc, and --zero-copy bridge to iceberg://).'),
    },
]


def list_connectors() -> list[dict[str, Any]]:
  """Returns all first-class `beam` connectors without importing `apache_beam`."""
  # Also discover any additional transforms declared in `standard_io.yaml` if
  # present on disk alongside the CLI package.
  discovered = list(_CATALOG)
  known_transforms = {
      item['read_transform']
      for item in _CATALOG
  } | {item['write_transform'] for item in _CATALOG}
  std_yaml = Path(__file__).resolve().parents[1] / 'yaml' / 'standard_io.yaml'
  extra_transforms: list[str] = []
  if std_yaml.is_file():
    for m in re.finditer(
        r"^\s*'(ReadFrom[A-Za-z0-9_]+|WriteTo[A-Za-z0-9_]+)'\s*:",
        std_yaml.read_text(encoding='utf-8'),
        re.MULTILINE):
      tname = m.group(1)
      if tname not in known_transforms and tname not in extra_transforms:
        extra_transforms.append(tname)
  if extra_transforms:
    discovered.append({
        'name': 'yaml-standard-io-extras',
        'uri_example': 'beam run pipeline.yaml',
        'read_transform': ', '.join(
            sorted(t for t in extra_transforms if t.startswith('ReadFrom'))),
        'write_transform': ', '.join(
            sorted(t for t in extra_transforms if t.startswith('WriteTo'))),
        'runtime': 'yaml standard_io',
        'supports_dlq': False,
        'options': [],
        'description': 'Additional Beam YAML transforms available in beam run.',
    })
  return discovered


def describe_connector(name: str) -> dict[str, Any]:
  key = name.strip().lower()
  for item in _CATALOG:
    if (item['name'] == key or item['read_transform'].lower() == key or
        item['write_transform'].lower() == key):
      return item
  transform = describe_transform(name)
  if transform is not None:
    return transform
  valid = ', '.join(item['name'] for item in _CATALOG)
  raise ValueError(
      f'unknown connector or transform {name!r}; connectors: {valid}; '
      'transforms: Beam YAML names such as Filter, MapToFields, Sql, '
      'Combine, Join, WindowInto')


def describe_transform(name: str) -> dict[str, Any] | None:
  """Describes a Beam YAML transform (e.g. Filter) for `beam io describe`.

  Only transforms implemented inline in Python are introspected, because
  asking a Java-backed provider for its schema can download expansion jars.
  Returns None if `name` is not a Beam YAML transform.
  """
  from apache_beam.yaml import yaml_provider
  providers = yaml_provider.standard_providers()
  wanted = name.strip().lower()
  # Language-specific transforms are registered as e.g. 'Filter-python'.
  keys = [t for t in providers if t.lower().split('-')[0] == wanted]
  if not keys:
    return None
  match = keys[0].split('-')[0]
  inline = [
      (t, p) for t in sorted(keys, key=lambda k: (k != match, k != f'{match}-python'))
      for p in providers.get(t, [])
      if isinstance(p, yaml_provider.InlineProvider)
  ]
  info: dict[str, Any] = {
      'name': match,
      'kind': 'yaml_transform',
      'docs': 'https://beam.apache.org/documentation/sdks/yaml/',
      'config_fields': [],
      'description': '',
  }
  if inline:
    t, provider = inline[0]
    try:
      schema = provider.config_schema(t)
      info['config_fields'] = [f.name for f in schema.fields] if schema else []
      info['description'] = (provider.description(t) or '').strip()
    except Exception:  # pylint: disable=broad-except
      pass
  return info
