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

import unittest

import yaml

from apache_beam.cli import pipe_spec
from apache_beam.cli.pipe_spec import PipeSpecError
from apache_beam.cli.pipe_spec import build_pipe_spec

CSV_TO_PARQUET = '''
pipeline:
  type: chain
  transforms:
    - type: ReadFromCsv
      config: {path: users.csv}
    - type: WriteToParquet
      config: {path: out/users.parquet}
'''


class BuildPipeSpecTest(unittest.TestCase):
  def test_csv_to_parquet(self):
    pipe = build_pipe_spec('users.csv', 'out/users.parquet')
    self.assertEqual(pipe.spec, yaml.safe_load(CSV_TO_PARQUET))
    self.assertEqual(pipe.output_prefix, 'out/users.parquet')

  def test_formats_come_from_extensions(self):
    for path, read_transform, write_transform in [
        ('a.csv', 'ReadFromCsv', 'WriteToCsv'),
        ('logs/*.json', 'ReadFromJson', 'WriteToJson'),
        ('a.jsonl', 'ReadFromJson', 'WriteToJson'),
        ('gs://bucket/a.parquet', 'ReadFromParquet', 'WriteToParquet'),
        ('GS://bucket/a.parquet', 'ReadFromParquet', 'WriteToParquet'),
        ('a.AVRO', 'ReadFromAvro', 'WriteToAvro'),
        ('out/users.parquet*', 'ReadFromParquet', 'WriteToParquet'),
        ('out/users.avro-*', 'ReadFromAvro', 'WriteToAvro'),
        ('out/users.json-00000-of-00001', 'ReadFromJson', 'WriteToJson'),
        (r'C:\Users\Ada\users.csv', 'ReadFromCsv', 'WriteToCsv'),
        (
            r'C:\Users\Ada\out\users.parquet*',
            'ReadFromParquet',
            'WriteToParquet'),
    ]:
      with self.subTest(path=path):
        pipe = build_pipe_spec(path, path)
        self.assertEqual(
            [t['type'] for t in pipe.spec['pipeline']['transforms']],
            [read_transform, write_transform])

  def test_explicit_formats_override_extensions(self):
    pipe = build_pipe_spec('data.txt', 'out', 'csv', 'json')
    self.assertEqual([t['type'] for t in pipe.spec['pipeline']['transforms']],
                     ['ReadFromCsv', 'WriteToJson'])

  def test_universal_uris_and_where_select_dlq(self):
    pipe = build_pipe_spec(
        'users.csv',
        'bigquery://my-proj.analytics.users',
        where="age >= 18 AND status != 'banned'",
        select='id, lower(email) AS email, age',
        dlq_sink='dlq/bad_rows.json',
        if_exists='append',
        include_counters=True,
        prefer_python_bigquery_sink=True)
    names = [t['name'] for t in pipe.spec['pipeline']['transforms']]
    self.assertIn('ReadSource', names)
    self.assertIn('FilterWhere', names)
    self.assertIn('SelectFields', names)
    self.assertIn('MergeDlq', names)
    self.assertIn('WriteDlq', names)
    self.assertIsNone(pipe.output_prefix)

  def test_errors_name_the_fix(self):
    cases = [
        (('users', 'out.csv'), {}, 'add --from-format'),
        ((r'C:\Users\ada.smith\users', 'out.csv'), {}, 'add --from-format'),
        (('a.csv', 'out/users'), {}, 'add --to-format'),
        (('a.csv', 'b.csv'), {
            'from_format': 'xml'
        },
         "unsupported format 'xml' .* use one of: csv, json, parquet, avro"),
        (('a.csv', 'b.csv'), {
            'to_format': 'xml'
        },
         "unsupported format 'xml' .* use one of: csv, json, parquet, avro"),
        (('ftp://example.com/a.csv', 'b.csv'), {}, 'unsupported location'),
        (('postgres://user:pass@host/db?table=t', 'b.csv'), {}, 'not allowed'),
        (('a.csv', 'b.csv'), {'if_exists': 'append'}, '--if-exists=append'),
    ]
    for args, kwargs, message in cases:
      with self.subTest(args=args, kwargs=kwargs):
        with self.assertRaisesRegex(PipeSpecError, message):
          build_pipe_spec(*args, **kwargs)

  def test_parse_window_spec(self):
    self.assertEqual(
        pipe_spec.parse_window_spec('fixed:60s'),
        {'type': 'fixed', 'size': '60s'})
    self.assertEqual(
        pipe_spec.parse_window_spec('60s'),
        {'type': 'fixed', 'size': '60s'})
    self.assertEqual(
        pipe_spec.parse_window_spec('sliding:5m,1m'),
        {'type': 'sliding', 'size': '5m', 'period': '1m'})
    self.assertEqual(
        pipe_spec.parse_window_spec('session:30m'),
        {'type': 'sessions', 'gap': '30m'})
    self.assertEqual(
        pipe_spec.parse_window_spec('sessions:5s'),
        {'type': 'sessions', 'gap': '5s'})
    self.assertEqual(
        pipe_spec.parse_window_spec('type=fixed,size=30s'),
        {'type': 'fixed', 'size': '30s'})
    self.assertEqual(
        pipe_spec.parse_window_spec({'type': 'session', 'gap': '10m'}),
        {'type': 'sessions', 'gap': '10m'})
    with self.assertRaisesRegex(PipeSpecError, 'cannot be empty'):
      pipe_spec.parse_window_spec('')
    with self.assertRaisesRegex(PipeSpecError, 'invalid sliding'):
      pipe_spec.parse_window_spec('sliding:5m')

  def test_window_and_timestamp_col_chain(self):
    pipe = build_pipe_spec(
        'users.csv',
        'out/users.parquet',
        window='fixed:60s',
        timestamp_col='event_time')
    transforms = pipe.spec['pipeline']['transforms']
    types = [t['type'] for t in transforms]
    self.assertIn('AssignTimestamps', types)
    self.assertIn('WindowInto', types)
    assign_step = next(t for t in transforms if t['type'] == 'AssignTimestamps')
    self.assertEqual(assign_step['config']['timestamp'], 'event_time')
    window_step = next(t for t in transforms if t['type'] == 'WindowInto')
    self.assertEqual(window_step['config']['windowing'], {'type': 'fixed', 'size': '60s'})

  def test_window_and_timestamp_col_dag(self):
    pipe = build_pipe_spec(
        'users.csv',
        'bigquery://my-proj.analytics.users',
        dlq_sink='dlq/bad_rows.json',
        window='sliding:5m,1m',
        timestamp_col='ts')
    transforms = pipe.spec['pipeline']['transforms']
    names = [t['name'] for t in transforms]
    self.assertIn('AssignTimestamps', names)
    self.assertIn('WindowInto', names)
    assign_step = next(t for t in transforms if t['name'] == 'AssignTimestamps')
    self.assertEqual(assign_step['config']['timestamp'], 'ts')
    window_step = next(t for t in transforms if t['name'] == 'WindowInto')
    self.assertEqual(
        window_step['config']['windowing'],
        {'type': 'sliding', 'size': '5m', 'period': '1m'})

  def test_zero_copy_rejects_window(self):
    with self.assertRaisesRegex(PipeSpecError, 'cannot be combined with .* --window'):
      build_pipe_spec(
          'data/*.parquet',
          'iceberg://lake.db.table',
          zero_copy=True,
          window='fixed:60s')


class ToYamlTest(unittest.TestCase):
  def test_starts_with_the_command_and_round_trips(self):
    for src, dst in [
        ('users.csv', 'out/users.avro'),
        (
            r'C:\Users\runneradmin\AppData\Local\Temp\tmp123\users.csv',
            r'C:\Users\runneradmin\AppData\Local\Temp\tmp123\out\users.json',
        ),
    ]:
      with self.subTest(src=src):
        pipe = build_pipe_spec(src, dst)
        text = pipe_spec.to_yaml(pipe.spec, ['--from', src, '--to', dst])
        self.assertTrue(text.startswith('# Generated by: beam pipe --from '))
        self.assertEqual(yaml.safe_load(text), pipe.spec)


if __name__ == '__main__':
  unittest.main()
