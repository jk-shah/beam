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

"""Unit tests for `apache_beam.cli.uris`."""

import os
import shutil
import tempfile
import unittest
from unittest import mock

from apache_beam.cli import uris
from apache_beam.cli.uris import UriError


class UrisTest(unittest.TestCase):
  def test_rejects_inline_credentials_and_secret_cli_flags(self):
    bad_uris = [
        'postgres://admin:s3cr3t@db.example.com:5432/prod?table=users',
        'postgres://admin@db.example.com:5432/prod?table=users',
        'postgres://db.example.com:5432/prod?table=users&password=abc',
        'kafka://user:pass@broker:9092/events',
    ]
    for u in bad_uris:
      with self.subTest(uri=u):
        with self.assertRaisesRegex(UriError, 'not allowed'):
          uris.parse_endpoint(u, is_read=True)

    with self.assertRaisesRegex(UriError, 'refusing secret option'):
      uris.parse_kv_options(['password=secret123'], flag_name='--from-opt')

  def test_bigquery_uri_normalization_and_dispositions(self):
    ep_read = uris.parse_endpoint(
        'bigquery://my-proj.analytics.events', is_read=True)
    self.assertEqual(ep_read.yaml_type, 'ReadFromBigQuery')
    self.assertEqual(ep_read.config, {'table': 'my-proj:analytics.events'})
    self.assertFalse(ep_read.is_java_xlang)

    ep_write = uris.parse_endpoint(
        'bigquery://analytics.events',
        is_read=False,
        if_exists='replace',
        default_project='fallback-proj')
    self.assertEqual(ep_write.yaml_type, 'WriteToBigQuery')
    self.assertEqual(ep_write.config['table'], 'fallback-proj:analytics.events')
    self.assertEqual(ep_write.config['write_disposition'], 'WRITE_TRUNCATE')

  def test_postgres_pubsub_kafka_and_iceberg_uris(self):
    with mock.patch.dict(
        os.environ, {
            'BEAM_POSTGRES_USER': 'app_ro',
            'BEAM_POSTGRES_PASSWORD': 'env_secret',
        }):
      pg = uris.parse_endpoint(
          'postgres://127.0.0.1:5432/warehouse?table=public.orders&sslmode=require',
          is_read=True)
    self.assertEqual(pg.yaml_type, 'ReadFromPostgres')
    self.assertEqual(
        pg.config['url'], 'jdbc:postgresql://127.0.0.1:5432/warehouse?sslmode=require')
    self.assertEqual(pg.config['table'], 'public.orders')
    self.assertEqual(pg.config['username'], 'app_ro')
    self.assertEqual(pg.config['password'], 'env_secret')

    ps = uris.parse_endpoint(
        'pubsub://projects/p1/subscriptions/sub1', is_read=True)
    self.assertEqual(ps.yaml_type, 'ReadFromPubSub')
    self.assertEqual(ps.config['subscription'], 'projects/p1/subscriptions/sub1')
    self.assertTrue(ps.is_unbounded)

    kf = uris.parse_endpoint('kafka://localhost:9092/clicks', is_read=False)
    self.assertEqual(kf.yaml_type, 'WriteToKafka')
    self.assertEqual(kf.config['bootstrap_servers'], 'localhost:9092')
    self.assertEqual(kf.config['topic'], 'clicks')

  def test_magic_byte_sniffing_on_extensionless_shards(self):
    tmp = tempfile.mkdtemp()
    self.addCleanup(shutil.rmtree, tmp)
    shard = os.path.join(tmp, 'part-00000-of-00001')
    with open(shard, 'wb') as f:
      f.write(b'PAR1\x00\x00\x00\x00PAR1')
    ep = uris.parse_endpoint(shard, is_read=True, allow_sniff=True)
    self.assertEqual(ep.format, 'parquet')
    self.assertEqual(ep.yaml_type, 'ReadFromParquet')

  def test_log_extension_and_plain_text_sniffing(self):
    tmp = tempfile.mkdtemp()
    self.addCleanup(shutil.rmtree, tmp)

    log_ep = uris.parse_endpoint(
        os.path.join(tmp, 'service.log'), is_read=True, allow_sniff=True)
    self.assertEqual(log_ep.format, 'text')
    self.assertEqual(log_ep.yaml_type, 'ReadFromText')

    syslog = os.path.join(tmp, 'messages')
    with open(syslog, 'w', encoding='utf-8') as f:
      f.write('Nov 12 14:40:00 host kernel: [12.34] initialized\n')
    text_ep = uris.parse_endpoint(syslog, is_read=True, allow_sniff=True)
    self.assertEqual(text_ep.format, 'text')
    self.assertEqual(text_ep.yaml_type, 'ReadFromText')

    binary_blob = os.path.join(tmp, 'raw_binary')
    with open(binary_blob, 'wb') as f:
      f.write(b'\x7fELF\x02\x01\x01\x00\x00\x00')
    with self.assertRaisesRegex(UriError, 'cannot tell the format'):
      uris.parse_endpoint(binary_blob, is_read=True, allow_sniff=True)


if __name__ == '__main__':
  unittest.main()
