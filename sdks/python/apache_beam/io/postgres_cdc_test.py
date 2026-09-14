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
import unittest
from unittest import mock

from apache_beam.io import postgres_cdc
from apache_beam.transforms import external


class PostgresCdcTest(unittest.TestCase):

  def test_urn_constants(self):
    self.assertEqual(
        postgres_cdc.POSTGRES_WRITE_URN,
        'beam:schematransform:org.apache.beam:postgres_write:v1')
    self.assertEqual(
        postgres_cdc.POSTGRES_CDC_READ_URN,
        'beam:schematransform:org.apache.beam:postgres_read_cdc:v1')
    self.assertNotEqual(
        postgres_cdc.POSTGRES_WRITE_URN,
        postgres_cdc.POSTGRES_CDC_READ_URN)

  def test_resolver_explicit_endpoint(self):
    svc = postgres_cdc.default_expansion_service('localhost:54321')
    self.assertEqual(svc, 'localhost:54321')

  def test_resolver_explicit_object(self):
    mock_service = mock.MagicMock()
    svc = postgres_cdc.default_expansion_service(mock_service)
    self.assertEqual(svc, mock_service)

  @mock.patch.dict(os.environ, {'BEAM_GO_EXPANSION_SERVICE': '127.0.0.1:40999'}, clear=True)
  def test_resolver_env_endpoint(self):
    svc = postgres_cdc.default_expansion_service()
    self.assertEqual(svc, '127.0.0.1:40999')

  def test_resolver_env_binary(self):
    fake_bin = '/tmp/fake-beam-go-expansion-service'
    with mock.patch.dict(os.environ, {'BEAM_GO_EXPANSION_SERVICE': fake_bin}), \
         mock.patch('os.path.exists', side_effect=lambda p: p == fake_bin):
      svc = postgres_cdc.default_expansion_service()
      self.assertIsInstance(svc, external.GoBinaryExpansionService)
      self.assertEqual(svc.path_to_binary, fake_bin)

  def test_resolver_sibling_binary(self):
    fake_which = '/usr/local/bin/beam-go-expansion-service'
    with mock.patch.dict(os.environ, {}, clear=True), \
         mock.patch('shutil.which', side_effect=lambda name: fake_which if name == 'beam-go-expansion-service' else None), \
         mock.patch('os.path.exists', side_effect=lambda p: p == fake_which):
      svc = postgres_cdc.default_expansion_service()
      self.assertIsInstance(svc, external.GoBinaryExpansionService)
      self.assertEqual(svc.path_to_binary, fake_which)

  def test_resolver_all_tiers_fail_raises(self):
    with mock.patch.dict(os.environ, {}, clear=True), \
         mock.patch('shutil.which', return_value=None), \
         mock.patch('os.path.exists', return_value=False):
      with self.assertRaises(postgres_cdc.ExpansionServiceNotFoundError) as ctx:
        postgres_cdc.default_expansion_service(allow_build=False)
      msg = str(ctx.exception)
      self.assertIn('Unable to locate or build the Apache Beam Go expansion service', msg)
      self.assertIn('BEAM_GO_EXPANSION_SERVICE', msg)

  def test_read_transform_construction(self):
    tf = postgres_cdc.ReadFromPostgresCDC(
        host='db.example.com',
        database='shop',
        slot_name='test_slot',
        publication='test_pub',
        username='cdc_user',
        password_env_var='SECRET_VAR',
        output_format='arrow',
        arrow_batch_rows=2048,
        expansion_service='localhost:9999',
    )
    self.assertEqual(tf._config['host'], 'db.example.com')
    self.assertEqual(tf._config['database'], 'shop')
    self.assertEqual(tf._config['slot_name'], 'test_slot')
    self.assertEqual(tf._config['publication'], 'test_pub')
    self.assertEqual(tf._config['username'], 'cdc_user')
    self.assertEqual(tf._config['password_env_var'], 'SECRET_VAR')
    self.assertEqual(tf._config['output_format'], 'arrow')
    self.assertEqual(tf._config['arrow_batch_rows'], 2048)
    self.assertEqual(tf._expansion_service, 'localhost:9999')

    # Test alias identity
    self.assertIs(postgres_cdc.ReadFromPostgresCdc, postgres_cdc.ReadFromPostgresCDC)
    self.assertIs(postgres_cdc.ReadFromPostgres, postgres_cdc.ReadFromPostgresCDC)

  def test_write_transform_construction(self):
    tf = postgres_cdc.WriteToPostgres(
        host='db.example.com',
        database='shop',
        table='public.orders',
        username='writer_user',
        password_env_var='SECRET_VAR',
        conflict_keys=['id'],
        update_fields=['status'],
        use_pgbouncer=True,
        expansion_service='localhost:9999',
    )
    self.assertEqual(tf._config['host'], 'db.example.com')
    self.assertEqual(tf._config['database'], 'shop')
    self.assertEqual(tf._config['table'], 'public.orders')
    self.assertEqual(tf._config['username'], 'writer_user')
    self.assertEqual(tf._config['password_env_var'], 'SECRET_VAR')
    self.assertEqual(tf._config['conflict_keys'], ['id'])
    self.assertEqual(tf._config['update_fields'], ['status'])
    self.assertTrue(tf._config['use_pgbouncer'])
    self.assertEqual(tf._expansion_service, 'localhost:9999')

    # Test alias identity
    self.assertIs(postgres_cdc.WriteToPostgresBulk, postgres_cdc.WriteToPostgres)

  def test_write_result_dlq_properties(self):
    mock_dict = {'output': 'success_col', 'errors': 'failed_col'}
    res = postgres_cdc.PostgreSqlWriteResult(mock_dict)
    self.assertEqual(res.successful_rows, 'success_col')
    self.assertEqual(res.failed_rows, 'failed_col')

    # Single output fallback
    res_single = postgres_cdc.PostgreSqlWriteResult('single_col')
    self.assertEqual(res_single.successful_rows, 'single_col')
    self.assertIsNone(res_single.failed_rows)


if __name__ == '__main__':
  unittest.main()
