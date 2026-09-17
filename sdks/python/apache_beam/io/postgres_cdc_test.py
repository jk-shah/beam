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

import inspect
import os
import re
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
    self.assertEqual(
        postgres_cdc.POSTGRES_READ_URN,
        'beam:schematransform:org.apache.beam:postgres_read:v1')
    self.assertNotEqual(
        postgres_cdc.POSTGRES_WRITE_URN, postgres_cdc.POSTGRES_CDC_READ_URN)
    self.assertNotEqual(
        postgres_cdc.POSTGRES_READ_URN, postgres_cdc.POSTGRES_CDC_READ_URN)

  def test_resolver_explicit_endpoint(self):
    svc = postgres_cdc.default_expansion_service('localhost:54321')
    self.assertEqual(svc, 'localhost:54321')

  def test_resolver_explicit_object(self):
    mock_service = mock.MagicMock()
    svc = postgres_cdc.default_expansion_service(mock_service)
    self.assertEqual(svc, mock_service)

  @mock.patch.dict(
      os.environ, {'BEAM_GO_EXPANSION_SERVICE': '127.0.0.1:40999'}, clear=True)
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
      self.assertIn(
          'Unable to locate or build the Apache Beam Go expansion service', msg)
      self.assertIn('BEAM_GO_EXPANSION_SERVICE', msg)

  def test_resolver_does_not_compile_by_default(self):
    """Applying a PTransform must not invoke a compiler unless asked to.

    The assertion is on subprocess.run rather than on the outcome, because a
    build that merely fails is still a build: it needs a toolchain, it can take
    tens of seconds, and it happens during graph construction.
    """
    with mock.patch.dict(os.environ, {}, clear=True), \
         mock.patch('shutil.which', return_value='/usr/bin/go'), \
         mock.patch('os.path.exists', return_value=False), \
         mock.patch('subprocess.run') as run:
      with self.assertRaises(postgres_cdc.ExpansionServiceNotFoundError) as ctx:
        postgres_cdc.default_expansion_service()
      run.assert_not_called()
      msg = str(ctx.exception)
      self.assertIn('not attempted', msg)
      self.assertIn(postgres_cdc._ALLOW_BUILD_ENV_VAR, msg)

  def test_resolver_compiles_only_when_opted_in(self):
    """Tier 5 is reachable here; the opt-in is the only thing that varies.

    Holding the Go toolchain and the source checkout present is what makes the
    default meaningful. It shows the compiler is skipped by choice rather than
    because the preconditions happened to be missing.
    """
    repo_root = os.path.join(os.sep, 'fake', 'beam')
    cmd_dir = os.path.join(
        repo_root, 'sdks', 'go', 'cmd', 'beam-go-expansion-service')

    def resolve(env):
      with mock.patch.dict(os.environ, env, clear=True), \
           mock.patch('shutil.which',
                      side_effect=lambda n: '/usr/bin/go' if n == 'go' else None), \
           mock.patch.object(postgres_cdc, '_find_beam_repo_root',
                             return_value=repo_root), \
           mock.patch('os.path.exists', side_effect=lambda p: p == cmd_dir), \
           mock.patch('os.makedirs'), \
           mock.patch('subprocess.run') as run:
        try:
          postgres_cdc.default_expansion_service()
        except postgres_cdc.ExpansionServiceNotFoundError:
          pass
        return run

    env_var = postgres_cdc._ALLOW_BUILD_ENV_VAR
    for env, want_build in (
        ({}, False),
        ({env_var: '1'}, True),
        ({env_var: 'true'}, True),
        ({env_var: 'ON'}, True),
        ({env_var: '0'}, False),
        ({env_var: ''}, False),
        ({env_var: 'maybe'}, False),
    ):
      with self.subTest(env=env):
        run = resolve(env)
        self.assertEqual(
            run.called,
            want_build,
            f'subprocess.run called={run.called} with env {env}, '
            f'expected {want_build}')
        if want_build:
          argv = run.call_args.args[0]
          self.assertEqual(argv[:2], ['/usr/bin/go', 'build'])

  def test_resolver_explicit_allow_build_overrides_environment(self):
    with mock.patch.dict(
        os.environ, {postgres_cdc._ALLOW_BUILD_ENV_VAR: '1'}, clear=True), \
         mock.patch('shutil.which', return_value=None), \
         mock.patch('os.path.exists', return_value=False):
      with self.assertRaises(postgres_cdc.ExpansionServiceNotFoundError) as ctx:
        postgres_cdc.default_expansion_service(allow_build=False)
      self.assertIn('not attempted', str(ctx.exception))

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
    self.assertIs(
        postgres_cdc.ReadFromPostgresCdc, postgres_cdc.ReadFromPostgresCDC)

  def test_batch_read_transform_construction(self):
    tf = postgres_cdc.ReadFromPostgres(
        host='db.example.com',
        database='shop',
        table='public.orders',
        username='batch_reader',
        password_env_var='SECRET_VAR',
        partition_column='order_id',
        num_partitions=8,
        fetch_size=10000,
        expansion_service='localhost:9999',
    )
    self.assertEqual(tf._config['host'], 'db.example.com')
    self.assertEqual(tf._config['database'], 'shop')
    self.assertEqual(tf._config['table'], 'public.orders')
    self.assertEqual(tf._config['username'], 'batch_reader')
    self.assertEqual(tf._config['password_env_var'], 'SECRET_VAR')
    self.assertEqual(tf._config['partition_column'], 'order_id')
    self.assertEqual(tf._config['num_partitions'], 8)
    self.assertEqual(tf._config['fetch_size'], 10000)
    self.assertEqual(tf._expansion_service, 'localhost:9999')

    # Test alias identity
    self.assertIs(
        postgres_cdc.ReadFromPostgresBatch, postgres_cdc.ReadFromPostgres)

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
    self.assertIs(
        postgres_cdc.WriteToPostgresBulk, postgres_cdc.WriteToPostgres)

  def test_write_result_dlq_properties(self):
    mock_dict = {'output': 'success_col', 'errors': 'failed_col'}
    res = postgres_cdc.PostgreSqlWriteResult(mock_dict)
    self.assertEqual(res.successful_rows, 'success_col')
    self.assertEqual(res.failed_rows, 'failed_col')

    # Single output fallback
    res_single = postgres_cdc.PostgreSqlWriteResult('single_col')
    self.assertEqual(res_single.successful_rows, 'single_col')
    self.assertIsNone(res_single.failed_rows)

  def test_write_merge_mode_construction(self):
    """MERGE-mode CDC replication requires op_column and delete_op_value."""
    tf = postgres_cdc.WriteToPostgres(
        host='db.example.com',
        database='shop',
        table='public.customer_accounts',
        username='writer_user',
        password_env_var='SECRET_VAR',
        conflict_keys=['account_id'],
        write_mode='MERGE',
        op_column='_op_type',
        delete_op_value='d',
        explain_analyze=True,
        expansion_service='localhost:9999',
    )
    self.assertEqual(tf._config['write_mode'], 'MERGE')
    self.assertEqual(tf._config['op_column'], '_op_type')
    self.assertEqual(tf._config['delete_op_value'], 'd')
    self.assertTrue(tf._config['explain_analyze'])

    # Unset optional parameters must not be sent, so the Go-side defaults hold.
    minimal = postgres_cdc.WriteToPostgres(
        host='db.example.com',
        database='shop',
        table='public.orders',
        username='writer_user',
        expansion_service='localhost:9999',
    )
    self.assertNotIn('write_mode', minimal._config)
    self.assertNotIn('op_column', minimal._config)
    self.assertNotIn('delete_op_value', minimal._config)
    self.assertNotIn('explain_analyze', minimal._config)

  def test_cdc_failover_slot_and_publication_tables_construction(self):
    publication_tables = [{
        'table_name': 'public.customer_accounts',
        'columns': ['account_id', 'region'],
        'row_filter': "region = 'US'",
    }]
    tf = postgres_cdc.ReadFromPostgresCDC(
        host='db.example.com',
        database='shop',
        slot_name='test_slot',
        publication='test_pub',
        username='cdc_user',
        failover_slot=True,
        publication_tables=publication_tables,
        expansion_service='localhost:9999',
    )
    self.assertTrue(tf._config['failover_slot'])
    self.assertEqual(tf._config['publication_tables'], publication_tables)

    minimal = postgres_cdc.ReadFromPostgresCDC(
        host='db.example.com',
        database='shop',
        slot_name='test_slot',
        publication='test_pub',
        username='cdc_user',
        expansion_service='localhost:9999',
    )
    self.assertNotIn('failover_slot', minimal._config)
    self.assertNotIn('publication_tables', minimal._config)

  def test_io_aliases(self):
    self.assertIs(
        postgres_cdc.ReadFromPostgresIO, postgres_cdc.ReadFromPostgres)
    self.assertIs(postgres_cdc.WriteToPostgresIO, postgres_cdc.WriteToPostgres)


# Maps each Go SchemaTransform config struct to the Python wrapper that must be
# able to express every field it declares.
_GO_CONFIG_TO_PYTHON_TRANSFORM = {
    'PostgreSqlWriteConfig': 'WriteToPostgres',
    'PostgreSqlReadCDCConfig': 'ReadFromPostgresCDC',
    'PostgreSqlReadConfig': 'ReadFromPostgres',
}

_GO_STRUCT_RE = re.compile(
    r'^type\s+(?P<name>\w+)\s+struct\s*\{(?P<body>.*?)^\}', re.M | re.S)
_BEAM_TAG_RE = re.compile(r'beam:"(?P<field>[a-z0-9_]+)')


def _go_schematransform_source():
  """Returns the Go SchemaTransform sources, or None outside a source checkout.

  The config structs are spread across schematransform.go and
  schematransform_cdc.go, so every non-test file in that family is
  concatenated rather than naming one of them.
  """
  root = postgres_cdc._find_beam_repo_root()
  if not root:
    return None
  directory = os.path.join(
      root, 'sdks', 'go', 'pkg', 'beam', 'io', 'postgresio')
  if not os.path.isdir(directory):
    return None
  sources = []
  for name in sorted(os.listdir(directory)):
    if not name.startswith('schematransform'):
      continue
    if not name.endswith('.go') or name.endswith('_test.go'):
      continue
    with open(os.path.join(directory, name), encoding='utf-8') as fh:
      sources.append(fh.read())
  if not sources:
    return None
  return '\n'.join(sources)


class PostgresWireContractParityTest(unittest.TestCase):
  """Guards against the Python wrappers drifting behind the Go wire contract.

  The Go config structs are the authoritative SchemaTransform contract. A field
  declared there but absent from the Python constructor is silently
  unreachable from Python and Beam YAML, which is how MERGE-mode CDC
  replication became inexpressible in an earlier revision.
  """
  def setUp(self):
    self.source = _go_schematransform_source()
    if self.source is None:
      self.skipTest('Go SDK sources are not available in this environment')

  def test_every_go_config_field_is_reachable_from_python(self):
    structs = {
        m.group('name'): m.group('body')
        for m in _GO_STRUCT_RE.finditer(self.source)
    }
    checked = 0
    for struct_name, transform_name in _GO_CONFIG_TO_PYTHON_TRANSFORM.items():
      self.assertIn(
          struct_name,
          structs,
          f'{struct_name} not found in the Go SchemaTransform sources')
      go_fields = set(_BEAM_TAG_RE.findall(structs[struct_name]))
      self.assertTrue(go_fields, f'{struct_name} declares no beam tags')

      transform = getattr(postgres_cdc, transform_name)
      accepted = set(inspect.signature(transform.__init__).parameters)
      accepted.discard('self')
      accepted.discard('expansion_service')

      missing = sorted(go_fields - accepted)
      self.assertEqual(
          missing, [],
          f'{transform_name} cannot express {struct_name} fields {missing}')
      checked += 1
    self.assertEqual(checked, len(_GO_CONFIG_TO_PYTHON_TRANSFORM))


if __name__ == '__main__':
  unittest.main()
