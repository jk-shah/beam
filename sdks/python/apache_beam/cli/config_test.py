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

import argparse
import contextlib
import io
import os
import shutil
import stat
import tempfile
import unittest
from unittest import mock

import yaml

from apache_beam.cli import config
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import WorkerOptions


def _ns(**kwargs):
  return argparse.Namespace(**kwargs)


class ConfigTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp()
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.cfg_path = os.path.join(self.tmp, '.beam', 'config.yaml')
    env_patcher = mock.patch.dict(
        os.environ,
        {
            'BEAM_HOME': os.path.join(self.tmp, '.beam'),
            'BEAM_CONFIG': self.cfg_path,
            'BEAM_PROFILE': '',
            'BEAM_NO_CONFIG': '',
        },
        clear=False)
    env_patcher.start()
    self.addCleanup(env_patcher.stop)

  def _run(self, **kwargs):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
      rc = config.run_command(_ns(**kwargs))
    return rc, out.getvalue(), err.getvalue()

  def test_set_get_unset_list_and_use_profiles(self):
    rc, _, _ = self._run(
        config_action='set',
        option='runner',
        value='DataflowRunner',
        profile=None)
    self.assertEqual(rc, 0)
    if os.name == 'posix':
      mode = stat.S_IMODE(os.stat(self.cfg_path).st_mode)
      self.assertEqual(mode, 0o600)

    rc, out, _ = self._run(config_action='get', option='runner', profile=None)
    self.assertEqual(rc, 0)
    self.assertEqual(out.strip(), 'DataflowRunner')

    rc, _, _ = self._run(config_action='get', option='project', profile=None)
    self.assertEqual(rc, 1)

    rc, out, _ = self._run(config_action='list', profile=None)
    self.assertEqual(rc, 0)
    self.assertIn('# Profile: default', out)
    parsed = yaml.safe_load(out)
    self.assertEqual(parsed, {'options': {'runner': 'DataflowRunner'}})

    self._run(
        config_action='set',
        option='runner',
        value='PrismRunner',
        profile='dev')
    rc, out, _ = self._run(config_action='use', profile_name='dev')
    self.assertEqual(rc, 0)
    self.assertIn("Switched active profile to 'dev'", out)

    rc, out, _ = self._run(config_action='get', option='runner', profile=None)
    self.assertEqual(rc, 0)
    self.assertEqual(out.strip(), 'PrismRunner')

    self._run(config_action='unset', option='runner', profile=None)
    rc, _, _ = self._run(config_action='get', option='runner', profile=None)
    self.assertEqual(rc, 1)

  def test_boolean_list_and_aliased_flag_serialization(self):
    self._run(
        config_action='set', option='streaming', value='true', profile=None)
    self._run(
        config_action='set',
        option='no_use_public_ips',
        value='true',
        profile=None)
    self._run(
        config_action='set',
        option='experiments',
        value='use_runner_v2,beam_fn_api',
        profile=None)
    self._run(
        config_action='set',
        option='labels',
        value='env=prod,team=data',
        profile=None)
    self._run(
        config_action='set',
        option='pubsub_root_url',
        value='http://localhost:8085',
        profile=None)

    err = io.StringIO()
    with contextlib.redirect_stderr(err):
      flags = config.resolve_pipeline_args([])

    self.assertIn('--streaming', flags)
    self.assertIn('--no_use_public_ips', flags)
    self.assertIn('--experiments=use_runner_v2,beam_fn_api', flags)
    self.assertIn('--labels=env=prod', flags)
    self.assertIn('--labels=team=data', flags)
    self.assertIn('--pubsub_root_url=http://localhost:8085', flags)
    self.assertIn("Using config profile 'default'", err.getvalue())

    opts = PipelineOptions(flags)
    self.assertTrue(opts.view_as(StandardOptions).streaming)
    self.assertFalse(opts.view_as(WorkerOptions).use_public_ips)
    self.assertEqual(
        opts.view_as(DebugOptions).experiments,
        ['use_runner_v2', 'beam_fn_api'])
    self.assertEqual(
        opts.view_as(GoogleCloudOptions).labels, ['env=prod', 'team=data'])

  def test_typo_warning_suggests_closest_pipeline_option(self):
    rc, _, err = self._run(
        config_action='set',
        option='temp-location',
        value='gs://bucket/tmp',
        profile=None)
    self.assertEqual(rc, 0)
    self.assertIn("did you mean 'temp_location'?", err)

  def test_rejects_secret_options_and_inline_secret_values(self):
    for secret in (
        'password',
        's3_secret_access_key',
        'sasl_jaas_config',
        'azure_connection_string',
        'api-key',
        'passphrase',
        'signing-key',
        'encryption_key',
        'bearer',
        'authorization',
        'db_pwd',
        'session_cookie',
        'auth-token',
    ):
      with self.subTest(secret=secret):
        with self.assertRaises(config.ConfigError) as ctx:
          self._run(
              config_action='set', option=secret, value='val', profile=None)
        self.assertEqual(ctx.exception.exit_code, 2)

    for bad_val in (
        'postgres://admin:s3cr3t@db.internal:5432/prod',
        'sk-proj-1234567890abcdef1234',
        'ya29.a0AfH6SMBx1234567890',
        'AIzaSyA12345678901234567890123456',
        'ghp_1234567890abcdef123456',
        'hf_1234567890abcdef123456',
    ):
      with self.subTest(bad_val=bad_val):
        with self.assertRaises(config.ConfigError) as ctx:
          self._run(
              config_action='set',
              option='temp_location',
              value=bad_val,
              profile=None)
        self.assertEqual(ctx.exception.exit_code, 2)

  def test_rejects_symlink_target_and_recovers_from_corrupt_config(self):
    os.makedirs(os.path.dirname(self.cfg_path), exist_ok=True)
    # 1. Corrupt YAML recovers cleanly on `set`, `use`, and `reset`.
    with open(self.cfg_path, 'w', encoding='utf-8') as f:
      f.write(':::corrupt: [unclosed yaml')
    if os.name == 'posix':
      os.chmod(self.cfg_path, 0o600)

    rc, _, err = self._run(
        config_action='set', option='project', value='recovered-p', profile=None)
    self.assertEqual(rc, 0)
    self.assertIn('.corrupt', err)
    self.assertTrue(os.path.isfile(self.cfg_path + '.corrupt'))
    rc_g, out_g, _ = self._run(
        config_action='get', option='project', profile=None)
    self.assertEqual(rc_g, 0)
    self.assertEqual(out_g.strip(), 'recovered-p')

    # Corrupt again and recover via `use`
    with open(self.cfg_path, 'w', encoding='utf-8') as f:
      f.write('- not_a_mapping\n')
    rc_u, _, _ = self._run(config_action='use', profile_name='staging')
    self.assertEqual(rc_u, 0)

    # Corrupt again and recover via `reset`
    with open(self.cfg_path, 'w', encoding='utf-8') as f:
      f.write(':::broken')
    rc_r, out_r, _ = self._run(config_action='reset', profile=None)
    self.assertEqual(rc_r, 0)
    self.assertIn('Reset configuration', out_r)

    # 2. Symlink config path is refused!
    target_other = os.path.join(self.tmp, 'other.yaml')
    with open(target_other, 'w', encoding='utf-8') as f:
      f.write('active_profile: default\n')
    os.remove(self.cfg_path)
    try:
      os.symlink(target_other, self.cfg_path)
    except (OSError, NotImplementedError):
      return
    with self.assertRaises(config.ConfigError):
      self._run(
          config_action='set', option='project', value='p2', profile=None)

  def test_precedence_cli_override_no_config_and_cwd_isolation(self):
    self._run(
        config_action='set',
        option='runner',
        value='DataflowRunner',
        profile=None)
    self._run(
        config_action='set', option='project', value='cfg-proj', profile=None)
    self._run(
        config_action='set', option='experiments', value='exp_a', profile=None)

    err = io.StringIO()
    with contextlib.redirect_stderr(err):
      flags = config.resolve_pipeline_args(
          ['--runner=PrismRunner', '--experiment=exp_b'])
    self.assertEqual(
        flags,
        ['--project=cfg-proj', '--runner=PrismRunner', '--experiment=exp_b'])
    self.assertIn('--project=cfg-proj', err.getvalue())
    self.assertNotIn('DataflowRunner', err.getvalue())
    self.assertNotIn('exp_a', err.getvalue())

    err = io.StringIO()
    with contextlib.redirect_stderr(err):
      self.assertEqual(
          config.resolve_pipeline_args(['--project=cli'], no_config=True),
          ['--project=cli'])
    self.assertEqual(err.getvalue(), '')

    with mock.patch.dict(os.environ, {'BEAM_NO_CONFIG': '1'}):
      self.assertEqual(config.resolve_pipeline_args([]), [])

    # Unrelated ./config.yaml in CWD is never consulted.
    cwd_dir = os.path.join(self.tmp, 'workdir')
    os.makedirs(cwd_dir)
    with open(os.path.join(cwd_dir, 'config.yaml'), 'w') as f:
      yaml.safe_dump(
          {'profiles': {
              'default': {
                  'options': {
                      'runner': 'DataflowRunner'
                  }
              }
          }},
          f)
    missing_cfg = os.path.join(self.tmp, 'nonexistent', 'config.yaml')
    prev_cwd = os.getcwd()
    os.chdir(cwd_dir)
    try:
      with mock.patch.dict(os.environ, {'BEAM_CONFIG': missing_cfg}):
        self.assertEqual(config.resolve_pipeline_args([]), [])
    finally:
      os.chdir(prev_cwd)

  def test_configured_sql_runner(self):
    self._run(
        config_action='set',
        option='runner',
        value='DataflowRunner',
        profile=None)
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
      self.assertEqual(config.configured_sql_runner(None), 'dataflow')
    self.assertIn('--runner=dataflow', err.getvalue())

    err = io.StringIO()
    with contextlib.redirect_stderr(err):
      self.assertEqual(config.configured_sql_runner('direct'), 'direct')
    self.assertEqual(err.getvalue(), '')

    self.assertEqual(
        config.configured_sql_runner(None, no_config=True), 'direct')

    self._run(
        config_action='set', option='runner', value='SparkRunner', profile=None)
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
      self.assertEqual(config.configured_sql_runner(None), 'direct')
    self.assertEqual(err.getvalue(), '')

  def test_option_catalog_disk_cache(self):
    cache_file = os.path.join(self.tmp, '.beam', 'cache', 'opts.json')
    config._cached_option_catalog.cache_clear()
    self.addCleanup(config._cached_option_catalog.cache_clear)

    by_dest, by_flag = config._cached_option_catalog(cache_file)
    self.assertTrue(os.path.isfile(cache_file))
    self.assertIn('runner', by_dest)
    self.assertEqual(by_flag['pubsub_root_url'][0], 'pubsubRootUrl')

    config._cached_option_catalog.cache_clear()
    with mock.patch.object(config,
                           '_introspect_pipeline_options',
                           side_effect=AssertionError('should hit disk cache')):
      warm_dest, warm_flag = config._cached_option_catalog(cache_file)
    self.assertEqual(warm_dest['pubsubRootUrl'], by_dest['pubsubRootUrl'])
    self.assertEqual(warm_flag['streaming'], by_flag['streaming'])

    # Corrupt cache gracefully regenerates via live introspection.
    with open(cache_file, 'w', encoding='utf-8') as f:
      f.write('{corrupt json')
    config._cached_option_catalog.cache_clear()
    regen_dest, _ = config._cached_option_catalog(cache_file)
    self.assertIn('runner', regen_dest)


if __name__ == '__main__':
  unittest.main()
