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

"""Subprocess isolation tests ensuring fast CLI paths skip `apache_beam`."""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

_LAUNCHER = Path(__file__).resolve().parents[2] / '_apache_beam_cli.py'

_PROBE_SCRIPT = """
import importlib.util
import json
import sys

launcher_path = sys.argv[1]
status_path = sys.argv[2]
cli_argv = sys.argv[3:]

spec = importlib.util.spec_from_file_location('_apache_beam_cli', launcher_path)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

try:
  code = mod.main(cli_argv)
except SystemExit as exc:
  code = exc.code if isinstance(exc.code, int) else (0 if exc.code is None else 1)

with open(status_path, 'w', encoding='utf-8') as f:
  json.dump(
      {
          'exit_code': code,
          'beam_imported': 'apache_beam' in sys.modules,
          'yaml_imported': 'yaml' in sys.modules,
      },
      f)
sys.exit(code)
"""


@unittest.skipUnless(_LAUNCHER.is_file(), f'missing launcher {_LAUNCHER}')
class FastCliIsolationTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp()
    self.addCleanup(shutil.rmtree, self.tmp)
    self.beam_home = os.path.join(self.tmp, '.beam')

  def _probe(self, argv, extra_env=None):
    status_path = os.path.join(self.tmp, 'probe_status.json')
    if os.path.exists(status_path):
      os.remove(status_path)
    env = dict(os.environ)
    env['BEAM_HOME'] = self.beam_home
    env['BEAM_CONFIG'] = os.path.join(self.beam_home, 'config.yaml')
    env.pop('BEAM_NO_CONFIG', None)
    env.pop('BEAM_PROFILE', None)
    if extra_env:
      env.update(extra_env)
    proc = subprocess.run(
        [sys.executable, '-c', _PROBE_SCRIPT, str(_LAUNCHER), status_path] +
        list(argv),
        capture_output=True,
        text=True,
        env=env,
        check=False)
    with open(status_path, encoding='utf-8') as f:
      status = json.load(f)
    return proc, status

  def test_help_paths_do_not_import_apache_beam(self):
    for argv in (
        ['--help'],
        ['run', '--help'],
        ['pipe', '--help'],
        ['sql', '--help'],
        ['config', '--help'],
    ):
      with self.subTest(argv=argv):
        proc, status = self._probe(argv)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertFalse(status['beam_imported'])
        self.assertFalse(status['yaml_imported'])

  def test_fast_error_paths_do_not_import_apache_beam(self):
    in_csv = os.path.join(self.tmp, 'in.csv')
    with open(in_csv, 'w', encoding='utf-8') as f:
      f.write('a,b\n1,2\n')
    out_prefix = os.path.join(self.tmp, 'out', 'data.json')
    os.makedirs(os.path.dirname(out_prefix))
    with open(out_prefix + '-00000-of-00001', 'w', encoding='utf-8') as f:
      f.write('{}\n')

    cases = [
        (['run', os.path.join(self.tmp, 'missing.yaml')], 2),
        (['pipe', '--from', in_csv, '--to', os.path.join(self.tmp, 'x')], 2),
        (['pipe', '--from', in_csv, '--to', out_prefix], 2),
        (['sql', '--version', '2.75.0'], 4),
    ]
    for argv, expected_code in cases:
      with self.subTest(argv=argv):
        extra = {
            'JAVA_HOME': os.path.join(self.tmp, 'no_jdk'), 'PATH': ''
        } if argv[0] == 'sql' else None
        proc, status = self._probe(argv, extra_env=extra)
        self.assertEqual(proc.returncode, expected_code, proc.stderr)
        self.assertFalse(status['beam_imported'])
        self.assertFalse(status['yaml_imported'])

  def test_warm_config_commands_do_not_import_apache_beam(self):
    # Cold `config set` populates the PipelineOptions disk cache once.
    proc, _ = self._probe(['config', 'set', 'runner', 'DataflowRunner'])
    self.assertEqual(proc.returncode, 0, proc.stderr)

    # All subsequent config operations (including `set` with typo validation!)
    # hit the disk cache and never import `apache_beam`.
    for argv in (
        ['config', 'set', 'project', 'my-gcp-project'],
        ['config', 'set', 'temp-location', 'gs://my-bucket/tmp'],
        ['config', 'get', 'runner'],
        ['config', 'list'],
        ['config', 'use', 'dev'],
        ['config', 'unset', 'temp-location', '--profile', 'default'],
    ):
      with self.subTest(argv=argv):
        proc, status = self._probe(argv)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertFalse(status['beam_imported'])

  def test_module_invocation_still_works(self):
    proc = subprocess.run(
        [sys.executable, '-m', 'apache_beam.cli.main', '--help'],
        capture_output=True,
        text=True,
        check=False)
    self.assertEqual(proc.returncode, 0, proc.stderr)
    self.assertIn('usage: beam', proc.stdout)


if __name__ == '__main__':
  unittest.main()
