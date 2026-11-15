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

import contextlib
import io
import unittest
from unittest import mock

from apache_beam.cli import llms_txt


class _FakeEntryPoint:
  def __init__(self, name: str, value: str = 'pkg.mod:fn'):
    self.name = name
    self.value = value


class LlmsTxtAndPluginsTest(unittest.TestCase):
  def test_render_llms_txt_includes_subcommands_exit_codes_and_version(self):
    with mock.patch.object(
        llms_txt.output, 'installed_beam_version', return_value='2.79.0'
    ):
      text = llms_txt.render_llms_txt()
    self.assertIn('Agent Manual (v2.79.0)', text)
    for subcmd in (
        'beam inspect',
        'beam tail',
        'beam catalog sync',
        'beam pipe',
        'beam schema',
        'beam export-schedule',
        'beam package',
        'beam diff',
        'beam check',
        'beam run',
        'beam doctor',
    ):
      self.assertIn(subcmd, text)
    for code in ('`0`', '`1`', '`2`', '`3`', '`4`', '`6`', '`7`', '`8`'):
      self.assertIn(code, text)

  def test_discover_cli_plugins_registers_valid_and_skips_reserved_or_blank(
      self):
    eps = [
        _FakeEntryPoint(''),
        _FakeEntryPoint('   '),
        _FakeEntryPoint('pipe', 'evil:shadow'),
        _FakeEntryPoint('mcp', 'beam_mcp:run'),
        _FakeEntryPoint('bench', 'beam_bench:run'),
    ]
    stderr = io.StringIO()
    with (
        mock.patch('importlib.metadata.entry_points', return_value=eps),
        contextlib.redirect_stderr(stderr),
    ):
      found = llms_txt.discover_cli_plugins(frozenset({'pipe', 'run', 'diff'}))

    self.assertEqual(set(found.keys()), {'mcp', 'bench'})
    self.assertIs(found['mcp'], eps[3])
    self.assertIn("ignoring CLI plugin 'pipe'", stderr.getvalue())

  def test_discover_cli_plugins_handles_entry_points_exception(self):
    with mock.patch(
        'importlib.metadata.entry_points', side_effect=RuntimeError('boom')
    ):
      self.assertEqual(
          llms_txt.discover_cli_plugins(frozenset({'pipe'})), {}
      )


if __name__ == '__main__':
  unittest.main()
