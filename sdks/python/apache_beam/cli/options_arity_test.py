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

"""Verifies static `_option_arity` tables against live `PipelineOptions`."""

import argparse
import unittest

from apache_beam.cli import _option_arity
from apache_beam.cli import main as cli
from apache_beam.options.pipeline_options import PipelineOptions


def _all_subclasses(cls):
  result = []
  for sub in cls.__subclasses__():
    result.append(sub)
    result.extend(_all_subclasses(sub))
  return result


class OptionAritySyncTest(unittest.TestCase):
  def test_static_tables_match_pipeline_options_subclasses(self):
    parser = argparse.ArgumentParser(add_help=False, conflict_handler='resolve')
    for sub in _all_subclasses(PipelineOptions):
      sub._add_argparse_args(parser)

    live_zero = set()
    live_one = set()
    live_multi = set()
    for action in parser._actions:
      if not action.option_strings:
        continue
      if action.nargs == 0:
        live_zero.update(action.option_strings)
      elif action.nargs in ('*', '+') or (isinstance(action.nargs, int) and
                                          action.nargs > 1):
        live_multi.update(action.option_strings)
      else:
        live_one.update(action.option_strings)

    self.assertEqual(set(_option_arity.PIPELINE_ZERO_ARG_FLAGS), live_zero)
    self.assertEqual(set(_option_arity.PIPELINE_ONE_ARG_FLAGS), live_one)
    self.assertEqual(set(_option_arity.PIPELINE_MULTI_ARG_FLAGS), live_multi)

  def test_space_separated_and_kebab_pipeline_options_anywhere(self):
    cases = [
        (
            ['--runner', 'DirectRunner', 'run', 'p.yaml', '--project', 'p1'],
            'run',
            'p.yaml',
            ['--runner=DirectRunner', '--project=p1'],
        ),
        (
            [
                'run',
                '--runner',
                'DirectRunner',
                '--temp-location',
                'gs://b/t',
                'p.yaml',
                '--streaming',
            ],
            'run',
            'p.yaml',
            [
                '--runner=DirectRunner',
                '--temp_location=gs://b/t',
                '--streaming'
            ],
        ),
        (
            ['run', '-', '--runner', 'DirectRunner'],
            'run',
            '-',
            ['--runner=DirectRunner'],
        ),
    ]
    parser = cli._build_parser()
    for raw_argv, expected_cmd, expected_file, expected_extra in cases:
      with self.subTest(raw_argv=raw_argv):
        norm = cli._normalize_cli_argv(raw_argv)
        args, extra = parser.parse_known_args(norm)
        self.assertEqual(args.command, expected_cmd)
        self.assertEqual(args.file, expected_file)
        self.assertEqual(extra, expected_extra)

  def test_sql_and_config_keep_their_own_flag_parsing(self):
    self.assertEqual(
        cli._normalize_cli_argv(
            ['sql', '--runner', 'dataflow', '--', '-e', 'SELECT 1']),
        ['sql', '--runner', 'dataflow', '--', '-e', 'SELECT 1'])
    self.assertEqual(
        cli._normalize_cli_argv(
            ['config', 'set', 'runner', 'DirectRunner', '--profle', 'dev']),
        ['config', 'set', 'runner', 'DirectRunner', '--profle', 'dev'])


if __name__ == '__main__':
  unittest.main()
