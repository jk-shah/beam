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

"""Conformance check of the PostgreSQL examples against the transform signatures.

Both the Beam YAML providers and the Python wrappers forward configuration by
keyword, so a key the constructor does not declare raises TypeError at pipeline
construction time. The examples are documentation that users copy, so this test
statically parses every example and asserts that every key it supplies is a
real parameter.
"""

import ast
import glob
import inspect
import os
import re
import unittest

from apache_beam.io import postgres_cdc

# Transform names as they appear in the examples, mapped to the class that
# ultimately receives the keyword arguments.
_CLASS_FOR_TYPE = {
    'ReadFromPostgres': 'ReadFromPostgres',
    'ReadFromPostgresIO': 'ReadFromPostgres',
    'ReadFromPostgresBatch': 'ReadFromPostgres',
    'WriteToPostgres': 'WriteToPostgres',
    'WriteToPostgresIO': 'WriteToPostgres',
    'WriteToPostgresBulk': 'WriteToPostgres',
    'ReadFromPostgresCDC': 'ReadFromPostgresCDC',
    'ReadFromPostgresCdc': 'ReadFromPostgresCDC',
}


def _examples_root():
  """Returns the PostgreSQL examples directory, or None outside a checkout."""
  root = postgres_cdc._find_beam_repo_root()
  if not root:
    return None
  path = os.path.join(root, 'sdks', 'go', 'examples', 'postgres')
  return path if os.path.isdir(path) else None


def _skill_document():
  """Returns the postgresio agent skill path, or None if it is not present."""
  root = postgres_cdc._find_beam_repo_root()
  if not root:
    return None
  path = os.path.join(root, '.agent', 'skills', 'postgresio', 'SKILL.md')
  return path if os.path.isfile(path) else None


def _fenced_blocks(text, language):
  """Returns the bodies of every ```<language> fenced block in a document."""
  pattern = re.compile(
      r'^```' + re.escape(language) + r'\s*$(.*?)^```\s*$', re.M | re.S)
  return [match.group(1) for match in pattern.finditer(text)]


def _accepted_parameters():
  """Returns {class_name: set(parameter names)} for the PostgreSQL transforms."""
  accepted = {}
  for name in ('ReadFromPostgres', 'WriteToPostgres', 'ReadFromPostgresCDC'):
    params = set(
        inspect.signature(getattr(postgres_cdc, name).__init__).parameters)
    params.discard('self')
    accepted[name] = params
  return accepted


def _parse_yaml_transform_blocks(text):
  """Yields (transform_type, [config keys]) pairs from a Beam YAML pipeline.

  An indentation-aware scan avoids depending on a YAML parser tolerating the
  templated values used throughout the examples.
  """
  blocks = []
  current_type = None
  config_indent = None
  keys = []
  for line in text.splitlines():
    stripped = line.strip()
    if not stripped or stripped.startswith('#'):
      continue
    match = re.match(r'^(\s*)-\s+type:\s*(\S+)\s*$', line)
    if match:
      if current_type:
        blocks.append((current_type, keys))
      current_type = match.group(2).strip('\'"')
      config_indent = None
      keys = []
      continue
    if current_type is None:
      continue
    match = re.match(r'^(\s*)config:\s*$', line)
    if match:
      config_indent = len(match.group(1))
      continue
    if config_indent is not None:
      indent = len(line) - len(line.lstrip())
      if indent <= config_indent:
        config_indent = None
        continue
      match = re.match(r'^\s*([A-Za-z_][A-Za-z0-9_]*):', line)
      # Only keys directly under config: map to constructor parameters.
      if match and indent == config_indent + 2:
        keys.append(match.group(1))
  if current_type:
    blocks.append((current_type, keys))
  return blocks


def _parse_python_call_sites(text, filename):
  """Yields (transform_name, [supplied kwargs]) pairs, resolving dict spreads."""
  tree = ast.parse(text, filename=filename)

  # Examples share sink settings through a dict that is splatted into several
  # calls, so those keys must be resolved to be checked.
  spread_dicts = {}
  for node in ast.walk(tree):
    if isinstance(node, ast.Assign) and isinstance(node.value, ast.Dict):
      for target in node.targets:
        if isinstance(target, ast.Name):
          spread_dicts[target.id] = [
              key.value for key in node.value.keys
              if isinstance(key, ast.Constant)
          ]

  call_sites = []
  for node in ast.walk(tree):
    if not isinstance(node, ast.Call):
      continue
    name = getattr(node.func, 'id', None) or getattr(node.func, 'attr', None)
    if name not in _CLASS_FOR_TYPE:
      continue
    supplied = []
    for keyword in node.keywords:
      if keyword.arg is None:
        if isinstance(keyword.value, ast.Name):
          supplied.extend(spread_dicts.get(keyword.value.id, []))
        elif isinstance(keyword.value, ast.Dict):
          supplied.extend(
              key.value for key in keyword.value.keys
              if isinstance(key, ast.Constant))
      else:
        supplied.append(keyword.arg)
    call_sites.append((name, supplied))
  return call_sites


class PostgresExampleConfigTest(unittest.TestCase):
  """Asserts the shipped examples only use parameters the transforms declare."""
  def setUp(self):
    self.root = _examples_root()
    if self.root is None:
      self.skipTest('PostgreSQL examples are not available in this environment')
    self.accepted = _accepted_parameters()

  def _defects_for(self, transform_name, supplied):
    accepted = self.accepted[_CLASS_FOR_TYPE[transform_name]]
    return sorted(set(supplied) - accepted)

  def test_yaml_examples_use_declared_config_keys(self):
    defects = []
    scanned = 0
    for path in sorted(glob.glob(f'{self.root}/**/pipeline.yaml',
                                 recursive=True)):
      with open(path, encoding='utf-8') as fh:
        blocks = _parse_yaml_transform_blocks(fh.read())
      for transform_name, keys in blocks:
        if transform_name not in _CLASS_FOR_TYPE:
          continue
        scanned += 1
        unknown = self._defects_for(transform_name, keys)
        if unknown:
          defects.append(f'{path}: {transform_name} config keys {unknown}')
    self.assertGreater(scanned, 0, 'no PostgreSQL YAML transforms were scanned')
    self.assertEqual(defects, [], '\n'.join(defects))

  def test_python_examples_use_declared_keyword_arguments(self):
    defects = []
    scanned = 0
    for path in sorted(glob.glob(f'{self.root}/**/pipeline.py',
                                 recursive=True)):
      with open(path, encoding='utf-8') as fh:
        call_sites = _parse_python_call_sites(fh.read(), path)
      for transform_name, supplied in call_sites:
        scanned += 1
        unknown = self._defects_for(transform_name, supplied)
        if unknown:
          defects.append(f'{path}: {transform_name}() kwargs {unknown}')
    self.assertGreater(
        scanned, 0, 'no PostgreSQL Python call sites were scanned')
    self.assertEqual(defects, [], '\n'.join(defects))

  def test_skill_document_snippets_use_declared_keys(self):
    """The agent skill is copied verbatim, so its snippets must be valid."""
    path = _skill_document()
    if path is None:
      self.skipTest('postgresio agent skill is not available')
    with open(path, encoding='utf-8') as fh:
      text = fh.read()

    defects = []
    scanned = 0

    yaml_blocks = _fenced_blocks(text, 'yaml')
    self.assertGreater(
        len(yaml_blocks), 0, 'no YAML snippets found in SKILL.md')
    for index, block in enumerate(yaml_blocks):
      for transform_name, keys in _parse_yaml_transform_blocks(block):
        if transform_name not in _CLASS_FOR_TYPE:
          continue
        scanned += 1
        unknown = self._defects_for(transform_name, keys)
        if unknown:
          defects.append(
              f'SKILL.md yaml block {index}: {transform_name} keys {unknown}')

    python_blocks = _fenced_blocks(text, 'python')
    self.assertGreater(
        len(python_blocks), 0, 'no Python snippets found in SKILL.md')
    for index, block in enumerate(python_blocks):
      # A snippet that no longer parses is itself a documentation defect.
      call_sites = _parse_python_call_sites(block, f'SKILL.md:python:{index}')
      for transform_name, supplied in call_sites:
        scanned += 1
        unknown = self._defects_for(transform_name, supplied)
        if unknown:
          defects.append(
              f'SKILL.md python block {index}: '
              f'{transform_name}() kwargs {unknown}')

    self.assertGreater(scanned, 0, 'no PostgreSQL snippets were scanned')
    self.assertEqual(defects, [], '\n'.join(defects))

  def test_checker_detects_an_unsupported_key(self):
    """Negative control: the parsers must flag a key that does not exist."""
    yaml_text = (
        'pipeline:\n'
        '  transforms:\n'
        '    - type: WriteToPostgresIO\n'
        '      config:\n'
        '        table: "public.orders"\n'
        '        primary_key_columns: ["order_id"]\n')
    blocks = _parse_yaml_transform_blocks(yaml_text)
    self.assertEqual(len(blocks), 1)
    self.assertEqual(
        self._defects_for(blocks[0][0], blocks[0][1]), ['primary_key_columns'])

    python_text = (
        'sink_kwargs = {"primary_key": ["order_id"]}\n'
        'WriteToPostgresIO(table="public.orders", batch_size=10,'
        ' **sink_kwargs)\n')
    call_sites = _parse_python_call_sites(python_text, 'negative_control.py')
    self.assertEqual(len(call_sites), 1)
    self.assertEqual(
        self._defects_for(call_sites[0][0], call_sites[0][1]),
        ['batch_size', 'primary_key'])


if __name__ == '__main__':
  unittest.main()
