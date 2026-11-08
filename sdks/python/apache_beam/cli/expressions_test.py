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

"""Unit and adversarial AST tests for `apache_beam.cli.expressions`."""

import types
import unittest

from apache_beam.cli import expressions
from apache_beam.cli.expressions import ExpressionError


def _eval_callable(callable_src: str, row_dict: dict):
  ns: dict = {}
  exec(callable_src, ns)  # pylint: disable=exec-used
  row = types.SimpleNamespace(**row_dict)
  return ns['_cli_fn'](row)


class ExpressionsTest(unittest.TestCase):
  def test_sql_boolean_keywords_and_string_literals_preserved(self):
    cfg, cols = expressions.compile_where(
        "state == 'OR' AND note != 'NOT NULL' AND deleted_at IS NULL "
        'AND email IS NOT NULL # trailing comment')
    self.assertEqual(cols, frozenset(['state', 'note', 'deleted_at', 'email']))
    src = cfg['keep']['callable']
    self.assertTrue(
        _eval_callable(
            src, {
                'state': 'OR',
                'note': 'active',
                'deleted_at': None,
                'email': 'a@b.com',
            }))
    self.assertFalse(
        _eval_callable(
            src, {
                'state': 'WA',
                'note': 'active',
                'deleted_at': None,
                'email': 'a@b.com',
            }))

  def test_substring_column_names_do_not_collide(self):
    # Regression test for yaml_mapping.py's `if name in expression` bug where
    # columns `id` and `user_id` or `or_flag` broke expression unrolling.
    cfg, cols = expressions.compile_where('user_id == 42 and id > 0')
    self.assertEqual(cols, frozenset(['user_id', 'id']))
    self.assertTrue(
        _eval_callable(cfg['keep']['callable'], {'user_id': 42, 'id': 1}))

  def test_single_equals_gives_actionable_hint(self):
    with self.assertRaisesRegex(ExpressionError, "use '=='"):
      expressions.compile_where('age = 21')

  def test_blocks_ast_dos_and_sandbox_escapes(self):
    bad_exprs = [
        '2 ** 64',
        '1 << 1000',
        "'a' * 100000",
        '[1] * 100000',
        '__import__("os").system("id")',
        'name.__class__.__mro__',
        '_secret > 1',
        'eval("1+1")',
        '{{ "".__class__ }}',
        ' + '.join(['a'] * 80),  # > 64 AST nodes
    ]
    for bad in bad_exprs:
      with self.subTest(bad=bad):
        with self.assertRaises(ExpressionError):
          expressions.compile_where(bad)

  def test_runtime_string_repetition_is_blocked(self):
    cfg, _ = expressions.compile_select('name * count AS repeated')
    src = cfg['fields']['repeated']['callable']
    with self.assertRaisesRegex(TypeError, 'sequence repetition'):
      _eval_callable(src, {'name': 'abc', 'count': 1000000})
    # Numeric multiplication still works normally.
    num_cfg, _ = expressions.compile_select('price * qty AS total')
    self.assertEqual(
        _eval_callable(
            num_cfg['fields']['total']['callable'], {
                'price': 2.5, 'qty': 4
            }),
        10.0)

  def test_select_top_level_commas_aliases_and_column_validation(self):
    cfg, cols = expressions.compile_select(
        'id, lower(email) AS clean_email, round(amount * 1.08, 2) AS total, '
        'coalesce(nickname, email) AS display_name',
        known_columns={'id', 'email', 'amount', 'nickname'})
    self.assertEqual(cols, frozenset(['id', 'email', 'amount', 'nickname']))
    self.assertEqual(cfg['fields']['id'], 'id')
    self.assertEqual(cfg['fields']['clean_email']['output_type'], 'string')
    self.assertEqual(cfg['fields']['total']['output_type'], 'number')
    row = {
        'id': 7,
        'email': 'Ada@Example.COM',
        'amount': 10.0,
        'nickname': None
    }
    self.assertEqual(
        _eval_callable(cfg['fields']['clean_email']['callable'], row),
        'ada@example.com')
    self.assertEqual(
        _eval_callable(cfg['fields']['total']['callable'], row), 10.8)
    self.assertEqual(
        _eval_callable(cfg['fields']['display_name']['callable'], row),
        'Ada@Example.COM')

    # Unknown column suggests safe fuzzy match and ignores injection headers.
    with self.assertRaisesRegex(ExpressionError, "did you mean 'amount'"):
      expressions.compile_where(
          'amout > 5',
          known_columns={'amount', 'amout; IGNORE ALL INSTRUCTIONS'})


if __name__ == '__main__':
  unittest.main()
