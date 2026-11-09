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


def _eval_keep(keep, row_dict: dict):
  """Evaluates a Filter `keep:` the way Beam YAML does.

  Native expressions are run like `yaml_mapping._expand_python_mapping_func`:
  every field whose name is a substring of the text is bound as a local.
  """
  if isinstance(keep, dict):
    return _eval_callable(keep['callable'], row_dict)
  lines = ['def fn(__row__):']
  lines += [f'  {n} = __row__.{n}' for n in row_dict if n in keep]
  lines.append(f'  return ({keep})')
  ns: dict = {}
  exec('\n'.join(lines), ns)  # pylint: disable=exec-used
  return ns['fn'](types.SimpleNamespace(**row_dict))


class ExpressionsTest(unittest.TestCase):
  def test_sql_boolean_keywords_and_string_literals_preserved(self):
    cfg, cols = expressions.compile_where(
        "state == 'OR' AND note != 'NOT NULL' AND deleted_at IS NULL "
        'AND email IS NOT NULL # trailing comment')
    self.assertEqual(cols, frozenset(['state', 'note', 'deleted_at', 'email']))
    src = cfg['keep']
    self.assertTrue(
        _eval_keep(
            src, {
                'state': 'OR',
                'note': 'active',
                'deleted_at': None,
                'email': 'a@b.com',
            }))
    self.assertFalse(
        _eval_keep(
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
        _eval_keep(cfg['keep'], {'user_id': 42, 'id': 1, 'or_flag': 0}))
    # Simple filters are emitted as native, NULL-guarded Beam YAML text.
    self.assertEqual(cfg['keep'], 'user_id == 42 and (id is not None and id > 0)')
    self.assertFalse(_eval_keep(cfg['keep'], {'user_id': 42, 'id': None}))

  def test_sql_equals_not_equals_cast_and_keyword_columns(self):
    cfg, cols = expressions.compile_where(
        "age = 21 AND country <> 'CA' AND class = 'enterprise' "
        "AND `order-type` = 'b2b'")
    self.assertEqual(
        cols, frozenset(['age', 'country', 'class', 'order-type']))
    src = cfg['keep']['callable']
    self.assertTrue(
        _eval_callable(
            src,
            {
                'age': 21,
                'country': 'US',
                'class': 'enterprise',
                'order-type': 'b2b',
            }))
    self.assertFalse(
        _eval_callable(
            src,
            {
                'age': 21,
                'country': 'CA',
                'class': 'enterprise',
                'order-type': 'b2b',
            }))

    sel_cfg, sel_cols = expressions.compile_select(
        'CAST(order_id AS INT), '
        'ROUND(CAST(amount AS FLOAT), 2) AS amount, '
        'IFNULL(tier, "STANDARD") AS tier, '
        '`class` AS cust_class')
    self.assertEqual(
        sel_cols, frozenset(['order_id', 'amount', 'tier', 'class']))
    self.assertEqual(sel_cfg['fields']['cust_class'], 'class')
    row = {'order_id': '101', 'amount': '120.556', 'tier': None, 'class': 'pro'}
    self.assertEqual(
        _eval_callable(sel_cfg['fields']['order_id']['callable'], row), 101)
    self.assertEqual(
        _eval_callable(sel_cfg['fields']['amount']['callable'], row), 120.56)
    self.assertEqual(
        _eval_callable(sel_cfg['fields']['tier']['callable'], row), 'STANDARD')

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

  def test_jinja_placeholders_allowed_only_when_allow_jinja_true(self):
    # Live execution (allow_jinja=False) strictly rejects Jinja/braces.
    with self.assertRaisesRegex(ExpressionError, 'braces'):
      expressions.compile_where('amount >= {{ min_amount | default(50) }}')
    with self.assertRaisesRegex(ExpressionError, 'braces'):
      expressions.compile_select(
          'order_id, amount * {{ rate | default(1.08) }} AS taxed')

    # --emit-yaml mode (allow_jinja=True) validates Jinja AST and preserves {{ ... }}.
    w_cfg, w_cols = expressions.compile_where(
        "amount >= {{ min_amount | default(50) }} AND country = '{{ region }}'",
        allow_jinja=True)
    self.assertEqual(w_cols, frozenset(['amount', 'country']))
    self.assertIn(
        '{{ min_amount | default(50) }}', w_cfg['keep']['callable'])
    self.assertIn("'{{ region }}'", w_cfg['keep']['callable'])

    s_cfg, s_cols = expressions.compile_select(
        'order_id, amount * {{ tax_rate | default(1.08) }} AS taxed_amount',
        allow_jinja=True)
    self.assertEqual(s_cols, frozenset(['order_id', 'amount']))
    self.assertEqual(s_cfg['fields']['order_id'], 'order_id')
    self.assertIn(
        '{{ tax_rate | default(1.08) }}',
        s_cfg['fields']['taxed_amount']['callable'])

    # Malformed Jinja, control blocks, unaliased Jinja, and Jinja function calls fail.
    for bad_where in (
        'amount >= {{ unclosed',
        'amount >= {{ }}',
        'amount >= {% if true %}1{% endif %}',
        '{{ fn_name }}(amount) > 0',
    ):
      with self.assertRaises(ExpressionError, msg=f'expected error: {bad_where}'):
        expressions.compile_where(bad_where, allow_jinja=True)

    with self.assertRaisesRegex(ExpressionError, 'requires an alias'):
      expressions.compile_select('{{ raw_val | default(1) }}', allow_jinja=True)

  def test_native_expressions_for_simple_cases(self):
    cfg, _ = expressions.compile_where('amount > 100')
    self.assertEqual(cfg['keep'], 'amount is not None and amount > 100')
    self.assertFalse(_eval_keep(cfg['keep'], {'amount': None}))
    self.assertTrue(_eval_keep(cfg['keep'], {'amount': 101}))
    for raw in ('NOT (amount > 5)', "upper(name) == 'A'", 'user.tier == 1',
                'qty * price > 3', '`order-id` > 1', 'CAST(zip AS int) > 1'):
      self.assertIsInstance(
          expressions.compile_where(raw)[0]['keep'], dict, raw)
    # A keyword-named source column that appears inside the text would break
    # Beam's field binding, so the callable is kept.
    cfg, _ = expressions.compile_where(
        'classification > 1', known_columns={'classification', 'class'})
    self.assertIsInstance(cfg['keep'], dict)
    cfg, _ = expressions.compile_where(
        'amount > 1', known_columns={'amount', 'class'})
    self.assertIsInstance(cfg['keep'], str)
    sel, _ = expressions.compile_select('id, amount + 1 AS a1, qty * 2 AS q')
    self.assertEqual(sel['fields']['a1'], 'None if amount is None else amount + 1')
    self.assertIn('callable', sel['fields']['q'])


class SqlNullSemanticsTest(unittest.TestCase):
  """--where must keep exactly the rows SQLite's WHERE keeps (Round 5 F2/F3).

  SQLite implements SQL three-valued logic, so it is the oracle: every
  expression is run against the same rows (with NULLs) through SQLite and
  through the compiled Filter, both as a callable and as native Beam YAML
  text evaluated the way Beam binds fields.
  """
  ROWS = [
      {'id': 1, 'state': 'CA', 'amount': 120.0, 'qty': 2},
      {'id': 2, 'state': None, 'amount': 50.0, 'qty': 0},
      {'id': 3, 'state': 'NY', 'amount': None, 'qty': 3},
      {'id': 4, 'state': 'C', 'amount': 0.0, 'qty': None},
      {'id': 5, 'state': '', 'amount': 300.0, 'qty': 1},
      {'id': 6, 'state': None, 'amount': None, 'qty': None},
  ]
  EXPRESSIONS = [
      "state = 'CA'",
      "state != 'CA'",
      "state <> 'CA'",
      "NOT state = 'CA'",
      "state IN ('CA')",
      "state IN ('CA', 'NY')",
      "state NOT IN ('CA', 'NY')",
      "NOT (state IN ('CA'))",
      'amount > 100',
      'NOT (amount > 100)',
      'amount >= 50 AND qty > 0',
      'amount > 100 OR qty = 0',
      'NOT (amount > 100 OR qty = 0)',
      'NOT (amount > 100 AND qty = 0)',
      "state IS NULL",
      "state IS NOT NULL AND state != ''",
      'amount + qty > 5',
      'qty != 0 AND amount / qty > 10',
      '1 < qty AND qty <= 3',
      "state = 'CA' OR amount IS NULL",
      "NOT (state IS NULL)",
  ]

  @classmethod
  def setUpClass(cls):
    import sqlite3
    cls.conn = sqlite3.connect(':memory:')
    cls.conn.execute('CREATE TABLE t (id INT, state TEXT, amount REAL, qty INT)')
    cls.conn.executemany(
        'INSERT INTO t VALUES (:id, :state, :amount, :qty)', cls.ROWS)

  def _sqlite_ids(self, expr):
    return sorted(
        r[0] for r in self.conn.execute(f'SELECT id FROM t WHERE {expr}'))

  def test_where_matches_sqlite(self):
    for expr in self.EXPRESSIONS:
      cfg, _ = expressions.compile_where(expr)
      got = sorted(r['id'] for r in self.ROWS if _eval_keep(cfg['keep'], r))
      self.assertEqual(got, self._sqlite_ids(expr), f'{expr!r} -> {cfg["keep"]}')

  def test_callable_form_matches_sqlite(self):
    from unittest import mock
    with mock.patch.object(expressions, '_native_expression', return_value=None):
      for expr in self.EXPRESSIONS:
        cfg, _ = expressions.compile_where(expr)
        self.assertIn('callable', cfg['keep'])
        got = sorted(r['id'] for r in self.ROWS if _eval_keep(cfg['keep'], r))
        self.assertEqual(got, self._sqlite_ids(expr), expr)

  def test_select_boolean_is_null_when_unknown(self):
    cfg, _ = expressions.compile_select(
        "id, amount > 100 OR qty = 0 AS flag, state IN ('CA') AS ca")
    rows = {r['id']: r for r in self.ROWS}

    def value(field, row):
      spec = cfg['fields'][field]
      if isinstance(spec, dict) and 'callable' in spec:
        return _eval_callable(spec['callable'], row)
      text = spec['expression'] if isinstance(spec, dict) else spec
      return _eval_keep(text, row)

    self.assertIs(value('flag', rows[1]), True)
    self.assertIs(value('flag', rows[3]), None)  # NULL OR FALSE
    self.assertIs(value('flag', rows[2]), True)  # FALSE OR TRUE
    self.assertIs(value('ca', rows[4]), False)  # 'C' is not in ('CA')
    self.assertIs(value('ca', rows[2]), None)


if __name__ == '__main__':
  unittest.main()
