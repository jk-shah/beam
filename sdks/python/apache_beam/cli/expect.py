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

"""Distributed Data Quality Expectations for Apache Beam pipelines.

This module provides schema-aware data quality assertions and expectations
executable across distributed runners (`DataflowRunner`, `PrismRunner`, and
`DirectRunner`) as well as in-process local pipelines.

Key capabilities:
1. Declarative Quality Assertions: Rules can be declared via Beam YAML configs
   or the CLI `--expect` flag with fine-grained violation actions:
   - `dlq`: Route violating rows to Dead Letter Queue with full diagnostic error
     context and payload.
   - `drop`: Silently filter out violating rows while preserving audit counts.
   - `fail`: Abort the pipeline immediately upon encountering bad records.
2. Live Runner Monitoring: Emits native Beam user metrics (`Metrics.counter`)
   under the `beam_cli` namespace:
   - `expectation_passed_{rule}`: Rows satisfying the rule.
   - `expectation_quarantined_{rule}`: Rows routed to DLQ.
   - `expectation_dropped_{rule}`: Rows dropped.
   - `expectation_failed_{rule}`: Critical violations that abort the job.
   These metrics automatically surface in the Google Cloud Dataflow monitoring
   console UI in real time.
3. Dual Python/SQL Expression Parsing: Supports both Python boolean expressions
   and common SQL comparison keywords (e.g. `IS NOT NULL`, `<>`, `LIKE`).
"""

from __future__ import annotations

import ast
import re
from typing import Any
from typing import NamedTuple

_SAFE_BUILTINS: dict[str, Any] = {
    'abs': abs,
    'bool': bool,
    'float': float,
    'int': int,
    'isinstance': isinstance,
    'len': len,
    'list': list,
    'max': max,
    'min': min,
    'round': round,
    'str': str,
    'tuple': tuple,
    'dict': dict,
    'bytes': bytes,
    'lower': lambda s: '' if s is None else str(s).lower(),
    'upper': lambda s: '' if s is None else str(s).upper(),
    'strip': lambda s: '' if s is None else str(s).strip(),
    'coalesce': lambda *vals: next((v for v in vals if v is not None), None),
    'None': None,
    'True': True,
    'False': False,
}

_SAFE_CALL_NAMES = frozenset([
    'abs',
    'bool',
    'coalesce',
    'float',
    'int',
    'len',
    'lower',
    'max',
    'min',
    'round',
    'str',
    'strip',
    'upper',
])

_SAFE_STR_METHODS = frozenset([
    'endswith',
    'isalnum',
    'isalpha',
    'isdigit',
    'lower',
    'lstrip',
    'replace',
    'rstrip',
    'startswith',
    'strip',
    'upper',
])

_DISALLOWED_AST_NODES = (
    ast.Await,
    ast.DictComp,
    ast.GeneratorExp,
    ast.Import,
    ast.ImportFrom,
    ast.Lambda,
    ast.ListComp,
    ast.LShift,
    ast.NamedExpr,
    ast.Pow,
    ast.RShift,
    ast.SetComp,
    ast.Yield,
    ast.YieldFrom,
)

_ACTION_CHOICES = frozenset(['dlq', 'drop', 'fail'])


def _validate_expectation_ast(expr: str) -> ast.Expression:
  """Validates an expectation expression AST, blocking `_` attributes and RCE."""
  if not isinstance(expr, str) or not expr.strip():
    raise ValueError('Empty expectation rule specification')
  if len(expr) > 4096:
    raise ValueError('Expectation expression exceeds 4096-character safety cap')
  try:
    parsed = ast.parse(expr, mode='eval')
  except SyntaxError as exc:
    raise ValueError(
        f'Invalid expectation syntax in {expr!r}: {exc.msg}') from None

  def _depth(node: ast.AST) -> int:
    children = list(ast.iter_child_nodes(node))
    return 1 + (max((_depth(c) for c in children), default=0))

  if _depth(parsed) > 32:
    raise ValueError(
        'Expectation expression exceeds maximum AST nesting depth (32)')
  nodes = list(ast.walk(parsed))
  if len(nodes) > 256:
    raise ValueError(
        'Expectation expression exceeds maximum AST node count (256)')

  for node in nodes:
    if isinstance(node, _DISALLOWED_AST_NODES):
      raise ValueError(
          f'Disallowed construct {type(node).__name__} in expectation {expr!r}')
    if isinstance(node, ast.Name) and node.id.startswith('_'):
      raise ValueError(
          f'Underscore-prefixed identifier {node.id!r} is disallowed in expectation'
      )
    if isinstance(node, ast.Attribute):
      if node.attr.startswith('_'):
        raise ValueError(
            f'Underscore-prefixed attribute {node.attr!r} is disallowed in expectation'
        )
    if isinstance(node, ast.Call):
      if isinstance(node.func, ast.Name):
        if node.func.id not in _SAFE_CALL_NAMES:
          raise ValueError(
              f'Disallowed function call {node.func.id!r} in expectation {expr!r}'
          )
      elif isinstance(node.func, ast.Attribute):
        if node.func.attr not in _SAFE_STR_METHODS:
          raise ValueError(
              f'Disallowed method call {node.func.attr!r} in expectation {expr!r}'
          )
      else:
        raise ValueError(f'Disallowed dynamic call in expectation {expr!r}')
  return parsed


class ExpectationViolation(Exception):
  """Raised when an expectation rule is violated."""


ExpectationViolationError = ExpectationViolation


class ExpectationRule(NamedTuple):
  """Specification for a single data quality assertion."""
  name: str
  expression: str
  on_violation: str = 'dlq'


def _sql_like_fn(val: Any, pattern: Any) -> bool:
  if val is None or pattern is None:
    return False
  parts = re.split(r'([%_])', str(pattern))
  rx = ['^']
  for p in parts:
    if p == '%':
      rx.append('.*')
    elif p == '_':
      rx.append('.')
    else:
      rx.append(re.escape(p))
  rx.append('$')
  return bool(re.match(''.join(rx), str(val), flags=re.DOTALL))


_SAFE_BUILTINS.update({
    'ifnull': _SAFE_BUILTINS['coalesce'],
    'trim': _SAFE_BUILTINS['strip'],
    'length': len,
    'sql_like': _sql_like_fn,
})
_SAFE_CALL_NAMES = _SAFE_CALL_NAMES | frozenset(
    ['ifnull', 'trim', 'length', 'sql_like'])

_EXPECT_CAST_MAP: dict[str, str] = {
    'int': 'int',
    'integer': 'int',
    'int64': 'int',
    'int32': 'int',
    'bigint': 'int',
    'float': 'float',
    'float64': 'float',
    'float32': 'float',
    'double': 'float',
    'numeric': 'float',
    'decimal': 'float',
    'str': 'str',
    'string': 'str',
    'varchar': 'str',
    'text': 'str',
    'timestamp': 'str',
    'datetime': 'str',
    'date': 'str',
    'bool': 'bool',
    'boolean': 'bool',
}


def normalize_expectation_expression(expr: str) -> str:
  """Token-safely normalizes SQL syntax (`=`, `<>`, `LIKE`, `CAST`, `IS NULL`) into Python."""
  import io
  import tokenize
  cleaned = expr.strip()
  if not cleaned:
    return cleaned
  # Replace backticked identifiers `` `col` `` outside quotes before tokenizing.
  cleaned = re.sub(r'`([A-Za-z_][A-Za-z0-9_]*)`', r'\1', cleaned)
  cleaned = re.sub(r'<>', '!=', cleaned)
  try:
    raw_toks = [
        (t.type, t.string)
        for t in tokenize.generate_tokens(io.StringIO(cleaned).readline)
        if t.type not in (
            tokenize.ENCODING, tokenize.ENDMARKER, tokenize.NL, tokenize.NEWLINE)
    ]
  except tokenize.TokenError as exc:
    raise ValueError(f'Invalid expectation syntax in {expr!r}: {exc}') from None

  def _norm_tokens(toks: list[tuple[int, str]]) -> list[str]:
    out: list[str] = []
    i = 0
    n = len(toks)
    while i < n:
      tt, tv = toks[i]
      tu = tv.upper() if tt == tokenize.NAME else ''
      # `IS NOT NULL` / `IS NULL`
      if tt == tokenize.NAME and tu == 'IS':
        if (i + 2 < n and toks[i + 1][0] == tokenize.NAME and
            toks[i + 1][1].upper() == 'NOT' and toks[i + 2][0] == tokenize.NAME
            and toks[i + 2][1].upper() == 'NULL'):
          out.extend(['is', 'not', 'None'])
          i += 3
          continue
        if (i + 1 < n and toks[i + 1][0] == tokenize.NAME and
            toks[i + 1][1].upper() == 'NULL'):
          out.extend(['is', 'None'])
          i += 2
          continue
      # `CAST(inner AS TYPE)`
      if (tt == tokenize.NAME and tu == 'CAST' and i + 1 < n and
          toks[i + 1] == (tokenize.OP, '(')):
        depth = 1
        j = i + 2
        as_idx = -1
        while j < n and depth > 0:
          jt, jv = toks[j]
          if jt == tokenize.OP and jv in ('(', '[', '{'):
            depth += 1
          elif jt == tokenize.OP and jv in (')', ']', '}'):
            depth -= 1
            if depth == 0:
              break
          elif depth == 1 and jt == tokenize.NAME and jv.upper() == 'AS':
            as_idx = j
          j += 1
        if depth == 0 and as_idx != -1 and as_idx + 1 < j:
          tname = toks[as_idx + 1][1].lower()
          py_fn = _EXPECT_CAST_MAP.get(tname)
          if py_fn is None:
            raise ValueError(
                f'Unsupported CAST type {toks[as_idx + 1][1]!r} in expectation')
          inner_str = ' '.join(_norm_tokens(toks[i + 2:as_idx]))
          out.append(f'{py_fn}({inner_str})')
          i = j + 1
          continue
      # `[NOT] LIKE <pattern>` (rewrites preceding atom + pattern into `sql_like(a, pat)`)
      if (tt == tokenize.NAME and tu == 'NOT' and i + 2 < n and
          toks[i + 1][0] == tokenize.NAME and
          toks[i + 1][1].upper() == 'LIKE' and out):
        lhs = out.pop()
        pat = toks[i + 2][1]
        out.append(f'(not sql_like({lhs}, {pat}))')
        i += 3
        continue
      if tt == tokenize.NAME and tu == 'LIKE' and i + 1 < n and out:
        lhs = out.pop()
        pat = toks[i + 1][1]
        out.append(f'sql_like({lhs}, {pat})')
        i += 2
        continue
      if tt == tokenize.OP and tv == '=':
        out.append('==')
      elif tt == tokenize.NAME and tu == 'AND':
        out.append('and')
      elif tt == tokenize.NAME and tu == 'OR':
        out.append('or')
      elif tt == tokenize.NAME and tu == 'NOT':
        out.append('not')
      elif tt == tokenize.NAME and tu == 'IN':
        out.append('in')
      elif tt == tokenize.NAME and tu == 'TRUE':
        out.append('True')
      elif tt == tokenize.NAME and tu == 'FALSE':
        out.append('False')
      elif tt == tokenize.NAME and tu == 'NULL':
        out.append('None')
      else:
        out.append(tv)
      i += 1
    return out

  pieces = _norm_tokens(raw_toks)
  joined: list[str] = []
  for idx, piece in enumerate(pieces):
    if idx > 0:
      prev = pieces[idx - 1]
      if prev != '.' and piece not in ('.', ',', ')', ']') and prev not in (
          '(', '['):
        joined.append(' ')
    joined.append(piece)
  return ''.join(joined)


def parse_expectation_rule(
    spec: str | dict[str, Any],
    default_on_violation: str = 'dlq',
) -> ExpectationRule:
  """Parses and AST-validates a string or dict into an `ExpectationRule`."""
  if default_on_violation not in _ACTION_CHOICES:
    default_on_violation = 'dlq'

  if isinstance(spec, dict):
    expr = str(spec.get('expression', '')).strip()
    if not expr:
      raise ValueError(
          f"Expectation rule dictionary missing 'expression': {spec!r}")
    action = str(spec.get('on_violation', default_on_violation)).strip().lower()
    if action not in _ACTION_CHOICES:
      raise ValueError(
          f'Invalid on_violation action {action!r}; must be one of {_ACTION_CHOICES}'
      )
    name = str(spec.get('name', '')).strip() or expr
    norm_expr = normalize_expectation_expression(expr)
    _validate_expectation_ast(norm_expr)
    return ExpectationRule(name=name, expression=norm_expr, on_violation=action)

  raw = str(spec).strip()
  if not raw:
    raise ValueError('Empty expectation rule specification')

  action = default_on_violation
  if '->' in raw:
    raw_expr, raw_action = raw.rsplit('->', 1)
    raw = raw_expr.strip()
    candidate_action = raw_action.strip().lower()
    if candidate_action in _ACTION_CHOICES:
      action = candidate_action
    else:
      raise ValueError(
          f'Invalid action {candidate_action!r} in rule spec; must be one of {_ACTION_CHOICES}'
      )

  if ':' in raw:
    prefix, body = raw.split(':', 1)
    prefix_clean = prefix.strip()
    if prefix_clean.isidentifier():
      name = prefix_clean
      expr = body.strip()
    else:
      name = raw
      expr = raw
  else:
    name = raw
    expr = raw

  norm_expr = normalize_expectation_expression(expr)
  _validate_expectation_ast(norm_expr)
  return ExpectationRule(name=name, expression=norm_expr, on_violation=action)


def compile_expectation_expression(expr: str) -> Any:
  """AST-validates and compiles a normalized expectation expression."""
  parsed = _validate_expectation_ast(expr)
  return compile(parsed, '<expectation>', 'eval')


def evaluate_record(
    compiled_code: Any,
    row: Any,
) -> bool:
  """Safely evaluates an AST-validated boolean expression against a row or dict."""
  if isinstance(compiled_code, str):
    compiled_code = compile_expectation_expression(compiled_code)
  elif hasattr(compiled_code, 'co_names'):
    if any(str(n).startswith('_') for n in compiled_code.co_names):
      raise ValueError('Underscore attribute/name access is disallowed')
  if hasattr(row, '_asdict'):
    row_dict = row._asdict()
  elif isinstance(row, dict):
    row_dict = row
  else:
    row_dict = {k: getattr(row, k) for k in dir(row) if not k.startswith('_')}

  scope = dict(row_dict)
  scope['row'] = row
  scope['element'] = row
  scope['record'] = row

  try:
    result = eval(compiled_code, {'__builtins__': _SAFE_BUILTINS}, scope)  # pylint: disable=eval-used
    return bool(result)
  except Exception:  # pylint: disable=broad-except
    return False


def cli_expect(
    rules: list[dict[str, Any] | str] | dict[str, Any],
    error_handling: dict[str, Any] | None = None,
    redact_fields: list[str] | tuple[str, ...] = (),
):
  """Beam YAML factory for the `CliExpect` transform."""
  import apache_beam as beam
  from apache_beam import metrics
  from apache_beam import pvalue
  from . import dlq

  parsed_rules: list[ExpectationRule] = []
  if isinstance(rules, list):
    for r in rules:
      parsed_rules.append(parse_expectation_rule(r))
  elif isinstance(rules, dict):
    if 'rules' in rules and isinstance(rules['rules'], list):
      def_act = str(rules.get('on_violation', 'dlq'))
      for r in rules['rules']:
        parsed_rules.append(
            parse_expectation_rule(r, default_on_violation=def_act))
    else:
      parsed_rules.append(parse_expectation_rule(rules))
  else:
    raise ValueError(f'Invalid rules specification for CliExpect: {rules!r}')

  err_tag = (error_handling or {}).get('output', 'errors')
  route_errors = bool(error_handling and 'output' in error_handling)
  redact_tuple = tuple(str(f) for f in (redact_fields or ()) if str(f).strip())

  class _ExpectDoFn(beam.DoFn):
    def __init__(
        self,
        rules_list: list[ExpectationRule],
        error_tag: str,
        routing: bool,
    ):
      self.rules = rules_list
      self.error_tag = error_tag
      self.routing = routing
      self._compiled: list[tuple[ExpectationRule, Any]] = []

    def setup(self):
      self._compiled = [(r, compile_expectation_expression(r.expression))
                        for r in self.rules]
      self._ctr_eval = metrics.Metrics.counter(
          'beam_cli', 'expectations_evaluated')
      self._ctr_violations = metrics.Metrics.counter(
          'beam_cli', 'expectation_violations_total')
      self._ctr_dropped_rows = metrics.Metrics.counter(
          'beam_cli', 'expectation_dropped_rows')
      self._ctr_quarantined_rows = metrics.Metrics.counter(
          'beam_cli', 'expectation_quarantined_rows')
      self._passed_ctrs = {
          r.name: metrics.Metrics.counter('beam_cli', f'expectation_passed_{r.name}')
          for r in self.rules
      }
      self._quarantined_ctrs = {
          r.name: metrics.Metrics.counter(
              'beam_cli', f'expectation_quarantined_{r.name}')
          for r in self.rules
      }
      self._dropped_ctrs = {
          r.name: metrics.Metrics.counter(
              'beam_cli', f'expectation_dropped_{r.name}')
          for r in self.rules
      }
      self._failed_ctrs = {
          r.name: metrics.Metrics.counter('beam_cli', f'expectation_failed_{r.name}')
          for r in self.rules
      }

    def process(self, element: Any) -> Any:
      self._ctr_eval.inc()
      failed_rule: ExpectationRule | None = None

      for rule, code in self._compiled:
        if evaluate_record(code, element):
          self._passed_ctrs[rule.name].inc()
        else:
          failed_rule = rule
          break

      if failed_rule is None:
        yield element
        return

      self._ctr_violations.inc()
      action = failed_rule.on_violation

      if action == 'fail':
        self._failed_ctrs[failed_rule.name].inc()
        raise ExpectationViolationError(
            f'Expectation rule {failed_rule.name!r} ({failed_rule.expression}) '
            'violated')

      if action == 'dlq':
        self._quarantined_ctrs[failed_rule.name].inc()
        self._ctr_quarantined_rows.inc()
        if not self.routing:
          raise ExpectationViolationError(
              f'Expectation rule {failed_rule.name!r} violated and no DLQ route '
              'configured')
        err_row = dlq.make_transform_dlq_row(
            'CliExpect',
            'ExpectationViolation',
            f'Rule {failed_rule.name!r} violated: {failed_rule.expression}',
            element,
            redact_fields=redact_tuple)
        yield pvalue.TaggedOutput(self.error_tag, err_row)
        return

      if action == 'drop':
        self._dropped_ctrs[failed_rule.name].inc()
        self._ctr_dropped_rows.inc()
        return

  dlq_row_type = dlq.transform_dlq_row_type()

  class _CliExpectTransform(beam.PTransform):
    def expand(self, pcoll):
      outputs = (
          pcoll
          | 'EvaluateExpectations' >> beam.ParDo(
              _ExpectDoFn(parsed_rules, err_tag, route_errors)
          ).with_outputs(err_tag, main='good')
      )
      good = outputs['good']
      bad = outputs[err_tag]
      if getattr(pcoll, 'element_type', None) is not None:
        good.element_type = pcoll.element_type
      bad.element_type = dlq_row_type
      if route_errors:
        return {'good': good, err_tag: bad}
      return good

  return _CliExpectTransform()
