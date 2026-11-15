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


_ALLOWED_EXPECT_AST_NODES = (
    ast.Expression,
    ast.BoolOp,
    ast.And,
    ast.Or,
    ast.UnaryOp,
    ast.Not,
    ast.UAdd,
    ast.USub,
    ast.BinOp,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.FloorDiv,
    ast.Mod,
    ast.Compare,
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
    ast.Is,
    ast.IsNot,
    ast.In,
    ast.NotIn,
    ast.IfExp,
    ast.Call,
    ast.Attribute,
    ast.Subscript,
    ast.Constant,
    ast.Name,
    ast.Load,
    ast.List,
    ast.Tuple,
)


def _validate_expectation_ast(expr: str) -> ast.Expression:
  """Validates an expectation expression AST using a strict allowlist."""
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

  if _depth(parsed) > 16:
    raise ValueError(
        'Expectation expression exceeds maximum AST nesting depth (16)')
  nodes = list(ast.walk(parsed))
  if len(nodes) > 128:
    raise ValueError(
        'Expectation expression exceeds maximum AST node count (128)')

  for node in nodes:
    if not isinstance(node, _ALLOWED_EXPECT_AST_NODES):
      raise ValueError(
          f'Disallowed construct {type(node).__name__} in expectation {expr!r}')
    if isinstance(node, ast.Name) and node.id.startswith('_'):
      raise ValueError(
          f'Underscore-prefixed identifier {node.id!r} is disallowed in expectation'
      )
    if isinstance(node, ast.Attribute):
      if node.attr.startswith('_') or node.attr in ('format', 'format_map'):
        raise ValueError(
            f'Disallowed attribute {node.attr!r} in expectation {expr!r}')
    if isinstance(node, ast.Subscript):
      if not isinstance(node.slice, ast.Constant) or (
          isinstance(node.slice.value, str) and
          node.slice.value.startswith('_')):
        raise ValueError(
            f'Disallowed subscript key in expectation {expr!r}')
    if isinstance(node, ast.Call):
      if node.keywords:
        raise ValueError(
            f'Keyword arguments are disallowed in expectation {expr!r}')
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


def _sql_like_fn(val: Any, pattern: Any) -> bool | None:
  """Linear/quadratic two-pointer SQL LIKE matcher (zero regex ReDoS)."""
  if val is None or pattern is None:
    return None
  s = str(val)
  raw_p = str(pattern)
  if len(s) > 65536 or len(raw_p) > 4096:
    return False
  # Tokenize pattern into ('*', '') | ('?', '') | ('L', char) with `\` escape,
  # collapsing consecutive `%` wildcards.
  ptoks: list[tuple[str, str]] = []
  idx = 0
  plen = len(raw_p)
  while idx < plen:
    ch = raw_p[idx]
    if ch == '\\' and idx + 1 < plen:
      ptoks.append(('L', raw_p[idx + 1]))
      idx += 2
    elif ch == '%':
      if not ptoks or ptoks[-1][0] != '*':
        ptoks.append(('*', ''))
      idx += 1
    elif ch == '_':
      ptoks.append(('?', ''))
      idx += 1
    else:
      ptoks.append(('L', ch))
      idx += 1

  si = 0
  pi = 0
  slen = len(s)
  ntoks = len(ptoks)
  star_pi = -1
  star_si = -1
  while si < slen:
    if pi < ntoks and (ptoks[pi][0] == '?' or
                       (ptoks[pi][0] == 'L' and ptoks[pi][1] == s[si])):
      si += 1
      pi += 1
    elif pi < ntoks and ptoks[pi][0] == '*':
      star_pi = pi
      star_si = si
      pi += 1
    elif star_pi != -1:
      pi = star_pi + 1
      star_si += 1
      si = star_si
    else:
      return False
  while pi < ntoks and ptoks[pi][0] == '*':
    pi += 1
  return pi == ntoks


def _exp_mul(a: Any, b: Any) -> Any:
  if a is None or b is None:
    return None
  if isinstance(a, (str, bytes, list, tuple)) or isinstance(
      b, (str, bytes, list, tuple)):
    raise ValueError('sequence repetition (*) is disallowed in expectations')
  return a * b


def _exp_not(v: Any) -> bool | None:
  if v is None:
    return None
  return not bool(v)


def _exp_and(vals: list[Any]) -> bool | None:
  saw_none = False
  for v in vals:
    if v is None:
      saw_none = True
    elif not bool(v):
      return False
  return None if saw_none else True


def _exp_or(vals: list[Any]) -> bool | None:
  saw_none = False
  for v in vals:
    if v is None:
      saw_none = True
    elif bool(v):
      return True
  return None if saw_none else False


def _exp_cmp(op_name: str, lhs: Any, rhs: Any) -> bool | None:
  if op_name == 'is':
    return lhs is rhs
  if op_name == 'is_not':
    return lhs is not rhs
  if lhs is None:
    return None
  if op_name in ('in', 'not_in'):
    if rhs is None:
      return None
    if isinstance(rhs, str):
      res: bool | None = str(lhs) in rhs
    else:
      saw_null = False
      matched = False
      for item in rhs:
        if item is None:
          saw_null = True
        elif lhs == item:
          matched = True
          break
      res = True if matched else (None if saw_null else False)
    return _exp_not(res) if op_name == 'not_in' else res
  if rhs is None:
    return None
  if op_name == 'eq':
    return lhs == rhs
  if op_name == 'ne':
    return lhs != rhs
  if op_name == 'lt':
    return lhs < rhs
  if op_name == 'le':
    return lhs <= rhs
  if op_name == 'gt':
    return lhs > rhs
  if op_name == 'ge':
    return lhs >= rhs
  return False


class _Expect3VLTransformer(ast.NodeTransformer):
  """Rewrites expectation AST to enforce SQL 3VL on NULLs and block sequence `*`."""

  _CMP_MAP = {
      ast.Eq: 'eq',
      ast.NotEq: 'ne',
      ast.Lt: 'lt',
      ast.LtE: 'le',
      ast.Gt: 'gt',
      ast.GtE: 'ge',
      ast.Is: 'is',
      ast.IsNot: 'is_not',
      ast.In: 'in',
      ast.NotIn: 'not_in',
  }

  def visit_BinOp(self, node: ast.BinOp) -> ast.AST:
    self.generic_visit(node)
    if isinstance(node.op, ast.Mult):
      return ast.Call(
          func=ast.Name(id='_cli_exp_mul', ctx=ast.Load()),
          args=[node.left, node.right],
          keywords=[])
    return node

  def visit_UnaryOp(self, node: ast.UnaryOp) -> ast.AST:
    self.generic_visit(node)
    if isinstance(node.op, ast.Not):
      return ast.Call(
          func=ast.Name(id='_cli_exp_not', ctx=ast.Load()),
          args=[node.operand],
          keywords=[])
    return node

  def visit_BoolOp(self, node: ast.BoolOp) -> ast.AST:
    self.generic_visit(node)
    fn_id = '_cli_exp_and' if isinstance(node.op, ast.And) else '_cli_exp_or'
    return ast.Call(
        func=ast.Name(id=fn_id, ctx=ast.Load()),
        args=[ast.List(elts=node.values, ctx=ast.Load())],
        keywords=[])

  def visit_Compare(self, node: ast.Compare) -> ast.AST:
    self.generic_visit(node)
    parts: list[ast.expr] = []
    cur_left = node.left
    for op, comp in zip(node.ops, node.comparators):
      op_str = self._CMP_MAP.get(type(op))
      if op_str is None:
        return node
      parts.append(
          ast.Call(
              func=ast.Name(id='_cli_exp_cmp', ctx=ast.Load()),
              args=[ast.Constant(value=op_str), cur_left, comp],
              keywords=[]))
      cur_left = comp
    if len(parts) == 1:
      return parts[0]
    return ast.Call(
        func=ast.Name(id='_cli_exp_and', ctx=ast.Load()),
        args=[ast.List(elts=parts, ctx=ast.Load())],
        keywords=[])


_SAFE_BUILTINS.update({
    'ifnull': _SAFE_BUILTINS['coalesce'],
    'trim': _SAFE_BUILTINS['strip'],
    'length': lambda s: None if s is None else len(s),
    'len': lambda s: None if s is None else len(s),
    'lower': lambda s: None if s is None else str(s).lower(),
    'upper': lambda s: None if s is None else str(s).upper(),
    'strip': lambda s: None if s is None else str(s).strip(),
    'sql_like': _sql_like_fn,
    '_cli_exp_mul': _exp_mul,
    '_cli_exp_not': _exp_not,
    '_cli_exp_and': _exp_and,
    '_cli_exp_or': _exp_or,
    '_cli_exp_cmp': _exp_cmp,
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


def _join_expect_pieces(pieces: list[str]) -> str:
  joined: list[str] = []
  for idx, piece in enumerate(pieces):
    if idx > 0:
      prev = pieces[idx - 1]
      if prev != '.' and piece not in ('.', ',', ')', ']') and prev not in (
          '(', '['):
        joined.append(' ')
    joined.append(piece)
  return ''.join(joined)


def _pop_trailing_atom(out: list[str]) -> str:
  """Pops a complete trailing primary atom (`a`, `a.b`, `a.lower()`, `(x)`) from `out`."""
  picked: list[str] = []
  while out:
    tok = out.pop()
    picked.append(tok)
    if tok in (')', ']'):
      open_tok = '(' if tok == ')' else '['
      depth = 1
      while out and depth > 0:
        t2 = out.pop()
        picked.append(t2)
        if t2 == tok:
          depth += 1
        elif t2 == open_tok:
          depth -= 1
      # Include optional callable/attribute prefix before `(` / `[`
      while out and (out[-1] == '.' or out[-1].isidentifier()):
        picked.append(out.pop())
      break
    if out and out[-1] == '.':
      picked.append(out.pop())
      continue
    break
  picked.reverse()
  return _join_expect_pieces(picked)


def normalize_expectation_expression(expr: str) -> str:
  """Token-safely normalizes SQL syntax (`=`, `<>`, `LIKE`, `IN`, `CAST`, `IS NULL`)."""
  import io
  import tokenize
  cleaned = expr.strip()
  if not cleaned:
    return cleaned
  # Strip backticks around bare identifiers only outside string literals.
  try:
    # Replace backticked identifiers safely via regex only when not inside quotes.
    cleaned_bt = re.sub(
        r"""('(?:\\.|[^'])*'|"(?:\\.|[^"])*")|`([A-Za-z_][A-Za-z0-9_]*)`""",
        lambda m: m.group(1) if m.group(1) is not None else m.group(2),
        cleaned)
    raw_toks = [
        (t.type, t.string)
        for t in tokenize.generate_tokens(io.StringIO(cleaned_bt).readline)
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
      # `<>` operator (single OP token `'<>'` in Python `tokenize`, or `'<'` + `'>'`)
      if tt == tokenize.OP and tv == '<>':
        out.append('!=')
        i += 1
        continue
      if (tt == tokenize.OP and tv == '<' and i + 1 < n and
          toks[i + 1] == (tokenize.OP, '>')):
        out.append('!=')
        i += 2
        continue
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
          inner_str = _join_expect_pieces(_norm_tokens(toks[i + 2:as_idx]))
          out.append(f'{py_fn}({inner_str})')
          i = j + 1
          continue
      # `[NOT] IN ( ... )` -> rewrite parenthesized SQL value list to `[...]`
      if (tt == tokenize.NAME and tu == 'NOT' and i + 2 < n and
          toks[i + 1][0] == tokenize.NAME and toks[i + 1][1].upper() == 'IN'
          and toks[i + 2] == (tokenize.OP, '(')):
        depth = 1
        j = i + 3
        while j < n and depth > 0:
          if toks[j][0] == tokenize.OP and toks[j][1] in ('(', '[', '{'):
            depth += 1
          elif toks[j][0] == tokenize.OP and toks[j][1] in (')', ']', '}'):
            depth -= 1
            if depth == 0:
              break
          j += 1
        if depth == 0:
          inner = _norm_tokens(toks[i + 3:j])
          out.extend(['not', 'in', '[', *inner, ']'])
          i = j + 1
          continue
      if (tt == tokenize.NAME and tu == 'IN' and i + 1 < n and
          toks[i + 1] == (tokenize.OP, '(')):
        depth = 1
        j = i + 2
        while j < n and depth > 0:
          if toks[j][0] == tokenize.OP and toks[j][1] in ('(', '[', '{'):
            depth += 1
          elif toks[j][0] == tokenize.OP and toks[j][1] in (')', ']', '}'):
            depth -= 1
            if depth == 0:
              break
          j += 1
        if depth == 0:
          inner = _norm_tokens(toks[i + 2:j])
          out.extend(['in', '[', *inner, ']'])
          i = j + 1
          continue
      # `[NOT] LIKE <pattern>`
      if (tt == tokenize.NAME and tu == 'NOT' and i + 2 < n and
          toks[i + 1][0] == tokenize.NAME and
          toks[i + 1][1].upper() == 'LIKE' and out):
        lhs = _pop_trailing_atom(out)
        pat = toks[i + 2][1]
        out.append(f'(not sql_like({lhs}, {pat}))')
        i += 3
        continue
      if tt == tokenize.NAME and tu == 'LIKE' and i + 1 < n and out:
        lhs = _pop_trailing_atom(out)
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

  return _join_expect_pieces(_norm_tokens(raw_toks))


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
  """AST-validates, applies SQL 3VL null-safety, and compiles an expectation."""
  parsed = _validate_expectation_ast(expr)
  transformed = ast.fix_missing_locations(_Expect3VLTransformer().visit(parsed))
  return compile(transformed, '<expectation>', 'eval')


def evaluate_record(
    compiled_code: Any,
    row: Any,
) -> bool:
  """Safely evaluates an AST-validated boolean expression against a row or dict."""
  if isinstance(compiled_code, str):
    compiled_code = compile_expectation_expression(compiled_code)
  elif hasattr(compiled_code, 'co_names'):
    if any(
        str(n).startswith('_') and not str(n).startswith('_cli_exp_')
        for n in compiled_code.co_names):
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
    return result is True or (result is not None and bool(result))
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
