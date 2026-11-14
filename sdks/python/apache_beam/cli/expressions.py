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

"""Safe tokenized AST expression compiler for `beam pipe --where` / `--select`.

Compiles user/agent filter and projection expressions into Beam YAML
`language: python` `callable:` definitions using `getattr(__row__, 'col')`:
- Bypasses `yaml_mapping.py`'s substring `if name in expression:` unroller bug.
- Rewrites SQL-style `AND`/`OR`/`NOT`, `IS NULL`, and `IS NOT NULL` via
  `tokenize.generate_tokens` without corrupting string literals.
- Enforces strict AST safety: max 64 nodes, max depth 12, no `**` / `<<` /
  bitwise ops, no sequence/string `*` repetition DoS, no `_`-prefixed names or
  attributes, and only whitelisted scalar functions/string methods.
- Treats NULL like SQL (three-valued logic): every comparison (`=`, `!=`,
  `<`, `IN`, ...), arithmetic, `NOT`, nested field access (`user.tier`),
  string functions and methods, and `int`/`float`/`abs`/`round`/`str` give
  NULL (None) when an operand is NULL, and `AND`/`OR` follow SQL truth tables
  while still short-circuiting. So `--where "state != 'CA'"` drops rows with
  no state, as in SQL. Only `IS [NOT] NULL` tests for NULL; `x = NULL` is
  read as `x IS NULL`. `x IN ('CA')` is a one-element list, never a substring
  test.
"""

import ast
import io
import keyword
import re
import tokenize
from typing import Any
from typing import NamedTuple

MAX_AST_NODES = 64
MAX_AST_DEPTH = 12
MAX_STRING_LITERAL_LEN = 4096

_IDENT_RE = re.compile(r'^[A-Za-z][A-Za-z0-9_]*$')

_SAFE_FUNCS = frozenset([
    'abs',
    'bool',
    'coalesce',
    'float',
    'ifnull',
    'int',
    'len',
    'length',
    'lower',
    'max',
    'min',
    'round',
    'str',
    'strip',
    'trim',
    'upper',
])

_SQL_CAST_TYPES: dict[str, str] = {
    'int': 'int',
    'integer': 'int',
    'int64': 'int',
    'int32': 'int',
    'smallint': 'int',
    'bigint': 'int',
    'float': 'float',
    'float64': 'float',
    'float32': 'float',
    'double': 'float',
    'real': 'float',
    'numeric': 'float',
    'decimal': 'float',
    'str': 'str',
    'string': 'str',
    'varchar': 'str',
    'char': 'str',
    'text': 'str',
    'bool': 'bool',
    'boolean': 'bool',
}

# Python keywords that retain expression syntax meaning and must NOT be treated
# as bare column identifiers (use backticks `` `in` `` if a column is named `in`).
_EXPR_KEYWORDS = frozenset([
    'and',
    'or',
    'not',
    'in',
    'is',
    'if',
    'else',
    'True',
    'False',
    'None',
    'lambda',
    'await',
    'async',
])

_SAFE_STR_METHODS = frozenset([
    'endswith',
    'lower',
    'lstrip',
    'replace',
    'rstrip',
    'startswith',
    'strip',
    'upper',
])

_ALLOWED_BINOPS = (
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.FloorDiv,
    ast.Mod,
)
_ALLOWED_UNARYOPS = (ast.Not, ast.UAdd, ast.USub)
_ALLOWED_CMPOPS = (
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
    ast.In,
    ast.NotIn,
    ast.Is,
    ast.IsNot,
)


class ExpressionError(ValueError):
  """Raised when a `--where` or `--select` expression fails safety/syntax."""


class CompiledField(NamedTuple):
  alias: str
  spec: Any
  referenced_columns: frozenset[str]


def _extract_backtick_columns(raw: str, colref_map: dict[str, str]) -> str:
  """Replaces backtick-quoted `` `col` `` outside string literals with placeholders."""
  out: list[str] = []
  quote: str | None = None
  escaped = False
  i = 0
  n = len(raw)
  while i < n:
    ch = raw[i]
    if quote is not None:
      out.append(ch)
      if escaped:
        escaped = False
      elif ch == '\\':
        escaped = True
      elif ch == quote:
        quote = None
      i += 1
      continue
    if ch in ("'", '"'):
      quote = ch
      out.append(ch)
      i += 1
      continue
    if ch == '#':
      out.append(raw[i:])
      break
    if ch == '`':
      end = raw.find('`', i + 1)
      if end == -1:
        raise ExpressionError('unterminated backtick identifier in expression')
      col_name = raw[i + 1:end]
      if not col_name:
        raise ExpressionError('empty backtick identifier in expression')
      placeholder = f'beam_colref__{len(colref_map)}'
      colref_map[placeholder] = col_name
      out.append(placeholder)
      i = end + 1
      continue
    out.append(ch)
    i += 1
  return ''.join(out)


def _validate_jinja_syntax(raw: str) -> None:
  """Validates `{{ ... }}` Jinja2 syntax via AST without rendering/executing."""
  if '{%' in raw or '%}' in raw or '{#' in raw or '#}' in raw:
    raise ExpressionError(
        'Jinja control/comment blocks ({% ... %}, {# ... #}) are not allowed '
        'in --where/--select; use {{ variable }} value placeholders')
  try:
    import jinja2
    from jinja2 import nodes as jnodes
  except ImportError:
    return
  try:
    parsed_j = jinja2.Environment().parse(raw)
  except jinja2.TemplateSyntaxError as e:
    raise ExpressionError(
        f'invalid Jinja2 placeholder syntax in expression: {e.message}'
    ) from None
  for jnode in parsed_j.find_all((jnodes.Getattr, jnodes.Getitem, jnodes.Call)):
    raise ExpressionError(
        f'disallowed Jinja2 syntax {type(jnode).__name__} in expression; '
        'only simple {{ param }} or {{ param | default(...) }} placeholders '
        'are permitted')
  for nnode in parsed_j.find_all(jnodes.Name):
    if str(nnode.name).startswith('_'):
      raise ExpressionError(
          f'underscore-prefixed Jinja2 variable {nnode.name!r} is not allowed')


def _extract_jinja_placeholders(raw: str, jinja_map: dict[str, str]) -> str:
  """Replaces unquoted `{{ ... }}` blocks with safe AST identifier tokens."""
  _validate_jinja_syntax(raw)
  out: list[str] = []
  quote: str | None = None
  escaped = False
  i = 0
  n = len(raw)
  while i < n:
    ch = raw[i]
    if quote is not None:
      out.append(ch)
      if escaped:
        escaped = False
      elif ch == '\\':
        escaped = True
      elif ch == quote:
        quote = None
      i += 1
      continue
    if ch in ("'", '"', '`'):
      quote = ch
      out.append(ch)
      i += 1
      continue
    if ch == '#':
      out.append(raw[i:])
      break
    if raw[i:i + 2] == '{{':
      j = i + 2
      in_q: str | None = None
      in_esc = False
      end = -1
      while j < n:
        jc = raw[j]
        if in_q is not None:
          if in_esc:
            in_esc = False
          elif jc == '\\':
            in_esc = True
          elif jc == in_q:
            in_q = None
          j += 1
          continue
        if jc in ("'", '"'):
          in_q = jc
          j += 1
          continue
        if raw[j:j + 2] == '}}':
          end = j
          break
        j += 1
      if end == -1:
        raise ExpressionError(
            'unclosed Jinja2 {{ ... }} placeholder in expression')
      inner = raw[i + 2:end].strip()
      if not inner:
        raise ExpressionError('empty Jinja2 {{ }} placeholder in expression')
      placeholder = f'beam_jinja__{len(jinja_map)}'
      jinja_map[placeholder] = raw[i:end + 2]
      out.append(f' {placeholder} ')
      i = end + 2
      continue
    out.append(ch)
    i += 1
  return ''.join(out)


def _rewrite_sql_cast_tokens(
    tokens: list[tuple[int, str]]) -> list[tuple[int, str]]:
  """Rewrites SQL `CAST(expr AS TYPE)` tokens into Python `int/float/str/bool(expr)`."""
  out: list[tuple[int, str]] = []
  i = 0
  n = len(tokens)
  while i < n:
    ttype, tval = tokens[i]
    if (ttype == tokenize.NAME and tval.upper() == 'CAST' and i + 1 < n and
        tokens[i + 1] == (tokenize.OP, '(')):
      # Find matching closing ')'
      depth = 1
      j = i + 2
      top_level_as_idx = -1
      while j < n and depth > 0:
        jt, jv = tokens[j]
        if jt == tokenize.OP and jv in ('(', '[', '{'):
          depth += 1
        elif jt == tokenize.OP and jv in (')', ']', '}'):
          depth -= 1
          if depth == 0:
            break
        elif depth == 1 and jt == tokenize.NAME and jv.upper() == 'AS':
          top_level_as_idx = j
        j += 1
      if depth != 0:
        raise ExpressionError('unclosed parenthesis in CAST(...) expression')
      if top_level_as_idx == -1:
        raise ExpressionError(
            'CAST(...) requires AS <TYPE>, e.g. CAST(order_id AS INT)')
      inner_tokens = tokens[i + 2:top_level_as_idx]
      type_tokens = tokens[top_level_as_idx + 1:j]
      if not inner_tokens or not type_tokens or type_tokens[0][0] != tokenize.NAME:
        raise ExpressionError('invalid CAST(expr AS TYPE) syntax')
      sql_type_name = type_tokens[0][1].lower()
      # Allow optional precision/scale parens like `DECIMAL(10, 2)` or `VARCHAR(64)`.
      if len(type_tokens) > 1:
        if not (type_tokens[1] == (tokenize.OP, '(') and
                type_tokens[-1] == (tokenize.OP, ')')):
          raise ExpressionError(
              f'invalid type specification in CAST: '
              f'{" ".join(t[1] for t in type_tokens)!r}')
      py_fn = _SQL_CAST_TYPES.get(sql_type_name)
      if py_fn is None:
        raise ExpressionError(
            f'unsupported CAST target type {type_tokens[0][1]!r}; supported: '
            'INT, FLOAT, DECIMAL, STRING, BOOL')
      rewritten_inner = _rewrite_sql_cast_tokens(inner_tokens)
      out.append((tokenize.NAME, py_fn))
      out.append((tokenize.OP, '('))
      out.extend(rewritten_inner)
      out.append((tokenize.OP, ')'))
      i = j + 1
      continue
    out.append((ttype, tval))
    i += 1
  return out


def _lex_and_normalize_with_colmap(
    raw: str) -> tuple[str, dict[str, str]]:
  """Normalizes SQL-like tokens (`=`, `<>`, `CAST`, `AND`/`OR`, `IS NULL`, keywords)."""
  if not raw or not raw.strip():
    raise ExpressionError('expression cannot be empty')
  if len(raw) > 8192:
    raise ExpressionError('expression exceeds maximum length (8192 chars)')

  colref_map: dict[str, str] = {}
  pre_raw = _extract_backtick_columns(raw, colref_map)
  # Support SQL `<>` inequality before Python tokenizer sees it.
  tokens: list[tuple[int, str]] = []
  try:
    raw_toks = list(tokenize.generate_tokens(io.StringIO(pre_raw).readline))
  except tokenize.TokenError as e:
    raise ExpressionError(f'unterminated string or bracket in expression: {e}'
                          ) from None

  k = 0
  while k < len(raw_toks):
    tok = raw_toks[k]
    if tok.type in (
        tokenize.ENCODING,
        tokenize.ENDMARKER,
        tokenize.NL,
        tokenize.NEWLINE,
        tokenize.COMMENT,
    ):
      k += 1
      continue
    if (tok.type == tokenize.OP and tok.string == '<' and
        k + 1 < len(raw_toks) and raw_toks[k + 1].type == tokenize.OP and
        raw_toks[k + 1].string == '>'):
      tokens.append((tokenize.OP, '!='))
      k += 2
      continue
    if tok.type == tokenize.OP and tok.string == '<>':
      tokens.append((tokenize.OP, '!='))
      k += 1
      continue
    if tok.type == tokenize.OP and tok.string == '=':
      tokens.append((tokenize.OP, '=='))
      k += 1
      continue
    if tok.type == tokenize.OP and tok.string in ('{', '}'):
      raise ExpressionError(
          'braces / template expressions are not allowed in --where/--select')
    tokens.append((tok.type, tok.string))
    k += 1

  if not tokens:
    raise ExpressionError('expression cannot be empty')

  tokens = _rewrite_sql_cast_tokens(tokens)

  rewritten: list[str] = []
  i = 0
  while i < len(tokens):
    ttype, tval = tokens[i]
    if ttype == tokenize.NAME:
      upper = tval.upper()
      # Rewrite `IS NOT NULL` -> `is not None` and `IS NULL` -> `is None`.
      if (upper == 'IS' and i + 2 < len(tokens) and
          tokens[i + 1][0] == tokenize.NAME and
          tokens[i + 1][1].upper() == 'NOT' and
          tokens[i + 2][0] == tokenize.NAME and
          tokens[i + 2][1].upper() == 'NULL'):
        rewritten.extend(['is', 'not', 'None'])
        i += 3
        continue
      if (upper == 'IS' and i + 1 < len(tokens) and
          tokens[i + 1][0] == tokenize.NAME and
          tokens[i + 1][1].upper() == 'NULL'):
        rewritten.extend(['is', 'None'])
        i += 2
        continue
      if upper in ('AND', 'OR', 'NOT', 'IN'):
        rewritten.append(upper.lower())
        i += 1
        continue
      if upper == 'NULL':
        rewritten.append('None')
        i += 1
        continue
      if upper == 'TRUE':
        rewritten.append('True')
        i += 1
        continue
      if upper == 'FALSE':
        rewritten.append('False')
        i += 1
        continue
      if keyword.iskeyword(tval) and tval not in _EXPR_KEYWORDS:
        placeholder = f'beam_colref__{len(colref_map)}'
        colref_map[placeholder] = tval
        rewritten.append(placeholder)
        i += 1
        continue
    rewritten.append(tval)
    i += 1

  # Join tokens with spaces where needed so adjacent identifiers/keywords never
  # fuse while operators and parentheses stay valid Python syntax.
  out: list[str] = []
  for idx, piece in enumerate(rewritten):
    if idx > 0:
      prev = rewritten[idx - 1]
      if prev != '.' and piece not in ('.', ',', ')', ']') and prev not in (
          '(', '['):
        out.append(' ')
    out.append(piece)
  return ''.join(out), colref_map


def _lex_and_normalize(raw: str) -> str:
  normalized, _ = _lex_and_normalize_with_colmap(raw)
  return normalized


def _ast_stats(node: ast.AST, depth: int = 1) -> tuple[int, int]:
  count = 1
  max_d = depth
  for child in ast.iter_child_nodes(node):
    c_count, c_depth = _ast_stats(child, depth + 1)
    count += c_count
    if c_depth > max_d:
      max_d = c_depth
  return count, max_d


def _is_literal(node: ast.AST) -> bool:
  """True for constants, signed numbers, and tuples/lists/sets of them."""
  if isinstance(node, ast.Constant):
    return True
  if (isinstance(node, ast.UnaryOp) and
      isinstance(node.op, (ast.USub, ast.UAdd)) and
      isinstance(node.operand, ast.Constant)):
    return True
  if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
    return all(_is_literal(e) for e in node.elts)
  return False


def _normalize_comparison(node: ast.Compare):
  """Returns `(ops, comparators)` with SQL meanings made explicit.

  `x = NULL` / `x != NULL` become `IS [NOT] NULL` (what users mean), and
  `x IN ('CA')` is a one-element list: Python would parse `('CA')` as the
  string 'CA' and turn IN into a substring test that matches 'C'.
  """
  ops = list(node.ops)
  comparators = list(node.comparators)
  left = node.left
  for i, op in enumerate(ops):
    right = comparators[i]
    is_none = lambda n: isinstance(n, ast.Constant) and n.value is None
    if isinstance(op, (ast.Eq, ast.NotEq)) and (is_none(right) or
                                                 is_none(left)):
      ops[i] = ast.Is() if isinstance(op, ast.Eq) else ast.IsNot()
    elif isinstance(op, (ast.In, ast.NotIn)) and (
        isinstance(right, ast.Constant) or
        (isinstance(right, ast.UnaryOp) and _is_literal(right))):
      comparators[i] = ast.Tuple(elts=[right], ctx=ast.Load())
    left = right
  return ops, comparators


class _RowExprCompiler(ast.NodeTransformer):
  """Validates AST safety and rewrites column `Name` nodes to `getattr`."""
  def __init__(
      self,
      colref_map: dict[str, str] | None = None,
      jinja_tokens: frozenset[str] | None = None,
  ) -> None:
    self.columns: set[str] = set()
    self.uses_safe_mul = False
    self.uses_coalesce = False
    self._colref_map = colref_map or {}
    self._jinja_tokens = jinja_tokens or frozenset()
    self._temp_count = 0
    # A --select field that is only a nested path (`user.name`) keeps plain
    # attribute access: Beam infers its type (Parquet sinks need it), and it
    # can't infer through the NULL check. A NULL parent then raises, which
    # --dlq routes.
    self.plain_attributes = False

  def _temp(self) -> str:
    self._temp_count += 1
    return f'_cli_t{self._temp_count}'

  def _null_safe(self, operands: list[ast.AST], build) -> ast.AST:
    """Returns `None if <any operand is None> else build(*operands)`.

    Non-constant operands are bound once to walrus temporaries, so they are
    evaluated once and Beam's type inference still sees the real operation.
    """
    tests = []
    refs = []
    for operand in operands:
      if isinstance(operand, ast.Constant):
        if operand.value is None:
          return ast.Constant(value=None)
        refs.append(operand)
        continue
      if _is_literal(operand):
        refs.append(operand)
        continue
      name = self._temp()
      tests.append(
          ast.Compare(
              left=ast.NamedExpr(
                  target=ast.Name(id=name, ctx=ast.Store()), value=operand),
              ops=[ast.Is()],
              comparators=[ast.Constant(value=None)]))
      refs.append(ast.Name(id=name, ctx=ast.Load()))
    body = build(*refs)
    if not tests:
      return body
    test = tests[0] if len(tests) == 1 else ast.BoolOp(op=ast.Or(), values=tests)
    return ast.IfExp(test=test, body=ast.Constant(value=None), orelse=body)

  def _bool3(self, op: ast.boolop, values: list[ast.AST]) -> ast.AST:
    """SQL AND/OR over `values`, short-circuiting like Python.

    AND is False as soon as one operand is False, NULL if any operand is NULL,
    else True; OR is True as soon as one is True, NULL if any is NULL, else
    False. Operands after a deciding one are not evaluated, so
    `amount != 0 AND 10 / amount > 1` still never divides by zero.
    """
    is_and = isinstance(op, ast.And)
    names = [self._temp() for _ in values]
    decided = ast.Constant(value=not is_and)
    none_test = ast.BoolOp(
        op=ast.Or(),
        values=[
            ast.Compare(
                left=ast.Name(id=n, ctx=ast.Load()),
                ops=[ast.Is()],
                comparators=[ast.Constant(value=None)]) for n in names
        ]) if len(names) > 1 else ast.Compare(
            left=ast.Name(id=names[0], ctx=ast.Load()),
            ops=[ast.Is()],
            comparators=[ast.Constant(value=None)])
    expr: ast.AST = ast.IfExp(
        test=none_test,
        body=ast.Constant(value=None),
        orelse=ast.Constant(value=is_and))
    for name, value in reversed(list(zip(names, values))):
      bound = ast.NamedExpr(
          target=ast.Name(id=name, ctx=ast.Store()), value=value)
      truth = ast.Name(id=name, ctx=ast.Load())
      if is_and:
        truth = ast.UnaryOp(op=ast.Not(), operand=truth)
      test = ast.BoolOp(
          op=ast.And(),
          values=[
              ast.Compare(
                  left=bound,
                  ops=[ast.IsNot()],
                  comparators=[ast.Constant(value=None)]),
              truth,
          ])
      expr = ast.IfExp(test=test, body=decided, orelse=expr)
    return expr

  def visit_BoolOp(self, node: ast.BoolOp) -> ast.AST:
    return self._bool3(node.op, [self.visit(v) for v in node.values])

  def visit(self, node: ast.AST) -> ast.AST:
    allowed = (
        ast.Expression,
        ast.BoolOp,
        ast.And,
        ast.Or,
        ast.UnaryOp,
        *_ALLOWED_UNARYOPS,
        ast.BinOp,
        *_ALLOWED_BINOPS,
        ast.Compare,
        *_ALLOWED_CMPOPS,
        ast.IfExp,
        ast.Constant,
        ast.Tuple,
        ast.List,
        ast.Set,
        ast.Load,
        ast.Name,
        ast.Attribute,
        ast.Call,
    )
    if not isinstance(node, allowed):
      raise ExpressionError(
          f'unsupported syntax {type(node).__name__} in expression')
    return super().visit(node)

  def visit_Constant(self, node: ast.Constant) -> ast.AST:
    val = node.value
    if val is not None and not isinstance(val, (bool, int, float, str)):
      raise ExpressionError(
          f'unsupported literal type {type(val).__name__} in expression')
    if isinstance(val, str) and len(val) > MAX_STRING_LITERAL_LEN:
      raise ExpressionError('string literal exceeds maximum length')
    return node

  def visit_BinOp(self, node: ast.BinOp) -> ast.AST:
    if not isinstance(node.op, _ALLOWED_BINOPS):
      raise ExpressionError(
          f'operator {type(node.op).__name__} is not allowed in expressions')
    if isinstance(node.op, ast.Mult):
      # Static fast-fail on literal sequence repetition (`'a' * 100`).
      for side in (node.left, node.right):
        if isinstance(side, (ast.List, ast.Tuple)) or (
            isinstance(side, ast.Constant) and isinstance(side.value, str)):
          raise ExpressionError(
              'string or sequence repetition (`*`) is not allowed')
      left = self.visit(node.left)
      right = self.visit(node.right)
      self.uses_safe_mul = True
      return self._null_safe([left, right],
                             lambda a, b: ast.Call(
                                 func=ast.Name(id='_cli_mul', ctx=ast.Load()),
                                 args=[a, b],
                                 keywords=[]))
    left = self.visit(node.left)
    right = self.visit(node.right)
    op = node.op
    return self._null_safe([left, right],
                           lambda a, b: ast.BinOp(left=a, op=op, right=b))

  def visit_UnaryOp(self, node: ast.UnaryOp) -> ast.AST:
    if not isinstance(node.op, _ALLOWED_UNARYOPS):
      raise ExpressionError(
          f'unary operator {type(node.op).__name__} is not allowed')
    operand = self.visit(node.operand)
    if isinstance(operand, ast.Constant) and not isinstance(node.op, ast.Not):
      return ast.UnaryOp(op=node.op, operand=operand)
    op = node.op
    return self._null_safe([operand],
                           lambda v: ast.UnaryOp(op=op, operand=v))

  def visit_Compare(self, node: ast.Compare) -> ast.AST:
    for op in node.ops:
      if not isinstance(op, _ALLOWED_CMPOPS):
        raise ExpressionError(
            f'comparison {type(op).__name__} is not allowed')
    ops, comparators = _normalize_comparison(node)
    operands = [self.visit(node.left)]
    operands.extend(self.visit(c) for c in comparators)
    parts: list[ast.AST] = []
    for i, op in enumerate(ops):
      a, b = operands[i], operands[i + 1]
      if isinstance(op, (ast.Is, ast.IsNot)):
        # `IS [NOT] NULL` is the one comparison that is never NULL itself.
        parts.append(ast.Compare(left=a, ops=[op], comparators=[b]))
      else:
        parts.append(
            self._null_safe([a, b],
                            lambda x, y, op=op: ast.Compare(
                                left=x, ops=[op], comparators=[y])))
    return parts[0] if len(parts) == 1 else self._bool3(ast.And(), parts)

  def visit_Name(self, node: ast.Name) -> ast.AST:
    if node.id in self._jinja_tokens:
      return ast.Name(id=node.id, ctx=ast.Load())
    col_name = self._colref_map.get(node.id, node.id)
    if col_name.startswith('_'):
      raise ExpressionError(
          f'underscore-prefixed identifier {col_name!r} is not allowed')
    self.columns.add(col_name)
    return ast.Call(
        func=ast.Name(id='getattr', ctx=ast.Load()),
        args=[
            ast.Name(id='__row__', ctx=ast.Load()),
            ast.Constant(value=col_name),
        ],
        keywords=[])

  def visit_Attribute(self, node: ast.Attribute) -> ast.AST:
    if node.attr.startswith('_'):
      raise ExpressionError(
          f'private attribute access {node.attr!r} is not allowed')
    if self.plain_attributes:
      return self.generic_visit(node)
    value = self.visit(node.value)
    attr = node.attr
    return self._null_safe([value],
                           lambda v: ast.Attribute(
                               value=v, attr=attr, ctx=ast.Load()))

  def visit_Call(self, node: ast.Call) -> ast.AST:
    if node.keywords:
      raise ExpressionError('keyword arguments are not allowed in expressions')
    if len(node.args) > 8:
      raise ExpressionError('too many function arguments in expression')

    if isinstance(node.func, ast.Name):
      if node.func.id in self._jinja_tokens:
        raise ExpressionError(
            'Jinja placeholders cannot be used as function names in expressions'
        )
      fn_name = self._colref_map.get(node.func.id, node.func.id)
      if fn_name.startswith('_'):
        raise ExpressionError(
            f'underscore-prefixed function {fn_name!r} is not allowed')
      lowered = fn_name.lower()
      if lowered not in _SAFE_FUNCS:
        raise ExpressionError(
            f'function {fn_name!r} is not allowed; allowed functions: '
            f'{", ".join(sorted(_SAFE_FUNCS))}')
      compiled_args = [self.visit(a) for a in node.args]
      if lowered in ('lower', 'upper', 'strip', 'trim'):
        if len(compiled_args) != 1:
          raise ExpressionError(f'{fn_name}() expects 1 argument')
        attr_name = 'strip' if lowered == 'trim' else lowered
        return self._null_safe([compiled_args[0]],
                               lambda v: ast.Call(
                                   func=ast.Attribute(
                                       value=ast.Call(
                                           func=ast.Name(
                                               id='str', ctx=ast.Load()),
                                           args=[v],
                                           keywords=[]),
                                       attr=attr_name,
                                       ctx=ast.Load()),
                                   args=[],
                                   keywords=[]))
      if lowered == 'length':
        if len(compiled_args) != 1:
          raise ExpressionError(f'{fn_name}() expects 1 argument')
        return self._null_safe([compiled_args[0]],
                               lambda v: ast.Call(
                                   func=ast.Name(id='len', ctx=ast.Load()),
                                   args=[
                                       ast.Call(
                                           func=ast.Name(
                                               id='str', ctx=ast.Load()),
                                           args=[v],
                                           keywords=[])
                                   ],
                                   keywords=[]))
      if lowered in ('coalesce', 'ifnull'):
        if not compiled_args:
          raise ExpressionError(f'{fn_name}() requires at least 1 argument')
        self.uses_coalesce = True
        return ast.Call(
            func=ast.Name(id='_cli_coalesce', ctx=ast.Load()),
            args=compiled_args,
            keywords=[])
      if lowered in ('int', 'float', 'abs', 'round', 'str') and compiled_args:
        rest = compiled_args[1:]
        return self._null_safe([compiled_args[0]],
                               lambda v: ast.Call(
                                   func=ast.Name(id=lowered, ctx=ast.Load()),
                                   args=[v, *rest],
                                   keywords=[]))
      return ast.Call(
          func=ast.Name(id=lowered, ctx=ast.Load()),
          args=compiled_args,
          keywords=[])

    if isinstance(node.func, ast.Attribute):
      method = node.func.attr
      if method.startswith('_') or method not in _SAFE_STR_METHODS:
        raise ExpressionError(
            f'method .{method}() is not allowed; allowed methods: '
            f'{", ".join(sorted(_SAFE_STR_METHODS))}')
      receiver = self.visit(node.func.value)
      compiled_args = [self.visit(a) for a in node.args]
      return self._null_safe([receiver],
                             lambda v: ast.Call(
                                 func=ast.Attribute(
                                     value=v, attr=method, ctx=ast.Load()),
                                 args=compiled_args,
                                 keywords=[]))

    raise ExpressionError('indirect function calls are not allowed')


def _infer_json_schema_type(tree_body: ast.AST) -> str | None:
  """Best-effort JSON Schema `output_type` for `MapToFields` callables."""
  if isinstance(tree_body, (ast.Compare, ast.BoolOp)):
    return 'boolean'
  if isinstance(tree_body, ast.UnaryOp) and isinstance(tree_body.op, ast.Not):
    return 'boolean'
  if isinstance(tree_body.value, bool) if isinstance(tree_body, ast.Constant) else False:
    return 'boolean'
  if isinstance(tree_body, ast.Constant):
    if isinstance(tree_body.value, int):
      return 'integer'
    if isinstance(tree_body.value, float):
      return 'number'
    if isinstance(tree_body.value, str):
      return 'string'
  if isinstance(tree_body, ast.Call):
    if isinstance(tree_body.func, ast.Name):
      fn = tree_body.func.id.lower()
      if fn == 'bool':
        return 'boolean'
      if fn in ('int', 'len', 'length'):
        return 'integer'
      if fn in ('float', 'round'):
        return 'number'
      if fn in ('str', 'lower', 'upper', 'strip', 'trim'):
        return 'string'
    if isinstance(tree_body.func, ast.Attribute):
      if tree_body.func.attr in ('startswith', 'endswith'):
        return 'boolean'
      if tree_body.func.attr in _SAFE_STR_METHODS:
        return 'string'
  if isinstance(tree_body, ast.BinOp):
    if isinstance(tree_body.op, ast.Div):
      return 'number'
    left_t = _infer_json_schema_type(tree_body.left)
    right_t = _infer_json_schema_type(tree_body.right)
    if 'number' in (left_t, right_t):
      return 'number'
    if left_t == 'integer' and right_t == 'integer' and isinstance(
        tree_body.op, (ast.Add, ast.Sub, ast.Mult, ast.FloorDiv, ast.Mod)):
      return 'integer'
    if left_t == 'string' or right_t == 'string':
      return 'string'
  return None


def _build_callable_source(
    compiled_expr: str,
    *,
    as_bool: bool,
    uses_safe_mul: bool,
    uses_coalesce: bool,
) -> str:
  lines = ['def _cli_fn(__row__):']
  if uses_safe_mul:
    lines.extend([
        '  def _cli_mul(a, b):',
        '    if isinstance(a, (str, bytes, list, tuple)) or isinstance(',
        '        b, (str, bytes, list, tuple)):',
        "      raise TypeError('sequence repetition is not allowed')",
        '    return a * b',
    ])
  if uses_coalesce:
    lines.extend([
        '  def _cli_coalesce(*vals):',
        '    for v in vals:',
        '      if v is not None:',
        '        return v',
        '    return None',
    ])
  ret = f'bool({compiled_expr})' if as_bool else compiled_expr
  lines.append(f'  return {ret}')
  return '\n'.join(lines) + '\n'


_SAFE_EXEC_BUILTINS: dict[str, Any] = {
    'abs': abs,
    'bool': bool,
    'bytes': bytes,
    'float': float,
    'getattr': getattr,
    'int': int,
    'isinstance': isinstance,
    'len': len,
    'list': list,
    'max': max,
    'min': min,
    'round': round,
    'str': str,
    'tuple': tuple,
    'TypeError': TypeError,
}


def compile_scalar_callable(
    raw_expr: str,
    *,
    as_bool: bool = False,
) -> tuple[Any, frozenset[str]]:
  """Compiles a scalar expression into `(safe_fn(__row__), referenced_columns)`."""
  if not isinstance(raw_expr, str) or not raw_expr.strip():
    raise ExpressionError('expression cannot be empty')
  normalized, colref_map = _lex_and_normalize_with_colmap(raw_expr)
  try:
    parsed = ast.parse(normalized, mode='eval')
  except SyntaxError as e:
    raise ExpressionError(f'invalid expression {raw_expr!r}: {e.msg}') from None

  nodes, depth = _ast_stats(parsed)
  if nodes > MAX_AST_NODES or depth > MAX_AST_DEPTH:
    raise ExpressionError(
        f'expression {raw_expr!r} is too complex ({nodes} AST nodes, '
        f'depth {depth}; max {MAX_AST_NODES} nodes, depth {MAX_AST_DEPTH})')

  compiler = _RowExprCompiler(colref_map)
  rewritten = ast.fix_missing_locations(compiler.visit(parsed))
  expr_code = ast.unparse(rewritten.body)
  callable_src = _build_callable_source(
      expr_code,
      as_bool=as_bool,
      uses_safe_mul=compiler.uses_safe_mul,
      uses_coalesce=compiler.uses_coalesce)
  ns: dict[str, Any] = {'__builtins__': _SAFE_EXEC_BUILTINS}
  exec(compile(callable_src, '<beam_cli_expr>', 'exec'), ns)  # pylint: disable=exec-used
  return ns['_cli_fn'], frozenset(compiler.columns)


def compile_where(
    raw_expr: str,
    known_columns: set[str] | None = None,
    *,
    allow_jinja: bool = False,
    flag_name: str = '--where',
) -> tuple[dict[str, Any], frozenset[str]]:
  """Compiles `--where` / `--post-where` into `(Filter config, cols)`."""
  jinja_map: dict[str, str] = {}
  expr_input = (
      _extract_jinja_placeholders(raw_expr, jinja_map)
      if allow_jinja and raw_expr else raw_expr)
  normalized, colref_map = _lex_and_normalize_with_colmap(expr_input)
  try:
    parsed = ast.parse(normalized, mode='eval')
  except SyntaxError as e:
    raise ExpressionError(
        f'invalid {flag_name} expression {raw_expr!r}: {e.msg}') from None

  nodes, depth = _ast_stats(parsed)
  if nodes > MAX_AST_NODES or depth > MAX_AST_DEPTH:
    raise ExpressionError(
        f'{flag_name} expression is too complex ({nodes} AST nodes, depth '
        f'{depth}; max {MAX_AST_NODES} nodes, depth {MAX_AST_DEPTH})')

  compiler = _RowExprCompiler(colref_map, frozenset(jinja_map))
  rewritten = ast.fix_missing_locations(compiler.visit(parsed))
  if known_columns is not None:
    _check_known_columns(compiler.columns, known_columns, flag_name)

  native = _native_expression(normalized, colref_map, jinja_map, known_columns)
  if native is not None:
    return (
        {
            'language': 'python',
            'keep': native,
        },
        frozenset(compiler.columns),
    )

  expr_code = ast.unparse(rewritten.body)
  for tok, raw_jinja in jinja_map.items():
    expr_code = expr_code.replace(tok, raw_jinja)
  callable_src = _build_callable_source(
      expr_code,
      as_bool=True,
      uses_safe_mul=compiler.uses_safe_mul,
      uses_coalesce=compiler.uses_coalesce)
  return (
      {
          'language': 'python',
          'keep': {
              'callable': callable_src
          },
      },
      frozenset(compiler.columns),
  )


_NATIVE_BINOPS = (ast.Add, ast.Sub, ast.Div)


def _native_expression(
    raw_text: str,
    colref_map: dict[str, str],
    jinja_map: dict[str, str],
    known_columns: set[str] | None,
    *,
    for_select: bool = False,
) -> str | None:
  """Returns a NULL-safe native Beam YAML expression, or None for a callable.

  Beam YAML evaluates `keep: "amount > 100"` by binding every input field
  whose name appears as a substring of the text, so the plain form is used
  only for simple expressions over plain, non-keyword column names:
  comparisons, `and`/`or`, literals, and `+`, `-`, `/` (`*` keeps the
  callable's repetition guard). Columns used in ordering comparisons or
  arithmetic get explicit `is not None` guards so NULL handling matches the
  callable form. Anything else (functions, casts, nested fields, `not`,
  backticks, Jinja) keeps the callable.
  """
  if colref_map or jinja_map:
    return None
  try:
    tree = ast.parse(raw_text, mode='eval').body
  except SyntaxError:
    return None
  allowed = (
      ast.BoolOp, ast.And, ast.Or, ast.Compare, *_ALLOWED_CMPOPS, ast.Name,
      ast.Constant, ast.Tuple, ast.List, ast.Set, ast.Load, ast.BinOp,
      *_NATIVE_BINOPS, ast.UnaryOp, ast.USub, ast.UAdd)
  for node in ast.walk(tree):
    if not isinstance(node, allowed):
      return None
    if isinstance(node, ast.UnaryOp) and not isinstance(node.operand,
                                                        ast.Constant):
      return None
    if isinstance(node, ast.Constant) and not (
        node.value is None or isinstance(node.value, (bool, int, float, str))):
      return None
    if isinstance(node, ast.Name) and (
        node.id.startswith('_') or keyword.iskeyword(node.id) or
        not _IDENT_RE.match(node.id)):
      return None

  def arith_names(node):
    return [
        n.id for b in ast.walk(node) if isinstance(b, ast.BinOp)
        for n in ast.walk(b) if isinstance(n, ast.Name)
    ]

  def needs_guard(cmp: ast.Compare) -> list[str]:
    # In a filter, `x = 'a'` and `x IN (...)` are already False for NULL and
    # native text never contains NOT, so only !=, NOT IN, <, <=, >, >= and
    # arithmetic need an explicit `is not None`. A --select outputs the value,
    # so there every comparison except IS [NOT] NULL must give NULL.
    names: list[str] = arith_names(cmp)
    unguarded = ((ast.Is, ast.IsNot) if for_select else
                 (ast.Is, ast.IsNot, ast.Eq, ast.In))
    if any(not isinstance(op, unguarded) for op in cmp.ops):
      names.extend(
          o.id for o in [cmp.left, *cmp.comparators]
          if isinstance(o, ast.Name))
    return list(dict.fromkeys(names))

  for cmp in (n for n in ast.walk(tree) if isinstance(n, ast.Compare)):
    norm_ops, norm_comparators = _normalize_comparison(cmp)
    if norm_ops != list(cmp.ops):
      return None
    cmp.comparators = norm_comparators  # `IN ('CA')` -> `in ('CA',)`
    if any(isinstance(op, (ast.Is, ast.IsNot)) for op in cmp.ops) and len(
        cmp.ops) > 1:
      return None
  if for_select and any(isinstance(n, ast.BoolOp) for n in ast.walk(tree)):
    return None

  def is_not_none(name):
    return ast.Compare(
        left=ast.Name(id=name, ctx=ast.Load()),
        ops=[ast.IsNot()],
        comparators=[ast.Constant(value=None)])

  if for_select:
    guards = arith_names(tree)
    for cmp in (n for n in ast.walk(tree) if isinstance(n, ast.Compare)):
      guards.extend(needs_guard(cmp))
    guards = list(dict.fromkeys(guards))
    text = ast.unparse(tree)
    if guards:
      test = ' or '.join(f'{g} is None' for g in guards)
      text = f'None if {test} else {text}'
  else:
    if isinstance(tree, (ast.BinOp, ast.Constant, ast.Tuple, ast.List,
                         ast.Set, ast.UnaryOp)):
      return None

    class _Guard(ast.NodeTransformer):
      def visit_Compare(self, node):
        names = needs_guard(node)
        if not names:
          return node
        return ast.BoolOp(
            op=ast.And(), values=[is_not_none(n) for n in names] + [node])

    text = ast.unparse(ast.fix_missing_locations(_Guard().visit(tree)))

  for col in known_columns or ():
    if (not _IDENT_RE.match(col) or keyword.iskeyword(col)) and col in text:
      return None
  return text


def _split_top_level_commas(text: str) -> list[str]:
  """Splits comma-separated `--select` items while respecting parens/quotes."""
  items: list[str] = []
  buf: list[str] = []
  depth = 0
  quote: str | None = None
  escaped = False
  for ch in text:
    if quote is not None:
      buf.append(ch)
      if escaped:
        escaped = False
      elif ch == '\\':
        escaped = True
      elif ch == quote:
        quote = None
      continue
    if ch in ("'", '"', '`'):
      quote = ch
      buf.append(ch)
    elif ch in ('(', '[', '{'):
      depth += 1
      buf.append(ch)
    elif ch in (')', ']', '}'):
      depth = max(0, depth - 1)
      buf.append(ch)
    elif ch == ',' and depth == 0:
      items.append(''.join(buf).strip())
      buf = []
    else:
      buf.append(ch)
  tail = ''.join(buf).strip()
  if tail or text.rstrip().endswith(','):
    items.append(tail)
  return items


_AS_ALIAS_RE = re.compile(
    r'^(?P<expr>.+?)\s+[Aa][Ss]\s+`?(?P<alias>[A-Za-z_][A-Za-z0-9_]*)`?$')
_EQ_ALIAS_RE = re.compile(
    r'^`?(?P<alias>[A-Za-z][A-Za-z0-9_]*)`?\s*=\s*(?P<expr>[^=].*)$')
_UNALIASED_CAST_RE = re.compile(
    r'^[Cc][Aa][Ss][Tt]\s*\(\s*`?(?P<col>[A-Za-z][A-Za-z0-9_]*)`?\s+[Aa][Ss]\s+[A-Za-z0-9_(),\s]+\)$'
)


def _unwrap_plain_col(token_str: str) -> str | None:
  s = token_str.strip()
  if len(s) >= 2 and s.startswith('`') and s.endswith('`') and '`' not in s[1:-1]:
    inner = s[1:-1]
    if not inner or inner.startswith('_'):
      return None
    return inner
  if _IDENT_RE.match(s):
    return s
  return None


def _is_nested_path(node: ast.AST) -> bool:
  """True for `a.b` or `a.b.c`: a column followed by field accesses."""
  if not isinstance(node, ast.Attribute):
    return False
  while isinstance(node, ast.Attribute):
    node = node.value
  return isinstance(node, ast.Name)


def compile_select(
    raw_select: str,
    known_columns: set[str] | None = None,
    *,
    allow_jinja: bool = False,
    flag_name: str = '--select',
) -> tuple[dict[str, Any], frozenset[str]]:
  """Compiles `--select` / `--pre-select` into `(MapToFields config, cols)`."""
  if not raw_select or not raw_select.strip():
    raise ExpressionError(f'{flag_name} cannot be empty')
  jinja_map: dict[str, str] = {}
  select_input = (
      _extract_jinja_placeholders(raw_select, jinja_map)
      if allow_jinja else raw_select)
  parts = _split_top_level_commas(select_input)
  if not parts or any(not p for p in parts):
    raise ExpressionError(f'invalid {flag_name} specification {raw_select!r}')

  fields_cfg: dict[str, Any] = {}
  all_cols: set[str] = set()
  has_star = False

  for part in parts:
    if part.strip() == '*':
      has_star = True
      continue

    plain_col = _unwrap_plain_col(part)
    # Plain column name (`id`, `user_email`, `class`, `` `order-id` ``) preserves
    # upstream Beam schema type!
    if plain_col is not None and plain_col not in jinja_map:
      if plain_col in fields_cfg:
        raise ExpressionError(
            f'duplicate output column {plain_col!r} in {flag_name}')
      all_cols.add(plain_col)
      if known_columns is not None:
        _check_known_columns({plain_col}, known_columns, flag_name)
      fields_cfg[plain_col] = plain_col
      continue

    m_as = _AS_ALIAS_RE.match(part)
    m_eq = _EQ_ALIAS_RE.match(part) if not m_as else None
    m_cast = _UNALIASED_CAST_RE.match(part) if not (m_as or m_eq) else None
    if m_as:
      alias = m_as.group('alias')
      expr_raw = m_as.group('expr').strip()
    elif m_eq:
      alias = m_eq.group('alias')
      expr_raw = m_eq.group('expr').strip()
    elif m_cast and m_cast.group('col') not in jinja_map:
      alias = m_cast.group('col')
      expr_raw = part
    else:
      shown_part = part
      for tok, raw_jinja in jinja_map.items():
        shown_part = shown_part.replace(tok, raw_jinja)
      raise ExpressionError(
          f'computed {flag_name} expression {shown_part!r} requires an alias, '
          'for example: "amount * 1.08 AS total"')

    if alias in jinja_map or not _IDENT_RE.match(alias):
      raise ExpressionError(
          f'invalid output column alias {alias!r}; must start with a letter '
          'and contain only letters, digits, or underscores')
    if alias in fields_cfg:
      raise ExpressionError(f'duplicate output column {alias!r} in {flag_name}')

    # Alias of an existing plain column (`user_id AS id` or `` `class` AS tier ``).
    plain_expr_col = _unwrap_plain_col(expr_raw)
    if plain_expr_col is not None and plain_expr_col not in jinja_map:
      all_cols.add(plain_expr_col)
      if known_columns is not None:
        _check_known_columns({plain_expr_col}, known_columns, flag_name)
      fields_cfg[alias] = plain_expr_col
      continue

    normalized, colref_map = _lex_and_normalize_with_colmap(expr_raw)
    try:
      parsed = ast.parse(normalized, mode='eval')
    except SyntaxError as e:
      raise ExpressionError(
          f'invalid {flag_name} expression {expr_raw!r}: {e.msg}') from None

    nodes, depth = _ast_stats(parsed)
    if nodes > MAX_AST_NODES or depth > MAX_AST_DEPTH:
      raise ExpressionError(
          f'{flag_name} expression for {alias!r} is too complex '
          f'({nodes} AST nodes, depth {depth})')

    inferred_type = _infer_json_schema_type(parsed.body)
    compiler = _RowExprCompiler(colref_map, frozenset(jinja_map))
    compiler.plain_attributes = _is_nested_path(parsed.body)
    rewritten = ast.fix_missing_locations(compiler.visit(parsed))
    all_cols.update(compiler.columns)
    if known_columns is not None:
      _check_known_columns(compiler.columns, known_columns, flag_name)

    native = _native_expression(
        normalized, colref_map, jinja_map, known_columns, for_select=True)
    if native is not None:
      fields_cfg[alias] = ({
          'expression': native, 'output_type': inferred_type
      } if inferred_type is not None else native)
      continue

    expr_code = ast.unparse(rewritten.body)
    for tok, raw_jinja in jinja_map.items():
      expr_code = expr_code.replace(tok, raw_jinja)
    callable_src = _build_callable_source(
        expr_code,
        as_bool=False,
        uses_safe_mul=compiler.uses_safe_mul,
        uses_coalesce=compiler.uses_coalesce)
    field_entry: dict[str, Any] = {'callable': callable_src}
    if inferred_type is not None:
      field_entry['output_type'] = inferred_type
    fields_cfg[alias] = field_entry

  if has_star and not fields_cfg:
    raise ExpressionError(
        f'{flag_name} "*" must include at least one computed or renamed column')
  out_cfg: dict[str, Any] = {
      'language': 'python',
      'fields': fields_cfg,
  }
  if has_star:
    out_cfg['append'] = True
    if known_columns is not None:
      overwritten = [a for a in fields_cfg if a in known_columns]
      if overwritten:
        out_cfg['drop'] = overwritten
  return (out_cfg, frozenset(all_cols))


def _check_known_columns(
    referenced: set[str], known_columns: set[str], flag_name: str) -> None:
  from . import output
  for col in sorted(referenced):
    if col not in known_columns:
      hint = output.safe_column_hint(col, known_columns)
      did_you_mean = f'; did you mean {hint!r}?' if hint else ''
      safe_known = sorted(
          c for c in known_columns if output.SAFE_IDENTIFIER_RE.match(str(c)))
      avail = (
          f' (available columns: {", ".join(safe_known[:12])})'
          if safe_known else '')
      raise ExpressionError(
          f'unknown column {col!r} in {flag_name}{did_you_mean}{avail}')
