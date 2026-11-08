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
"""

import ast
import io
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


def _lex_and_normalize(raw: str) -> str:
  """Normalizes SQL-like tokens (`AND`/`OR`/`NOT`, `IS [NOT] NULL`) safely."""
  if not raw or not raw.strip():
    raise ExpressionError('expression cannot be empty')
  if len(raw) > 8192:
    raise ExpressionError('expression exceeds maximum length (8192 chars)')

  tokens: list[tuple[int, str]] = []
  try:
    for tok in tokenize.generate_tokens(io.StringIO(raw).readline):
      if tok.type in (
          tokenize.ENCODING,
          tokenize.ENDMARKER,
          tokenize.NL,
          tokenize.NEWLINE,
      ):
        continue
      if tok.type == tokenize.COMMENT:
        continue
      if tok.type == tokenize.OP and tok.string == '=':
        raise ExpressionError(
            "invalid '=' in expression; use '==' for equality comparison")
      if tok.type == tokenize.OP and tok.string in ('{', '}'):
        raise ExpressionError(
            'braces / template expressions are not allowed in --where/--select')
      tokens.append((tok.type, tok.string))
  except tokenize.TokenError as e:
    raise ExpressionError(f'unterminated string or bracket in expression: {e}'
                          ) from None

  if not tokens:
    raise ExpressionError('expression cannot be empty')

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
  return ''.join(out)


def _ast_stats(node: ast.AST, depth: int = 1) -> tuple[int, int]:
  count = 1
  max_d = depth
  for child in ast.iter_child_nodes(node):
    c_count, c_depth = _ast_stats(child, depth + 1)
    count += c_count
    if c_depth > max_d:
      max_d = c_depth
  return count, max_d


class _RowExprCompiler(ast.NodeTransformer):
  """Validates AST safety and rewrites column `Name` nodes to `getattr`."""
  def __init__(self) -> None:
    self.columns: set[str] = set()
    self.uses_safe_mul = False
    self.uses_coalesce = False

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
      return ast.Call(
          func=ast.Name(id='_cli_mul', ctx=ast.Load()),
          args=[left, right],
          keywords=[])
    return self.generic_visit(node)

  def visit_UnaryOp(self, node: ast.UnaryOp) -> ast.AST:
    if not isinstance(node.op, _ALLOWED_UNARYOPS):
      raise ExpressionError(
          f'unary operator {type(node.op).__name__} is not allowed')
    return self.generic_visit(node)

  def visit_Compare(self, node: ast.Compare) -> ast.AST:
    for op in node.ops:
      if not isinstance(op, _ALLOWED_CMPOPS):
        raise ExpressionError(
            f'comparison {type(op).__name__} is not allowed')
    return self.generic_visit(node)

  def visit_Name(self, node: ast.Name) -> ast.AST:
    if node.id.startswith('_'):
      raise ExpressionError(
          f'underscore-prefixed identifier {node.id!r} is not allowed')
    self.columns.add(node.id)
    return ast.Call(
        func=ast.Name(id='getattr', ctx=ast.Load()),
        args=[
            ast.Name(id='__row__', ctx=ast.Load()),
            ast.Constant(value=node.id),
        ],
        keywords=[])

  def visit_Attribute(self, node: ast.Attribute) -> ast.AST:
    if node.attr.startswith('_'):
      raise ExpressionError(
          f'private attribute access {node.attr!r} is not allowed')
    return self.generic_visit(node)

  def visit_Call(self, node: ast.Call) -> ast.AST:
    if node.keywords:
      raise ExpressionError('keyword arguments are not allowed in expressions')
    if len(node.args) > 8:
      raise ExpressionError('too many function arguments in expression')

    if isinstance(node.func, ast.Name):
      fn_name = node.func.id
      if fn_name.startswith('_'):
        raise ExpressionError(
            f'underscore-prefixed function {fn_name!r} is not allowed')
      lowered = fn_name.lower()
      if lowered not in _SAFE_FUNCS:
        raise ExpressionError(
            f'function {fn_name!r} is not allowed; allowed functions: '
            f'{", ".join(sorted(_SAFE_FUNCS))}')
      compiled_args = [self.visit(a) for a in node.args]
      if lowered in ('lower', 'upper', 'strip'):
        if len(compiled_args) != 1:
          raise ExpressionError(f'{fn_name}() expects 1 argument')
        str_call = ast.Call(
            func=ast.Name(id='str', ctx=ast.Load()),
            args=[compiled_args[0]],
            keywords=[])
        return ast.Call(
            func=ast.Attribute(value=str_call, attr=lowered, ctx=ast.Load()),
            args=[],
            keywords=[])
      if lowered == 'coalesce':
        if not compiled_args:
          raise ExpressionError('coalesce() requires at least 1 argument')
        self.uses_coalesce = True
        return ast.Call(
            func=ast.Name(id='_cli_coalesce', ctx=ast.Load()),
            args=compiled_args,
            keywords=[])
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
      return ast.Call(
          func=ast.Attribute(value=receiver, attr=method, ctx=ast.Load()),
          args=compiled_args,
          keywords=[])

    raise ExpressionError('indirect function calls are not allowed')


def _infer_json_schema_type(tree_body: ast.AST) -> str | None:
  """Best-effort JSON Schema `output_type` for `MapToFields` callables."""
  if isinstance(tree_body, (ast.Compare, ast.BoolOp)):
    return 'boolean'
  if isinstance(tree_body, ast.UnaryOp) and isinstance(tree_body.op, ast.Not):
    return 'boolean'
  if isinstance(tree_body, ast.Constant):
    if isinstance(tree_body.value, bool):
      return 'boolean'
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
      if fn in ('int', 'len'):
        return 'integer'
      if fn in ('float', 'round'):
        return 'number'
      if fn in ('str', 'lower', 'upper', 'strip'):
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


def compile_where(
    raw_expr: str,
    known_columns: set[str] | None = None,
) -> tuple[dict[str, Any], frozenset[str]]:
  """Compiles `--where EXPR` into `(Filter config dict, referenced_columns)`."""
  normalized = _lex_and_normalize(raw_expr)
  try:
    parsed = ast.parse(normalized, mode='eval')
  except SyntaxError as e:
    raise ExpressionError(f'invalid --where expression {raw_expr!r}: {e.msg}'
                          ) from None

  nodes, depth = _ast_stats(parsed)
  if nodes > MAX_AST_NODES or depth > MAX_AST_DEPTH:
    raise ExpressionError(
        f'--where expression is too complex ({nodes} AST nodes, depth {depth}; '
        f'max {MAX_AST_NODES} nodes, depth {MAX_AST_DEPTH})')

  compiler = _RowExprCompiler()
  rewritten = ast.fix_missing_locations(compiler.visit(parsed))
  if known_columns is not None:
    _check_known_columns(compiler.columns, known_columns, '--where')

  expr_code = ast.unparse(rewritten.body)
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
    if ch in ("'", '"'):
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


_AS_ALIAS_RE = re.compile(r'^(?P<expr>.+?)\s+[Aa][Ss]\s+(?P<alias>[A-Za-z_][A-Za-z0-9_]*)$')
_EQ_ALIAS_RE = re.compile(r'^(?P<alias>[A-Za-z][A-Za-z0-9_]*)\s*=\s*(?P<expr>[^=].*)$')


def compile_select(
    raw_select: str,
    known_columns: set[str] | None = None,
) -> tuple[dict[str, Any], frozenset[str]]:
  """Compiles `--select` into `(MapToFields config dict, referenced_columns)`."""
  if not raw_select or not raw_select.strip():
    raise ExpressionError('--select cannot be empty')
  parts = _split_top_level_commas(raw_select)
  if not parts or any(not p for p in parts):
    raise ExpressionError(f'invalid --select specification {raw_select!r}')

  fields_cfg: dict[str, Any] = {}
  all_cols: set[str] = set()

  for part in parts:
    # Plain column name (`id`, `user_email`) preserves upstream Beam schema type!
    if _IDENT_RE.match(part):
      if part in fields_cfg:
        raise ExpressionError(f'duplicate output column {part!r} in --select')
      all_cols.add(part)
      if known_columns is not None:
        _check_known_columns({part}, known_columns, '--select')
      fields_cfg[part] = part
      continue

    m_as = _AS_ALIAS_RE.match(part)
    m_eq = _EQ_ALIAS_RE.match(part) if not m_as else None
    if m_as:
      alias = m_as.group('alias')
      expr_raw = m_as.group('expr').strip()
    elif m_eq:
      alias = m_eq.group('alias')
      expr_raw = m_eq.group('expr').strip()
    else:
      raise ExpressionError(
          f'computed --select expression {part!r} requires an alias, for '
          'example: "amount * 1.08 AS total"')

    if not _IDENT_RE.match(alias):
      raise ExpressionError(
          f'invalid output column alias {alias!r}; must start with a letter '
          'and contain only letters, digits, or underscores')
    if alias in fields_cfg:
      raise ExpressionError(f'duplicate output column {alias!r} in --select')

    # Alias of an existing plain column (`user_id AS id`).
    if _IDENT_RE.match(expr_raw):
      all_cols.add(expr_raw)
      if known_columns is not None:
        _check_known_columns({expr_raw}, known_columns, '--select')
      fields_cfg[alias] = expr_raw
      continue

    normalized = _lex_and_normalize(expr_raw)
    try:
      parsed = ast.parse(normalized, mode='eval')
    except SyntaxError as e:
      raise ExpressionError(
          f'invalid --select expression {expr_raw!r}: {e.msg}') from None

    nodes, depth = _ast_stats(parsed)
    if nodes > MAX_AST_NODES or depth > MAX_AST_DEPTH:
      raise ExpressionError(
          f'--select expression for {alias!r} is too complex '
          f'({nodes} AST nodes, depth {depth})')

    inferred_type = _infer_json_schema_type(parsed.body)
    compiler = _RowExprCompiler()
    rewritten = ast.fix_missing_locations(compiler.visit(parsed))
    all_cols.update(compiler.columns)
    if known_columns is not None:
      _check_known_columns(compiler.columns, known_columns, '--select')

    expr_code = ast.unparse(rewritten.body)
    callable_src = _build_callable_source(
        expr_code,
        as_bool=False,
        uses_safe_mul=compiler.uses_safe_mul,
        uses_coalesce=compiler.uses_coalesce)
    field_entry: dict[str, Any] = {'callable': callable_src}
    if inferred_type is not None:
      field_entry['output_type'] = inferred_type
    fields_cfg[alias] = field_entry

  return (
      {
          'language': 'python',
          'fields': fields_cfg,
      },
      frozenset(all_cols),
  )


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
