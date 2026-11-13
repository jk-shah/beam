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

"""Multi-input SQL joins & aggregations (`--sql` / `--join`) for `beam pipe`.

Supports:
1. Local `<100ms` zero-JVM execution over CSV, Parquet, JSON/JSONL, Avro,
   `delta://`, and `iceberg://` via an authorizer-sandboxed `:memory:` SQLite
   engine (`LEFT JOIN`, `INNER JOIN`, `FULL OUTER JOIN`, `COALESCE`, `GROUP BY`,
   `HAVING`, `ORDER BY`, CTEs `WITH`, and window functions).
2. Portable Beam YAML `type: Sql` compilation (`--emit-yaml` / `--dry-run` and
   remote `DataflowRunner` execution).
"""

from __future__ import annotations

import csv
import decimal
import glob
import io
import json
import math
import os
import re
import sqlite3
from typing import Any
from typing import Iterable
from typing import NamedTuple

from . import dlq
from . import uris

_ALIAS_PREFIX_RE = re.compile(r'^(?P<alias>[A-Za-z_][A-Za-z0-9_]*)=(?P<uri>.+)$')
_SAFE_IDENT_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


class SqlPipeError(ValueError):
  """Raised on invalid `--sql` / `--join` arguments or SQL query errors."""


class NamedSource(NamedTuple):
  alias: str
  uri: str
  endpoint: uris.ParsedEndpoint


def _derive_alias_from_uri(raw_uri: str, used: set[str]) -> str:
  cleaned = raw_uri.split('?', 1)[0].rstrip('/')
  if '://' in cleaned:
    cleaned = cleaned.split('://', 1)[1]
  parts = [p for p in cleaned.replace('\\', '/').split('/') if p]
  stem = parts[-1] if parts else 'source'
  if any(ch in stem for ch in '*?['):
    stem = parts[-2] if len(parts) >= 2 else 'source'
  # Strip known extensions or Iceberg namespace prefix (`analytics.events` -> `events`).
  lower_stem = stem.lower()
  for ext in (
      '.parquet',
      '.pq',
      '.jsonl',
      '.ndjson',
      '.json',
      '.csv',
      '.tsv',
      '.avro',
      '.txt',
  ):
    if lower_stem.endswith(ext):
      stem = stem[:-len(ext)]
      break
  else:
    if '.' in stem:
      stem = stem.rsplit('.', 1)[-1]
  ident = re.sub(r'[^A-Za-z0-9_]', '_', stem).strip('_')
  if not ident or ident[0].isdigit():
    ident = 'tbl_' + (ident or '1')
  candidate = ident
  idx = 2
  used_lower = {u.lower() for u in used}
  while candidate.lower() in used_lower:
    candidate = f'{ident}_{idx}'
    idx += 1
  return candidate


def parse_named_sources(
    raw_sources: Iterable[str],
    raw_joins: Iterable[str] | None,
    *,
    from_format: str | None = None,
    from_opts: dict[str, Any] | None = None,
    default_project: str | None = None,
    allow_sniff: bool = True,
    allow_jinja: bool = False,
) -> list[NamedSource]:
  """Parses `--from [ALIAS=]URI` and `--join [ALIAS=]URI` into `NamedSource`s."""
  combined = list(raw_sources or []) + list(raw_joins or [])
  if not combined:
    raise SqlPipeError('--from requires at least one source URI')

  out: list[NamedSource] = []
  used_aliases: set[str] = set()
  for entry in combined:
    entry_s = (entry or '').strip()
    if not entry_s:
      raise SqlPipeError('source URI cannot be empty')
    explicit_alias: str | None = None
    target_uri = entry_s
    m = _ALIAS_PREFIX_RE.match(entry_s)
    if m and not os.path.exists(entry_s):
      cand_uri = m.group('uri').strip()
      if cand_uri:
        explicit_alias = m.group('alias')
        target_uri = cand_uri
    if explicit_alias is not None:
      if explicit_alias.lower() in {a.lower() for a in used_aliases}:
        raise SqlPipeError(
            f'duplicate SQL source alias {explicit_alias!r}; each --from/--join '
            'source must have a unique alias')
      alias = explicit_alias
    else:
      alias = _derive_alias_from_uri(target_uri, used_aliases)
    used_aliases.add(alias)

    has_jinja_syntax = uris._has_jinja_syntax(target_uri)
    mixed_groups = (
        uris.local_format_groups(target_uri)
        if (not from_format and (allow_sniff or not has_jinja_syntax)) else {})
    eff_format = from_format or (next(iter(mixed_groups), None) if mixed_groups else None)

    ep = uris.parse_endpoint(
        target_uri,
        is_read=True,
        explicit_format=eff_format,
        options=dict(from_opts or {}),
        default_project=default_project,
        allow_sniff=allow_sniff,
        allow_jinja=allow_jinja)
    if mixed_groups:
      ep.config['_mixed_groups'] = mixed_groups
    elif from_format:
      ep.config['_explicit_format'] = True
    out.append(NamedSource(alias=alias, uri=target_uri, endpoint=ep))
  return out


def validate_readonly_select_sql(
    sql_query: str, *, allow_jinja: bool = False) -> str:
  """Ensures `--sql` is a single read-only `SELECT` or `WITH ... SELECT`."""
  q = (sql_query or '').strip()
  if not q:
    raise SqlPipeError('--sql query cannot be empty')
  if uris._has_jinja_syntax(q):
    if not allow_jinja:
      raise SqlPipeError(
          'Jinja2 template placeholders in --sql require --emit-yaml '
          '(or expand template variables before running)')
    try:
      uris._validate_jinja_uri(q)
    except uris.UriError as e:
      raise SqlPipeError(str(e)) from None
  # Strip optional single trailing semicolon.
  if q.endswith(';'):
    q = q[:-1].strip()
  if not q:
    raise SqlPipeError('--sql query cannot be empty')
  if not sqlite3.complete_statement(q + ';'):
    raise SqlPipeError(f'incomplete --sql query: {sql_query!r}')
  lead = q.lstrip('(').lstrip().split(None, 1)[0].upper() if q.strip('(').strip() else ''
  if lead not in ('SELECT', 'WITH'):
    raise SqlPipeError(
        f'--sql only accepts read-only SELECT or WITH queries (got {lead!r})')
  return q


def _unquote_sql_ident(ident: str) -> str:
  s = ident.strip()
  if len(s) >= 2 and s[0] in ('"', '`') and s[-1] == s[0]:
    return s[1:-1].replace(s[0] * 2, s[0]).strip()
  return s


def _split_top_level_sql_and(clause: str) -> list[str]:
  """Splits a SQL boolean expression on top-level `AND`, preserving `BETWEEN ... AND`."""
  parts: list[str] = []
  cur: list[str] = []
  depth = 0
  in_between = False
  token_re = re.compile(r'(\(|\)|\bBETWEEN\b|\bAND\b|\bOR\b)', re.I)

  for is_code, seg in _scan_sql_outside_literals(clause):
    if not is_code:
      cur.append(seg)
      continue
    pos = 0
    for m in token_re.finditer(seg):
      cur.append(seg[pos:m.start()])
      tok = m.group(0)
      up = tok.upper()
      if tok == '(':
        depth += 1
        cur.append(tok)
      elif tok == ')':
        depth = max(0, depth - 1)
        cur.append(tok)
      elif depth == 0 and up == 'OR':
        raise SqlPipeError(
            'distributed temporal interval join does not support top-level OR '
            'in ON (...); use --interval-join or split queries')
      elif depth == 0 and up == 'BETWEEN':
        in_between = True
        cur.append(tok)
      elif depth == 0 and up == 'AND':
        if in_between:
          in_between = False
          cur.append(tok)
        else:
          piece = ''.join(cur).strip()
          if piece:
            parts.append(piece)
          cur = []
      else:
        cur.append(tok)
      pos = m.end()
    cur.append(seg[pos:])
  tail = ''.join(cur).strip()
  if tail:
    parts.append(tail)
  return parts


_SQL_TS_OFFSET_RE = re.compile(
    r'^\s*(?P<alias>[A-Za-z0-9_"`]+)\s*\.\s*(?P<col>[A-Za-z0-9_"`]+)'
    r'(?:\s*(?P<op>[+-])\s*INTERVAL\s+'
    r'(?:[\'"](?P<qnum>[0-9.]+)\s*(?P<qunit>[A-Za-z]+)[\'"]'
    r'|[\'"](?P<qnum2>[0-9.]+)[\'"]\s*(?P<qunit2>[A-Za-z]+)'
    r'|(?P<num>[0-9.]+)\s*(?P<unit>[A-Za-z]+)))?\s*$',
    re.I)


def _parse_ts_offset_expr(expr: str) -> tuple[str, str, float] | None:
  s = expr.strip()
  while s.startswith('(') and s.endswith(')'):
    inner = s[1:-1].strip()
    # Verify outer parens are balanced
    bal = 0
    ok = True
    for is_c, seg in _scan_sql_outside_literals(inner):
      if not is_c:
        continue
      for ch in seg:
        if ch == '(':
          bal += 1
        elif ch == ')':
          bal -= 1
          if bal < 0:
            ok = False
            break
      if not ok:
        break
    if ok and bal == 0:
      s = inner
    else:
      break
  m = _SQL_TS_OFFSET_RE.match(s)
  if not m:
    return None
  alias = _unquote_sql_ident(m.group('alias'))
  col = _unquote_sql_ident(m.group('col'))
  if not m.group('op'):
    return (alias, col, 0.0)
  sign = -1.0 if m.group('op') == '-' else 1.0
  num_s = m.group('qnum') or m.group('qnum2') or m.group('num') or '0'
  unit_s = m.group('qunit') or m.group('qunit2') or m.group('unit') or 's'
  secs = _interval_unit_to_seconds(num_s, unit_s)
  return (alias, col, sign * secs)


def _format_seconds_duration(sec: float) -> str:
  if float(sec).is_integer():
    return f'{int(sec)}s'
  return f'{sec}s'


def _lower_temporal_interval_join_sql(
    named_sources: list[NamedSource],
    clean_q: str,
    read_transforms: list[dict[str, Any]],
    sql_inputs: dict[str, str],
    sink_ep: uris.ParsedEndpoint | None,
    *,
    has_unbounded: bool,
    max_watermark_skew: str | None = None,
    skew_action: str | None = None,
    dlq_ep: uris.ParsedEndpoint | None = None,
    allow_jinja: bool = False,
) -> dict[str, Any]:
  """Fail-closed lowering of 2-source SQL interval joins onto `CliIntervalJoin`."""
  from . import dlq
  from . import expressions
  from . import interval_join
  from . import output

  hint_suffix = (
      '; for distributed interval joins use a single 2-source '
      'SELECT ... FROM a [INNER|LEFT|RIGHT|FULL] JOIN b ON a.k = b.k '
      "AND b.ts BETWEEN a.ts - INTERVAL '1' MINUTE AND a.ts + INTERVAL '15' MINUTE, "
      'or pass --from a=URI --join b=URI --interval-join '
      '"key=k,left_time=ts,right_time=ts,lower=-1m,upper=15m"')

  rewritten_q = _rewrite_source_uri_table_refs(clean_q, named_sources)
  code_only = ' '.join(
      seg for is_c, seg in _scan_sql_outside_literals(rewritten_q) if is_c)
  lead = code_only.lstrip('(').lstrip().split(
      None, 1)[0].upper() if code_only.strip() else ''
  if lead == 'WITH':
    raise SqlPipeError(
        'distributed SQL temporal interval joins do not support CTEs (WITH ...)'
        + hint_suffix)
  if len(named_sources) != 2 or len(re.findall(r'\bJOIN\b', code_only,
                                               re.I)) != 1:
    raise SqlPipeError(
        'distributed SQL temporal interval join requires exactly 2 sources and '
        f'1 JOIN (got {len(named_sources)} source(s))' + hint_suffix)
  if re.search(r'\bCROSS\s+JOIN\b', code_only, re.I):
    raise SqlPipeError(
        'distributed SQL temporal interval join does not support CROSS JOIN' +
        hint_suffix)
  if re.search(
      r'\b(?:GROUP\s+BY|HAVING|UNION|INTERSECT|EXCEPT|OVER|QUALIFY|ORDER\s+BY|LIMIT)\b',
      code_only,
      re.I):
    raise SqlPipeError(
        'distributed SQL temporal interval join cannot combine JOIN ... INTERVAL '
        'with GROUP BY / ORDER BY / LIMIT / window analytics in one SQL step' +
        hint_suffix)

  clause_re = re.compile(
      r'^\s*SELECT\s+(?P<select>.+?)\s+FROM\s+(?P<from_tbl>[A-Za-z0-9_"`]+)'
      r'(?:\s+(?:AS\s+)?(?P<from_alias>(?!(?:INNER|LEFT|RIGHT|FULL|OUTER|JOIN)\b)[A-Za-z0-9_"`]+))?'
      r'\s+(?P<jkind>(?:INNER|(?:LEFT|RIGHT|FULL)(?:\s+OUTER)?)\s+)?JOIN\s+'
      r'(?P<join_tbl>[A-Za-z0-9_"`]+)'
      r'(?:\s+(?:AS\s+)?(?P<join_alias>(?!(?:ON|WHERE)\b)[A-Za-z0-9_"`]+))?'
      r'\s+ON\s+(?P<on_body>.+?)(?:\s+WHERE\s+(?P<where_body>.+))?\s*$',
      re.I | re.S)
  m = clause_re.match(rewritten_q)
  if not m:
    raise SqlPipeError(
        'could not lower complex SQL temporal join safely for distributed '
        'execution' + hint_suffix)

  by_alias_low = {ns.alias.lower(): ns for ns in named_sources}
  from_tbl = _unquote_sql_ident(m.group('from_tbl'))
  join_tbl = _unquote_sql_ident(m.group('join_tbl'))
  left_ns = by_alias_low.get(from_tbl.lower())
  right_ns = by_alias_low.get(join_tbl.lower())
  if left_ns is None or right_ns is None or left_ns.alias == right_ns.alias:
    raise SqlPipeError(
        f'SQL JOIN tables ({from_tbl!r}, {join_tbl!r}) must match the two '
        f'distinct input aliases {[ns.alias for ns in named_sources]}' +
        hint_suffix)

  left_sql_alias = _unquote_sql_ident(m.group('from_alias') or left_ns.alias)
  right_sql_alias = _unquote_sql_ident(m.group('join_alias') or right_ns.alias)
  l_low = left_sql_alias.lower()
  r_low = right_sql_alias.lower()

  raw_jkind = (m.group('jkind') or 'INNER').strip().upper().split()[0]
  join_type = {
      'INNER': 'inner',
      'LEFT': 'left',
      'RIGHT': 'right',
      'FULL': 'full',
  }.get(raw_jkind, 'inner')

  conjuncts = _split_top_level_sql_and(m.group('on_body'))
  left_keys: list[str] = []
  right_keys: list[str] = []
  left_time: str | None = None
  right_time: str | None = None
  lower_bound: float | None = None
  upper_bound: float | None = None
  extra_inner_filters: list[str] = []

  between_re = re.compile(
      r'^\s*(?P<lhs>.+?)\s+(?P<neg>NOT\s+)?BETWEEN\s+(?P<low>.+?)\s+AND\s+(?P<high>.+?)\s*$',
      re.I | re.S)
  cmp_re = re.compile(
      r'^\s*(?P<lhs>.+?)\s*(?P<op><=|>=|<>|!=|=|<|>)\s*(?P<rhs>.+?)\s*$', re.S)

  for raw_conj in conjuncts:
    conj = raw_conj.strip()
    while conj.startswith('(') and conj.endswith(')'):
      inner_c = conj[1:-1].strip()
      if len(_split_top_level_sql_and(inner_c)) == 1:
        conj = inner_c
      else:
        break

    mb = between_re.match(conj)
    if mb:
      if mb.group('neg'):
        raise SqlPipeError(
            'distributed temporal interval join does not support NOT BETWEEN' +
            hint_suffix)
      p_lhs = _parse_ts_offset_expr(mb.group('lhs'))
      p_low = _parse_ts_offset_expr(mb.group('low'))
      p_high = _parse_ts_offset_expr(mb.group('high'))
      if (not p_lhs or not p_low or not p_high or p_lhs[2] != 0.0 or
          p_low[0].lower() != p_high[0].lower() or p_low[1] != p_high[1]):
        raise SqlPipeError(
            f'unsupported BETWEEN interval bound {conj!r}' + hint_suffix)
      lhs_a, lhs_c, _ = p_lhs
      oth_a, oth_c, off_low = p_low
      _, _, off_high = p_high
      if {lhs_a.lower(), oth_a.lower()} != {l_low, r_low}:
        raise SqlPipeError(
            f'BETWEEN interval bound {conj!r} must correlate {left_sql_alias} '
            f'and {right_sql_alias}' + hint_suffix)
      if lhs_a.lower() == r_low:
        c_lt, c_rt = oth_c, lhs_c
        c_low, c_high = off_low, off_high
      else:
        # Swapped `left.ts BETWEEN right.ts + off_low AND right.ts + off_high`
        # => `left.ts - off_high <= right.ts <= left.ts - off_low`!
        c_lt, c_rt = lhs_c, oth_c
        c_low, c_high = -off_high, -off_low
      left_time = left_time or c_lt
      right_time = right_time or c_rt
      lower_bound = c_low if lower_bound is None else max(lower_bound, c_low)
      upper_bound = c_high if upper_bound is None else min(upper_bound, c_high)
      continue

    mc = cmp_re.match(conj)
    if mc:
      op = mc.group('op')
      p_l = _parse_ts_offset_expr(mc.group('lhs'))
      p_r = _parse_ts_offset_expr(mc.group('rhs'))
      if (p_l and p_r and {p_l[0].lower(), p_r[0].lower()} == {l_low, r_low}):
        if op == '=' and p_l[2] == 0.0 and p_r[2] == 0.0 and not re.search(
            r'\bINTERVAL\b', conj, re.I):
          if p_l[0].lower() == l_low:
            left_keys.append(p_l[1])
            right_keys.append(p_r[1])
          else:
            left_keys.append(p_r[1])
            right_keys.append(p_l[1])
          continue
        if op in ('<', '>'):
          raise SqlPipeError(
              f'distributed temporal interval join requires closed bounds '
              f'(BETWEEN or >= / <=), got strict {op!r} in {conj!r}; rewrite '
              'with <= / >= or use --interval-join')
        if op in ('<=', '>='):
          a1, c1, off1 = p_l
          a2, c2, off2 = p_r
          if a1.lower() == r_low:
            left_time = left_time or c2
            right_time = right_time or c1
            bound_val = off2 - off1
            if op == '<=':
              upper_bound = (
                  bound_val if upper_bound is None else min(
                      upper_bound, bound_val))
            else:
              lower_bound = (
                  bound_val if lower_bound is None else max(
                      lower_bound, bound_val))
          else:
            left_time = left_time or c1
            right_time = right_time or c2
            bound_val = off1 - off2
            if op == '<=':
              lower_bound = (
                  bound_val if lower_bound is None else max(
                      lower_bound, bound_val))
            else:
              upper_bound = (
                  bound_val if upper_bound is None else min(
                      upper_bound, bound_val))
          continue

    if join_type != 'inner':
      raise SqlPipeError(
          f'distributed {join_type.upper()} interval join cannot push '
          f'non-temporal ON predicate {conj!r} without altering outer NULL '
          'semantics; move post-join filters to WHERE or use --interval-join')
    if re.search(r'\b(?:SELECT|INTERVAL)\b', conj, re.I):
      raise SqlPipeError(
          f'unsupported ON predicate {conj!r} in distributed interval join' +
          hint_suffix)
    extra_inner_filters.append(conj)

  if not left_keys or not left_time or not right_time or (lower_bound is None or
                                                          upper_bound is None):
    raise SqlPipeError(
        'distributed SQL interval join requires both an equi-join key '
        '(a.id = b.id) and closed temporal range bounds (BETWEEN or >= AND <=)'
        + hint_suffix)
  if lower_bound > upper_bound:
    raise SqlPipeError(
        f'invalid SQL interval bounds: lower_bound ({lower_bound}s) > '
        f'upper_bound ({upper_bound}s)')

  select_raw = m.group('select').strip()
  if re.match(r'^DISTINCT\b', select_raw, re.I):
    raise SqlPipeError(
        'distributed SQL interval join does not support SELECT DISTINCT; '
        'use --interval-join with --dedupe-by instead')
  select_map: dict[str, str] | None = None
  if select_raw != '*':
    select_map = {}
    as_re = re.compile(
        r'^(?P<expr>.+?)\s+AS\s+(?P<alias>[A-Za-z0-9_"`]+)\s*$', re.I | re.S)
    bare_alias_re = re.compile(
        r'^(?P<expr>[A-Za-z0-9_"`]+\.[A-Za-z0-9_"`]+)\s+(?P<alias>(?!(?:AND|OR)\b)[A-Za-z_][A-Za-z0-9_]*)\s*$',
        re.I)
    for item in interval_join.split_bracket_tokens(select_raw):
      tok = item.strip()
      if tok.endswith('.*') or tok == '*':
        raise SqlPipeError(
            f'SELECT wildcard {tok!r} cannot be mixed with explicit columns in '
            'distributed SQL interval join; use SELECT * or list columns explicitly'
        )
      if re.search(r'\b(?:COUNT|SUM|AVG|MIN|MAX)\s*\(', tok, re.I):
        raise SqlPipeError(
            f'aggregate expression {tok!r} in distributed SQL interval join '
            'requires a separate --window aggregation stage')
      ma = as_re.match(tok) or bare_alias_re.match(tok)
      if ma:
        expr_s = ma.group('expr').strip()
        out_name = _unquote_sql_ident(ma.group('alias'))
      else:
        expr_s = tok
        if '.' in tok:
          out_name = _unquote_sql_ident(tok.rsplit('.', 1)[-1])
        else:
          out_name = _unquote_sql_ident(tok)
      if not output.SAFE_IDENTIFIER_RE.match(out_name):
        raise SqlPipeError(
            f'SELECT expression {tok!r} requires an explicit AS <column_name> alias'
        )
      if out_name in select_map:
        raise SqlPipeError(
            f'duplicate output column name {out_name!r} in SELECT; disambiguate with AS'
        )
      # Normalize backticked/double-quoted `a`.`col` to `a.col`
      expr_norm = re.sub(r'([`"])([A-Za-z_][A-Za-z0-9_]*)\1', r'\2',
                         expr_s).strip()
      select_map[out_name] = expr_norm

  where_parts: list[str] = list(extra_inner_filters)
  if m.group('where_body'):
    where_parts.append(f'({m.group("where_body").strip()})')
  combined_where = ' AND '.join(where_parts) if where_parts else None

  temp_where_cols: list[str] = []
  final_user_cols: list[str] = list(select_map.keys()) if select_map else []
  compiled_where_cfg: dict[str, Any] | None = None

  if combined_where:
    qual_ref_re = re.compile(
        r'\b(?P<a>[A-Za-z_][A-Za-z0-9_]*)\s*\.\s*(?P<c>[A-Za-z_][A-Za-z0-9_]*)\b'
    )
    norm_where = re.sub(
        r'([`"])([A-Za-z_][A-Za-z0-9_]*)\1', r'\2', combined_where)
    if select_map is not None:
      rev_map = {v: k for k, v in select_map.items()}

      def _sub_qual(qm: re.Match) -> str:
        qa = qm.group('a')
        qc = qm.group('c')
        if qa.lower() not in (l_low, r_low):
          return qm.group(0)
        canon_ref = f'{left_sql_alias if qa.lower() == l_low else right_sql_alias}.{qc}'
        if canon_ref in rev_map:
          return rev_map[canon_ref]
        tmp_col = f'beam_sql_w{len(temp_where_cols)}'
        temp_where_cols.append(tmp_col)
        select_map[tmp_col] = canon_ref
        rev_map[canon_ref] = tmp_col
        return tmp_col

      rewritten_where_segs = [
          qual_ref_re.sub(_sub_qual, seg) if is_c else seg
          for is_c, seg in _scan_sql_outside_literals(norm_where)
      ]
      norm_where = ''.join(rewritten_where_segs)
    else:
      # SELECT * with WHERE: strip unambiguous table alias prefix if non-key
      def _sub_star_qual(qm: re.Match) -> str:
        qa = qm.group('a')
        qc = qm.group('c')
        if qa.lower() in (l_low, r_low):
          return qc
        return qm.group(0)

      norm_where = ''.join(
          qual_ref_re.sub(_sub_star_qual, seg) if is_c else seg
          for is_c, seg in _scan_sql_outside_literals(norm_where))
    try:
      compiled_where_cfg = expressions.compile_where(
          norm_where, known_columns=None, allow_jinja=allow_jinja)[0]
    except expressions.ExpressionError as e:
      raise SqlPipeError(
          f'unsupported WHERE filter in SQL interval join: {e}') from None

  eff_skew_action = skew_action or (
      'dlq' if
      (dlq_ep is not None and max_watermark_skew is not None) else 'drop')
  if eff_skew_action == 'dlq' and dlq_ep is None:
    raise SqlPipeError('--skew-action=dlq requires --dlq <SINK>')

  ij_cfg: dict[str, Any] = {
      'left_alias': left_sql_alias,
      'right_alias': right_sql_alias,
      'left_key': left_keys[0] if len(left_keys) == 1 else left_keys,
      'right_key': right_keys[0] if len(right_keys) == 1 else right_keys,
      'left_time': left_time,
      'right_time': right_time,
      'lower_bound': _format_seconds_duration(lower_bound),
      'upper_bound': _format_seconds_duration(upper_bound),
      'join_type': join_type,
      'skew_action': eff_skew_action,
  }
  if select_map is not None:
    ij_cfg['select'] = select_map
  if max_watermark_skew is not None:
    ij_cfg['max_watermark_skew'] = max_watermark_skew

  try:
    interval_join.parse_interval_join_config(ij_cfg)
  except ValueError as e:
    raise SqlPipeError(str(e)) from None

  transforms = list(read_transforms)
  dlq_norm_refs: list[str] = []

  left_read_ref = sql_inputs[left_ns.alias]
  right_read_ref = sql_inputs[right_ns.alias]
  ts_l_name = f'AssignTimestamps_{left_ns.alias}'
  ts_r_name = f'AssignTimestamps_{right_ns.alias}'
  ts_l_cfg: dict[str, Any] = {'timestamp_col': left_time}
  ts_r_cfg: dict[str, Any] = {'timestamp_col': right_time}
  if dlq_ep is not None:
    ts_l_cfg['error_handling'] = {'output': 'errors'}
    ts_r_cfg['error_handling'] = {'output': 'errors'}
  transforms.append({
      'name': ts_l_name,
      'type': 'CliAssignTimestamps',
      'input': left_read_ref,
      'config': ts_l_cfg,
  })
  transforms.append({
      'name': ts_r_name,
      'type': 'CliAssignTimestamps',
      'input': right_read_ref,
      'config': ts_r_cfg,
  })
  if dlq_ep is not None:
    ij_left_ref = f'{ts_l_name}.good'
    ij_right_ref = f'{ts_r_name}.good'
    n_l = f'NormalizeTsLeftDlq_{left_ns.alias}'
    n_r = f'NormalizeTsRightDlq_{right_ns.alias}'
    transforms.append(
        dlq.build_dlq_normalizer_transform(
            n_l, f'{ts_l_name}.errors', ts_l_name))
    transforms.append(
        dlq.build_dlq_normalizer_transform(
            n_r, f'{ts_r_name}.errors', ts_r_name))
    dlq_norm_refs.extend([n_l, n_r])
    ij_cfg['error_handling'] = {'output': 'errors'}
  else:
    ij_left_ref = ts_l_name
    ij_right_ref = ts_r_name

  transforms.append({
      'name': 'IntervalJoin',
      'type': 'CliIntervalJoin',
      'input': {
          'left': ij_left_ref,
          'right': ij_right_ref,
      },
      'config': ij_cfg,
  })
  if dlq_ep is not None:
    current_ref = 'IntervalJoin.good'
    transforms.append(
        dlq.build_dlq_normalizer_transform(
            'NormalizeJoinDlq', 'IntervalJoin.errors', 'CliIntervalJoin'))
    dlq_norm_refs.append('NormalizeJoinDlq')
  else:
    current_ref = 'IntervalJoin'

  if compiled_where_cfg is not None:
    w_cfg = dict(compiled_where_cfg)
    if dlq_ep is not None:
      w_cfg['error_handling'] = {'output': 'errors'}
    transforms.append({
        'name': 'FilterWhere',
        'type': 'Filter',
        'input': current_ref,
        'config': w_cfg,
    })
    if dlq_ep is not None:
      transforms.append(
          dlq.build_dlq_normalizer_transform(
              'NormalizeFilterDlq', 'FilterWhere.errors', 'Filter'))
      dlq_norm_refs.append('NormalizeFilterDlq')
    current_ref = 'FilterWhere'

  if temp_where_cols and final_user_cols:
    transforms.append({
        'name': 'SelectFields',
        'type': 'MapToFields',
        'input': current_ref,
        'config': {
            'language': 'generic',
            'fields': {
                c: c
                for c in final_user_cols
            },
        },
    })
    current_ref = 'SelectFields'

  if sink_ep is not None:
    transforms.append({
        'name': 'WriteSink',
        'type': sink_ep.yaml_type,
        'input': current_ref,
        'config': dict(sink_ep.config),
    })

  if dlq_ep is not None and dlq_norm_refs:
    if len(dlq_norm_refs) == 1:
      dlq_in = dlq_norm_refs[0]
    else:
      transforms.append({
          'name': 'MergeDlq',
          'type': 'Flatten',
          'input': dlq_norm_refs,
      })
      dlq_in = 'MergeDlq'
    transforms.append({
        'name': 'WriteDlq',
        'type': dlq_ep.yaml_type,
        'input': dlq_in,
        'config': dict(dlq_ep.config),
    })

  spec_out: dict[str, Any] = {
      'pipeline': {
          'transforms': transforms,
      },
      'providers': [dlq.cli_yaml_provider_spec()],
  }
  if has_unbounded:
    spec_out['options'] = {'streaming': True}
  return spec_out


def build_sql_yaml_spec(
    named_sources: list[NamedSource],
    sql_query: str,
    sink_ep: uris.ParsedEndpoint | None,
    *,
    allow_jinja: bool = False,
    max_watermark_skew: str | None = None,
    skew_action: str | None = None,
    dlq_ep: uris.ParsedEndpoint | None = None,
) -> dict[str, Any]:
  """Builds portable Beam YAML spec (`type: Sql` or lowered `CliIntervalJoin`)."""
  clean_q = validate_readonly_select_sql(sql_query, allow_jinja=allow_jinja)
  has_unbounded = any(ns.endpoint.is_unbounded for ns in named_sources)
  transforms: list[dict[str, Any]] = []
  sql_inputs: dict[str, str] = {}
  for ns in named_sources:
    read_name = f'Read_{ns.alias}'
    mixed_groups = ns.endpoint.config.get('_mixed_groups')
    if mixed_groups:
      sub_inputs = []
      for sub_fmt, sub_files in mixed_groups.items():
        sub_read_name = f'Read_{ns.alias}_{sub_fmt}'
        sub_yaml_type = f'ReadFrom{uris.FILE_FORMATS.get(sub_fmt, sub_fmt.capitalize())}'
        sub_cfg = {
            k: v for k, v in ns.endpoint.config.items()
            if not k.startswith('_')
        }
        sub_cfg['path'] = list(sub_files)
        transforms.append({
            'name': sub_read_name,
            'type': sub_yaml_type,
            'config': sub_cfg,
        })
        sub_inputs.append(sub_read_name)
      transforms.append({
          'name': read_name,
          'type': 'Flatten',
          'input': sub_inputs,
      })
    else:
      cfg = {
          k: v for k, v in ns.endpoint.config.items()
          if not k.startswith('_')
      }
      transforms.append({
          'name': read_name,
          'type': ns.endpoint.yaml_type,
          'config': cfg,
      })
    sql_inputs[ns.alias] = read_name

  code_only = ' '.join(
      seg for is_c, seg in _scan_sql_outside_literals(clean_q) if is_c)
  has_join = bool(re.search(r'\bJOIN\b', code_only, re.I))
  has_interval = bool(re.search(r'\bINTERVAL\b', code_only, re.I))
  if (max_watermark_skew is not None or
      skew_action is not None) and not has_join:
    raise SqlPipeError(
        '--max-watermark-skew / --skew-action with --sql requires a 2-source '
        'temporal JOIN (or use --interval-join without --sql)')
  if has_join and (has_interval or max_watermark_skew is not None or
                   skew_action is not None or
                   (has_unbounded and len(named_sources) >= 2)):
    return _lower_temporal_interval_join_sql(
        named_sources,
        clean_q,
        transforms,
        sql_inputs,
        sink_ep,
        has_unbounded=has_unbounded,
        max_watermark_skew=max_watermark_skew,
        skew_action=skew_action,
        dlq_ep=dlq_ep,
        allow_jinja=allow_jinja)

  sql_input_val: Any = (
      next(iter(sql_inputs.values()))
      if len(sql_inputs) == 1 and 'pcollection' in clean_q.lower() else
      sql_inputs)
  transforms.append({
      'name': 'SqlQuery',
      'type': 'Sql',
      'input': sql_input_val,
      'config': {
          'query': clean_q
      },
  })
  if sink_ep is not None:
    transforms.append({
        'name': 'WriteSink',
        'type': sink_ep.yaml_type,
        'input': 'SqlQuery',
        'config': dict(sink_ep.config),
    })
  spec_out: dict[str, Any] = {
      'pipeline': {
          'transforms': transforms,
      }
  }
  if has_unbounded:
    spec_out['options'] = {'streaming': True}
  return spec_out


def _match_source_files(
    path_pattern: str | list[str] | tuple[str, ...]) -> list[str]:
  if isinstance(path_pattern, (list, tuple)):
    return sorted(str(p) for p in path_pattern if os.path.isfile(str(p)))
  if '://' not in path_pattern:
    if os.path.isfile(path_pattern):
      return [path_pattern]
    if os.path.isdir(path_pattern):
      files = sorted(
          os.path.join(path_pattern, f) for f in os.listdir(path_pattern)
          if not f.startswith('.') and
          os.path.isfile(os.path.join(path_pattern, f)))
      if files:
        return files
    matched = sorted(p for p in glob.glob(path_pattern) if os.path.isfile(p))
    if matched:
      return matched
    shard_matched = sorted(
        p for p in glob.glob(glob.escape(path_pattern) + '-*-of-*')
        if os.path.isfile(p))
    if shard_matched:
      return shard_matched
    raise FileNotFoundError(f'no input files matched {path_pattern!r}')

  from apache_beam.io.filesystems import FileSystems
  matches = FileSystems.match([path_pattern])[0].metadata_list
  if not matches and not any(ch in path_pattern for ch in '*?['):
    matches = FileSystems.match([path_pattern + '-*-of-*'])[0].metadata_list
  if not matches:
    raise FileNotFoundError(f'no input files matched {path_pattern!r}')
  return sorted(m.path for m in matches)


def _open_binary(path: str):
  if '://' not in path:
    return open(path, 'rb')
  from apache_beam.io.filesystems import FileSystems
  return FileSystems.open(path)


def _coerce_csv_val(
    val: str, kind: str, null_values=dlq.DEFAULT_CSV_NULL_VALUES) -> Any:
  s = val.strip()
  if s == '' or val in null_values or s in null_values:
    return None
  if kind == 'int':
    return int(s)
  if kind == 'float':
    v = float(s)
    if math.isnan(v) or math.isinf(v):
      raise ValueError(f'non-finite float {val!r}')
    return v
  if kind == 'bool':
    low = s.lower()
    if low in ('true', '1', 't', 'yes'):
      return True
    if low in ('false', '0', 'f', 'no'):
      return False
    raise ValueError(f'invalid boolean {val!r}')
  return val


def load_endpoint_rows(
    ep: uris.ParsedEndpoint,
    *,
    default_project: str | None = None,
    dlq_records: list[dict[str, Any]] | None = None,
    warnings: list[str] | None = None,
) -> tuple[list[tuple[str, str]], list[dict[str, Any]], int]:
  """Reads `(columns_with_sqlite_types, rows, raw_rows_read)` from `ep`."""
  from . import lakehouse

  if ep.kind == 'delta':
    table_root = ep.config.get('table') or ep.config.get('path')
    ver = ep.config.get('version')
    snap, rows = lakehouse.read_delta_rows_local(
        table_root,
        version=int(ver) if ver is not None else None,
        limit=None)
    cols = []
    for f in snap.get('schema', []):
      ft = str(f.get('type', 'string')).lower()
      sq_t = (
          'INTEGER' if ft in ('long', 'integer', 'short', 'byte', 'boolean')
          else 'REAL' if ft in ('double', 'float') or ft.startswith('decimal')
          else 'TEXT')
      cols.append((str(f['name']), sq_t))
    return cols, rows, len(rows)

  if ep.kind == 'iceberg':
    tdir = lakehouse.resolve_iceberg_table_dir(
        ep.config, default_project=default_project)
    state = lakehouse.load_iceberg_state(tdir)
    _, rows = lakehouse.read_iceberg_rows_local(state, limit=None)
    cols = []
    for f in state.get('schema', []):
      ft = str(f.get('type', 'string')).lower()
      sq_t = (
          'INTEGER' if ft in ('long', 'int', 'boolean') else 'REAL'
          if ft in ('double', 'float') or ft.startswith('decimal') else 'TEXT')
      cols.append((str(f['name']), sq_t))
    return cols, rows, len(rows)

  if ep.kind != 'file':
    raise SqlPipeError(
        f'local --sql currently supports file (csv, parquet, json, avro), '
        f'delta://, and iceberg:// sources (got {ep.kind!r}); use --emit-yaml '
        'or --runner=DataflowRunner for remote connectors')

  mixed_groups = ep.config.get('_mixed_groups')
  if not mixed_groups and not ep.config.get('_explicit_format'):
    p_val = ep.config.get('path')
    if isinstance(p_val, str):
      mixed_groups = uris.local_format_groups(p_val)

  if mixed_groups:
    merged_cols: list[tuple[str, str]] = []
    seen_col_names: set[str] = set()
    col_type_map: dict[str, str] = {}
    rows: list[dict[str, Any]] = []
    total_raw_count = 0

    for sub_fmt, sub_files in mixed_groups.items():
      sub_cfg = dict(ep.config)
      sub_cfg['path'] = list(sub_files)
      sub_cfg['_explicit_format'] = True
      sub_cfg.pop('_mixed_groups', None)
      sub_ep = ep._replace(format=sub_fmt, config=sub_cfg)
      sub_cols, sub_rows, sub_cnt = load_endpoint_rows(
          sub_ep,
          default_project=default_project,
          dlq_records=dlq_records,
          warnings=warnings)
      total_raw_count += sub_cnt
      rows.extend(sub_rows)
      for c_name, c_type in sub_cols:
        if c_name not in seen_col_names:
          seen_col_names.add(c_name)
          merged_cols.append((c_name, c_type))
          col_type_map[c_name] = c_type
        else:
          curr_type = col_type_map[c_name]
          if curr_type != c_type:
            if {curr_type, c_type} == {'INTEGER', 'REAL'}:
              col_type_map[c_name] = 'REAL'
            else:
              col_type_map[c_name] = 'TEXT'

    final_cols = [(cn, col_type_map[cn]) for cn, _ in merged_cols]
    return final_cols, rows, total_raw_count

  files = _match_source_files(ep.config['path'])
  fmt = ep.format

  if fmt == 'parquet':
    import pyarrow as pa
    import pyarrow.parquet as pq
    tables = []
    for fp in files:
      with _open_binary(fp) as bf:
        tables.append(pq.read_table(bf))
    if not tables:
      return [], [], 0
    full = pa.concat_tables(tables, promote_options='default')
    cols = []
    for field in full.schema:
      t = field.type
      if pa.types.is_integer(t) or pa.types.is_boolean(t):
        sq_t = 'INTEGER'
      elif pa.types.is_floating(t) or pa.types.is_decimal(t):
        sq_t = 'REAL'
      else:
        sq_t = 'TEXT'
      cols.append((field.name, sq_t))
    rows = full.to_pylist()
    return cols, rows, len(rows)

  if fmt == 'csv':
    delim = str(ep.config.get('delimiter', ',') or ',')
    nulls = dlq.DEFAULT_CSV_NULL_VALUES | frozenset(
        ep.config.get('null_values') or ())
    header, kinds = dlq._preflight_csv_schema(
        ep.config['path'],
        delimiter=delim,
        allow_dirty=bool(dlq_records is not None),
        null_values=nulls,
        scan_all_for_leading_zeros=('://' not in str(ep.config['path'])))
    if ep.config.get('fields'):
      from . import cdc as cdc_mod
      try:
        parsed_types = dict(
            cdc_mod.parse_fields_shorthand(str(ep.config['fields'])))
        kinds = dlq.apply_type_overrides(
            kinds, parsed_types, str(ep.raw), allow_new_columns=True)
      except ValueError as e:
        raise SqlPipeError(str(e)) from None
    cols = [
        (
            c,
            'INTEGER' if kinds.get(c) in ('int', 'bool') else
            'REAL' if kinds.get(c) == 'float' else 'TEXT',
        ) for c in header
    ]
    rows = []
    raw_count = 0
    for fp in files:
      with _open_binary(fp) as bf:
        text = bf.read().decode('utf-8-sig', errors='replace')
      reader = csv.reader(io.StringIO(text, newline=''), delimiter=delim)
      first = True
      for rec in reader:
        if first:
          first = False
          continue
        if not rec or (len(rec) == 1 and rec[0] == ''):
          continue
        raw_count += 1
        if len(rec) != len(header):
          err_msg = f'expected {len(header)} columns, got {len(rec)}'
          if dlq_records is not None:
            dlq_records.append({
                'step': 'ReadCsvWithDlq',
                'error_type': 'CsvColumnCountMismatch',
                'error': err_msg,
                'raw_payload': json.dumps(rec),
            })
            continue
          raise SqlPipeError(f'malformed CSV row in {fp}: {err_msg}')
        try:
          row_d = {
              col_n: _coerce_csv_val(val, kinds[col_n], nulls)
              for col_n, val in zip(header, rec)
          }
          rows.append(row_d)
        except Exception as e:  # pylint: disable=broad-except
          if dlq_records is not None:
            dlq_records.append({
                'step': 'ReadCsvWithDlq',
                'error_type': type(e).__name__,
                'error': str(e),
                'raw_payload': json.dumps(dict(zip(header, rec))),
            })
            continue
          raise SqlPipeError(f'corrupt CSV value in {fp}: {e}') from None
    return cols, rows, raw_count

  if fmt == 'json':
    rows = []
    raw_count = 0
    col_types: dict[str, str] = {}
    try:
      eff_sample = dlq.resolve_json_sample_rows(
          files, ep.config.get('sample_rows'))
      allow_unknown = (
          dlq.parse_unknown_fields_opt(ep.config.get('on_unknown_fields')) ==
          'ignore')
    except ValueError as e:
      raise SqlPipeError(str(e)) from None
    # JSONL is typed from a sample like `beam pipe` does, so a value that
    # doesn't fit its column (amount "abc") or a late field absent from the
    # sample goes to --dlq or fails with a hint, instead of silently dropping
    # fields or reaching SQLite as text.
    kinds = _jsonl_kinds(files, sample_rows=eff_sample)
    if kinds is not None and ep.config.get('fields'):
      from . import cdc as cdc_mod
      try:
        parsed_types = dict(
            cdc_mod.parse_fields_shorthand(str(ep.config['fields'])))
        kinds = dlq.apply_type_overrides(
            kinds, parsed_types, str(ep.raw), allow_new_columns=True)
      except ValueError as e:
        raise SqlPipeError(str(e)) from None
    drift_count = 0
    drift_fields: set[str] = set()
    for fp in files:
      with _open_binary(fp) as bf:
        text = bf.read().decode('utf-8', errors='replace')
      stripped_all = text.strip()
      if stripped_all.startswith('['):
        parsed_arr = json.loads(stripped_all)
        if isinstance(parsed_arr, list):
          for obj in parsed_arr:
            raw_count += 1
            if isinstance(obj, dict):
              rows.append(obj)
              for k, v in obj.items():
                if v is not None and k not in col_types:
                  col_types[k] = (
                      'INTEGER' if isinstance(v, (bool, int)) else
                      'REAL' if isinstance(v, float) else 'TEXT')
                elif k not in col_types:
                  col_types[k] = 'TEXT'
          continue
      for line in text.splitlines():
        s = line.strip()
        if not s:
          continue
        raw_count += 1
        if kinds is not None:
          try:
            rows.append(
                dlq.parse_json_record(
                    s, kinds, _dict_row, allow_unknown_fields=allow_unknown))
          except Exception as e:  # pylint: disable=broad-except
            if dlq_records is None:
              raise SqlPipeError(dlq.json_fail_fast_message(s, e)) from None
            if isinstance(e, dlq.SchemaDriftError):
              drift_count += 1
              drift_fields.update(e.extra_fields)
            dlq_records.append(dlq.json_dlq_record(s, e))
          continue
        try:
          obj = json.loads(s)
          if not isinstance(obj, dict):
            raise ValueError('expected JSON object per line')
          rows.append(obj)
          for k, v in obj.items():
            if v is not None and (k not in col_types or col_types[k] == 'TEXT'):
              if isinstance(v, (bool, int)):
                col_types.setdefault(k, 'INTEGER')
              elif isinstance(v, float):
                col_types[k] = 'REAL'
              else:
                col_types.setdefault(k, 'TEXT')
            elif k not in col_types:
              col_types[k] = 'TEXT'
        except Exception as e:  # pylint: disable=broad-except
          if dlq_records is not None:
            dlq_records.append({
                'step': 'ReadFromJsonWithDlq',
                'error_type': type(e).__name__,
                'error': str(e),
                'raw_payload': s,
            })
            continue
          raise SqlPipeError(f'invalid JSON record in {fp}: {e}') from None
    if drift_count > 0 and warnings is not None:
      warnings.append(dlq.schema_drift_warning(drift_count, drift_fields))
    if kinds is not None:
      col_types = {
          k: ('INTEGER' if v in ('int', 'bool') else
              'REAL' if v == 'float' else 'TEXT')
          for k, v in kinds.items()
      }
    cols = list(col_types.items())
    return cols, rows, raw_count

  if fmt == 'avro':
    import fastavro
    rows = []
    col_types = {}
    for fp in files:
      with _open_binary(fp) as bf:
        for obj in fastavro.reader(bf):
          d = dict(obj)
          rows.append(d)
          for k, v in d.items():
            if k not in col_types and v is not None:
              col_types[k] = (
                  'INTEGER' if isinstance(v, (bool, int)) else
                  'REAL' if isinstance(v, float) else 'TEXT')
            elif k not in col_types:
              col_types[k] = 'TEXT'
    return list(col_types.items()), rows, len(rows)

  raise SqlPipeError(f'unsupported local --sql input format {fmt!r}')


def _dict_row(**fields):
  return dict(fields)


def _jsonl_kinds(files, *, sample_rows: int | None = 100):
  """Sampled JSONL field kinds, or None for JSON array files or no sample."""
  for fp in files:
    try:
      with _open_binary(fp) as bf:
        head = bf.read(256).decode('utf-8', errors='replace').lstrip()
    except OSError:
      return None
    if head.startswith('['):
      return None
  try:
    kinds = dlq.infer_json_kinds(list(files), sample_rows=sample_rows)
  except Exception:  # pylint: disable=broad-except
    return None
  return kinds or None


def _quote_ident(name: str) -> str:
  return '"' + name.replace('"', '""') + '"'


def _normalize_sqlite_param(v: Any) -> Any:
  if v is None or isinstance(v, (int, float, str, bytes)):
    return v
  if isinstance(v, bool):
    return 1 if v else 0
  return json.dumps(v, default=str)


def _register_scalar_functions(conn: sqlite3.Connection) -> None:
  conn.create_function(
      'CONCAT',
      -1,
      lambda *args: ''.join('' if a is None else str(a) for a in args),
      deterministic=True)
  conn.create_function(
      'FLOOR',
      1,
      lambda x: None if x is None else math.floor(float(x)),
      deterministic=True)
  conn.create_function(
      'CEIL',
      1,
      lambda x: None if x is None else math.ceil(float(x)),
      deterministic=True)
  conn.create_function(
      'CEILING',
      1,
      lambda x: None if x is None else math.ceil(float(x)),
      deterministic=True)
  conn.create_function(
      'POWER',
      2,
      lambda x, y: None if (x is None or y is None) else math.pow(
          float(x), float(y)),
      deterministic=True)
  conn.create_function(
      'SQRT',
      1,
      lambda x: None if x is None else math.sqrt(float(x)),
      deterministic=True)

  def _split_part(s: Any, delim: Any, idx: Any) -> str | None:
    if s is None or delim is None or idx is None:
      return None
    parts = str(s).split(str(delim))
    pos = int(idx)
    if pos <= 0 or pos > len(parts):
      return ''
    return parts[pos - 1]

  conn.create_function('SPLIT_PART', 3, _split_part, deterministic=True)
  def _to_utc(val: Any) -> str | None:
    """Normalizes ISO-8601 (Z / offset / naive) or epoch sec/ms to ISO UTC."""
    if val is None:
      return None
    s = str(val).strip()
    if not s:
      return None
    from datetime import datetime
    from datetime import timezone
    try:
      num = float(s)
      if s.lstrip('+-').replace('.', '', 1).isdigit():
        if abs(num) >= 1e11:
          num /= 1000.0
        dt = datetime.fromtimestamp(num, tz=timezone.utc)
        return (
            dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            if dt.microsecond == 0 else dt.isoformat().replace('+00:00', 'Z'))
    except (ValueError, OverflowError, OSError):
      pass
    iso = s[:-1] + '+00:00' if s.endswith(('Z', 'z')) else s
    dt = datetime.fromisoformat(iso)
    dt = (
        dt.replace(tzinfo=timezone.utc)
        if dt.tzinfo is None else dt.astimezone(timezone.utc))
    return (
        dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        if dt.microsecond == 0 else dt.isoformat().replace('+00:00', 'Z'))

  conn.create_function('TO_UTC', 1, _to_utc, deterministic=True)

  def _beam_interval_add(ts: Any, seconds: Any) -> Any:
    if ts is None:
      return None
    try:
      secs = float(seconds)
    except (ValueError, TypeError):
      return ts
    try:
      num = float(ts) if not isinstance(ts, (int, float)) and str(ts).strip().lstrip('+-').replace('.', '', 1).isdigit() else ts
      if isinstance(num, (int, float)):
        if abs(num) >= 1e11:
          return num + (secs * 1000.0)
        return num + secs
    except Exception:
      pass

    s = str(ts).strip()
    try:
      from datetime import datetime
      from datetime import timezone
      from datetime import timedelta
      iso = s[:-1] + '+00:00' if s.endswith(('Z', 'z')) else s
      dt = datetime.fromisoformat(iso)
      dt = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
      new_dt = dt + timedelta(seconds=secs)
      return (
          new_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
          if new_dt.microsecond == 0 else new_dt.isoformat().replace('+00:00', 'Z'))
    except Exception:
      return ts

  def _beam_interval_sub(ts: Any, seconds: Any) -> Any:
    try:
      secs = float(seconds)
    except (ValueError, TypeError):
      secs = 0.0
    return _beam_interval_add(ts, -secs)

  conn.create_function(
      'BEAM_INTERVAL_ADD', 2, _beam_interval_add, deterministic=True)
  conn.create_function(
      'BEAM_INTERVAL_SUB', 2, _beam_interval_sub, deterministic=True)
  _register_aggregate_functions(conn)


class _Quantile:
  """Exact quantile with linear interpolation (numpy/pandas default).

  Registered by `_register_aggregate_functions`. `scale` is 1 for fractions
  (quantile(x, 0.95)) and 100 for percentages (percentile(x, 95)).
  """
  scale = 1.0

  def __init__(self):
    self._values: list[float] = []
    self._q_dec: decimal.Decimal | None = None

  def step(self, value, q=None):
    if q is not None:
      self._q_dec = (
          decimal.Decimal(str(q)) / decimal.Decimal(str(self.scale)))
    if value is not None:
      self._values.append(float(value))

  def finalize(self):
    if not self._values or self._q_dec is None:
      return None
    if not decimal.Decimal(0) <= self._q_dec <= decimal.Decimal(1):
      raise ValueError(
          f'quantile must be between 0 and 1 (percentile between 0 and 100), '
          f'got {float(self._q_dec) * self.scale}')
    vals = sorted(self._values)
    pos_dec = decimal.Decimal(len(vals) - 1) * self._q_dec
    lo = int(pos_dec)
    hi = min(lo + 1, len(vals) - 1)
    frac = float(pos_dec - lo)
    return vals[lo] + (vals[hi] - vals[lo]) * frac


class _Percentile(_Quantile):
  scale = 100.0


class _Median(_Quantile):
  def step(self, value):  # pylint: disable=arguments-differ
    self._q_dec = decimal.Decimal('0.5')
    super().step(value)


class _Stddev:
  """Sample standard deviation (STDDEV / STDDEV_SAMP)."""
  def __init__(self):
    self._values: list[float] = []

  def step(self, value):
    if value is not None:
      try:
        self._values.append(float(value))
      except (ValueError, TypeError):
        pass

  def finalize(self):
    if len(self._values) < 2:
      return None
    mean = sum(self._values) / len(self._values)
    return math.sqrt(
        sum((v - mean)**2 for v in self._values) / (len(self._values) - 1))


class _StddevPop:
  """Population standard deviation (STDDEV_POP)."""
  def __init__(self):
    self._values: list[float] = []

  def step(self, value):
    if value is not None:
      try:
        self._values.append(float(value))
      except (ValueError, TypeError):
        pass

  def finalize(self):
    if not self._values:
      return None
    mean = sum(self._values) / len(self._values)
    return math.sqrt(sum((v - mean)**2 for v in self._values) / len(self._values))


class _VarianceSamp:
  """Sample variance (VARIANCE / VAR_SAMP)."""
  def __init__(self):
    self._values: list[float] = []

  def step(self, value):
    if value is not None:
      try:
        self._values.append(float(value))
      except (ValueError, TypeError):
        pass

  def finalize(self):
    if len(self._values) < 2:
      return None
    mean = sum(self._values) / len(self._values)
    return sum((v - mean)**2 for v in self._values) / (len(self._values) - 1)


class _VariancePop:
  """Population variance (VAR_POP)."""
  def __init__(self):
    self._values: list[float] = []

  def step(self, value):
    if value is not None:
      try:
        self._values.append(float(value))
      except (ValueError, TypeError):
        pass

  def finalize(self):
    if not self._values:
      return None
    mean = sum(self._values) / len(self._values)
    return sum((v - mean)**2 for v in self._values) / len(self._values)


class _RegrAccumulator:
  """Numerically stable two-variable accumulator for linear regression (OLS).

  Why Welford's Algorithm:
  The naive textbook formulas for covariance and variance (e.g. sum(x*y) - sum(x)*sum(y)/n)
  suffer from catastrophic floating-point cancellation when x and y values are large
  (such as epoch timestamps or high-magnitude IDs) with small relative variance.
  This leads to precision collapse or false divide-by-zero errors.

  Welford's two-variable online algorithm updates running means and cross-product
  sums of deviations in a single pass with provable numerical stability:
    dx = x_i - mean_x_{prev}
    mean_x += dx / n
    dy = y_i - mean_y_{prev}
    mean_y += dy / n
    m2_x += dx * (x_i - mean_x_{new})
    m2_y += dy * (y_i - mean_y_{new})
    c_xy += dx * (y_i - mean_y_{new})

  Maintains Welford running sums for (y, x) pairs where neither y nor x is null:
    n: count of pairs
    mean_x, mean_y: running sample means
    m2_x: sum of squared deviations for x
    m2_y: sum of squared deviations for y
    c_xy: sum of cross-products of deviations
  """
  def __init__(self):
    self.n = 0
    self.mean_x = 0.0
    self.mean_y = 0.0
    self.m2_x = 0.0
    self.m2_y = 0.0
    self.c_xy = 0.0

  def step(self, y, x):
    if y is None or x is None:
      return
    try:
      yf = float(y)
      xf = float(x)
    except (ValueError, TypeError):
      return
    self.n += 1
    dx = xf - self.mean_x
    self.mean_x += dx / self.n
    dy = yf - self.mean_y
    self.mean_y += dy / self.n
    self.m2_x += dx * (xf - self.mean_x)
    self.m2_y += dy * (yf - self.mean_y)
    self.c_xy += dx * (yf - self.mean_y)


class _RegrSlope(_RegrAccumulator):
  def finalize(self):
    if self.n < 2 or self.m2_x <= 0:
      return None
    return self.c_xy / self.m2_x


class _RegrIntercept(_RegrAccumulator):
  def finalize(self):
    if self.n < 2 or self.m2_x <= 0:
      return None
    slope = self.c_xy / self.m2_x
    return self.mean_y - slope * self.mean_x


class _RegrR2(_RegrAccumulator):
  def finalize(self):
    if self.n < 2 or self.m2_x <= 0 or self.m2_y <= 0:
      return None
    r = self.c_xy / math.sqrt(self.m2_x * self.m2_y)
    return r * r


class _Corr(_RegrAccumulator):
  def finalize(self):
    if self.n < 2 or self.m2_x <= 0 or self.m2_y <= 0:
      return None
    val = self.c_xy / math.sqrt(self.m2_x * self.m2_y)
    return max(-1.0, min(1.0, val))


class _CovarPop(_RegrAccumulator):
  def finalize(self):
    if self.n < 1:
      return None
    return self.c_xy / self.n


class _CovarSamp(_RegrAccumulator):
  def finalize(self):
    if self.n < 2:
      return None
    return self.c_xy / (self.n - 1)


class _RegrCount(_RegrAccumulator):
  def finalize(self):
    return self.n


class _RegrAvgX(_RegrAccumulator):
  def finalize(self):
    return self.mean_x if self.n > 0 else None


class _RegrAvgY(_RegrAccumulator):
  def finalize(self):
    return self.mean_y if self.n > 0 else None


class _RegrSXX(_RegrAccumulator):
  def finalize(self):
    return self.m2_x if self.n >= 2 else None


class _RegrSYY(_RegrAccumulator):
  def finalize(self):
    return self.m2_y if self.n >= 2 else None


class _RegrSXY(_RegrAccumulator):
  def finalize(self):
    return self.c_xy if self.n >= 2 else None


def _register_aggregate_functions(conn: sqlite3.Connection) -> None:
  """Statistical aggregates agents reach for that plain SQLite lacks.

  They are defined here (rather than relying on SQLite's optional percentile
  extension) so results are identical on every platform.
  """
  conn.create_aggregate('MEDIAN', 1, _Median)
  conn.create_aggregate('PERCENTILE', 2, _Percentile)
  conn.create_aggregate('QUANTILE', 2, _Quantile)
  conn.create_aggregate('QUANTILE_CONT', 2, _Quantile)
  conn.create_aggregate('STDDEV', 1, _Stddev)
  conn.create_aggregate('STDDEV_SAMP', 1, _Stddev)
  conn.create_aggregate('STDDEV_POP', 1, _StddevPop)
  conn.create_aggregate('VARIANCE', 1, _VarianceSamp)
  conn.create_aggregate('VAR_SAMP', 1, _VarianceSamp)
  conn.create_aggregate('VAR_POP', 1, _VariancePop)
  conn.create_aggregate('REGR_SLOPE', 2, _RegrSlope)
  conn.create_aggregate('REGR_INTERCEPT', 2, _RegrIntercept)
  conn.create_aggregate('REGR_R2', 2, _RegrR2)
  conn.create_aggregate('CORR', 2, _Corr)
  conn.create_aggregate('COVAR_POP', 2, _CovarPop)
  conn.create_aggregate('COVAR_SAMP', 2, _CovarSamp)
  conn.create_aggregate('REGR_COUNT', 2, _RegrCount)
  conn.create_aggregate('REGR_AVGX', 2, _RegrAvgX)
  conn.create_aggregate('REGR_AVGY', 2, _RegrAvgY)
  conn.create_aggregate('REGR_SXX', 2, _RegrSXX)
  conn.create_aggregate('REGR_SYY', 2, _RegrSYY)
  conn.create_aggregate('REGR_SXY', 2, _RegrSXY)


# Functions and syntax from other dialects mapped to what local SQL supports.
_SQL_DIALECT_HINTS = (
    (re.compile(r'within\s+group', re.I),
     'WITHIN GROUP is not supported by the default SQLite engine; use '
     'percentile(col, 95), quantile(col, 0.95) or median(col), or pass '
     '--sql-engine duckdb'),
    (re.compile(r'no such function: (percentile_cont|percentile_disc|'
                r'approx_quantiles|approx_percentile)', re.I),
     'use percentile(col, 95), quantile(col, 0.95) or median(col) (exact, '
     'linear interpolation)'),
    (re.compile(r'no such function: approx_count_distinct', re.I),
     'use COUNT(DISTINCT col)'),
    (re.compile(r'no such function: (date_trunc|format_timestamp|extract)',
                re.I),
     "use strftime(), e.g. strftime('%Y-%m', ts)"),
    (re.compile(
        r'no such function: (arg_max|arg_min|'
        r'max_by|min_by|mode|bool_and|bool_or|string_agg|array_agg)',
        re.I),
     'advanced warehouse aggregates require --sql-engine duckdb'),
    (re.compile(r'no such table|table .* does not exist|replacement scan', re.I),
     'name each input with --from ALIAS=PATH / --join ALIAS=PATH (inputs are '
     'pre-loaded as tables, never file paths; without an alias use the file '
     'name or FROM source)'),
)

_SQLITE_CAST_TYPE_MAP = {
    'string': 'TEXT',
    'bytes': 'BLOB',
    'int64': 'INTEGER',
    'float64': 'REAL',
    'bignumeric': 'REAL',
}


def _scan_sql_outside_literals(sql_text: str):
  """Lexical state machine yielding `(is_code, segment)` tuples for SQL text.

  Design Rationale:
  Avoids heavyweight external SQL AST parsing packages (e.g. sqlparse, antlr)
  to ensure zero-overhead CLI cold starts (<50ms).

  Accurately differentiates executable SQL syntax from:
  1. Single-quoted string literals (`'value'`, with `''` escape support).
  2. Double-quoted identifiers (`"column"`) and backtick identifiers (``` `column` ```).
  3. Single-line comments (`-- comment\\n`).
  4. Multi-line block comments (`/* comment */`).

  Yields:
    `(True, code_segment)` for raw SQL code (safe for rewriting/keyword checks).
    `(False, literal_segment)` for strings, comments, and identifiers (preserved verbatim).
  """
  n = len(sql_text)
  i = 0
  code_start = 0
  while i < n:
    ch = sql_text[i]
    # Check for string literal or quoted identifier start ('...', "...", `...`)
    if ch in ("'", '"', '`'):
      if i > code_start:
        yield True, sql_text[code_start:i]
      q = ch
      j = i + 1
      while j < n:
        if sql_text[j] == q:
          # Handle doubled quote escapes: '' or "" or ``
          if j + 1 < n and sql_text[j + 1] == q:
            j += 2
            continue
          j += 1
          break
        j += 1
      yield False, sql_text[i:j]
      i = code_start = j
      continue

    # Check for single-line SQL comment: -- ...
    if sql_text[i:i + 2] == '--':
      if i > code_start:
        yield True, sql_text[code_start:i]
      nl = sql_text.find('\n', i + 2)
      end = n if nl == -1 else nl + 1
      yield False, sql_text[i:end]
      i = code_start = end
      continue

    # Check for multi-line block comment: /* ... */
    if sql_text[i:i + 2] == '/*':
      if i > code_start:
        yield True, sql_text[code_start:i]
      end_c = sql_text.find('*/', i + 2)
      end = n if end_c == -1 else end_c + 2
      yield False, sql_text[i:end]
      i = code_start = end
      continue

    i += 1

  if code_start < n:
    yield True, sql_text[code_start:]


def _normalize_sqlite_cast_types(sql_query: str) -> str:
  """Maps BigQuery/ANSI `CAST(... AS STRING|INT64|FLOAT64|BYTES)` to SQLite types.

  SQLite treats unknown type names without `CHAR`/`CLOB`/`TEXT` (including
  `STRING`) as `NUMERIC` affinity, which would silently coerce `'u1'` -> `0`!
  """
  def _sub_cast_type(m: re.Match) -> str:
    mapped = _SQLITE_CAST_TYPE_MAP.get(m.group(2).lower())
    return f'{m.group(1)}{mapped}{m.group(3)}' if mapped else m.group(0)

  cast_as_re = re.compile(
      r'(\bAS\s+)(STRING|BYTES|INT64|FLOAT64|BIGNUMERIC)(\s*\))', re.I)
  parts: list[str] = []
  for is_code, seg in _scan_sql_outside_literals(sql_query):
    parts.append(cast_as_re.sub(_sub_cast_type, seg) if is_code else seg)
  return ''.join(parts)


_INTERVAL_ADD_RE = re.compile(
    r'(?P<col>[A-Za-z0-9_."`]+)\s*\+\s*INTERVAL\s+(?:[\'"]([0-9.]+)\s*([A-Za-z]+)[\'"]|[\'"]?([0-9.]+)[\'"]?\s*([A-Za-z]+))',
    re.I)
_INTERVAL_SUB_RE = re.compile(
    r'(?P<col>[A-Za-z0-9_."`]+)\s*-\s*INTERVAL\s+(?:[\'"]([0-9.]+)\s*([A-Za-z]+)[\'"]|[\'"]?([0-9.]+)[\'"]?\s*([A-Za-z]+))',
    re.I)


def _interval_unit_to_seconds(num_str: str, unit: str) -> float:
  try:
    num = float(num_str)
  except (ValueError, TypeError):
    return 0.0
  u = unit.lower().rstrip('s')
  mult = {
      'millisecond': 0.001,
      'ms': 0.001,
      'second': 1.0,
      'sec': 1.0,
      'minute': 60.0,
      'min': 60.0,
      'm': 60.0,
      'hour': 3600.0,
      'hr': 3600.0,
      'h': 3600.0,
      'day': 86400.0,
      'd': 86400.0,
  }.get(u, 1.0)
  return num * mult


def _normalize_sqlite_interval_expressions(sql_query: str) -> str:
  """Rewrites ANSI/Beam SQL `INTERVAL` expressions to SQLite `BEAM_INTERVAL_ADD/SUB`.

  Examples:
    `o.time BETWEEN c.time AND c.time + INTERVAL '15' MINUTE`
    -> `o.time BETWEEN c.time AND BEAM_INTERVAL_ADD(c.time, 900.0)`
    `o.time BETWEEN c.time - INTERVAL '1' MINUTE AND c.time + INTERVAL 15 SECOND`
    -> `o.time BETWEEN BEAM_INTERVAL_SUB(c.time, 60.0) AND BEAM_INTERVAL_ADD(c.time, 15.0)`
  """
  if not re.search(r'\bINTERVAL\b', sql_query, re.I):
    return sql_query

  def _sub_add(m: re.Match) -> str:
    col = m.group('col')
    num = m.group(2) or m.group(4)
    unit = m.group(3) or m.group(5)
    secs = _interval_unit_to_seconds(num, unit)
    return f'BEAM_INTERVAL_ADD({col}, {secs})'

  def _sub_sub(m: re.Match) -> str:
    col = m.group('col')
    num = m.group(2) or m.group(4)
    unit = m.group(3) or m.group(5)
    secs = _interval_unit_to_seconds(num, unit)
    return f'BEAM_INTERVAL_SUB({col}, {secs})'

  res = _INTERVAL_ADD_RE.sub(_sub_add, sql_query)
  res = _INTERVAL_SUB_RE.sub(_sub_sub, res)
  return res


def _rewrite_source_uri_table_refs(
    sql_query: str, named_sources: list[NamedSource]) -> str:
  """Rewrites `FROM "delta://..."` / `FROM "iceberg://..."` matching a lakehouse input to its alias."""
  uri_to_alias: dict[str, str] = {}
  for ns in named_sources:
    if ns.endpoint.kind not in ('delta', 'iceberg'):
      continue
    quoted = _quote_ident(ns.alias)
    for cand in (ns.uri, ns.endpoint.raw):
      if isinstance(cand, str) and cand.strip():
        uri_to_alias[cand.strip()] = quoted
  if not uri_to_alias:
    return sql_query

  segs = list(_scan_sql_outside_literals(sql_query))
  out: list[str] = []
  for idx, (is_code, seg) in enumerate(segs):
    if (not is_code and len(seg) >= 2 and seg[0] in ('"', '`') and
        seg[-1] == seg[0] and idx > 0 and segs[idx - 1][0]):
      prev_code = segs[idx - 1][1]
      if re.search(r'\b(?:FROM|JOIN)\s*$', prev_code, re.I):
        unq = seg[1:-1].replace(seg[0] * 2, seg[0]).strip()
        if unq in uri_to_alias:
          out.append(uri_to_alias[unq])
          continue
    out.append(seg)
  return ''.join(out)


def _sql_error_hint(message: str, query: str) -> str:
  """Returns a '; hint: ...' suffix for a local SQL error, or ''."""
  for pattern, hint in _SQL_DIALECT_HINTS:
    if pattern.search(message) or (
        pattern.pattern.startswith('within') and pattern.search(query)):
      return f'; hint: {hint}'
  if 'no such function' in message.lower():
    return (
        '; hint: local SQL is SQLite plus to_utc, median, percentile, '
        'quantile, stddev, concat, split_part, floor, ceil, power and sqrt; '
        'for ANSI analytics pass --sql-engine duckdb')
  return ''


# Functions and clauses that local `--sql` engines (SQLite / DuckDB) accept
# but Beam's distributed SQL dialect (Apache Calcite) does not.
_LOCAL_ONLY_SQL_FUNCTIONS = (
    'median',
    'percentile',
    'quantile',
    'quantile_cont',
    'percentile_cont',
    'percentile_disc',
    'approx_quantiles',
    'split_part',
    'to_utc',
    'strftime',
    'julianday',
    'unixepoch',
    'ifnull',
    'iif',
    'group_concat',
    'string_agg',
    'printf',
    'instr',
    'typeof',
    'json_extract',
    'json_group_array',
    'json_object',
)

_LOCAL_ONLY_SQL_KEYWORDS = ('qualify', 'glob', 'regexp', 'ilike')


def analyze_sql_portability(
    query: str, *, sql_engine: str = 'sqlite') -> list[str]:
  """Returns non-portable SQL functions/clauses used outside string literals."""
  del sql_engine
  code_only = ' '.join(
      seg for is_code, seg in _scan_sql_outside_literals(query)
      if is_code).lower()
  found: list[str] = []
  for fn in _LOCAL_ONLY_SQL_FUNCTIONS:
    if re.search(r'\b' + fn + r'\s*\(', code_only):
      found.append(f'{fn}()')
  for kw in _LOCAL_ONLY_SQL_KEYWORDS:
    if re.search(r'\b' + kw + r'\b', code_only):
      found.append(kw.upper())
  return found


def local_only_sql_functions(query: str) -> list[str]:
  """Names of local-only constructs used in `query`."""
  return [item.rstrip('()').lower() for item in analyze_sql_portability(query)]


_LHS_BOOL_RE = re.compile(
    r'(?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)?([A-Za-z_][A-Za-z0-9_]*)'
    r'\s*(?:=|==|!=|<>|\bIS(?:\s+NOT)?)\s*$',
    re.I)
_OP_ONLY_RE = re.compile(r'^\s*(?:=|==|!=|<>|\bIS(?:\s+NOT)?)\s*$', re.I)
_RHS_BOOL_RE = re.compile(
    r'^\s*(?:=|==|!=|<>)\s*(?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)?'
    r'([A-Za-z_][A-Za-z0-9_]*)\b',
    re.I)
_RHS_OP_ONLY_RE = re.compile(
    r'^\s*(?:=|==|!=|<>)\s*(?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)?$')


def _is_bool_col_comparison(
    segs: list[tuple[bool, str]], idx: int, bool_cols: set[str]) -> bool:
  """Returns True if the string literal at `segs[idx]` is in comparison with a boolean column.

  Handles both unquoted identifiers (e.g. `col = 'true'`, `t.col = 'true'`)
  and quoted identifiers (e.g. `"col" = 'true'`, `` `col` = 'true' ``),
  whether the boolean column is on the left-hand side or right-hand side of
  the comparison operator.
  """
  # 1. Left-hand side check: e.g. `col = 'true'` or `"col" = 'true'`
  if idx > 0 and segs[idx - 1][0]:
    prev_code = segs[idx - 1][1]
    m_l = _LHS_BOOL_RE.search(prev_code)
    if m_l and m_l.group(1).lower() in bool_cols:
      return True
    # Quoted column name followed by standalone operator: `"col"` + ` = ` + `'true'`
    if (idx >= 2 and not segs[idx - 2][0] and
        segs[idx - 2][1][:1] in ('"', '`') and
        _OP_ONLY_RE.fullmatch(prev_code)):
      unq = segs[idx - 2][1][1:-1].strip().lower()
      if unq in bool_cols:
        return True

  # 2. Right-hand side check: e.g. `'true' = col` or `'true' = "col"`
  if idx + 1 < len(segs) and segs[idx + 1][0]:
    next_code = segs[idx + 1][1]
    m_r = _RHS_BOOL_RE.match(next_code)
    if m_r and m_r.group(1).lower() in bool_cols:
      return True
    # Standalone operator followed by quoted column name: `'true'` + ` = ` + `"col"`
    if (idx + 2 < len(segs) and not segs[idx + 2][0] and
        segs[idx + 2][1][:1] in ('"', '`') and
        _RHS_OP_ONLY_RE.fullmatch(next_code)):
      unq = segs[idx + 2][1][1:-1].strip().lower()
      if unq in bool_cols:
        return True

  return False


def _normalize_sqlite_bool_comparisons(
    sql_query: str, bool_cols: set[str]) -> str:
  """Coerces `'true'` / `'false'` string literals to integer `1` / `0` for boolean columns.

  Rationale (The SQLite Boolean Affinity Cliff):
  SQLite lacks a dedicated BOOLEAN storage class; boolean values are represented
  as INTEGER (1 for True, 0 for False). In standard ANSI SQL and Apache Beam SQL
  (Calcite), queries frequently use string literals `'true'` / `'false'`.
  However, in SQLite, comparing an INTEGER column to text `'true'` evaluates to
  FALSE for both 0 and 1 due to SQLite's strict type affinity rules!
  This causes queries like `WHERE is_active = 'true'` to silently return 0 rows.

  This function detects columns whose non-null values are exclusively booleans
  and rewrites `'true'` -> `1` and `'false'` -> `0` when appearing in binary
  equality or inequality comparisons with those boolean columns.
  """
  if not bool_cols:
    return sql_query
  segs = list(_scan_sql_outside_literals(sql_query))
  out: list[str] = []

  for idx, (is_code, seg) in enumerate(segs):
    if (not is_code and len(seg) >= 2 and seg[0] == "'" and seg[-1] == "'"):
      inner = seg[1:-1].replace("''", "'").strip().lower()
      if inner in ('true', 'false') and _is_bool_col_comparison(segs, idx, bool_cols):
        out.append('1' if inner == 'true' else '0')
        continue
    out.append(seg)
  return ''.join(out)


# Local `--sql` engines. SQLite ships with Python; DuckDB is optional
# (`pip install duckdb`) and adds ANSI analytics such as
# `PERCENTILE_CONT ... WITHIN GROUP`, `QUALIFY`, struct field access and
# `list` functions.
SQL_ENGINES = ('sqlite', 'duckdb')
DUCKDB_INSTALL_HINT = "pip install 'duckdb>=1.1'"


class SqlEngineMissingError(SqlPipeError):
  """The requested `--sql-engine` is not installed."""


def duckdb_version() -> str | None:
  """Installed DuckDB version, without importing it, or None."""
  try:
    from importlib import metadata
    return metadata.version('duckdb')
  except Exception:  # pylint: disable=broad-except
    return None


_ARROW_TYPES_FOR_SQLITE = {'INTEGER': 'int64', 'REAL': 'float64'}


def _arrow_table(cols, rows):
  """Builds a pyarrow Table from loaded rows, keeping nested values."""
  import pyarrow as pa
  names = [c for c, _ in cols] or (list(rows[0]) if rows else ['_empty'])
  types = dict(cols)
  arrays = []
  for name in names:
    values = [r.get(name) for r in rows]
    declared = _ARROW_TYPES_FOR_SQLITE.get(types.get(name, ''), None)
    try:
      if declared and not any(isinstance(v, (dict, list)) for v in values):
        arrays.append(pa.array(values, type=declared))
      else:
        arrays.append(pa.array(values) if values else pa.array(
            [], type=pa.string()))
    except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError):
      # Mixed types in one column: keep the values as text.
      arrays.append(
          pa.array([
              None if v is None else
              (json.dumps(v, default=str) if isinstance(v, (dict, list)) else
               str(v)) for v in values
          ],
                   type=pa.string()))
  return pa.Table.from_arrays(arrays, names=names)


def _run_duckdb(
    named_sources, clean_q, *, default_project, dlq_records, warnings=None):
  """Runs one read-only SELECT in a sandboxed in-memory DuckDB.

  Inputs are loaded by the same readers as the SQLite engine (typing, NULLs,
  DLQ) and registered as in-memory Arrow tables. The connection then has
  file system access, extension install/load and configuration changes
  disabled, so SQL can't read or write files (`read_csv`, `COPY`, `ATTACH`).
  """
  try:
    import duckdb
  except ImportError:
    raise SqlEngineMissingError(
        f'--sql-engine duckdb needs the duckdb package: {DUCKDB_INSTALL_HINT} '
        '(check with: beam doctor)') from None
  clean_q = _rewrite_source_uri_table_refs(clean_q, named_sources)
  total_read = 0
  con = duckdb.connect(
      ':memory:',
      config={
          'enable_external_access': False,
          'autoinstall_known_extensions': False,
          'autoload_known_extensions': False,
      })
  try:
    statements = con.extract_statements(clean_q)
    if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
      raise SqlPipeError('--sql only accepts a single read-only SELECT query')
    first_alias = None
    used_lower = set()
    for ns in named_sources:
      cols, rows, raw_cnt = load_endpoint_rows(
          ns.endpoint,
          default_project=default_project,
          dlq_records=dlq_records,
          warnings=warnings)
      total_read += raw_cnt
      con.register(ns.alias, _arrow_table(cols, rows))
      used_lower.add(ns.alias.lower())
      first_alias = first_alias or ns.alias
    if first_alias is not None:
      # Register Beam SQL standard `PCOLLECTION` and `source` views on the
      # primary input, plus conventional single-table aliases (`data`, `tbl`,
      # `input`) when there is only one source so queries like
      # `SELECT * FROM data` execute without requiring the user to know the
      # inferred file stem alias.
      extra_views = (
          ('PCOLLECTION', 'source', 'data', 'tbl', 'input')
          if len(named_sources) == 1 else ('PCOLLECTION', 'source'))
      for view_name in extra_views:
        if view_name.lower() not in used_lower:
          con.execute(
              f'CREATE VIEW {_quote_ident(view_name)} AS '
              f'SELECT * FROM {_quote_ident(first_alias)}')
    con.execute("SET TimeZone = 'UTC'")
    con.execute(r'''
      CREATE MACRO to_utc(x) AS (
        CASE
          WHEN x IS NULL OR trim(CAST(x AS VARCHAR)) = '' THEN NULL
          WHEN regexp_matches(trim(CAST(x AS VARCHAR)), '^[+-]?[0-9]+(\.[0-9]+)?$') THEN
            strftime(
              to_timestamp(
                CASE WHEN abs(TRY_CAST(x AS DOUBLE)) >= 1e11
                     THEN TRY_CAST(x AS DOUBLE) / 1000.0
                     ELSE TRY_CAST(x AS DOUBLE) END
              ) AT TIME ZONE 'UTC',
              '%Y-%m-%dT%H:%M:%SZ'
            )
          ELSE
            strftime(
              CAST(trim(CAST(x AS VARCHAR)) AS TIMESTAMPTZ) AT TIME ZONE 'UTC',
              '%Y-%m-%dT%H:%M:%SZ'
            )
        END
      )
    ''')
    raw_mem = os.environ.get('BEAM_CLI_DUCKDB_MAX_MEMORY', '').strip() or '512MB'
    if not re.fullmatch(r'\d+\s*(?:KB|MB|GB|KiB|MiB|GiB)', raw_mem, re.I):
      raw_mem = '512MB'
    raw_max_rows = os.environ.get('BEAM_CLI_SQL_MAX_RESULT_ROWS', '').strip()
    max_sql_rows = int(raw_max_rows) if raw_max_rows else 1_000_000
    con.execute(f"SET memory_limit = '{raw_mem}'")
    con.execute("SET max_temp_directory_size = '0B'")
    con.execute('SET threads = 2')
    con.execute('SET lock_configuration = true')
    try:
      cur = con.execute(clean_q)
      out_cols = [d[0] for d in (cur.description or [])]
      fetched: list[Any] = []
      while True:
        chunk = cur.fetchmany(4096)
        if not chunk:
          break
        fetched.extend(chunk)
        if len(fetched) > max_sql_rows:
          raise SqlPipeError(
              f'SQL query exceeded local result limit ({max_sql_rows:,} rows); '
              'add LIMIT or run distributed SQL with --runner=DataflowRunner')
    except duckdb.Error as e:
      avail_list = [ns.alias for ns in named_sources]
      if len(named_sources) == 1 and 'source' not in used_lower:
        avail_list.append('source')
      avail = ', '.join(avail_list)
      first_line = str(e).splitlines()[0]
      raise SqlPipeError(
          f'SQL execution error (DuckDB): {first_line} '
          f'(available tables: {avail})'
          f'{_sql_error_hint(first_line, clean_q)}') from None
    return out_cols, [dict(zip(out_cols, t)) for t in fetched], total_read
  finally:
    con.close()


def run_in_memory_sql(
    named_sources: list[NamedSource],
    sql_query: str,
    *,
    default_project: str | None = None,
    enable_dlq: bool = False,
    engine: str = 'sqlite',
    warnings: list[str] | None = None,
) -> tuple[list[str], list[dict[str, Any]], list[dict[str, Any]], int]:
  """Executes `sql_query` in a sandboxed in-memory SQLite or DuckDB database.

  Returns `(out_columns, out_rows, dlq_records, total_rows_read)`.
  """
  if engine not in SQL_ENGINES:
    raise SqlPipeError(
        f'unknown --sql-engine {engine!r}; use one of {", ".join(SQL_ENGINES)}')
  clean_q = validate_readonly_select_sql(sql_query)
  dlq_records: list[dict[str, Any]] | None = [] if enable_dlq else None
  if engine == 'duckdb':
    out_cols, out_rows, total_read = _run_duckdb(
        named_sources,
        clean_q,
        default_project=default_project,
        dlq_records=dlq_records,
        warnings=warnings)
    return out_cols, out_rows, (dlq_records or []), total_read
  clean_q = _normalize_sqlite_interval_expressions(
      _normalize_sqlite_cast_types(
          _rewrite_source_uri_table_refs(clean_q, named_sources)))
  total_read = 0

  conn = sqlite3.connect(':memory:')
  try:
    _register_scalar_functions(conn)
    used_lower: set[str] = set()
    first_alias: str | None = None
    bool_cols: set[str] = set()
    non_bool_cols: set[str] = set()

    for ns in named_sources:
      cols, rows, raw_cnt = load_endpoint_rows(
          ns.endpoint,
          default_project=default_project,
          dlq_records=dlq_records,
          warnings=warnings)
      total_read += raw_cnt
      if not cols and rows:
        cols = [(k, 'TEXT') for k in rows[0].keys()]
      if not cols:
        # Empty dataset with no schema columns -> dummy placeholder column
        cols = [('_empty', 'TEXT')]
      col_defs = ', '.join(f'{_quote_ident(cn)} {ct}' for cn, ct in cols)
      t_ident = _quote_ident(ns.alias)
      conn.execute(f'CREATE TABLE {t_ident} ({col_defs})')
      if rows:
        col_names = [cn for cn, _ in cols]
        for cn in col_names:
          non_null_vals = [r.get(cn) for r in rows if r.get(cn) is not None]
          if non_null_vals and all(isinstance(v, bool) for v in non_null_vals):
            bool_cols.add(cn.lower())
          elif non_null_vals:
            non_bool_cols.add(cn.lower())
        placeholders = ', '.join('?' for _ in col_names)
        col_list_sql = ', '.join(_quote_ident(cn) for cn in col_names)
        batch = [
            tuple(_normalize_sqlite_param(r.get(cn)) for cn in col_names)
            for r in rows
        ]
        conn.executemany(
            f'INSERT INTO {t_ident} ({col_list_sql}) VALUES ({placeholders})',
            batch)
      used_lower.add(ns.alias.lower())
      if first_alias is None:
        first_alias = ns.alias

    pure_bool_cols = bool_cols - non_bool_cols
    if pure_bool_cols:
      clean_q = _normalize_sqlite_bool_comparisons(clean_q, pure_bool_cols)

    # Register Beam SQL `PCOLLECTION` and `source` views on the primary input
    # (plus common single-table aliases `data`, `tbl`, `input` when unary).
    if first_alias is not None:
      extra_views = (
          ('PCOLLECTION', 'source', 'data', 'tbl', 'input')
          if len(named_sources) == 1 else ('PCOLLECTION', 'source'))
      for view_name in extra_views:
        if view_name.lower() not in used_lower:
          conn.execute(
              f'CREATE VIEW {_quote_ident(view_name)} AS '
              f'SELECT * FROM {_quote_ident(first_alias)}')

    # -----------------------------------------------------------------------
    # Sandboxing & Security Guarantees:
    # 1. Authorizer Callback:
    #    Strictly restricts SQLite bytecode evaluation to read-only operations
    #    (SQLITE_SELECT, SQLITE_READ, SQLITE_FUNCTION, and SQLITE_RECURSIVE).
    #    Explicitly blocks ATTACH DATABASE, PRAGMA, DDL, and DML mutations,
    #    preventing queries from accessing local disk files or modifying state.
    # 2. Progress Handler & Row-Limit Guard:
    #    Defends against runaway queries (accidental Cartesian cross-joins or
    #    infinite recursive CTEs) by checking deadlines every 10k opcodes and
    #    enforcing BEAM_CLI_SQL_MAX_SECONDS (default 30s) and
    #    BEAM_CLI_SQL_MAX_RESULT_ROWS (default 1,000,000 rows).
    # -----------------------------------------------------------------------
    allowed_actions = {
        sqlite3.SQLITE_SELECT,
        sqlite3.SQLITE_READ,
        sqlite3.SQLITE_FUNCTION,
    }
    if hasattr(sqlite3, 'SQLITE_RECURSIVE'):
      allowed_actions.add(sqlite3.SQLITE_RECURSIVE)

    def _readonly_authorizer(action_code, arg1, arg2, dbname, source):
      del arg1, arg2, dbname, source
      if action_code in allowed_actions:
        return sqlite3.SQLITE_OK
      return sqlite3.SQLITE_DENY

    import time as _time
    raw_max_sec = os.environ.get('BEAM_CLI_SQL_MAX_SECONDS', '').strip()
    max_sql_sec = float(raw_max_sec) if raw_max_sec else 30.0
    raw_max_rows = os.environ.get('BEAM_CLI_SQL_MAX_RESULT_ROWS', '').strip()
    max_sql_rows = int(raw_max_rows) if raw_max_rows else 1_000_000
    sql_deadline = _time.monotonic() + max_sql_sec
    timed_out = False

    def _progress_guard() -> int:
      nonlocal timed_out
      if _time.monotonic() > sql_deadline:
        timed_out = True
        return 1
      return 0

    conn.set_progress_handler(_progress_guard, 10_000)
    conn.set_authorizer(_readonly_authorizer)
    try:
      cur = conn.execute(clean_q)
      out_cols = [d[0] for d in (cur.description or [])]
      fetched: list[Any] = []
      while True:
        chunk = cur.fetchmany(4096)
        if not chunk:
          break
        fetched.extend(chunk)
        if len(fetched) > max_sql_rows:
          raise SqlPipeError(
              f'SQL query exceeded local result limit ({max_sql_rows:,} rows); '
              'add LIMIT or run distributed SQL with --runner=DataflowRunner')
    except sqlite3.Error as e:
      if timed_out or 'interrupted' in str(e).lower():
        raise SqlPipeError(
            f'SQL query exceeded local execution time limit '
            f'({max_sql_sec:g}s); check for unbounded recursive CTEs or '
            'cross joins') from None
      avail_list = [ns.alias for ns in named_sources]
      if len(named_sources) == 1 and 'source' not in used_lower:
        avail_list.append('source')
      avail = ', '.join(avail_list)
      raise SqlPipeError(
          f'SQL execution error: {e} (available tables: {avail})'
          f'{_sql_error_hint(str(e), clean_q)}') from None

    out_rows = [dict(zip(out_cols, tup)) for tup in fetched]
    return out_cols, out_rows, (dlq_records or []), total_read
  finally:
    conn.close()


# `--to out.json --to-opt key=COL [--to-opt value=COL]`: one JSON document
# keyed by COL instead of JSON lines, for answers such as
# {"West": {"events": 6, ...}, "East": {...}}.
KEYED_JSON_OPTIONS = frozenset(['key', 'value'])


def keyed_json_options(sink_ep) -> dict[str, str] | None:
  """The key/value options of a keyed JSON document sink, or None."""
  if sink_ep is None or 'key' not in (sink_ep.config or {}):
    return None
  target = str(sink_ep.output_prefix or sink_ep.config.get('path') or '')
  if sink_ep.format != 'json' or not target.lower().endswith('.json'):
    raise SqlPipeError(
        '--to-opt key= writes one JSON document, so --to must be a local '
        '.json file (not .jsonl or another format)')
  return {k: str(sink_ep.config[k]) for k in KEYED_JSON_OPTIONS
          if k in sink_ep.config}


def _embedded_json(value):
  """JSON text from json_object()/json_group_array() is embedded as JSON."""
  if isinstance(value, str) and value[:1] in ('[', '{'):
    try:
      return json.loads(value)
    except ValueError:
      return value
  return value


def keyed_json_document(columns, rows, key: str, value: str | None = None):
  """`{row[key]: {other columns} | row[value]}`; fails on missing/dup keys."""
  if key not in columns:
    raise SqlPipeError(
        f'--to-opt key={key}: no such output column (columns: '
        f'{", ".join(columns)})')
  if value is not None and value not in columns:
    raise SqlPipeError(
        f'--to-opt value={value}: no such output column (columns: '
        f'{", ".join(columns)})')
  doc: dict[str, Any] = {}
  for r in rows:
    k = r.get(key)
    if k is None:
      raise SqlPipeError(
          f'--to-opt key={key}: a row has NULL {key}; replace it first, for '
          f"example COALESCE({key}, 'UNMAPPED')")
    k = str(k)
    if k in doc:
      raise SqlPipeError(
          f'--to-opt key={key}: value {k!r} appears more than once; '
          f'aggregate to one row per {key} first (GROUP BY {key})')
    if value is not None:
      doc[k] = _embedded_json(r.get(value))
    else:
      doc[k] = {c: _embedded_json(r.get(c)) for c in columns if c != key}
  return doc


def _new_sql_stage_path(target: str, tag: str) -> str:
  import tempfile
  abs_target = os.path.abspath(target)
  parent = os.path.dirname(abs_target) or '.'
  os.makedirs(parent, exist_ok=True)
  base = os.path.basename(abs_target) or 'out'
  fd, tmp_path = tempfile.mkstemp(
      prefix=f'.{base}.{tag}.', suffix='.tmp', dir=parent)
  os.close(fd)
  try:
    os.chmod(tmp_path, 0o644)
  except OSError:
    pass
  return tmp_path


def write_json_document(path: str, doc: Any) -> str:
  tmp_path = _new_sql_stage_path(path, 'doc')
  try:
    with open(tmp_path, 'w', encoding='utf-8') as f:
      json.dump(doc, f, indent=2, default=str)
      f.write('\n')
    os.replace(tmp_path, path)
  finally:
    if os.path.exists(tmp_path):
      try:
        os.unlink(tmp_path)
      except OSError:
        pass
  return path


def _mixed_columns_as_text(columns, rows, warnings=None):
  import pyarrow as pa
  arrays = {}
  for col in columns:
    values = [r.get(col) for r in rows]
    try:
      arrays[col] = pa.array(values)
    except (pa.ArrowInvalid, pa.ArrowTypeError):
      if warnings is not None:
        warnings.append(
            f'column {col!r} mixes numbers and text, so it was written as '
            f'text; use CAST({col} AS REAL) in --sql (non-numbers become '
            'NULL) or fix the source values')
      arrays[col] = pa.array([
          None if v is None else
          json.dumps(v, default=str) if isinstance(v, (dict, list)) else str(v)
          for v in values
      ],
                             type=pa.string())
  return pa.table(arrays)


def stage_rows_to_local_file(
    out_cols: list[str],
    out_rows: list[dict[str, Any]],
    sink_ep: uris.ParsedEndpoint,
    *,
    if_exists: str = 'fail',
    warnings: list[str] | None = None,
    tag: str = 'sql',
) -> tuple[str, str]:
  """Writes `out_rows` to an exclusive sibling `.tmp` file, returning `(tmp_path, final_path)`."""
  target = sink_ep.output_prefix or sink_ep.config.get('path')
  if not target:
    raise SqlPipeError(f'missing target path for sink {sink_ep.raw!r}')

  keyed = keyed_json_options(sink_ep)
  if keyed is not None:
    if if_exists == 'append':
      raise SqlPipeError('--to-opt key= cannot be combined with --if-exists append')
    doc = keyed_json_document(out_cols, out_rows, keyed['key'], keyed.get('value'))
    tmp_path = _new_sql_stage_path(target, tag)
    try:
      with open(tmp_path, 'w', encoding='utf-8') as f:
        json.dump(doc, f, indent=2, default=str)
        f.write('\n')
    except BaseException:
      if os.path.exists(tmp_path):
        try:
          os.unlink(tmp_path)
        except OSError:
          pass
      raise
    return tmp_path, target

  ext = os.path.splitext(target)[1].lower()
  has_explicit_ext = ext in (
      '.parquet',
      '.pq',
      '.json',
      '.jsonl',
      '.ndjson',
      '.csv',
      '.tsv',
      '.txt',
  )
  final_path = target if has_explicit_ext else f'{target}-00000-of-00001'
  rows_to_write = list(out_rows)
  if if_exists == 'append' and os.path.isfile(final_path):
    prev_ep = uris.parse_endpoint(
        final_path, is_read=True, explicit_format=sink_ep.format)
    _, prev_rows, _ = load_endpoint_rows(prev_ep)
    rows_to_write = prev_rows + rows_to_write
  tmp_path = _new_sql_stage_path(final_path, tag)

  fmt = sink_ep.format
  try:
    if fmt == 'parquet':
      import pyarrow as pa
      import pyarrow.parquet as pq
      if rows_to_write:
        try:
          tbl = pa.Table.from_pylist(rows_to_write)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
          # A column holds mixed types (SQLite is dynamically typed): write
          # those columns as text rather than failing the whole run.
          tbl = _mixed_columns_as_text(out_cols, rows_to_write, warnings)
      else:
        tbl = pa.table({c: pa.array([], type=pa.string()) for c in out_cols})
      pq.write_table(tbl, tmp_path)
    elif fmt == 'csv':
      delim = str(sink_ep.config.get('delimiter', ',') or ',')
      with open(tmp_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(
            f, fieldnames=out_cols, delimiter=delim, extrasaction='ignore')
        writer.writeheader()
        for r in rows_to_write:
          writer.writerow({
              k: ('' if v is None else json.dumps(v, default=str)
                  if isinstance(v, (dict, list)) else v)
              for k, v in r.items()
          })
    else:
      # Default JSONL (`json` / `text`).
      with open(tmp_path, 'w', encoding='utf-8') as f:
        for r in rows_to_write:
          f.write(json.dumps(r, default=str) + '\n')
  except BaseException:
    if os.path.exists(tmp_path):
      try:
        os.unlink(tmp_path)
      except OSError:
        pass
    raise
  return tmp_path, final_path


def write_rows_to_local_file(
    out_cols: list[str],
    out_rows: list[dict[str, Any]],
    sink_ep: uris.ParsedEndpoint,
    *,
    if_exists: str = 'fail',
    warnings: list[str] | None = None,
) -> list[str]:
  """Writes `out_rows` directly to a local file (`parquet`, `json`, `csv`)."""
  tmp_path, final_path = stage_rows_to_local_file(
      out_cols, out_rows, sink_ep, if_exists=if_exists, warnings=warnings)
  try:
    os.replace(tmp_path, final_path)
  finally:
    if os.path.exists(tmp_path):
      try:
        os.unlink(tmp_path)
      except OSError:
        pass
  return [final_path]
