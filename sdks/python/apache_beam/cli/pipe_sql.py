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
import datetime
import decimal
import glob
import io
import json
import math
import os
import re
import sqlite3
import threading
from typing import Any
from typing import Iterable
from typing import NamedTuple
from typing import Sequence

from . import dlq
from . import uris

dlq.ensure_csv_field_size_limit()

_SQL_THREAD_LOCAL = threading.local()
_EMPTY_PLACEHOLDER_COL = '__beam_cli_empty_placeholder__'

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
  # Strip compression suffix first (`events.csv.gz` -> `events.csv`), then
  # known extensions or Iceberg namespace prefix (`analytics.events` -> `events`).
  for comp_ext in ('.gz', '.bz2'):
    if stem.lower().endswith(comp_ext) and len(stem) > len(comp_ext):
      stem = stem[:-len(comp_ext)]
      break
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
  if len(q.encode('utf-8', errors='replace')) > 1_048_576:
    raise SqlPipeError('--sql query exceeds 1 MB safety limit')
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


class _ParsedAsofSql(NamedTuple):
  left_ns: NamedSource
  right_ns: NamedSource
  left_sql_alias: str
  right_sql_alias: str
  join_type: str
  left_keys: list[str]
  right_keys: list[str]
  left_time: str
  right_time: str
  direction: str
  strict: bool
  tolerance: float | None
  select_raw: str
  extra_on_filters: list[str]
  where_body: str | None
  tail_sql: str


def _parse_asof_sql_join(
    clean_q: str,
    named_sources: list[NamedSource],
    *,
    allow_post_clauses: bool,
) -> _ParsedAsofSql:
  """Parses a 2-source DuckDB/Snowflake `ASOF JOIN` SQL query."""
  hint_suffix = (
      '; supported form: SELECT ... FROM a [ASOF [LEFT|INNER] JOIN | '
      '[LEFT|INNER] ASOF JOIN] b [MATCH_CONDITION (a.ts >= b.ts)] '
      'ON a.k = b.k [AND a.ts >= b.ts] [AND a.ts - b.ts <= INTERVAL ...], '
      'or use --asof-join "key=k,left_time=ts,right_time=ts,tolerance=5m"')

  rewritten_q = _rewrite_source_uri_table_refs(clean_q, named_sources)
  code_only = ' '.join(
      seg for is_c, seg in _scan_sql_outside_literals(rewritten_q) if is_c)
  lead = code_only.lstrip('(').lstrip().split(
      None, 1)[0].upper() if code_only.strip() else ''
  if lead == 'WITH':
    raise SqlPipeError(
        'SQL ASOF JOIN lowering does not support CTEs (WITH ...)' + hint_suffix)
  if re.search(
      r'\b(?:RIGHT|FULL)\s+(?:OUTER\s+)?ASOF\b|\bASOF\s+(?:RIGHT|FULL)\b',
      code_only,
      re.I):
    raise SqlPipeError(
        'ASOF JOIN only supports INNER or LEFT join semantics '
        '(not RIGHT or FULL)' + hint_suffix)
  if len(named_sources) != 2 or len(re.findall(r'\bJOIN\b', code_only,
                                               re.I)) != 1:
    raise SqlPipeError(
        f'SQL ASOF JOIN requires exactly 2 sources and 1 JOIN '
        f'(got {len(named_sources)} source(s))' + hint_suffix)
  if not allow_post_clauses and re.search(
      r'\b(?:GROUP\s+BY|HAVING|UNION|INTERSECT|EXCEPT|OVER|QUALIFY|ORDER\s+BY|LIMIT)\b',
      code_only,
      re.I):
    raise SqlPipeError(
        'distributed SQL ASOF JOIN cannot combine ASOF JOIN with '
        'GROUP BY / ORDER BY / LIMIT / window analytics in one SQL step; '
        'use --asof-join followed by --window / --select' + hint_suffix)

  asof_re = re.compile(
      r'^\s*SELECT\s+(?P<select>.+?)\s+FROM\s+(?P<from_tbl>[A-Za-z0-9_"`]+)'
      r'(?:\s+(?:AS\s+)?(?P<from_alias>(?!(?:ASOF|INNER|LEFT|OUTER|JOIN)\b)[A-Za-z0-9_"`]+))?'
      r'\s+(?P<j_spec>(?:ASOF(?:\s+(?:INNER|LEFT(?:\s+OUTER)?))?|(?:INNER|LEFT(?:\s+OUTER)?)\s+ASOF))\s+JOIN\s+'
      r'(?P<join_tbl>[A-Za-z0-9_"`]+)'
      r'(?:\s+(?:AS\s+)?(?P<join_alias>(?!(?:MATCH_CONDITION|ON|WHERE|GROUP|ORDER|LIMIT)\b)[A-Za-z0-9_"`]+))?'
      r'(?:\s+MATCH_CONDITION\s*\((?P<match_cond>[^)]+)\))?'
      r'\s+ON\s+(?P<after_on>.+?)\s*$',
      re.I | re.S)
  m = asof_re.match(rewritten_q)
  if not m:
    raise SqlPipeError('could not parse SQL ASOF JOIN safely' + hint_suffix)

  # Split `after_on` cleanly at top-level WHERE / GROUP BY / ORDER BY / LIMIT
  after_on = m.group('after_on')
  tail_kw_re = re.compile(
      r'\b(?:WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT)\b', re.I)
  # Locate earliest top-level clause boundary outside parens/literals
  depth = 0
  pos = 0
  split_where_start: int | None = None
  split_tail_start: int | None = None
  for is_c, seg in _scan_sql_outside_literals(after_on):
    if not is_c:
      pos += len(seg)
      continue
    idx_in_seg = 0
    while idx_in_seg < len(seg):
      ch = seg[idx_in_seg]
      if ch == '(':
        depth += 1
        idx_in_seg += 1
        continue
      if ch == ')':
        depth = max(0, depth - 1)
        idx_in_seg += 1
        continue
      if depth == 0:
        km = tail_kw_re.match(seg[idx_in_seg:])
        if km and (idx_in_seg == 0 or not (seg[idx_in_seg - 1].isalnum() or
                                           seg[idx_in_seg - 1] == '_')):
          kw_up = km.group(0).upper()
          abs_p = pos + idx_in_seg
          if kw_up == 'WHERE' and split_where_start is None and split_tail_start is None:
            split_where_start = abs_p
            idx_in_seg += len(km.group(0))
            continue
          if kw_up != 'WHERE' and split_tail_start is None:
            split_tail_start = abs_p
            break
      idx_in_seg += 1
    if split_tail_start is not None:
      break
    pos += len(seg)

  if split_where_start is not None:
    on_body = after_on[:split_where_start].strip()
    where_end = split_tail_start if split_tail_start is not None else len(
        after_on)
    where_slice = after_on[split_where_start:where_end].strip()
    where_body = re.sub(
        r'^WHERE\s+', '', where_slice, count=1, flags=re.I).strip()
    tail_sql = after_on[split_tail_start:].strip(
    ) if split_tail_start is not None else ''
  elif split_tail_start is not None:
    on_body = after_on[:split_tail_start].strip()
    where_body = None
    tail_sql = after_on[split_tail_start:].strip()
  else:
    on_body = after_on.strip()
    where_body = None
    tail_sql = ''

  by_alias_low = {ns.alias.lower(): ns for ns in named_sources}
  from_tbl = _unquote_sql_ident(m.group('from_tbl'))
  join_tbl = _unquote_sql_ident(m.group('join_tbl'))
  left_ns = by_alias_low.get(from_tbl.lower())
  right_ns = by_alias_low.get(join_tbl.lower())
  if left_ns is None or right_ns is None or left_ns.alias == right_ns.alias:
    raise SqlPipeError(
        f'SQL ASOF JOIN tables ({from_tbl!r}, {join_tbl!r}) must match the two '
        f'distinct input aliases {[ns.alias for ns in named_sources]}' +
        hint_suffix)

  left_sql_alias = _unquote_sql_ident(m.group('from_alias') or left_ns.alias)
  right_sql_alias = _unquote_sql_ident(m.group('join_alias') or right_ns.alias)
  l_low = left_sql_alias.lower()
  r_low = right_sql_alias.lower()

  j_spec_up = m.group('j_spec').upper()
  join_type = 'left' if 'LEFT' in j_spec_up else (
      'inner' if 'INNER' in j_spec_up else 'left')
  # Note: bare `ASOF JOIN` in DuckDB defaults to INNER, whereas `ASOF LEFT JOIN` is LEFT:
  if 'LEFT' not in j_spec_up:
    join_type = 'inner'

  all_conds: list[str] = []
  if m.group('match_cond'):
    all_conds.extend(_split_top_level_sql_and(m.group('match_cond')))
  all_conds.extend(_split_top_level_sql_and(on_body))

  cmp_re = re.compile(
      r'^\s*(?P<lhs>.+?)\s*(?P<op><=|>=|<>|!=|=|<|>)\s*(?P<rhs>.+?)\s*$', re.S)
  diff_interval_re = re.compile(
      r'^\s*(?P<a1>[A-Za-z0-9_"`]+)\s*\.\s*(?P<c1>[A-Za-z0-9_"`]+)\s*-\s*'
      r'(?P<a2>[A-Za-z0-9_"`]+)\s*\.\s*(?P<c2>[A-Za-z0-9_"`]+)\s*<=\s*'
      r'(?:INTERVAL\s+(?:\'(?P<qnum>[0-9.]+)\'\s*(?P<qunit>[A-Za-z]+)|'
      r'\'(?P<qnum2>[0-9.]+)\s+(?P<qunit2>[A-Za-z]+)\'|'
      r'(?P<num>[0-9.]+)\s+(?P<unit>[A-Za-z]+))|(?P<sec>[0-9.]+))\s*$',
      re.I | re.S)

  left_keys: list[str] = []
  right_keys: list[str] = []
  left_time: str | None = None
  right_time: str | None = None
  direction: str | None = None
  strict = False
  tolerance: float | None = None
  extra_on_filters: list[str] = []

  for raw_c in all_conds:
    conj = raw_c.strip()
    while conj.startswith('(') and conj.endswith(')'):
      inner_c = conj[1:-1].strip()
      if len(_split_top_level_sql_and(inner_c)) == 1:
        conj = inner_c
      else:
        break

    mdiff = diff_interval_re.match(conj)
    if mdiff:
      a1 = _unquote_sql_ident(mdiff.group('a1')).lower()
      c1 = _unquote_sql_ident(mdiff.group('c1'))
      a2 = _unquote_sql_ident(mdiff.group('a2')).lower()
      c2 = _unquote_sql_ident(mdiff.group('c2'))
      if {a1, a2} == {l_low, r_low}:
        if mdiff.group('sec'):
          tol_val = float(mdiff.group('sec'))
        else:
          n_s = mdiff.group('qnum') or mdiff.group('qnum2') or mdiff.group(
              'num')
          u_s = mdiff.group('qunit') or mdiff.group('qunit2') or mdiff.group(
              'unit')
          tol_val = _interval_unit_to_seconds(n_s, u_s)
        tolerance = tol_val if tolerance is None else min(tolerance, tol_val)
        l_col = c1 if a1 == l_low else c2
        r_col = c2 if a2 == r_low else c1
        left_time = left_time or l_col
        right_time = right_time or r_col
        continue

    mc = cmp_re.match(conj)
    if not mc:
      extra_on_filters.append(conj)
      continue
    op = mc.group('op')
    p_lhs = _parse_ts_offset_expr(mc.group('lhs'))
    p_rhs = _parse_ts_offset_expr(mc.group('rhs'))
    if (p_lhs and p_rhs and
        {p_lhs[0].lower(), p_rhs[0].lower()} == {l_low, r_low}):
      if p_lhs[0].lower() == l_low:
        l_col, l_off = p_lhs[1], p_lhs[2]
        r_col, r_off = p_rhs[1], p_rhs[2]
        eff_op = op
      else:
        r_col, r_off = p_lhs[1], p_lhs[2]
        l_col, l_off = p_rhs[1], p_rhs[2]
        eff_op = {'<=': '>=', '>=': '<=', '<': '>', '>': '<'}.get(op, op)

      if eff_op == '=' and l_off == 0.0 and r_off == 0.0:
        left_keys.append(l_col)
        right_keys.append(r_col)
        continue

      # Point-in-time ASOF inequality (zero offset): e.g. `l.ts >= r.ts`
      if l_off == 0.0 and r_off == 0.0 and eff_op in ('>=', '>', '<=', '<'):
        left_time = left_time or l_col
        right_time = right_time or r_col
        if eff_op in ('>=', '>'):
          direction = 'backward'
          strict = eff_op == '>'
        else:
          direction = 'forward'
          strict = eff_op == '<'
        continue

      # Offset tolerance inequality: e.g. `r.ts >= l.ts - INTERVAL '5' MINUTE`
      # (`l.ts + l_off <= r.ts + r_off` -> `r.ts >= l.ts + (l_off - r_off)`)
      shift = r_off - l_off
      left_time = left_time or l_col
      right_time = right_time or r_col
      if eff_op in ('<=', '<') and shift > 0:
        # `l.ts <= r.ts + tol` -> `t_l - t_r <= tol` (backward tolerance!)
        tolerance = shift if tolerance is None else min(tolerance, shift)
        continue
      if eff_op in ('>=', '>') and shift < 0:
        # `l.ts >= r.ts - tol` -> `t_r - t_l <= tol` (forward tolerance!)
        tolerance = (-shift) if tolerance is None else min(tolerance, -shift)
        continue

    extra_on_filters.append(conj)

  if not left_keys or not left_time or not right_time or direction is None:
    raise SqlPipeError(
        'SQL ASOF JOIN requires both an equi-join key (a.id = b.id) and a '
        'temporal inequality (e.g. a.ts >= b.ts or MATCH_CONDITION(a.ts >= b.ts))'
        + hint_suffix)
  if join_type == 'left' and extra_on_filters:
    raise SqlPipeError(
        f'unsupported non-key ON predicate(s) {extra_on_filters} in LEFT ASOF JOIN; '
        'move post-join filters to WHERE')

  return _ParsedAsofSql(
      left_ns=left_ns,
      right_ns=right_ns,
      left_sql_alias=left_sql_alias,
      right_sql_alias=right_sql_alias,
      join_type=join_type,
      left_keys=left_keys,
      right_keys=right_keys,
      left_time=left_time,
      right_time=right_time,
      direction=direction,
      strict=strict,
      tolerance=tolerance,
      select_raw=m.group('select').strip(),
      extra_on_filters=extra_on_filters,
      where_body=where_body,
      tail_sql=tail_sql,
  )


def _lower_asof_join_sql(
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
  """Fail-closed lowering of 2-source SQL `ASOF JOIN` onto `CliAsofJoin`."""
  from . import asof_join
  from . import dlq
  from . import expressions
  from . import interval_join
  from . import output

  parsed = _parse_asof_sql_join(
      clean_q, named_sources, allow_post_clauses=False)
  if has_unbounded and parsed.direction == 'forward' and parsed.tolerance is None:
    raise SqlPipeError(
        'unbounded streaming forward SQL ASOF JOIN (a.ts <= b.ts) requires a '
        "finite tolerance bound (e.g. AND b.ts <= a.ts + INTERVAL '5' MINUTE)")

  select_raw = parsed.select_raw
  if re.match(r'^DISTINCT\b', select_raw, re.I):
    raise SqlPipeError(
        'distributed SQL ASOF JOIN does not support SELECT DISTINCT; '
        'use --asof-join with --dedupe-by instead')

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
            'distributed SQL ASOF JOIN; use SELECT * or list columns explicitly'
        )
      if re.search(r'\b(?:COUNT|SUM|AVG|MIN|MAX)\s*\(', tok, re.I):
        raise SqlPipeError(
            f'aggregate expression {tok!r} in distributed SQL ASOF JOIN '
            'requires a separate --window aggregation stage')
      ma = as_re.match(tok) or bare_alias_re.match(tok)
      if ma:
        expr_s = ma.group('expr').strip()
        out_name = _unquote_sql_ident(ma.group('alias'))
      else:
        expr_s = tok
        out_name = _unquote_sql_ident(
            tok.rsplit('.', 1)[-1] if '.' in tok else tok)
      if not output.SAFE_IDENTIFIER_RE.match(out_name):
        raise SqlPipeError(
            f'SELECT expression {tok!r} requires an explicit AS <column_name> alias'
        )
      if out_name in select_map:
        raise SqlPipeError(
            f'duplicate output column name {out_name!r} in SELECT; disambiguate with AS'
        )
      expr_norm = re.sub(r'([`"])([A-Za-z_][A-Za-z0-9_]*)\1', r'\2',
                         expr_s).strip()
      select_map[out_name] = expr_norm

  where_parts: list[str] = list(parsed.extra_on_filters)
  if parsed.where_body:
    where_parts.append(f'({parsed.where_body})')
  combined_where = ' AND '.join(where_parts) if where_parts else None

  temp_where_cols: list[str] = []
  final_user_cols: list[str] = list(select_map.keys()) if select_map else []
  compiled_where_cfg: dict[str, Any] | None = None
  l_low = parsed.left_sql_alias.lower()
  r_low = parsed.right_sql_alias.lower()

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
        canon_ref = (
            f'{parsed.left_sql_alias if qa.lower() == l_low else parsed.right_sql_alias}.{qc}'
        )
        if canon_ref in rev_map:
          return rev_map[canon_ref]
        tmp_col = f'beam_sql_w{len(temp_where_cols)}'
        temp_where_cols.append(tmp_col)
        select_map[tmp_col] = canon_ref
        rev_map[canon_ref] = tmp_col
        return tmp_col

      norm_where = ''.join(
          qual_ref_re.sub(_sub_qual, seg) if is_c else seg
          for is_c, seg in _scan_sql_outside_literals(norm_where))
    else:

      def _sub_star_qual(qm: re.Match) -> str:
        qa = qm.group('a')
        qc = qm.group('c')
        return qc if qa.lower() in (l_low, r_low) else qm.group(0)

      norm_where = ''.join(
          qual_ref_re.sub(_sub_star_qual, seg) if is_c else seg
          for is_c, seg in _scan_sql_outside_literals(norm_where))
    try:
      compiled_where_cfg = expressions.compile_where(
          norm_where, known_columns=None, allow_jinja=allow_jinja)[0]
    except expressions.ExpressionError as e:
      raise SqlPipeError(
          f'unsupported WHERE filter in SQL ASOF JOIN: {e}') from None

  eff_skew_action = skew_action or (
      'dlq' if
      (dlq_ep is not None and max_watermark_skew is not None) else 'drop')
  if eff_skew_action == 'dlq' and dlq_ep is None:
    raise SqlPipeError('--skew-action=dlq requires --dlq <SINK>')

  aj_cfg: dict[str, Any] = {
      'left_alias': parsed.left_sql_alias,
      'right_alias': parsed.right_sql_alias,
      'left_key': (
          parsed.left_keys[0]
          if len(parsed.left_keys) == 1 else parsed.left_keys),
      'right_key': (
          parsed.right_keys[0]
          if len(parsed.right_keys) == 1 else parsed.right_keys),
      'left_time': parsed.left_time,
      'right_time': parsed.right_time,
      'direction': parsed.direction,
      'join_type': parsed.join_type,
      'skew_action': eff_skew_action,
  }
  if parsed.strict:
    aj_cfg['strict'] = True
  if parsed.tolerance is not None:
    aj_cfg['tolerance'] = _format_seconds_duration(parsed.tolerance)
  if select_map is not None:
    aj_cfg['select'] = select_map
  if max_watermark_skew is not None:
    aj_cfg['max_watermark_skew'] = max_watermark_skew

  try:
    asof_join.parse_asof_join_config(aj_cfg)
  except ValueError as e:
    raise SqlPipeError(str(e)) from None

  transforms = list(read_transforms)
  dlq_norm_refs: list[str] = []
  left_read_ref = sql_inputs[parsed.left_ns.alias]
  right_read_ref = sql_inputs[parsed.right_ns.alias]
  ts_l_name = f'AssignTimestamps_{parsed.left_ns.alias}'
  ts_r_name = f'AssignTimestamps_{parsed.right_ns.alias}'
  ts_l_cfg: dict[str, Any] = {'timestamp_col': parsed.left_time}
  ts_r_cfg: dict[str, Any] = {'timestamp_col': parsed.right_time}
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
    aj_left_ref = f'{ts_l_name}.good'
    aj_right_ref = f'{ts_r_name}.good'
    n_l = f'NormalizeTsLeftDlq_{parsed.left_ns.alias}'
    n_r = f'NormalizeTsRightDlq_{parsed.right_ns.alias}'
    transforms.append(
        dlq.build_dlq_normalizer_transform(
            n_l, f'{ts_l_name}.errors', ts_l_name))
    transforms.append(
        dlq.build_dlq_normalizer_transform(
            n_r, f'{ts_r_name}.errors', ts_r_name))
    dlq_norm_refs.extend([n_l, n_r])
    aj_cfg['error_handling'] = {'output': 'errors'}
  else:
    aj_left_ref = ts_l_name
    aj_right_ref = ts_r_name

  transforms.append({
      'name': 'AsofJoin',
      'type': 'CliAsofJoin',
      'input': {
          'left': aj_left_ref,
          'right': aj_right_ref,
      },
      'config': aj_cfg,
  })
  if dlq_ep is not None:
    current_ref = 'AsofJoin.good'
    transforms.append(
        dlq.build_dlq_normalizer_transform(
            'NormalizeAsofJoinDlq', 'AsofJoin.errors', 'CliAsofJoin'))
    dlq_norm_refs.append('NormalizeAsofJoinDlq')
  else:
    current_ref = 'AsofJoin'

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


def _merge_extra_sql_dlq_refs(
    spec_out: dict[str, Any],
    extra_refs: list[str],
    dlq_ep: uris.ParsedEndpoint,
) -> None:
  """Merges source DLQ normalizer refs into a lowered SQL join's `WriteDlq`."""
  if not extra_refs:
    return
  transforms = spec_out.setdefault('pipeline', {}).setdefault('transforms', [])
  merge_node = next(
      (t for t in transforms if t.get('name') == 'MergeDlq'), None)
  write_node = next(
      (t for t in transforms if t.get('name') == 'WriteDlq'), None)
  if merge_node is not None:
    cur_in = list(merge_node.get('input') or [])
    for r in extra_refs:
      if r not in cur_in:
        cur_in.insert(0, r)
    merge_node['input'] = cur_in
  elif write_node is not None:
    prev_in = write_node['input']
    all_refs = list(extra_refs) + (
        [prev_in] if isinstance(prev_in, str) else list(prev_in))
    if len(all_refs) == 1:
      write_node['input'] = all_refs[0]
    else:
      w_idx = transforms.index(write_node)
      transforms.insert(
          w_idx, {
              'name': 'MergeDlq',
              'type': 'Flatten',
              'input': all_refs,
          })
      write_node['input'] = 'MergeDlq'
  else:
    if len(extra_refs) == 1:
      dlq_in = extra_refs[0]
    else:
      transforms.append({
          'name': 'MergeDlq',
          'type': 'Flatten',
          'input': list(extra_refs),
      })
      dlq_in = 'MergeDlq'
    transforms.append({
        'name': 'WriteDlq',
        'type': dlq_ep.yaml_type,
        'input': dlq_in,
        'config': dict(dlq_ep.config),
    })


def _rewrite_sql_case_and_cast(code_seg: str) -> str:
  """Lowers SQL CAST(...) and CASE WHEN ... END on placeholder-protected SQL."""
  out = code_seg.replace('<>', '!=')
  cast_re = re.compile(
      r'\bCAST\s*\(\s*(?P<inner>(?:[^()]|\([^()]*\))+?)\s+AS\s+'
      r'(?P<typ>[A-Za-z0-9_]+)(?:\s*\([^()]*\))?\s*\)',
      re.I)
  for _ in range(6):

    def _rep_cast(m: re.Match[str]) -> str:
      inner = m.group('inner').strip()
      t = m.group('typ').upper()
      if t in ('INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'INT64'):
        return f'int({inner})'
      if t in ('FLOAT', 'DOUBLE', 'REAL', 'DECIMAL', 'NUMERIC', 'FLOAT64'):
        return f'float({inner})'
      if t in ('VARCHAR', 'CHAR', 'TEXT', 'STRING'):
        return f'str({inner})'
      if t in ('BOOL', 'BOOLEAN'):
        return f'bool({inner})'
      return m.group(0)

    nxt = cast_re.sub(_rep_cast, out)
    if nxt == out:
      break
    out = nxt

  # Also lower simple searched `CASE x WHEN v1 THEN r1 ... END` and `CASE WHEN c1 THEN r1 ... END`
  case_re = re.compile(r'\bCASE\b(?P<body>[\s\S]+?)\bEND\b', re.I)
  when_re = re.compile(
      r'\bWHEN\s+(?P<cond>[\s\S]+?)\s+THEN\s+(?P<val>[\s\S]+?)(?=\bWHEN\b|\bELSE\b|$)',
      re.I)
  else_re = re.compile(r'\bELSE\s+(?P<els>[\s\S]+)$', re.I)

  def _rep_case(m: re.Match[str]) -> str:
    body = m.group('body').strip()
    if re.search(r'\bCASE\b', body, re.I):
      return m.group(0)
    first_when = re.search(r'\bWHEN\b', body, re.I)
    if not first_when:
      return m.group(0)
    subject = body[:first_when.start()].strip()
    rest_body = body[first_when.start():]
    branches = list(when_re.finditer(rest_body))
    if not branches:
      return m.group(0)
    em = else_re.search(rest_body[branches[-1].end():])
    cur = f'({em.group("els").strip()})' if em else 'None'
    for br in reversed(branches):
      c_s = br.group('cond').strip()
      if subject:
        c_s = f'({subject}) == ({c_s})'
      v_s = br.group('val').strip()
      cur = f'({v_s} if ({c_s}) else {cur})'
    return cur

  for _ in range(6):
    nxt = case_re.sub(_rep_case, out)
    if nxt == out:
      break
    out = nxt
  return out


def _normalize_sql_scalar_expr(
    raw_expr: str, table_aliases: set[str]) -> str:
  low_aliases = {a.lower() for a in table_aliases if a}
  qual_re = re.compile(
      r'(?<![A-Za-z0-9_])(?P<qa>[A-Za-z_][A-Za-z0-9_]*|`[^`]+`|"[^"]+")\s*\.\s*'
      r'(?P<qc>[A-Za-z_][A-Za-z0-9_]*|`[^`]+`|"[^"]+"|\*)')
  lit_map: dict[str, str] = {}
  protected_parts: list[str] = []
  for is_c, seg in _scan_sql_outside_literals(raw_expr):
    if not is_c:
      tok = f'__SQLLIT_{len(lit_map)}__'
      lit_map[tok] = seg
      protected_parts.append(tok)
      continue

    def _sub_q(m: re.Match[str]) -> str:
      qa = _unquote_sql_ident(m.group('qa')).lower()
      if qa in low_aliases:
        return m.group('qc')
      return m.group(0)

    protected_parts.append(qual_re.sub(_sub_q, seg))

  rewritten = _rewrite_sql_case_and_cast(''.join(protected_parts))
  for tok, orig_lit in lit_map.items():
    rewritten = rewritten.replace(tok, orig_lit)
  return rewritten.strip()


def _parse_sql_interval_literal(raw_iv: str) -> float:
  m = re.match(
      r"^\s*INTERVAL\s+(?:'(?P<qn>[0-9.]+)'\s*(?P<qu>[A-Za-z]+)|'(?P<qn2>[0-9.]+)\s+(?P<qu2>[A-Za-z]+)'|(?P<n>[0-9.]+)\s+(?P<u>[A-Za-z]+))\s*$",
      raw_iv.strip(),
      re.I)
  if not m:
    raise SqlPipeError(f'unsupported SQL INTERVAL literal: {raw_iv!r}')
  n_s = m.group('qn') or m.group('qn2') or m.group('n')
  u_s = m.group('qu') or m.group('qu2') or m.group('u')
  return _interval_unit_to_seconds(n_s, u_s)


def has_streaming_window_sql(sql_query: str | None) -> bool:
  """Returns True when `sql_query` uses Calcite TVF or GROUP BY windowing."""
  if not sql_query:
    return False
  code_only = ' '.join(
      seg for is_c, seg in _scan_sql_outside_literals(sql_query) if is_c)
  return bool(
      re.search(
          r'\bTABLE\s*\(\s*(?:TUMBLE|HOP|SESSION)\s*\('
          r'|\b(?:TUMBLE|HOP|SESSION)(?:_START|_END|_ROWTIME)?\s*\(',
          code_only,
          re.I))


def _rewrite_sql_window_bound_calls(expr_s: str) -> str:
  """Normalizes TUMBLE_START/END, HOP_START/END, SESSION_START/END & window_*."""
  start_re = re.compile(
      r'\b(?:TUMBLE|HOP|SESSION)_START\s*\((?:[^()]|\([^()]*\))+\)', re.I)
  end_re = re.compile(
      r'\b(?:TUMBLE|HOP|SESSION)_(?:END|ROWTIME)\s*\((?:[^()]|\([^()]*\))+\)',
      re.I)
  ws_re = re.compile(r'\bwindow_start\b', re.I)
  we_re = re.compile(r'\b(?:window_end|window_time)\b', re.I)
  literals: list[str] = []
  masked_parts: list[str] = []
  for is_c, seg in _scan_sql_outside_literals(expr_s):
    if is_c:
      masked_parts.append(seg)
    else:
      tok = f'__BEAM_WLIT_{len(literals)}__'
      literals.append(seg)
      masked_parts.append(tok)
  s = ''.join(masked_parts)
  s = start_re.sub('window_start', s)
  s = end_re.sub('window_end', s)
  s = ws_re.sub('window_start', s)
  s = we_re.sub('window_end', s)
  for idx, lit in enumerate(literals):
    s = s.replace(f'__BEAM_WLIT_{idx}__', lit)
  return s


class LoweredSqlPlan(NamedTuple):
  transforms: list[dict[str, Any]]
  spec: dict[str, Any]


def try_lower_sql_to_python_yaml(
    sql_query: str,
    source_aliases: Iterable[str] = ('source',),
    *,
    strict: bool = False,
) -> LoweredSqlPlan | None:
  """Unit-testable entrypoint returning pure-Python lowered YAML transforms."""
  aliases = list(source_aliases) or ['source']
  clean_q = validate_readonly_select_sql(sql_query, allow_jinja=False)
  if len(aliases) != 1:
    if strict:
      raise SqlPipeError(
          '--sql-engine=python single-source lowering expects 1 source alias')
    return None
  dummy_ep = uris.ParsedEndpoint(
      kind='file',
      scheme='memory',
      raw='memory://input.jsonl',
      format='json',
      yaml_type='ReadFromJson',
      config={'path': 'memory://input.jsonl'})
  ns = NamedSource(alias=aliases[0], uri='memory://input.jsonl', endpoint=dummy_ep)
  spec = _try_lower_single_source_sql(
      ns,
      clean_q,
      [],
      f'Read_{ns.alias}',
      None,
      has_unbounded=False,
      wire_yaml_dlq=False,
      dlq_ep=None,
      sql_dlq_norm_refs=[],
      allow_jinja=False,
      strict=strict)
  if spec is None:
    return None
  return LoweredSqlPlan(
      transforms=list(spec['pipeline']['transforms']), spec=spec)


def _try_lower_single_source_sql(
    only_ns: NamedSource,
    clean_q: str,
    read_transforms: list[dict[str, Any]],
    read_ref: str,
    sink_ep: uris.ParsedEndpoint | None,
    *,
    has_unbounded: bool,
    wire_yaml_dlq: bool,
    dlq_ep: uris.ParsedEndpoint | None,
    sql_dlq_norm_refs: list[str],
    allow_jinja: bool,
    strict: bool,
) -> dict[str, Any] | None:
  """Lowers single-source SQL into pure-Python Beam YAML transforms (zero JVM)."""
  from . import expressions
  from . import output
  from . import pipe_spec

  def _bail(reason: str) -> None:
    if strict:
      raise SqlPipeError(
          f'--sql-engine=python cannot lower query ({reason}); supported pure-Python '
          'constructs are single-source SELECT [DISTINCT] / WHERE / GROUP BY / '
          'HAVING / TUMBLE / HOP / SESSION / LIMIT and 2-source ASOF / INTERVAL JOIN. '
          'Omit --sql-engine=python to use Calcite SQL or local DuckDB/SQLite.')

  code_upper = ' '.join(
      seg for is_c, seg in _scan_sql_outside_literals(clean_q) if is_c)
  if re.search(r'^\s*WITH\b', code_upper, re.I):
    _bail('WITH CTEs require Calcite or local SQL engine')
    return None
  if re.search(r'\bOVER\s*\(', code_upper, re.I):
    _bail('analytic OVER(...) window functions require Calcite or local SQL engine')
    return None
  if re.search(r'\b(?:UNION|INTERSECT)\b|\bEXCEPT\s+(?:ALL\s+|DISTINCT\s+)?SELECT\b',
               code_upper, re.I):
    _bail('set operations (UNION/INTERSECT/EXCEPT SELECT) require Calcite or local SQL engine')
    return None

  # Scan top-level clause boundaries outside string literals and parentheses.
  kw_re = re.compile(
      r'\b(?P<kw>SELECT|FROM|WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|JOIN)\b',
      re.I)
  clauses: list[tuple[str, int, int]] = []
  depth = 0
  pos = 0
  for is_c, seg in _scan_sql_outside_literals(clean_q):
    if not is_c:
      pos += len(seg)
      continue
    i = 0
    while i < len(seg):
      ch = seg[i]
      if ch == '(':
        depth += 1
        i += 1
        continue
      if ch == ')':
        depth = max(0, depth - 1)
        i += 1
        continue
      prev_ok = (i == 0 or not (seg[i - 1].isalnum() or seg[i - 1] == '_'))
      if prev_ok:
        km = kw_re.match(seg[i:])
        if km:
          kw_norm = re.sub(r'\s+', ' ', km.group('kw').upper())
          if depth > 0 and kw_norm == 'SELECT':
            _bail('nested subqueries require Calcite or local SQL engine')
            return None
          if depth == 0:
            if kw_norm == 'JOIN':
              _bail('non-temporal JOIN requires Calcite or local SQL engine')
              return None
            if kw_norm == 'ORDER BY':
              _bail('ORDER BY is not supported on unordered distributed PCollections')
              return None
            clauses.append((kw_norm, pos + i, pos + i + len(km.group(0))))
            i += len(km.group(0))
            continue
      i += 1
    pos += len(seg)

  if len(clauses) < 2 or clauses[0][0] != 'SELECT' or clauses[1][0] != 'FROM':
    _bail('expected SELECT ... FROM ...')
    return None
  expected_order = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'LIMIT']
  seen_order: list[str] = []
  clause_bodies: dict[str, str] = {}
  for idx, (kw_name, _s, e_pos) in enumerate(clauses):
    if kw_name in clause_bodies or kw_name not in expected_order:
      _bail(f'duplicate or unsupported clause {kw_name}')
      return None
    if seen_order and expected_order.index(kw_name) < expected_order.index(seen_order[-1]):
      _bail(f'out-of-order SQL clause {kw_name}')
      return None
    seen_order.append(kw_name)
    next_s = clauses[idx + 1][1] if idx + 1 < len(clauses) else len(clean_q)
    clause_bodies[kw_name] = clean_q[e_pos:next_s].strip()

  select_raw = clause_bodies['SELECT']
  is_distinct = False
  if re.match(r'^DISTINCT\b', select_raw, re.I):
    is_distinct = True
    select_raw = re.sub(r'^DISTINCT\s+', '', select_raw, count=1, flags=re.I).strip()
  elif re.match(r'^ALL\b', select_raw, re.I):
    select_raw = re.sub(r'^ALL\s+', '', select_raw, count=1, flags=re.I).strip()

  from_raw = clause_bodies['FROM']
  window_cfg: dict[str, Any] | None = None
  ts_col: str | None = None
  table_aliases: set[str] = {only_ns.alias, 'source', 'PCOLLECTION'}

  tvf_m = re.match(
      r'^TABLE\s*\(\s*(?P<fn>TUMBLE|HOP|SESSION)\s*\(\s*TABLE\s+(?P<tbl>[A-Za-z0-9_"`]+)\s*,\s*'
      r'DESCRIPTOR\s*\(\s*(?P<ts>[A-Za-z0-9_"`]+)\s*\)\s*,\s*(?P<ivs>.+?)\s*\)\s*\)'
      r'(?:\s+(?:AS\s+)?(?P<alias>[A-Za-z_][A-Za-z0-9_]*))?\s*$',
      from_raw,
      re.I | re.S)
  if tvf_m:
    tbl_ident = _unquote_sql_ident(tvf_m.group('tbl'))
    if tbl_ident.lower() not in {a.lower() for a in table_aliases}:
      _bail(f'unknown source table {tbl_ident!r}')
      return None
    if tvf_m.group('alias'):
      table_aliases.add(_unquote_sql_ident(tvf_m.group('alias')))
    ts_col = _unquote_sql_ident(tvf_m.group('ts'))
    iv_parts = expressions._split_top_level_commas(tvf_m.group('ivs'))
    w_fn = tvf_m.group('fn').upper()
    if w_fn == 'TUMBLE' and len(iv_parts) == 1:
      window_cfg = {
          'type': 'fixed',
          'size': _format_seconds_duration(_parse_sql_interval_literal(iv_parts[0])),
      }
    elif w_fn == 'HOP' and len(iv_parts) == 2:
      window_cfg = {
          'type': 'sliding',
          'period': _format_seconds_duration(_parse_sql_interval_literal(iv_parts[0])),
          'size': _format_seconds_duration(_parse_sql_interval_literal(iv_parts[1])),
      }
    elif w_fn == 'SESSION' and len(iv_parts) == 1:
      window_cfg = {
          'type': 'sessions',
          'gap': _format_seconds_duration(_parse_sql_interval_literal(iv_parts[0])),
      }
    else:
      _bail(f'invalid {w_fn} TVF interval arguments')
      return None
  else:
    tbl_m = re.match(
        r'^(?P<tbl>[A-Za-z0-9_"`]+)(?:\s+(?:AS\s+)?(?P<alias>(?!(?:WHERE|GROUP|HAVING|LIMIT|ORDER|JOIN)\b)[A-Za-z_][A-Za-z0-9_]*))?\s*$',
        from_raw,
        re.I)
    if not tbl_m:
      _bail(f'unsupported FROM clause {from_raw!r}')
      return None
    tbl_ident = _unquote_sql_ident(tbl_m.group('tbl'))
    if tbl_ident.lower() not in {a.lower() for a in table_aliases}:
      _bail(f'unknown source table {tbl_ident!r}')
      return None
    if tbl_m.group('alias'):
      table_aliases.add(_unquote_sql_ident(tbl_m.group('alias')))

  limit_val: int | None = None
  if 'LIMIT' in clause_bodies:
    if not re.fullmatch(r'[0-9]+', clause_bodies['LIMIT']):
      _bail(f'LIMIT expects a non-negative integer, got {clause_bodies["LIMIT"]!r}')
      return None
    limit_val = int(clause_bodies['LIMIT'])

  transforms = list(read_transforms)
  dlq_refs = list(sql_dlq_norm_refs)
  current_ref = read_ref

  group_raw = clause_bodies.get('GROUP BY')
  raw_group_exprs: list[str] = []
  if group_raw:
    leg_win_re = re.compile(
        r'^(?P<fn>TUMBLE|HOP|SESSION)\s*\(\s*(?P<ts>[A-Za-z0-9_`".]+)\s*,\s*(?P<ivs>.+)\s*\)\s*$',
        re.I | re.S)
    for g_part in expressions._split_top_level_commas(group_raw):
      gm = leg_win_re.match(g_part.strip())
      if gm:
        if window_cfg is not None:
          _bail('cannot specify multiple windowing functions in FROM and GROUP BY')
          return None
        raw_ts = _normalize_sql_scalar_expr(gm.group('ts'), table_aliases)
        ts_col = _unquote_sql_ident(raw_ts)
        iv_parts = expressions._split_top_level_commas(gm.group('ivs'))
        w_fn = gm.group('fn').upper()
        if w_fn == 'TUMBLE' and len(iv_parts) == 1:
          window_cfg = {
              'type': 'fixed',
              'size': _format_seconds_duration(_parse_sql_interval_literal(iv_parts[0])),
          }
        elif w_fn == 'HOP' and len(iv_parts) == 2:
          window_cfg = {
              'type': 'sliding',
              'period': _format_seconds_duration(_parse_sql_interval_literal(iv_parts[0])),
              'size': _format_seconds_duration(_parse_sql_interval_literal(iv_parts[1])),
          }
        elif w_fn == 'SESSION' and len(iv_parts) == 1:
          window_cfg = {
              'type': 'sessions',
              'gap': _format_seconds_duration(_parse_sql_interval_literal(iv_parts[0])),
          }
        else:
          _bail(f'invalid {w_fn} window arguments in GROUP BY')
          return None
      else:
        raw_group_exprs.append(_normalize_sql_scalar_expr(g_part, table_aliases))

  if ts_col is not None:
    ts_cfg: dict[str, Any] = {'timestamp_col': ts_col}
    if wire_yaml_dlq:
      ts_cfg['error_handling'] = {'output': 'errors'}
    transforms.append({
        'name': 'AssignSqlTimestamps',
        'type': 'CliAssignTimestamps',
        'input': current_ref,
        'config': ts_cfg,
    })
    if wire_yaml_dlq:
      transforms.append(
          dlq.build_dlq_normalizer_transform(
              'NormalizeSqlTsDlq', 'AssignSqlTimestamps.errors', 'CliAssignTimestamps'))
      dlq_refs.append('NormalizeSqlTsDlq')
      current_ref = 'AssignSqlTimestamps.good'
    else:
      current_ref = 'AssignSqlTimestamps'

  if 'WHERE' in clause_bodies:
    norm_where = _normalize_sql_scalar_expr(clause_bodies['WHERE'], table_aliases)
    try:
      where_cfg = expressions.compile_where(
          norm_where, known_columns=None, allow_jinja=allow_jinja, flag_name='--sql WHERE')[0]
    except expressions.ExpressionError as e:
      if strict:
        raise SqlPipeError(f'unsupported WHERE expression in --sql-engine=python: {e}') from None
      return None
    if wire_yaml_dlq:
      where_cfg = dict(where_cfg)
      where_cfg['error_handling'] = {'output': 'errors'}
    transforms.append({
        'name': 'FilterSqlWhere',
        'type': 'Filter',
        'input': current_ref,
        'config': where_cfg,
    })
    if wire_yaml_dlq:
      transforms.append(
          dlq.build_dlq_normalizer_transform(
              'NormalizeSqlWhereDlq', 'FilterSqlWhere.errors', 'Filter'))
      dlq_refs.append('NormalizeSqlWhereDlq')
    current_ref = 'FilterSqlWhere'

  if window_cfg is not None:
    transforms.append({
        'name': 'SqlWindowInto',
        'type': 'WindowInto',
        'input': current_ref,
        'config': {'windowing': window_cfg},
    })
    current_ref = 'SqlWindowInto'

  agg_head_re = re.compile(
      r'^(?P<fn>COUNT|SUM|AVG|MIN|MAX|STDDEV_POP|STDDEV|VAR_POP)\s*\(',
      re.I)

  def _match_whole_agg_call(expr_text: str) -> tuple[str, str] | None:
    s_ex = expr_text.strip()
    mh = agg_head_re.match(s_ex)
    if not mh or not s_ex.endswith(')'):
      return None
    open_pos = mh.end() - 1
    p_depth = 0
    in_q: str | None = None
    for idx_c in range(open_pos, len(s_ex)):
      ch = s_ex[idx_c]
      if in_q is not None:
        if ch == in_q:
          in_q = None
        continue
      if ch in ("'", '"', '`'):
        in_q = ch
      elif ch == '(':
        p_depth += 1
      elif ch == ')':
        p_depth -= 1
        if p_depth == 0:
          if idx_c != len(s_ex) - 1:
            return None
          return mh.group('fn'), s_ex[open_pos + 1:-1].strip()
    return None

  inl_agg_re = re.compile(
      r'\b(?P<fn>COUNT|SUM|AVG|MIN|MAX|STDDEV_POP|STDDEV|VAR_POP)\s*\(\s*'
      r'(?P<arg>\*|(?:[^()]|\([^()]*\))+?)\s*\)',
      re.I)
  bare_alias_re = re.compile(
      r'^(?P<expr>.+?)\s+(?P<alias>(?!(?:AS|AND|OR|NOT|END|NULL|TRUE|FALSE)\b)[A-Za-z_][A-Za-z0-9_]*)\s*$',
      re.I | re.S)
  as_alias_re = re.compile(
      r'^(?P<expr>.+?)\s+AS\s+(?P<alias>[A-Za-z_][A-Za-z0-9_]*|`[^`]+`|"[^"]+")\s*$',
      re.I | re.S)

  norm_select_str = _normalize_sql_scalar_expr(select_raw, table_aliases)
  if window_cfg is not None:
    norm_select_str = _rewrite_sql_window_bound_calls(norm_select_str)

  select_items_raw = expressions._split_top_level_commas(norm_select_str)
  parsed_select_items: list[tuple[str, str | None]] = []
  has_agg_in_select = False
  needs_window_bounds = False
  select_alias_to_expr: dict[str, str] = {}
  for s_item in select_items_raw:
    st = s_item.strip()
    if st == '*' or re.match(r'^\*\s+EXCEPT\s*\(', st, re.I):
      parsed_select_items.append((st, None))
      continue
    ma = as_alias_re.match(st)
    if ma:
      e_part = ma.group('expr').strip()
      a_part = _unquote_sql_ident(ma.group('alias'))
    else:
      mb = bare_alias_re.match(st)
      if mb and not mb.group('expr').strip().upper().endswith(('IS', 'NOT')):
        e_part = mb.group('expr').strip()
        a_part = _unquote_sql_ident(mb.group('alias'))
      else:
        e_part = st
        a_part = None
    if _match_whole_agg_call(e_part) is not None or inl_agg_re.search(e_part):
      has_agg_in_select = True
    if window_cfg is not None and re.search(r'\b(?:window_start|window_end)\b', e_part):
      needs_window_bounds = True
    if a_part:
      select_alias_to_expr[a_part.lower()] = e_part
    parsed_select_items.append((e_part, a_part))

  # Resolve GROUP BY ordinals (`GROUP BY 1, 2`), SELECT aliases, and window bounds.
  group_exprs: list[str] = []
  for g_raw_item in raw_group_exprs:
    g_item = (
        _rewrite_sql_window_bound_calls(g_raw_item)
        if window_cfg is not None else g_raw_item).strip()
    if g_item.isdigit():
      ord_idx = int(g_item) - 1
      if ord_idx < 0 or ord_idx >= len(parsed_select_items):
        _bail(f'GROUP BY ordinal {g_item} out of range (1..{len(parsed_select_items)})')
        return None
      g_item = parsed_select_items[ord_idx][0]
    elif g_item.lower() in select_alias_to_expr:
      cand_expr = select_alias_to_expr[g_item.lower()]
      if not (_match_whole_agg_call(cand_expr) is not None or
              inl_agg_re.search(cand_expr)):
        g_item = cand_expr
    if window_cfg is not None and g_item in ('window_start', 'window_end'):
      needs_window_bounds = True
      continue
    if g_item not in group_exprs:
      group_exprs.append(g_item)

  is_grouped_query = bool(
      group_raw is not None or has_agg_in_select or 'HAVING' in clause_bodies)

  if is_grouped_query:
    pre_proj_exprs: list[str] = []
    group_cols: list[str] = []
    group_expr_to_col: dict[str, str] = {}
    for idx_g, g_ex in enumerate(group_exprs):
      plain_g = expressions._unwrap_plain_col(g_ex)
      if plain_g is not None:
        group_cols.append(plain_g)
        group_expr_to_col[g_ex] = plain_g
      else:
        syn_g = f'sql_grp_{idx_g}'
        pre_proj_exprs.append(f'{g_ex} AS {syn_g}')
        group_cols.append(syn_g)
        group_expr_to_col[g_ex] = syn_g

    combines: dict[str, Any] = {}
    agg_sig_to_alias: dict[str, str] = {}
    post_proj_items: list[str] = []
    need_post_proj = bool(needs_window_bounds)
    syn_agg_counter = 0

    def _register_agg(
        fn_raw: str, arg_raw: str, preferred_alias: str | None = None
    ) -> str | None:
      nonlocal syn_agg_counter
      fn_low = fn_raw.strip().lower()
      arg_s = arg_raw.strip()
      is_dist_arg = False
      if re.match(r'^DISTINCT\b', arg_s, re.I):
        if fn_low != 'count':
          _bail(f'{fn_low.upper()}(DISTINCT ...) requires Calcite or local SQL engine')
          return None
        is_dist_arg = True
        arg_s = re.sub(r'^DISTINCT\s+', '', arg_s, count=1, flags=re.I).strip()
        if not arg_s or arg_s == '*' or expressions._split_top_level_commas(arg_s) != [arg_s]:
          _bail('COUNT(DISTINCT ...) expects a single expression in --sql-engine=python')
          return None

      sig = (
          f'count(distinct {arg_s})'.lower()
          if is_dist_arg else f'{fn_low}({arg_s})'.lower())
      if sig in agg_sig_to_alias:
        return agg_sig_to_alias[sig]

      if preferred_alias and output.SAFE_IDENTIFIER_RE.match(
          preferred_alias) and preferred_alias not in combines and preferred_alias not in group_cols:
        dest_col = preferred_alias
      else:
        dest_col = f'sql_agg_{syn_agg_counter}'
        syn_agg_counter += 1

      if is_dist_arg:
        p_arg = expressions._unwrap_plain_col(arg_s)
        if p_arg is None:
          p_arg = f'sql_agg_in_{syn_agg_counter}'
          syn_agg_counter += 1
          pre_proj_exprs.append(f'{arg_s} AS {p_arg}')
        combines[dest_col] = {
            'value': p_arg,
            'fn': {
                'type': 'apache_beam.cli.windowing.CountDistinctCombineFn',
                'config': {},
            },
            '_needs_python': True,
        }
      elif fn_low == 'count':
        if arg_s == '*':
          c_src = 'lambda row: 1'
        else:
          p_arg = expressions._unwrap_plain_col(arg_s)
          if p_arg is None:
            p_arg = f'sql_agg_in_{syn_agg_counter}'
            syn_agg_counter += 1
            pre_proj_exprs.append(f'{arg_s} AS {p_arg}')
          c_src = f'lambda row: 0 if getattr(row, {p_arg!r}, None) is None else 1'
        combines[dest_col] = {
            'value': {'callable': c_src, 'output_type': 'integer'},
            'fn': 'sum',
            '_needs_python': True,
        }
      elif fn_low in ('stddev_pop', 'stddev', 'var_pop'):
        if arg_s == '*':
          _bail(f'{fn_low.upper()}(*) is invalid')
          return None
        p_arg = expressions._unwrap_plain_col(arg_s)
        if p_arg is None:
          p_arg = f'sql_agg_in_{syn_agg_counter}'
          syn_agg_counter += 1
          pre_proj_exprs.append(f'{arg_s} AS {p_arg}')
        comb_fqn = (
            'apache_beam.cli.windowing.VarPopCombineFn'
            if fn_low == 'var_pop' else
            'apache_beam.cli.windowing.StddevPopCombineFn')
        combines[dest_col] = {
            'value': p_arg,
            'fn': {'type': comb_fqn, 'config': {}},
            '_needs_python': True,
        }
      else:
        if arg_s == '*':
          _bail(f'{fn_low.upper()}(*) is invalid')
          return None
        fn_clean = 'mean' if fn_low == 'avg' else fn_low
        p_arg = expressions._unwrap_plain_col(arg_s)
        if p_arg is None:
          p_arg = f'sql_agg_in_{syn_agg_counter}'
          syn_agg_counter += 1
          pre_proj_exprs.append(f'{arg_s} AS {p_arg}')
        combines[dest_col] = {'value': p_arg, 'fn': fn_clean}

      agg_sig_to_alias[sig] = dest_col
      return dest_col

    for idx_s, (e_part, a_part) in enumerate(parsed_select_items):
      whole_agg = _match_whole_agg_call(e_part)
      if whole_agg is not None:
        fn_low = whole_agg[0].lower()
        arg_s = whole_agg[1].strip()
        default_alias = (
            expressions._unwrap_plain_col(arg_s) or
            (fn_low if fn_low != 'mean' else 'avg'))
        out_alias = a_part or default_alias or f'{fn_low}_{idx_s}'
        if not output.SAFE_IDENTIFIER_RE.match(out_alias):
          out_alias = f'{fn_low}_{idx_s}'
        reg_col = _register_agg(fn_low, arg_s, preferred_alias=out_alias)
        if reg_col is None:
          return None
        if reg_col != out_alias:
          post_proj_items.append(f'{reg_col} AS {out_alias}')
          need_post_proj = True
        else:
          post_proj_items.append(out_alias)
      elif inl_agg_re.search(e_part):
        if not a_part:
          _bail(f'compound aggregate SELECT expression {e_part!r} requires AS <alias>')
          return None
        agg_failed = False

        def _sub_sel_agg(sm: re.Match[str]) -> str:
          nonlocal agg_failed
          rc = _register_agg(sm.group('fn'), sm.group('arg'), preferred_alias=None)
          if rc is None:
            agg_failed = True
            return sm.group(0)
          return rc

        rewritten_sel = ''.join(
            inl_agg_re.sub(_sub_sel_agg, seg) if is_c else seg
            for is_c, seg in _scan_sql_outside_literals(e_part))
        if agg_failed:
          return None
        for g_orig, g_syn in group_expr_to_col.items():
          if g_orig != g_syn:
            rewritten_sel = rewritten_sel.replace(g_orig, g_syn)
        post_proj_items.append(f'{rewritten_sel} AS {a_part}')
        need_post_proj = True
      else:
        mapped_g = group_expr_to_col.get(e_part)
        plain_e = mapped_g or expressions._unwrap_plain_col(e_part)
        if window_cfg is not None and plain_e in ('window_start', 'window_end'):
          needs_window_bounds = True
          need_post_proj = True
          if a_part and a_part != plain_e:
            post_proj_items.append(f'{plain_e} AS {a_part}')
          else:
            post_proj_items.append(plain_e)
        elif plain_e is not None and plain_e in group_cols:
          if a_part and a_part != plain_e:
            post_proj_items.append(f'{plain_e} AS {a_part}')
            need_post_proj = True
          else:
            post_proj_items.append(plain_e)
        else:
          _bail(f'non-aggregated SELECT expression {e_part!r} must appear in GROUP BY')
          return None

    having_norm: str | None = None
    if 'HAVING' in clause_bodies:
      raw_hav = _normalize_sql_scalar_expr(clause_bodies['HAVING'], table_aliases)
      if window_cfg is not None:
        raw_hav = _rewrite_sql_window_bound_calls(raw_hav)
        if re.search(r'\b(?:window_start|window_end)\b', raw_hav):
          needs_window_bounds = True
          need_post_proj = True
      hav_failed = False

      def _sub_hav_agg(hm: re.Match[str]) -> str:
        nonlocal need_post_proj, hav_failed
        sig_probe = (
            f'count(distinct {re.sub(r"^DISTINCT\\s+", "", hm.group("arg").strip(), count=1, flags=re.I).strip()})'.lower()
            if re.match(r'^DISTINCT\b', hm.group('arg').strip(), re.I) else
            f'{hm.group("fn").lower()}({hm.group("arg").strip()})'.lower())
        if sig_probe not in agg_sig_to_alias:
          need_post_proj = True
        rc = _register_agg(hm.group('fn'), hm.group('arg'), preferred_alias=None)
        if rc is None:
          hav_failed = True
          return hm.group(0)
        return rc

      having_norm = ''.join(
          inl_agg_re.sub(_sub_hav_agg, seg) if is_c else seg
          for is_c, seg in _scan_sql_outside_literals(raw_hav))
      if hav_failed:
        return None

    if not combines and group_cols and window_cfg is None:
      # `SELECT region FROM s GROUP BY region` -> deduplicate by group_cols!
      if pre_proj_exprs:
        try:
          pre_cfg = expressions.compile_select(
              '*, ' + ', '.join(pre_proj_exprs),
              known_columns=None,
              allow_jinja=allow_jinja,
              flag_name='--sql GROUP BY')[0]
        except expressions.ExpressionError as e:
          if strict:
            raise SqlPipeError(str(e)) from None
          return None
        transforms.append({
            'name': 'PreGroupProject',
            'type': 'MapToFields',
            'input': current_ref,
            'config': pre_cfg,
        })
        current_ref = 'PreGroupProject'
      transforms.append({
          'name': 'GroupDistinct',
          'type': 'CliDeduplicate',
          'input': current_ref,
          'config': {'keys': group_cols, 'keep': 'first'},
      })
      current_ref = 'GroupDistinct'
      need_post_proj = True
    else:
      if not combines:
        # Windowed GROUP BY without explicit agg (`SELECT window_start, region ... GROUP BY window_start, region`)
        combines['sql_win_cnt'] = {
            'value': {'callable': 'lambda row: 1', 'output_type': 'integer'},
            'fn': 'sum',
            '_needs_python': True,
        }
        need_post_proj = True
      if pre_proj_exprs:
        try:
          pre_cfg = expressions.compile_select(
              '*, ' + ', '.join(pre_proj_exprs),
              known_columns=None,
              allow_jinja=allow_jinja,
              flag_name='--sql')[0]
        except expressions.ExpressionError as e:
          if strict:
            raise SqlPipeError(str(e)) from None
          return None
        transforms.append({
            'name': 'PreGroupProject',
            'type': 'MapToFields',
            'input': current_ref,
            'config': pre_cfg,
        })
        current_ref = 'PreGroupProject'

      comb_cfg = pipe_spec._build_combine_config(
          {'group_by': group_cols, 'combine': combines})
      transforms.append({
          'name': 'SqlCombine',
          'type': 'Combine',
          'input': current_ref,
          'config': comb_cfg,
      })
      current_ref = 'SqlCombine'

    if needs_window_bounds:
      transforms.append({
          'name': 'AttachSqlWindowBounds',
          'type': 'CliExtractPaneInfo',
          'input': current_ref,
      })
      current_ref = 'AttachSqlWindowBounds'

    if having_norm is not None:
      try:
        hav_cfg = expressions.compile_where(
            having_norm,
            known_columns=None,
            allow_jinja=allow_jinja,
            flag_name='--sql HAVING')[0]
      except expressions.ExpressionError as e:
        if strict:
          raise SqlPipeError(f'unsupported HAVING expression: {e}') from None
        return None
      transforms.append({
          'name': 'FilterSqlHaving',
          'type': 'Filter',
          'input': current_ref,
          'config': hav_cfg,
      })
      current_ref = 'FilterSqlHaving'

    if need_post_proj and post_proj_items:
      try:
        post_cfg = expressions.compile_select(
            ', '.join(post_proj_items),
            known_columns=None,
            allow_jinja=allow_jinja,
            flag_name='--sql SELECT')[0]
      except expressions.ExpressionError as e:
        if strict:
          raise SqlPipeError(str(e)) from None
        return None
      transforms.append({
          'name': 'PostGroupSelect',
          'type': 'MapToFields',
          'input': current_ref,
          'config': post_cfg,
      })
      current_ref = 'PostGroupSelect'
  else:
    # Non-aggregated SELECT projection
    if needs_window_bounds:
      transforms.append({
          'name': 'AttachSqlWindowBounds',
          'type': 'CliExtractPaneInfo',
          'input': current_ref,
      })
      current_ref = 'AttachSqlWindowBounds'

    norm_sel_parts: list[str] = []
    distinct_keys: list[str] = []
    for e_part, a_part in parsed_select_items:
      if e_part == '*' or re.match(r'^\*\s+EXCEPT\s*\(', e_part, re.I):
        norm_sel_parts.append(e_part)
      elif a_part:
        norm_sel_parts.append(f'{e_part} AS {a_part}')
        distinct_keys.append(a_part)
      else:
        plain_c = expressions._unwrap_plain_col(e_part)
        if plain_c is None:
          _bail(f'computed SELECT expression {e_part!r} requires AS <alias>')
          return None
        norm_sel_parts.append(e_part)
        distinct_keys.append(plain_c)

    if not (len(norm_sel_parts) == 1 and norm_sel_parts[0] == '*'):
      try:
        sel_cfg = expressions.compile_select(
            ', '.join(norm_sel_parts),
            known_columns=None,
            allow_jinja=allow_jinja,
            flag_name='--sql SELECT')[0]
      except expressions.ExpressionError as e:
        if strict:
          raise SqlPipeError(f'unsupported SELECT expression in --sql-engine=python: {e}') from None
        return None
      if wire_yaml_dlq:
        sel_cfg = dict(sel_cfg)
        sel_cfg['error_handling'] = {'output': 'errors'}
      transforms.append({
          'name': 'SelectSqlFields',
          'type': 'MapToFields',
          'input': current_ref,
          'config': sel_cfg,
      })
      if wire_yaml_dlq:
        transforms.append(
            dlq.build_dlq_normalizer_transform(
                'NormalizeSqlSelectDlq', 'SelectSqlFields.errors', 'MapToFields'))
        dlq_refs.append('NormalizeSqlSelectDlq')
      current_ref = 'SelectSqlFields'

    if is_distinct:
      if not distinct_keys or any(
          p == '*' or re.match(r'^\*\s+EXCEPT\s*\(', p, re.I) for p in norm_sel_parts):
        _bail('SELECT DISTINCT with * requires explicit column names in --sql-engine=python')
        return None
      transforms.append({
          'name': 'SelectSqlDistinct',
          'type': 'CliDeduplicate',
          'input': current_ref,
          'config': {'keys': distinct_keys, 'keep': 'first'},
      })
      current_ref = 'SelectSqlDistinct'

  if limit_val is not None:
    transforms.append({
        'name': 'LimitSqlRows',
        'type': 'CliLimitRows',
        'input': current_ref,
        'config': {'limit': limit_val},
    })
    current_ref = 'LimitSqlRows'

  if sink_ep is not None:
    sink_cfg = dict(sink_ep.config)
    if wire_yaml_dlq and sink_ep.yaml_type in dlq.TRANSFORMS_SUPPORTING_ERROR_HANDLING:
      sink_cfg['error_handling'] = {'output': 'errors'}
    transforms.append({
        'name': 'WriteSink',
        'type': sink_ep.yaml_type,
        'input': current_ref,
        'config': sink_cfg,
    })
    if 'error_handling' in sink_cfg:
      transforms.append(
          dlq.build_dlq_normalizer_transform(
              'NormalizeSinkDlq', 'WriteSink.errors', sink_ep.yaml_type))
      dlq_refs.append('NormalizeSinkDlq')

  if dlq_ep is not None and dlq_refs:
    if len(dlq_refs) == 1:
      dlq_in = dlq_refs[0]
    else:
      transforms.append({
          'name': 'MergeDlq',
          'type': 'Flatten',
          'input': dlq_refs,
      })
      dlq_in = 'MergeDlq'
    transforms.append({
        'name': 'WriteDlq',
        'type': dlq_ep.yaml_type,
        'input': dlq_in,
        'config': dict(dlq_ep.config),
    })

  needs_cli_provider = bool(
      dlq_refs or any(
          str(t.get('type', '')).startswith(('Cli', 'ReadCsvWithDlq', 'ReadJsonWithDlq'))
          for t in transforms))
  spec_out: dict[str, Any] = {'pipeline': {'transforms': transforms}}
  if needs_cli_provider:
    spec_out['providers'] = [dlq.cli_yaml_provider_spec()]
  if has_unbounded:
    spec_out['options'] = {'streaming': True}
  return spec_out


def _parse_redact_fields(
    dlq_redact_fields: Sequence[str] | str | None,
) -> tuple[str, ...]:
  if not dlq_redact_fields:
    return ()
  items = (
      [dlq_redact_fields]
      if isinstance(dlq_redact_fields, str) else list(dlq_redact_fields))
  out: list[str] = []
  seen: set[str] = set()
  for item in items:
    for part in str(item or '').split(','):
      col = part.strip()
      if col and col not in seen:
        seen.add(col)
        out.append(col)
  return tuple(out)


def _normalize_yaml_file_read_cfg(
    s_type: str, cfg: dict[str, Any]) -> dict[str, Any]:
  out = dict(cfg)
  if s_type == 'ReadFromJson':
    out.pop('sample_rows', None)
    out.pop('on_unknown_fields', None)
  if s_type == 'ReadFromCsv' and 'null_values' in out:
    out['na_values'] = [''] + list(out.pop('null_values') or [])
    out['keep_default_na'] = False
  if s_type in ('ReadFromCsv', 'ReadFromJson') and 'types' in out:
    dtype_map = {
        'int': 'Int64',
        'float': 'float64',
        'bool': 'boolean',
        'str': 'string',
    }
    out['dtype'] = {
        c: dtype_map.get(t, 'string')
        for c, t in dict(out.pop('types') or {}).items()
    }
  return out


def _apply_dlq_redaction_to_spec(
    spec_out: dict[str, Any], redact_fields: tuple[str, ...]
) -> dict[str, Any]:
  if not redact_fields:
    return spec_out
  transforms = spec_out.get('pipeline', {}).get('transforms', [])
  step_ret_re = re.compile(r'return\s+([\'"][^\'"]*[\'"])')
  for idx, t in enumerate(transforms):
    if (t.get('type') == 'MapToFields' and
        str(t.get('name', '')).startswith('Normalize')):
      fields_cfg = (t.get('config') or {}).get('fields') or {}
      if set(fields_cfg.keys()) >= {'step', 'error_type', 'error', 'raw_payload'}:
        step_src = str((fields_cfg.get('step') or {}).get('callable') or '')
        m = step_ret_re.search(step_src)
        step_label = ast_literal_str(m.group(1)) if m else str(t.get('name'))
        transforms[idx] = dlq.build_dlq_normalizer_transform(
            str(t['name']),
            str(t['input']),
            step_label,
            redact_fields=redact_fields)
  return spec_out


def ast_literal_str(raw_lit: str) -> str:
  import ast
  try:
    val = ast.literal_eval(raw_lit)
    return str(val)
  except Exception:  # pylint: disable=broad-except
    return raw_lit.strip('\'"')


def build_sql_yaml_spec(
    named_sources: list[NamedSource],
    sql_query: str,
    sink_ep: uris.ParsedEndpoint | None,
    *,
    allow_jinja: bool = False,
    max_watermark_skew: str | None = None,
    skew_action: str | None = None,
    dlq_ep: uris.ParsedEndpoint | None = None,
    dlq_redact_fields: Sequence[str] | str | None = None,
    for_local_only: bool = False,
    prefer_python_lowering: bool = False,
    strict_python_lowering: bool = False,
    pre_where: str | None = None,
    post_select: str | None = None,
) -> dict[str, Any]:
  """Builds portable Beam YAML spec (`type: Sql` or lowered pure-Python transforms)."""
  redact_tuple = _parse_redact_fields(dlq_redact_fields)
  if redact_tuple and dlq_ep is None:
    raise SqlPipeError('--dlq-redact-fields requires --dlq <SINK>')
  clean_q = validate_readonly_select_sql(sql_query, allow_jinja=allow_jinja)
  has_unbounded = any(ns.endpoint.is_unbounded for ns in named_sources)
  transforms: list[dict[str, Any]] = []
  sql_inputs: dict[str, str] = {}
  sql_dlq_norm_refs: list[str] = []
  wire_yaml_dlq = bool(dlq_ep is not None and not for_local_only)
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
        sub_cfg = _normalize_yaml_file_read_cfg(sub_yaml_type, sub_cfg)
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
      sql_inputs[ns.alias] = read_name
    else:
      cfg = {
          k: v for k, v in ns.endpoint.config.items()
          if not k.startswith('_')
      }
      s_type = ns.endpoint.yaml_type
      counts_int = False
      if wire_yaml_dlq:
        if ns.endpoint.kind == 'file' and ns.endpoint.format == 'csv':
          s_type = 'ReadCsvWithDlq'
          cfg = {
              k: v
              for k, v in cfg.items()
              if k in ('path', 'delimiter', 'null_values', 'types')
          }
          cfg['error_handling'] = {'output': 'errors'}
          counts_int = True
        elif ns.endpoint.kind == 'file' and ns.endpoint.format == 'json':
          s_type = 'ReadJsonWithDlq'
          cfg = {
              k: v
              for k, v in cfg.items()
              if k in ('path', 'types', 'sample_rows', 'on_unknown_fields')
          }
          cfg['error_handling'] = {'output': 'errors'}
          counts_int = True
        elif s_type in dlq.TRANSFORMS_SUPPORTING_ERROR_HANDLING:
          cfg = _normalize_yaml_file_read_cfg(s_type, cfg)
          cfg['error_handling'] = {'output': 'errors'}
        else:
          cfg = _normalize_yaml_file_read_cfg(s_type, cfg)
      else:
        cfg = _normalize_yaml_file_read_cfg(s_type, cfg)
      transforms.append({
          'name': read_name,
          'type': s_type,
          'config': cfg,
      })
      if 'error_handling' in cfg:
        norm_read_dlq = f'NormalizeSourceDlq_{ns.alias}'
        transforms.append(
            dlq.build_dlq_normalizer_transform(
                norm_read_dlq,
                f'{read_name}.errors',
                s_type,
                redact_fields=redact_tuple))
        sql_dlq_norm_refs.append(norm_read_dlq)
        sql_inputs[ns.alias] = f'{read_name}.good' if counts_int else read_name
      else:
        sql_inputs[ns.alias] = read_name

  from . import expressions
  if pre_where:
    if len(named_sources) != 1:
      raise SqlPipeError(
          '--where with --sql is only supported on a single input source; '
          'for multi-input SQL joins, move the filter into --sql WHERE ...')
    try:
      pre_w_cfg = dict(
          expressions.compile_where(
              pre_where, known_columns=None, allow_jinja=allow_jinja)[0])
    except expressions.ExpressionError as e:
      raise SqlPipeError(str(e)) from None
    only_a = named_sources[0].alias
    if wire_yaml_dlq:
      pre_w_cfg['error_handling'] = {'output': 'errors'}
    transforms.append({
        'name': 'PreSqlWhere',
        'type': 'Filter',
        'input': sql_inputs[only_a],
        'config': pre_w_cfg,
    })
    if wire_yaml_dlq:
      transforms.append(
          dlq.build_dlq_normalizer_transform(
              'NormalizePreSqlWhereDlq',
              'PreSqlWhere.errors',
              'Filter',
              redact_fields=redact_tuple))
      sql_dlq_norm_refs.append('NormalizePreSqlWhereDlq')
    sql_inputs[only_a] = 'PreSqlWhere'

  post_s_cfg: dict[str, Any] | None = None
  if post_select:
    try:
      post_s_cfg = dict(
          expressions.compile_select(
              post_select, known_columns=None, allow_jinja=allow_jinja)[0])
    except expressions.ExpressionError as e:
      raise SqlPipeError(str(e)) from None

  def _attach_post_select(spec_d: dict[str, Any]) -> dict[str, Any]:
    if post_s_cfg is None:
      return spec_d
    t_list = spec_d.get('pipeline', {}).get('transforms')
    if not isinstance(t_list, list) or 'local_asof_sql' in spec_d.get(
        'pipeline', {}):
      return spec_d
    cfg_ps = dict(post_s_cfg)
    sink_idx = next(
        (i for i, t in enumerate(t_list) if t.get('name') == 'WriteSink'), None)
    if sink_idx is not None:
      prev_in = t_list[sink_idx]['input']
      if wire_yaml_dlq:
        cfg_ps['error_handling'] = {'output': 'errors'}
      ps_step = {
          'name': 'PostSqlSelect',
          'type': 'MapToFields',
          'input': prev_in,
          'config': cfg_ps,
      }
      t_list.insert(sink_idx, ps_step)
      t_list[sink_idx + 1]['input'] = 'PostSqlSelect'
      if wire_yaml_dlq:
        norm_ps = dlq.build_dlq_normalizer_transform(
            'NormalizePostSqlSelectDlq',
            'PostSqlSelect.errors',
            'MapToFields',
            redact_fields=redact_tuple)
        t_list.insert(sink_idx + 1, norm_ps)
        _merge_extra_sql_dlq_refs(spec_d, ['NormalizePostSqlSelectDlq'], dlq_ep)
    elif t_list:
      last_name = t_list[-1].get('name')
      if last_name:
        t_list.append({
            'name': 'PostSqlSelect',
            'type': 'MapToFields',
            'input': last_name,
            'config': cfg_ps,
        })
    return spec_d

  code_only = ' '.join(
      seg for is_c, seg in _scan_sql_outside_literals(clean_q) if is_c)
  has_asof = bool(re.search(r'\bASOF\b', code_only, re.I))
  has_join = bool(re.search(r'\bJOIN\b', code_only, re.I))
  has_interval = bool(re.search(r'\bINTERVAL\b', code_only, re.I))
  if (max_watermark_skew is not None or
      skew_action is not None) and not has_join:
    raise SqlPipeError(
        '--max-watermark-skew / --skew-action with --sql requires a 2-source '
        'temporal JOIN (or use --interval-join / --asof-join without --sql)')
  if has_asof:
    if for_local_only:
      _parse_asof_sql_join(clean_q, named_sources, allow_post_clauses=True)
      return {
          'pipeline': {
              'transforms': transforms,
              'local_asof_sql': clean_q,
          }
      }
    res_asof = _lower_asof_join_sql(
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
    if sql_dlq_norm_refs and dlq_ep is not None:
      _merge_extra_sql_dlq_refs(res_asof, sql_dlq_norm_refs, dlq_ep)
    return _apply_dlq_redaction_to_spec(
        _attach_post_select(res_asof), redact_tuple)
  if has_join and (has_interval or max_watermark_skew is not None or
                   skew_action is not None or
                   (has_unbounded and len(named_sources) >= 2)):
    res_ij = _lower_temporal_interval_join_sql(
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
    if sql_dlq_norm_refs and dlq_ep is not None:
      _merge_extra_sql_dlq_refs(res_ij, sql_dlq_norm_refs, dlq_ep)
    return _apply_dlq_redaction_to_spec(
        _attach_post_select(res_ij), redact_tuple)

  if prefer_python_lowering or strict_python_lowering:
    if len(named_sources) != 1 or has_join:
      if strict_python_lowering:
        raise SqlPipeError(
            '--sql-engine=python supports single-source SQL or 2-source ASOF / '
            'temporal INTERVAL JOINs; omit --sql-engine=python to use Calcite '
            'or local DuckDB/SQLite for general multi-table joins')
    else:
      only_ns = named_sources[0]
      py_spec = _try_lower_single_source_sql(
          only_ns,
          clean_q,
          transforms,
          sql_inputs[only_ns.alias],
          sink_ep,
          has_unbounded=has_unbounded,
          wire_yaml_dlq=wire_yaml_dlq,
          dlq_ep=dlq_ep,
          sql_dlq_norm_refs=sql_dlq_norm_refs,
          allow_jinja=allow_jinja,
          strict=strict_python_lowering)
      if py_spec is not None:
        return _apply_dlq_redaction_to_spec(
            _attach_post_select(py_spec), redact_tuple)

  _SQL_RESERVED_TAGS = frozenset({
      'in', 'select', 'from', 'where', 'group', 'order', 'by', 'join', 'on',
      'as', 'table', 'user', 'values', 'union', 'limit', 'having', 'all', 'and',
      'or', 'not', 'null', 'is', 'true', 'false', 'case', 'when', 'then', 'end',
  })
  eff_sql_inputs = dict(sql_inputs)
  calcite_query = clean_q
  use_single_pcollection = False
  if len(eff_sql_inputs) == 1:
    only_alias, only_read = next(iter(eff_sql_inputs.items()))
    refs_source = bool(re.search(r'\bFROM\s+source\b', code_only, re.I))
    refs_alias = bool(
        re.search(rf'\bFROM\s+[`"]?{re.escape(only_alias)}[`"]?\b', code_only, re.I))
    if refs_source and not refs_alias and only_alias.lower() != 'source':
      rw_parts: list[str] = []
      for is_c, seg in _scan_sql_outside_literals(calcite_query):
        if is_c:
          s_rw = re.sub(r'\bFROM\s+source\b', 'FROM PCOLLECTION', seg, flags=re.I)
          s_rw = re.sub(r'\bsource\s*\.', 'PCOLLECTION.', s_rw, flags=re.I)
          rw_parts.append(s_rw)
        else:
          rw_parts.append(seg)
      calcite_query = ''.join(rw_parts)
      use_single_pcollection = True
    elif refs_source and 'source' not in eff_sql_inputs:
      eff_sql_inputs['source'] = only_read
    if (not use_single_pcollection and only_alias.lower() in _SQL_RESERVED_TAGS
        and only_alias in eff_sql_inputs):
      safe_alias = f'tbl_{only_alias}'
      if not refs_alias:
        del eff_sql_inputs[only_alias]
        if not eff_sql_inputs:
          eff_sql_inputs[safe_alias] = only_read

  sql_input_val: Any = (
      next(iter(eff_sql_inputs.values()))
      if len(eff_sql_inputs) == 1 and (
          use_single_pcollection or 'pcollection' in code_only.lower()) else
      eff_sql_inputs)
  transforms.append({
      'name': 'SqlQuery',
      'type': 'Sql',
      'input': sql_input_val,
      'config': {
          'query': calcite_query
      },
  })
  sql_out_ref = 'SqlQuery'
  if post_s_cfg is not None:
    cfg_ps = dict(post_s_cfg)
    if wire_yaml_dlq:
      cfg_ps['error_handling'] = {'output': 'errors'}
    transforms.append({
        'name': 'PostSqlSelect',
        'type': 'MapToFields',
        'input': sql_out_ref,
        'config': cfg_ps,
    })
    if wire_yaml_dlq:
      transforms.append(
          dlq.build_dlq_normalizer_transform(
              'NormalizePostSqlSelectDlq',
              'PostSqlSelect.errors',
              'MapToFields',
              redact_fields=redact_tuple))
      sql_dlq_norm_refs.append('NormalizePostSqlSelectDlq')
    sql_out_ref = 'PostSqlSelect'
  if sink_ep is not None:
    sink_cfg = dict(sink_ep.config)
    if wire_yaml_dlq and sink_ep.yaml_type in dlq.TRANSFORMS_SUPPORTING_ERROR_HANDLING:
      sink_cfg['error_handling'] = {'output': 'errors'}
    transforms.append({
        'name': 'WriteSink',
        'type': sink_ep.yaml_type,
        'input': sql_out_ref,
        'config': sink_cfg,
    })
    if 'error_handling' in sink_cfg:
      transforms.append(
          dlq.build_dlq_normalizer_transform(
              'NormalizeSinkDlq',
              'WriteSink.errors',
              sink_ep.yaml_type,
              redact_fields=redact_tuple))
      sql_dlq_norm_refs.append('NormalizeSinkDlq')
  if dlq_ep is not None and sql_dlq_norm_refs:
    if len(sql_dlq_norm_refs) == 1:
      dlq_in = sql_dlq_norm_refs[0]
    else:
      transforms.append({
          'name': 'MergeDlq',
          'type': 'Flatten',
          'input': sql_dlq_norm_refs,
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
      }
  }
  if sql_dlq_norm_refs:
    spec_out['providers'] = [dlq.cli_yaml_provider_spec()]
  if has_unbounded:
    spec_out['options'] = {'streaming': True}
  return _apply_dlq_redaction_to_spec(spec_out, redact_tuple)


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


def _open_binary(path: str, *, decompress: bool = True):
  low = path.lower()
  if '://' not in path:
    if decompress and low.endswith('.gz'):
      import gzip
      return gzip.open(path, 'rb')
    if decompress and low.endswith('.bz2'):
      import bz2
      return bz2.open(path, 'rb')
    return open(path, 'rb')
  from apache_beam.io.filesystem import CompressionTypes
  from apache_beam.io.filesystems import FileSystems
  comp = CompressionTypes.AUTO if decompress else CompressionTypes.UNCOMPRESSED
  return FileSystems.open(path, compression_type=comp)


def _coerce_csv_val(
    val: str, kind: str, null_values=dlq.DEFAULT_CSV_NULL_VALUES) -> Any:
  if kind == 'str':
    if val == '' or val in null_values or (
        val.strip() and val.strip() in null_values):
      return None
    return val
  s = val.strip()
  if s == '' or val in null_values or s in null_values:
    return None
  if kind == 'int':
    return int(s)
  if kind == 'float':
    v = float(s)
    if math.isnan(v) or math.isinf(v):
      raise ValueError(f'non-finite float token (len={len(s)})')
    return v
  if kind == 'bool':
    low = s.lower()
    if low in ('true', '1', 't', 'yes'):
      return True
    if low in ('false', '0', 'f', 'no'):
      return False
    raise ValueError(f'invalid boolean token (len={len(s)})')
  return val


def _redact_dlq_entry(
    entry: dict[str, Any],
    redact_fields: tuple[str, ...],
    *,
    header: list[str] | None = None,
    delim: str = ',',
) -> dict[str, Any]:
  if not redact_fields:
    return entry
  rk_set = set(redact_fields)
  out = dict(entry)
  err_msg = str(out.get('error', ''))
  for col in redact_fields:
    err_msg = re.sub(
        r'([\'"]?' + re.escape(col) +
        r'[\'"]?\s*[:=]\s*)(?:([\'"]).*?\2|[^,\s)\]}]+)',
        r'\1<REDACTED>',
        err_msg)
  out['error'] = err_msg

  raw_p = out.get('raw_payload')
  if isinstance(raw_p, str):
    scrubbed: str | None = None
    try:
      parsed_p = json.loads(raw_p)
      if isinstance(parsed_p, dict):
        scrubbed = json.dumps(
            dlq.redact_nested_fields(parsed_p, redact_fields),
            default=str,
            allow_nan=False)
      elif isinstance(parsed_p, list) and header:
        masked_list = [
            '<REDACTED>' if idx < len(header) and header[idx] in rk_set else val
            for idx, val in enumerate(parsed_p)
        ]
        scrubbed = json.dumps(masked_list, default=str, allow_nan=False)
      elif isinstance(parsed_p, list):
        scrubbed = json.dumps(
            dlq.redact_nested_fields(parsed_p, redact_fields),
            default=str,
            allow_nan=False)
    except Exception:  # pylint: disable=broad-except
      scrubbed = None
    if scrubbed is None:
      tmp_s = raw_p
      replaced_any = False
      for col in redact_fields:
        nxt_s, cnt = re.subn(
            r'([\'"]' + re.escape(col) +
            r'[\'"]\s*:\s*)(?:"(?:\\.|[^"])*"|\'(?:\\.|[^\'])*\'|[^,\s}\]]+)',
            r'\1"<REDACTED>"',
            tmp_s)
        if cnt:
          replaced_any = True
          tmp_s = nxt_s
      if replaced_any:
        scrubbed = tmp_s
      elif header and any(h in rk_set for h in header):
        parts = raw_p.split(delim)
        for idx, col_n in enumerate(header):
          if col_n in rk_set and idx < len(parts):
            parts[idx] = '<REDACTED>'
        scrubbed = delim.join(parts)
      else:
        scrubbed = tmp_s
    out['raw_payload'] = scrubbed
  return out


def _lakehouse_type_to_arrow(ft: str):
  import pyarrow as pa
  low = ft.strip().lower()
  if low in ('boolean', 'bool'):
    return pa.bool_()
  if low in ('int', 'integer', 'short', 'byte'):
    return pa.int32()
  if low == 'long':
    return pa.int64()
  if low == 'float':
    return pa.float32()
  if low == 'double':
    return pa.float64()
  m_dec = re.match(r'^decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$', low)
  if m_dec:
    p, s = int(m_dec.group(1)), int(m_dec.group(2))
    return pa.decimal128(min(p, 38), s) if p <= 38 else pa.decimal256(p, s)
  if low.startswith('decimal'):
    return pa.decimal128(38, 9)
  if low == 'date':
    return pa.date32()
  if low == 'timestamptz':
    return pa.timestamp('us', tz='UTC')
  if low.startswith('timestamp'):
    return pa.timestamp('us')
  if low in ('binary', 'bytes'):
    return pa.binary()
  return pa.string()


def load_endpoint_rows(
    ep: uris.ParsedEndpoint,
    *,
    default_project: str | None = None,
    dlq_records: list[dict[str, Any]] | None = None,
    dlq_redact_fields: Sequence[str] | str | None = None,
    warnings: list[str] | None = None,
) -> tuple[list[tuple[str, str]], list[dict[str, Any]], int]:
  """Reads `(columns_with_sqlite_types, rows, raw_rows_read)` from `ep`."""
  from . import lakehouse

  dlq.ensure_csv_field_size_limit()
  redact_tuple = _parse_redact_fields(dlq_redact_fields)
  _SQL_THREAD_LOCAL.last_endpoint_arrow_types = {}

  if ep.kind == 'delta':
    table_root = ep.config.get('table') or ep.config.get('path')
    ver = ep.config.get('version')
    snap, rows = lakehouse.read_delta_rows_local(
        table_root,
        version=int(ver) if ver is not None else None,
        limit=None)
    cols = []
    arrow_map = {}
    for f in snap.get('schema', []):
      fname = str(f['name'])
      ft = str(f.get('type', 'string')).lower()
      sq_t = (
          'BOOLEAN' if ft in ('boolean', 'bool') else
          'INTEGER' if ft in ('long', 'integer', 'short', 'byte') else
          'REAL' if ft in ('double', 'float') or ft.startswith('decimal') else
          'TEXT')
      cols.append((fname, sq_t))
      arrow_map[fname] = _lakehouse_type_to_arrow(ft)
    _SQL_THREAD_LOCAL.last_endpoint_arrow_types = arrow_map
    return cols, rows, len(rows)

  if ep.kind == 'iceberg':
    tdir = lakehouse.resolve_iceberg_table_dir(
        ep.config, default_project=default_project)
    state = lakehouse.load_iceberg_state(tdir)
    _, rows = lakehouse.read_iceberg_rows_local(state, limit=None)
    cols = []
    arrow_map = {}
    for f in state.get('schema', []):
      fname = str(f['name'])
      ft = str(f.get('type', 'string')).lower()
      sq_t = (
          'BOOLEAN' if ft in ('boolean', 'bool') else
          'INTEGER' if ft in ('long', 'int') else 'REAL'
          if ft in ('double', 'float') or ft.startswith('decimal') else 'TEXT')
      cols.append((fname, sq_t))
      arrow_map[fname] = _lakehouse_type_to_arrow(ft)
    _SQL_THREAD_LOCAL.last_endpoint_arrow_types = arrow_map
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
          dlq_redact_fields=redact_tuple,
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
      with _open_binary(fp, decompress=False) as bf:
        tables.append(pq.read_table(bf))
    if not tables:
      return [], [], 0
    full = pa.concat_tables(tables, promote_options='default')
    cols = []
    arrow_map = {}
    for field in full.schema:
      t = field.type
      arrow_map[field.name] = t
      if pa.types.is_boolean(t):
        sq_t = 'BOOLEAN'
      elif pa.types.is_integer(t):
        sq_t = 'INTEGER'
      elif pa.types.is_floating(t) or pa.types.is_decimal(t):
        sq_t = 'REAL'
      else:
        sq_t = 'TEXT'
      cols.append((field.name, sq_t))
    _SQL_THREAD_LOCAL.last_endpoint_arrow_types = arrow_map
    rows = full.to_pylist()
    return cols, rows, len(rows)

  if fmt == 'csv':
    import pyarrow as pa
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
            'BOOLEAN' if kinds.get(c) == 'bool' else
            'INTEGER' if kinds.get(c) == 'int' else
            'REAL' if kinds.get(c) == 'float' else 'TEXT',
        ) for c in header
    ]
    _SQL_THREAD_LOCAL.last_endpoint_arrow_types = {
        c: (
            pa.bool_() if kinds.get(c) == 'bool' else
            pa.int64() if kinds.get(c) == 'int' else
            pa.float64() if kinds.get(c) == 'float' else pa.string())
        for c in header
    }
    rows = []
    raw_count = 0
    for fp in files:
      with _open_binary(fp) as bf:
        text_stream = io.TextIOWrapper(
            bf, encoding='utf-8-sig', errors='replace', newline='')
        try:
          first = True
          for rec, csv_err, bad_line in dlq.iter_csv_rows_recovering(
              text_stream, delimiter=delim):
            if csv_err is not None:
              if first:
                first = False
                if dlq_records is not None:
                  dlq_records.append(
                      _redact_dlq_entry(
                          {
                              'step': 'ReadCsvWithDlq',
                              'error_type': type(csv_err).__name__,
                              'error': str(csv_err),
                              'raw_payload': bad_line.rstrip('\r\n'),
                          },
                          redact_tuple,
                          header=header,
                          delim=delim))
                  continue
                raise SqlPipeError(
                    f'malformed CSV header in {fp}: {csv_err}') from None
              raw_count += 1
              if dlq_records is not None:
                dlq_records.append(
                    _redact_dlq_entry(
                        {
                            'step': 'ReadCsvWithDlq',
                            'error_type': type(csv_err).__name__,
                            'error': str(csv_err),
                            'raw_payload': bad_line.rstrip('\r\n'),
                        },
                        redact_tuple,
                        header=header,
                        delim=delim))
                continue
              raise SqlPipeError(
                  f'malformed CSV row in {fp}: {csv_err}') from None
            if first:
              first = False
              continue
            if not rec or (len(rec) == 1 and rec[0] == ''):
              continue
            raw_count += 1
            if len(rec) != len(header):
              err_msg = f'expected {len(header)} columns, got {len(rec)}'
              if dlq_records is not None:
                dlq_records.append(
                    _redact_dlq_entry(
                        {
                            'step': 'ReadCsvWithDlq',
                            'error_type': 'CsvColumnCountMismatch',
                            'error': err_msg,
                            'raw_payload': json.dumps(rec),
                        },
                        redact_tuple,
                        header=header,
                        delim=delim))
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
                dlq_records.append(
                    _redact_dlq_entry(
                        {
                            'step': 'ReadCsvWithDlq',
                            'error_type': type(e).__name__,
                            'error': str(e),
                            'raw_payload': json.dumps(dict(zip(header, rec))),
                        },
                        redact_tuple,
                        header=header,
                        delim=delim))
                continue
              raise SqlPipeError(f'corrupt CSV value in {fp}: {e}') from None
        finally:
          try:
            text_stream.detach()
          except Exception:  # pylint: disable=broad-except
            pass
    return cols, rows, raw_count

  if fmt == 'json':
    import pyarrow as pa
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
                      'BOOLEAN' if isinstance(v, bool) else
                      'INTEGER' if isinstance(v, int) else
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
            dlq_records.append(
                _redact_dlq_entry(dlq.json_dlq_record(s, e), redact_tuple))
          continue
        try:
          obj = json.loads(s)
          if not isinstance(obj, dict):
            raise ValueError('expected JSON object per line')
          rows.append(obj)
          for k, v in obj.items():
            if v is not None and (k not in col_types or col_types[k] == 'TEXT'):
              if isinstance(v, bool):
                col_types.setdefault(k, 'BOOLEAN')
              elif isinstance(v, int):
                col_types.setdefault(k, 'INTEGER')
              elif isinstance(v, float):
                col_types[k] = 'REAL'
              else:
                col_types.setdefault(k, 'TEXT')
            elif k not in col_types:
              col_types[k] = 'TEXT'
        except Exception as e:  # pylint: disable=broad-except
          if dlq_records is not None:
            dlq_records.append(
                _redact_dlq_entry(
                    {
                        'step': 'ReadFromJsonWithDlq',
                        'error_type': type(e).__name__,
                        'error': str(e),
                        'raw_payload': s,
                    },
                    redact_tuple))
            continue
          raise SqlPipeError(
              f'invalid JSON record in {fp}: {type(e).__name__}') from None
    if drift_count > 0 and warnings is not None:
      warnings.append(dlq.schema_drift_warning(drift_count, drift_fields))
    if kinds is not None:
      col_types = {
          k: ('BOOLEAN' if v == 'bool' else
              'INTEGER' if v == 'int' else
              'REAL' if v == 'float' else 'TEXT')
          for k, v in kinds.items()
      }
    _SQL_THREAD_LOCAL.last_endpoint_arrow_types = {
        k: (
            pa.bool_() if ct == 'BOOLEAN' else
            pa.int64() if ct == 'INTEGER' else
            pa.float64() if ct == 'REAL' else pa.string())
        for k, ct in col_types.items()
    }
    cols = list(col_types.items())
    return cols, rows, raw_count

  if fmt == 'avro':
    import fastavro
    import pyarrow as pa
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
                  'BOOLEAN' if isinstance(v, bool) else
                  'INTEGER' if isinstance(v, int) else
                  'REAL' if isinstance(v, (float, decimal.Decimal)) else 'TEXT')
            elif k not in col_types:
              col_types[k] = 'TEXT'
    _SQL_THREAD_LOCAL.last_endpoint_arrow_types = {
        k: (
            pa.bool_() if ct == 'BOOLEAN' else
            pa.int64() if ct == 'INTEGER' else
            pa.float64() if ct == 'REAL' else pa.string())
        for k, ct in col_types.items()
    }
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
  if v is None:
    return None
  if isinstance(v, bool):
    return 1 if v else 0
  if isinstance(v, (int, float, str, bytes)):
    return v
  if isinstance(v, decimal.Decimal):
    if v.is_nan():
      return None
    if v.is_infinite():
      return float('-inf') if v.is_signed() else float('inf')
    if v == v.to_integral_value() and v.as_tuple().exponent >= 0:
      iv = int(v)
      if -(1 << 63) <= iv <= (1 << 63) - 1:
        return iv
    try:
      return float(v)
    except OverflowError:
      return float('-inf') if v.is_signed() else float('inf')
  if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
    return v.isoformat()
  return json.dumps(v, default=str)


def _register_scalar_functions(conn: sqlite3.Connection) -> None:
  conn.create_function(
      'LEN',
      1,
      lambda x: None if x is None else len(str(x)),
      deterministic=True)
  conn.create_function(
      'STRIP',
      1,
      lambda x: None if x is None else str(x).strip(),
      deterministic=True)
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
      qf = float(q)
      if math.isnan(qf) or math.isinf(qf):
        raise ValueError(f'quantile must be finite, got {q!r}')
      self._q_dec = (
          decimal.Decimal(str(q)) / decimal.Decimal(str(self.scale)))
    if value is not None:
      try:
        fv = float(value)
      except (ValueError, TypeError):
        return
      if not math.isnan(fv):
        self._values.append(fv)

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
        fv = float(value)
      except (ValueError, TypeError):
        return
      if not math.isnan(fv):
        self._values.append(fv)

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
        fv = float(value)
      except (ValueError, TypeError):
        return
      if not math.isnan(fv):
        self._values.append(fv)

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
        fv = float(value)
      except (ValueError, TypeError):
        return
      if not math.isnan(fv):
        self._values.append(fv)

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
        fv = float(value)
      except (ValueError, TypeError):
        return
      if not math.isnan(fv):
        self._values.append(fv)

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
    if math.isnan(yf) or math.isnan(xf):
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


_ARROW_TYPES_FOR_SQLITE = {
    'INTEGER': 'int64',
    'REAL': 'float64',
    'BOOLEAN': 'bool',
}


def _arrow_table(cols, rows, arrow_hints: dict[str, Any] | None = None):
  """Builds a pyarrow Table from loaded rows, keeping nested values."""
  import pyarrow as pa
  names = [c for c, _ in cols] or (
      list(rows[0]) if rows else [_EMPTY_PLACEHOLDER_COL])
  types = dict(cols)
  hints = (
      arrow_hints
      if arrow_hints is not None else getattr(
          _SQL_THREAD_LOCAL, 'last_endpoint_arrow_types', None) or {})
  arrays = []
  for name in names:
    values = [r.get(name) for r in rows]
    exact_pa = hints.get(name)
    declared = _ARROW_TYPES_FOR_SQLITE.get(types.get(name, ''), None)
    try:
      if exact_pa is not None and not any(
          isinstance(v, (dict, list)) for v in values):
        arrays.append(pa.array(values, type=exact_pa))
      elif declared and not any(
          isinstance(v, (dict, list, decimal.Decimal)) for v in values):
        arrays.append(pa.array(values, type=declared))
      else:
        arr = pa.array(values) if values else pa.array([], type=pa.string())
        if pa.types.is_null(arr.type):
          arr = pa.array(values, type=pa.string())
        arrays.append(arr)
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


def _duckdb_type_to_arrow(type_desc: Any):
  import pyarrow as pa
  raw = str(type_desc or '').strip().upper()
  if raw in ('BOOLEAN', 'BOOL'):
    return pa.bool_()
  if raw == 'TINYINT':
    return pa.int8()
  if raw == 'SMALLINT':
    return pa.int16()
  if raw in ('INTEGER', 'INT', 'INT4'):
    return pa.int32()
  if raw in ('BIGINT', 'INT8', 'HUGEINT'):
    return pa.int64()
  if raw == 'UTINYINT':
    return pa.uint8()
  if raw == 'USMALLINT':
    return pa.uint16()
  if raw == 'UINTEGER':
    return pa.uint32()
  if raw in ('UBIGINT', 'UHUGEINT'):
    return pa.uint64()
  if raw in ('FLOAT', 'REAL', 'FLOAT4'):
    return pa.float32()
  if raw in ('DOUBLE', 'FLOAT8'):
    return pa.float64()
  m_dec = re.match(r'^(?:DECIMAL|NUMERIC)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$', raw)
  if m_dec:
    p, s = int(m_dec.group(1)), int(m_dec.group(2))
    return pa.decimal128(min(p, 38), s) if p <= 38 else pa.decimal256(p, s)
  if raw == 'DATE':
    return pa.date32()
  if raw.startswith('TIME') and not raw.startswith('TIMESTAMP'):
    return pa.time64('us')
  if 'WITH TIME ZONE' in raw or raw == 'TIMESTAMPTZ':
    return pa.timestamp('us', tz='UTC')
  if raw == 'TIMESTAMP_S':
    return pa.timestamp('s')
  if raw == 'TIMESTAMP_MS':
    return pa.timestamp('ms')
  if raw == 'TIMESTAMP_NS':
    return pa.timestamp('ns')
  if raw.startswith('TIMESTAMP') or raw == 'DATETIME':
    return pa.timestamp('us')
  if raw in ('BLOB', 'BYTEA', 'BINARY', 'VARBINARY'):
    return pa.binary()
  return pa.string()


def _run_duckdb(
    named_sources,
    clean_q,
    *,
    default_project,
    dlq_records,
    dlq_redact_fields=None,
    warnings=None,
    pre_where_fn=None):
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
  injected_empty = False
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
          dlq_redact_fields=dlq_redact_fields,
          warnings=warnings)
      total_read += raw_cnt
      if pre_where_fn is not None:
        rows = pre_where_fn(rows)
      if not cols and not rows:
        injected_empty = True
      ep_hints = dict(
          getattr(_SQL_THREAD_LOCAL, 'last_endpoint_arrow_types', None) or {})
      con.register(ns.alias, _arrow_table(cols, rows, arrow_hints=ep_hints))
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
    raw_max_sec = os.environ.get('BEAM_CLI_SQL_MAX_SECONDS', '').strip()
    try:
      max_sql_sec = float(raw_max_sec) if raw_max_sec else 30.0
      if not math.isfinite(max_sql_sec) or max_sql_sec <= 0:
        max_sql_sec = 30.0
    except ValueError:
      max_sql_sec = 30.0
    raw_max_rows = os.environ.get('BEAM_CLI_SQL_MAX_RESULT_ROWS', '').strip()
    try:
      max_sql_rows = int(raw_max_rows) if raw_max_rows else 1_000_000
      if max_sql_rows <= 0:
        max_sql_rows = 1_000_000
    except ValueError:
      max_sql_rows = 1_000_000
    con.execute(f"SET memory_limit = '{raw_mem}'")
    con.execute("SET max_temp_directory_size = '0B'")
    con.execute('SET threads = 2')
    con.execute('SET lock_configuration = true')
    timed_out = False

    def _interrupt_duckdb() -> None:
      nonlocal timed_out
      timed_out = True
      try:
        con.interrupt()
      except Exception:  # pylint: disable=broad-except
        pass

    watchdog = threading.Timer(max_sql_sec, _interrupt_duckdb)
    watchdog.daemon = True
    watchdog.start()
    try:
      cur = con.execute(clean_q)
      desc = list(cur.description or [])
      out_cols = [d[0] for d in desc]
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
      if timed_out or 'interrupt' in str(e).lower():
        raise SqlPipeError(
            f'SQL query exceeded local execution time limit '
            f'({max_sql_sec:g}s); check for unbounded recursive CTEs or '
            'cross joins') from None
      avail_list = [ns.alias for ns in named_sources]
      if len(named_sources) == 1 and 'source' not in used_lower:
        avail_list.append('source')
      avail = ', '.join(avail_list)
      first_line = str(e).splitlines()[0].replace(
          _EMPTY_PLACEHOLDER_COL, '<empty>')
      raise SqlPipeError(
          f'SQL execution error (DuckDB): {first_line} '
          f'(available tables: {avail})'
          f'{_sql_error_hint(first_line, clean_q)}') from None
    finally:
      watchdog.cancel()

    if injected_empty and _EMPTY_PLACEHOLDER_COL in out_cols:
      keep_idx = [
          idx for idx, c in enumerate(out_cols) if c != _EMPTY_PLACEHOLDER_COL
      ]
      out_cols = [out_cols[i] for i in keep_idx]
      desc = [desc[i] for i in keep_idx]
      fetched = [tuple(t[i] for i in keep_idx) for t in fetched]

    _SQL_THREAD_LOCAL.last_cols = tuple(out_cols)
    _SQL_THREAD_LOCAL.last_arrow_types = {
        d[0]: _duckdb_type_to_arrow(d[1])
        for d in desc
    }
    return out_cols, [dict(zip(out_cols, t)) for t in fetched], total_read
  finally:
    con.close()


def _build_local_sql_pre_where_fn(
    pre_where: str | None,
    dlq_records: list[dict[str, Any]] | None,
    dlq_redact_fields: Sequence[str] | str | None,
):
  if not pre_where:
    return None
  from . import expressions
  from . import fast_local
  redact_tuple = _parse_redact_fields(dlq_redact_fields)
  try:
    where_cfg = expressions.compile_where(pre_where, known_columns=None)[0]
    keep_fn = fast_local._compile_field(where_cfg.get('keep'))
  except (expressions.ExpressionError, fast_local.FastPipeError) as e:
    raise SqlPipeError(str(e)) from None

  def _filter_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    kept: list[dict[str, Any]] = []
    for r in rows:
      try:
        if keep_fn(fast_local._rowify(r)):
          kept.append(r)
      except Exception as exc:  # pylint: disable=broad-except
        if dlq_records is None:
          raise SqlPipeError(
              f"{type(exc).__name__}: {exc} [while running '--where']"
          ) from None
        dlq_records.append(
            dlq.make_transform_dlq_row(
                'Filter', exc, r, redact_fields=redact_tuple))
    return kept

  return _filter_rows


def _apply_local_sql_post_select(
    out_cols: list[str],
    out_rows: list[dict[str, Any]],
    post_select: str | None,
    dlq_records: list[dict[str, Any]] | None,
    dlq_redact_fields: Sequence[str] | str | None,
) -> tuple[list[str], list[dict[str, Any]]]:
  if not post_select:
    return out_cols, out_rows
  from . import expressions
  from . import fast_local
  redact_tuple = _parse_redact_fields(dlq_redact_fields)
  try:
    sel_cfg = expressions.compile_select(post_select, known_columns=None)[0]
    raw_fields = sel_cfg.get('fields', {})
    compiled_fields = {
        name: fast_local._compile_field(spec)
        for name, spec in raw_fields.items()
    }
  except (expressions.ExpressionError, fast_local.FastPipeError) as e:
    raise SqlPipeError(str(e)) from None

  is_append = bool(sel_cfg.get('append'))
  drop_set = set(sel_cfg.get('drop') or ())
  base_cols = ([
      c for c in out_cols if c not in drop_set and c not in compiled_fields
  ] if is_append else [])
  final_cols = list(base_cols) + list(compiled_fields.keys())
  projected: list[dict[str, Any]] = []
  for r in out_rows:
    try:
      row_obj = fast_local._rowify(r)
      computed = {
          name: fast_local._plain(fn(row_obj))
          for name, fn in compiled_fields.items()
      }
      if is_append:
        merged = {c: r.get(c) for c in base_cols}
        merged.update(computed)
        projected.append(merged)
      else:
        projected.append(computed)
    except Exception as exc:  # pylint: disable=broad-except
      if dlq_records is None:
        raise SqlPipeError(
            f"{type(exc).__name__}: {exc} [while running '--select']"
        ) from None
      dlq_records.append(
          dlq.make_transform_dlq_row(
              'MapToFields', exc, r, redact_fields=redact_tuple))
  _SQL_THREAD_LOCAL.last_cols = None
  _SQL_THREAD_LOCAL.last_arrow_types = None
  return final_cols, projected


def run_in_memory_sql(
    named_sources: list[NamedSource],
    sql_query: str,
    *,
    default_project: str | None = None,
    enable_dlq: bool = False,
    dlq_redact_fields: Sequence[str] | str | None = None,
    engine: str = 'sqlite',
    warnings: list[str] | None = None,
    pre_where: str | None = None,
    post_select: str | None = None,
) -> tuple[list[str], list[dict[str, Any]], list[dict[str, Any]], int]:
  """Executes `sql_query` in a sandboxed in-memory SQLite or DuckDB database.

  Returns `(out_columns, out_rows, dlq_records, total_rows_read)`.
  """
  if engine not in SQL_ENGINES:
    raise SqlPipeError(
        f'unknown --sql-engine {engine!r}; use one of {", ".join(SQL_ENGINES)}')
  if pre_where and len(named_sources) != 1:
    raise SqlPipeError(
        '--where with --sql is only supported on a single input source; '
        'for multi-input SQL joins, move the filter into --sql WHERE ...')
  clean_q = validate_readonly_select_sql(sql_query)
  orig_code_only = ' '.join(
      seg for is_c, seg in _scan_sql_outside_literals(clean_q) if is_c)
  dlq_records: list[dict[str, Any]] | None = [] if enable_dlq else None
  pre_where_fn = _build_local_sql_pre_where_fn(
      pre_where, dlq_records, dlq_redact_fields)
  if engine == 'duckdb':
    out_cols, out_rows, total_read = _run_duckdb(
        named_sources,
        clean_q,
        default_project=default_project,
        dlq_records=dlq_records,
        dlq_redact_fields=dlq_redact_fields,
        warnings=warnings,
        pre_where_fn=pre_where_fn)
    out_cols, out_rows = _apply_local_sql_post_select(
        out_cols, out_rows, post_select, dlq_records, dlq_redact_fields)
    return out_cols, out_rows, (dlq_records or []), total_read

  is_sqlite_asof = bool(re.search(r'\bASOF\b', orig_code_only, re.I))
  parsed_asof: _ParsedAsofSql | None = None
  if is_sqlite_asof:
    parsed_asof = _parse_asof_sql_join(
        clean_q, named_sources, allow_post_clauses=True)

  clean_q = _normalize_sqlite_interval_expressions(
      _normalize_sqlite_cast_types(
          _rewrite_source_uri_table_refs(clean_q, named_sources)))
  total_read = 0
  injected_empty = False
  src_arrow_types: dict[str, Any] = {}
  ambiguous_arrow_cols: set[str] = set()

  def _record_src_arrow(col_name: str, pa_type: Any) -> None:
    if pa_type is None:
      return
    low_c = col_name.lower()
    if low_c in ambiguous_arrow_cols:
      return
    if low_c in src_arrow_types and src_arrow_types[low_c] != pa_type:
      del src_arrow_types[low_c]
      ambiguous_arrow_cols.add(low_c)
      return
    src_arrow_types[low_c] = pa_type

  conn = sqlite3.connect(':memory:')
  try:
    _register_scalar_functions(conn)
    used_lower: set[str] = set()
    first_alias: str | None = None
    bool_cols: set[str] = set()
    non_bool_cols: set[str] = set()

    if parsed_asof is not None:
      from . import asof_join
      l_cols, l_rows, l_raw = load_endpoint_rows(
          parsed_asof.left_ns.endpoint,
          default_project=default_project,
          dlq_records=dlq_records,
          dlq_redact_fields=dlq_redact_fields,
          warnings=warnings)
      l_pa_map = dict(
          getattr(_SQL_THREAD_LOCAL, 'last_endpoint_arrow_types', None) or {})
      r_cols, r_rows, r_raw = load_endpoint_rows(
          parsed_asof.right_ns.endpoint,
          default_project=default_project,
          dlq_records=dlq_records,
          dlq_redact_fields=dlq_redact_fields,
          warnings=warnings)
      r_pa_map = dict(
          getattr(_SQL_THREAD_LOCAL, 'last_endpoint_arrow_types', None) or {})
      total_read = l_raw + r_raw
      if not l_cols and l_rows:
        l_cols = [(k, 'TEXT') for k in l_rows[0].keys()]
      if not r_cols and r_rows:
        r_cols = [(k, 'TEXT') for k in r_rows[0].keys()]

      for cn, ct in l_cols:
        _record_src_arrow(cn, l_pa_map.get(cn))
        if ct == 'BOOLEAN':
          bool_cols.add(cn.lower())
        elif ct in ('INTEGER', 'REAL'):
          non_bool_cols.add(cn.lower())
      for cn, ct in r_cols:
        _record_src_arrow(cn, r_pa_map.get(cn))
        if ct == 'BOOLEAN':
          bool_cols.add(cn.lower())
        elif ct in ('INTEGER', 'REAL'):
          non_bool_cols.add(cn.lower())

      l_names = [cn for cn, _ in l_cols]
      r_names = [cn for cn, _ in r_cols]
      synth_select: dict[str, str] = {}
      synth_cols: list[tuple[str, str]] = []
      for cn, ct in l_cols:
        sk = f'__l__{cn}'
        synth_select[sk] = f'{parsed_asof.left_sql_alias}.{cn}'
        synth_cols.append((sk, ct))
      for cn, ct in r_cols:
        sk = f'__r__{cn}'
        synth_select[sk] = f'{parsed_asof.right_sql_alias}.{cn}'
        synth_cols.append((sk, ct))

      aj_cfg = asof_join.parse_asof_join_config({
          'left_alias': parsed_asof.left_sql_alias,
          'right_alias': parsed_asof.right_sql_alias,
          'left_key': parsed_asof.left_keys,
          'right_key': parsed_asof.right_keys,
          'left_time': parsed_asof.left_time,
          'right_time': parsed_asof.right_time,
          'direction': parsed_asof.direction,
          'strict': parsed_asof.strict,
          'tolerance': (
              _format_seconds_duration(parsed_asof.tolerance)
              if parsed_asof.tolerance is not None else None),
          'join_type': parsed_asof.join_type,
          'left_fields': l_names,
          'right_fields': r_names,
          'select': synth_select or None,
      })
      joined_rows, _, _ = asof_join.join_bounded_asof(l_rows, r_rows, aj_cfg)

      if not synth_cols:
        synth_cols = [(_EMPTY_PLACEHOLDER_COL, 'TEXT')]
        injected_empty = True
      col_defs = ', '.join(f'{_quote_ident(cn)} {ct}' for cn, ct in synth_cols)
      conn.execute(f'CREATE TABLE "_asof_joined" ({col_defs})')
      if joined_rows:
        s_names = [cn for cn, _ in synth_cols]
        for sn in s_names:
          orig_c = sn[5:] if sn.startswith(('__l__', '__r__')) else sn
          non_null = [
              jr.get(sn) for jr in joined_rows if jr.get(sn) is not None
          ]
          if non_null and all(isinstance(v, bool) for v in non_null):
            bool_cols.add(orig_c.lower())
          elif non_null:
            non_bool_cols.add(orig_c.lower())
        placeholders = ', '.join('?' for _ in s_names)
        col_list_sql = ', '.join(_quote_ident(sn) for sn in s_names)
        batch = [
            tuple(_normalize_sqlite_param(jr.get(sn)) for sn in s_names)
            for jr in joined_rows
        ]
        conn.executemany(
            f'INSERT INTO "_asof_joined" ({col_list_sql}) VALUES ({placeholders})',
            batch)

      l_low = parsed_asof.left_sql_alias.lower()
      r_low = parsed_asof.right_sql_alias.lower()
      l_set = {c.lower(): c for c in l_names}
      r_set = {c.lower(): c for c in r_names}
      qual_re = re.compile(
          r'(?:`|")?(?P<a>[A-Za-z_][A-Za-z0-9_]*)(?:`|")?\s*\.\s*(?:`|")?(?P<c>[A-Za-z_][A-Za-z0-9_]*)(?:`|")?'
      )

      def _rewrite_asof_fragment(
          frag: str, *, auto_alias_select: bool = False) -> str:
        if auto_alias_select and frag.strip() == '*':
          star_items: list[str] = []
          for lk in parsed_asof.left_keys:
            star_items.append(
                f'{_quote_ident("__l__" + lk)} AS {_quote_ident(lk)}')
          conflicts = (set(l_names) & set(r_names)) - set(parsed_asof.left_keys)
          for cn in l_names:
            if cn in parsed_asof.left_keys:
              continue
            out_n = f'{parsed_asof.left_sql_alias}_{cn}' if cn in conflicts else cn
            star_items.append(
                f'{_quote_ident("__l__" + cn)} AS {_quote_ident(out_n)}')
          for cn in r_names:
            if cn in parsed_asof.right_keys:
              continue
            out_n = f'{parsed_asof.right_sql_alias}_{cn}' if cn in conflicts else cn
            star_items.append(
                f'{_quote_ident("__r__" + cn)} AS {_quote_ident(out_n)}')
          return ', '.join(star_items) or _quote_ident(_EMPTY_PLACEHOLDER_COL)

        def _rep_qual(qm: re.Match) -> str:
          qa = qm.group('a').lower()
          qc = qm.group('c')
          if qa == l_low and qc.lower() in l_set:
            return _quote_ident(f'__l__{l_set[qc.lower()]}')
          if qa == r_low and qc.lower() in r_set:
            return _quote_ident(f'__r__{r_set[qc.lower()]}')
          return qm.group(0)

        def _rep_unqual(seg_s: str) -> str:
          # Rewrite bare column identifiers that unambiguously belong to L or R
          ident_re = re.compile(
              r'(?<![A-Za-z0-9_."`])([A-Za-z_][A-Za-z0-9_]*)(?![A-Za-z0-9_."`(])'
          )

          def _u_cb(um: re.Match) -> str:
            tok = um.group(1)
            tl = tok.lower()
            if tl in l_set and tl not in r_set:
              return _quote_ident(f'__l__{l_set[tl]}')
            if tl in r_set and tl not in l_set:
              return _quote_ident(f'__r__{r_set[tl]}')
            return tok

          return ident_re.sub(_u_cb, seg_s)

        if auto_alias_select:
          from . import interval_join as _ij
          out_toks: list[str] = []
          as_chk = re.compile(r'\s+AS\s+[A-Za-z0-9_"`]+\s*$', re.I)
          bare_q_re = re.compile(
              r'^\s*(?:`|")?[A-Za-z_][A-Za-z0-9_]*(?:`|")?\s*\.\s*(?:`|")?(?P<c>[A-Za-z_][A-Za-z0-9_]*)(?:`|")?\s*$'
          )
          bare_id_re = re.compile(r'^\s*(?P<c>[A-Za-z_][A-Za-z0-9_]*)\s*$')
          for raw_tok in _ij.split_bracket_tokens(frag):
            rt = raw_tok.strip()
            rw = ''.join(
                _rep_unqual(qual_re.sub(_rep_qual, s)) if ic else s
                for ic, s in _scan_sql_outside_literals(rt))
            if not as_chk.search(rt):
              mq = bare_q_re.match(rt) or bare_id_re.match(rt)
              if mq:
                rw = f'{rw} AS {_quote_ident(mq.group("c"))}'
            out_toks.append(rw)
          return ', '.join(out_toks)

        return ''.join(
            _rep_unqual(qual_re.sub(_rep_qual, s)) if ic else s
            for ic, s in _scan_sql_outside_literals(frag))

      sel_sql = _rewrite_asof_fragment(
          parsed_asof.select_raw, auto_alias_select=True)
      w_clauses: list[str] = []
      for ef in parsed_asof.extra_on_filters:
        w_clauses.append(f'({_rewrite_asof_fragment(ef)})')
      if parsed_asof.where_body:
        w_clauses.append(f'({_rewrite_asof_fragment(parsed_asof.where_body)})')
      where_sql = f' WHERE {" AND ".join(w_clauses)}' if w_clauses else ''
      tail_rewritten = (
          ' ' + _rewrite_asof_fragment(parsed_asof.tail_sql)
          if parsed_asof.tail_sql else '')
      clean_q = _normalize_sqlite_interval_expressions(
          _normalize_sqlite_cast_types(
              f'SELECT {sel_sql} FROM "_asof_joined"{where_sql}{tail_rewritten}'
          ))
      used_lower.add('_asof_joined')
      first_alias = '_asof_joined'
    else:
      for ns in named_sources:
        cols, rows, raw_cnt = load_endpoint_rows(
            ns.endpoint,
            default_project=default_project,
            dlq_records=dlq_records,
            dlq_redact_fields=dlq_redact_fields,
            warnings=warnings)
        ep_pa_map = dict(
            getattr(_SQL_THREAD_LOCAL, 'last_endpoint_arrow_types', None) or {})
        total_read += raw_cnt
        if pre_where_fn is not None:
          rows = pre_where_fn(rows)
        if not cols and rows:
          cols = [(k, 'TEXT') for k in rows[0].keys()]
        for cn, ct in cols:
          _record_src_arrow(cn, ep_pa_map.get(cn))
          if ct == 'BOOLEAN':
            bool_cols.add(cn.lower())
          elif ct in ('INTEGER', 'REAL'):
            non_bool_cols.add(cn.lower())
        if not cols:
          # Empty dataset with no schema columns -> internal placeholder column
          cols = [(_EMPTY_PLACEHOLDER_COL, 'TEXT')]
          injected_empty = True
        col_defs = ', '.join(f'{_quote_ident(cn)} {ct}' for cn, ct in cols)
        t_ident = _quote_ident(ns.alias)
        conn.execute(f'CREATE TABLE {t_ident} ({col_defs})')
        if rows:
          col_names = [cn for cn, _ in cols]
          for cn in col_names:
            non_null_vals = [r.get(cn) for r in rows if r.get(cn) is not None]
            if non_null_vals and all(isinstance(v, bool)
                                     for v in non_null_vals):
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
      del arg1, dbname, source
      if action_code == sqlite3.SQLITE_FUNCTION and (
          str(arg2 or '').lower() in {'zeroblob', 'randomblob', 'load_extension'}
      ):
        return sqlite3.SQLITE_DENY
      if action_code in allowed_actions:
        return sqlite3.SQLITE_OK
      return sqlite3.SQLITE_DENY

    import time as _time
    raw_max_sec = os.environ.get('BEAM_CLI_SQL_MAX_SECONDS', '').strip()
    try:
      max_sql_sec = float(raw_max_sec) if raw_max_sec else 30.0
      if not math.isfinite(max_sql_sec) or max_sql_sec <= 0:
        max_sql_sec = 30.0
    except ValueError:
      max_sql_sec = 30.0
    raw_max_rows = os.environ.get('BEAM_CLI_SQL_MAX_RESULT_ROWS', '').strip()
    try:
      max_sql_rows = int(raw_max_rows) if raw_max_rows else 1_000_000
      if max_sql_rows <= 0:
        max_sql_rows = 1_000_000
    except ValueError:
      max_sql_rows = 1_000_000
    sql_deadline = _time.monotonic() + max_sql_sec
    timed_out = False

    def _progress_guard() -> int:
      nonlocal timed_out
      if _time.monotonic() > sql_deadline:
        timed_out = True
        return 1
      return 0

    if hasattr(conn, 'setlimit') and hasattr(sqlite3, 'SQLITE_LIMIT_LENGTH'):
      try:
        conn.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 16 * 1024 * 1024)
      except Exception:  # pylint: disable=broad-except
        pass
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
      err_str = str(e).replace(_EMPTY_PLACEHOLDER_COL, '<empty>')
      raise SqlPipeError(
          f'SQL execution error: {err_str} (available tables: {avail})'
          f'{_sql_error_hint(err_str, clean_q)}') from None

    if injected_empty and _EMPTY_PLACEHOLDER_COL in out_cols:
      keep_idx = [
          idx for idx, c in enumerate(out_cols) if c != _EMPTY_PLACEHOLDER_COL
      ]
      out_cols = [out_cols[i] for i in keep_idx]
      fetched = [tuple(t[i] for i in keep_idx) for t in fetched]

    out_rows = [dict(zip(out_cols, tup)) for tup in fetched]

    # Restore boolean values and record Arrow types for unambiguous output cols.
    try:
      import pyarrow as pa
    except ImportError:
      pa = None
    out_bool_cols: set[str] = set()
    for oc in out_cols:
      esc_oc = re.escape(oc)
      cast_bool = bool(
          re.search(
              rf'\bCAST\s*\([^)]+\bAS\s+(?:BOOL|BOOLEAN)\s*\)\s+AS\s+[`"]?{esc_oc}[`"]?\b',
              orig_code_only,
              re.I))
      non_bool_expr = bool(
          re.search(
              rf'\b(?:COUNT|SUM|AVG|TOTAL|CAST)\s*\([^)]*\)\s+AS\s+[`"]?{esc_oc}[`"]?\b',
              orig_code_only,
              re.I)) and not cast_bool
      if (cast_bool or (oc.lower() in pure_bool_cols and not non_bool_expr)):
        if all(
            r.get(oc) is None or (isinstance(r.get(oc), int) and r.get(oc) in (0, 1))
            for r in out_rows):
          out_bool_cols.add(oc)
          for r in out_rows:
            val = r.get(oc)
            if val is not None:
              r[oc] = bool(val)

    if pa is not None:
      out_arrow_types: dict[str, Any] = {}
      for oc in out_cols:
        esc_oc = re.escape(oc)
        if oc in out_bool_cols:
          out_arrow_types[oc] = pa.bool_()
        elif re.search(
            rf'\bCOUNT\s*\([^)]*\)\s+AS\s+[`"]?{esc_oc}[`"]?\b'
            rf'|\bCAST\s*\([^)]+\bAS\s+(?:INT|INTEGER|INT64|BIGINT)\s*\)\s+AS\s+[`"]?{esc_oc}[`"]?\b',
            orig_code_only,
            re.I):
          out_arrow_types[oc] = pa.int64()
        elif re.search(
            rf'\b(?:AVG|MEDIAN|PERCENTILE|QUANTILE\w*|STDDEV\w*|VAR\w*|CORR|COVAR\w*|REGR_\w+)\s*\([^)]*\)\s+AS\s+[`"]?{esc_oc}[`"]?\b'
            rf'|\bCAST\s*\([^)]+\bAS\s+(?:FLOAT|FLOAT64|DOUBLE|REAL|DECIMAL|NUMERIC|BIGNUMERIC)\s*\)\s+AS\s+[`"]?{esc_oc}[`"]?\b',
            orig_code_only,
            re.I):
          out_arrow_types[oc] = pa.float64()
        elif oc.lower() in src_arrow_types:
          out_arrow_types[oc] = src_arrow_types[oc.lower()]
        else:
          out_arrow_types[oc] = pa.string()
      _SQL_THREAD_LOCAL.last_cols = tuple(out_cols)
      _SQL_THREAD_LOCAL.last_arrow_types = out_arrow_types

    out_cols, out_rows = _apply_local_sql_post_select(
        out_cols, out_rows, post_select, dlq_records, dlq_redact_fields)
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
    cur_umask = os.umask(0o077)
    os.umask(cur_umask)
    os.chmod(tmp_path, 0o666 & ~cur_umask)
  except OSError:
    pass
  return tmp_path


def _sanitize_json_nan(val: Any) -> Any:
  if isinstance(val, float):
    return None if (math.isnan(val) or math.isinf(val)) else val
  if isinstance(val, decimal.Decimal):
    return None if (val.is_nan() or val.is_infinite()) else val
  if isinstance(val, dict):
    return {k: _sanitize_json_nan(v) for k, v in val.items()}
  if isinstance(val, (list, tuple)):
    return [_sanitize_json_nan(v) for v in val]
  return val


def write_json_document(path: str, doc: Any) -> str:
  tmp_path = _new_sql_stage_path(path, 'doc')
  try:
    with open(tmp_path, 'w', encoding='utf-8') as f:
      json.dump(
          _sanitize_json_nan(doc), f, indent=2, default=str, allow_nan=False)
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
      arr = pa.array(values)
      if pa.types.is_null(arr.type):
        arr = pa.array(values, type=pa.string())
      arrays[col] = arr
    except (pa.ArrowInvalid, pa.ArrowTypeError):
      if warnings is not None:
        warnings.append(
            f'column {col!r} mixes numbers and text, so it was written as '
            f'text; use CAST({col} AS REAL) in --sql (non-numbers become '
            'NULL) or fix the source values')
      arrays[col] = pa.array([
          None if v is None else
          json.dumps(_sanitize_json_nan(v), default=str, allow_nan=False)
          if isinstance(v, (dict, list)) else str(v) for v in values
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
    column_types: dict[str, Any] | None = None,
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
        json.dump(
            _sanitize_json_nan(doc), f, indent=2, default=str, allow_nan=False)
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
      '.avro',
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
      eff_col_types: dict[str, Any] = {}
      if column_types is not None:
        eff_col_types = dict(column_types)
      elif (tag != 'dlq' and
            getattr(_SQL_THREAD_LOCAL, 'last_cols', None) == tuple(out_cols)):
        eff_col_types = dict(
            getattr(_SQL_THREAD_LOCAL, 'last_arrow_types', None) or {})
      if rows_to_write:
        try:
          tbl = pa.Table.from_pylist(rows_to_write)
          if any(pa.types.is_null(f.type) for f in tbl.schema):
            fixed_cols = {}
            for f in tbl.schema:
              if pa.types.is_null(f.type):
                fb_t = eff_col_types.get(f.name, pa.string())
                fixed_cols[f.name] = pa.array(
                    [None] * len(rows_to_write), type=fb_t)
              else:
                fixed_cols[f.name] = tbl.column(f.name)
            tbl = pa.table(fixed_cols)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
          # A column holds mixed types (SQLite is dynamically typed): write
          # those columns as text rather than failing the whole run.
          tbl = _mixed_columns_as_text(out_cols, rows_to_write, warnings)
      else:
        tbl = pa.table({
            c: pa.array([], type=eff_col_types.get(c, pa.string()))
            for c in out_cols
        })
      pq.write_table(tbl, tmp_path)
    elif fmt == 'avro':
      try:
        import fastavro
      except ImportError as exc:
        raise SqlEngineMissingError(
            "Writing Avro locally requires 'fastavro'; install with: "
            'pip install fastavro') from exc
      avro_fields: list[dict[str, Any]] = []
      avro_kinds: dict[str, str] = {}
      for col in out_cols:
        non_null = [r.get(col) for r in rows_to_write if r.get(col) is not None]
        if non_null and all(isinstance(v, bool) for v in non_null):
          ak = 'boolean'
        elif non_null and all(
            isinstance(v, int) and not isinstance(v, bool) for v in non_null):
          ak = 'long'
        elif non_null and all(
            isinstance(v, (int, float)) and not isinstance(v, bool)
            for v in non_null):
          ak = 'double'
        elif non_null and all(
            isinstance(v, (bytes, bytearray, memoryview)) for v in non_null):
          ak = 'bytes'
        else:
          ak = 'string'
        avro_kinds[col] = ak
        avro_fields.append({
            'name': col,
            'type': ['null', ak],
            'default': None,
        })
      parsed_schema = fastavro.parse_schema({
          'type': 'record',
          'name': 'BeamCliSqlRecord',
          'fields': avro_fields,
      })
      avro_records: list[dict[str, Any]] = []
      for r in rows_to_write:
        rec_out: dict[str, Any] = {}
        for col in out_cols:
          v = r.get(col)
          if v is None:
            rec_out[col] = None
          elif avro_kinds[col] == 'double':
            rec_out[col] = float(v)
          elif avro_kinds[col] == 'bytes':
            rec_out[col] = bytes(v)
          elif avro_kinds[col] == 'string' and not isinstance(v, str):
            rec_out[col] = (
                json.dumps(_sanitize_json_nan(v), default=str, allow_nan=False)
                if isinstance(v, (dict, list)) else str(v))
          else:
            rec_out[col] = v
        avro_records.append(rec_out)
      with open(tmp_path, 'wb') as f:
        fastavro.writer(f, parsed_schema, avro_records)
    elif fmt == 'csv':
      delim = str(sink_ep.config.get('delimiter', ',') or ',')
      with open(tmp_path, 'w', encoding='utf-8', newline='') as f:
        if out_cols or rows_to_write:
          writer = csv.DictWriter(
              f, fieldnames=out_cols, delimiter=delim, extrasaction='ignore')
          writer.writeheader()
          for r in rows_to_write:
            writer.writerow({
                k: ('' if v is None else json.dumps(
                    _sanitize_json_nan(v), default=str, allow_nan=False)
                    if isinstance(v, (dict, list)) else v)
                for k, v in r.items()
            })
    else:
      # Default JSONL (`json` / `text`).
      with open(tmp_path, 'w', encoding='utf-8') as f:
        for r in rows_to_write:
          f.write(
              json.dumps(_sanitize_json_nan(r), default=str, allow_nan=False) +
              '\n')
  except BaseException:
    if os.path.exists(tmp_path):
      try:
        os.unlink(tmp_path)
      except OSError:
        pass
    raise
  return tmp_path, final_path

