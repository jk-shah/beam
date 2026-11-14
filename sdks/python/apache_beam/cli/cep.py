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

"""Declarative Event Sequence / Pattern Matching CEP (`CliPatternMatch`, `B2.2`).

Implements lightweight Complex Event Processing (`--pattern`) for Apache Beam CLI:
- Supports sequential patterns (`A -> B -> C within 5m`), bounded fixed
  quantifiers (`fail_login{3} -> transfer within 2m`), and trailing negated
  absence detection (`checkout -> !payment within 15m`).
- Supports `mode=relaxed|contiguous` and `after_match=skip_past_last|skip_to_next`.
- Evaluates per-step `where` predicates through a zero-`eval` recursive AST
  interpreter validated by `expressions.compile_where(...)`.
- Uses watermark-ordered `PatternMatchStatefulDoFn` (`BagStateSpec` +
  `ReadModifyWriteStateSpec` + 1ms-aligned `TimeDomain.WATERMARK` `CEP_TIMER`,
  zero `OrderedListStateSpec`) with hard `max_active_matches_per_key` bounds
  (default `64`) and a bounded `GroupByKey` fast path producing identical
  results.
"""

from __future__ import annotations

import ast
from collections.abc import Iterable
from collections.abc import Mapping
from datetime import datetime
from datetime import timezone
import json
import math
import re
from typing import Any
from typing import NamedTuple
from typing import Optional

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.coders import FloatCoder
from apache_beam.coders import StrUtf8Coder
from apache_beam.coders import TupleCoder
from apache_beam.coders import VarIntCoder
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.transforms.userstate import TimeDomain
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import on_timer
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.window import TimestampedValue
from apache_beam.typehints import schemas
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.utils import timestamp as beam_timestamp

from . import enrich as enrich_mod
from . import expressions
from . import interval_join
from . import output

_ALLOWED_MODES = frozenset(['relaxed', 'contiguous'])
_ALLOWED_AFTER_MATCH = frozenset(['skip_past_last', 'skip_to_next'])
_ALLOWED_OVERFLOW_ACTIONS = frozenset(['drop', 'dlq', 'fail'])
_MAX_EXPANDED_STEPS = 16
_MAX_ACTIVE_CAP = 1024

_STEP_TOKEN_RE = re.compile(
    r'^(?P<neg>!)?(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?:\{(?P<quant>\d+)\})?$')
_SEQ_WITHIN_RE = re.compile(
    r'^(?P<seq_body>.+?)\s+within\s+(?P<within>[A-Za-z0-9_.]+)\s*$',
    re.I | re.S)


class PatternStep(NamedTuple):
  base_name: str
  step_alias: str  # e.g. `fail_1`, `fail_2` when quantified, or `login`
  negated: bool = False
  predicate_expr: str | None = None


class PatternConfig(NamedTuple):
  keys: tuple[str, ...]
  steps: tuple[PatternStep, ...]
  within_sec: float
  within_str: str
  raw_seq: str
  event_col: str = 'event_type'
  timestamp_col: str | None = None
  mode: str = 'relaxed'
  after_match: str = 'skip_past_last'
  allowed_lateness: float = 0.0
  allowed_lateness_str: str | None = None
  max_active_per_key: int = 64
  overflow_action: str = 'drop'
  where_map: dict[str, str] | None = None
  select_map: dict[str, str] | None = None
  dlq_redact_fields: tuple[str, ...] = ()
  force_stateful: bool = False


def _format_iso_ts(sec: float) -> str:
  dt = datetime.fromtimestamp(float(sec), tz=timezone.utc)
  if dt.microsecond:
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
  return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def _parse_seq_and_within(
    raw_seq: str, explicit_within: str | None
) -> tuple[list[tuple[str, str, bool]], float, str, str]:
  """Parses `A -> B{2} -> !C [within 5m]` into `(expanded_steps, within_sec, within_str, seq_only)`."""
  s = (raw_seq or '').strip()
  if not s:
    raise ValueError('CliPatternMatch requires a non-empty seq=... pattern')

  inline_within: str | None = None
  mw = _SEQ_WITHIN_RE.match(s)
  if mw:
    s = mw.group('seq_body').strip()
    inline_within = mw.group('within').strip()

  eff_within = (explicit_within or inline_within or '').strip()
  if not eff_within:
    raise ValueError(
        'CliPatternMatch requires a positive horizon via "... within <DURATION>" '
        'or within=<DURATION> (e.g. "A -> B within 5m")')
  within_sec = interval_join.parse_interval_duration(eff_within)
  if not math.isfinite(within_sec) or within_sec <= 0:
    raise ValueError(
        f'CliPatternMatch within duration must be > 0, got {eff_within!r}')

  if '+' in s or '*' in s:
    raise ValueError(
        f'unbounded Kleene quantifiers (+ / *) are not supported in {s!r}; '
        'use a bounded fixed quantifier like step{3} (1..16)')

  raw_parts = [p.strip() for p in s.split('->')]
  if len(raw_parts) < 2 or any(not p for p in raw_parts):
    raise ValueError(
        f'invalid pattern sequence {s!r}; expected at least 2 steps separated '
        'by "->" (e.g. "login -> purchase within 5m")')

  expanded: list[tuple[str, str, bool]] = []
  for idx, tok in enumerate(raw_parts):
    mt = _STEP_TOKEN_RE.match(tok)
    if not mt:
      raise ValueError(
          f'invalid pattern step {tok!r} in {s!r}; expected identifier, '
          'step{N}, or trailing !step')
    neg = bool(mt.group('neg'))
    name = mt.group('name')
    quant = int(mt.group('quant')) if mt.group('quant') is not None else 1
    if quant < 1 or quant > _MAX_EXPANDED_STEPS:
      raise ValueError(
          f'step quantifier {{{quant}}} on {name!r} must be between 1 and '
          f'{_MAX_EXPANDED_STEPS}')
    if neg:
      if idx != len(raw_parts) - 1:
        raise ValueError(
            f'negated absence step !{name} is only supported as the final step '
            f'of a pattern (e.g. "A -> !{name} within {eff_within}")')
      if quant != 1:
        raise ValueError(
            f'negated absence step !{name} cannot use quantifier {{{quant}}}')
      expanded.append((name, name, True))
    elif quant == 1:
      expanded.append((name, name, False))
    else:
      for q_i in range(1, quant + 1):
        expanded.append((name, f'{name}_{q_i}', False))

  if len(expanded) < 2 or len(expanded) > _MAX_EXPANDED_STEPS:
    raise ValueError(
        f'expanded pattern length ({len(expanded)}) must be between 2 and '
        f'{_MAX_EXPANDED_STEPS} steps')
  if expanded[0][2]:
    raise ValueError('pattern cannot start with a negated step (!A)')

  # Disambiguate duplicate step_alias if user wrote `A -> A` manually
  counts: dict[str, int] = {}
  for _, al, _ in expanded:
    counts[al] = counts.get(al, 0) + 1
  seen_dup: dict[str, int] = {}
  final_expanded: list[tuple[str, str, bool]] = []
  for base_n, al, neg in expanded:
    if counts[al] > 1:
      seen_dup[al] = seen_dup.get(al, 0) + 1
      final_expanded.append((base_n, f'{al}_{seen_dup[al]}', neg))
    else:
      final_expanded.append((base_n, al, neg))

  return final_expanded, float(within_sec), eff_within, s


def _parse_where_spec(raw: Any) -> dict[str, str] | None:
  if raw is None:
    return None
  out: dict[str, str] = {}
  if isinstance(raw, Mapping):
    for k, v in raw.items():
      ks = str(k).strip()
      if not output.SAFE_IDENTIFIER_RE.match(ks):
        raise ValueError(f'invalid step name {ks!r} in pattern where')
      out[ks] = str(v).strip()
    return out or None
  s = str(raw).strip()
  if s.startswith('[') and s.endswith(']'):
    s = s[1:-1].strip()
  if not s:
    return None
  for item in interval_join.split_bracket_tokens(s):
    tok = item.strip()
    if not tok:
      continue
    if ':' not in tok:
      raise ValueError(
          f'invalid pattern where item {tok!r}; expected "step_name: <expr>"')
    st_name, expr_s = tok.split(':', 1)
    st_clean = st_name.strip()
    if not output.SAFE_IDENTIFIER_RE.match(st_clean):
      raise ValueError(f'invalid step name {st_clean!r} in pattern where')
    out[st_clean] = expr_s.strip()
  return out or None


def _parse_select_spec(raw: Any) -> dict[str, str] | None:
  if raw is None:
    return None
  if isinstance(raw, Mapping):
    out = {}
    for k, v in raw.items():
      ks = str(k).strip()
      if not output.SAFE_IDENTIFIER_RE.match(ks):
        raise ValueError(f'invalid select column {ks!r} in CliPatternMatch')
      out[ks] = str(v).strip()
    return out or None
  s = str(raw).strip()
  if s.startswith('[') and s.endswith(']'):
    s = s[1:-1].strip()
  if not s:
    return None
  out = {}
  for item in interval_join.split_bracket_tokens(s):
    tok = item.strip()
    if not tok:
      continue
    parts = tok.rsplit(' AS ', 1)
    if len(parts) == 1:
      parts = tok.rsplit(' as ', 1)
    if len(parts) == 2:
      expr_s, alias_s = parts[0].strip(), parts[1].strip()
    elif '=' in tok and '==' not in tok:
      alias_s, expr_s = [x.strip() for x in tok.split('=', 1)]
    else:
      expr_s = tok
      alias_s = tok.replace('.', '_').strip()
    if not output.SAFE_IDENTIFIER_RE.match(alias_s):
      raise ValueError(
          f'pattern select item {tok!r} requires a valid identifier alias')
    out[alias_s] = expr_s
  return out or None


class _CompiledPredicate:
  """Zero-`eval` recursive AST evaluator validated via `expressions.compile_where`."""
  def __init__(self, expr_str: str):
    # First run full CLI expression security & complexity validation
    expressions.compile_where(
        expr_str, known_columns=None, flag_name='pattern.where')
    normalized, colref_map = expressions._lex_and_normalize_with_colmap(
        expr_str)
    self._tree = ast.parse(normalized, mode='eval').body
    self._colref_map = colref_map

  def matches(self, row_dict: Mapping[str, Any]) -> bool:
    try:
      val = self._eval_node(self._tree, row_dict)
      return bool(val) if val is not None else False
    except Exception:  # pylint: disable=broad-except
      return False

  def _resolve_path(self, obj: Any, attr: str) -> Any:
    if obj is None:
      return None
    if isinstance(obj, Mapping):
      return obj.get(attr)
    return getattr(obj, attr, None)

  def _eval_node(self, node: ast.AST, row_dict: Mapping[str, Any]) -> Any:
    if isinstance(node, ast.Constant):
      return node.value
    if isinstance(node, ast.Name):
      col = self._colref_map.get(node.id, node.id)
      return row_dict.get(col)
    if isinstance(node, ast.Attribute):
      base = self._eval_node(node.value, row_dict)
      return self._resolve_path(base, node.attr)
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
      return tuple(self._eval_node(el, row_dict) for el in node.elts)
    if isinstance(node, ast.UnaryOp):
      v = self._eval_node(node.operand, row_dict)
      if isinstance(node.op, ast.Not):
        return None if v is None else (not bool(v))
      if v is None:
        return None
      if isinstance(node.op, ast.USub):
        return -v
      if isinstance(node.op, ast.UAdd):
        return +v
      raise ValueError('unsupported unary op')
    if isinstance(node, ast.BoolOp):
      if isinstance(node.op, ast.And):
        saw_null = False
        for sub in node.values:
          sv = self._eval_node(sub, row_dict)
          if sv is False:
            return False
          if sv is None:
            saw_null = True
          elif not bool(sv):
            return False
        return None if saw_null else True
      if isinstance(node.op, ast.Or):
        saw_null = False
        for sub in node.values:
          sv = self._eval_node(sub, row_dict)
          if sv is True or (sv is not None and bool(sv)):
            return True
          if sv is None:
            saw_null = True
        return None if saw_null else False
    if isinstance(node, ast.BinOp):
      lv = self._eval_node(node.left, row_dict)
      rv = self._eval_node(node.right, row_dict)
      if lv is None or rv is None:
        return None
      if isinstance(node.op, ast.Add):
        return lv + rv
      if isinstance(node.op, ast.Sub):
        return lv - rv
      if isinstance(node.op, ast.Mult):
        if isinstance(lv, (str, bytes, list, tuple)) or isinstance(
            rv, (str, bytes, list, tuple)):
          raise ValueError('sequence repetition disallowed')
        return lv * rv
      if isinstance(node.op, ast.Div):
        return lv / rv
      if isinstance(node.op, ast.FloorDiv):
        return lv // rv
      if isinstance(node.op, ast.Mod):
        return lv % rv
      raise ValueError('unsupported binary op')
    if isinstance(node, ast.Compare):
      left_v = self._eval_node(node.left, row_dict)
      for op, comp in zip(node.ops, node.comparators):
        right_v = self._eval_node(comp, row_dict)
        if isinstance(op, ast.Is):
          ok = left_v is right_v
        elif isinstance(op, ast.IsNot):
          ok = left_v is not right_v
        elif isinstance(op, (ast.In, ast.NotIn)):
          if left_v is None or right_v is None:
            return None
          seq = right_v if isinstance(right_v, (list, tuple, set,
                                                frozenset)) else (right_v, )
          has_match = any(x is not None and left_v == x for x in seq)
          has_null = any(x is None for x in seq)
          if isinstance(op, ast.In):
            if has_match:
              ok = True
            elif has_null:
              return None
            else:
              ok = False
          else:
            if has_match:
              ok = False
            elif has_null:
              return None
            else:
              ok = True
        else:
          if left_v is None or right_v is None:
            return None
          if isinstance(op, ast.Eq):
            ok = left_v == right_v
          elif isinstance(op, ast.NotEq):
            ok = left_v != right_v
          elif isinstance(op, ast.Lt):
            ok = left_v < right_v
          elif isinstance(op, ast.LtE):
            ok = left_v <= right_v
          elif isinstance(op, ast.Gt):
            ok = left_v > right_v
          elif isinstance(op, ast.GtE):
            ok = left_v >= right_v
          else:
            raise ValueError('unsupported comparison op')
        if not ok:
          return False
        left_v = right_v
      return True
    if isinstance(node, ast.Call):
      args = [self._eval_node(a, row_dict) for a in node.args]
      if isinstance(node.func, ast.Name):
        fn = node.func.id.lower()
        if fn in ('coalesce', 'ifnull'):
          for a in args:
            if a is not None:
              return a
          return None
        if any(a is None for a in args):
          return None
        if fn == 'lower':
          return str(args[0]).lower()
        if fn == 'upper':
          return str(args[0]).upper()
        if fn in ('strip', 'trim'):
          return str(args[0]).strip()
        if fn in ('len', 'length'):
          return len(
              str(args[0]) if not hasattr(args[0], '__len__') else args[0])
        if fn == 'abs':
          return abs(args[0])
        if fn == 'round':
          return round(args[0], int(args[1])) if len(args) > 1 else round(
              args[0])
        if fn == 'int':
          return int(args[0])
        if fn == 'float':
          return float(args[0])
        if fn == 'str':
          return str(args[0])
        if fn == 'bool':
          return bool(args[0])
        if fn == 'min':
          return min(args)
        if fn == 'max':
          return max(args)
      elif isinstance(node.func, ast.Attribute):
        target = self._eval_node(node.func.value, row_dict)
        if target is None or any(a is None for a in args):
          return None
        mname = node.func.attr
        s_t = str(target)
        if mname == 'lower':
          return s_t.lower()
        if mname == 'upper':
          return s_t.upper()
        if mname == 'strip':
          return s_t.strip()
        if mname == 'startswith':
          return s_t.startswith(str(args[0]))
        if mname == 'endswith':
          return s_t.endswith(str(args[0]))
    raise ValueError(f'unsupported AST node {type(node).__name__}')


def parse_pattern_spec_str(
    spec_str: str,
    *,
    timestamp_col: str | None = None,
    allowed_lateness: str | None = None,
    dlq_redact_fields: Iterable[str] | None = None,
) -> dict[str, Any]:
  """Parses `--pattern "key=user_id,seq=A -> B within 5m,..."` into YAML dict."""
  s = (spec_str or '').strip()
  if not s:
    raise ValueError('--pattern specification cannot be empty')
  if s.startswith('{') and s.endswith('}'):
    try:
      raw_dict = json.loads(s)
    except Exception as e:
      raise ValueError(f'malformed JSON in --pattern: {e}') from None
    if not isinstance(raw_dict, dict):
      raise ValueError('--pattern JSON must be an object')
  else:
    raw_dict = {}
    for tok in interval_join.split_bracket_tokens(s):
      if '=' not in tok:
        raise ValueError(
            f'invalid token {tok!r} in --pattern; expected key=value '
            '(e.g. "key=user_id,seq=login -> purchase within 5m")')
      k, v = tok.split('=', 1)
      ks = k.strip().lower().replace('-', '_')
      vs = v.strip()
      if len(vs) >= 2 and vs[0] == vs[-1] and vs[0] in ("'", '"'):
        vs = vs[1:-1]
      raw_dict[ks] = vs

  if timestamp_col and 'timestamp_col' not in raw_dict:
    raw_dict['timestamp_col'] = timestamp_col
  if allowed_lateness and 'allowed_lateness' not in raw_dict:
    raw_dict['allowed_lateness'] = allowed_lateness
  if dlq_redact_fields and 'dlq_redact_fields' not in raw_dict:
    raw_dict['dlq_redact_fields'] = list(dlq_redact_fields)

  parsed = parse_pattern_config(raw_dict)
  return pattern_config_to_yaml_dict(parsed)


def parse_pattern_config(raw: Mapping[str, Any]) -> PatternConfig:
  """Validates and normalizes `CliPatternMatch` config."""
  if not isinstance(raw, Mapping):
    raise ValueError('CliPatternMatch config must be a mapping')
  norm = {str(k).strip().lower().replace('-', '_'): v for k, v in raw.items()}
  allowed = {
      'key',
      'keys',
      'seq',
      'pattern',
      'within',
      'event_col',
      'timestamp_col',
      'time',
      'mode',
      'contiguity',
      'after_match',
      'allowed_lateness',
      'max_active',
      'max_active_matches_per_key',
      'overflow_action',
      'where',
      'select',
      'dlq_redact_fields',
      'force_stateful',
      'error_handling',
  }
  unknown = [k for k in norm if k not in allowed]
  if unknown:
    raise ValueError(f'unknown option(s) {sorted(unknown)} in CliPatternMatch')

  keys = interval_join.parse_column_list(
      norm.get('key') if norm.get('key') is not None else norm.get('keys'))
  if not keys:
    raise ValueError(
        'CliPatternMatch requires partition key column(s) via key=...')
  for col in keys:
    if not output.SAFE_IDENTIFIER_RE.match(col):
      raise ValueError(f'invalid key column {col!r} in CliPatternMatch')

  raw_seq = str(norm.get('seq') or norm.get('pattern') or '').strip()
  raw_within = str(norm['within']).strip() if norm.get('within') else None
  expanded_tuples, within_sec, within_str, seq_clean = _parse_seq_and_within(
      raw_seq, raw_within)

  ev_col = str(norm.get('event_col') or 'event_type').strip()
  if not output.SAFE_IDENTIFIER_RE.match(ev_col):
    raise ValueError(f'invalid event_col {ev_col!r} in CliPatternMatch')

  ts_raw = norm.get('timestamp_col') or norm.get('time')
  ts_col = str(ts_raw).strip() if ts_raw else None
  if ts_col is not None and not output.SAFE_IDENTIFIER_RE.match(ts_col):
    raise ValueError(f'invalid timestamp_col {ts_col!r} in CliPatternMatch')

  mode = str(norm.get('mode') or norm.get('contiguity') or
             'relaxed').strip().lower()
  if mode not in _ALLOWED_MODES:
    raise ValueError(
        f'invalid mode {mode!r} in CliPatternMatch; expected relaxed or contiguous'
    )

  after_match = str(norm.get('after_match') or 'skip_past_last').strip().lower()
  if after_match not in _ALLOWED_AFTER_MATCH:
    raise ValueError(
        f'invalid after_match {after_match!r} in CliPatternMatch; expected '
        'skip_past_last or skip_to_next')

  lat_raw = norm.get('allowed_lateness')
  lat_sec = 0.0
  lat_str: str | None = None
  if lat_raw is not None and str(lat_raw).strip() != '':
    lat_str = str(lat_raw).strip()
    lat_sec = interval_join.parse_interval_duration(lat_str)
    if lat_sec < 0:
      raise ValueError(f'allowed_lateness must be >= 0, got {lat_raw!r}')

  max_act_raw = (
      norm['max_active_matches_per_key']
      if 'max_active_matches_per_key' in norm else norm.get('max_active', 64))
  max_act = int(max_act_raw)
  if max_act < 1 or max_act > _MAX_ACTIVE_CAP:
    raise ValueError(
        f'max_active_matches_per_key must be in 1..{_MAX_ACTIVE_CAP}, got {max_act}'
    )

  ovf_act = str(norm.get('overflow_action') or 'drop').strip().lower()
  if ovf_act not in _ALLOWED_OVERFLOW_ACTIONS:
    raise ValueError(
        f'invalid overflow_action {ovf_act!r}; expected drop, dlq, or fail')

  where_map = _parse_where_spec(norm.get('where'))
  valid_step_names = {b
                      for b, _, _ in expanded_tuples
                      } | {al
                           for _, al, _ in expanded_tuples}
  if where_map:
    for st_k, pred_s in where_map.items():
      if st_k not in valid_step_names:
        raise ValueError(
            f'where predicate step {st_k!r} does not match any step in '
            f'{sorted(valid_step_names)}')
      try:
        _CompiledPredicate(pred_s)
      except expressions.ExpressionError as e:
        raise ValueError(
            f'invalid pattern where predicate for {st_k!r}: {e}') from None

  steps = tuple(
      PatternStep(
          base_name=b,
          step_alias=al,
          negated=neg,
          predicate_expr=((
              where_map.get(al) or where_map.get(b)) if where_map else None),
      ) for b, al, neg in expanded_tuples)

  select_map = _parse_select_spec(norm.get('select'))
  if select_map:
    builtin_meta = {
        'pattern_start_ts',
        'pattern_end_ts',
        'matched_steps',
        'events_json',
        *keys,
    }
    for out_col, src_ref in select_map.items():
      if '.' in src_ref:
        st_ref, col_ref = [x.strip() for x in src_ref.split('.', 1)]
        if st_ref not in valid_step_names:
          raise ValueError(
              f'select reference {src_ref!r} for {out_col!r} names unknown '
              f'step {st_ref!r} (valid steps: {sorted(valid_step_names)})')
        if not output.SAFE_IDENTIFIER_RE.match(col_ref):
          raise ValueError(
              f'invalid column name {col_ref!r} in pattern select {src_ref!r}')
      elif src_ref not in builtin_meta:
        raise ValueError(
            f'unqualified pattern select {src_ref!r} must be one of '
            f'{sorted(builtin_meta)} or <step>.<column>')

  dlq_redact: tuple[str, ...] = ()
  if norm.get('dlq_redact_fields'):
    dlq_redact = enrich_mod._validate_ident_list(
        interval_join.parse_column_list(norm['dlq_redact_fields']),
        'dlq_redact_fields')

  force_st = str(norm.get('force_stateful', False)).strip().lower() in (
      '1',
      'true',
      'yes',
  ) if not isinstance(norm.get('force_stateful'), bool) else bool(
      norm.get('force_stateful'))

  return PatternConfig(
      keys=keys,
      steps=steps,
      within_sec=within_sec,
      within_str=within_str,
      raw_seq=f'{seq_clean} within {within_str}',
      event_col=ev_col,
      timestamp_col=ts_col,
      mode=mode,
      after_match=after_match,
      allowed_lateness=lat_sec,
      allowed_lateness_str=lat_str if lat_sec > 0 else None,
      max_active_per_key=max_act,
      overflow_action=ovf_act,
      where_map=where_map,
      select_map=select_map,
      dlq_redact_fields=dlq_redact,
      force_stateful=force_st,
  )


def pattern_config_to_yaml_dict(cfg: PatternConfig) -> dict[str, Any]:
  out: dict[str, Any] = {
      'key': cfg.keys[0] if len(cfg.keys) == 1 else list(cfg.keys),
      'seq': cfg.raw_seq,
      'event_col': cfg.event_col,
      'mode': cfg.mode,
      'after_match': cfg.after_match,
      'max_active_matches_per_key': cfg.max_active_per_key,
      'overflow_action': cfg.overflow_action,
  }
  if cfg.timestamp_col:
    out['timestamp_col'] = cfg.timestamp_col
  if cfg.allowed_lateness_str:
    out['allowed_lateness'] = cfg.allowed_lateness_str
  elif cfg.allowed_lateness > 0:
    out['allowed_lateness'] = f'{cfg.allowed_lateness:g}s'
  if cfg.where_map:
    out['where'] = dict(cfg.where_map)
  if cfg.select_map:
    out['select'] = dict(cfg.select_map)
  if cfg.dlq_redact_fields:
    out['dlq_redact_fields'] = list(cfg.dlq_redact_fields)
  if cfg.force_stateful:
    out['force_stateful'] = True
  return out


def _make_cep_dlq_row(
    err_type: str,
    msg: str,
    elem: Any,
    redact_fields: tuple[str, ...] = (),
) -> beam.Row:
  try:
    plain = interval_join._to_plain_dict(elem)
    if isinstance(plain, dict) and redact_fields:
      plain = {
          k: ('<REDACTED>' if k in redact_fields else v)
          for k, v in plain.items()
      }
    raw_json = json.dumps(plain, default=str, sort_keys=True)
  except Exception:  # pylint: disable=broad-except
    raw_json = '<unserializable_row>'
  return beam.Row(
      step='CliPatternMatch',
      error_type=err_type,
      error=msg[:4096],
      raw_payload=raw_json,
      element=raw_json,
  )


def _compile_step_matchers(
    cfg: PatternConfig,
) -> list[tuple[PatternStep, _CompiledPredicate | None]]:
  out: list[tuple[PatternStep, _CompiledPredicate | None]] = []
  for st in cfg.steps:
    pred = _CompiledPredicate(st.predicate_expr) if st.predicate_expr else None
    out.append((st, pred))
  return out


def _step_matches_event(
    step: PatternStep,
    pred: _CompiledPredicate | None,
    row_dict: Mapping[str, Any],
    event_col: str,
) -> bool:
  if pred is not None:
    return pred.matches(row_dict)
  val = row_dict.get(event_col)
  return val is not None and str(val) == step.base_name


def _project_match_row(
    cfg: PatternConfig,
    matched_events: list[tuple[str, str, float, dict[str, Any]]],
    end_ts: float,
) -> dict[str, Any]:
  """Projects a completed match into a concrete flat dict matching `_infer_cep_row_type`."""
  start_ts = matched_events[0][2]
  first_ev = matched_events[0][3]
  step_labels: list[str] = [al for _, al, _, _ in matched_events]
  if cfg.steps[-1].negated:
    step_labels.append(f'!{cfg.steps[-1].base_name}')

  by_step: dict[str, dict[str, Any]] = {}
  for base_n, alias_n, _, ev_d in matched_events:
    by_step[alias_n] = ev_d
    by_step[base_n] = ev_d

  meta: dict[str, Any] = {
      'pattern_start_ts': _format_iso_ts(start_ts),
      'pattern_end_ts': _format_iso_ts(end_ts),
      'matched_steps': ','.join(step_labels),
      'events_json': json.dumps([ev_d for _, _, _, ev_d in matched_events],
                                default=str,
                                sort_keys=True),
  }
  for k in cfg.keys:
    meta[k] = first_ev.get(k)

  if not cfg.select_map:
    out: dict[str, Any] = {}
    for k in cfg.keys:
      out[k] = first_ev.get(k)
    out['pattern_start_ts'] = meta['pattern_start_ts']
    out['pattern_end_ts'] = meta['pattern_end_ts']
    out['matched_steps'] = meta['matched_steps']
    out['events_json'] = meta['events_json']
    return out

  out = {}
  for out_col, src_ref in cfg.select_map.items():
    if '.' in src_ref:
      st_ref, col_ref = [x.strip() for x in src_ref.split('.', 1)]
      st_dict = by_step.get(st_ref)
      out[out_col] = st_dict.get(col_ref) if st_dict is not None else None
    else:
      out[out_col] = meta.get(src_ref)
  return out


def _advance_nfa_runs(
    cfg: PatternConfig,
    compiled_steps: list[tuple[PatternStep, _CompiledPredicate | None]],
    active_runs: list[dict[str, Any]],
    ready_events: list[tuple[float, int, dict[str, Any]]],
    horizon_sec: float | None,
) -> tuple[
    list[dict[str, Any]],
    list[tuple[float, dict[str, Any]]],
    int,
    int,
    list[dict[str, Any]],
]:
  """Advances NFA state over sorted `ready_events` and watermark `horizon_sec`.

  Returns `(surviving_runs, emitted_matches, expired_count, overflow_count, overflow_rows)`.
  Each `active_run` dict has:
    - `start_ts`: float
    - `start_seq`: int
    - `last_seq`: int
    - `next_idx`: int  (index into `cfg.steps` currently awaited)
    - `events`: list of `[base_name, step_alias, ts, row_dict]`
  """
  emitted: list[tuple[float, dict[str, Any]]] = []
  expired_cnt = 0
  overflow_cnt = 0
  overflow_rows: list[dict[str, Any]] = []
  num_steps = len(compiled_steps)
  has_trailing_neg = compiled_steps[-1][0].negated

  def _flush_horizon_up_to(cutoff_ts: float,
                           runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Resolves timeouts (`< cutoff_ts`) and `!absence` completions (`<= cutoff_ts`)."""
    nonlocal expired_cnt
    kept: list[dict[str, Any]] = []
    consumed_through_seq: int | None = None
    # Sort runs by `(start_ts, start_seq)` so earliest non-overlapping match wins!
    runs.sort(key=lambda r: (r['start_ts'], r['start_seq']))
    for run in runs:
      if (cfg.after_match == 'skip_past_last' and
          consumed_through_seq is not None and
          run['start_seq'] <= consumed_through_seq):
        continue
      deadline = run['start_ts'] + cfg.within_sec
      nxt_i = run['next_idx']
      st_obj, _ = compiled_steps[nxt_i]
      if st_obj.negated:
        # Absence `!S_m`: if clock passed `deadline`, `!S_m` SUCCEEDED!
        if cutoff_ts >= deadline - 1e-9:
          ev_tuples = [(b, al, float(ts_e), d)
                       for b, al, ts_e, d in run['events']]
          emitted.append(
              (deadline, _project_match_row(cfg, ev_tuples, deadline)))
          if cfg.after_match == 'skip_past_last':
            consumed_through_seq = (
                run['last_seq'] if consumed_through_seq is None else max(
                    consumed_through_seq, run['last_seq']))
          continue
        kept.append(run)
      else:
        if cutoff_ts > deadline + 1e-9:
          expired_cnt += 1
          continue
        kept.append(run)
    if cfg.after_match == 'skip_past_last' and consumed_through_seq is not None:
      kept = [r for r in kept if r['start_seq'] > consumed_through_seq]
    return kept

  runs = list(active_runs)
  for ev_ts, ev_seq, ev_dict in ready_events:
    # Before processing `ev_ts`, resolve any runs whose window already closed before `ev_ts`!
    # Note: for `!S_m`, if `ev_ts > start_ts + within_sec`, the absence window `[start, start+within]`
    # ALREADY elapsed strictly before `ev_ts`, so an `S_m` at `ev_ts > deadline` does NOT cancel it!
    next_runs: list[dict[str, Any]] = []
    consumed_up_to_seq: int | None = None
    event_consumed_by_completion = False

    runs.sort(key=lambda r: (r['start_ts'], r['start_seq']))
    for run in runs:
      if (cfg.after_match == 'skip_past_last' and
          consumed_up_to_seq is not None and
          run['start_seq'] <= consumed_up_to_seq):
        continue
      deadline = run['start_ts'] + cfg.within_sec
      nxt_i = run['next_idx']
      st_obj, st_pred = compiled_steps[nxt_i]

      if st_obj.negated:
        if ev_ts > deadline + 1e-9:
          # Window `[start_ts, deadline]` completed cleanly before `ev_ts`!
          ev_tuples = [(b, al, float(ts_e), d)
                       for b, al, ts_e, d in run['events']]
          emitted.append(
              (deadline, _project_match_row(cfg, ev_tuples, deadline)))
          if cfg.after_match == 'skip_past_last':
            consumed_up_to_seq = (
                run['last_seq'] if consumed_up_to_seq is None else max(
                    consumed_up_to_seq, run['last_seq']))
          continue
        # Within `[start_ts, deadline]`: if `ev_dict` matches forbidden `S_m`, cancel run!
        if _step_matches_event(st_obj, st_pred, ev_dict, cfg.event_col):
          expired_cnt += 1
          continue
        next_runs.append(run)
        continue

      # Positive step `S_{nxt_i}`:
      if ev_ts > deadline + 1e-9:
        expired_cnt += 1
        continue

      if _step_matches_event(st_obj, st_pred, ev_dict, cfg.event_col):
        if cfg.mode == 'relaxed':
          # Branch: under `skip_to_next`, keep waiting copy too? Standard NFA:
          # greedily advance run on first matching step (`A -> B` matches earliest `B`).
          pass
        new_events = list(run['events']) + [[
            st_obj.base_name, st_obj.step_alias, ev_ts, ev_dict
        ]]
        new_idx = nxt_i + 1
        if new_idx == num_steps:
          ev_tuples = [(b, al, float(ts_e), d) for b, al, ts_e, d in new_events]
          emitted.append((ev_ts, _project_match_row(cfg, ev_tuples, ev_ts)))
          if cfg.after_match == 'skip_past_last':
            consumed_up_to_seq = (
                ev_seq if consumed_up_to_seq is None else max(
                    consumed_up_to_seq, ev_seq))
            event_consumed_by_completion = True
        else:
          # Either advancing to next positive step OR entering trailing `!S_m` wait state!
          next_runs.append({
              'start_ts': run['start_ts'],
              'start_seq': run['start_seq'],
              'last_seq': ev_seq,
              'next_idx': new_idx,
              'events': new_events,
          })
      else:
        if cfg.mode == 'contiguous':
          expired_cnt += 1
        else:
          next_runs.append(run)

    if cfg.after_match == 'skip_past_last' and consumed_up_to_seq is not None:
      next_runs = [r for r in next_runs if r['start_seq'] > consumed_up_to_seq]

    # Check if `ev_dict` can also start a fresh `S_0` run!
    can_start = not (
        cfg.after_match == 'skip_past_last' and event_consumed_by_completion)
    s0_step, s0_pred = compiled_steps[0]
    if can_start and _step_matches_event(
        s0_step, s0_pred, ev_dict, cfg.event_col):
      if len(next_runs) >= cfg.max_active_per_key:
        overflow_cnt += 1
        overflow_rows.append(ev_dict)
      else:
        next_runs.append({
            'start_ts': ev_ts,
            'start_seq': ev_seq,
            'last_seq': ev_seq,
            'next_idx': 1,
            'events': [[s0_step.base_name, s0_step.step_alias, ev_ts, ev_dict]],
        })

    runs = next_runs

  if horizon_sec is not None and runs:
    runs = _flush_horizon_up_to(horizon_sec, runs)

  # Suppress unused lint on `has_trailing_neg` (used via `compiled_steps[nxt_i]`)
  del has_trailing_neg
  return runs, emitted, expired_cnt, overflow_cnt, overflow_rows


def match_bounded_patterns(
    records: Iterable[Any],
    config: PatternConfig) -> tuple[list[dict[str, Any]], int, int, int]:
  """Deterministic in-memory CEP evaluation over bounded records.

  Returns `(matched_rows, matched_cnt, expired_cnt, overflow_cnt)`.
  """
  compiled = _compile_step_matchers(config)
  by_key: dict[bytes, list[tuple[float, int, dict[str, Any]]]] = {}
  for seq_i, rec in enumerate(records):
    d = interval_join._to_plain_dict(rec)
    k_bytes = interval_join.encode_canonical_key_values(
        tuple(d.get(k) for k in config.keys))
    t = interval_join.extract_record_timestamp(d, config.timestamp_col)
    by_key.setdefault(k_bytes, []).append((t, seq_i, d))

  all_matches: list[dict[str, Any]] = []
  total_exp = 0
  total_ovf = 0
  for _, ev_list in by_key.items():
    ev_list.sort(key=lambda it: (it[0], it[1]))
    _, emitted, exp_c, ovf_c, _ = _advance_nfa_runs(
        config, compiled, [], ev_list, horizon_sec=float('inf'))
    for _, m_row in emitted:
      all_matches.append(m_row)
    total_exp += exp_c
    total_ovf += ovf_c

  return all_matches, len(all_matches), total_exp, total_ovf


def _infer_cep_row_type(
    in_element_type: Any, cfg: PatternConfig) -> RowTypeConstraint:
  in_map: dict[str, Any] = {}
  if in_element_type is not None:
    try:
      in_map = dict(schemas.named_fields_from_element_type(in_element_type))
    except Exception:  # pylint: disable=broad-except
      in_map = {}

  if not cfg.select_map:
    fields: list[tuple[str, Any]] = []
    for k in cfg.keys:
      fields.append((k, interval_join._make_optional_type(in_map.get(k, str))))
    fields.extend([
        ('pattern_start_ts', str),
        ('pattern_end_ts', str),
        ('matched_steps', str),
        ('events_json', str),
    ])
    return RowTypeConstraint.from_fields(fields)

  fields = []
  for out_col, src_ref in cfg.select_map.items():
    if '.' in src_ref:
      _, col_ref = [x.strip() for x in src_ref.split('.', 1)]
      fields.append((
          out_col, interval_join._make_optional_type(in_map.get(col_ref, str))))
    elif src_ref in ('pattern_start_ts',
                     'pattern_end_ts',
                     'matched_steps',
                     'events_json'):
      fields.append((out_col, str))
    elif src_ref in in_map:
      fields.append(
          (out_col, interval_join._make_optional_type(in_map[src_ref])))
    else:
      fields.append((out_col, Optional[str]))
  return RowTypeConstraint.from_fields(fields)


_PENDING_EV_CODER = TupleCoder((FloatCoder(), VarIntCoder(), StrUtf8Coder()))


class PatternMatchStatefulDoFn(beam.DoFn):
  """Watermark-ordered stateful CEP engine with 1ms-aligned `CEP_TIMER`."""

  PENDING_EVENTS = BagStateSpec('pending_events', _PENDING_EV_CODER)
  ACTIVE_RUNS_JSON = ReadModifyWriteStateSpec(
      'active_runs_json', StrUtf8Coder())
  SEQ_COUNTER = ReadModifyWriteStateSpec('seq_counter', VarIntCoder())
  NEXT_TIMER_MS = ReadModifyWriteStateSpec('next_timer_ms', VarIntCoder())
  CEP_TIMER = TimerSpec('cep_timer', TimeDomain.WATERMARK)

  def __init__(
      self,
      config: PatternConfig,
      err_tag: str = 'errors',
      route_errors: bool = False,
  ):
    self.config = config
    self.err_tag = err_tag
    self.route_errors = route_errors
    self._compiled_steps: list[tuple[PatternStep,
                                     _CompiledPredicate | None]] | None = None
    self.matched_ctr = beam.metrics.Metrics.counter(
        'beam_cli', 'patterns_matched')
    self.expired_ctr = beam.metrics.Metrics.counter(
        'beam_cli', 'patterns_expired')
    self.overflow_ctr = beam.metrics.Metrics.counter(
        'beam_cli', 'pattern_overflow_dropped')

  def setup(self):
    self._compiled_steps = _compile_step_matchers(self.config)

  def _ensure_compiled(self):
    if self._compiled_steps is None:
      self._compiled_steps = _compile_step_matchers(self.config)
    return self._compiled_steps

  def process(
      self,
      element,
      pending_ev=beam.DoFn.StateParam(PENDING_EVENTS),
      seq_ctr=beam.DoFn.StateParam(SEQ_COUNTER),
      next_timer=beam.DoFn.StateParam(NEXT_TIMER_MS),
      cep_timer=beam.DoFn.TimerParam(CEP_TIMER),
  ):
    _, (t_e, row_dict) = element
    cur_seq = (seq_ctr.read() or 0) + 1
    seq_ctr.write(cur_seq)

    ev_json = interval_join._encode_state_payload(row_dict)
    pending_ev.add((float(t_e), cur_seq, ev_json))

    eval_ms = int(
        math.ceil((float(t_e) + self.config.allowed_lateness) * 1000.0))
    cur_next = next_timer.read()
    if cur_next is None or eval_ms < cur_next:
      next_timer.write(eval_ms)
      cep_timer.set(beam_timestamp.Timestamp(micros=eval_ms * 1000))

  @on_timer(CEP_TIMER)
  def on_cep_timer(
      self,
      timestamp=beam.DoFn.TimestampParam,
      pending_ev=beam.DoFn.StateParam(PENDING_EVENTS),
      active_json=beam.DoFn.StateParam(ACTIVE_RUNS_JSON),
      seq_ctr=beam.DoFn.StateParam(SEQ_COUNTER),
      next_timer=beam.DoFn.StateParam(NEXT_TIMER_MS),
      cep_timer=beam.DoFn.TimerParam(CEP_TIMER),
  ):
    compiled = self._ensure_compiled()
    fired_ms = int(timestamp.micros) // 1000
    fired_sec = fired_ms / 1000.0
    horizon_sec = fired_sec - self.config.allowed_lateness

    raw_pending = list(pending_ev.read())
    ready_raw = [
        it for it in raw_pending
        if int(math.ceil((it[0] + self.config.allowed_lateness) *
                         1000.0)) <= fired_ms
    ]
    waiting_raw = [
        it for it in raw_pending
        if int(math.ceil((it[0] + self.config.allowed_lateness) *
                         1000.0)) > fired_ms
    ]
    ready_raw.sort(key=lambda it: (it[0], it[1]))
    ready_decoded = [(it[0], it[1], interval_join._decode_state_payload(it[2]))
                     for it in ready_raw]

    raw_runs_s = active_json.read()
    cur_runs: list[dict[str,
                        Any]] = json.loads(raw_runs_s) if raw_runs_s else []

    surviving_runs, emitted, exp_c, ovf_c, ovf_rows = _advance_nfa_runs(
        self.config, compiled, cur_runs, ready_decoded, horizon_sec=horizon_sec)

    if emitted:
      self.matched_ctr.inc(len(emitted))
      for m_ts, m_dict in emitted:
        yield TimestampedValue(
            beam.Row(**m_dict), beam_timestamp.Timestamp.of(m_ts))
    if exp_c:
      self.expired_ctr.inc(exp_c)
    if ovf_c:
      if self.config.overflow_action == 'fail':
        raise ValueError(
            f'CliPatternMatch exceeded max_active_matches_per_key='
            f'{self.config.max_active_per_key}')
      if self.config.overflow_action == 'dlq' and self.route_errors:
        for ov_r in ovf_rows:
          yield pvalue.TaggedOutput(
              self.err_tag,
              _make_cep_dlq_row(
                  'PatternStateOverflowError',
                  f'exceeded max_active_matches_per_key={self.config.max_active_per_key}',
                  ov_r,
                  redact_fields=self.config.dlq_redact_fields))
      else:
        self.overflow_ctr.inc(ovf_c)

    pending_ev.clear()
    for w_it in waiting_raw:
      pending_ev.add(w_it)

    next_candidates: list[int] = [
        int(math.ceil((it[0] + self.config.allowed_lateness) * 1000.0))
        for it in waiting_raw
    ]
    for r in surviving_runs:
      # Schedule timeout / `!absence` completion check `+ 1ms` past `start + within + lateness`
      r_deadline_sec = (
          float(r['start_ts']) + self.config.within_sec +
          self.config.allowed_lateness + 0.001)
      r_ms = max(fired_ms + 1, int(math.ceil(r_deadline_sec * 1000.0)))
      next_candidates.append(r_ms)

    if surviving_runs:
      active_json.write(json.dumps(surviving_runs, default=str))
    else:
      active_json.clear()

    if next_candidates:
      nxt = min(next_candidates)
      next_timer.write(nxt)
      cep_timer.set(beam_timestamp.Timestamp(micros=nxt * 1000))
    else:
      next_timer.clear()
      if not surviving_runs:
        seq_ctr.clear()


class _BoundedPatternMatchDoFn(beam.DoFn):
  """Bounded `GroupByKey` fast path sharing the exact `_advance_nfa_runs` NFA."""
  def __init__(
      self,
      config: PatternConfig,
      err_tag: str = 'errors',
      route_errors: bool = False,
  ):
    self.config = config
    self.err_tag = err_tag
    self.route_errors = route_errors
    self._compiled_steps: list[tuple[PatternStep,
                                     _CompiledPredicate | None]] | None = None
    self.matched_ctr = beam.metrics.Metrics.counter(
        'beam_cli', 'patterns_matched')
    self.expired_ctr = beam.metrics.Metrics.counter(
        'beam_cli', 'patterns_expired')
    self.overflow_ctr = beam.metrics.Metrics.counter(
        'beam_cli', 'pattern_overflow_dropped')

  def setup(self):
    self._compiled_steps = _compile_step_matchers(self.config)

  def process(self, element):
    if self._compiled_steps is None:
      self._compiled_steps = _compile_step_matchers(self.config)
    _, raw_events = element
    ev_list = list(raw_events)
    ev_list.sort(key=lambda it: (it[0], it[1]))
    _, emitted, exp_c, ovf_c, ovf_rows = _advance_nfa_runs(
        self.config,
        self._compiled_steps,
        [],
        ev_list,
        horizon_sec=float('inf'))
    if emitted:
      self.matched_ctr.inc(len(emitted))
      for m_ts, m_dict in emitted:
        yield TimestampedValue(
            beam.Row(**m_dict), beam_timestamp.Timestamp.of(m_ts))
    if exp_c:
      self.expired_ctr.inc(exp_c)
    if ovf_c:
      if self.config.overflow_action == 'fail':
        raise ValueError(
            f'CliPatternMatch exceeded max_active_matches_per_key='
            f'{self.config.max_active_per_key}')
      if self.config.overflow_action == 'dlq' and self.route_errors:
        for ov_r in ovf_rows:
          yield pvalue.TaggedOutput(
              self.err_tag,
              _make_cep_dlq_row(
                  'PatternStateOverflowError',
                  f'exceeded max_active_matches_per_key={self.config.max_active_per_key}',
                  ov_r,
                  redact_fields=self.config.dlq_redact_fields))
      else:
        self.overflow_ctr.inc(ovf_c)


def cli_pattern_match(
    error_handling: dict[str, Any] | None = None,
    **kwargs,
) -> beam.PTransform:
  """Beam YAML provider callable for `type: CliPatternMatch` / `PatternMatch`."""
  cfg = parse_pattern_config(kwargs)
  err_tag = (error_handling or {}).get('output', 'errors')
  route_errors = bool(error_handling and 'output' in error_handling)
  if cfg.overflow_action == 'dlq' and not route_errors:
    raise ValueError(
        'CliPatternMatch overflow_action="dlq" requires error_handling / --dlq')

  dlq_row_type = RowTypeConstraint.from_fields([
      ('step', str),
      ('error_type', str),
      ('error', str),
      ('raw_payload', str),
      ('element', str),
  ])

  class _TagKeyTsDoFn(beam.DoFn):
    def __init__(self, bounded_seq: bool):
      self._bounded_seq = bounded_seq
      self._local_seq = 0

    def start_bundle(self):
      self._local_seq = 0

    def process(self, record, elem_ts=beam.DoFn.TimestampParam):
      d = interval_join._to_plain_dict(record)
      try:
        ckey = interval_join.encode_canonical_key_values(
            tuple(d.get(k) for k in cfg.keys))
        if cfg.timestamp_col:
          t = interval_join.extract_record_timestamp(d, cfg.timestamp_col)
        else:
          t = float(elem_ts)
        self._local_seq += 1
        payload = (t, self._local_seq, d) if self._bounded_seq else (t, d)
        yield TimestampedValue((ckey, payload), beam_timestamp.Timestamp.of(t))
      except Exception as e:  # pylint: disable=broad-except
        if route_errors:
          yield pvalue.TaggedOutput(
              err_tag,
              _make_cep_dlq_row(
                  type(e).__name__,
                  str(e),
                  d,
                  redact_fields=cfg.dlq_redact_fields))
        else:
          raise

  class _CliPatternMatchTransform(beam.PTransform):
    def expand(self, pcoll):
      in_el_type = getattr(pcoll, 'element_type', None)
      out_row_type = _infer_cep_row_type(in_el_type, cfg)
      is_bounded = getattr(pcoll, 'is_bounded', True)
      win_pcoll = pcoll | 'GlobalWinPattern' >> beam.WindowInto(GlobalWindows())

      dlq_pcolls: list[Any] = []
      use_bounded = bool(is_bounded and not cfg.force_stateful)
      tagged = win_pcoll | 'KeyAndExtractTs' >> beam.ParDo(
          _TagKeyTsDoFn(bounded_seq=use_bounded)).with_outputs(
              err_tag, main='good')
      if route_errors:
        dlq_pcolls.append(tagged[err_tag])
      typed_kv = tagged['good'] | 'TypeKeyedPattern' >> beam.Map(
          lambda kv: kv).with_output_types(tuple[bytes, Any])

      if use_bounded:
        grouped = typed_kv | 'GroupPatternByKey' >> beam.GroupByKey()
        cep_out = grouped | 'BoundedPatternMatch' >> beam.ParDo(
            _BoundedPatternMatchDoFn(
                cfg, err_tag=err_tag, route_errors=route_errors)).with_outputs(
                    err_tag, main='good')
      else:
        cep_out = typed_kv | 'StatefulPatternMatch' >> beam.ParDo(
            PatternMatchStatefulDoFn(
                cfg, err_tag=err_tag, route_errors=route_errors)).with_outputs(
                    err_tag, main='good')

      good = cep_out['good']
      good.element_type = out_row_type
      if route_errors:
        dlq_pcolls.append(cep_out[err_tag])
        for d_pc in dlq_pcolls:
          d_pc.element_type = dlq_row_type
        bad = (
            dlq_pcolls[0] if len(dlq_pcolls) == 1 else
            (tuple(dlq_pcolls) | 'FlattenPatternDlq' >> beam.Flatten()))
        bad.element_type = dlq_row_type
        return {'good': good, err_tag: bad}
      return good

  return _CliPatternMatchTransform()


CliPatternMatch = cli_pattern_match
