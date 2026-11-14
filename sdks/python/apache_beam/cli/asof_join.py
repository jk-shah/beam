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

"""Declarative Point-in-Time `CliAsofJoin` (`--asof-join` & SQL `ASOF JOIN`, `B2.1`).

Bridges ahead of Apache Beam `#41309` with leak-free 1:1 point-in-time joins:
- Supports `backward` (`t_r <= t_l`, default), `forward` (`t_r >= t_l`), and
  `nearest` (`argmin |t_l - t_r|` with causal backward tie-breaking), optional
  strict inequality (`strict=true`), optional `--asof-tolerance` (`tolerance=`),
  `type=left|inner`, and deterministic tie-breaking (`right_order=`).
- Watermark-synchronized `AsofJoinStatefulDoFn` using `BagStateSpec` +
  `ReadModifyWriteStateSpec` (`TupleCoder` / `VarIntCoder` / `StrUtf8Coder` —
  zero `OrderedListStateSpec`, running identically across
  `BundleBasedDirectRunner` `TestStream`, `FnApiRunner`, `PrismRunner`, and
  `DataflowRunner` Windmill) with 1ms-aligned `EVAL_TIMER` and $O(1)$
  dominated-anchor right-state compaction.
- $O((|L| + |R|) \\log |R|)$ binary-search bounded `CoGroupByKey` fast path and
  in-memory `join_bounded_asof` engine shared by zero-DuckDB local SQL `ASOF JOIN`.
"""

from __future__ import annotations

import bisect
from collections.abc import Iterable
from collections.abc import Mapping
import decimal
import json
import math
from typing import Any
from typing import NamedTuple

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
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.utils import timestamp as beam_timestamp

from . import enrich as enrich_mod
from . import interval_join
from . import output

_ALLOWED_DIRECTIONS = frozenset(['backward', 'forward', 'nearest'])
_ALLOWED_JOIN_TYPES = frozenset(['left', 'inner'])
_ALLOWED_SKEW_ACTIONS = frozenset(['drop', 'warn', 'dlq', 'fail'])


class AsofJoinConfig(NamedTuple):
  left_key: tuple[str, ...]
  right_key: tuple[str, ...]
  left_timestamp: str | None = None
  right_timestamp: str | None = None
  direction: str = 'backward'
  strict: bool = False
  tolerance: float | None = None
  tolerance_str: str | None = None
  join_type: str = 'left'
  right_order: str | None = None
  allowed_lateness: float = 0.0
  allowed_lateness_str: str | None = None
  state_ttl: float | None = None
  state_ttl_str: str | None = None
  max_watermark_skew: float | None = None
  skew_action: str = 'drop'
  select: dict[str, str] | None = None
  left_alias: str = 'left'
  right_alias: str = 'right'
  left_fields: tuple[str, ...] = ()
  right_fields: tuple[str, ...] = ()
  dlq_redact_fields: tuple[str, ...] = ()
  force_stateful: bool = False


def _parse_bool(raw: Any, label: str) -> bool:
  if isinstance(raw, bool):
    return raw
  s = str(raw).strip().lower()
  if s in ('1', 'true', 'yes', 'on'):
    return True
  if s in ('0', 'false', 'no', 'off'):
    return False
  raise ValueError(f'{label} must be true or false, got {raw!r}')


def _parse_opt_nonneg_duration(raw: Any,
                               label: str) -> tuple[float | None, str | None]:
  if raw is None or str(raw).strip() == '':
    return None, None
  raw_s = str(raw).strip()
  sec = interval_join.parse_interval_duration(raw_s)
  if not math.isfinite(sec) or sec < 0:
    raise ValueError(f'{label} must be >= 0, got {raw!r}')
  return float(sec), raw_s


def _parse_select_spec(raw: Any) -> dict[str, str] | None:
  if raw is None:
    return None
  if isinstance(raw, Mapping):
    out: dict[str, str] = {}
    for k, v in raw.items():
      ks = str(k).strip()
      if not output.SAFE_IDENTIFIER_RE.match(ks):
        raise ValueError(f'invalid select output column {ks!r} in CliAsofJoin')
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
      alias_s = tok.rsplit('.', 1)[-1].strip()
    if not output.SAFE_IDENTIFIER_RE.match(alias_s):
      raise ValueError(
          f'select item {tok!r} in --asof-join requires an AS <identifier> alias'
      )
    out[alias_s] = expr_s
  return out or None


def parse_asof_join_spec_str(
    spec_str: str,
    *,
    max_watermark_skew: str | None = None,
    skew_action: str | None = None,
    left_alias: str | None = None,
    right_alias: str | None = None,
    allowed_lateness: str | None = None,
    dlq_redact_fields: Iterable[str] | None = None,
) -> dict[str, Any]:
  """Parses CLI `--asof-join "key=sym,left_time=t_ts,right_time=q_ts,..."`."""
  s = (spec_str or '').strip()
  if not s:
    raise ValueError('--asof-join specification cannot be empty')
  if s.startswith('{') and s.endswith('}'):
    try:
      raw_dict = json.loads(s)
    except Exception as e:
      raise ValueError(f'malformed JSON in --asof-join: {e}') from None
    if not isinstance(raw_dict, dict):
      raise ValueError('--asof-join JSON must be an object')
  else:
    raw_dict = {}
    for tok in interval_join.split_bracket_tokens(s):
      if '=' not in tok:
        raise ValueError(
            f'invalid token {tok!r} in --asof-join; expected key=value '
            '(e.g. "key=symbol,left_time=trade_ts,right_time=quote_ts,tolerance=5m")'
        )
      k, v = tok.split('=', 1)
      ks = k.strip().lower().replace('-', '_')
      vs = v.strip()
      if len(vs) >= 2 and vs[0] == vs[-1] and vs[0] in ("'", '"'):
        vs = vs[1:-1]
      raw_dict[ks] = vs

  if left_alias and 'left_alias' not in raw_dict:
    raw_dict['left_alias'] = left_alias
  if right_alias and 'right_alias' not in raw_dict:
    raw_dict['right_alias'] = right_alias
  if max_watermark_skew is not None and 'max_watermark_skew' not in raw_dict:
    raw_dict['max_watermark_skew'] = max_watermark_skew
  if skew_action is not None and 'skew_action' not in raw_dict:
    raw_dict['skew_action'] = skew_action
  if allowed_lateness is not None and 'allowed_lateness' not in raw_dict:
    raw_dict['allowed_lateness'] = allowed_lateness
  if dlq_redact_fields and 'dlq_redact_fields' not in raw_dict:
    raw_dict['dlq_redact_fields'] = list(dlq_redact_fields)

  parsed = parse_asof_join_config(raw_dict)
  return asof_join_config_to_yaml_dict(parsed)


def parse_asof_join_config(raw: Mapping[str, Any]) -> AsofJoinConfig:
  """Validates and normalizes `CliAsofJoin` options into `AsofJoinConfig`."""
  if not isinstance(raw, Mapping):
    raise ValueError('CliAsofJoin config must be a mapping')

  norm = {str(k).strip().lower().replace('-', '_'): v for k, v in raw.items()}
  allowed = {
      'left',
      'right',
      'key',
      'keys',
      'left_key',
      'right_key',
      'time',
      'timestamp',
      'left_time',
      'right_time',
      'left_timestamp',
      'right_timestamp',
      'direction',
      'strict',
      'tolerance',
      'asof_tolerance',
      'type',
      'join_type',
      'right_order',
      'allowed_lateness',
      'state_ttl',
      'max_watermark_skew',
      'skew_action',
      'select',
      'left_alias',
      'right_alias',
      'left_fields',
      'right_fields',
      'dlq_redact_fields',
      'force_stateful',
      'error_handling',
  }
  unknown = [k for k in norm if k not in allowed]
  if unknown:
    raise ValueError(f'unknown option(s) {sorted(unknown)} in CliAsofJoin')

  shared_key = interval_join.parse_column_list(
      norm.get('key') if norm.get('key') is not None else norm.get('keys'))
  l_key = interval_join.parse_column_list(norm.get('left_key')) or shared_key
  r_key = interval_join.parse_column_list(norm.get('right_key')) or shared_key
  if not l_key or not r_key:
    raise ValueError(
        'CliAsofJoin requires join key column(s) via key=... or '
        'left_key=... and right_key=...')
  if len(l_key) != len(r_key):
    raise ValueError(
        f'CliAsofJoin left_key {l_key} and right_key {r_key} must have '
        'equal arity')
  for col in (*l_key, *r_key):
    if not output.SAFE_IDENTIFIER_RE.match(col):
      raise ValueError(f'invalid join key identifier {col!r} in CliAsofJoin')

  shared_ts = norm.get('time') or norm.get('timestamp')
  l_ts_raw = norm.get('left_time') or norm.get('left_timestamp') or shared_ts
  r_ts_raw = norm.get('right_time') or norm.get('right_timestamp') or shared_ts
  l_ts = str(l_ts_raw).strip() if l_ts_raw else None
  r_ts = str(r_ts_raw).strip() if r_ts_raw else None
  for tc in (l_ts, r_ts):
    if tc is not None and not output.SAFE_IDENTIFIER_RE.match(tc):
      raise ValueError(f'invalid timestamp column {tc!r} in CliAsofJoin')

  direction = str(norm.get('direction') or 'backward').strip().lower()
  if direction not in _ALLOWED_DIRECTIONS:
    raise ValueError(
        f'invalid direction {direction!r} in CliAsofJoin; expected '
        'backward, forward, or nearest')

  strict = _parse_bool(norm.get('strict', False), 'strict')
  if strict and direction == 'nearest':
    raise ValueError(
        'strict=true cannot be combined with direction="nearest" in CliAsofJoin'
    )

  tol_raw = (
      norm['tolerance'] if 'tolerance' in norm else norm.get('asof_tolerance'))
  tol_sec, tol_str = _parse_opt_nonneg_duration(tol_raw, 'tolerance')

  jtype = str(norm.get('join_type') or norm.get('type') or
              'left').strip().lower()
  if jtype.endswith('_asof'):
    jtype = jtype[:-5]
  if jtype not in _ALLOWED_JOIN_TYPES:
    raise ValueError(
        f'invalid join_type {jtype!r} in CliAsofJoin; expected left or inner '
        '(ASOF JOIN preserves left-stream lookup semantics)')

  r_order_raw = norm.get('right_order')
  r_order = str(r_order_raw).strip() if r_order_raw else None
  if r_order is not None and not output.SAFE_IDENTIFIER_RE.match(r_order):
    raise ValueError(f'invalid right_order column {r_order!r} in CliAsofJoin')

  lat_sec, lat_str = _parse_opt_nonneg_duration(
      norm.get('allowed_lateness'), 'allowed_lateness')
  ttl_sec, ttl_str = _parse_opt_nonneg_duration(
      norm.get('state_ttl'), 'state_ttl')
  if ttl_sec is not None and ttl_sec <= 0:
    raise ValueError('state_ttl must be > 0 when specified')

  skew_sec, _ = _parse_opt_nonneg_duration(
      norm.get('max_watermark_skew'), 'max_watermark_skew')
  if skew_sec is not None and skew_sec <= 0:
    raise ValueError('max_watermark_skew must be > 0 when specified')

  skew_act = str(norm.get('skew_action') or 'drop').strip().lower()
  if skew_act not in _ALLOWED_SKEW_ACTIONS:
    raise ValueError(
        f'invalid skew_action {skew_act!r}; expected drop, warn, dlq, or fail')

  select_map = _parse_select_spec(norm.get('select'))
  if select_map:
    for expr_s in select_map.values():
      interval_join._compile_join_select_expr(expr_s)

  l_alias = str(norm.get('left_alias') or 'left').strip()
  r_alias = str(norm.get('right_alias') or 'right').strip()
  for al in (l_alias, r_alias):
    if not output.SAFE_IDENTIFIER_RE.match(al):
      raise ValueError(f'invalid stream alias {al!r} in CliAsofJoin')

  l_fields = interval_join.parse_column_list(norm.get('left_fields'))
  r_fields = interval_join.parse_column_list(norm.get('right_fields'))

  dlq_redact: tuple[str, ...] = ()
  if norm.get('dlq_redact_fields'):
    dlq_redact = enrich_mod._validate_ident_list(
        interval_join.parse_column_list(norm['dlq_redact_fields']),
        'dlq_redact_fields')

  force_st = _parse_bool(norm.get('force_stateful', False), 'force_stateful')

  return AsofJoinConfig(
      left_key=l_key,
      right_key=r_key,
      left_timestamp=l_ts,
      right_timestamp=r_ts,
      direction=direction,
      strict=strict,
      tolerance=tol_sec,
      tolerance_str=tol_str,
      join_type=jtype,
      right_order=r_order,
      allowed_lateness=lat_sec or 0.0,
      allowed_lateness_str=lat_str if (lat_sec and lat_sec > 0) else None,
      state_ttl=ttl_sec,
      state_ttl_str=ttl_str,
      max_watermark_skew=skew_sec,
      skew_action=skew_act,
      select=select_map,
      left_alias=l_alias,
      right_alias=r_alias,
      left_fields=l_fields,
      right_fields=r_fields,
      dlq_redact_fields=dlq_redact,
      force_stateful=force_st,
  )


def asof_join_config_to_yaml_dict(cfg: AsofJoinConfig) -> dict[str, Any]:
  out: dict[str, Any] = {
      'left_alias': cfg.left_alias,
      'right_alias': cfg.right_alias,
      'left_key': (
          cfg.left_key[0] if len(cfg.left_key) == 1 else list(cfg.left_key)),
      'right_key': (
          cfg.right_key[0] if len(cfg.right_key) == 1 else list(cfg.right_key)),
      'direction': cfg.direction,
      'join_type': cfg.join_type,
      'skew_action': cfg.skew_action,
  }
  if cfg.left_timestamp:
    out['left_time'] = cfg.left_timestamp
  if cfg.right_timestamp:
    out['right_time'] = cfg.right_timestamp
  if cfg.strict:
    out['strict'] = True
  if cfg.tolerance_str is not None:
    out['tolerance'] = cfg.tolerance_str
  elif cfg.tolerance is not None:
    out['tolerance'] = f'{cfg.tolerance:g}s'
  if cfg.right_order:
    out['right_order'] = cfg.right_order
  if cfg.allowed_lateness_str:
    out['allowed_lateness'] = cfg.allowed_lateness_str
  elif cfg.allowed_lateness > 0:
    out['allowed_lateness'] = f'{cfg.allowed_lateness:g}s'
  if cfg.state_ttl_str:
    out['state_ttl'] = cfg.state_ttl_str
  elif cfg.state_ttl is not None:
    out['state_ttl'] = f'{cfg.state_ttl:g}s'
  if cfg.max_watermark_skew is not None:
    out['max_watermark_skew'] = f'{cfg.max_watermark_skew:g}s'
  if cfg.select:
    out['select'] = dict(cfg.select)
  if cfg.left_fields:
    out['left_fields'] = list(cfg.left_fields)
  if cfg.right_fields:
    out['right_fields'] = list(cfg.right_fields)
  if cfg.dlq_redact_fields:
    out['dlq_redact_fields'] = list(cfg.dlq_redact_fields)
  if cfg.force_stateful:
    out['force_stateful'] = True
  return out


def _as_interval_projection_config(
    cfg: AsofJoinConfig,
) -> interval_join.IntervalJoinConfig:
  return interval_join.IntervalJoinConfig(
      left_key=cfg.left_key,
      right_key=cfg.right_key,
      lower_bound=0.0,
      upper_bound=0.0,
      join_type=cfg.join_type,
      left_timestamp=cfg.left_timestamp,
      right_timestamp=cfg.right_timestamp,
      max_watermark_skew=cfg.max_watermark_skew,
      skew_action=cfg.skew_action,
      select=cfg.select,
      left_alias=cfg.left_alias,
      right_alias=cfg.right_alias,
      left_fields=cfg.left_fields,
      right_fields=cfg.right_fields,
      force_stateful=cfg.force_stateful,
  )


def _encode_tie_key(row_dict: Mapping[str, Any], order_col: str | None) -> str:
  """Encodes a lexicographically sortable tie-breaker string (never raises `TypeError`)."""
  if not order_col:
    return ''
  val = row_dict.get(order_col)
  if val is None:
    return '0:'
  if isinstance(val, bool):
    return f'1:{int(val)}'
  if isinstance(val, (int, float, decimal.Decimal)):
    f = float(val)
    if math.isnan(f):
      return '0:'
    # Offset-binary IEEE-like sortable fixed-width decimal for numbers
    sign = '+' if f >= 0 else '-'
    mag = abs(f)
    if sign == '+':
      return f'2:+{mag:028.9f}'
    inv = max(0.0, 9_999_999_999_999_999.0 - mag)
    return f'2:-{inv:028.9f}'
  return f'3:{val}'


def _make_asof_dlq_row(
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
      step='CliAsofJoin',
      error_type=err_type,
      error=msg[:4096],
      raw_payload=raw_json,
      element=raw_json,
  )


def _find_best_right_entry(
    t_l: float,
    sorted_right: list[tuple[float, str, str, dict[str, Any]]],
    t_list: list[float],
    cfg: AsofJoinConfig,
) -> dict[str, Any] | None:
  """Finds the single best right dict in `O(log |R|)` from `sorted_right`."""
  if not sorted_right:
    return None

  def _pick_backward(strict_flag: bool) -> tuple[float, dict[str, Any]] | None:
    pos = (
        bisect.bisect_left(t_list, t_l) if strict_flag else bisect.bisect_right(
            t_list, t_l))
    if pos <= 0:
      return None
    cand = sorted_right[pos - 1]
    if cfg.tolerance is not None and (t_l - cand[0]) > (cfg.tolerance + 1e-9):
      return None
    return cand[0], cand[3]

  def _pick_forward(strict_flag: bool) -> tuple[float, dict[str, Any]] | None:
    pos = (
        bisect.bisect_right(t_list, t_l) if strict_flag else bisect.bisect_left(
            t_list, t_l))
    if pos >= len(sorted_right):
      return None
    min_tr = sorted_right[pos][0]
    if cfg.tolerance is not None and (min_tr - t_l) > (cfg.tolerance + 1e-9):
      return None
    # Among ties sharing `min_tr`, pick the highest `(tie_str, canon_json)`!
    end_pos = bisect.bisect_right(t_list, min_tr)
    cand = sorted_right[end_pos - 1]
    return cand[0], cand[3]

  if cfg.direction == 'backward':
    res = _pick_backward(cfg.strict)
    return res[1] if res is not None else None

  if cfg.direction == 'forward':
    res = _pick_forward(cfg.strict)
    return res[1] if res is not None else None

  # `nearest`: causal backward tie-breaking when distances are equal!
  b_res = _pick_backward(False)
  f_res = _pick_forward(False)
  if b_res is None:
    return f_res[1] if f_res is not None else None
  if f_res is None:
    return b_res[1]
  dist_b = abs(t_l - b_res[0])
  dist_f = abs(f_res[0] - t_l)
  return b_res[1] if dist_b <= (dist_f + 1e-12) else f_res[1]


def join_bounded_asof(
    left_records: Iterable[Any],
    right_records: Iterable[Any],
    config: AsofJoinConfig,
) -> tuple[list[dict[str, Any]], int, int]:
  """Executes deterministic in-memory `ASOF JOIN`, returning `(rows, matched, unmatched)`."""
  proj_cfg = _as_interval_projection_config(config)
  left_list = [interval_join._to_plain_dict(r) for r in left_records]
  right_list = [interval_join._to_plain_dict(r) for r in right_records]

  left_cols: list[str] = list(config.left_fields)
  for r in left_list:
    for k in r:
      if k not in left_cols:
        left_cols.append(k)
  right_cols: list[str] = list(config.right_fields)
  for r in right_list:
    for k in r:
      if k not in right_cols:
        right_cols.append(k)

  right_by_key: dict[bytes, list[tuple[float, str, str, dict[str, Any]]]] = {}
  for r in right_list:
    k_bytes = interval_join.encode_canonical_key_values(
        tuple(r.get(c) for c in config.right_key))
    t_r = interval_join.extract_record_timestamp(r, config.right_timestamp)
    tie_s = _encode_tie_key(r, config.right_order)
    canon_s = interval_join._encode_state_payload(r)
    right_by_key.setdefault(k_bytes, []).append((t_r, tie_s, canon_s, r))

  sorted_right_by_key: dict[bytes,
                            tuple[list[tuple[float, str, str, dict[str, Any]]],
                                  list[float]]] = {}
  for k_bytes, r_items in right_by_key.items():
    r_items.sort(key=lambda item: (item[0], item[1], item[2]))
    sorted_right_by_key[k_bytes] = (r_items, [x[0] for x in r_items])

  results: list[dict[str, Any]] = []
  matched = 0
  unmatched = 0
  for l_row in left_list:
    k_bytes = interval_join.encode_canonical_key_values(
        tuple(l_row.get(c) for c in config.left_key))
    t_l = interval_join.extract_record_timestamp(l_row, config.left_timestamp)
    r_pair = sorted_right_by_key.get(k_bytes)
    best_r = (
        _find_best_right_entry(t_l, r_pair[0], r_pair[1], config)
        if r_pair is not None else None)
    if best_r is not None:
      matched += 1
      results.append(
          interval_join.project_joined_row(
              l_row,
              best_r,
              proj_cfg,
              left_cols=left_cols,
              right_cols=right_cols))
    else:
      unmatched += 1
      if config.join_type == 'left':
        results.append(
            interval_join.project_joined_row(
                l_row,
                None,
                proj_cfg,
                left_cols=left_cols,
                right_cols=right_cols))

  return results, matched, unmatched


class _BoundedAsofJoinDoFn(beam.DoFn):
  """Executes `ASOF JOIN` per key over `CoGroupByKey` batches on bounded inputs."""
  def __init__(
      self,
      config: AsofJoinConfig,
      err_tag: str = 'errors',
      route_errors: bool = False,
  ):
    self.config = config
    self.err_tag = err_tag
    self.route_errors = route_errors
    self.matched_ctr = beam.metrics.Metrics.counter('beam_cli', 'asof_matched')
    self.unmatched_ctr = beam.metrics.Metrics.counter(
        'beam_cli', 'asof_unmatched')
    self.skew_dropped_ctr = beam.metrics.Metrics.counter(
        'beam_cli', 'skew_dropped')

  def process(self, element):
    _, grouped = element
    raw_left = list(grouped.get('left', []))
    raw_right = list(grouped.get('right', []))

    valid_left: list[tuple[float, dict[str, Any]]] = []
    for r in raw_left:
      try:
        t = interval_join.extract_record_timestamp(
            r, self.config.left_timestamp)
        valid_left.append((t, interval_join._to_plain_dict(r)))
      except Exception as e:  # pylint: disable=broad-except
        if self.route_errors or self.config.skew_action == 'dlq':
          if not self.route_errors:
            raise
          yield pvalue.TaggedOutput(
              self.err_tag,
              _make_asof_dlq_row(
                  type(e).__name__,
                  str(e),
                  r,
                  redact_fields=self.config.dlq_redact_fields))
        elif self.config.skew_action == 'fail':
          raise
        else:
          self.skew_dropped_ctr.inc(1)

    valid_right: list[tuple[float, dict[str, Any]]] = []
    for r in raw_right:
      try:
        t = interval_join.extract_record_timestamp(
            r, self.config.right_timestamp)
        valid_right.append((t, interval_join._to_plain_dict(r)))
      except Exception as e:  # pylint: disable=broad-except
        if self.route_errors or self.config.skew_action == 'dlq':
          if not self.route_errors:
            raise
          yield pvalue.TaggedOutput(
              self.err_tag,
              _make_asof_dlq_row(
                  type(e).__name__,
                  str(e),
                  r,
                  redact_fields=self.config.dlq_redact_fields))
        elif self.config.skew_action == 'fail':
          raise
        else:
          self.skew_dropped_ctr.inc(1)

    if self.config.max_watermark_skew is not None and valid_left and valid_right:
      max_l = max(t for t, _ in valid_left)
      max_r = max(t for t, _ in valid_right)
      kept_left: list[tuple[float, dict[str, Any]]] = []
      for t_l, r in valid_left:
        if (max_r - t_l) > self.config.max_watermark_skew:
          if self.config.skew_action == 'dlq':
            if not self.route_errors:
              raise ValueError(
                  'skew_action="dlq" requires --dlq / error_handling')
            yield pvalue.TaggedOutput(
                self.err_tag,
                _make_asof_dlq_row(
                    'WatermarkSkewError',
                    f'left timestamp {t_l} lagged right max {max_r} by '
                    f'> {self.config.max_watermark_skew}s',
                    r,
                    redact_fields=self.config.dlq_redact_fields))
            continue
          if self.config.skew_action == 'fail':
            raise ValueError(
                f'watermark skew exceeded {self.config.max_watermark_skew}s')
          if self.config.skew_action == 'drop':
            self.skew_dropped_ctr.inc(1)
            continue
        kept_left.append((t_l, r))

      kept_right: list[tuple[float, dict[str, Any]]] = []
      for t_r, r in valid_right:
        if (max_l - t_r) > self.config.max_watermark_skew:
          if self.config.skew_action == 'dlq':
            if not self.route_errors:
              raise ValueError(
                  'skew_action="dlq" requires --dlq / error_handling')
            yield pvalue.TaggedOutput(
                self.err_tag,
                _make_asof_dlq_row(
                    'WatermarkSkewError',
                    f'right timestamp {t_r} lagged left max {max_l} by '
                    f'> {self.config.max_watermark_skew}s',
                    r,
                    redact_fields=self.config.dlq_redact_fields))
            continue
          if self.config.skew_action == 'fail':
            raise ValueError(
                f'watermark skew exceeded {self.config.max_watermark_skew}s')
          if self.config.skew_action == 'drop':
            self.skew_dropped_ctr.inc(1)
            continue
        kept_right.append((t_r, r))
      valid_left = kept_left
      valid_right = kept_right

    if not valid_left:
      return

    out_rows, matched, unmatched = join_bounded_asof(
        [r for _, r in valid_left],
        [r for _, r in valid_right],
        self.config._replace(max_watermark_skew=None))
    if matched:
      self.matched_ctr.inc(matched)
    if unmatched:
      self.unmatched_ctr.inc(unmatched)
    for row_dict in out_rows:
      yield beam.Row(**row_dict)


_LEFT_PENDING_CODER = TupleCoder((FloatCoder(), VarIntCoder(), StrUtf8Coder()))
_RIGHT_ENTRY_CODER = TupleCoder((FloatCoder(), StrUtf8Coder(), StrUtf8Coder()))


class AsofJoinStatefulDoFn(beam.DoFn):
  """Watermark-synchronized stateful `ASOF JOIN` with $O(1)$ anchor compaction."""

  LEFT_PENDING = BagStateSpec('left_pending', _LEFT_PENDING_CODER)
  RIGHT_BUFFER = BagStateSpec('right_buffer', _RIGHT_ENTRY_CODER)
  WATERMARK_STATE = ReadModifyWriteStateSpec('wm_state', StrUtf8Coder())
  NEXT_EVAL_MS = ReadModifyWriteStateSpec('next_eval_ms', VarIntCoder())
  EVAL_TIMER = TimerSpec('eval_timer', TimeDomain.WATERMARK)

  def __init__(
      self,
      config: AsofJoinConfig,
      err_tag: str = 'errors',
      route_errors: bool = False,
  ):
    self.config = config
    self.proj_cfg = _as_interval_projection_config(config)
    self.err_tag = err_tag
    self.route_errors = route_errors
    self.matched_ctr = beam.metrics.Metrics.counter('beam_cli', 'asof_matched')
    self.unmatched_ctr = beam.metrics.Metrics.counter(
        'beam_cli', 'asof_unmatched')
    self.skew_dropped_ctr = beam.metrics.Metrics.counter(
        'beam_cli', 'skew_dropped')
    self.skew_warn_ctr = beam.metrics.Metrics.counter(
        'AsofJoin', 'watermark_skew_warn')

  def _handle_bad_or_skewed(
      self, err_type: str, msg: str, payload: Any, *, is_skew: bool = False):
    action = self.config.skew_action
    if action == 'dlq' or (not is_skew and self.route_errors):
      if not self.route_errors:
        raise ValueError(f'{err_type}: {msg} (configure --dlq to quarantine)')
      return pvalue.TaggedOutput(
          self.err_tag,
          _make_asof_dlq_row(
              err_type,
              msg,
              payload,
              redact_fields=self.config.dlq_redact_fields))
    if action == 'fail':
      raise ValueError(f'{err_type}: {msg}')
    self.skew_dropped_ctr.inc(1)
    return None

  def _compute_eval_ms(self, t_l: float) -> int:
    forward_extra = ((self.config.tolerance or 0.0) if self.config.direction
                     in ('forward', 'nearest') else 0.0)
    eval_sec = t_l + forward_extra + self.config.allowed_lateness
    # Align up to 1ms boundary so FnAPI `beam:coder:timer:v1` never truncates!
    return int(math.ceil(eval_sec * 1000.0))

  def _compute_right_cleanup_ms(self, t_r: float) -> int | None:
    horizon_span = (
        self.config.tolerance
        if self.config.tolerance is not None else self.config.state_ttl)
    if horizon_span is None:
      return None
    clean_sec = t_r + horizon_span + self.config.allowed_lateness + 0.001
    return int(math.ceil(clean_sec * 1000.0))

  def process(
      self,
      element,
      left_pending=beam.DoFn.StateParam(LEFT_PENDING),
      right_buf=beam.DoFn.StateParam(RIGHT_BUFFER),
      wm_state=beam.DoFn.StateParam(WATERMARK_STATE),
      next_eval=beam.DoFn.StateParam(NEXT_EVAL_MS),
      eval_timer=beam.DoFn.TimerParam(EVAL_TIMER),
  ):
    _, (tag, t, row_payload) = element
    if t is None or not isinstance(t, (int, float)) or not math.isfinite(t):
      dlq_out = self._handle_bad_or_skewed(
          'InvalidTimestampError',
          f'non-finite event timestamp {t!r}',
          row_payload,
          is_skew=False)
      if dlq_out is not None:
        yield dlq_out
      return

    t = float(t)
    raw_wm = wm_state.read()
    cur_wm: dict[str, Any] = json.loads(raw_wm) if raw_wm else {}
    max_skew = self.config.max_watermark_skew

    if max_skew is not None:
      partner_side = 'R' if tag == 'L' else 'L'
      partner_max = cur_wm.get(partner_side)
      if partner_max is not None:
        if (partner_max - t) > max_skew:
          if self.config.skew_action == 'warn':
            self.skew_warn_ctr.inc(1)
          else:
            dlq_out = self._handle_bad_or_skewed(
                'WatermarkSkewError',
                f'{tag} timestamp {t} lagged partner max {partner_max} by '
                f'> {max_skew}s',
                row_payload,
                is_skew=True)
            if dlq_out is not None:
              yield dlq_out
            return
        elif (t - partner_max) > max_skew:
          self.skew_warn_ctr.inc(1)
      prev_self = cur_wm.get(tag)
      cur_wm[tag] = t if prev_self is None else max(prev_self, t)
      wm_state.write(json.dumps(cur_wm))

    evicted_ms = cur_wm.get('evicted_ms')
    encoded_json = interval_join._encode_state_payload(row_payload)

    if tag == 'L':
      eval_ms = self._compute_eval_ms(t)
      if evicted_ms is not None and eval_ms < evicted_ms:
        # Late left arrival past already-evaluated watermark horizon
        if self.config.skew_action == 'dlq' and self.route_errors:
          yield pvalue.TaggedOutput(
              self.err_tag,
              _make_asof_dlq_row(
                  'LateArrivalError',
                  f'late left ASOF record at {t} arrived after watermark '
                  f'{evicted_ms / 1000.0}s',
                  row_payload,
                  redact_fields=self.config.dlq_redact_fields))
        else:
          self.skew_dropped_ctr.inc(1)
        return
      left_pending.add((t, eval_ms, encoded_json))
      cur_next = next_eval.read()
      if cur_next is None or eval_ms < cur_next:
        next_eval.write(eval_ms)
        eval_timer.set(beam_timestamp.Timestamp(micros=eval_ms * 1000))
    else:
      prev_r_cols = list(cur_wm.get('r_cols') or [])
      if isinstance(row_payload, Mapping):
        changed_cols = False
        for rk in row_payload:
          if rk not in prev_r_cols:
            prev_r_cols.append(rk)
            changed_cols = True
        if changed_cols:
          cur_wm['r_cols'] = prev_r_cols
          wm_state.write(json.dumps(cur_wm))
      tie_s = _encode_tie_key(row_payload, self.config.right_order)
      right_buf.add((t, tie_s, encoded_json))
      clean_ms = self._compute_right_cleanup_ms(t)
      if clean_ms is not None:
        cur_next = next_eval.read()
        if cur_next is None or clean_ms < cur_next:
          next_eval.write(clean_ms)
          eval_timer.set(beam_timestamp.Timestamp(micros=clean_ms * 1000))

  @on_timer(EVAL_TIMER)
  def on_eval(
      self,
      timestamp=beam.DoFn.TimestampParam,
      left_pending=beam.DoFn.StateParam(LEFT_PENDING),
      right_buf=beam.DoFn.StateParam(RIGHT_BUFFER),
      wm_state=beam.DoFn.StateParam(WATERMARK_STATE),
      next_eval=beam.DoFn.StateParam(NEXT_EVAL_MS),
      eval_timer=beam.DoFn.TimerParam(EVAL_TIMER),
  ):
    fired_ms = int(timestamp.micros) // 1000
    fired_sec = fired_ms / 1000.0

    raw_wm = wm_state.read()
    cur_wm: dict[str, Any] = json.loads(raw_wm) if raw_wm else {}

    left_items = list(left_pending.read())
    right_raw = list(right_buf.read())

    ready_left = [it for it in left_items if it[1] <= fired_ms]
    waiting_left = [it for it in left_items if it[1] > fired_ms]
    ready_left.sort(key=lambda it: (it[0], it[2]))

    sorted_right: list[tuple[float, str, str, dict[str, Any]]] = []
    right_cols: list[str] = list(self.config.right_fields)
    for rc in cur_wm.get('r_cols') or ():
      if rc not in right_cols:
        right_cols.append(rc)
    for t_r, tie_s, r_json in right_raw:
      r_decoded = interval_join._decode_state_payload(r_json)
      for rk in r_decoded:
        if rk not in right_cols:
          right_cols.append(rk)
      sorted_right.append((t_r, tie_s, r_json, r_decoded))
    sorted_right.sort(key=lambda it: (it[0], it[1], it[2]))
    t_list = [it[0] for it in sorted_right]

    matched = 0
    unmatched = 0
    for t_l, _, l_json in ready_left:
      l_dict = interval_join._decode_state_payload(l_json)
      best_r = _find_best_right_entry(t_l, sorted_right, t_list, self.config)
      if best_r is not None:
        matched += 1
        proj = interval_join.project_joined_row(
            l_dict, best_r, self.proj_cfg, right_cols=right_cols)
        yield TimestampedValue(
            beam.Row(**proj), beam_timestamp.Timestamp.of(t_l))
      else:
        unmatched += 1
        if self.config.join_type == 'left':
          proj = interval_join.project_joined_row(
              l_dict, None, self.proj_cfg, right_cols=right_cols)
          yield TimestampedValue(
              beam.Row(**proj), beam_timestamp.Timestamp.of(t_l))

    if matched:
      self.matched_ctr.inc(matched)
    if unmatched:
      self.unmatched_ctr.inc(unmatched)

    # Dominated-anchor right-state compaction:
    # Any future left event has `t_l >= safe_horizon`.
    safe_horizon = (
        min(it[0] for it in waiting_left) if waiting_left else
        (fired_sec - self.config.allowed_lateness))

    surviving_right: list[tuple[float, str, str]] = []
    if self.config.direction == 'backward':
      le_h = [it for it in sorted_right if it[0] <= safe_horizon]
      gt_h = [it for it in sorted_right if it[0] > safe_horizon]
      if le_h:
        anchor = le_h[-1]
        expired_by_tol = (
            self.config.tolerance is not None and
            (safe_horizon - anchor[0]) > (self.config.tolerance + 1e-9))
        expired_by_ttl = (
            self.config.state_ttl is not None and
            (fired_sec - anchor[0]) > (self.config.state_ttl + 1e-9))
        if not expired_by_tol and not expired_by_ttl:
          surviving_right.append((anchor[0], anchor[1], anchor[2]))
      for it in gt_h:
        surviving_right.append((it[0], it[1], it[2]))
    else:
      tol = self.config.tolerance or 0.0
      for it in sorted_right:
        if it[0] >= (safe_horizon - tol - 1e-9):
          surviving_right.append((it[0], it[1], it[2]))

    left_pending.clear()
    for it in waiting_left:
      left_pending.add(it)

    right_buf.clear()
    for r_it in surviving_right:
      right_buf.add(r_it)

    next_candidates: list[int] = [it[1] for it in waiting_left]
    if not next_candidates and surviving_right:
      for t_r, _, _ in surviving_right:
        c_ms = self._compute_right_cleanup_ms(t_r)
        if c_ms is not None:
          next_candidates.append(max(fired_ms + 1, c_ms))

    if next_candidates:
      nxt = min(next_candidates)
      next_eval.write(nxt)
      eval_timer.set(beam_timestamp.Timestamp(micros=nxt * 1000))
      cur_wm['evicted_ms'] = fired_ms
      wm_state.write(json.dumps(cur_wm))
    elif surviving_right or cur_wm.get('r_cols'):
      next_eval.clear()
      cur_wm['evicted_ms'] = fired_ms
      wm_state.write(json.dumps(cur_wm))
    else:
      next_eval.clear()
      wm_state.clear()


def cli_asof_join(
    left: Any = None,
    right: Any = None,
    error_handling: dict[str, Any] | None = None,
    **kwargs,
) -> beam.PTransform:
  """Beam YAML provider callable for `type: CliAsofJoin` / `AsofJoin`."""
  cfg_dict = dict(kwargs)
  if left is not None and 'left' not in cfg_dict:
    cfg_dict['left'] = left
  if right is not None and 'right' not in cfg_dict:
    cfg_dict['right'] = right

  base_cfg = parse_asof_join_config(cfg_dict)
  err_tag = (error_handling or {}).get('output', 'errors')
  route_errors = bool(error_handling and 'output' in error_handling)
  if base_cfg.skew_action == 'dlq' and not route_errors:
    raise ValueError(
        'CliAsofJoin skew_action="dlq" requires error_handling / --dlq')

  dlq_row_type = RowTypeConstraint.from_fields([
      ('step', str),
      ('error_type', str),
      ('error', str),
      ('raw_payload', str),
      ('element', str),
  ])

  class _StreamTagDoFn(beam.DoFn):
    def __init__(
        self, side: str, key_cols: tuple[str, ...], ts_col: str | None):
      self.side = side
      self.key_cols = key_cols
      self.ts_col = ts_col
      self.dropped_ctr = beam.metrics.Metrics.counter(
          'beam_cli', 'skew_dropped')

    def process(self, record, elem_ts=beam.DoFn.TimestampParam):
      d = interval_join._to_plain_dict(record)
      try:
        ckey = interval_join.encode_canonical_key_values(
            tuple(d.get(k) for k in self.key_cols))
        if self.ts_col:
          t = interval_join.extract_record_timestamp(d, self.ts_col)
        else:
          t = float(elem_ts)
        yield TimestampedValue((ckey, (self.side, t, d)),
                               beam_timestamp.Timestamp.of(t))
      except Exception as e:  # pylint: disable=broad-except
        if route_errors:
          yield pvalue.TaggedOutput(
              err_tag,
              _make_asof_dlq_row(
                  type(e).__name__,
                  str(e),
                  d,
                  redact_fields=base_cfg.dlq_redact_fields))
        elif base_cfg.skew_action == 'fail':
          raise
        else:
          self.dropped_ctr.inc(1)

  class _CliAsofJoinTransform(beam.PTransform):
    def expand(self, pcolls):
      pcoll_left = None
      pcoll_right = None
      if isinstance(pcolls, dict):
        if 'left' in pcolls and 'right' in pcolls:
          pcoll_left = pcolls['left']
          pcoll_right = pcolls['right']
        elif base_cfg.left_alias in pcolls and base_cfg.right_alias in pcolls:
          pcoll_left = pcolls[base_cfg.left_alias]
          pcoll_right = pcolls[base_cfg.right_alias]
        elif len(pcolls) == 2:
          vals = list(pcolls.values())
          pcoll_left, pcoll_right = vals[0], vals[1]
      elif isinstance(pcolls, (list, tuple)) and len(pcolls) == 2:
        pcoll_left, pcoll_right = pcolls[0], pcolls[1]

      if pcoll_left is None or pcoll_right is None:
        raise ValueError(
            'CliAsofJoin requires exactly 2 input PCollections (left and right), '
            f'got: {pcolls!r}')

      is_bounded = (
          getattr(pcoll_left, 'is_bounded', True) and
          getattr(pcoll_right, 'is_bounded', True))
      if (not is_bounded and base_cfg.direction in ('forward', 'nearest') and
          base_cfg.tolerance is None):
        raise ValueError(
            f'unbounded streaming CliAsofJoin with direction={base_cfg.direction!r} '
            'requires a finite tolerance=DURATION (e.g. tolerance=5m)')

      l_schema_fields = base_cfg.left_fields or interval_join._schema_field_names(
          getattr(pcoll_left, 'element_type', None))
      r_schema_fields = base_cfg.right_fields or interval_join._schema_field_names(
          getattr(pcoll_right, 'element_type', None))
      join_cfg = base_cfg._replace(
          left_fields=l_schema_fields, right_fields=r_schema_fields)

      win_left = pcoll_left | 'GlobalWinLeft' >> beam.WindowInto(
          GlobalWindows())
      win_right = pcoll_right | 'GlobalWinRight' >> beam.WindowInto(
          GlobalWindows())

      dlq_pcolls: list[Any] = []
      if is_bounded and not join_cfg.force_stateful:
        keyed_left = (
            win_left
            | 'KeyLeft' >> beam.Map(
                lambda r: (
                    interval_join.encode_canonical_key_values(
                        interval_join.extract_record_key(r, join_cfg.left_key)),
                    r)).with_output_types(tuple[bytes, Any]))
        keyed_right = (
            win_right
            | 'KeyRight' >> beam.Map(
                lambda r: (
                    interval_join.encode_canonical_key_values(
                        interval_join.extract_record_key(r, join_cfg.right_key)
                    ), r)).with_output_types(tuple[bytes, Any]))
        grouped = {
            'left': keyed_left, 'right': keyed_right
        } | 'CoGroupByKey' >> beam.CoGroupByKey()
        joined_out = (
            grouped
            | 'BoundedAsofJoin' >> beam.ParDo(
                _BoundedAsofJoinDoFn(
                    join_cfg, err_tag=err_tag,
                    route_errors=route_errors)).with_outputs(
                        err_tag, main='good'))
        good = joined_out['good']
        if route_errors:
          dlq_pcolls.append(joined_out[err_tag])
      else:
        t_left = win_left | 'TagLeft' >> beam.ParDo(
            _StreamTagDoFn('L', join_cfg.left_key,
                           join_cfg.left_timestamp)).with_outputs(
                               err_tag, main='good')
        t_right = win_right | 'TagRight' >> beam.ParDo(
            _StreamTagDoFn('R', join_cfg.right_key,
                           join_cfg.right_timestamp)).with_outputs(
                               err_tag, main='good')
        if route_errors:
          dlq_pcolls.extend([t_left[err_tag], t_right[err_tag]])

        typed_left = t_left['good'] | 'TypeLeft' >> beam.Map(
            lambda kv: kv).with_output_types(tuple[bytes, Any])
        typed_right = t_right['good'] | 'TypeRight' >> beam.Map(
            lambda kv: kv).with_output_types(tuple[bytes, Any])
        flattened = (typed_left, typed_right) | 'FlattenTagged' >> beam.Flatten(
        ).with_output_types(tuple[bytes, Any])
        st_out = (
            flattened
            | 'StatefulAsofJoin' >> beam.ParDo(
                AsofJoinStatefulDoFn(
                    join_cfg, err_tag=err_tag,
                    route_errors=route_errors)).with_outputs(
                        err_tag, main='good'))
        good = st_out['good']
        if route_errors:
          dlq_pcolls.append(st_out[err_tag])

      out_row_type = interval_join._infer_joined_row_type(
          getattr(pcoll_left, 'element_type', None),
          getattr(pcoll_right, 'element_type', None),
          _as_interval_projection_config(join_cfg))
      if out_row_type is not None:
        good.element_type = out_row_type

      if route_errors:
        for d_pc in dlq_pcolls:
          d_pc.element_type = dlq_row_type
        bad = (
            dlq_pcolls[0] if len(dlq_pcolls) == 1 else
            (tuple(dlq_pcolls) | 'FlattenAsofJoinDlq' >> beam.Flatten()))
        bad.element_type = dlq_row_type
        return {'good': good, err_tag: bad}
      return good

  return _CliAsofJoinTransform()


CliAsofJoin = cli_asof_join
