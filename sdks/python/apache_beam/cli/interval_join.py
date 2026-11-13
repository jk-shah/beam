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

"""Declarative Event-Time Interval Joins for Beam SQL & Beam YAML (`B1.1`).

Supports:
1. Event-time sliding interval correlation:
   `left.timestamp + lower_bound <= right.timestamp <= left.timestamp + upper_bound`
2. Stateful stream processing on Google Cloud Dataflow Windmill and PrismRunner
   using deterministic `BytesCoder` keys, non-pickle `TupleCoder` state,
   multi-record `NEXT_GC_STATE` timer re-arming, `WATERMARK_STATE` drain cleanup,
   and `TaggedOutput` DLQ routing (`skew_action: dlq`).
3. In-memory / bounded `CoGroupByKey` execution for local runs and SQL queries,
   plus `force_stateful: true` for deterministic stateful testing.
4. Seamless registration as Beam YAML transform (`type: CliIntervalJoin`).
"""

from __future__ import annotations

import base64
import datetime
import decimal
import functools
import json
import math
import re
import struct
import types
from typing import Any
from typing import Iterable
from typing import NamedTuple
from typing import Optional

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.coders import BooleanCoder
from apache_beam.coders import FloatCoder
from apache_beam.coders import StrUtf8Coder
from apache_beam.coders import TupleCoder
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.transforms.userstate import TimeDomain
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import on_timer
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.window import TimestampedValue
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.typehints.schemas import named_fields_from_element_type
from apache_beam.utils import timestamp as beam_timestamp

from . import expressions
from . import output

_DURATION_RE = re.compile(
    r'^\s*(?P<sign>[+-]?)\s*(?P<num>\d+(?:\.\d+)?)\s*(?P<unit>[a-zA-Z]*)\s*$')
_MIN_BEAM_TS = -9_000_000_000_000.0
_MAX_BEAM_TS = 9_000_000_000_000.0


def split_bracket_tokens(raw: str) -> list[str]:
  """Splits comma-separated tokens while respecting `[...]` and quotes."""
  tokens: list[str] = []
  cur: list[str] = []
  depth = 0
  quote: str | None = None
  escaped = False
  for ch in raw:
    if quote is not None:
      cur.append(ch)
      if escaped:
        escaped = False
      elif ch == '\\':
        escaped = True
      elif ch == quote:
        quote = None
      continue
    if ch in ("'", '"'):
      quote = ch
      cur.append(ch)
    elif ch in ('[', '(', '{'):
      depth += 1
      cur.append(ch)
    elif ch in (']', ')', '}'):
      depth = max(0, depth - 1)
      cur.append(ch)
    elif ch == ',' and depth == 0:
      tok = ''.join(cur).strip()
      if tok:
        tokens.append(tok)
      cur = []
    else:
      cur.append(ch)
  tail = ''.join(cur).strip()
  if tail:
    tokens.append(tail)
  return tokens


def parse_column_list(raw: Any) -> tuple[str, ...]:
  """Normalizes `'a,b'`, `'[a,b]'`, or `['a', 'b']` into a tuple of column names."""
  if raw is None:
    return ()
  items: list[str] = []
  seq = [raw] if isinstance(raw, str) else list(raw)
  for entry in seq:
    s = str(entry).strip()
    if len(s) >= 2 and s[0] == '[' and s[-1] == ']':
      s = s[1:-1].strip()
    for part in split_bracket_tokens(s):
      clean = part.strip().strip('\'"')
      if clean:
        items.append(clean)
  return tuple(dict.fromkeys(items))


def _canonical_number_bytes(val: int | float | decimal.Decimal) -> bytes:
  if isinstance(val, float):
    if math.isnan(val):
      return b'NaN'
    if math.isinf(val):
      return b'+Inf' if val > 0 else b'-Inf'
    dec = decimal.Decimal(str(val))
  elif isinstance(val, int):
    dec = decimal.Decimal(val)
  else:
    dec = val
  if dec.is_nan():
    return b'NaN'
  if dec.is_infinite():
    return b'+Inf' if dec > 0 else b'-Inf'
  norm = dec.normalize()
  if norm == 0:
    return b'0'
  return format(norm, 'f').encode('ascii')


def encode_canonical_key_values(values: Iterable[Any]) -> bytes:
  """Encodes composite key values into collision-free deterministic `bytes`."""
  parts: list[bytes] = []
  for val in values:
    if val is None:
      tag, payload = 0, b''
    elif isinstance(val, bool):
      tag, payload = 1, (b'\x01' if val else b'\x00')
    elif isinstance(val, (int, float, decimal.Decimal)):
      tag, payload = 2, _canonical_number_bytes(val)
    elif isinstance(val, str):
      tag, payload = 3, val.encode('utf-8', errors='surrogatepass')
    elif isinstance(val, (bytes, bytearray, memoryview)):
      tag, payload = 4, bytes(val)
    elif hasattr(val, 'to_rfc3339'):
      tag, payload = 5, val.to_rfc3339().encode('utf-8')
    elif hasattr(val, 'isoformat'):
      tag, payload = 5, val.isoformat().encode('utf-8')
    else:
      tag, payload = 6, repr(val).encode('utf-8')
    parts.append(struct.pack('>BI', tag, len(payload)) + payload)
  return b''.join(parts)


def _make_dlq_row(step: str, err_type: str, msg: str, elem: Any) -> beam.Row:
  try:
    raw_json = json.dumps(_to_plain_dict(elem), default=str, sort_keys=True)
  except Exception:  # pylint: disable=broad-except
    raw_json = str(elem)[:4096]
  return beam.Row(
      step=step,
      error_type=err_type,
      error=msg[:4096],
      raw_payload=raw_json,
      element=raw_json,
  )


def parse_interval_duration(raw: str | int | float) -> float:
  """Parses a signed duration string or number into seconds (float)."""
  if isinstance(raw, (int, float)) and not isinstance(raw, bool):
    val = float(raw)
    if not math.isfinite(val):
      raise ValueError(f'interval duration must be finite (got {raw!r})')
    return val
  if not isinstance(raw, str) or not raw.strip():
    raise ValueError(f'invalid empty interval duration: {raw!r}')
  m = _DURATION_RE.match(raw)
  if not m:
    raise ValueError(
        f'invalid interval duration {raw!r}; examples: 0s, 15m, -1m, 900s, 500ms, 1h'
    )
  sign = -1.0 if m.group('sign') == '-' else 1.0
  num = float(m.group('num'))
  raw_unit = (m.group('unit') or 's').lower()
  if raw_unit in ('ms', 'millisecond', 'milliseconds'):
    mult = 0.001
  else:
    unit = raw_unit.rstrip('s') if raw_unit not in ('s',) else 's'
    mult = {
        's': 1.0,
        'sec': 1.0,
        'second': 1.0,
        'm': 60.0,
        'min': 60.0,
        'minute': 60.0,
        'h': 3600.0,
        'hr': 3600.0,
        'hour': 3600.0,
        'd': 86400.0,
        'day': 86400.0,
    }.get(unit)
    if mult is None:
      raise ValueError(f'unknown duration unit {raw_unit!r} in {raw!r}')
  return sign * num * mult


class IntervalJoinConfig(NamedTuple):
  left_alias: str
  right_alias: str
  left_key: tuple[str, ...]
  right_key: tuple[str, ...]
  left_timestamp: str | None
  right_timestamp: str | None
  lower_bound: float
  upper_bound: float
  join_type: str = 'inner'  # 'inner', 'left', 'right', 'full'
  select: dict[str, str] | None = None
  max_watermark_skew: float | None = None
  skew_action: str = 'drop'  # 'drop', 'warn', 'dlq', 'fail'
  left_fields: tuple[str, ...] = ()
  right_fields: tuple[str, ...] = ()
  force_stateful: bool = False

  @property
  def lower_bound_sec(self) -> float:
    return self.lower_bound

  @property
  def upper_bound_sec(self) -> float:
    return self.upper_bound


def parse_interval_join_spec_str(
    spec_str: str,
    *,
    max_watermark_skew: str | None = None,
    skew_action: str | None = None,
    left_alias: str | None = None,
    right_alias: str | None = None,
) -> dict[str, Any]:
  """Parses CLI `--interval-join SPEC` shorthand into a YAML-ready config dict."""
  if not isinstance(spec_str, str) or not spec_str.strip():
    raise ValueError('--interval-join specification cannot be empty')
  cfg: dict[str, Any] = {}
  for token in split_bracket_tokens(spec_str):
    if '=' not in token:
      raise ValueError(
          f'invalid --interval-join token {token!r}; expected key=value '
          '(wrap composite keys in brackets, e.g. key=[tenant_id,order_id])')
    k_raw, v_raw = token.split('=', 1)
    k = k_raw.strip().lower()
    v = v_raw.strip()
    if not k or not v:
      raise ValueError(f'empty key or value in --interval-join token {token!r}')
    if k in ('key', 'left_key', 'right_key'):
      cols = list(parse_column_list(v))
      cfg[k] = cols[0] if len(cols) == 1 else cols
    elif k in ('lower', 'lower_bound'):
      cfg['lower_bound'] = v
    elif k in ('upper', 'upper_bound'):
      cfg['upper_bound'] = v
    elif k in ('type', 'join_type'):
      cfg['join_type'] = v
    elif k in ('left_time', 'left_timestamp'):
      cfg['left_time'] = v
    elif k in ('right_time', 'right_timestamp'):
      cfg['right_time'] = v
    elif k == 'timestamp':
      cfg['timestamp'] = v
    elif k in ('left', 'left_alias'):
      cfg['left_alias'] = v
    elif k in ('right', 'right_alias'):
      cfg['right_alias'] = v
    elif k in ('max_skew', 'max_watermark_skew'):
      cfg['max_watermark_skew'] = v
    elif k == 'skew_action':
      cfg['skew_action'] = v
    elif k == 'force_stateful':
      cfg['force_stateful'] = v.lower() in ('1', 'true', 'yes')
    else:
      raise ValueError(f'unknown --interval-join option {k_raw.strip()!r}')

  if left_alias and 'left_alias' not in cfg:
    cfg['left_alias'] = left_alias
  if right_alias and 'right_alias' not in cfg:
    cfg['right_alias'] = right_alias
  if max_watermark_skew is not None:
    cfg['max_watermark_skew'] = max_watermark_skew
  if skew_action is not None:
    cfg['skew_action'] = skew_action
  # Validate eagerly so invalid CLI flags fail immediately at build time.
  parse_interval_join_config(cfg)
  return cfg


def parse_interval_join_config(cfg: dict[str, Any]) -> IntervalJoinConfig:
  """Validates and parses interval join configuration for Beam YAML and CLI pipelines."""
  left_alias = str(cfg.get('left_alias') or cfg.get('left') or 'left').strip()
  right_alias = str(cfg.get('right_alias') or cfg.get('right') or
                    'right').strip()

  key_raw = cfg.get('key')
  left_key_raw = cfg.get('left_key') if cfg.get(
      'left_key') is not None else key_raw
  right_key_raw = cfg.get('right_key') if cfg.get(
      'right_key') is not None else key_raw

  left_key = parse_column_list(left_key_raw)
  right_key = parse_column_list(right_key_raw)
  if not left_key or not right_key:
    raise ValueError(
        'interval join requires join key(s); specify key: "col" or '
        'left_key: "l_col", right_key: "r_col"')
  if len(left_key) != len(right_key):
    raise ValueError(
        f'mismatched join key lengths: left has {len(left_key)} key(s) {left_key}, '
        f'right has {len(right_key)} key(s) {right_key}')

  left_ts = cfg.get('left_timestamp') or cfg.get('timestamp') or cfg.get('left_time')
  right_ts = cfg.get('right_timestamp') or cfg.get('timestamp') or cfg.get('right_time')
  if not left_ts or not right_ts:
    raise ValueError(
        'interval join requires timestamp column(s); specify left_time and right_time or timestamp'
    )

  lower_raw = cfg.get('lower_bound', cfg.get('lower', 0.0))
  upper_raw = cfg.get('upper_bound', cfg.get('upper'))
  if upper_raw is None:
    raise ValueError(
        'interval join requires upper_bound (e.g. upper_bound: "15m")')

  lower_bound = parse_interval_duration(lower_raw)
  upper_bound = parse_interval_duration(upper_raw)
  if lower_bound > upper_bound:
    raise ValueError(
        f'invalid interval bounds: lower_bound ({lower_bound}s) cannot be '
        f'greater than upper_bound ({upper_bound}s)')

  join_type = str(cfg.get('join_type') or cfg.get('type') or
                  'inner').strip().lower()
  if join_type in ('outer', 'full_outer'):
    join_type = 'full'
  elif join_type == 'left_outer':
    join_type = 'left'
  elif join_type == 'right_outer':
    join_type = 'right'
  if join_type not in ('inner', 'left', 'right', 'full'):
    raise ValueError(
        f'unsupported join_type {join_type!r}; expected "inner", "left", '
        '"right", or "full"')

  select = cfg.get('select')
  if select is not None:
    if not isinstance(select, dict):
      raise ValueError('select must be a mapping of output_field: expression')
    for out_k, expr_val in select.items():
      if not output.SAFE_IDENTIFIER_RE.match(str(out_k)):
        raise ValueError(f'invalid select output column {out_k!r}')
      if not isinstance(expr_val, str) or not expr_val.strip():
        raise ValueError(
            f'select expression for {out_k!r} must be a non-empty string')
      _compile_join_select_expr(expr_val.strip())

  max_skew_raw = (
      cfg.get('max_watermark_skew') or cfg.get('max_skew') or
      cfg.get('watermark_skew_breaker') or
      cfg.get('max_watermark_skew_duration'))
  max_watermark_skew = (
      parse_interval_duration(max_skew_raw)
      if max_skew_raw is not None else None)
  if max_watermark_skew is not None and max_watermark_skew <= 0:
    raise ValueError(
        f'max_watermark_skew must be positive (got {max_watermark_skew}s)')

  skew_action = str(cfg.get('skew_action') or 'drop').strip().lower()
  if skew_action not in ('drop', 'warn', 'dlq', 'fail'):
    raise ValueError(
        f'skew_action must be "drop", "warn", "dlq", or "fail" (got {skew_action!r})'
    )

  left_fields = parse_column_list(cfg.get('left_fields'))
  right_fields = parse_column_list(cfg.get('right_fields'))
  force_stateful = bool(cfg.get('force_stateful'))

  return IntervalJoinConfig(
      left_alias=left_alias,
      right_alias=right_alias,
      left_key=left_key,
      right_key=right_key,
      left_timestamp=str(left_ts).strip() if left_ts else None,
      right_timestamp=str(right_ts).strip() if right_ts else None,
      lower_bound=lower_bound,
      upper_bound=upper_bound,
      join_type=join_type,
      select=dict(select) if select else None,
      max_watermark_skew=max_watermark_skew,
      skew_action=skew_action,
      left_fields=left_fields,
      right_fields=right_fields,
      force_stateful=force_stateful,
  )


def extract_record_key(
    elem: Any,
    key_fields: str | tuple[str, ...] | list[str]) -> tuple[Any, ...]:
  """Extracts composite key tuple from a Row or dict."""
  fields = (key_fields, ) if isinstance(key_fields, str) else tuple(key_fields)
  d = _to_plain_dict(elem)
  return tuple(d.get(k) for k in fields)


def extract_record_timestamp(elem: Any, ts_field: str | None) -> float:
  """Extracts event timestamp in epoch seconds (float) from a Row or dict."""
  if not ts_field:
    raise ValueError('timestamp column name is required')
  d = _to_plain_dict(elem)
  if ts_field not in d or d[ts_field] is None:
    raise ValueError(f'missing or null timestamp column {ts_field!r}')
  val = d[ts_field]

  if isinstance(val, bool):
    raise ValueError(f'boolean value is not a valid timestamp in {ts_field!r}')
  if isinstance(val, (int, float, decimal.Decimal)):
    f = float(val)
    if not math.isfinite(f):
      raise ValueError(
          f'timestamp column {ts_field!r} must be a finite numeric value, got {val!r}'
      )
    return f / 1000.0 if abs(f) > 1e11 else f
  if isinstance(val, datetime.datetime):
    if val.tzinfo is None:
      val = val.replace(tzinfo=datetime.timezone.utc)
    return val.timestamp()

  s = str(val).strip()
  try:
    if re.fullmatch(r'[+-]?\d+(?:\.\d+)?', s):
      f = float(s)
      if not math.isfinite(f):
        raise ValueError('non-finite timestamp')
      return f / 1000.0 if abs(f) > 1e11 else f
    dt = datetime.datetime.fromisoformat(s.replace('Z', '+00:00'))
    if dt.tzinfo is None:
      dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.timestamp()
  except Exception as e:
    raise ValueError(f'cannot parse timestamp value {val!r}: {e}') from None


def _to_plain_dict(elem: Any) -> dict[str, Any]:
  if elem is None:
    return {}
  if isinstance(elem, dict):
    return dict(elem)
  if hasattr(elem, '_asdict'):
    return dict(elem._asdict())
  if hasattr(elem, '__dict__'):
    return {k: v for k, v in elem.__dict__.items() if not k.startswith('_')}
  return {'value': elem}


def _encode_state_payload(d: dict[str, Any]) -> str:
  """Deterministic non-pickle JSON serializer for Windmill state payloads."""
  def _enc(v: Any) -> Any:
    if v is None or isinstance(v, (bool, int, str)):
      return v
    if isinstance(v, float):
      if math.isnan(v) or math.isinf(v):
        return {'__beam_float__': str(v)}
      return v
    if isinstance(v, decimal.Decimal):
      return {'__beam_dec__': str(v)}
    if isinstance(v, (bytes, bytearray, memoryview)):
      return {'__beam_b64__': base64.b64encode(bytes(v)).decode('ascii')}
    if isinstance(v, datetime.datetime):
      return {'__beam_dt__': v.isoformat()}
    if isinstance(v, dict):
      return {str(k): _enc(val) for k, val in v.items()}
    if isinstance(v, (list, tuple)):
      return [_enc(x) for x in v]
    return str(v)

  return json.dumps(_enc(d), separators=(',', ':'), sort_keys=True)


def _decode_state_payload(raw: str) -> dict[str, Any]:
  def _hook(obj: dict[str, Any]) -> Any:
    if len(obj) == 1:
      if '__beam_float__' in obj:
        return float(obj['__beam_float__'])
      if '__beam_dec__' in obj:
        return decimal.Decimal(obj['__beam_dec__'])
      if '__beam_b64__' in obj:
        return base64.b64decode(obj['__beam_b64__'])
      if '__beam_dt__' in obj:
        return datetime.datetime.fromisoformat(obj['__beam_dt__'])
    return obj

  res = json.loads(raw, object_hook=_hook)
  return res if isinstance(res, dict) else {}


@functools.lru_cache(maxsize=256)
def _compile_join_select_expr(expr: str) -> Any:
  fn, _ = expressions.compile_scalar_callable(expr)
  return fn


class _NullDefaultNamespace(types.SimpleNamespace):
  """Namespace returning `None` for missing public fields on outer-join rows."""

  def __getattr__(self, name: str) -> Any:
    if name.startswith('_'):
      raise AttributeError(name)
    return None


def _to_namespace(val: Any) -> Any:
  if isinstance(val, dict):
    return _NullDefaultNamespace(
        **{
            str(k): _to_namespace(v)
            for k, v in val.items()
            if isinstance(k, str) and not k.startswith('_')
        })
  return val


def project_joined_row(
    left_elem: Any | None,
    right_elem: Any | None,
    config: IntervalJoinConfig,
    *,
    left_cols: Iterable[str] = (),
    right_cols: Iterable[str] = (),
) -> dict[str, Any]:
  """Projects fields from joined left and right records into a unified result dict."""
  left_dict = _to_plain_dict(left_elem)
  right_dict = _to_plain_dict(right_elem)

  if config.select:
    left_ns = _to_namespace(left_dict)
    right_ns = _to_namespace(right_dict)
    ctx: dict[str, Any] = {
        'left': left_ns,
        'right': right_ns,
        config.left_alias: left_ns,
        config.right_alias: right_ns,
        **{
            f'left_{k}': _to_namespace(v)
            for k, v in left_dict.items()
        },
        **{
            f'right_{k}': _to_namespace(v)
            for k, v in right_dict.items()
        },
    }
    for k, v in left_dict.items():
      if k not in ctx:
        ctx[k] = _to_namespace(v)
    for k, v in right_dict.items():
      if k not in ctx and k not in left_dict:
        ctx[k] = _to_namespace(v)
    row_ns = _NullDefaultNamespace(**ctx)

    out: dict[str, Any] = {}
    for out_k, expr_str in config.select.items():
      expr = expr_str.strip()
      if '.' in expr:
        parts = expr.split('.')
        if len(parts) == 2 and all(output.SAFE_IDENTIFIER_RE.match(p)
                                   for p in parts):
          side, field = parts
          if side in ('left', config.left_alias):
            out[out_k] = left_dict.get(field)
            continue
          if side in ('right', config.right_alias):
            out[out_k] = right_dict.get(field)
            continue
      if expr in left_dict:
        out[out_k] = left_dict[expr]
        continue
      if expr in right_dict:
        out[out_k] = right_dict[expr]
        continue
      fn = _compile_join_select_expr(expr)
      try:
        out[out_k] = fn(row_ns)
      except Exception:
        out[out_k] = None
    return out

  # Default projection: merge dictionaries, prefixing collisions and padding
  # missing partner columns on outer joins with explicit `None`.
  all_l_cols = list(
      dict.fromkeys(
          list(config.left_fields) + list(left_cols) + list(left_dict.keys())))
  all_r_cols = list(
      dict.fromkeys(
          list(config.right_fields) + list(right_cols) +
          list(right_dict.keys())))

  out = {}
  for idx, lk in enumerate(config.left_key):
    rk = config.right_key[idx]
    if lk in left_dict:
      out[lk] = left_dict[lk]
    elif rk in right_dict:
      out[lk] = right_dict[rk]
    else:
      out[lk] = None

  conflicts = (set(all_l_cols) & set(all_r_cols)) - set(config.left_key)
  for k in all_l_cols:
    if k in config.left_key:
      continue
    target_k = f'{config.left_alias}_{k}' if k in conflicts else k
    out[target_k] = left_dict.get(k)

  for k in all_r_cols:
    if k in config.right_key:
      continue
    target_k = f'{config.right_alias}_{k}' if k in conflicts else k
    out[target_k] = right_dict.get(k)

  return out


def join_bounded_records(
    left_records: Iterable[Any],
    right_records: Iterable[Any],
    config: IntervalJoinConfig,
) -> list[dict[str, Any]]:
  """Executes an in-memory temporal interval join over bounded collections."""
  left_list_raw = list(left_records)
  right_list_raw = list(right_records)

  left_cols: list[str] = list(config.left_fields)
  right_cols: list[str] = list(config.right_fields)
  for r in left_list_raw:
    for k in _to_plain_dict(r):
      if k not in left_cols:
        left_cols.append(k)
  for r in right_list_raw:
    for k in _to_plain_dict(r):
      if k not in right_cols:
        right_cols.append(k)

  left_by_key: dict[bytes, list[list[Any]]] = {}
  right_by_key: dict[bytes, list[list[Any]]] = {}

  for r in left_list_raw:
    k = encode_canonical_key_values(
        extract_record_key(r, config.left_key))
    ts = extract_record_timestamp(r, config.left_timestamp)
    left_by_key.setdefault(k, []).append([ts, r, False])

  for r in right_list_raw:
    k = encode_canonical_key_values(
        extract_record_key(r, config.right_key))
    ts = extract_record_timestamp(r, config.right_timestamp)
    right_by_key.setdefault(k, []).append([ts, r, False])

  all_keys = list(
      dict.fromkeys(list(left_by_key.keys()) + list(right_by_key.keys())))
  results: list[dict[str, Any]] = []

  for k in all_keys:
    l_list = left_by_key.get(k, [])
    r_list = right_by_key.get(k, [])

    for l_entry in l_list:
      t_l, l_elem, _ = l_entry
      for r_entry in r_list:
        t_r, r_elem, _ = r_entry
        if (config.max_watermark_skew is not None and
            abs(t_r - t_l) > config.max_watermark_skew):
          continue
        if (t_l + config.lower_bound) <= t_r <= (t_l + config.upper_bound):
          l_entry[2] = True
          r_entry[2] = True
          results.append(
              project_joined_row(
                  l_elem,
                  r_elem,
                  config,
                  left_cols=left_cols,
                  right_cols=right_cols))

    if config.join_type in ('left', 'full'):
      for l_entry in l_list:
        if not l_entry[2]:
          results.append(
              project_joined_row(
                  l_entry[1],
                  None,
                  config,
                  left_cols=left_cols,
                  right_cols=right_cols))

    if config.join_type in ('right', 'full'):
      for r_entry in r_list:
        if not r_entry[2]:
          results.append(
              project_joined_row(
                  None,
                  r_entry[1],
                  config,
                  left_cols=left_cols,
                  right_cols=right_cols))

  return results


class _BoundedIntervalJoinDoFn(beam.DoFn):
  """Executes interval join over CoGroupByKey batches in bounded execution."""
  def __init__(
      self,
      config: IntervalJoinConfig,
      err_tag: str = 'errors',
      route_errors: bool = False,
  ):
    self.config = config
    self.err_tag = err_tag
    self.route_errors = route_errors
    self.cli_skew_dropped = beam.metrics.Metrics.counter(
        'beam_cli', 'skew_dropped')

  def process(self, element):
    _, grouped = element
    raw_left = list(grouped.get('left', []))
    raw_right = list(grouped.get('right', []))

    valid_left = []
    for r in raw_left:
      try:
        t = extract_record_timestamp(r, self.config.left_timestamp)
        valid_left.append((t, r))
      except Exception as e:  # pylint: disable=broad-except
        if self.route_errors or self.config.skew_action == 'dlq':
          if not self.route_errors:
            raise
          yield pvalue.TaggedOutput(
              self.err_tag,
              _make_dlq_row(
                  'CliIntervalJoin', type(e).__name__, str(e), r))
        elif self.config.skew_action == 'fail':
          raise
        else:
          self.cli_skew_dropped.inc(1)

    valid_right = []
    for r in raw_right:
      try:
        t = extract_record_timestamp(r, self.config.right_timestamp)
        valid_right.append((t, r))
      except Exception as e:  # pylint: disable=broad-except
        if self.route_errors or self.config.skew_action == 'dlq':
          if not self.route_errors:
            raise
          yield pvalue.TaggedOutput(
              self.err_tag,
              _make_dlq_row(
                  'CliIntervalJoin', type(e).__name__, str(e), r))
        elif self.config.skew_action == 'fail':
          raise
        else:
          self.cli_skew_dropped.inc(1)

    # Check skew drops/DLQs explicitly on bounded runs when configured
    if self.config.max_watermark_skew is not None and valid_left and valid_right:
      max_l = max(t for t, _ in valid_left)
      max_r = max(t for t, _ in valid_right)
      kept_left = []
      for t_l, r in valid_left:
        if (max_r - t_l) > self.config.max_watermark_skew:
          if self.config.skew_action == 'dlq':
            if not self.route_errors:
              raise ValueError(
                  'skew_action="dlq" requires --dlq / error_handling')
            yield pvalue.TaggedOutput(
                self.err_tag,
                _make_dlq_row(
                    'CliIntervalJoin',
                    'WatermarkSkewError',
                    f'left record timestamp {t_l} lagged right max {max_r} by '
                    f'> {self.config.max_watermark_skew}s',
                    r))
            continue
          if self.config.skew_action == 'fail':
            raise ValueError(
                f'watermark skew exceeded {self.config.max_watermark_skew}s')
          if self.config.skew_action == 'drop':
            self.cli_skew_dropped.inc(1)
            continue
        kept_left.append((t_l, r))
      kept_right = []
      for t_r, r in valid_right:
        if (max_l - t_r) > self.config.max_watermark_skew:
          if self.config.skew_action == 'dlq':
            if not self.route_errors:
              raise ValueError(
                  'skew_action="dlq" requires --dlq / error_handling')
            yield pvalue.TaggedOutput(
                self.err_tag,
                _make_dlq_row(
                    'CliIntervalJoin',
                    'WatermarkSkewError',
                    f'right record timestamp {t_r} lagged left max {max_l} by '
                    f'> {self.config.max_watermark_skew}s',
                    r))
            continue
          if self.config.skew_action == 'fail':
            raise ValueError(
                f'watermark skew exceeded {self.config.max_watermark_skew}s')
          if self.config.skew_action == 'drop':
            self.cli_skew_dropped.inc(1)
            continue
        kept_right.append((t_r, r))
      valid_left = kept_left
      valid_right = kept_right

    no_skew_cfg = self.config._replace(max_watermark_skew=None)
    for row_dict in join_bounded_records([r for _, r in valid_left],
                                         [r for _, r in valid_right],
                                         no_skew_cfg):
      yield beam.Row(**row_dict)


_BUFFER_ENTRY_CODER = TupleCoder((FloatCoder(), StrUtf8Coder(), BooleanCoder()))


class IntervalJoinStatefulDoFn(beam.DoFn):
  """Stateful streaming DoFn executing interval joins on Dataflow Windmill & Prism."""

  LEFT_BUFFER = BagStateSpec('left_buffer', _BUFFER_ENTRY_CODER)
  RIGHT_BUFFER = BagStateSpec('right_buffer', _BUFFER_ENTRY_CODER)
  WATERMARK_STATE = ReadModifyWriteStateSpec('wm_state', StrUtf8Coder())
  NEXT_GC_STATE = ReadModifyWriteStateSpec('next_gc', FloatCoder())
  GC_TIMER = TimerSpec('gc_timer', TimeDomain.WATERMARK)

  def __init__(
      self,
      config: IntervalJoinConfig,
      err_tag: str = 'errors',
      route_errors: bool = False,
  ):
    self.config = config
    self.err_tag = err_tag
    self.route_errors = route_errors
    self.skew_dropped_counter = beam.metrics.Metrics.counter(
        'IntervalJoin', 'watermark_skew_dropped')
    self.skew_warn_counter = beam.metrics.Metrics.counter(
        'IntervalJoin', 'watermark_skew_warn')
    self.cli_skew_dropped_counter = beam.metrics.Metrics.counter(
        'beam_cli', 'skew_dropped')
    self.payload_lag_counter = beam.metrics.Metrics.counter(
        'beam_cli', 'interval_join_payload_lag_rows')

  def _handle_bad_or_skewed(
      self, err_type: str, msg: str, payload: Any, *, is_skew: bool = False):
    action = self.config.skew_action
    if action == 'dlq' or (not is_skew and self.route_errors):
      if not self.route_errors:
        raise ValueError(f'{err_type}: {msg} (configure --dlq to quarantine)')
      return pvalue.TaggedOutput(
          self.err_tag,
          _make_dlq_row('CliIntervalJoin', err_type, msg, payload))
    if action == 'fail':
      raise ValueError(f'{err_type}: {msg}')
    self.skew_dropped_counter.inc(1)
    self.cli_skew_dropped_counter.inc(1)
    return None

  def process(
      self,
      element,
      elem_ts=beam.DoFn.TimestampParam,
      left_buf=beam.DoFn.StateParam(LEFT_BUFFER),
      right_buf=beam.DoFn.StateParam(RIGHT_BUFFER),
      timer=beam.DoFn.TimerParam(GC_TIMER),
      wm_state=beam.DoFn.StateParam(WATERMARK_STATE),
      next_gc=beam.DoFn.StateParam(NEXT_GC_STATE),
  ):
    _, (tag, t, row_payload) = element
    if tag == 'ERR':
      # Pre-extraction error forwarded from upstream stream tagger
      dlq_out = self._handle_bad_or_skewed(
          'InvalidTimestampError', str(t), row_payload, is_skew=False)
      if dlq_out is not None:
        yield dlq_out
      return

    if t is None or not isinstance(t, (int, float)) or not math.isfinite(t):
      dlq_out = self._handle_bad_or_skewed(
          'InvalidTimestampError',
          f'non-finite event timestamp {t!r}',
          row_payload,
          is_skew=False)
      if dlq_out is not None:
        yield dlq_out
      return

    deadline = ((t + self.config.upper_bound + 1.0) if tag == 'L' else
                (t - self.config.lower_bound + 1.0))
    if not (_MIN_BEAM_TS < t < _MAX_BEAM_TS and
            _MIN_BEAM_TS < deadline < _MAX_BEAM_TS):
      dlq_out = self._handle_bad_or_skewed(
          'InvalidTimestampError',
          f'event timestamp {t!r} outside valid Beam watermark bounds',
          row_payload,
          is_skew=False)
      if dlq_out is not None:
        yield dlq_out
      return

    try:
      runner_ts = float(elem_ts)
      if math.isfinite(runner_ts) and runner_ts > deadline:
        self.payload_lag_counter.inc(1)
    except Exception:  # pylint: disable=broad-except
      pass

    raw_wm = wm_state.read()
    cur_wm = json.loads(raw_wm) if raw_wm else {
        'L': None,
        'R': None,
        'dropped': 0,
        'evicted_wm': None,
    }
    max_skew = self.config.max_watermark_skew

    if max_skew is not None:
      if tag == 'L':
        r_max = cur_wm.get('R')
        if r_max is not None:
          if (r_max - t) > max_skew:
            if self.config.skew_action == 'warn':
              self.skew_warn_counter.inc(1)
            else:
              if self.config.skew_action == 'drop':
                cur_wm['dropped'] = cur_wm.get('dropped', 0) + 1
                wm_state.write(json.dumps(cur_wm))
              dlq_out = self._handle_bad_or_skewed(
                  'WatermarkSkewError',
                  f'left timestamp {t} lagged right max {r_max} by > {max_skew}s',
                  row_payload,
                  is_skew=True)
              if dlq_out is not None:
                yield dlq_out
              return
          elif (t - r_max) > max_skew:
            self.skew_warn_counter.inc(1)
        prev_l = cur_wm.get('L')
        cur_wm['L'] = t if prev_l is None else max(prev_l, t)
        wm_state.write(json.dumps(cur_wm))
      elif tag == 'R':
        l_max = cur_wm.get('L')
        if l_max is not None:
          if (l_max - t) > max_skew:
            if self.config.skew_action == 'warn':
              self.skew_warn_counter.inc(1)
            else:
              if self.config.skew_action == 'drop':
                cur_wm['dropped'] = cur_wm.get('dropped', 0) + 1
                wm_state.write(json.dumps(cur_wm))
              dlq_out = self._handle_bad_or_skewed(
                  'WatermarkSkewError',
                  f'right timestamp {t} lagged left max {l_max} by > {max_skew}s',
                  row_payload,
                  is_skew=True)
              if dlq_out is not None:
                yield dlq_out
              return
          elif (t - l_max) > max_skew:
            self.skew_warn_counter.inc(1)
        prev_r = cur_wm.get('R')
        cur_wm['R'] = t if prev_r is None else max(prev_r, t)
        wm_state.write(json.dumps(cur_wm))

    evicted_wm = cur_wm.get('evicted_wm')
    already_past_gc = (
        evicted_wm is not None and
        ((tag == 'L' and (t + self.config.upper_bound) < evicted_wm) or
         (tag == 'R' and (t - self.config.lower_bound) < evicted_wm)))

    encoded_self = _encode_state_payload(row_payload)

    if tag == 'L':
      existing_right = list(right_buf.read())
      updated_right = []
      left_matched = False
      for (t_r, r_json, r_matched) in existing_right:
        if (t + self.config.lower_bound) <= t_r <= (t + self.config.upper_bound):
          left_matched = True
          r_payload = _decode_state_payload(r_json)
          updated_right.append((t_r, r_json, True))
          yield TimestampedValue(
              beam.Row(
                  **project_joined_row(row_payload, r_payload, self.config)),
              beam_timestamp.Timestamp.of(t))
        else:
          updated_right.append((t_r, r_json, r_matched))

      if existing_right:
        right_buf.clear()
        for item in updated_right:
          right_buf.add(item)

      if already_past_gc:
        # Late arrival after GC horizon: do not buffer or re-emit outer rows.
        if not left_matched and self.config.skew_action == 'dlq' and self.route_errors:
          yield pvalue.TaggedOutput(
              self.err_tag,
              _make_dlq_row(
                  'CliIntervalJoin',
                  'LateArrivalError',
                  f'late left record at {t} arrived after GC watermark {evicted_wm}',
                  row_payload))
        return

      left_buf.add((t, encoded_self, left_matched))

    elif tag == 'R':
      existing_left = list(left_buf.read())
      updated_left = []
      right_matched = False
      for (t_l, l_json, l_matched) in existing_left:
        if (t_l + self.config.lower_bound) <= t <= (t_l + self.config.upper_bound):
          right_matched = True
          l_payload = _decode_state_payload(l_json)
          updated_left.append((t_l, l_json, True))
          yield TimestampedValue(
              beam.Row(
                  **project_joined_row(l_payload, row_payload, self.config)),
              beam_timestamp.Timestamp.of(t_l))
        else:
          updated_left.append((t_l, l_json, l_matched))

      if existing_left:
        left_buf.clear()
        for item in updated_left:
          left_buf.add(item)

      if already_past_gc:
        if not right_matched and self.config.skew_action == 'dlq' and self.route_errors:
          yield pvalue.TaggedOutput(
              self.err_tag,
              _make_dlq_row(
                  'CliIntervalJoin',
                  'LateArrivalError',
                  f'late right record at {t} arrived after GC watermark {evicted_wm}',
                  row_payload))
        return

      right_buf.add((t, encoded_self, right_matched))

    cur_next = next_gc.read()
    if cur_next is None or deadline < cur_next:
      next_gc.write(deadline)
      timer.set(beam_timestamp.Timestamp.of(deadline))

  @on_timer(GC_TIMER)
  def on_gc(
      self,
      timestamp=beam.DoFn.TimestampParam,
      left_buf=beam.DoFn.StateParam(LEFT_BUFFER),
      right_buf=beam.DoFn.StateParam(RIGHT_BUFFER),
      timer=beam.DoFn.TimerParam(GC_TIMER),
      wm_state=beam.DoFn.StateParam(WATERMARK_STATE),
      next_gc=beam.DoFn.StateParam(NEXT_GC_STATE),
  ):
    watermark = float(timestamp)
    min_surviving_deadline: float | None = None

    left_items = list(left_buf.read())
    left_buf.clear()
    for (t_l, l_json, l_matched) in left_items:
      if (t_l + self.config.upper_bound) < watermark:
        if self.config.join_type in ('left', 'full') and not l_matched:
          l_payload = _decode_state_payload(l_json)
          yield TimestampedValue(
              beam.Row(**project_joined_row(l_payload, None, self.config)),
              beam_timestamp.Timestamp.of(t_l))
      else:
        left_buf.add((t_l, l_json, l_matched))
        dl = t_l + self.config.upper_bound + 1.0
        if min_surviving_deadline is None or dl < min_surviving_deadline:
          min_surviving_deadline = dl

    right_items = list(right_buf.read())
    right_buf.clear()
    for (t_r, r_json, r_matched) in right_items:
      if (t_r - self.config.lower_bound) < watermark:
        if self.config.join_type in ('right', 'full') and not r_matched:
          r_payload = _decode_state_payload(r_json)
          yield TimestampedValue(
              beam.Row(**project_joined_row(None, r_payload, self.config)),
              beam_timestamp.Timestamp.of(t_r))
      else:
        right_buf.add((t_r, r_json, r_matched))
        dl = t_r - self.config.lower_bound + 1.0
        if min_surviving_deadline is None or dl < min_surviving_deadline:
          min_surviving_deadline = dl

    if min_surviving_deadline is not None:
      raw_wm = wm_state.read()
      cur_wm = json.loads(raw_wm) if raw_wm else {
          'L': None,
          'R': None,
          'dropped': 0,
      }
      prev_ev = cur_wm.get('evicted_wm')
      cur_wm['evicted_wm'] = (
          watermark if prev_ev is None else max(prev_ev, watermark))
      wm_state.write(json.dumps(cur_wm))
      next_gc.write(min_surviving_deadline)
      timer.set(beam_timestamp.Timestamp.of(min_surviving_deadline))
    else:
      # Both buffers completely drained: clear all per-key Windmill state!
      next_gc.clear()
      wm_state.clear()


def cli_assign_timestamps(
    timestamp_col: str,
    error_handling: dict[str, Any] | None = None,
) -> beam.PTransform:
  """Safe event-time assigner supporting ISO-8601 strings, epoch sec/ms, & DLQ."""
  col = str(timestamp_col or '').strip()
  if not col:
    raise ValueError('CliAssignTimestamps requires timestamp_col')
  err_tag = (error_handling or {}).get('output', 'errors')
  route_errors = bool(error_handling and 'output' in error_handling)
  dlq_row_type = RowTypeConstraint.from_fields([
      ('step', str),
      ('error_type', str),
      ('error', str),
      ('raw_payload', str),
      ('element', str),
  ])

  class _AssignTsDoFn(beam.DoFn):
    def process(self, element):
      try:
        t = extract_record_timestamp(element, col)
        if not (_MIN_BEAM_TS < t < _MAX_BEAM_TS):
          raise ValueError(f'timestamp {t} outside Beam bounds')
        yield TimestampedValue(element, beam_timestamp.Timestamp.of(t))
      except Exception as e:  # pylint: disable=broad-except
        if route_errors:
          yield pvalue.TaggedOutput(
              err_tag,
              _make_dlq_row(
                  'CliAssignTimestamps', type(e).__name__, str(e), element))
        else:
          raise

  class _AssignTsTransform(beam.PTransform):
    def expand(self, pcoll):
      in_type = getattr(pcoll, 'element_type', None)
      out = pcoll | 'AssignEventTimestamps' >> beam.ParDo(
          _AssignTsDoFn()).with_outputs(
              err_tag, main='good')
      good = out['good']
      if in_type is not None:
        good.element_type = in_type
      if route_errors:
        bad = out[err_tag]
        bad.element_type = dlq_row_type
        return {'good': good, err_tag: bad}
      return good

  return _AssignTsTransform()


def _schema_field_names(element_type: Any) -> tuple[str, ...]:
  if element_type is None:
    return ()
  try:
    return tuple(
        name for name, _ in named_fields_from_element_type(element_type))
  except Exception:  # pylint: disable=broad-except
    return ()


def _make_optional_type(tp: Any) -> Any:
  if tp is None or tp is Any:
    return Optional[str]
  if isinstance(tp, RowTypeConstraint):
    return Optional[tp.user_type] if getattr(tp, 'user_type',
                                             None) else Optional[str]
  try:
    return Optional[tp]
  except TypeError:
    return Optional[str]


def _infer_select_expr_type(
    expr_str: str,
    l_map: dict[str, Any],
    r_map: dict[str, Any],
    config: IntervalJoinConfig,
) -> Any:
  expr = expr_str.strip()
  if '.' in expr:
    parts = expr.split('.')
    if len(parts) == 2:
      side, field = parts[0].strip(), parts[1].strip()
      if side in ('left', config.left_alias) and field in l_map:
        return _make_optional_type(l_map[field])
      if side in ('right', config.right_alias) and field in r_map:
        return _make_optional_type(r_map[field])
  if expr in l_map:
    return _make_optional_type(l_map[expr])
  if expr in r_map:
    return _make_optional_type(r_map[expr])
  low = expr.lower()
  if re.match(r'^int\s*\(', low) or re.search(
      r'\bas\s+(?:int|int64|integer|bigint)\s*\)', low):
    return Optional[int]
  if re.match(r'^(?:float|round|abs)\s*\(', low) or re.search(
      r'\bas\s+(?:float|float64|double|numeric)\s*\)', low):
    return Optional[float]
  if re.match(r'^bool\s*\(', low) or re.search(r'\bas\s+(?:bool|boolean)\s*\)',
                                               low):
    return Optional[bool]
  if re.match(r'^(?:str|lower|upper|strip|trim)\s*\(', low) or re.search(
      r'\bas\s+(?:str|string|text|varchar)\s*\)', low):
    return Optional[str]
  if any(op in expr for op in ('+', '-', '*', '/')):
    return Optional[float]
  return Optional[str]


def _infer_joined_row_type(
    left_type: Any,
    right_type: Any,
    config: IntervalJoinConfig,
) -> Any | None:
  if left_type is None or right_type is None:
    return None
  try:
    l_pairs = list(named_fields_from_element_type(left_type))
    r_pairs = list(named_fields_from_element_type(right_type))
  except Exception:  # pylint: disable=broad-except
    return None
  if not l_pairs or not r_pairs:
    return None
  l_map = dict(l_pairs)
  r_map = dict(r_pairs)

  fields: list[tuple[str, Any]] = []
  if config.select:
    for out_k, expr_str in config.select.items():
      fields.append(
          (str(out_k), _infer_select_expr_type(expr_str, l_map, r_map, config)))
    return RowTypeConstraint.from_fields(fields)

  all_l_cols = list(
      dict.fromkeys(list(config.left_fields) + [c for c, _ in l_pairs]))
  all_r_cols = list(
      dict.fromkeys(list(config.right_fields) + [c for c, _ in r_pairs]))

  for idx, lk in enumerate(config.left_key):
    rk = config.right_key[idx]
    base_t = l_map.get(lk) or r_map.get(rk) or str
    fields.append((lk, _make_optional_type(base_t)))

  conflicts = (set(all_l_cols) & set(all_r_cols)) - set(config.left_key)
  for k in all_l_cols:
    if k in config.left_key:
      continue
    target_k = f'{config.left_alias}_{k}' if k in conflicts else k
    fields.append((target_k, _make_optional_type(l_map.get(k, str))))

  for k in all_r_cols:
    if k in config.right_key:
      continue
    target_k = f'{config.right_alias}_{k}' if k in conflicts else k
    fields.append((target_k, _make_optional_type(r_map.get(k, str))))

  return RowTypeConstraint.from_fields(fields)


def cli_interval_join(
    left: Any = None,
    right: Any = None,
    error_handling: dict[str, Any] | None = None,
    **kwargs,
) -> beam.PTransform:
  """Beam YAML factory callable for `type: CliIntervalJoin` / `IntervalJoin`."""
  cfg_dict = dict(kwargs)
  if left is not None and 'left' not in cfg_dict:
    cfg_dict['left'] = left
  if right is not None and 'right' not in cfg_dict:
    cfg_dict['right'] = right

  base_cfg = parse_interval_join_config(cfg_dict)
  err_tag = (error_handling or {}).get('output', 'errors')
  route_errors = bool(error_handling and 'output' in error_handling)
  if base_cfg.skew_action == 'dlq' and not route_errors:
    raise ValueError(
        'IntervalJoin skew_action="dlq" requires error_handling / --dlq')

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

    def process(self, record):
      d = _to_plain_dict(record)
      try:
        ckey = encode_canonical_key_values(
            tuple(d.get(k) for k in self.key_cols))
        t = extract_record_timestamp(d, self.ts_col)
        yield (ckey, (self.side, t, d))
      except Exception as e:  # pylint: disable=broad-except
        if route_errors:
          yield pvalue.TaggedOutput(
              err_tag,
              _make_dlq_row(
                  'CliIntervalJoin', type(e).__name__, str(e), d))
        elif base_cfg.skew_action == 'fail':
          raise
        else:
          self.dropped_ctr.inc(1)

  class _CliIntervalJoinTransform(beam.PTransform):
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
            'IntervalJoin requires exactly 2 input PCollections (left and right), '
            f'got: {pcolls!r}')

      l_schema_fields = base_cfg.left_fields or _schema_field_names(
          getattr(pcoll_left, 'element_type', None))
      r_schema_fields = base_cfg.right_fields or _schema_field_names(
          getattr(pcoll_right, 'element_type', None))
      join_cfg = base_cfg._replace(
          left_fields=l_schema_fields, right_fields=r_schema_fields)

      # Re-window both inputs into GlobalWindows so upstream fixed/sliding
      # windows never partition state across window boundaries.
      win_left = pcoll_left | 'GlobalWinLeft' >> beam.WindowInto(
          GlobalWindows())
      win_right = pcoll_right | 'GlobalWinRight' >> beam.WindowInto(
          GlobalWindows())

      is_bounded = (
          getattr(pcoll_left, 'is_bounded', True) and
          getattr(pcoll_right, 'is_bounded', True))

      dlq_pcolls: list[Any] = []

      if is_bounded and not join_cfg.force_stateful:
        keyed_left = (
            win_left
            | 'KeyLeft' >> beam.Map(
                lambda r: (
                    encode_canonical_key_values(
                        extract_record_key(r, join_cfg.left_key)), r)).
            with_output_types(tuple[bytes, Any]))
        keyed_right = (
            win_right
            | 'KeyRight' >> beam.Map(
                lambda r: (
                    encode_canonical_key_values(
                        extract_record_key(r, join_cfg.right_key)), r)).
            with_output_types(tuple[bytes, Any]))
        grouped = {
            'left': keyed_left, 'right': keyed_right
        } | 'CoGroupByKey' >> beam.CoGroupByKey()
        joined_out = (
            grouped
            | 'BoundedIntervalJoin' >> beam.ParDo(
                _BoundedIntervalJoinDoFn(
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
            | 'StatefulIntervalJoin' >> beam.ParDo(
                IntervalJoinStatefulDoFn(
                    join_cfg, err_tag=err_tag,
                    route_errors=route_errors)).with_outputs(
                        err_tag, main='good'))
        good = st_out['good']
        if route_errors:
          dlq_pcolls.append(st_out[err_tag])

      out_row_type = _infer_joined_row_type(
          getattr(pcoll_left, 'element_type', None),
          getattr(pcoll_right, 'element_type', None),
          join_cfg)
      if out_row_type is not None:
        good.element_type = out_row_type

      if route_errors:
        for d_pc in dlq_pcolls:
          d_pc.element_type = dlq_row_type
        bad = (
            dlq_pcolls[0] if len(dlq_pcolls) == 1 else
            (tuple(dlq_pcolls) | 'FlattenIntervalJoinDlq' >> beam.Flatten()))
        bad.element_type = dlq_row_type
        return {'good': good, err_tag: bad}
      return good

  return _CliIntervalJoinTransform()


CliIntervalJoin = cli_interval_join
CliAssignTimestamps = cli_assign_timestamps
