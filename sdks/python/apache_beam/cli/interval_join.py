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

"""Declarative Event-Time Interval Joins for Beam SQL & Beam YAML (Proposal 3).

Supports:
1. Event-time sliding interval correlation:
   left.timestamp + lower_bound <= right.timestamp <= left.timestamp + upper_bound
2. Stateful stream processing on Google Cloud Dataflow Windmill and PrismRunner
   using `BagStateSpec` buffers and `TimerSpec(..., TimeDomain.WATERMARK)`.
3. In-memory / bounded execution for local runs, SQL execution, and hermetic tests.
4. Seamless registration as Beam YAML transform (`type: IntervalJoin`).
"""

from __future__ import annotations

import datetime
import math
import re
from typing import Any
from typing import Iterable
from typing import NamedTuple

import apache_beam as beam
from apache_beam.coders import PickleCoder
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.transforms.userstate import TimeDomain
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import on_timer

_DURATION_RE = re.compile(
    r'^(?P<sign>[+-]?)(?P<num>[0-9]+(?:\.[0-9]+)?)\s*(?P<unit>[A-Za-z]*)$')


def parse_interval_duration(raw: str | int | float) -> float:
  """Parses durations like '15m', '-1m', '0s', '900', '500ms', '1h', '2d' into seconds."""
  if raw is None or (isinstance(raw, str) and not raw.strip()):
    raise ValueError('empty interval duration')
  if isinstance(raw, (int, float)):
    return float(raw)
  s = str(raw).strip()
  if s in ('0', '0s', '+0', '-0'):
    return 0.0
  m = _DURATION_RE.match(s)
  if not m:
    raise ValueError(
        f'invalid interval duration {raw!r}; examples: 0s, 15m, -1m, 900s, 500ms, 1h')
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
  join_type: str = 'inner'  # 'inner', 'left', 'full'
  select: dict[str, str] | None = None
  max_watermark_skew: float | None = None
  skew_action: str = 'drop'  # 'drop' or 'warn'

  @property
  def lower_bound_sec(self) -> float:
    return self.lower_bound

  @property
  def upper_bound_sec(self) -> float:
    return self.upper_bound


def parse_interval_join_config(cfg: dict[str, Any]) -> IntervalJoinConfig:
  """Validates and parses interval join configuration."""
  left_alias = str(cfg.get('left') or 'left').strip()
  right_alias = str(cfg.get('right') or 'right').strip()

  key_raw = cfg.get('key')
  left_key_raw = cfg.get('left_key') or key_raw
  right_key_raw = cfg.get('right_key') or key_raw

  def _to_key_tuple(k: Any) -> tuple[str, ...]:
    if isinstance(k, str):
      return tuple(part.strip() for part in k.split(',') if part.strip())
    if isinstance(k, (list, tuple)):
      return tuple(str(x).strip() for x in k if str(x).strip())
    return ()

  left_key = _to_key_tuple(left_key_raw)
  right_key = _to_key_tuple(right_key_raw)
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
        'interval join requires timestamp column(s); specify left_time and right_time or timestamp')

  lower_raw = cfg.get('lower_bound', 0.0)
  upper_raw = cfg.get('upper_bound')
  if upper_raw is None:
    raise ValueError('interval join requires upper_bound (e.g. upper_bound: "15m")')

  lower_bound = parse_interval_duration(lower_raw)
  upper_bound = parse_interval_duration(upper_raw)
  if lower_bound > upper_bound:
    raise ValueError(
        f'invalid interval bounds: lower_bound ({lower_bound}s) cannot be '
        f'greater than upper_bound ({upper_bound}s)')

  join_type = str(cfg.get('join_type') or cfg.get('type') or 'inner').strip().lower()
  if join_type in ('outer', 'full_outer'):
    join_type = 'full'
  elif join_type == 'left_outer':
    join_type = 'left'
  if join_type not in ('inner', 'left', 'full'):
    raise ValueError(
        f'unsupported join_type {join_type!r}; expected "inner", "left", or "full"')

  select = cfg.get('select')
  if select is not None and not isinstance(select, dict):
    raise ValueError('select must be a mapping of output_field: expression')

  max_skew_raw = (
      cfg.get('max_watermark_skew')
      or cfg.get('max_skew')
      or cfg.get('watermark_skew_breaker')
      or cfg.get('max_watermark_skew_duration')
  )
  max_watermark_skew = (
      parse_interval_duration(max_skew_raw) if max_skew_raw is not None else None
  )
  if max_watermark_skew is not None and max_watermark_skew <= 0:
    raise ValueError(
        f'max_watermark_skew must be positive (got {max_watermark_skew}s)')

  skew_action = str(cfg.get('skew_action') or 'drop').strip().lower()
  if skew_action not in ('drop', 'warn'):
    raise ValueError(f'skew_action must be "drop" or "warn" (got {skew_action!r})')

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
  )


def extract_record_key(
    element: Any, keys: tuple[str, ...] | list[str] | str) -> tuple[Any, ...]:
  """Extracts the tuple of join key values from a Beam Row or dict."""
  if isinstance(keys, str):
    key_tuple = (keys,)
  else:
    key_tuple = tuple(keys)
  out = []
  for k in key_tuple:
    if hasattr(element, k):
      out.append(getattr(element, k))
    elif isinstance(element, dict) and k in element:
      out.append(element[k])
    elif hasattr(element, '_asdict'):
      out.append(element._asdict().get(k))
    else:
      out.append(None)
  return tuple(out)


def extract_record_timestamp(
    element: Any,
    ts_attr: str | None,
    window_ts: Any = None,
) -> float:
  """Extracts epoch timestamp in seconds from a record or window timestamp."""
  val = None
  if ts_attr:
    if hasattr(element, ts_attr):
      val = getattr(element, ts_attr)
    elif isinstance(element, dict) and ts_attr in element:
      val = element[ts_attr]
    elif hasattr(element, '_asdict'):
      val = element._asdict().get(ts_attr)

  if val is None and window_ts is not None:
    try:
      return float(window_ts)
    except (ValueError, TypeError):
      pass

  if val is None:
    if hasattr(element, 'timestamp'):
      val = element.timestamp
    else:
      raise ValueError(
          f'timestamp attribute {ts_attr!r} not found in element: {element!r}')

  if isinstance(val, (int, float)):
    # Epoch ms vs epoch s heuristic (> year 1973 in ms)
    if abs(val) > 1e11:
      return float(val) / 1000.0
    return float(val)

  if isinstance(val, datetime.datetime):
    if val.tzinfo is None:
      val = val.replace(tzinfo=datetime.timezone.utc)
    return val.timestamp()

  s = str(val).strip()
  try:
    if s.replace('.', '', 1).isdigit():
      f = float(s)
      return f / 1000.0 if abs(f) > 1e11 else f
    dt = datetime.datetime.fromisoformat(s.replace('Z', '+00:00'))
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


def project_joined_row(
    left_elem: Any | None,
    right_elem: Any | None,
    config: IntervalJoinConfig,
) -> dict[str, Any]:
  """Projects fields from joined left and right records into a unified result dict."""
  left_dict = _to_plain_dict(left_elem)
  right_dict = _to_plain_dict(right_elem)

  if config.select:
    # Build evaluation context
    ctx = {
        'left': left_dict,
        'right': right_dict,
        **{f'left_{k}': v for k, v in left_dict.items()},
        **{f'right_{k}': v for k, v in right_dict.items()},
    }
    # Direct field aliases without prefix if unique
    for k, v in left_dict.items():
      if k not in ctx:
        ctx[k] = v
    for k, v in right_dict.items():
      if k not in ctx and k not in left_dict:
        ctx[k] = v

    out: dict[str, Any] = {}
    for out_k, expr_str in config.select.items():
      expr = expr_str.strip()
      # 1. Direct path lookup: left.col or right.col or col
      if '.' in expr:
        side, field = expr.split('.', 1)
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
      # 2. Simple expression evaluation (arithmetic/expressions)
      try:
        out[out_k] = eval(expr, {'__builtins__': {}}, ctx)
      except Exception:
        out[out_k] = None
    return out

  # Default projection: merge dictionaries, prefixing collisions
  out = {}
  # First populate join keys from available side
  for k in config.left_key:
    if k in left_dict:
      out[k] = left_dict[k]
    elif k in right_dict:
      out[k] = right_dict[k]

  conflicts = (set(left_dict.keys()) & set(right_dict.keys())) - set(config.left_key)
  for k, v in left_dict.items():
    if k in config.left_key:
      continue
    target_k = f'{config.left_alias}_{k}' if k in conflicts else k
    out[target_k] = v

  for k, v in right_dict.items():
    if k in config.right_key:
      continue
    target_k = f'{config.right_alias}_{k}' if k in conflicts else k
    out[target_k] = v

  return out


def join_bounded_records(
    left_records: Iterable[Any],
    right_records: Iterable[Any],
    config: IntervalJoinConfig,
) -> list[dict[str, Any]]:
  """In-memory interval join over bounded collections."""
  left_by_key: dict[tuple[Any, ...], list[tuple[float, Any, bool]]] = {}
  right_by_key: dict[tuple[Any, ...], list[tuple[float, Any, bool]]] = {}

  for r in left_records:
    k = extract_record_key(r, config.left_key)
    ts = extract_record_timestamp(r, config.left_timestamp)
    left_by_key.setdefault(k, []).append([ts, r, False])

  for r in right_records:
    k = extract_record_key(r, config.right_key)
    ts = extract_record_timestamp(r, config.right_timestamp)
    right_by_key.setdefault(k, []).append([ts, r, False])

  all_keys = set(left_by_key.keys()) | set(right_by_key.keys())
  results: list[dict[str, Any]] = []

  for k in all_keys:
    l_list = left_by_key.get(k, [])
    r_list = right_by_key.get(k, [])

    for l_entry in l_list:
      t_l, l_elem, _ = l_entry
      for r_entry in r_list:
        t_r, r_elem, _ = r_entry
        if config.max_watermark_skew is not None and abs(t_r - t_l) > config.max_watermark_skew:
          continue
        if (t_l + config.lower_bound) <= t_r <= (t_l + config.upper_bound):
          l_entry[2] = True
          r_entry[2] = True
          results.append(project_joined_row(l_elem, r_elem, config))

    if config.join_type in ('left', 'full'):
      for l_entry in l_list:
        if not l_entry[2]:  # Unmatched
          results.append(project_joined_row(l_entry[1], None, config))

    if config.join_type in ('right', 'full'):
      for r_entry in r_list:
        if not r_entry[2]:  # Unmatched
          results.append(project_joined_row(None, r_entry[1], config))

  return results


class _BoundedIntervalJoinDoFn(beam.DoFn):
  """Executes interval join over CoGroupByKey batches in bounded/batch execution."""

  def __init__(self, config: IntervalJoinConfig):
    self.config = config

  def process(self, element):
    key, grouped = element
    left_elements = grouped.get('left', [])
    right_elements = grouped.get('right', [])
    for row_dict in join_bounded_records(left_elements, right_elements, self.config):
      yield beam.Row(**row_dict)


class IntervalJoinStatefulDoFn(beam.DoFn):
  """Stateful streaming DoFn executing interval joins on Dataflow Windmill & Prism."""

  LEFT_BUFFER = BagStateSpec('left_buffer', PickleCoder())
  RIGHT_BUFFER = BagStateSpec('right_buffer', PickleCoder())
  WATERMARK_STATE = ReadModifyWriteStateSpec('wm_state', PickleCoder())
  GC_TIMER = TimerSpec('gc_timer', TimeDomain.WATERMARK)

  def __init__(self, config: IntervalJoinConfig):
    self.config = config
    self.skew_dropped_counter = beam.metrics.Metrics.counter(
        'IntervalJoin', 'watermark_skew_dropped')
    self.skew_warn_counter = beam.metrics.Metrics.counter(
        'IntervalJoin', 'watermark_skew_warn')

  def process(
      self,
      element,
      left_buf=beam.DoFn.StateParam(LEFT_BUFFER),
      right_buf=beam.DoFn.StateParam(RIGHT_BUFFER),
      timer=beam.DoFn.TimerParam(GC_TIMER),
      wm_state=beam.DoFn.StateParam(WATERMARK_STATE),
  ):
    key, (tag, t, row_payload) = element
    max_skew = self.config.max_watermark_skew

    if max_skew is not None:
      cur_wm = wm_state.read() or {'L': None, 'R': None, 'dropped': 0}
      if tag == 'L':
        r_max = cur_wm.get('R')
        if r_max is not None:
          if (r_max - t) > max_skew:
            cur_wm['dropped'] = cur_wm.get('dropped', 0) + 1
            wm_state.write(cur_wm)
            self.skew_dropped_counter.inc(1)
            if self.config.skew_action == 'drop':
              return
          elif (t - r_max) > max_skew:
            self.skew_warn_counter.inc(1)
        cur_wm['L'] = max(cur_wm.get('L') or t, t)
        wm_state.write(cur_wm)
      elif tag == 'R':
        l_max = cur_wm.get('L')
        if l_max is not None:
          if (l_max - t) > max_skew:
            cur_wm['dropped'] = cur_wm.get('dropped', 0) + 1
            wm_state.write(cur_wm)
            self.skew_dropped_counter.inc(1)
            if self.config.skew_action == 'drop':
              return
          elif (t - l_max) > max_skew:
            self.skew_warn_counter.inc(1)
        cur_wm['R'] = max(cur_wm.get('R') or t, t)
        wm_state.write(cur_wm)

    if tag == 'L':
      # Scan existing right elements
      existing_right = list(right_buf.read())
      updated_right = []
      left_matched = False
      for (t_r, r_payload, r_matched) in existing_right:
        if (t + self.config.lower_bound) <= t_r <= (t + self.config.upper_bound):
          left_matched = True
          updated_right.append((t_r, r_payload, True))
          yield beam.Row(**project_joined_row(row_payload, r_payload, self.config))
        else:
          updated_right.append((t_r, r_payload, r_matched))

      if existing_right:
        right_buf.clear()
        for item in updated_right:
          right_buf.add(item)

      left_buf.add((t, row_payload, left_matched))
      # Set GC timer for when watermark advances past this element's match window
      try:
        timer.set(t + self.config.upper_bound + 1.0)
      except Exception:
        pass

    elif tag == 'R':
      # Scan existing left elements
      existing_left = list(left_buf.read())
      updated_left = []
      right_matched = False
      for (t_l, l_payload, l_matched) in existing_left:
        if (t_l + self.config.lower_bound) <= t <= (t_l + self.config.upper_bound):
          right_matched = True
          updated_left.append((t_l, l_payload, True))
          yield beam.Row(**project_joined_row(l_payload, row_payload, self.config))
        else:
          updated_left.append((t_l, l_payload, l_matched))

      if existing_left:
        left_buf.clear()
        for item in updated_left:
          left_buf.add(item)

      right_buf.add((t, row_payload, right_matched))
      try:
        timer.set(t - self.config.lower_bound + 1.0)
      except Exception:
        pass

  @on_timer(GC_TIMER)
  def on_gc(
      self,
      timestamp=beam.DoFn.TimestampParam,
      left_buf=beam.DoFn.StateParam(LEFT_BUFFER),
      right_buf=beam.DoFn.StateParam(RIGHT_BUFFER),
  ):
    watermark = float(timestamp)
    # Evict expired elements
    left_items = list(left_buf.read())
    left_buf.clear()
    for (t_l, l_payload, l_matched) in left_items:
      if (t_l + self.config.upper_bound) < watermark:
        # Expired: if left outer and not matched, emit outer row
        if self.config.join_type in ('left', 'full') and not l_matched:
          yield beam.Row(**project_joined_row(l_payload, None, self.config))
      else:
        left_buf.add((t_l, l_payload, l_matched))

    right_items = list(right_buf.read())
    right_buf.clear()
    for (t_r, r_payload, r_matched) in right_items:
      if (t_r - self.config.lower_bound) < watermark:
        # Expired: if full/right outer and not matched, emit outer row
        if self.config.join_type in ('right', 'full') and not r_matched:
          yield beam.Row(**project_joined_row(None, r_payload, self.config))
      else:
        right_buf.add((t_r, r_payload, r_matched))


def cli_interval_join(
    left: Any = None,
    right: Any = None,
    **kwargs,
) -> beam.PTransform:
  """Beam YAML factory callable for `type: IntervalJoin`."""
  cfg_dict = dict(kwargs)
  if left is not None and 'left' not in cfg_dict:
    cfg_dict['left'] = left
  if right is not None and 'right' not in cfg_dict:
    cfg_dict['right'] = right

  join_cfg = parse_interval_join_config(cfg_dict)

  class _CliIntervalJoinTransform(beam.PTransform):

    def expand(self, pcolls):
      pcoll_left = None
      pcoll_right = None

      if isinstance(pcolls, dict):
        if 'left' in pcolls and 'right' in pcolls:
          pcoll_left = pcolls['left']
          pcoll_right = pcolls['right']
        elif join_cfg.left_alias in pcolls and join_cfg.right_alias in pcolls:
          pcoll_left = pcolls[join_cfg.left_alias]
          pcoll_right = pcolls[join_cfg.right_alias]
        elif len(pcolls) == 2:
          vals = list(pcolls.values())
          pcoll_left, pcoll_right = vals[0], vals[1]
      elif isinstance(pcolls, (list, tuple)) and len(pcolls) == 2:
        pcoll_left, pcoll_right = pcolls[0], pcolls[1]

      if pcoll_left is None or pcoll_right is None:
        raise ValueError(
            'IntervalJoin requires exactly 2 input PCollections (left and right), '
            f'got: {pcolls!r}')

      is_bounded = getattr(pcoll_left, 'is_bounded', True) and getattr(pcoll_right, 'is_bounded', True)

      if is_bounded:
        # CoGroupByKey batch path
        keyed_left = pcoll_left | 'KeyLeft' >> beam.Map(
            lambda r: (extract_record_key(r, join_cfg.left_key), r))
        keyed_right = pcoll_right | 'KeyRight' >> beam.Map(
            lambda r: (extract_record_key(r, join_cfg.right_key), r))
        grouped = {'left': keyed_left, 'right': keyed_right} | 'CoGroupByKey' >> beam.CoGroupByKey()
        return grouped | 'BoundedIntervalJoin' >> beam.ParDo(
            _BoundedIntervalJoinDoFn(join_cfg))

      # Stateful streaming path
      tagged_left = pcoll_left | 'TagLeft' >> beam.Map(
          lambda r: (
              extract_record_key(r, join_cfg.left_key),
              ('L', extract_record_timestamp(r, join_cfg.left_timestamp), _to_plain_dict(r)),
          ))
      tagged_right = pcoll_right | 'TagRight' >> beam.Map(
          lambda r: (
              extract_record_key(r, join_cfg.right_key),
              ('R', extract_record_timestamp(r, join_cfg.right_timestamp), _to_plain_dict(r)),
          ))
      flattened = (tagged_left, tagged_right) | 'FlattenTagged' >> beam.Flatten()
      return flattened | 'StatefulIntervalJoin' >> beam.ParDo(
          IntervalJoinStatefulDoFn(join_cfg))

  return _CliIntervalJoinTransform()


CliIntervalJoin = cli_interval_join
