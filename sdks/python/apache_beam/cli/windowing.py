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

"""Declarative triggers, allowed lateness, late-data DLQ & pane metadata (`B2.3`).

Bridges Beam YAML's trigger-less `WindowInto` (`yaml_provider.py:1185`) inside
`apache_beam.cli`:
- Compiles declarative `--trigger` specs (`watermark[,early=...][,late=...]`,
  `every:DUR`, `count:N`, `any:(every:DUR,count:N)`) into `GroupByKey`-safe
  `TriggerFn` instances:
  - Automatically defaults `late=AfterCount(1)` on `AfterWatermark` when
    `allowed_lateness > 0` so `GroupByKey` never raises
    `Unsafe trigger: MAY_FINISH` (`transforms/core.py:3524-3540`).
  - Wraps standalone processing-time / count triggers in `Repeatedly(...)`.
  - Validates `AfterProcessingTime` delays as integer seconds `>= 1` so protobuf
    `int64` `delay_millis` serialization (`transforms/trigger.py:430`) never
    fails with float `TypeError`.
- Quarantines or counts droppably-late records (`watermark > window.end +
  allowed_lateness`) via a sharded `GlobalWindows()` stateful `_LateDataAuditDoFn`
  using `BagStateSpec`/`ReadModifyWriteStateSpec`/`SetStateSpec` (zero
  `OrderedListStateSpec`, working across `BundleBasedDirectRunner` `TestStream`,
  `FnApiRunner`, `PrismRunner`, and `DataflowRunner` Windmill) without delaying
  on-time window watermarks (`min(W_in, H_future) == W_in`).
- Appends concrete `RowTypeConstraint` pane & window columns via
  `CliExtractPaneInfo` (`--emit-pane-info`) compatible with `WriteToParquet`.
"""

from __future__ import annotations

from collections.abc import Mapping
import datetime
import json
import math
from typing import Any
from typing import NamedTuple
from typing import Optional
import zlib

import apache_beam as beam
from apache_beam.coders import BytesCoder
from apache_beam.coders import VarIntCoder
from apache_beam.metrics import Metrics
from apache_beam.pvalue import TaggedOutput
from apache_beam.transforms import trigger
from apache_beam.transforms import window
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.transforms.userstate import SetStateSpec
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import on_timer
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.window import TimestampedValue
from apache_beam.typehints import schemas
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.utils import timestamp as beam_timestamp
from apache_beam.utils import windowed_value

from . import enrich as enrich_mod
from . import interval_join

_ALLOWED_WINDOW_TYPES = frozenset(['fixed', 'sliding', 'sessions', 'global'])
_ALLOWED_ACCUMULATION_MODES = frozenset(['discarding', 'accumulating'])
_ALLOWED_LATE_ACTIONS = frozenset(['allow', 'drop', 'dlq'])

_DLQ_ROW_SCHEMA = RowTypeConstraint.from_fields([
    ('step', str),
    ('error_type', str),
    ('error', str),
    ('raw_payload', str),
    ('element', str),
])

_PANE_INFO_FIELDS: tuple[tuple[str, Any], ...] = (
    ('window_start', Optional[str]),
    ('window_end', Optional[str]),
    ('pane_timing', str),
    ('pane_index', int),
    ('pane_is_first', bool),
    ('pane_is_last', bool),
)


class SubTriggerSpec(NamedTuple):
  kind: str  # 'every', 'count', 'any'
  delay_sec: int | None = None
  count: int | None = None
  children: tuple['SubTriggerSpec', ...] = ()


class ParsedTriggerSpec(NamedTuple):
  mode: str  # 'default', 'watermark', 'standalone'
  early: SubTriggerSpec | None = None
  late: SubTriggerSpec | None = None
  standalone: SubTriggerSpec | None = None
  canonical: str = 'default'
  uses_processing_time: bool = False
  is_multi_pane: bool = False


class WindowingConfig(NamedTuple):
  window_type: str
  size: str | None
  size_sec: float | None
  period: str | None
  period_sec: float | None
  offset: str | None
  offset_sec: float
  gap: str | None
  gap_sec: float | None
  allowed_lateness: str | None
  allowed_lateness_sec: float
  trigger: str | None
  parsed_trigger: ParsedTriggerSpec
  accumulation_mode: str | None
  late_action: str
  late_shards: int
  watermark_lag: str | None
  watermark_lag_sec: float | None
  dlq_redact_fields: tuple[str, ...]


def _parse_non_negative_duration(raw: Any,
                                 label: str) -> tuple[str | None, float]:
  if raw is None or str(raw).strip() == '':
    return None, 0.0
  raw_s = str(raw).strip()
  if raw_s in ('0', '0s', '0ms'):
    return '0s', 0.0
  sec = interval_join.parse_interval_duration(raw_s)
  if not math.isfinite(sec) or sec < 0:
    raise ValueError(f'{label} must be >= 0, got {raw!r}')
  return raw_s, float(sec)


def _parse_positive_duration(raw: Any, label: str) -> tuple[str, float]:
  if raw is None or str(raw).strip() == '':
    raise ValueError(f'{label} is required')
  raw_s = str(raw).strip()
  sec = interval_join.parse_interval_duration(raw_s)
  if not math.isfinite(sec) or sec <= 0:
    raise ValueError(f'{label} must be > 0, got {raw!r}')
  return raw_s, float(sec)


def _parse_subtrigger(raw: str, label: str) -> SubTriggerSpec:
  s = raw.strip()
  if not s:
    raise ValueError(f'empty trigger clause in {label}')
  lower = s.lower()
  if lower.startswith('any:(') and s.endswith(')'):
    inner = s[5:-1].strip()
    parts = interval_join.split_bracket_tokens(inner)
    if len(parts) < 2:
      raise ValueError(
          f'any:(...) trigger in {label} requires at least 2 subtriggers '
          '(e.g. any:(every:10s,count:100))')
    children = tuple(_parse_subtrigger(p, label) for p in parts)
    if any(c.kind == 'any' for c in children):
      raise ValueError(
          f'nested any:(...) triggers are not supported in {label}')
    return SubTriggerSpec(kind='any', children=children)

  if ':' not in s:
    raise ValueError(
        f'invalid trigger clause {s!r} in {label}; expected every:<DURATION>, '
        'count:<N>, or any:(every:<DURATION>,count:<N>)')
  prefix, val = s.split(':', 1)
  pk = prefix.strip().lower()
  val_s = val.strip()
  if pk in ('every', 'processing_time', 'processing'):
    _, sec = _parse_positive_duration(val_s, f'{label} every:')
    if sec < 1.0 or abs(sec - round(sec)) > 1e-6:
      raise ValueError(
          f'{label} processing-time delay {val_s!r} must be a whole number of '
          'seconds >= 1s (Beam AfterProcessingTime serializes integer seconds)')
    return SubTriggerSpec(kind='every', delay_sec=int(round(sec)))

  if pk == 'count':
    try:
      cnt = int(val_s)
    except ValueError:
      raise ValueError(
          f'{label} count: must be a positive integer, got {val_s!r}') from None
    if cnt < 1:
      raise ValueError(f'{label} count: must be >= 1, got {cnt}')
    return SubTriggerSpec(kind='count', count=cnt)

  raise ValueError(
      f'unknown subtrigger {prefix!r} in {label}; supported: every:DUR, '
      'count:N, any:(...)')


def _subtrigger_uses_pt(sub: SubTriggerSpec | None) -> bool:
  if sub is None:
    return False
  if sub.kind == 'every':
    return True
  return any(_subtrigger_uses_pt(c) for c in sub.children)


def _format_subtrigger(sub: SubTriggerSpec) -> str:
  if sub.kind == 'every':
    return f'every:{sub.delay_sec}s'
  if sub.kind == 'count':
    return f'count:{sub.count}'
  return 'any:(' + ','.join(_format_subtrigger(c) for c in sub.children) + ')'


def parse_trigger_spec(raw: Any) -> ParsedTriggerSpec:
  """Parses CLI `--trigger` shorthand into a validated `ParsedTriggerSpec`."""
  if raw is None or str(raw).strip() == '' or str(
      raw).strip().lower() == 'default':
    return ParsedTriggerSpec(
        mode='default',
        canonical='default',
        uses_processing_time=False,
        is_multi_pane=False,
    )

  s = str(raw).strip()
  tokens = interval_join.split_bracket_tokens(s)
  if not tokens:
    return ParsedTriggerSpec(mode='default', canonical='default')

  head = tokens[0].strip().lower()
  if head in ('watermark', 'event_time', 'after_watermark'):
    early_sub: SubTriggerSpec | None = None
    late_sub: SubTriggerSpec | None = None
    for tok in tokens[1:]:
      if '=' not in tok:
        raise ValueError(
            f'invalid qualifier {tok!r} in --trigger {s!r}; expected '
            'early=<SUBTRIGGER> or late=<SUBTRIGGER>')
      k, v = tok.split('=', 1)
      kl = k.strip().lower()
      if kl == 'early':
        if early_sub is not None:
          raise ValueError('duplicate early= clause in --trigger')
        early_sub = _parse_subtrigger(v, '--trigger early')
      elif kl == 'late':
        if late_sub is not None:
          raise ValueError('duplicate late= clause in --trigger')
        late_sub = _parse_subtrigger(v, '--trigger late')
      else:
        raise ValueError(
            f'unknown watermark trigger qualifier {k.strip()!r}; '
            'expected early= or late=')
    parts = ['watermark']
    if early_sub is not None:
      parts.append(f'early={_format_subtrigger(early_sub)}')
    if late_sub is not None:
      parts.append(f'late={_format_subtrigger(late_sub)}')
    return ParsedTriggerSpec(
        mode='watermark',
        early=early_sub,
        late=late_sub,
        canonical=','.join(parts),
        uses_processing_time=(
            _subtrigger_uses_pt(early_sub) or _subtrigger_uses_pt(late_sub)),
        is_multi_pane=bool(early_sub is not None or late_sub is not None),
    )

  if len(tokens) > 1:
    raise ValueError(
        f'invalid standalone --trigger {s!r}; combine multiple triggers via '
        '"watermark,early=...,late=..." or "any:(every:10s,count:100)"')
  standalone = _parse_subtrigger(tokens[0], '--trigger')
  return ParsedTriggerSpec(
      mode='standalone',
      standalone=standalone,
      canonical=_format_subtrigger(standalone),
      uses_processing_time=_subtrigger_uses_pt(standalone),
      is_multi_pane=True,
  )


def _instantiate_subtrigger(sub: SubTriggerSpec) -> trigger.TriggerFn:
  if sub.kind == 'every':
    assert sub.delay_sec is not None
    return trigger.AfterProcessingTime(int(sub.delay_sec))
  if sub.kind == 'count':
    assert sub.count is not None
    return trigger.AfterCount(int(sub.count))
  if sub.kind == 'any':
    return trigger.AfterAny(*(_instantiate_subtrigger(c) for c in sub.children))
  raise ValueError(f'unsupported subtrigger kind {sub.kind!r}')


def build_trigger_and_accumulation(
    cfg: WindowingConfig,
) -> tuple[trigger.TriggerFn | None, int | None]:
  """Constructs `GroupByKey`-safe `(TriggerFn, AccumulationMode)` for `WindowInto`."""
  acc_enum: int | None = None
  if cfg.accumulation_mode == 'accumulating':
    acc_enum = trigger.AccumulationMode.ACCUMULATING
  elif cfg.accumulation_mode == 'discarding':
    acc_enum = trigger.AccumulationMode.DISCARDING

  pt = cfg.parsed_trigger
  if pt.mode == 'default':
    if cfg.allowed_lateness_sec > 0 and acc_enum is not None:
      return trigger.DefaultTrigger(), acc_enum
    return None, acc_enum

  # Non-trivial triggers require explicit accumulation_mode in Beam Windowing!
  if acc_enum is None:
    acc_enum = trigger.AccumulationMode.DISCARDING

  if pt.mode == 'watermark':
    early_fn = _instantiate_subtrigger(pt.early) if pt.early else None
    if pt.late is not None:
      late_fn: trigger.TriggerFn | None = _instantiate_subtrigger(pt.late)
    elif cfg.allowed_lateness_sec > 0:
      # Prevent GroupByKey `Unsafe trigger: MAY_FINISH` (`core.py:3524-3540`)
      # when allowed_lateness > 0!
      late_fn = trigger.AfterCount(1)
    else:
      late_fn = None
    if early_fn is None and late_fn is None:
      return trigger.DefaultTrigger(), acc_enum
    return trigger.AfterWatermark(early=early_fn, late=late_fn), acc_enum

  assert pt.standalone is not None
  return trigger.Repeatedly(_instantiate_subtrigger(pt.standalone)), acc_enum


def build_window_fn(cfg: WindowingConfig) -> window.WindowFn:
  if cfg.window_type == 'global':
    return window.GlobalWindows()
  if cfg.window_type == 'fixed':
    assert cfg.size_sec is not None
    return window.FixedWindows(cfg.size_sec, cfg.offset_sec)
  if cfg.window_type == 'sliding':
    assert cfg.size_sec is not None and cfg.period_sec is not None
    return window.SlidingWindows(cfg.size_sec, cfg.period_sec, cfg.offset_sec)
  if cfg.window_type == 'sessions':
    assert cfg.gap_sec is not None
    return window.Sessions(cfg.gap_sec)
  raise ValueError(f'unsupported window type {cfg.window_type!r}')


def parse_windowing_config(raw_cfg: Mapping[str, Any]) -> WindowingConfig:
  """Validates a `CliWindowInto` config mapping and returns `WindowingConfig`."""
  if not isinstance(raw_cfg, Mapping):
    raise ValueError('CliWindowInto config must be a mapping')

  flat: dict[str, Any] = {}
  if isinstance(raw_cfg.get('windowing'), Mapping):
    for wk, wv in raw_cfg['windowing'].items():
      flat[str(wk).strip().lower().replace('-', '_')] = wv
  for k, v in raw_cfg.items():
    nk = str(k).strip().lower().replace('-', '_')
    if nk == 'windowing':
      continue
    flat[nk] = v

  allowed_keys = {
      'type',
      'window_type',
      'size',
      'period',
      'offset',
      'gap',
      'allowed_lateness',
      'trigger',
      'accumulation_mode',
      'accumulation',
      'late_action',
      'late_shards',
      'watermark_lag',
      'dlq_redact_fields',
      'error_handling',
  }
  unknown = [k for k in flat if k not in allowed_keys]
  if unknown:
    raise ValueError(f'unknown option(s) {sorted(unknown)} in CliWindowInto')

  raw_wtype = str(flat.get('window_type') or flat.get('type') or
                  '').strip().lower()
  if raw_wtype == 'session':
    raw_wtype = 'sessions'
  if raw_wtype not in _ALLOWED_WINDOW_TYPES:
    raise ValueError(
        f'invalid window type {raw_wtype!r}; expected fixed, sliding, '
        'sessions, or global')

  size_s: str | None = None
  size_sec: float | None = None
  period_s: str | None = None
  period_sec: float | None = None
  gap_s: str | None = None
  gap_sec: float | None = None
  offset_s, offset_sec = _parse_non_negative_duration(
      flat.get('offset'), 'window offset')

  if raw_wtype == 'fixed':
    size_s, size_sec = _parse_positive_duration(flat.get('size'), 'fixed window size')
  elif raw_wtype == 'sliding':
    size_s, size_sec = _parse_positive_duration(
        flat.get('size'), 'sliding window size')
    period_s, period_sec = _parse_positive_duration(
        flat.get('period'), 'sliding window period')
  elif raw_wtype == 'sessions':
    gap_s, gap_sec = _parse_positive_duration(
        flat.get('gap'), 'session window gap')

  lat_s, lat_sec = _parse_non_negative_duration(
      flat.get('allowed_lateness'), '--allowed-lateness')
  if raw_wtype == 'global' and lat_sec > 0:
    raise ValueError(
        '--allowed-lateness > 0 is not applicable to global windows '
        '(use fixed:, sliding:, or session: windows)')

  raw_trig = flat.get('trigger')
  parsed_trig = parse_trigger_spec(raw_trig)
  if raw_wtype == 'global' and parsed_trig.mode == 'watermark':
    raise ValueError(
        'watermark trigger is not applicable to global windows; '
        'use --trigger every:<DURATION> or --trigger count:<N>')
  trig_str = None if parsed_trig.mode == 'default' else parsed_trig.canonical

  raw_acc = flat.get('accumulation_mode')
  if raw_acc is None:
    raw_acc = flat.get('accumulation')
  acc_mode: str | None = None
  if raw_acc is not None and str(raw_acc).strip() != '':
    acc_mode = str(raw_acc).strip().lower()
    if acc_mode not in _ALLOWED_ACCUMULATION_MODES:
      raise ValueError(
          f'invalid --accumulation {raw_acc!r}; expected discarding or '
          'accumulating')
  elif trig_str is not None:
    acc_mode = 'discarding'

  late_action = str(flat.get('late_action') or 'allow').strip().lower()
  if late_action not in _ALLOWED_LATE_ACTIONS:
    raise ValueError(
        f'invalid --late-action {late_action!r}; expected allow, drop, or dlq')
  if raw_wtype == 'global' and late_action != 'allow':
    raise ValueError(
        f'--late-action={late_action} requires bounded-horizon windows '
        '(fixed:, sliding:, or session:)')

  late_shards = int(flat.get('late_shards', 1))
  if late_shards < 1 or late_shards > 256:
    raise ValueError(
        f'late_shards must be between 1 and 256, got {late_shards}')

  wm_lag_s: str | None = None
  wm_lag_sec: float | None = None
  if flat.get('watermark_lag') is not None and str(
      flat.get('watermark_lag')).strip():
    wm_lag_s, wm_lag_sec = _parse_non_negative_duration(
        flat.get('watermark_lag'), 'watermark_lag')

  dlq_redact_fields: tuple[str, ...] = ()
  if flat.get('dlq_redact_fields'):
    dlq_redact_fields = enrich_mod._validate_ident_list(
        interval_join.parse_column_list(flat['dlq_redact_fields']),
        'dlq_redact_fields')

  return WindowingConfig(
      window_type=raw_wtype,
      size=size_s,
      size_sec=size_sec,
      period=period_s,
      period_sec=period_sec,
      offset=offset_s if offset_sec > 0 else None,
      offset_sec=offset_sec,
      gap=gap_s,
      gap_sec=gap_sec,
      allowed_lateness=lat_s if lat_sec > 0 else None,
      allowed_lateness_sec=lat_sec,
      trigger=trig_str,
      parsed_trigger=parsed_trig,
      accumulation_mode=acc_mode,
      late_action=late_action,
      late_shards=late_shards,
      watermark_lag=wm_lag_s,
      watermark_lag_sec=wm_lag_sec,
      dlq_redact_fields=dlq_redact_fields,
  )


def windowing_config_to_yaml_dict(cfg: WindowingConfig) -> dict[str, Any]:
  w_dict: dict[str, Any] = {'type': cfg.window_type}
  if cfg.size is not None:
    w_dict['size'] = cfg.size
  if cfg.period is not None:
    w_dict['period'] = cfg.period
  if cfg.offset is not None:
    w_dict['offset'] = cfg.offset
  if cfg.gap is not None:
    w_dict['gap'] = cfg.gap

  out: dict[str, Any] = {'windowing': w_dict}
  if cfg.allowed_lateness is not None:
    out['allowed_lateness'] = cfg.allowed_lateness
  if cfg.trigger is not None:
    out['trigger'] = cfg.trigger
  if cfg.accumulation_mode is not None:
    out['accumulation'] = cfg.accumulation_mode
  if cfg.late_action != 'allow':
    out['late_action'] = cfg.late_action
  if cfg.late_shards != 1:
    out['late_shards'] = cfg.late_shards
  if cfg.watermark_lag is not None:
    out['watermark_lag'] = cfg.watermark_lag
  if cfg.dlq_redact_fields:
    out['dlq_redact_fields'] = list(cfg.dlq_redact_fields)
  return out


def _format_utc_iso(ts: beam_timestamp.Timestamp) -> str:
  dt = ts.to_utc_datetime()
  if dt.tzinfo is None:
    dt = dt.replace(tzinfo=datetime.timezone.utc)
  if dt.microsecond:
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
  return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def _make_late_dlq_row(
    elem: Any,
    msg: str,
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
      step='CliWindowInto',
      error_type='LateDataError',
      error=msg[:4096],
      raw_payload=raw_json,
      element=raw_json,
  )


def _shard_key_bytes(elem: Any, num_shards: int) -> bytes:
  if num_shards <= 1:
    return b'\x00'
  try:
    plain = interval_join._to_plain_dict(elem)
    payload = json.dumps(plain, default=str, sort_keys=True).encode('utf-8')
  except Exception:  # pylint: disable=broad-except
    payload = repr(elem).encode('utf-8')
  sid = zlib.crc32(payload) % num_shards
  return sid.to_bytes(2, 'big')


class _LateDataAuditDoFn(beam.DoFn):
  """Quarantines or counts droppably-late elements (`W > w.end + allowed_lateness`).

  Executes in `GlobalWindows()` over a fixed shard ring (`1..256` keys) using
  `ReadModifyWriteStateSpec` + `SetStateSpec` (`VarIntCoder`) + `TimerSpec(WATERMARK)`.
  Because every active timer has `expire_us > W_in`, `min(W_in, expire_us) == W_in`,
  so on-time window watermarks downstream are never delayed.
  """

  CLOSED_WM_US = ReadModifyWriteStateSpec('closed_wm_us', VarIntCoder())
  NEXT_TIMER_US = ReadModifyWriteStateSpec('next_timer_us', VarIntCoder())
  MAX_EVENT_US = ReadModifyWriteStateSpec('max_event_us', VarIntCoder())
  PENDING_HORIZONS_US = SetStateSpec('pending_horizons_us', VarIntCoder())
  HORIZON_TIMER = TimerSpec('horizon_timer', TimeDomain.WATERMARK)

  def __init__(self, cfg: WindowingConfig, has_dlq: bool):
    self._cfg = cfg
    self._has_dlq = has_dlq
    self._windowfn = build_window_fn(cfg)
    self._lateness_us = int(round(cfg.allowed_lateness_sec * 1_000_000))
    self._sim_lag_us = (
        int(round(cfg.watermark_lag_sec *
                  1_000_000)) if cfg.watermark_lag_sec is not None else None)
    self._late_dropped_counter = Metrics.counter(
        'beam_cli', 'late_rows_dropped')

  def process(
      self,
      kv_elem,
      ts=beam.DoFn.TimestampParam,
      closed_wm=beam.DoFn.StateParam(CLOSED_WM_US),
      next_timer=beam.DoFn.StateParam(NEXT_TIMER_US),
      max_event=beam.DoFn.StateParam(MAX_EVENT_US),
      pending_horizons=beam.DoFn.StateParam(PENDING_HORIZONS_US),
      horizon_timer=beam.DoFn.TimerParam(HORIZON_TIMER),
  ):
    _, row = kv_elem
    assign_ctx = window.WindowFn.AssignContext(
        ts, element=row, window=GlobalWindow())
    assigned = list(self._windowfn.assign(assign_ctx))
    max_end_us = max(int(w.end.micros) for w in assigned)
    horizon_us = max_end_us + self._lateness_us
    # Beam Portability `beam:coder:timer:v1` encodes fire_timestamp at 1ms
    # resolution (`1000` micros). Step to the next whole millisecond (> horizon)
    # so `timestamp.micros == expire_us` without sub-millisecond truncation loops!
    expire_us = ((horizon_us // 1000) + 1) * 1000

    eff_closed_us = closed_wm.read()
    elem_us = int(ts.micros)
    if self._sim_lag_us is not None:
      prev_max = max_event.read()
      cur_max = elem_us if prev_max is None else max(prev_max, elem_us)
      if cur_max != prev_max:
        max_event.write(cur_max)
      sim_wm_us = cur_max - self._sim_lag_us
      eff_closed_us = (
          sim_wm_us if eff_closed_us is None else max(eff_closed_us, sim_wm_us))

    if eff_closed_us is not None and eff_closed_us >= expire_us:
      horizon_ts = beam_timestamp.Timestamp(micros=horizon_us)
      wm_ts = beam_timestamp.Timestamp(micros=eff_closed_us)
      msg = (
          f'CliWindowInto late record at event_time={_format_utc_iso(ts)} '
          f'arrived after window closing horizon {_format_utc_iso(horizon_ts)} '
          f'(watermark={_format_utc_iso(wm_ts)}, '
          f'allowed_lateness={self._cfg.allowed_lateness or "0s"})')
      if self._cfg.late_action == 'dlq':
        if not self._has_dlq:
          raise ValueError(
              f'{msg}; pass --dlq or --late-data-dlq to quarantine late records'
          )
        yield TaggedOutput(
            'errors',
            TimestampedValue(
                _make_late_dlq_row(
                    row, msg, redact_fields=self._cfg.dlq_redact_fields),
                ts))
      else:
        self._late_dropped_counter.inc()
      return

    pending_horizons.add(expire_us)
    cur_next = next_timer.read()
    if cur_next is None or expire_us < cur_next:
      next_timer.write(expire_us)
      horizon_timer.set(beam_timestamp.Timestamp(micros=expire_us))

    yield TimestampedValue(row, ts)

  @on_timer(HORIZON_TIMER)
  def on_horizon(
      self,
      timestamp=beam.DoFn.TimestampParam,
      closed_wm=beam.DoFn.StateParam(CLOSED_WM_US),
      next_timer=beam.DoFn.StateParam(NEXT_TIMER_US),
      pending_horizons=beam.DoFn.StateParam(PENDING_HORIZONS_US),
      horizon_timer=beam.DoFn.TimerParam(HORIZON_TIMER),
  ):
    fired_us = int(timestamp.micros)
    prev_closed = closed_wm.read()
    if prev_closed is None or fired_us > prev_closed:
      closed_wm.write(fired_us)

    all_pending = list(pending_horizons.read())
    surviving = [h for h in all_pending if h > fired_us]
    pending_horizons.clear()
    if surviving:
      for h in surviving:
        pending_horizons.add(h)
      nxt = min(surviving)
      next_timer.write(nxt)
      horizon_timer.set(beam_timestamp.Timestamp(micros=nxt))
    else:
      next_timer.clear()


class CliWindowInto(beam.PTransform):
  """Window assignment with triggers, allowed lateness, and late-data DLQ."""
  def __init__(
      self,
      cfg: WindowingConfig | Mapping[str, Any],
      *,
      error_handling: Mapping[str, Any] | None = None,
  ):
    super().__init__()
    self._cfg = (
        cfg
        if isinstance(cfg, WindowingConfig) else parse_windowing_config(cfg))
    self._error_handling = dict(error_handling) if error_handling else None

  def expand(self, pcoll):
    in_type = pcoll.element_type
    has_dlq = bool(self._error_handling and self._error_handling.get('output'))
    windowfn = build_window_fn(self._cfg)
    triggerfn, acc_mode = build_trigger_and_accumulation(self._cfg)

    w_kwargs: dict[str, Any] = {
        'allowed_lateness': self._cfg.allowed_lateness_sec,
    }
    if triggerfn is not None:
      w_kwargs['trigger'] = triggerfn
    if acc_mode is not None:
      w_kwargs['accumulation_mode'] = acc_mode

    if self._cfg.late_action in ('dlq', 'drop'):
      if self._cfg.late_action == 'dlq' and not has_dlq:
        raise ValueError(
            'CliWindowInto late_action="dlq" requires error_handling / --dlq / '
            '--late-data-dlq')
      sharded = (
          pcoll
          | 'RewindowGlobalForLateAudit' >> beam.WindowInto(GlobalWindows())
          | 'KeyByLateShard' >> beam.Map(
              lambda r, n=self._cfg.late_shards:
              (_shard_key_bytes(r, n), r)).with_output_types(tuple[bytes, Any]))
      # Force deterministic BytesCoder on the stateful DoFn key
      _ = BytesCoder
      audited = (
          sharded
          | 'AuditLateRecords' >> beam.ParDo(
              _LateDataAuditDoFn(self._cfg, has_dlq=has_dlq)).with_outputs(
                  'errors', main='good'))
      good_stream = audited['good']
      if in_type is not None:
        good_stream = good_stream | 'RestoreInputType' >> beam.Map(
            lambda r: r).with_output_types(in_type)
      windowed = good_stream | 'AssignWindows' >> beam.WindowInto(
          windowfn, **w_kwargs)
      if has_dlq:
        err_tag = str(self._error_handling['output'])
        late_errs = (
            audited['errors']
            | 'TypeLateErrors' >>
            beam.Map(lambda r: r).with_output_types(_DLQ_ROW_SCHEMA))
        return {'good': windowed, err_tag: late_errs}
      return windowed

    windowed = pcoll | 'AssignWindows' >> beam.WindowInto(windowfn, **w_kwargs)
    if has_dlq:
      err_tag = str(self._error_handling['output'])
      empty_errs = (
          pcoll.pipeline
          | 'EmptyWindowDlq' >> beam.Create(
              []).with_output_types(_DLQ_ROW_SCHEMA))
      return {'good': windowed, err_tag: empty_errs}
    return windowed


class _ExtractPaneInfoDoFn(beam.DoFn):
  """Appends typed window bounds and `PaneInfo` metadata to each row."""
  def __init__(self):
    self._late_panes = Metrics.counter('beam_cli', 'late_panes_emitted')

  def process(
      self,
      element,
      win=beam.DoFn.WindowParam,
      pane=beam.DoFn.PaneInfoParam,
  ):
    row_dict = interval_join._to_plain_dict(element)
    if isinstance(win, GlobalWindow) or not hasattr(win, 'start'):
      w_start: str | None = None
      w_end: str | None = None
    else:
      w_start = _format_utc_iso(win.start)
      w_end = _format_utc_iso(win.end)

    if pane is not None:
      timing_str = windowed_value.PaneInfoTiming.to_string(pane.timing)
      if pane.timing == windowed_value.PaneInfoTiming.LATE:
        self._late_panes.inc()
      p_idx = int(pane.index)
      p_first = bool(pane.is_first)
      p_last = bool(pane.is_last)
    else:
      timing_str = 'UNKNOWN'
      p_idx = 0
      p_first = True
      p_last = True

    row_dict['window_start'] = w_start
    row_dict['window_end'] = w_end
    row_dict['pane_timing'] = timing_str
    row_dict['pane_index'] = p_idx
    row_dict['pane_is_first'] = p_first
    row_dict['pane_is_last'] = p_last
    yield beam.Row(**row_dict)


class CliExtractPaneInfo(beam.PTransform):
  """Appends concrete `RowTypeConstraint` window & pane columns (`--emit-pane-info`)."""
  def expand(self, pcoll):
    out_schema: RowTypeConstraint | None = None
    try:
      in_fields = list(
          schemas.named_fields_from_element_type(pcoll.element_type))
      if in_fields:
        merged_types = {name: typ for name, typ in in_fields}
        for pname, ptyp in _PANE_INFO_FIELDS:
          merged_types[pname] = ptyp
        out_schema = RowTypeConstraint.from_fields(list(merged_types.items()))
    except Exception:  # pylint: disable=broad-except
      out_schema = None

    pardo = beam.ParDo(_ExtractPaneInfoDoFn())
    if out_schema is not None:
      pardo = pardo.with_output_types(out_schema)
    return pcoll | 'ExtractPaneInfo' >> pardo


def cli_window_into(
    windowing: Optional[Mapping[str, Any]] = None,
    type: Optional[str] = None,  # pylint: disable=redefined-builtin
    window_type: Optional[str] = None,
    size: Optional[str] = None,
    period: Optional[str] = None,
    offset: Optional[str] = None,
    gap: Optional[str] = None,
    allowed_lateness: Optional[str] = None,
    trigger: Optional[str] = None,  # pylint: disable=redefined-outer-name
    accumulation: Optional[str] = None,
    accumulation_mode: Optional[str] = None,
    late_action: Optional[str] = None,
    late_shards: Optional[int] = None,
    watermark_lag: Optional[str] = None,
    dlq_redact_fields: Optional[Any] = None,
    error_handling: Optional[Mapping[str, Any]] = None,
) -> CliWindowInto:
  """Beam YAML provider entrypoint for `type: CliWindowInto`."""
  raw: dict[str, Any] = {}
  for k, v in (
      ('windowing', windowing),
      ('type', type),
      ('window_type', window_type),
      ('size', size),
      ('period', period),
      ('offset', offset),
      ('gap', gap),
      ('allowed_lateness', allowed_lateness),
      ('trigger', trigger),
      ('accumulation', accumulation),
      ('accumulation_mode', accumulation_mode),
      ('late_action', late_action),
      ('late_shards', late_shards),
      ('watermark_lag', watermark_lag),
      ('dlq_redact_fields', dlq_redact_fields),
  ):
    if v is not None:
      raw[k] = v
  return CliWindowInto(
      parse_windowing_config(raw), error_handling=error_handling)


def cli_extract_pane_info() -> CliExtractPaneInfo:
  """Beam YAML provider entrypoint for `type: CliExtractPaneInfo`."""
  return CliExtractPaneInfo()
