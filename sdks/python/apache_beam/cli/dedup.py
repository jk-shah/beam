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

"""Keyed streaming and bounded deduplication (`--dedupe-by` / `CliDeduplicate`).

Supports:
1. Deterministic binary composite key encoding (`BytesCoder`) satisfying
   `verify_deterministic()` on `DirectRunner`, `PrismRunner`, and Cloud Dataflow
   Windmill (`None`, `bool`, `int`/`float`/`Decimal` with `-0.0 == 0`, `str`,
   `bytes`, `datetime`).
2. Zero-latency streaming `keep='first'` deduplication using time-bucketed
   state (`(canon_key, floor(t / W))`) + `TimeDomain.WATERMARK` TTL cleanup
   (immune to out-of-order cross-window interference) or `TimeDomain.REAL_TIME`
   processing-time TTLs.
3. Memory-bounded `CombinePerKey` / `GroupByKey` batch deduplication supporting
   `keep='first'|'last'` and deterministic `--dedupe-order <COL>` tie-breaking.
4. Optional `--dedupe-dlq` routing of suppressed duplicates to `--dlq` (without
   double-counting `duplicates_dropped` in `--assert-counts`).
"""

from __future__ import annotations

import decimal
import math
import struct
from typing import Any
from typing import NamedTuple

import apache_beam as beam
from apache_beam import metrics
from apache_beam import pvalue
from apache_beam.coders import BooleanCoder
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.transforms.userstate import TimeDomain
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import on_timer
from apache_beam.transforms.window import TimestampedValue
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.utils import timestamp as beam_timestamp

from . import interval_join
from . import output

split_bracket_tokens = interval_join.split_bracket_tokens
parse_column_list = interval_join.parse_column_list
encode_canonical_key_values = interval_join.encode_canonical_key_values
_make_dlq_row = interval_join._make_dlq_row

_MIN_BEAM_TS = -9_000_000_000_000.0
_MAX_BEAM_TS = 9_000_000_000_000.0


def _record_to_dict(elem: Any) -> dict[str, Any]:
  if isinstance(elem, dict):
    return elem
  if hasattr(elem, '_asdict'):
    return elem._asdict()
  if hasattr(elem, '__dict__'):
    return {k: v for k, v in elem.__dict__.items() if not k.startswith('_')}
  raise TypeError(f'expected Row or dict record, got {type(elem).__name__}')


def extract_canonical_key(record: Any, key_fields: tuple[str, ...]) -> bytes:
  """Extracts `key_fields` from `record` and returns deterministic `bytes`."""
  d = _record_to_dict(record)
  vals: list[Any] = []
  for k in key_fields:
    if k not in d:
      raise KeyError(f'deduplication/join key column {k!r} not found in record')
    vals.append(d[k])
  return encode_canonical_key_values(vals)


class DeduplicateConfig(NamedTuple):
  keys: tuple[str, ...]
  window_sec: float | None = None
  keep: str = 'first'
  order_col: str | None = None
  time_domain: str = 'auto'
  timestamp_col: str | None = None
  route_duplicates_to_dlq: bool = False
  force_stateful: bool = False


def parse_deduplicate_config(cfg: dict[str, Any]) -> DeduplicateConfig:
  """Validates and normalizes `CliDeduplicate` configuration."""
  raw_keys = cfg.get('keys') or cfg.get('key') or cfg.get('dedupe_by')
  keys = parse_column_list(raw_keys)
  if not keys:
    raise ValueError(
        'CliDeduplicate requires at least one key column (e.g. keys: ["event_id"])'
    )
  for k in keys:
    if not output.SAFE_IDENTIFIER_RE.match(k):
      raise ValueError(f'invalid deduplication key column name {k!r}')

  raw_win = cfg.get('window') if 'window' in cfg else cfg.get('dedupe_window')
  window_sec: float | None = None
  if raw_win is not None and str(raw_win).strip() != '':
    window_sec = interval_join.parse_interval_duration(raw_win)
    if window_sec <= 0:
      raise ValueError(
          f'deduplication window must be positive (got {raw_win!r})')

  keep = str(cfg.get('keep') or cfg.get('dedupe_keep') or
             'first').strip().lower()
  if keep not in ('first', 'last'):
    raise ValueError(f'keep must be "first" or "last" (got {keep!r})')

  raw_order = cfg.get('order_by') or cfg.get('order_col') or cfg.get(
      'dedupe_order')
  order_col = str(raw_order).strip() if raw_order else None
  if order_col is not None and not output.SAFE_IDENTIFIER_RE.match(order_col):
    raise ValueError(f'invalid deduplication order column name {order_col!r}')

  time_domain = str(
      cfg.get('time_domain') or cfg.get('dedupe_time_domain') or
      'auto').strip().lower()
  if time_domain not in ('auto', 'event', 'processing'):
    raise ValueError(
        f'time_domain must be "auto", "event", or "processing" (got {time_domain!r})'
    )

  raw_ts = cfg.get('timestamp_col') or cfg.get('timestamp')
  timestamp_col = str(raw_ts).strip() if raw_ts else None
  if timestamp_col is not None and not output.SAFE_IDENTIFIER_RE.match(
      timestamp_col):
    raise ValueError(f'invalid timestamp_col {timestamp_col!r}')

  route_dlq = bool(cfg.get('route_duplicates_to_dlq') or cfg.get('dedupe_dlq'))
  force_stateful = bool(cfg.get('force_stateful'))

  return DeduplicateConfig(
      keys=keys,
      window_sec=window_sec,
      keep=keep,
      order_col=order_col,
      time_domain=time_domain,
      timestamp_col=timestamp_col,
      route_duplicates_to_dlq=route_dlq,
      force_stateful=force_stateful,
  )


def _sortable_order_key(record: Any, order_col: str | None) -> tuple[int, Any]:
  """Returns a total-order-safe tuple `(non_null_rank, comparable_val)`."""
  if not order_col:
    return (0, 0)
  d = _record_to_dict(record)
  val = d.get(order_col)
  if val is None:
    return (0, '')
  if isinstance(val, bool):
    return (1, int(val))
  if isinstance(val, (int, float, decimal.Decimal)):
    f = float(val)
    if math.isnan(f):
      return (0, '')
    return (2, f)
  if hasattr(val, 'timestamp'):
    try:
      return (2, float(val.timestamp()))
    except Exception:  # pylint: disable=broad-except
      pass
  return (3, str(val))


class _BoundedDedupeCombineFn(beam.CombineFn):
  """$O(1)$-per-key bounded combiner when duplicate rows are not routed to DLQ."""
  def __init__(self, keep: str, order_col: str | None):
    self.keep = keep
    self.order_col = order_col
    self._dropped_ctr = metrics.Metrics.counter(
        'beam_cli', 'duplicates_dropped')

  def create_accumulator(self):
    # (has_value: bool, order_tuple: Any, winner_ts: float|None, winner_row: Any, count: int)
    return (False, None, None, None, 0)

  def add_input(self, acc, element):
    has_val, best_ord, best_ts, best_row, cnt = acc
    elem_ts, row = element
    ord_key = _sortable_order_key(row, self.order_col)
    if not has_val:
      return (True, ord_key, elem_ts, row, 1)
    if self.order_col:
      take_new = (ord_key
                  >= best_ord) if self.keep == 'last' else (ord_key < best_ord)
    else:
      take_new = self.keep == 'last'
    if take_new:
      return (True, ord_key, elem_ts, row, cnt + 1)
    return (True, best_ord, best_ts, best_row, cnt + 1)

  def merge_accumulators(self, accumulators):
    has_val = False
    best_ord = None
    best_ts = None
    best_row = None
    total_cnt = 0
    for a_has, a_ord, a_ts, a_row, a_cnt in accumulators:
      if not a_has:
        continue
      if not has_val:
        has_val, best_ord, best_ts, best_row, total_cnt = (
            True, a_ord, a_ts, a_row, a_cnt)
        continue
      total_cnt += a_cnt
      if self.order_col:
        take_new = (a_ord
                    >= best_ord) if self.keep == 'last' else (a_ord < best_ord)
      else:
        take_new = self.keep == 'last'
      if take_new:
        best_ord, best_ts, best_row = a_ord, a_ts, a_row
    return (has_val, best_ord, best_ts, best_row, total_cnt)

  def extract_output(self, acc):
    has_val, _, best_ts, best_row, total_cnt = acc
    if has_val and total_cnt > 1:
      self._dropped_ctr.inc(total_cnt - 1)
    return (best_ts, best_row)


class _StreamingEventDedupeDoFn(beam.DoFn):
  """Time-bucketed `(key, floor(t / W))` watermark-TTL deduplicator."""

  SEEN_STATE = ReadModifyWriteStateSpec('seen', BooleanCoder())
  TTL_TIMER = TimerSpec('ttl', TimeDomain.WATERMARK)

  def __init__(
      self,
      window_sec: float,
      route_dupes_to_dlq: bool,
      err_tag: str,
      route_errors: bool,
  ):
    self.window_sec = window_sec
    self.route_dupes_to_dlq = route_dupes_to_dlq
    self.err_tag = err_tag
    self.route_errors = route_errors
    self._dropped_ctr = metrics.Metrics.counter(
        'beam_cli', 'duplicates_dropped')

  def process(
      self,
      element,
      seen=beam.DoFn.StateParam(SEEN_STATE),
      ttl_timer=beam.DoFn.TimerParam(TTL_TIMER),
  ):
    _, (bucket_idx, row) = element
    if seen.read():
      if self.route_dupes_to_dlq and self.route_errors:
        yield pvalue.TaggedOutput(
            self.err_tag,
            _make_dlq_row(
                'CliDeduplicate',
                'DuplicateKeyError',
                'Duplicate key suppressed within deduplication window',
                row))
      else:
        self._dropped_ctr.inc(1)
      return

    seen.write(True)
    expiry = min(
        _MAX_BEAM_TS, max(_MIN_BEAM_TS, (bucket_idx + 1) * self.window_sec))
    ttl_timer.set(beam_timestamp.Timestamp.of(expiry))
    yield row

  @on_timer(TTL_TIMER)
  def on_ttl(self, seen=beam.DoFn.StateParam(SEEN_STATE)):
    seen.clear()


class _StreamingProcessingDedupeDoFn(beam.DoFn):
  """Processing-time (`TimeDomain.REAL_TIME`) fixed-TTL deduplicator."""

  SEEN_STATE = ReadModifyWriteStateSpec('seen', BooleanCoder())
  TTL_TIMER = TimerSpec('ttl', TimeDomain.REAL_TIME)

  def __init__(
      self,
      window_sec: float,
      route_dupes_to_dlq: bool,
      err_tag: str,
      route_errors: bool,
  ):
    self.window_sec = window_sec
    self.route_dupes_to_dlq = route_dupes_to_dlq
    self.err_tag = err_tag
    self.route_errors = route_errors
    self._dropped_ctr = metrics.Metrics.counter(
        'beam_cli', 'duplicates_dropped')

  def process(
      self,
      element,
      seen=beam.DoFn.StateParam(SEEN_STATE),
      ttl_timer=beam.DoFn.TimerParam(TTL_TIMER),
  ):
    _, row = element
    if seen.read():
      if self.route_dupes_to_dlq and self.route_errors:
        yield pvalue.TaggedOutput(
            self.err_tag,
            _make_dlq_row(
                'CliDeduplicate',
                'DuplicateKeyError',
                'Duplicate key suppressed within processing-time window',
                row))
      else:
        self._dropped_ctr.inc(1)
      return

    seen.write(True)
    ttl_timer.set(beam_timestamp.Timestamp.now() + self.window_sec)
    yield row

  @on_timer(TTL_TIMER)
  def on_ttl(self, seen=beam.DoFn.StateParam(SEEN_STATE)):
    seen.clear()


def cli_deduplicate(
    error_handling: dict[str, Any] | None = None,
    **kwargs,
) -> beam.PTransform:
  """Beam YAML factory callable for `type: CliDeduplicate` / `Deduplicate`."""
  cfg = parse_deduplicate_config(kwargs)
  err_tag = (error_handling or {}).get('output', 'errors')
  route_errors = bool(error_handling and 'output' in error_handling)
  if cfg.route_duplicates_to_dlq and not route_errors:
    raise ValueError(
        'dedupe_dlq requires error_handling / --dlq to be configured')

  dlq_row_type = RowTypeConstraint.from_fields([
      ('step', str),
      ('error_type', str),
      ('error', str),
      ('raw_payload', str),
      ('element', str),
  ])

  class _KeyExtractDoFn(beam.DoFn):
    def __init__(
        self,
        d_cfg: DeduplicateConfig,
        use_buckets: bool,
        use_elem_ts: bool,
        wrap_ts: bool = False,
    ):
      self.cfg = d_cfg
      self.use_buckets = use_buckets
      self.use_elem_ts = use_elem_ts
      self.wrap_ts = wrap_ts

    def process(self, element, elem_ts=beam.DoFn.TimestampParam):
      try:
        ckey = extract_canonical_key(element, self.cfg.keys)
        raw_ts: float | None = None
        if self.wrap_ts:
          try:
            et = float(elem_ts)
            if math.isfinite(et) and _MIN_BEAM_TS < et < _MAX_BEAM_TS:
              raw_ts = et
          except Exception:  # pylint: disable=broad-except
            raw_ts = None
        payload_elem = (raw_ts, element) if self.wrap_ts else element
        if self.use_buckets and self.cfg.window_sec is not None:
          if self.cfg.timestamp_col:
            t = interval_join.extract_record_timestamp(
                element, self.cfg.timestamp_col)
          elif self.use_elem_ts:
            t = float(elem_ts)
          else:
            t = 0.0
          if not math.isfinite(t) or t <= _MIN_BEAM_TS or t >= _MAX_BEAM_TS:
            raise ValueError(f'out-of-range deduplication timestamp {t!r}')
          bucket_idx = int(math.floor(t / self.cfg.window_sec))
          full_key = ckey + struct.pack('>q', bucket_idx)
          yield (full_key, (bucket_idx, payload_elem))
        else:
          yield (ckey, payload_elem)
      except Exception as e:  # pylint: disable=broad-except
        if route_errors:
          yield pvalue.TaggedOutput(
              err_tag,
              _make_dlq_row(
                  'CliDeduplicate', type(e).__name__, str(e), element))
        else:
          raise

  class _EmitBoundedWinnerDoFn(beam.DoFn):
    def process(self, kv):
      _, (w_ts, w_row) = kv
      if w_ts is not None:
        yield TimestampedValue(w_row, beam_timestamp.Timestamp.of(w_ts))
      else:
        yield w_row

  class _BoundedGroupDoFn(beam.DoFn):
    def __init__(self, d_cfg: DeduplicateConfig):
      self.cfg = d_cfg
      self._dropped_ctr = metrics.Metrics.counter(
          'beam_cli', 'duplicates_dropped')

    def process(self, element):
      _, items_iter = element
      unpacked: list[tuple[float | None, Any]] = []
      for x in items_iter:
        inner = (
            x[1] if (
                self.cfg.window_sec is not None and isinstance(x, tuple) and
                len(x) == 2 and isinstance(x[0], int)) else x)
        if isinstance(
            inner, tuple) and len(inner) == 2 and (inner[0] is None or
                                                   isinstance(inner[0], float)):
          unpacked.append(inner)
        else:
          unpacked.append((None, inner))
      if not unpacked:
        return
      winner_idx = 0
      best_ord = _sortable_order_key(unpacked[0][1], self.cfg.order_col)
      for idx in range(1, len(unpacked)):
        cand_ord = _sortable_order_key(unpacked[idx][1], self.cfg.order_col)
        if self.cfg.order_col:
          take_new = ((cand_ord >= best_ord) if self.cfg.keep == 'last' else
                      (cand_ord < best_ord))
        else:
          take_new = self.cfg.keep == 'last'
        if take_new:
          winner_idx = idx
          best_ord = cand_ord
      w_ts, w_row = unpacked[winner_idx]
      if w_ts is not None:
        yield TimestampedValue(w_row, beam_timestamp.Timestamp.of(w_ts))
      else:
        yield w_row
      for idx, (_, row) in enumerate(unpacked):
        if idx == winner_idx:
          continue
        if self.cfg.route_duplicates_to_dlq and route_errors:
          yield pvalue.TaggedOutput(
              err_tag,
              _make_dlq_row(
                  'CliDeduplicate',
                  'DuplicateKeyError',
                  'Duplicate key suppressed',
                  row))
        else:
          self._dropped_ctr.inc(1)

  class _CliDeduplicateTransform(beam.PTransform):
    def expand(self, pcoll):
      in_type = getattr(pcoll, 'element_type', None)
      is_bounded = getattr(pcoll, 'is_bounded', True)

      if not is_bounded:
        if cfg.window_sec is None:
          raise ValueError(
              'CliDeduplicate on an unbounded stream requires --dedupe-window '
              '(e.g. window: "10m") to prevent unbounded Windmill state growth')
        if cfg.keep != 'first' or cfg.order_col is not None:
          raise ValueError(
              'CliDeduplicate on an unbounded stream only supports keep="first" '
              'without order_by in v1; use bounded inputs or --window for '
              'keep="last"')
      elif not cfg.force_stateful and cfg.time_domain == 'processing':
        raise ValueError(
            'CliDeduplicate time_domain="processing" cannot be used on bounded '
            'inputs; use time_domain="event" or omit time_domain')

      dlq_pcolls: list[Any] = []

      if is_bounded and not cfg.force_stateful:
        use_buckets = bool(cfg.window_sec is not None)
        keyed_out = (
            pcoll
            | 'KeyBounded' >> beam.ParDo(
                _KeyExtractDoFn(
                    cfg,
                    use_buckets=use_buckets,
                    use_elem_ts=bool(cfg.window_sec and not cfg.timestamp_col),
                    wrap_ts=True)).with_outputs(err_tag, main='good'))
        keyed_good = keyed_out['good'] | 'TypeKeyedBounded' >> beam.Map(
            lambda kv: kv).with_output_types(tuple[bytes, Any])
        if route_errors:
          dlq_pcolls.append(keyed_out[err_tag])

        if not cfg.route_duplicates_to_dlq and not use_buckets:
          good = (
              keyed_good
              | 'CombinePerKeyDedupe' >> beam.CombinePerKey(
                  _BoundedDedupeCombineFn(cfg.keep, cfg.order_col))
              | 'EmitWinnerDedupe' >> beam.ParDo(_EmitBoundedWinnerDoFn()))
        else:
          grp_out = (
              keyed_good
              | 'GroupByKeyDedupe' >> beam.GroupByKey()
              | 'SelectWinnerBounded' >> beam.ParDo(
                  _BoundedGroupDoFn(cfg)).with_outputs(err_tag, main='good'))
          good = grp_out['good']
          if route_errors:
            dlq_pcolls.append(grp_out[err_tag])
      else:
        if cfg.window_sec is None:
          raise ValueError(
              'stateful CliDeduplicate requires a positive window duration')
        resolved_domain = ((
            'event' if
            (cfg.timestamp_col or cfg.force_stateful) else 'processing')
                           if cfg.time_domain == 'auto' else cfg.time_domain)
        use_buckets = resolved_domain == 'event'
        keyed_out = (
            pcoll
            | 'KeyStreaming' >> beam.ParDo(
                _KeyExtractDoFn(cfg, use_buckets=use_buckets,
                                use_elem_ts=True)).with_outputs(
                                    err_tag, main='good'))
        keyed_good = keyed_out['good'] | 'TypeKeyedStreaming' >> beam.Map(
            lambda kv: kv).with_output_types(tuple[bytes, Any])
        if route_errors:
          dlq_pcolls.append(keyed_out[err_tag])

        state_dofn = (
            _StreamingEventDedupeDoFn(
                cfg.window_sec,
                cfg.route_duplicates_to_dlq,
                err_tag,
                route_errors)
            if resolved_domain == 'event' else _StreamingProcessingDedupeDoFn(
                cfg.window_sec,
                cfg.route_duplicates_to_dlq,
                err_tag,
                route_errors))
        st_out = (
            keyed_good
            | 'StatefulDeduplicate' >> beam.ParDo(state_dofn).with_outputs(
                err_tag, main='good'))
        good = st_out['good']
        if route_errors:
          dlq_pcolls.append(st_out[err_tag])

      if in_type is not None:
        good.element_type = in_type
      if route_errors:
        for d_pc in dlq_pcolls:
          d_pc.element_type = dlq_row_type
        bad = (
            dlq_pcolls[0] if len(dlq_pcolls) == 1 else
            (tuple(dlq_pcolls) | 'FlattenDedupeDlq' >> beam.Flatten()))
        bad.element_type = dlq_row_type
        return {'good': good, err_tag: bad}
      return good

  return _CliDeduplicateTransform()


CliDeduplicate = cli_deduplicate
