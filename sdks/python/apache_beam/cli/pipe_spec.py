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

"""Builds the Beam YAML pipeline that `beam pipe` runs.

Supports:
- File formats (`csv`, `json`/`jsonl`, `parquet`, `avro`) across local & cloud FS
- Universal URIs (`bigquery://`, `postgres://`, `pubsub://`, `kafka://`, `iceberg://`)
- Safe tokenized `--where` and `--select` AST compilation (`language: python`
  `callable:` via `getattr(__row__, 'col')`)
- RFC-4180 multi-shard CSV & JSONL `--dlq` routing + normalized DLQ `Flatten`
- Fusion-safe `beam_cli` row counters (`rows_read`, `pre_sink_rows`, `dlq_rows`)
"""

import os
import re
import shlex
from typing import Any
from typing import Iterable
from typing import NamedTuple

from . import dlq
from . import expressions
from . import uris


class PipeSpecError(ValueError):
  """A problem with `beam pipe`'s flags, reported to the user as-is."""


class PipeSpec(NamedTuple):
  """The pipeline for one `beam pipe` command."""
  spec: dict[str, Any]
  output_prefix: str | None
  source_endpoint: uris.ParsedEndpoint | None = None
  sink_endpoint: uris.ParsedEndpoint | None = None
  dlq_endpoint: uris.ParsedEndpoint | None = None
  requires_java_xlang: bool = False
  is_unbounded: bool = False
  late_dlq_endpoint: uris.ParsedEndpoint | None = None


def _normalize_primary_keys(primary_keys: Iterable[str] | None) -> list[str]:
  """Flattens and deduplicates primary key arguments.

  Supports repeated arguments (`--key id --key dept`), comma-separated strings
  (`--key id,dept`), and mixed collections, preserving first-seen column ordering.
  """
  out: list[str] = []
  for raw_pk in primary_keys or ():
    for part in str(raw_pk).split(','):
      col = part.strip()
      if col and col not in out:
        out.append(col)
  return out


def _materialize_cdc_config(pks, seq_col):
  cfg: dict[str, Any] = {'primary_key': pks}
  if seq_col:
    cfg['sequence_column'] = seq_col
  return cfg


_AGG_CALL_RE = re.compile(
    r'^(?:(?P<alias>[A-Za-z0-9_]+)\s*=\s*)?(?P<fn>[A-Za-z0-9_]+)\s*\(\s*(?P<val>[A-Za-z0-9_*]*)\s*\)$'
)


def _split_window_tokens(s: str) -> list[str]:
  """Splits window specification tokens by commas outside nested delimiters.

  Respects grouping delimiters `()`, `[]`, and `{}` so that embedded lists
  (e.g. `group_by=[region, dept]`) or composite arguments (e.g. `sum(metric)`)
  are not prematurely split across commas.
  """
  tokens: list[str] = []
  cur: list[str] = []
  depth = 0
  for ch in s:
    if ch in '([{':
      depth += 1
      cur.append(ch)
    elif ch in ')]}':
      if depth > 0:
        depth -= 1
      cur.append(ch)
    elif ch == ',' and depth == 0:
      tok = ''.join(cur).strip()
      if tok:
        tokens.append(tok)
      cur = []
    else:
      cur.append(ch)
  if cur:
    tok = ''.join(cur).strip()
    if tok:
      tokens.append(tok)
  return tokens


def _parse_inline_aggregations(
    tokens: list[str]
) -> tuple[dict[str, Any], list[str], list[str]]:
  """Extracts inline group_by and aggregation expressions from window tokens.

  Rationale:
  In Apache Beam YAML, `WindowInto` defines element assignment to time windows
  along the watermark timeline, but does not itself reduce data. Aggregation
  is performed by `Combine`. To give users an ergonomic shorthand like:
    --window "fixed:60s,sum(metric),avg(latency),by=region"
  this function separates the window parameters from the reduction spec,
  allowing the pipeline compiler to emit both a `WindowInto` step and a
  subsequent `Combine` step.

  Returns:
    (combines_dict, group_by_list, remaining_window_tokens)
  """
  combines: dict[str, Any] = {}
  group_by: list[str] = []
  remaining: list[str] = []
  for t in tokens:
    # 1. Parse optional grouping key specification: group_by=... or by=...
    if t.startswith(('group_by=', 'by=')):
      val = t.split('=', 1)[1]
      cols = [c.strip("[] '\"") for c in val.split(',') if c.strip("[] '\"")]
      group_by.extend(cols)
      continue

    # 2. Parse aggregation call: [alias=]fn(col)
    m = _AGG_CALL_RE.match(t)
    if m:
      fn = m.group('fn').lower()
      # Beam YAML Combine uses 'mean' for averages.
      fn_clean = 'mean' if fn == 'avg' else fn
      val = m.group('val').strip()
      alias = m.group('alias')
      # Infer output alias if not explicitly provided
      if not alias:
        alias = val if val and val != '*' else (fn if fn != 'mean' else 'avg')
      if not val or val == '*':
        val = alias if alias != fn_clean else '1'
      combines[alias] = {'value': val, 'fn': fn_clean}
      continue

    remaining.append(t)
  return combines, group_by, remaining


def parse_window_spec(spec: str | dict[str, Any]) -> dict[str, Any]:
  """Parses a `--window` string into a Beam YAML windowing dict."""
  if isinstance(spec, dict):
    res = dict(spec)
    w_type = str(res.get('type') or '').lower()
    if w_type == 'session':
      res['type'] = 'sessions'
    return res

  s = str(spec or '').strip()
  if not s:
    raise PipeSpecError('--window specification cannot be empty')

  if s.startswith('{') and s.endswith('}'):
    import json
    try:
      d = json.loads(s)
      return parse_window_spec(d)
    except Exception as e:
      raise PipeSpecError(f'malformed JSON in --window: {e}') from None

  if '=' in s and ':' not in s.split('=', 1)[0] and not _AGG_CALL_RE.search(s):
    parts = [p.strip() for p in s.split(',') if p.strip()]
    kv: dict[str, Any] = {}
    for p in parts:
      if '=' in p:
        k, v = p.split('=', 1)
        kv[k.strip().lower()] = v.strip()
    if 'type' in kv:
      if kv['type'] == 'session':
        kv['type'] = 'sessions'
      return kv

  prefix = ''
  rest = s
  if ':' in s:
    cand_prefix, cand_rest = s.split(':', 1)
    if cand_prefix.lower() in ('fixed', 'sliding', 'session', 'sessions', 'global'):
      prefix = cand_prefix.lower()
      rest = cand_rest.strip()

  tokens = _split_window_tokens(rest)
  combines, group_by, rem_tokens = _parse_inline_aggregations(tokens)

  if not prefix:
    prefix = 'fixed'

  if prefix == 'fixed':
    if not rem_tokens:
      raise PipeSpecError(
          f"invalid fixed window specification {s!r}; expected fixed:DURATION (e.g. 'fixed:60s')")
    out: dict[str, Any] = {'type': 'fixed', 'size': rem_tokens[0]}
    if len(rem_tokens) > 1:
      out['offset'] = rem_tokens[1]
  elif prefix == 'sliding':
    if len(rem_tokens) < 2:
      raise PipeSpecError(
          f"invalid sliding window specification {s!r}; expected sliding:SIZE,PERIOD (e.g. 'sliding:5m,1m')")
    out = {'type': 'sliding', 'size': rem_tokens[0], 'period': rem_tokens[1]}
    if len(rem_tokens) > 2:
      out['offset'] = rem_tokens[2]
  elif prefix in ('session', 'sessions'):
    if not rem_tokens:
      raise PipeSpecError(
          f"invalid session window specification {s!r}; expected session:GAP (e.g. 'session:30m')")
    out = {'type': 'sessions', 'gap': rem_tokens[0]}
  elif prefix == 'global':
    out = {'type': 'global'}
  else:
    raise PipeSpecError(
        f"unknown window type in --window {s!r}; expected fixed:DURATION, "
        "sliding:SIZE,PERIOD, or session:GAP")

  if combines:
    out['aggregations'] = {'group_by': group_by, 'combine': combines}
  return out


def _split_source_alias_prefix(raw: str) -> tuple[str | None, str]:
  s = (raw or '').strip()
  if '=' in s and not os.path.exists(s):
    m = re.match(r'^(?P<alias>[A-Za-z_][A-Za-z0-9_]*)=(?P<uri>.+)$', s)
    if m and m.group('uri').strip():
      return m.group('alias'), m.group('uri').strip()
  return None, s


def build_pipe_spec(
    source: str,
    sink: str,
    from_format: str | None = None,
    to_format: str | None = None,
    *,
    where: str | None = None,
    select: str | None = None,
    dlq_sink: str | None = None,
    dlq_format: str | None = None,
    null_values: Iterable[str] | None = None,
    if_exists: str = 'fail',
    from_opts: Iterable[str] | None = None,
    to_opts: Iterable[str] | None = None,
    from_opt_file: str | None = None,
    to_opt_file: str | None = None,
    default_project: str | None = None,
    known_source_columns: set[str] | None = None,
    include_counters: bool = False,
    prefer_python_bigquery_sink: bool = False,
    cdc: bool = False,
    primary_keys: Iterable[str] | None = None,
    cdc_format: str = 'auto',
    cdc_op_col: str | None = None,
    cdc_seq_col: str | None = None,
    zero_copy: bool = False,
    prefer_python_lakehouse_and_cdc: bool = False,
    allow_jinja: bool = False,
    window: str | dict[str, Any] | None = None,
    timestamp_col: str | None = None,
    expect: list[str] | list[dict[str, Any]] | None = None,
    on_violation: str = 'dlq',
    join_source: str | None = None,
    interval_join: str | dict[str, Any] | None = None,
    asof_join: str | dict[str, Any] | None = None,
    max_watermark_skew: str | None = None,
    skew_action: str | None = None,
    dedupe_by: Iterable[str] | str | None = None,
    dedupe_window: str | None = None,
    dedupe_keep: str = 'first',
    dedupe_order: str | None = None,
    dedupe_time_domain: str = 'auto',
    dedupe_dlq: bool = False,
    pre_select: str | None = None,
    post_where: str | None = None,
    enrich: Iterable[str | dict[str, Any]] | str | dict[str, Any] | None = None,
    enrich_opts: Iterable[str] | None = None,
    enrich_opt_file: str | None = None,
    infer: Iterable[str | dict[str, Any]] | str | dict[str, Any] | None = None,
    infer_opts: Iterable[str] | None = None,
    infer_opt_file: str | None = None,
    infer_fields: str | None = None,
    infer_tag: str | None = None,
    infer_flatten: bool = False,
    allowed_lateness: str | None = None,
    trigger: str | None = None,
    accumulation: str | None = None,
    late_action: str | None = None,
    late_data_dlq: str | None = None,
    emit_pane_info: bool = False,
    watermark_lag: str | None = None,
    dlq_redact_fields: Iterable[str] | str | None = None,
) -> PipeSpec:
  """Returns the `PipeSpec` for `beam pipe`."""
  if if_exists not in ('fail', 'append', 'replace'):
    raise PipeSpecError(
        f'invalid --if-exists mode {if_exists!r}; use fail, append, or replace')

  pks = _normalize_primary_keys(primary_keys)
  if cdc_format not in ('auto', 'debezium', 'flat'):
    raise PipeSpecError(
        f'invalid --cdc-format {cdc_format!r}; use auto, debezium, or flat')

  if interval_join and asof_join:
    raise PipeSpecError(
        '--interval-join and --asof-join are mutually exclusive')
  has_two_stream_join = bool(interval_join or asof_join)
  join_flag_label = '--asof-join' if asof_join else '--interval-join'
  if has_two_stream_join and not join_source:
    raise PipeSpecError(
        f'{join_flag_label} requires a second source via --join [ALIAS=]URI '
        '(or a second --from)')
  if join_source and not has_two_stream_join:
    raise PipeSpecError(
        '--join without --sql requires --interval-join <SPEC> or '
        '--asof-join <SPEC>')
  if (max_watermark_skew is not None or
      skew_action is not None) and not has_two_stream_join:
    raise PipeSpecError(
        '--max-watermark-skew / --skew-action requires --interval-join or '
        '--asof-join (or a 2-source temporal --sql JOIN)')
  if has_two_stream_join and cdc:
    raise PipeSpecError(f'{join_flag_label} cannot be combined with --cdc')

  left_alias_hint: str | None = None
  right_alias_hint: str | None = None
  clean_join_source: str | None = None
  if has_two_stream_join:
    left_alias_hint, source = _split_source_alias_prefix(source)
    right_alias_hint, clean_join_source = _split_source_alias_prefix(
        str(join_source))

  try:
    parsed_from_opts = uris.parse_kv_options(
        from_opts, from_opt_file, '--from-opt')
    parsed_to_opts = uris.parse_kv_options(to_opts, to_opt_file, '--to-opt')
    raw_cdc_fields = parsed_from_opts.pop('fields', None) if cdc else None
    if (',' in source and '://' not in source and not allow_jinja and
        not dlq._match_paths(source)):
      parts = [p.strip() for p in source.split(',') if p.strip()]
      if len(parts) >= 2 and any(
          uris.extension_of(p) or dlq._match_paths(p) for p in parts):
        raise PipeSpecError(
            'comma-separated paths in --from are not supported; pass '
            '--from a=FIRST --join b=SECOND with --sql and UNION ALL '
            '(or a single glob such as dir/*)')
    src_ep = uris.parse_endpoint(
        source,
        is_read=True,
        explicit_format=from_format,
        options=dict(parsed_from_opts),
        default_project=default_project,
        allow_sniff=not allow_jinja,
        cdc=cdc,
        allow_jinja=allow_jinja)
    right_ep = (
        uris.parse_endpoint(
            clean_join_source,
            is_read=True,
            explicit_format=from_format,
            options=dict(parsed_from_opts),
            default_project=default_project,
            allow_sniff=not allow_jinja,
            allow_jinja=allow_jinja) if clean_join_source else None)
    dst_ep = uris.parse_endpoint(
        sink,
        is_read=False,
        explicit_format=to_format,
        options=parsed_to_opts,
        if_exists=if_exists,
        default_project=default_project,
        cdc=cdc,
        zero_copy=zero_copy,
        allow_jinja=allow_jinja)
    dlq_default_fmt = None
    if dlq_sink and not dlq_format:
      if not (uris.extension_of(dlq_sink) or (
          '://' in dlq_sink and
          dlq_sink.split('://', 1)[0].lower() not in uris.FILE_SCHEMES)):
        dlq_default_fmt = (
            (uris._infer_jinja_file_format(dlq_sink) or 'json')
            if allow_jinja and uris._has_jinja_syntax(dlq_sink) else 'json')
    dlq_ep = (
        uris.parse_endpoint(
            dlq_sink,
            is_read=False,
            explicit_format=dlq_format or dlq_default_fmt,
            if_exists='append' if if_exists == 'append' else 'fail',
            default_project=default_project,
            allow_jinja=allow_jinja) if dlq_sink else None)
    late_dlq_default_fmt = None
    if late_data_dlq:
      if not (uris.extension_of(late_data_dlq) or
              ('://' in late_data_dlq and late_data_dlq.split(
                  '://', 1)[0].lower() not in uris.FILE_SCHEMES)):
        late_dlq_default_fmt = (
            (uris._infer_jinja_file_format(late_data_dlq) or 'json') if
            allow_jinja and uris._has_jinja_syntax(late_data_dlq) else 'json')
    late_dlq_ep = (
        uris.parse_endpoint(
            late_data_dlq,
            is_read=False,
            explicit_format=late_dlq_default_fmt,
            if_exists='append' if if_exists == 'append' else 'fail',
            default_project=default_project,
            allow_jinja=allow_jinja) if late_data_dlq else None)
  except uris.UriError as e:
    raise PipeSpecError(str(e)) from None

  is_unbounded = bool(
      src_ep.is_unbounded or (right_ep is not None and right_ep.is_unbounded))

  parsed_expect_rules = []
  if expect:
    from . import expect as expect_mod
    for item in expect:
      try:
        r = expect_mod.parse_expectation_rule(item, default_on_violation=on_violation)
        parsed_expect_rules.append(r)
      except (ValueError, TypeError) as e:
        raise PipeSpecError(f'invalid --expect rule {item!r}: {e}') from None
    if any(r.on_violation == 'dlq' for r in parsed_expect_rules) and dlq_ep is None:
      raise PipeSpecError(
          "cannot route expectation violations to DLQ without a DLQ sink; "
          "specify --dlq <PATH> or use --on-violation {fail|drop}")

  has_b23_window_opts = bool(
      (
          allowed_lateness is not None and
          (window is not None or asof_join is None)) or trigger is not None or
      accumulation is not None or late_action is not None or
      late_dlq_ep is not None or emit_pane_info or watermark_lag is not None)

  if zero_copy:
    if (cdc or where or pre_select or post_where or select or
        dlq_ep is not None or window or has_b23_window_opts or timestamp_col or
        expect or interval_join or asof_join or allowed_lateness is not None or
        dedupe_by or enrich or infer):
      raise PipeSpecError(
          '--zero-copy registers existing Parquet files in-place without '
          'rewriting data, so it cannot be combined with --cdc, --where, '
          '--pre-select, --enrich, --infer, --post-where, --select, --window, '
          '--allowed-lateness, --trigger, --timestamp-col, --expect, '
          '--interval-join, --asof-join, --dedupe-by, or --dlq')
    if dst_ep.kind != 'iceberg':
      raise PipeSpecError(
          f'--zero-copy requires an iceberg:// destination, got {dst_ep.raw!r}')
    if src_ep.kind != 'delta' and not (
        src_ep.kind == 'file' and src_ep.format == 'parquet'):
      raise PipeSpecError(
          '--zero-copy requires a delta:// table or Parquet file/glob source, '
          f'got {src_ep.raw!r}')

    # Build canonical `IcebergAddFiles` YAML representation for `--emit-yaml`.
    zc_steps: list[dict[str, Any]] = []
    zc_providers: list[dict[str, Any]] = []
    if src_ep.kind == 'delta':
      active_paths: list[dict[str, str]] = []
      try:
        from . import lakehouse
        snap = lakehouse.parse_delta_log(
            src_ep.config['table'],
            target_version=(
                int(src_ep.config['version'])
                if src_ep.config.get('version') is not None else None))
        active_paths = [{'path': f['path']} for f in snap['active_files']]
      except Exception:  # pylint: disable=broad-except
        active_paths = []
      if active_paths:
        zc_steps.append({'type': 'Create', 'config': {'elements': active_paths}})
      else:
        pat = src_ep.config['table'].rstrip('/') + '/*.parquet'
        zc_steps.extend([
            {'type': 'ReadMatchFiles', 'config': {'file_pattern': pat}},
            {
                'type': 'MapToFields',
                'config': {
                    'language': 'python',
                    'fields': {
                        'path': {
                            'callable': 'lambda row: row.path',
                            'output_type': 'string',
                        }
                    },
                },
            },
        ])
        zc_providers.append({
            'type': 'python',
            'config': {},
            'transforms': {'ReadMatchFiles': 'apache_beam.io.fileio.MatchFiles'},
        })
    else:
      zc_steps.extend([
          {
              'type': 'ReadMatchFiles',
              'config': {'file_pattern': src_ep.config['path']},
          },
          {
              'type': 'MapToFields',
              'config': {
                  'language': 'python',
                  'fields': {
                      'path': {
                          'callable': 'lambda row: row.path',
                          'output_type': 'string',
                      }
                  },
              },
          },
      ])
      zc_providers.append({
          'type': 'python',
          'config': {},
          'transforms': {'ReadMatchFiles': 'apache_beam.io.fileio.MatchFiles'},
      })
    zc_steps.append({'type': 'IcebergAddFiles', 'config': dict(dst_ep.config)})
    zc_spec: dict[str, Any] = {
        'pipeline': {'type': 'chain', 'transforms': zc_steps}
    }
    if zc_providers:
      zc_spec['providers'] = zc_providers
    return PipeSpec(
        spec=zc_spec,
        output_prefix=None,
        source_endpoint=src_ep,
        sink_endpoint=dst_ep,
        dlq_endpoint=None,
        requires_java_xlang=True,
        is_unbounded=False)

  if cdc and dst_ep.kind in ('bigquery', 'iceberg') and not pks:
    raise PipeSpecError(
        f'--cdc targeting {dst_ep.kind}:// requires --primary-key <col>')

  if dst_ep.kind == 'file' and if_exists == 'append':
    raise PipeSpecError(
        '--if-exists=append is only supported for table/stream sinks '
        '(such as bigquery://, iceberg://, or postgres://); file sinks write '
        'immutable shards (use a new prefix or --if-exists=replace)')

  window_spec = parse_window_spec(window) if window else None
  window_aggs = window_spec.pop('aggregations', None) if window_spec else None
  clean_timestamp_col = str(timestamp_col).strip() if timestamp_col else None
  if clean_timestamp_col == '':
    raise PipeSpecError('--timestamp-col cannot be empty')

  interval_join_cfg: dict[str, Any] | None = None
  ij_parsed = None
  if interval_join is not None:
    from . import interval_join as ij_mod
    eff_skew_act = skew_action or (
        'dlq' if
        (dlq_ep is not None and max_watermark_skew is not None) else None)
    try:
      if isinstance(interval_join, str):
        interval_join_cfg = ij_mod.parse_interval_join_spec_str(
            interval_join,
            max_watermark_skew=max_watermark_skew,
            skew_action=eff_skew_act,
            left_alias=left_alias_hint,
            right_alias=right_alias_hint)
      elif isinstance(interval_join, dict):
        interval_join_cfg = dict(interval_join)
        if left_alias_hint and 'left_alias' not in interval_join_cfg:
          interval_join_cfg['left_alias'] = left_alias_hint
        if right_alias_hint and 'right_alias' not in interval_join_cfg:
          interval_join_cfg['right_alias'] = right_alias_hint
        if max_watermark_skew is not None:
          interval_join_cfg['max_watermark_skew'] = max_watermark_skew
        if eff_skew_act is not None and 'skew_action' not in interval_join_cfg:
          interval_join_cfg['skew_action'] = eff_skew_act
      else:
        raise ValueError(
            f'unsupported interval_join type {type(interval_join).__name__}')
      ij_parsed = ij_mod.parse_interval_join_config(interval_join_cfg)
    except ValueError as e:
      raise PipeSpecError(str(e)) from None
    if ij_parsed.skew_action == 'dlq' and dlq_ep is None:
      raise PipeSpecError(
          '--skew-action=dlq requires --dlq <SINK> (or use --skew-action {drop|warn|fail})'
      )

  asof_join_cfg: dict[str, Any] | None = None
  aj_parsed = None
  if asof_join is not None:
    from . import asof_join as aj_mod
    eff_skew_act = skew_action or (
        'dlq' if
        (dlq_ep is not None and max_watermark_skew is not None) else None)
    eff_asof_lateness = allowed_lateness if window is None else None
    try:
      if isinstance(asof_join, str):
        asof_join_cfg = aj_mod.parse_asof_join_spec_str(
            asof_join,
            max_watermark_skew=max_watermark_skew,
            skew_action=eff_skew_act,
            left_alias=left_alias_hint,
            right_alias=right_alias_hint,
            allowed_lateness=eff_asof_lateness)
      elif isinstance(asof_join, dict):
        asof_join_cfg = dict(asof_join)
        if left_alias_hint and 'left_alias' not in asof_join_cfg:
          asof_join_cfg['left_alias'] = left_alias_hint
        if right_alias_hint and 'right_alias' not in asof_join_cfg:
          asof_join_cfg['right_alias'] = right_alias_hint
        if max_watermark_skew is not None:
          asof_join_cfg['max_watermark_skew'] = max_watermark_skew
        if eff_skew_act is not None and 'skew_action' not in asof_join_cfg:
          asof_join_cfg['skew_action'] = eff_skew_act
        if (eff_asof_lateness is not None and
            'allowed_lateness' not in asof_join_cfg):
          asof_join_cfg['allowed_lateness'] = eff_asof_lateness
      else:
        raise ValueError(
            f'unsupported asof_join type {type(asof_join).__name__}')
      aj_parsed = aj_mod.parse_asof_join_config(asof_join_cfg)
    except ValueError as e:
      raise PipeSpecError(str(e)) from None
    if aj_parsed.skew_action == 'dlq' and dlq_ep is None:
      raise PipeSpecError(
          '--skew-action=dlq requires --dlq <SINK> (or use --skew-action {drop|warn|fail})'
      )
    if (is_unbounded and aj_parsed.direction in ('forward', 'nearest') and
        aj_parsed.tolerance is None):
      raise PipeSpecError(
          f'unbounded streaming --asof-join with direction={aj_parsed.direction!r} '
          'requires a finite tolerance=DURATION (e.g. tolerance=5m)')

  dedupe_yaml_cfg: dict[str, Any] | None = None
  if not dedupe_by:
    if (dedupe_window is not None or dedupe_order is not None or dedupe_dlq or
        (dedupe_keep and dedupe_keep != 'first') or
        (dedupe_time_domain and dedupe_time_domain != 'auto')):
      raise PipeSpecError(
          '--dedupe-window, --dedupe-keep, --dedupe-order, '
          '--dedupe-time-domain, and --dedupe-dlq require --dedupe-by <COLS>')
  else:
    from . import dedup as dedup_mod
    raw_dcfg: dict[str, Any] = {
        'keys': dedupe_by,
        'keep': dedupe_keep or 'first',
        'time_domain': dedupe_time_domain or 'auto',
    }
    if dedupe_window is not None:
      raw_dcfg['window'] = dedupe_window
    if dedupe_order is not None:
      raw_dcfg['order_by'] = dedupe_order
    if clean_timestamp_col:
      raw_dcfg['timestamp_col'] = clean_timestamp_col
    elif ij_parsed is not None and ij_parsed.left_timestamp:
      raw_dcfg['timestamp_col'] = ij_parsed.left_timestamp
    elif aj_parsed is not None and aj_parsed.left_timestamp:
      raw_dcfg['timestamp_col'] = aj_parsed.left_timestamp
    if dedupe_dlq:
      raw_dcfg['dedupe_dlq'] = True
    if isinstance(dedupe_by, dict) and dedupe_by.get('force_stateful'):
      raw_dcfg['force_stateful'] = True
    try:
      parsed_dedup = dedup_mod.parse_deduplicate_config(raw_dcfg)
    except ValueError as e:
      raise PipeSpecError(str(e)) from None
    if is_unbounded and parsed_dedup.window_sec is None:
      raise PipeSpecError(
          'streaming --dedupe-by on an unbounded source requires '
          '--dedupe-window <DURATION> (e.g. --dedupe-window 10m)')
    if is_unbounded and (parsed_dedup.keep != 'first' or
                         parsed_dedup.order_col is not None):
      raise PipeSpecError(
          'streaming --dedupe-by on an unbounded source only supports '
          '--dedupe-keep=first without --dedupe-order; use bounded inputs or '
          '--window aggregations for keep=last')
    if (not is_unbounded and parsed_dedup.time_domain == 'processing' and
        not parsed_dedup.force_stateful):
      raise PipeSpecError(
          '--dedupe-time-domain=processing is only valid on unbounded '
          'streaming sources; use --dedupe-time-domain=event or omit it on '
          'bounded inputs')
    if parsed_dedup.route_duplicates_to_dlq and dlq_ep is None:
      raise PipeSpecError('--dedupe-dlq requires --dlq <SINK>')
    dedupe_yaml_cfg = {
        'keys': list(parsed_dedup.keys),
        'keep': parsed_dedup.keep,
    }
    if dedupe_window is not None:
      dedupe_yaml_cfg['window'] = str(dedupe_window).strip()
    if parsed_dedup.order_col:
      dedupe_yaml_cfg['order_by'] = parsed_dedup.order_col
    if parsed_dedup.time_domain != 'auto':
      dedupe_yaml_cfg['time_domain'] = parsed_dedup.time_domain
    if parsed_dedup.timestamp_col:
      dedupe_yaml_cfg['timestamp_col'] = parsed_dedup.timestamp_col
    if parsed_dedup.route_duplicates_to_dlq:
      dedupe_yaml_cfg['dedupe_dlq'] = True
    if parsed_dedup.force_stateful:
      dedupe_yaml_cfg['force_stateful'] = True

  parsed_dlq_redact: tuple[str, ...] = ()
  if dlq_redact_fields:
    if dlq_ep is None:
      raise PipeSpecError('--dlq-redact-fields requires --dlq <SINK>')
    from . import interval_join as ij_mod
    from . import output as out_mod
    raw_rf = ij_mod.parse_column_list(dlq_redact_fields)
    for rf in raw_rf:
      if not out_mod.SAFE_IDENTIFIER_RE.match(rf):
        raise PipeSpecError(
            f'invalid column identifier {rf!r} in --dlq-redact-fields')
    parsed_dlq_redact = raw_rf

  raw_enrich_items: list[str | dict[str, Any]]
  if enrich is None:
    raw_enrich_items = []
  elif isinstance(enrich, (str, dict)):
    raw_enrich_items = [enrich]
  else:
    raw_enrich_items = list(enrich)

  enrich_yaml_cfgs: list[dict[str, Any]] = []
  if not raw_enrich_items:
    if enrich_opts or enrich_opt_file:
      raise PipeSpecError(
          '--enrich-opt / --enrich-opt-file requires --enrich <SPEC>')
  else:
    from . import enrich as enrich_mod
    try:
      parsed_enrich_opts = uris.parse_kv_options(
          enrich_opts, enrich_opt_file, '--enrich-opt')
    except uris.UriError as e:
      raise PipeSpecError(str(e)) from None
    if parsed_dlq_redact:
      parsed_enrich_opts.setdefault(
          'dlq_redact_fields', list(parsed_dlq_redact))
    single_stage = len(raw_enrich_items) == 1
    for e_item in raw_enrich_items:
      try:
        if isinstance(e_item, str):
          e_yaml = enrich_mod.parse_enrich_spec_str(
              e_item,
              extra_opts=parsed_enrich_opts,
              default_project=default_project,
              single_stage=single_stage)
        elif isinstance(e_item, dict):
          merged_ecfg = {**parsed_enrich_opts, **e_item}
          e_yaml = enrich_mod.enrich_config_to_yaml_dict(
              enrich_mod.parse_enrich_config(
                  merged_ecfg, default_project=default_project))
        else:
          raise ValueError(
              f'unsupported --enrich item type {type(e_item).__name__}')
        e_parsed = enrich_mod.parse_enrich_config(
            e_yaml, default_project=default_project)
      except ValueError as e:
        raise PipeSpecError(str(e)) from None
      if e_parsed.on_miss == 'dlq' and dlq_ep is None:
        raise PipeSpecError(
            '--enrich on_miss=dlq requires --dlq <SINK> '
            '(or use on_miss={null|drop|fail})')
      enrich_yaml_cfgs.append(e_yaml)

  raw_infer_items: list[str | dict[str, Any]]
  if infer is None:
    raw_infer_items = []
  elif isinstance(infer, (str, dict)):
    raw_infer_items = [infer]
  else:
    raw_infer_items = list(infer)

  infer_yaml_cfgs: list[dict[str, Any]] = []
  if not raw_infer_items:
    if (infer_opts or infer_opt_file or infer_fields is not None or
        infer_tag is not None or infer_flatten):
      raise PipeSpecError(
          '--infer-opt / --infer-opt-file / --infer-fields / --infer-tag / '
          '--infer-flatten requires --infer <SPEC>')
  else:
    from . import infer as infer_mod
    try:
      parsed_infer_opts = uris.parse_kv_options(
          infer_opts, infer_opt_file, '--infer-opt')
    except uris.UriError as e:
      raise PipeSpecError(str(e)) from None
    if parsed_dlq_redact:
      parsed_infer_opts.setdefault('dlq_redact_fields', list(parsed_dlq_redact))
    single_infer = len(raw_infer_items) == 1
    seen_infer_tags: set[str] = set()
    for i_item in raw_infer_items:
      try:
        if isinstance(i_item, str):
          i_yaml = infer_mod.parse_infer_spec_str(
              i_item,
              extra_opts=parsed_infer_opts,
              infer_fields=infer_fields,
              infer_tag=infer_tag,
              infer_flatten=infer_flatten,
              default_project=default_project,
              single_stage=single_infer)
        elif isinstance(i_item, dict):
          merged_icfg = {**parsed_infer_opts, **i_item}
          if infer_fields is not None:
            merged_icfg.setdefault('fields', infer_fields)
          if infer_tag is not None:
            merged_icfg.setdefault('tag', infer_tag)
          if infer_flatten:
            merged_icfg.setdefault('flatten', True)
          i_yaml = infer_mod.infer_config_to_yaml_dict(
              infer_mod.parse_infer_config(
                  merged_icfg, default_project=default_project))
        else:
          raise ValueError(
              f'unsupported --infer item type {type(i_item).__name__}')
        i_parsed = infer_mod.parse_infer_config(
            i_yaml, default_project=default_project)
      except ValueError as e:
        raise PipeSpecError(str(e)) from None
      if i_parsed.tag in seen_infer_tags:
        raise PipeSpecError(
            f'duplicate inference output tag {i_parsed.tag!r} across multiple '
            '--infer stages; specify a distinct tag=NAME on each stage')
      seen_infer_tags.add(i_parsed.tag)
      infer_yaml_cfgs.append(i_yaml)

  cli_window_yaml_cfg: dict[str, Any] | None = None
  cli_window_parsed = None
  if has_b23_window_opts:
    if window_spec is None:
      raise PipeSpecError(
          '--allowed-lateness / --trigger / --accumulation / --late-action / '
          '--late-data-dlq / --emit-pane-info requires --window <SPEC>')
    from . import windowing as windowing_mod
    eff_late_act = late_action
    if eff_late_act is None and late_dlq_ep is not None:
      eff_late_act = 'dlq'
    raw_w_cfg: dict[str, Any] = {'windowing': dict(window_spec)}
    if allowed_lateness is not None:
      raw_w_cfg['allowed_lateness'] = allowed_lateness
    if trigger is not None:
      raw_w_cfg['trigger'] = trigger
    if accumulation is not None:
      raw_w_cfg['accumulation'] = accumulation
    if eff_late_act is not None:
      raw_w_cfg['late_action'] = eff_late_act
    if watermark_lag is not None:
      raw_w_cfg['watermark_lag'] = watermark_lag
    if parsed_dlq_redact:
      raw_w_cfg['dlq_redact_fields'] = list(parsed_dlq_redact)
    try:
      cli_window_parsed = windowing_mod.parse_windowing_config(raw_w_cfg)
    except ValueError as e:
      raise PipeSpecError(str(e)) from None
    if (cli_window_parsed.late_action == 'dlq' and dlq_ep is None and
        late_dlq_ep is None):
      raise PipeSpecError(
          '--late-action=dlq requires --late-data-dlq <SINK> or --dlq <SINK> '
          '(or use --late-action=drop)')
    if (not is_unbounded and
        cli_window_parsed.parsed_trigger.mode == 'standalone' and
        cli_window_parsed.parsed_trigger.uses_processing_time):
      raise PipeSpecError(
          'standalone processing-time --trigger every:<DURATION> requires an '
          'unbounded streaming source (use --trigger "watermark,early=..." or '
          '--trigger count:<N> on bounded inputs)')
    cli_window_yaml_cfg = windowing_mod.windowing_config_to_yaml_dict(
        cli_window_parsed)

  if asof_join_cfg is not None and parsed_dlq_redact:
    asof_join_cfg['dlq_redact_fields'] = list(parsed_dlq_redact)
  two_stream_cfg = (
      interval_join_cfg if interval_join_cfg is not None else asof_join_cfg)

  eff_where_cols = None if two_stream_cfg is not None else known_source_columns
  eff_pre_select_cols = (
      None if two_stream_cfg is not None else known_source_columns)
  eff_select_cols = (
      None if (
          two_stream_cfg is not None or window_aggs is not None or
          pre_select is not None or bool(enrich_yaml_cfgs) or
          bool(infer_yaml_cfgs) or emit_pane_info) else known_source_columns)

  try:
    where_cfg = (
        expressions.compile_where(
            where, known_columns=eff_where_cols, allow_jinja=allow_jinja)[0]
        if where else None)
    pre_select_cfg = (
        expressions.compile_select(
            pre_select,
            known_columns=eff_pre_select_cols,
            allow_jinja=allow_jinja,
            flag_name='--pre-select')[0] if pre_select else None)
    post_where_cfg = (
        expressions.compile_where(
            post_where,
            known_columns=None,
            allow_jinja=allow_jinja,
            flag_name='--post-where')[0] if post_where else None)
    select_cfg = (
        expressions.compile_select(
            select, known_columns=eff_select_cols, allow_jinja=allow_jinja)[0]
        if select else None)
  except expressions.ExpressionError as e:
    raise PipeSpecError(str(e)) from None

  if (cdc and select_cfg is not None and prefer_python_lakehouse_and_cdc and
      dst_ep.kind != 'file'):
    # Preserve normalized CDC metadata columns through `--select` projections
    # when targeting native CDC sinks (BigQuery / Iceberg); file sinks run
    # `CliMaterializeCdc` before `MapToFields`, which already strips `beam_cdc_*`.
    sel_fields = dict(select_cfg.get('fields') or {})
    for mcol, mtype in (
        ('beam_cdc_op', 'string'),
        ('beam_cdc_seq', 'integer'),
        ('beam_cdc_hex_seq', 'string'),
    ):
      sel_fields.setdefault(
          mcol,
          {
              'callable': f"def _keep(row):\n  return getattr(row, '{mcol}')\n",
              'output_type': mtype,
          })
    select_cfg = {**select_cfg, 'fields': sel_fields}

  needs_cli_provider = False
  if null_values:
    if src_ep.kind != 'file' or src_ep.format != 'csv':
      raise PipeSpecError('--null-values applies to CSV sources only')
    src_ep.config['null_values'] = [str(v) for v in null_values]
  if (not cdc and src_ep.kind == 'file' and src_ep.format in ('csv', 'json') and
      'fields' in src_ep.config):
    # Explicit column types override sampling: `fields=amount:float,id:int`.
    from . import cdc as cdc_mod
    try:
      parsed_types = dict(
          cdc_mod.parse_fields_shorthand(str(src_ep.config.pop('fields'))))
    except ValueError as e:
      raise PipeSpecError(f'--from-opt fields: {e}') from None
    bad = {c: t for c, t in parsed_types.items() if t == 'bytes'}
    if bad:
      raise PipeSpecError(
          f'--from-opt fields: bytes is not supported for {src_ep.format} '
          f'columns ({", ".join(bad)})')
    src_ep.config['types'] = parsed_types
  if src_ep.kind == 'file' and src_ep.format == 'json':
    try:
      if 'sample_rows' in src_ep.config:
        dlq.parse_sample_rows_opt(src_ep.config['sample_rows'])
      if 'on_unknown_fields' in src_ep.config:
        src_ep.config['on_unknown_fields'] = dlq.parse_unknown_fields_opt(
            src_ep.config['on_unknown_fields'])
    except ValueError as e:
      raise PipeSpecError(str(e)) from None
  src_type = src_ep.yaml_type
  src_cfg = dict(src_ep.config)
  if src_type == 'ReadFromJson':
    src_cfg.pop('sample_rows', None)
    src_cfg.pop('on_unknown_fields', None)
  if src_type == 'ReadFromCsv' and 'null_values' in src_cfg:
    # Exported YAML reads CSV with pandas: same markers, no pandas defaults.
    src_cfg['na_values'] = [''] + src_cfg.pop('null_values')
    src_cfg['keep_default_na'] = False
  if src_type in ('ReadFromCsv', 'ReadFromJson') and 'types' in src_cfg:
    # pandas equivalents of the explicit column types.
    src_cfg['dtype'] = {
        c: {
            'int': 'Int64', 'float': 'float64', 'bool': 'boolean',
            'str': 'string'
        }[t]
        for c, t in src_cfg.pop('types').items()
    }
  src_java = src_ep.is_java_xlang

  sink_type = dst_ep.yaml_type
  sink_cfg = dict(dst_ep.config)
  sink_java = dst_ep.is_java_xlang

  cdc_norm_cfg: dict[str, Any] | None = None
  if cdc:
    from . import cdc as cdc_mod
    resolved_cdc_fields: list[tuple[str, str]] = []
    cdc_fields_inferred_from_jsonl = False
    if raw_cdc_fields is not None:
      try:
        resolved_cdc_fields = cdc_mod.parse_fields_shorthand(
            str(raw_cdc_fields))
      except ValueError as e:
        raise PipeSpecError(str(e)) from None
    elif src_ep.kind == 'file' and src_ep.format == 'json':
      cdc_sample_rows = dlq.resolve_json_sample_rows(
          src_ep.config['path'], src_ep.config.get('sample_rows'))
      resolved_cdc_fields = cdc_mod.infer_cdc_jsonl_fields(
          src_ep.config['path'],
          cdc_format=cdc_format,
          op_col=cdc_op_col,
          seq_col=cdc_seq_col,
          primary_keys=pks,
          sample_rows=cdc_sample_rows)
      if resolved_cdc_fields:
        cdc_fields_inferred_from_jsonl = True
      elif prefer_python_lakehouse_and_cdc:
        resolved_cdc_fields = cdc_mod.infer_target_table_fields(
            dst_ep, default_project=default_project)
    elif src_ep.kind in ('pubsub', 'kafka') and 'schema' not in src_ep.config:
      if prefer_python_lakehouse_and_cdc:
        resolved_cdc_fields = cdc_mod.infer_target_table_fields(
            dst_ep, default_project=default_project)
        if not resolved_cdc_fields:
          raise PipeSpecError(
              f'streaming CDC source {src_ep.raw!r} has no static schema and '
              f'target {dst_ep.raw!r} does not exist yet; pass --from-opt '
              'fields="id:int,col:str,..." (or create the target table first)')
      else:
        resolved_cdc_fields = [(pk, 'str') for pk in pks]

    if resolved_cdc_fields and pks:
      known_fnames = {c for c, _ in resolved_cdc_fields}
      missing_pks = [pk for pk in pks if pk not in known_fnames]
      if missing_pks:
        raise PipeSpecError(
            f'--primary-key column(s) {missing_pks} not found in CDC fields '
            f'{sorted(known_fnames)}')

    if prefer_python_lakehouse_and_cdc:
      cdc_norm_cfg = {
          'primary_key': pks,
          'cdc_format': cdc_format,
      }
      if cdc_op_col:
        cdc_norm_cfg['op_col'] = cdc_op_col
      if cdc_seq_col:
        cdc_norm_cfg['seq_col'] = cdc_seq_col
      if resolved_cdc_fields:
        cdc_norm_cfg['fields_spec'] = [
            [cname, ckind] for cname, ckind in resolved_cdc_fields
        ]
      if cdc_fields_inferred_from_jsonl:
        cdc_norm_cfg['strict_inferred_fields'] = True
        if 'on_unknown_fields' in src_ep.config:
          cdc_norm_cfg['on_unknown_fields'] = src_ep.config['on_unknown_fields']

  if prefer_python_lakehouse_and_cdc:
    if src_ep.kind == 'delta':
      src_type = 'ReadFromDeltaCdcPy' if cdc else 'ReadFromDeltaPy'
      src_java = False
      needs_cli_provider = True
    elif src_ep.kind == 'iceberg':
      src_type = 'ReadFromIcebergCdcPy' if cdc else 'ReadFromIcebergPy'
      src_java = False
      needs_cli_provider = True
    elif cdc and src_ep.kind == 'file' and src_ep.format == 'json':
      src_type = 'ReadFromText'
      src_cfg = {'path': src_ep.config['path']}
      src_java = False
    elif src_ep.kind == 'file' and src_ep.format == 'json':
      # Beam's ReadFromJson turns nested objects into Python repr strings
      # (apache/beam#41098), so local runs use the CLI reader, which keeps them
      # as nested rows and fails on bad lines unless --dlq routes them.
      src_type = 'ReadJsonWithDlq'
      src_cfg = {
          k: v
          for k, v in src_ep.config.items()
          if k in ('path', 'types', 'sample_rows', 'on_unknown_fields')
      }
      src_java = False
      needs_cli_provider = True
    elif (src_ep.kind == 'file' and src_ep.format == 'csv' and
          set(src_ep.config) <= {'path', 'delimiter', 'null_values', 'types'}):
      # Use the CLI's CSV reader (the one --dlq uses) so a column is typed the
      # same with or without --dlq and on the fast local path. Beam's pandas
      # reader would turn zip codes like "00501" into the integer 501. Without
      # --dlq a bad record fails the run instead of being dropped.
      src_type = 'ReadCsvWithDlq'
      src_cfg = {
          k: v
          for k, v in src_ep.config.items() if k in ('path', 'delimiter', 'null_values', 'types')
      }
      src_java = False
      needs_cli_provider = True
    elif cdc and src_ep.kind == 'pubsub' and 'schema' not in src_ep.config:
      src_type = 'ReadFromPubSub'
      src_cfg = {**src_ep.config, 'format': 'STRING'}
      src_java = False

    if cdc_norm_cfg is not None:
      needs_cli_provider = True

    if dst_ep.kind == 'iceberg':
      sink_type = 'CliWriteToIceberg'
      sink_cfg = dict(dst_ep.config)
      sink_cfg['if_exists'] = 'append' if (cdc and if_exists == 'fail') else if_exists
      if is_unbounded:
        freq = None
        if 'triggering_frequency_seconds' in dst_ep.config:
          freq = int(dst_ep.config['triggering_frequency_seconds'])
        elif window_spec:
          w_size = window_spec.get('period') or window_spec.get('size')
          if w_size:
            from . import jobs
            try:
              freq = max(1, int(jobs.parse_duration_seconds(w_size)))
            except Exception:
              freq = 60
        sink_cfg.setdefault('triggering_frequency_seconds', freq or 60)
      if cdc:
        sink_cfg.setdefault('mode', 'merge-on-read')
        sink_cfg.setdefault('equality_columns', pks)
        sink_cfg.setdefault('change_type_column', 'beam_cdc_op')
        sink_cfg.setdefault('sequence_number_column', 'beam_cdc_seq')
      sink_java = False
      needs_cli_provider = True
    elif dst_ep.kind == 'bigquery' and cdc:
      sink_type = 'CliWriteToBigQueryCDC'
      sink_cfg = {
          'table': dst_ep.config['table'],
          'primary_key': pks,
          'write_disposition': dst_ep.config.get(
              'write_disposition', 'WRITE_APPEND'),
      }
      sink_java = False
      needs_cli_provider = True
    elif prefer_python_bigquery_sink and dst_ep.kind == 'bigquery':
      sink_type = 'CliWriteToBigQuery'
      sink_java = False
      needs_cli_provider = True
  else:
    # Portable Beam YAML export (`--emit-yaml` / `--dry-run`)
    if src_ep.kind == 'delta' and cdc:
      if 'start_version' not in src_cfg and 'start_timestamp' not in src_cfg:
        src_cfg['start_version'] = 0
      src_cfg.setdefault(
          'include_metadata_columns', ['_change_type', '_commit_version'])
    if dst_ep.kind == 'iceberg':
      if is_unbounded:
        freq = None
        if 'triggering_frequency_seconds' in dst_ep.config:
          freq = int(dst_ep.config['triggering_frequency_seconds'])
        elif window_spec:
          w_size = window_spec.get('period') or window_spec.get('size')
          if w_size:
            from . import jobs
            try:
              freq = max(1, int(jobs.parse_duration_seconds(w_size)))
            except Exception:
              freq = 60
        sink_cfg.setdefault('triggering_frequency_seconds', freq or 60)
      if cdc:
        sink_cfg.setdefault('mode', 'merge-on-read')
        sink_cfg.setdefault('equality_columns', pks)
        sink_cfg.setdefault(
            'change_type_column',
            cdc_op_col or (
                '_change_type' if src_ep.kind in ('delta', 'iceberg') else 'op'))
        if src_ep.kind == 'delta' and not cdc_op_col:
          sink_cfg.setdefault(
              'change_type_map', {
                  'insert': 'INSERT',
                  'update_preimage': 'UPDATE_BEFORE',
                  'update_postimage': 'UPDATE_AFTER',
                  'delete': 'DELETE',
              })
        sink_cfg.setdefault(
            'sequence_number_column',
            cdc_seq_col or (
                '_commit_version' if src_ep.kind == 'delta' else
                '_commit_snapshot_sequence_number'
                if src_ep.kind == 'iceberg' else 'ts_ms'))
    elif dst_ep.kind == 'bigquery' and cdc:
      sink_cfg['primary_key'] = pks
    elif prefer_python_bigquery_sink and dst_ep.kind == 'bigquery':
      sink_type = 'CliWriteToBigQuery'
      sink_java = False
      needs_cli_provider = True

  # Prepare optional second stream (`--join` + `--interval-join`).
  right_type: str | None = None
  right_cfg: dict[str, Any] = {}
  right_java = False
  if right_ep is not None:
    if null_values and right_ep.kind == 'file' and right_ep.format == 'csv':
      right_ep.config['null_values'] = [str(v) for v in null_values]
    if right_ep.kind == 'file' and right_ep.format in (
        'csv', 'json') and 'fields' in right_ep.config:
      from . import cdc as cdc_mod
      try:
        r_types = dict(
            cdc_mod.parse_fields_shorthand(str(right_ep.config.pop('fields'))))
      except ValueError as e:
        raise PipeSpecError(f'--from-opt fields: {e}') from None
      right_ep.config['types'] = r_types
    right_type = right_ep.yaml_type
    right_cfg = dict(right_ep.config)
    right_java = right_ep.is_java_xlang
    if right_type == 'ReadFromJson':
      right_cfg.pop('sample_rows', None)
      right_cfg.pop('on_unknown_fields', None)
    if right_type == 'ReadFromCsv' and 'null_values' in right_cfg:
      right_cfg['na_values'] = [''] + right_cfg.pop('null_values')
      right_cfg['keep_default_na'] = False
    if right_type in ('ReadFromCsv', 'ReadFromJson') and 'types' in right_cfg:
      right_cfg['dtype'] = {
          c: {
              'int': 'Int64',
              'float': 'float64',
              'bool': 'boolean',
              'str': 'string',
          }[t]
          for c, t in right_cfg.pop('types').items()
      }
    if prefer_python_lakehouse_and_cdc:
      if right_ep.kind == 'delta':
        right_type = 'ReadFromDeltaPy'
        right_java = False
        needs_cli_provider = True
      elif right_ep.kind == 'iceberg':
        right_type = 'ReadFromIcebergPy'
        right_java = False
        needs_cli_provider = True
      elif right_ep.kind == 'file' and right_ep.format == 'json':
        right_type = 'ReadJsonWithDlq'
        right_cfg = {
            k: v
            for k, v in right_ep.config.items()
            if k in ('path', 'types', 'sample_rows', 'on_unknown_fields')
        }
        right_java = False
        needs_cli_provider = True
      elif (right_ep.kind == 'file' and right_ep.format == 'csv' and set(
          right_ep.config) <= {'path', 'delimiter', 'null_values', 'types'}):
        right_type = 'ReadCsvWithDlq'
        right_cfg = {
            k: v
            for k, v in right_ep.config.items()
            if k in ('path', 'delimiter', 'null_values', 'types')
        }
        right_java = False
        needs_cli_provider = True

  # Plain files can't apply upserts or deletes like Iceberg/BigQuery CDC sinks,
  # so --cdc into a file writes the latest live row per key.
  materialize_cdc = cdc_norm_cfg is not None and dst_ep.kind == 'file'
  # Parquet/Avro readers yield dicts for struct columns (apache/beam#41100);
  # expressions like address.city need rows, so convert them locally.
  rowify_nested = bool(
      prefer_python_lakehouse_and_cdc and src_ep.kind == 'file' and
      src_ep.format in ('parquet', 'avro') and (
          where_cfg or pre_select_cfg or post_where_cfg or enrich_yaml_cfgs or
          infer_yaml_cfgs or select_cfg or two_stream_cfg is not None or
          cli_window_yaml_cfg is not None))
  right_rowify_nested = bool(
      prefer_python_lakehouse_and_cdc and right_ep is not None and
      right_ep.kind == 'file' and right_ep.format in ('parquet', 'avro'))
  if (rowify_nested or right_rowify_nested or two_stream_cfg is not None or
      dedupe_yaml_cfg is not None or bool(enrich_yaml_cfgs) or
      bool(infer_yaml_cfgs) or cli_window_yaml_cfg is not None):
    needs_cli_provider = True

  # Pure Java-to-Java fast path: skip Python counter stages unless explicitly
  # requested (`include_counters=True` when `--assert-counts` or Python stage).
  pure_java_pipe = (
      src_java and sink_java and cdc_norm_cfg is None and not where_cfg and
      not pre_select_cfg and not post_where_cfg and not enrich_yaml_cfgs and
      not infer_yaml_cfgs and not select_cfg and not dlq_ep and
      not late_dlq_ep and two_stream_cfg is None and dedupe_yaml_cfg is None and
      cli_window_yaml_cfg is None)
  inject_counters = bool(include_counters) and not pure_java_pipe
  if inject_counters:
    needs_cli_provider = True

  # Simple linear chain when neither `--dlq` nor a 2-stream join is active!
  if dlq_ep is None and late_dlq_ep is None and two_stream_cfg is None:
    chain_steps: list[dict[str, Any]] = [
        {
            'type': src_type,
            'config': src_cfg,
        }
    ]
    if rowify_nested:
      # Must directly follow the source: a schema-typed step in between would
      # encode the nested dicts with RowCoder and fail.
      chain_steps.append({'type': 'CliRowifyNested', 'config': {}})
    if inject_counters and src_type not in ('ReadJsonWithDlq',
                                            'ReadCsvWithDlq'):
      # The CLI readers increment rows_read themselves.
      chain_steps.append({
          'type': 'CliCountRows',
          'config': {
              'counter': 'rows_read'
          },
      })
    if clean_timestamp_col:
      if cli_window_yaml_cfg is not None:
        chain_steps.append({
            'type': 'CliAssignTimestamps',
            'config': {
                'timestamp_col': clean_timestamp_col
            },
        })
      else:
        chain_steps.append({
            'type': 'AssignTimestamps',
            'config': {
                'timestamp': clean_timestamp_col
            },
        })
    if cdc_norm_cfg is not None:
      chain_steps.append({
          'type': 'CliNormalizeCdc',
          'config': dict(cdc_norm_cfg),
      })
      if materialize_cdc:
        chain_steps.append({
            'type': 'CliMaterializeCdc',
            'config': _materialize_cdc_config(pks, cdc_seq_col),
        })
    if dedupe_yaml_cfg is not None:
      chain_steps.append({
          'type': 'CliDeduplicate',
          'config': dict(dedupe_yaml_cfg),
      })
    if where_cfg is not None:
      if inject_counters:
        chain_steps.append({
            'type': 'CliCountRows',
            'config': {
                'counter': 'pre_filter_rows'
            },
        })
      chain_steps.append({'type': 'Filter', 'config': where_cfg})
      if inject_counters:
        chain_steps.append({
            'type': 'CliCountRows',
            'config': {
                'counter': 'post_filter_rows'
            },
        })
    if pre_select_cfg is not None:
      chain_steps.append({'type': 'MapToFields', 'config': pre_select_cfg})
    for e_cfg in enrich_yaml_cfgs:
      chain_steps.append({
          'type': 'CliEnrich',
          'config': dict(e_cfg),
      })
    for i_cfg in infer_yaml_cfgs:
      chain_steps.append({
          'type': 'CliInfer',
          'config': dict(i_cfg),
      })
    if post_where_cfg is not None:
      if inject_counters:
        chain_steps.append({
            'type': 'CliCountRows',
            'config': {
                'counter': 'pre_filter_rows'
            },
        })
      chain_steps.append({'type': 'Filter', 'config': post_where_cfg})
      if inject_counters:
        chain_steps.append({
            'type': 'CliCountRows',
            'config': {
                'counter': 'post_filter_rows'
            },
        })
    if window_spec is not None:
      if cli_window_yaml_cfg is not None:
        chain_steps.append({
            'type': 'CliWindowInto',
            'config': dict(cli_window_yaml_cfg),
        })
      else:
        chain_steps.append({
            'type': 'WindowInto',
            'config': {
                'windowing': window_spec
            },
        })
      if window_aggs:
        comb_cfg: dict[str, Any] = {
            'group_by': window_aggs.get('group_by', []),
            'combine': window_aggs.get('combine', {}),
        }
        if cli_window_yaml_cfg is not None:
          comb_cfg['language'] = 'python'
        chain_steps.append({
            'type': 'Combine',
            'config': comb_cfg,
        })
      if emit_pane_info:
        chain_steps.append({'type': 'CliExtractPaneInfo', 'config': {}})
    if select_cfg is not None:
      chain_steps.append({'type': 'MapToFields', 'config': select_cfg})
    if parsed_expect_rules:
      needs_cli_provider = True
      chain_steps.append({
          'type': 'CliExpect',
          'config': {
              'rules': [
                  {'name': r.name, 'expression': r.expression, 'on_violation': r.on_violation}
                  for r in parsed_expect_rules
              ],
          },
      })
    if inject_counters:
      chain_steps.append({
          'type': 'CliCountRows',
          'config': {
              'counter': 'pre_sink_rows'
          },
      })
    chain_steps.append({
        'type': sink_type,
        'config': sink_cfg,
    })

    spec: dict[str, Any] = {
        'pipeline': {
            'type': 'chain',
            'transforms': chain_steps,
        }
    }
    if is_unbounded:
      spec['options'] = {'streaming': True}
    if needs_cli_provider:
      spec['providers'] = [dlq.cli_yaml_provider_spec()]
    return PipeSpec(
        spec=spec,
        output_prefix=dst_ep.output_prefix,
        source_endpoint=src_ep,
        sink_endpoint=dst_ep,
        dlq_endpoint=None,
        requires_java_xlang=src_java or sink_java,
        is_unbounded=is_unbounded)

  # Non-linear DAG when `--dlq` or `--interval-join` IS active!
  has_dlq = dlq_ep is not None
  needs_cli_provider = True
  dag_transforms: list[dict[str, Any]] = []
  dlq_norm_refs: list[str] = []

  def _norm_dlq(name: str, input_ref: str, step_label: str) -> dict[str, Any]:
    return dlq.build_dlq_normalizer_transform(
        name, input_ref, step_label, redact_fields=parsed_dlq_redact)

  def _attach_source_node(
      node_name: str,
      ep: uris.ParsedEndpoint,
      s_type: str,
      s_cfg: dict[str, Any],
      norm_dlq_name: str,
  ) -> tuple[str, bool]:
    counts_internally = False
    cfg_copy = dict(s_cfg)
    if has_dlq:
      if ep.kind == 'file' and ep.format == 'csv':
        s_type = 'ReadCsvWithDlq'
        cfg_copy = {
            k: v
            for k, v in ep.config.items()
            if k in ('path', 'delimiter', 'null_values', 'types')
        }
        cfg_copy['error_handling'] = {'output': 'errors'}
        counts_internally = True
      elif ep.kind == 'file' and ep.format == 'json' and not cdc:
        s_type = 'ReadJsonWithDlq'
        cfg_copy = {
            **{
                k: v
                for k, v in ep.config.items() if k in (
                    'path', 'types', 'sample_rows', 'on_unknown_fields')
            },
            'error_handling': {
                'output': 'errors'
            },
        }
        counts_internally = True
      elif (s_type in dlq.TRANSFORMS_SUPPORTING_ERROR_HANDLING and
            not (cdc and s_type == 'ReadFromPubSub' and
                 cfg_copy.get('format') == 'STRING')):
        cfg_copy['error_handling'] = {'output': 'errors'}
    elif s_type in ('ReadCsvWithDlq', 'ReadJsonWithDlq'):
      counts_internally = True

    dag_transforms.append({
        'name': node_name, 'type': s_type, 'config': cfg_copy
    })
    if 'error_handling' in cfg_copy:
      out_ref = f'{node_name}.good' if counts_internally else node_name
      dag_transforms.append(
          _norm_dlq(norm_dlq_name, f'{node_name}.errors', s_type))
      dlq_norm_refs.append(norm_dlq_name)
    else:
      out_ref = node_name
    return out_ref, counts_internally

  if two_stream_cfg is not None and right_ep is not None and right_type is not None:
    left_ref, left_counted = _attach_source_node(
        'ReadLeft', src_ep, src_type, src_cfg, 'NormalizeLeftSourceDlq')
    if rowify_nested:
      dag_transforms.append({
          'name': 'RowifyNestedLeft',
          'type': 'CliRowifyNested',
          'input': left_ref,
          'config': {},
      })
      left_ref = 'RowifyNestedLeft'
    if inject_counters and not left_counted:
      dag_transforms.append({
          'name': 'CountReadLeft',
          'type': 'CliCountRows',
          'input': left_ref,
          'config': {
              'counter': 'rows_read'
          },
      })
      left_ref = 'CountReadLeft'

    right_ref, right_counted = _attach_source_node(
        'ReadRight', right_ep, right_type, right_cfg, 'NormalizeRightSourceDlq')
    if right_rowify_nested:
      dag_transforms.append({
          'name': 'RowifyNestedRight',
          'type': 'CliRowifyNested',
          'input': right_ref,
          'config': {},
      })
      right_ref = 'RowifyNestedRight'
    if inject_counters and not right_counted:
      dag_transforms.append({
          'name': 'CountReadRight',
          'type': 'CliCountRows',
          'input': right_ref,
          'config': {
              'counter': 'rows_read'
          },
      })
      right_ref = 'CountReadRight'

    is_asof = asof_join_cfg is not None
    parsed_2s = aj_parsed if is_asof else ij_parsed
    assert parsed_2s is not None
    l_ts_col = parsed_2s.left_timestamp
    r_ts_col = parsed_2s.right_timestamp

    if l_ts_col:
      ts_l_cfg: dict[str, Any] = {'timestamp_col': l_ts_col}
      if has_dlq:
        ts_l_cfg['error_handling'] = {'output': 'errors'}
      dag_transforms.append({
          'name': 'AssignTimestampsLeft',
          'type': 'CliAssignTimestamps',
          'input': left_ref,
          'config': ts_l_cfg,
      })
      if has_dlq:
        ij_l_in = 'AssignTimestampsLeft.good'
        dag_transforms.append(
            _norm_dlq(
                'NormalizeAssignTsLeftDlq',
                'AssignTimestampsLeft.errors',
                'CliAssignTimestamps'))
        dlq_norm_refs.append('NormalizeAssignTsLeftDlq')
      else:
        ij_l_in = 'AssignTimestampsLeft'
    else:
      ij_l_in = left_ref

    if r_ts_col:
      ts_r_cfg: dict[str, Any] = {'timestamp_col': r_ts_col}
      if has_dlq:
        ts_r_cfg['error_handling'] = {'output': 'errors'}
      dag_transforms.append({
          'name': 'AssignTimestampsRight',
          'type': 'CliAssignTimestamps',
          'input': right_ref,
          'config': ts_r_cfg,
      })
      if has_dlq:
        ij_r_in = 'AssignTimestampsRight.good'
        dag_transforms.append(
            _norm_dlq(
                'NormalizeAssignTsRightDlq',
                'AssignTimestampsRight.errors',
                'CliAssignTimestamps'))
        dlq_norm_refs.append('NormalizeAssignTsRightDlq')
      else:
        ij_r_in = 'AssignTimestampsRight'
    else:
      ij_r_in = right_ref

    join_node_name = 'AsofJoin' if is_asof else 'IntervalJoin'
    join_node_type = 'CliAsofJoin' if is_asof else 'CliIntervalJoin'
    join_norm_name = (
        'NormalizeAsofJoinDlq' if is_asof else 'NormalizeIntervalJoinDlq')
    ij_node_cfg = dict(two_stream_cfg)
    if has_dlq:
      ij_node_cfg['error_handling'] = {'output': 'errors'}
    dag_transforms.append({
        'name': join_node_name,
        'type': join_node_type,
        'input': {
            'left': ij_l_in,
            'right': ij_r_in,
        },
        'config': ij_node_cfg,
    })
    if has_dlq:
      current_ref = f'{join_node_name}.good'
      dag_transforms.append(
          _norm_dlq(join_norm_name, f'{join_node_name}.errors', join_node_type))
      dlq_norm_refs.append(join_norm_name)
    else:
      current_ref = join_node_name

    if clean_timestamp_col:
      dag_transforms.append({
          'name': 'AssignTimestamps',
          'type': 'AssignTimestamps',
          'input': current_ref,
          'config': {
              'timestamp': clean_timestamp_col
          },
      })
      current_ref = 'AssignTimestamps'
  else:
    current_ref, source_counts_internally = _attach_source_node(
        'ReadSource', src_ep, src_type, src_cfg, 'NormalizeSourceDlq')
    if rowify_nested:
      # Must directly follow the source (see the chain form above).
      dag_transforms.append({
          'name': 'RowifyNested',
          'type': 'CliRowifyNested',
          'input': current_ref,
          'config': {},
      })
      current_ref = 'RowifyNested'

    if inject_counters and not source_counts_internally:
      dag_transforms.append({
          'name': 'CountRead',
          'type': 'CliCountRows',
          'input': current_ref,
          'config': {
              'counter': 'rows_read'
          },
      })
      current_ref = 'CountRead'

    if clean_timestamp_col:
      if cli_window_yaml_cfg is not None:
        ts_dag_cfg: dict[str, Any] = {'timestamp_col': clean_timestamp_col}
        if has_dlq:
          ts_dag_cfg['error_handling'] = {'output': 'errors'}
        dag_transforms.append({
            'name': 'AssignTimestamps',
            'type': 'CliAssignTimestamps',
            'input': current_ref,
            'config': ts_dag_cfg,
        })
        if has_dlq:
          dag_transforms.append(
              _norm_dlq(
                  'NormalizeAssignTsDlq',
                  'AssignTimestamps.errors',
                  'CliAssignTimestamps'))
          dlq_norm_refs.append('NormalizeAssignTsDlq')
          current_ref = 'AssignTimestamps.good'
        else:
          current_ref = 'AssignTimestamps'
      else:
        dag_transforms.append({
            'name': 'AssignTimestamps',
            'type': 'AssignTimestamps',
            'input': current_ref,
            'config': {
                'timestamp': clean_timestamp_col
            },
        })
        current_ref = 'AssignTimestamps'

  if cdc_norm_cfg is not None:
    norm_cdc = dict(cdc_norm_cfg)
    if has_dlq:
      norm_cdc['error_handling'] = {'output': 'errors'}
    dag_transforms.append({
        'name': 'NormalizeCdc',
        'type': 'CliNormalizeCdc',
        'input': current_ref,
        'config': norm_cdc,
    })
    if has_dlq:
      dag_transforms.append(
          _norm_dlq('NormalizeCdcDlq', 'NormalizeCdc.errors', 'NormalizeCdc'))
      dlq_norm_refs.append('NormalizeCdcDlq')
      current_ref = 'NormalizeCdc.good'
    else:
      current_ref = 'NormalizeCdc'
    if materialize_cdc:
      dag_transforms.append({
          'name': 'MaterializeCdc',
          'type': 'CliMaterializeCdc',
          'input': current_ref,
          'config': _materialize_cdc_config(pks, cdc_seq_col),
      })
      current_ref = 'MaterializeCdc'

  if dedupe_yaml_cfg is not None:
    d_cfg = dict(dedupe_yaml_cfg)
    if has_dlq:
      d_cfg['error_handling'] = {'output': 'errors'}
    dag_transforms.append({
        'name': 'Deduplicate',
        'type': 'CliDeduplicate',
        'input': current_ref,
        'config': d_cfg,
    })
    if has_dlq:
      dag_transforms.append(
          _norm_dlq(
              'NormalizeDedupeDlq', 'Deduplicate.errors', 'CliDeduplicate'))
      dlq_norm_refs.append('NormalizeDedupeDlq')
      current_ref = 'Deduplicate.good'
    else:
      current_ref = 'Deduplicate'

  if where_cfg is not None:
    if inject_counters:
      dag_transforms.append({
          'name': 'CountPreFilter',
          'type': 'CliCountRows',
          'input': current_ref,
          'config': {
              'counter': 'pre_filter_rows'
          },
      })
      current_ref = 'CountPreFilter'
    w_cfg = dict(where_cfg)
    if has_dlq:
      w_cfg['error_handling'] = {'output': 'errors'}
    dag_transforms.append({
        'name': 'FilterWhere',
        'type': 'Filter',
        'input': current_ref,
        'config': w_cfg,
    })
    if has_dlq:
      dag_transforms.append(
          _norm_dlq('NormalizeFilterDlq', 'FilterWhere.errors', 'Filter'))
      filter_dlq_ref = 'NormalizeFilterDlq'
      if inject_counters:
        dag_transforms.append({
            'name': 'CountFilterDlq',
            'type': 'CliCountRows',
            'input': filter_dlq_ref,
            'config': {
                'counter': 'filter_dlq_rows'
            },
        })
        filter_dlq_ref = 'CountFilterDlq'
      dlq_norm_refs.append(filter_dlq_ref)
    current_ref = 'FilterWhere'
    if inject_counters:
      dag_transforms.append({
          'name': 'CountPostFilter',
          'type': 'CliCountRows',
          'input': current_ref,
          'config': {
              'counter': 'post_filter_rows'
          },
      })
      current_ref = 'CountPostFilter'

  if pre_select_cfg is not None:
    ps_cfg = dict(pre_select_cfg)
    if has_dlq:
      ps_cfg['error_handling'] = {'output': 'errors'}
    dag_transforms.append({
        'name': 'PreSelectFields',
        'type': 'MapToFields',
        'input': current_ref,
        'config': ps_cfg,
    })
    if has_dlq:
      dag_transforms.append(
          _norm_dlq(
              'NormalizePreSelectDlq', 'PreSelectFields.errors', 'MapToFields'))
      dlq_norm_refs.append('NormalizePreSelectDlq')
    current_ref = 'PreSelectFields'

  for e_idx, e_yaml_cfg in enumerate(enrich_yaml_cfgs, start=1):
    e_name = 'Enrich' if len(enrich_yaml_cfgs) == 1 else f'Enrich_{e_idx}'
    e_norm_name = (
        'NormalizeEnrichDlq'
        if len(enrich_yaml_cfgs) == 1 else f'NormalizeEnrichDlq_{e_idx}')
    e_cfg_copy = dict(e_yaml_cfg)
    if has_dlq:
      e_cfg_copy['error_handling'] = {'output': 'errors'}
    dag_transforms.append({
        'name': e_name,
        'type': 'CliEnrich',
        'input': current_ref,
        'config': e_cfg_copy,
    })
    if has_dlq:
      dag_transforms.append(
          _norm_dlq(e_norm_name, f'{e_name}.errors', 'CliEnrich'))
      dlq_norm_refs.append(e_norm_name)
      current_ref = f'{e_name}.good'
    else:
      current_ref = e_name

  for i_idx, i_yaml_cfg in enumerate(infer_yaml_cfgs, start=1):
    i_name = 'Infer' if len(infer_yaml_cfgs) == 1 else f'Infer_{i_idx}'
    i_norm_name = (
        'NormalizeInferDlq'
        if len(infer_yaml_cfgs) == 1 else f'NormalizeInferDlq_{i_idx}')
    i_cfg_copy = dict(i_yaml_cfg)
    if has_dlq:
      i_cfg_copy['error_handling'] = {'output': 'errors'}
    dag_transforms.append({
        'name': i_name,
        'type': 'CliInfer',
        'input': current_ref,
        'config': i_cfg_copy,
    })
    if has_dlq:
      dag_transforms.append(
          _norm_dlq(i_norm_name, f'{i_name}.errors', 'CliInfer'))
      dlq_norm_refs.append(i_norm_name)
      current_ref = f'{i_name}.good'
    else:
      current_ref = i_name

  if post_where_cfg is not None:
    if inject_counters:
      dag_transforms.append({
          'name': 'CountPrePostFilter',
          'type': 'CliCountRows',
          'input': current_ref,
          'config': {
              'counter': 'pre_filter_rows'
          },
      })
      current_ref = 'CountPrePostFilter'
    pw_cfg = dict(post_where_cfg)
    if has_dlq:
      pw_cfg['error_handling'] = {'output': 'errors'}
    dag_transforms.append({
        'name': 'FilterPostWhere',
        'type': 'Filter',
        'input': current_ref,
        'config': pw_cfg,
    })
    if has_dlq:
      dag_transforms.append(
          _norm_dlq(
              'NormalizePostFilterDlq', 'FilterPostWhere.errors', 'Filter'))
      post_f_dlq_ref = 'NormalizePostFilterDlq'
      if inject_counters:
        dag_transforms.append({
            'name': 'CountPostFilterDlq',
            'type': 'CliCountRows',
            'input': post_f_dlq_ref,
            'config': {
                'counter': 'filter_dlq_rows'
            },
        })
        post_f_dlq_ref = 'CountPostFilterDlq'
      dlq_norm_refs.append(post_f_dlq_ref)
    current_ref = 'FilterPostWhere'
    if inject_counters:
      dag_transforms.append({
          'name': 'CountPostPostFilter',
          'type': 'CliCountRows',
          'input': current_ref,
          'config': {
              'counter': 'post_filter_rows'
          },
      })
      current_ref = 'CountPostPostFilter'

  late_dlq_norm_ref: str | None = None
  if window_spec is not None:
    if cli_window_yaml_cfg is not None:
      w_cfg_copy = dict(cli_window_yaml_cfg)
      want_win_dlq = bool(has_dlq or late_dlq_ep is not None)
      if want_win_dlq:
        w_cfg_copy['error_handling'] = {'output': 'errors'}
      dag_transforms.append({
          'name': 'WindowInto',
          'type': 'CliWindowInto',
          'input': current_ref,
          'config': w_cfg_copy,
      })
      if want_win_dlq:
        dag_transforms.append(
            _norm_dlq(
                'NormalizeWindowDlq', 'WindowInto.errors', 'CliWindowInto'))
        if late_dlq_ep is not None:
          late_dlq_norm_ref = 'NormalizeWindowDlq'
        else:
          dlq_norm_refs.append('NormalizeWindowDlq')
        current_ref = 'WindowInto.good'
      else:
        current_ref = 'WindowInto'
    else:
      dag_transforms.append({
          'name': 'WindowInto',
          'type': 'WindowInto',
          'input': current_ref,
          'config': {
              'windowing': window_spec
          },
      })
      current_ref = 'WindowInto'
    if window_aggs:
      comb_dag_cfg: dict[str, Any] = {
          'group_by': window_aggs.get('group_by', []),
          'combine': window_aggs.get('combine', {}),
      }
      if cli_window_yaml_cfg is not None:
        comb_dag_cfg['language'] = 'python'
      dag_transforms.append({
          'name': 'WindowCombine',
          'type': 'Combine',
          'input': current_ref,
          'config': comb_dag_cfg,
      })
      current_ref = 'WindowCombine'
    if emit_pane_info:
      dag_transforms.append({
          'name': 'ExtractPaneInfo',
          'type': 'CliExtractPaneInfo',
          'input': current_ref,
          'config': {},
      })
      current_ref = 'ExtractPaneInfo'

  if select_cfg is not None:
    s_cfg = dict(select_cfg)
    if has_dlq:
      s_cfg['error_handling'] = {'output': 'errors'}
    dag_transforms.append({
        'name': 'SelectFields',
        'type': 'MapToFields',
        'input': current_ref,
        'config': s_cfg,
    })
    if has_dlq:
      dag_transforms.append(
          _norm_dlq('NormalizeSelectDlq', 'SelectFields.errors', 'MapToFields'))
      dlq_norm_refs.append('NormalizeSelectDlq')
    current_ref = 'SelectFields'

  if parsed_expect_rules:
    needs_cli_provider = True
    exp_cfg: dict[str, Any] = {
        'rules': [{
            'name': r.name,
            'expression': r.expression,
            'on_violation': r.on_violation
        } for r in parsed_expect_rules],
    }
    if has_dlq:
      exp_cfg['error_handling'] = {'output': 'errors'}
    dag_transforms.append({
        'name': 'ValidateExpect',
        'type': 'CliExpect',
        'input': current_ref,
        'config': exp_cfg,
    })
    if has_dlq:
      dag_transforms.append(
          _norm_dlq(
              'NormalizeExpectDlq', 'ValidateExpect.errors', 'CliExpect'))
      dlq_norm_refs.append('NormalizeExpectDlq')
      current_ref = 'ValidateExpect.good'
    else:
      current_ref = 'ValidateExpect'

  if inject_counters:
    dag_transforms.append({
        'name': 'CountPreSink',
        'type': 'CliCountRows',
        'input': current_ref,
        'config': {
            'counter': 'pre_sink_rows'
        },
    })
    current_ref = 'CountPreSink'

  dst_cfg = dict(sink_cfg)
  if has_dlq and (sink_type in dlq.TRANSFORMS_SUPPORTING_ERROR_HANDLING or
                  sink_type in ('CliWriteToBigQuery', 'CliWriteToBigQueryCDC')):
    dst_cfg['error_handling'] = {'output': 'errors'}
    dag_transforms.append({
        'name': 'WriteSink',
        'type': sink_type,
        'input': current_ref,
        'config': dst_cfg,
    })
    dag_transforms.append(
        _norm_dlq('NormalizeSinkDlq', 'WriteSink.errors', dst_ep.yaml_type))
    dlq_norm_refs.append('NormalizeSinkDlq')
  else:
    dag_transforms.append({
        'name': 'WriteSink',
        'type': sink_type,
        'input': current_ref,
        'config': dst_cfg,
    })

  if has_dlq and dlq_norm_refs:
    assert dlq_ep is not None
    if len(dlq_norm_refs) == 1:
      dlq_input_ref = dlq_norm_refs[0]
    else:
      dag_transforms.append({
          'name': 'MergeDlq',
          'type': 'Flatten',
          'input': dlq_norm_refs,
      })
      dlq_input_ref = 'MergeDlq'

    if inject_counters:
      dag_transforms.append({
          'name': 'CountDlq',
          'type': 'CliCountRows',
          'input': dlq_input_ref,
          'config': {
              'counter': 'dlq_rows'
          },
      })
      dlq_input_ref = 'CountDlq'

    dlq_sink_type = (
        'CliWriteToBigQuery' if
        (prefer_python_bigquery_sink and dlq_ep.kind == 'bigquery') else
        dlq_ep.yaml_type)
    dag_transforms.append({
        'name': 'WriteDlq',
        'type': dlq_sink_type,
        'input': dlq_input_ref,
        'config': dict(dlq_ep.config),
    })

  if late_dlq_ep is not None and late_dlq_norm_ref is not None:
    late_input_ref = late_dlq_norm_ref
    if inject_counters:
      dag_transforms.append({
          'name': 'CountLateDlq',
          'type': 'CliCountRows',
          'input': late_input_ref,
          'config': {
              'counter': 'dlq_rows'
          },
      })
      late_input_ref = 'CountLateDlq'
    late_sink_type = (
        'CliWriteToBigQuery' if
        (prefer_python_bigquery_sink and
         late_dlq_ep.kind == 'bigquery') else late_dlq_ep.yaml_type)
    dag_transforms.append({
        'name': 'WriteLateDlq',
        'type': late_sink_type,
        'input': late_input_ref,
        'config': dict(late_dlq_ep.config),
    })

  spec = {
      'pipeline': {
          'transforms': dag_transforms,
      },
      'providers': [dlq.cli_yaml_provider_spec()],
  }
  if is_unbounded:
    spec['options'] = {'streaming': True}
  return PipeSpec(
      spec=spec,
      output_prefix=dst_ep.output_prefix,
      source_endpoint=src_ep,
      sink_endpoint=dst_ep,
      dlq_endpoint=dlq_ep,
      requires_java_xlang=(
          src_java or right_java or sink_java or
          (dlq_ep is not None and dlq_ep.is_java_xlang) or
          (late_dlq_ep is not None and late_dlq_ep.is_java_xlang)),
      is_unbounded=is_unbounded,
      late_dlq_endpoint=late_dlq_ep)


def _redact_spec_secrets(
    obj: Any, parent_key: str = '', *, _skip_key_match: bool = False) -> Any:
  from . import output
  if isinstance(obj, dict):
    skip_keys = (
        _skip_key_match or parent_key in ('fields', 'types', 'rename_fields') or
        obj.get('type') in ('MapToFields', 'Filter'))
    return {
        k: (
            '<REDACTED_SECRET>'
            if not skip_keys and uris._SECRET_KEY_RE.search(str(k)) and
            not isinstance(v, (dict, list)) else _redact_spec_secrets(
                v, str(k), _skip_key_match=skip_keys))
        for k, v in obj.items()
    }
  if isinstance(obj, list):
    return [
        _redact_spec_secrets(item, parent_key, _skip_key_match=_skip_key_match)
        for item in obj
    ]
  if isinstance(obj, str):
    return output.redact_inline_secrets(obj, placeholder='<REDACTED_SECRET>')
  return obj


def _redact_argv_tokens(argv: list[str]) -> list[str]:
  """Redacts credentials inside CLI argv tokens before emitting YAML headers."""
  from . import output
  redacted: list[str] = []
  redact_next = False
  for tok in argv:
    s = str(tok)
    if redact_next:
      redacted.append('<REDACTED_SECRET>')
      redact_next = False
      continue
    if s.startswith('-'):
      flag_part, sep, val_part = s.partition('=')
      flag_key = flag_part.lstrip('-')
      if sep:
        if uris._SECRET_KEY_RE.search(flag_key):
          redacted.append(f'{flag_part}=<REDACTED_SECRET>')
        else:
          redacted.append(
              f'{flag_part}='
              f'{output.redact_inline_secrets(val_part, "<REDACTED_SECRET>")}')
        continue
      if uris._SECRET_KEY_RE.search(flag_key):
        redact_next = True
      redacted.append(s)
      continue
    redacted.append(output.redact_inline_secrets(s, '<REDACTED_SECRET>'))
  return redacted


def redact_yaml_text(yaml_text: str) -> str:
  """Redacts secrets in serialized Beam YAML text before job display_data upload."""
  from . import output
  try:
    import yaml
    parsed = output.safe_yaml_load(yaml_text)
    if isinstance(parsed, dict):
      dumper = getattr(yaml, 'CSafeDumper', yaml.SafeDumper)
      return yaml.dump(
          _redact_spec_secrets(parsed),
          Dumper=dumper,
          sort_keys=False,
          width=100000)
  except Exception:  # pylint: disable=broad-except
    pass
  return output.redact_inline_secrets(
      yaml_text, placeholder='<REDACTED_SECRET>')


def to_yaml(spec: dict[str, Any], argv: list[str]) -> str:
  """Renders a pipeline as a Beam YAML file to save and run with `beam run`."""
  import yaml
  safe_argv = _redact_argv_tokens(argv)
  header = '# Generated by: beam pipe ' + shlex.join(safe_argv).replace(
      '\n', ' ')
  dumper = getattr(yaml, 'CSafeDumper', yaml.SafeDumper)
  return header + '\n' + yaml.dump(
      _redact_spec_secrets(spec), Dumper=dumper, sort_keys=False, width=100000)
