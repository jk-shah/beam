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
    if_exists: str = 'fail',
    from_opts: Iterable[str] | None = None,
    to_opts: Iterable[str] | None = None,
    from_opt_file: str | None = None,
    to_opt_file: str | None = None,
    default_project: str | None = None,
    known_source_columns: set[str] | None = None,
    include_counters: bool = False,
    prefer_python_bigquery_sink: bool = False,
) -> PipeSpec:
  """Returns the `PipeSpec` for `beam pipe`."""
  if if_exists not in ('fail', 'append', 'replace'):
    raise PipeSpecError(
        f'invalid --if-exists mode {if_exists!r}; use fail, append, or replace')

  try:
    parsed_from_opts = uris.parse_kv_options(
        from_opts, from_opt_file, '--from-opt')
    parsed_to_opts = uris.parse_kv_options(to_opts, to_opt_file, '--to-opt')
    src_ep = uris.parse_endpoint(
        source,
        is_read=True,
        explicit_format=from_format,
        options=parsed_from_opts,
        default_project=default_project,
        allow_sniff=True)
    dst_ep = uris.parse_endpoint(
        sink,
        is_read=False,
        explicit_format=to_format,
        options=parsed_to_opts,
        if_exists=if_exists,
        default_project=default_project)
    dlq_ep = (
        uris.parse_endpoint(
            dlq_sink,
            is_read=False,
            explicit_format=dlq_format or (
                None if uris.extension_of(dlq_sink) or '://' in dlq_sink and
                not dlq_sink.split('://', 1)[0].lower() in uris.FILE_SCHEMES
                else 'json'),
            if_exists='append' if if_exists == 'append' else 'fail',
            default_project=default_project) if dlq_sink else None)
  except uris.UriError as e:
    raise PipeSpecError(str(e)) from None

  if dst_ep.kind == 'file' and if_exists == 'append':
    raise PipeSpecError(
        '--if-exists=append is only supported for table/stream sinks '
        '(such as bigquery:// or postgres://); file sinks write immutable '
        'shards (use a new prefix or --if-exists=replace)')

  try:
    where_cfg = (
        expressions.compile_where(where, known_columns=known_source_columns)[0]
        if where else None)
    # After `--where`, `--select` still sees the same source columns.
    select_cfg = (
        expressions.compile_select(
            select, known_columns=known_source_columns)[0]
        if select else None)
  except expressions.ExpressionError as e:
    raise PipeSpecError(str(e)) from None

  needs_cli_provider = False
  sink_type = dst_ep.yaml_type
  sink_java = dst_ep.is_java_xlang
  if prefer_python_bigquery_sink and dst_ep.kind == 'bigquery':
    sink_type = 'CliWriteToBigQuery'
    sink_java = False
    needs_cli_provider = True

  # Pure Java-to-Java fast path: skip Python counter stages unless explicitly
  # requested (`include_counters=True` when `--assert-counts` or Python stage).
  pure_java_pipe = (
      src_ep.is_java_xlang and sink_java and not where_cfg and
      not select_cfg and not dlq_ep)
  inject_counters = bool(include_counters) and not pure_java_pipe
  if inject_counters:
    needs_cli_provider = True

  # Simple linear chain when `--dlq` is NOT active -> clean, idiomatic YAML!
  if dlq_ep is None:
    chain_steps: list[dict[str, Any]] = [
        {
            'type': src_ep.yaml_type,
            'config': dict(src_ep.config),
        }
    ]
    if inject_counters:
      chain_steps.append({
          'type': 'CliCountRows',
          'config': {
              'counter': 'rows_read'
          },
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
    if select_cfg is not None:
      chain_steps.append({'type': 'MapToFields', 'config': select_cfg})
    if inject_counters:
      chain_steps.append({
          'type': 'CliCountRows',
          'config': {
              'counter': 'pre_sink_rows'
          },
      })
    chain_steps.append({
        'type': sink_type,
        'config': dict(dst_ep.config),
    })

    spec: dict[str, Any] = {
        'pipeline': {
            'type': 'chain',
            'transforms': chain_steps,
        }
    }
    if needs_cli_provider:
      spec['providers'] = [dlq.cli_yaml_provider_spec()]
    return PipeSpec(
        spec=spec,
        output_prefix=dst_ep.output_prefix,
        source_endpoint=src_ep,
        sink_endpoint=dst_ep,
        dlq_endpoint=None,
        requires_java_xlang=src_ep.is_java_xlang or sink_java,
        is_unbounded=src_ep.is_unbounded)

  # Non-linear DAG when `--dlq` IS active!
  needs_cli_provider = True
  dag_transforms: list[dict[str, Any]] = []
  dlq_norm_refs: list[str] = []

  src_type = src_ep.yaml_type
  src_cfg = dict(src_ep.config)
  source_counts_internally = False
  if src_ep.kind == 'file' and src_ep.format == 'csv':
    src_type = 'ReadCsvWithDlq'
    src_cfg = {
        k: v
        for k, v in src_cfg.items() if k in ('path', 'delimiter')
    }
    src_cfg['error_handling'] = {'output': 'errors'}
    source_counts_internally = True
  elif src_ep.kind == 'file' and src_ep.format == 'json':
    src_type = 'ReadJsonWithDlq'
    src_cfg = {'path': src_cfg['path'], 'error_handling': {'output': 'errors'}}
    source_counts_internally = True
  elif src_type in dlq.TRANSFORMS_SUPPORTING_ERROR_HANDLING:
    src_cfg['error_handling'] = {'output': 'errors'}

  dag_transforms.append({'name': 'ReadSource', 'type': src_type, 'config': src_cfg})
  if 'error_handling' in src_cfg:
    current_ref = (
        'ReadSource.good' if source_counts_internally else 'ReadSource')
    norm_name = 'NormalizeSourceDlq'
    dag_transforms.append(
        dlq.build_dlq_normalizer_transform(
            norm_name, 'ReadSource.errors', src_type))
    dlq_norm_refs.append(norm_name)
  else:
    current_ref = 'ReadSource'

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
    w_cfg['error_handling'] = {'output': 'errors'}
    dag_transforms.append({
        'name': 'FilterWhere',
        'type': 'Filter',
        'input': current_ref,
        'config': w_cfg,
    })
    dag_transforms.append(
        dlq.build_dlq_normalizer_transform(
            'NormalizeFilterDlq', 'FilterWhere.errors', 'Filter'))
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

  if select_cfg is not None:
    s_cfg = dict(select_cfg)
    s_cfg['error_handling'] = {'output': 'errors'}
    dag_transforms.append({
        'name': 'SelectFields',
        'type': 'MapToFields',
        'input': current_ref,
        'config': s_cfg,
    })
    dag_transforms.append(
        dlq.build_dlq_normalizer_transform(
            'NormalizeSelectDlq', 'SelectFields.errors', 'MapToFields'))
    dlq_norm_refs.append('NormalizeSelectDlq')
    current_ref = 'SelectFields'

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

  dst_cfg = dict(dst_ep.config)
  if sink_type in dlq.TRANSFORMS_SUPPORTING_ERROR_HANDLING or (
      sink_type == 'CliWriteToBigQuery'):
    dst_cfg['error_handling'] = {'output': 'errors'}
    dag_transforms.append({
        'name': 'WriteSink',
        'type': sink_type,
        'input': current_ref,
        'config': dst_cfg,
    })
    dag_transforms.append(
        dlq.build_dlq_normalizer_transform(
            'NormalizeSinkDlq', 'WriteSink.errors', dst_ep.yaml_type))
    dlq_norm_refs.append('NormalizeSinkDlq')
  else:
    dag_transforms.append({
        'name': 'WriteSink',
        'type': sink_type,
        'input': current_ref,
        'config': dst_cfg,
    })

  if dlq_norm_refs:
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

  spec = {
      'pipeline': {
          'transforms': dag_transforms,
      },
      'providers': [dlq.cli_yaml_provider_spec()],
  }
  return PipeSpec(
      spec=spec,
      output_prefix=dst_ep.output_prefix,
      source_endpoint=src_ep,
      sink_endpoint=dst_ep,
      dlq_endpoint=dlq_ep,
      requires_java_xlang=(
          src_ep.is_java_xlang or sink_java or dlq_ep.is_java_xlang),
      is_unbounded=src_ep.is_unbounded)


def to_yaml(spec: dict[str, Any], argv: list[str]) -> str:
  """Renders a pipeline as a Beam YAML file to save and run with `beam run`."""
  import yaml
  header = '# Generated by: beam pipe ' + shlex.join(argv)
  dumper = getattr(yaml, 'CSafeDumper', yaml.SafeDumper)
  return header + '\n' + yaml.dump(spec, Dumper=dumper, sort_keys=False)
