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


def _normalize_primary_keys(primary_keys: Iterable[str] | None) -> list[str]:
  out: list[str] = []
  for raw_pk in primary_keys or ():
    for part in str(raw_pk).split(','):
      col = part.strip()
      if col and col not in out:
        out.append(col)
  return out


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
    cdc: bool = False,
    primary_keys: Iterable[str] | None = None,
    cdc_format: str = 'auto',
    cdc_op_col: str | None = None,
    cdc_seq_col: str | None = None,
    zero_copy: bool = False,
    prefer_python_lakehouse_and_cdc: bool = False,
) -> PipeSpec:
  """Returns the `PipeSpec` for `beam pipe`."""
  if if_exists not in ('fail', 'append', 'replace'):
    raise PipeSpecError(
        f'invalid --if-exists mode {if_exists!r}; use fail, append, or replace')

  pks = _normalize_primary_keys(primary_keys)
  if cdc_format not in ('auto', 'debezium', 'flat'):
    raise PipeSpecError(
        f'invalid --cdc-format {cdc_format!r}; use auto, debezium, or flat')

  try:
    parsed_from_opts = uris.parse_kv_options(
        from_opts, from_opt_file, '--from-opt')
    parsed_to_opts = uris.parse_kv_options(to_opts, to_opt_file, '--to-opt')
    raw_cdc_fields = parsed_from_opts.pop('fields', None) if cdc else None
    src_ep = uris.parse_endpoint(
        source,
        is_read=True,
        explicit_format=from_format,
        options=parsed_from_opts,
        default_project=default_project,
        allow_sniff=True,
        cdc=cdc)
    dst_ep = uris.parse_endpoint(
        sink,
        is_read=False,
        explicit_format=to_format,
        options=parsed_to_opts,
        if_exists=if_exists,
        default_project=default_project,
        cdc=cdc,
        zero_copy=zero_copy)
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

  if zero_copy:
    if cdc or where or select or dlq_ep is not None:
      raise PipeSpecError(
          '--zero-copy registers existing Parquet files in-place without '
          'rewriting data, so it cannot be combined with --cdc, --where, '
          '--select, or --dlq')
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

  try:
    where_cfg = (
        expressions.compile_where(where, known_columns=known_source_columns)[0]
        if where else None)
    select_cfg = (
        expressions.compile_select(
            select, known_columns=known_source_columns)[0]
        if select else None)
  except expressions.ExpressionError as e:
    raise PipeSpecError(str(e)) from None

  if cdc and select_cfg is not None and prefer_python_lakehouse_and_cdc:
    # Preserve normalized CDC metadata columns through `--select` projections.
    sel_fields = dict(select_cfg.get('fields') or {})
    for mcol, mtype in (
        ('beam_cdc_op', 'string'),
        ('beam_cdc_seq', 'int64'),
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
  src_type = src_ep.yaml_type
  src_cfg = dict(src_ep.config)
  src_java = src_ep.is_java_xlang

  sink_type = dst_ep.yaml_type
  sink_cfg = dict(dst_ep.config)
  sink_java = dst_ep.is_java_xlang

  cdc_norm_cfg: dict[str, Any] | None = None
  if cdc:
    from . import cdc as cdc_mod
    resolved_cdc_fields: list[tuple[str, str]] = []
    if raw_cdc_fields is not None:
      try:
        resolved_cdc_fields = cdc_mod.parse_fields_shorthand(
            str(raw_cdc_fields))
      except ValueError as e:
        raise PipeSpecError(str(e)) from None
    elif src_ep.kind == 'file' and src_ep.format == 'json':
      resolved_cdc_fields = cdc_mod.infer_cdc_jsonl_fields(
          src_ep.config['path'],
          cdc_format=cdc_format,
          op_col=cdc_op_col,
          seq_col=cdc_seq_col,
          primary_keys=pks)
      if not resolved_cdc_fields and prefer_python_lakehouse_and_cdc:
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
    if dst_ep.kind == 'iceberg' and cdc:
      sink_cfg.setdefault('mode', 'merge-on-read')
      sink_cfg.setdefault('equality_columns', pks)
      sink_cfg.setdefault(
          'change_type_column',
          cdc_op_col or (
              '_change_type' if src_ep.kind in ('delta', 'iceberg') else 'op'))
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

  # Pure Java-to-Java fast path: skip Python counter stages unless explicitly
  # requested (`include_counters=True` when `--assert-counts` or Python stage).
  pure_java_pipe = (
      src_java and sink_java and cdc_norm_cfg is None and not where_cfg and
      not select_cfg and not dlq_ep)
  inject_counters = bool(include_counters) and not pure_java_pipe
  if inject_counters:
    needs_cli_provider = True

  # Simple linear chain when `--dlq` is NOT active -> clean, idiomatic YAML!
  if dlq_ep is None:
    chain_steps: list[dict[str, Any]] = [
        {
            'type': src_type,
            'config': src_cfg,
        }
    ]
    if inject_counters:
      chain_steps.append({
          'type': 'CliCountRows',
          'config': {
              'counter': 'rows_read'
          },
      })
    if cdc_norm_cfg is not None:
      chain_steps.append({
          'type': 'CliNormalizeCdc',
          'config': dict(cdc_norm_cfg),
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
        'config': sink_cfg,
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
        requires_java_xlang=src_java or sink_java,
        is_unbounded=src_ep.is_unbounded)

  # Non-linear DAG when `--dlq` IS active!
  needs_cli_provider = True
  dag_transforms: list[dict[str, Any]] = []
  dlq_norm_refs: list[str] = []

  source_counts_internally = False
  if src_ep.kind == 'file' and src_ep.format == 'csv':
    src_type = 'ReadCsvWithDlq'
    src_cfg = {
        k: v
        for k, v in src_cfg.items() if k in ('path', 'delimiter')
    }
    src_cfg['error_handling'] = {'output': 'errors'}
    source_counts_internally = True
  elif src_ep.kind == 'file' and src_ep.format == 'json' and not cdc:
    src_type = 'ReadJsonWithDlq'
    src_cfg = {'path': src_cfg['path'], 'error_handling': {'output': 'errors'}}
    source_counts_internally = True
  elif (
      src_type in dlq.TRANSFORMS_SUPPORTING_ERROR_HANDLING
      and not (cdc and src_type == 'ReadFromPubSub'
               and src_cfg.get('format') == 'STRING')):
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

  if cdc_norm_cfg is not None:
    norm_cdc = dict(cdc_norm_cfg)
    norm_cdc['error_handling'] = {'output': 'errors'}
    dag_transforms.append({
        'name': 'NormalizeCdc',
        'type': 'CliNormalizeCdc',
        'input': current_ref,
        'config': norm_cdc,
    })
    dag_transforms.append(
        dlq.build_dlq_normalizer_transform(
            'NormalizeCdcDlq', 'NormalizeCdc.errors', 'NormalizeCdc'))
    dlq_norm_refs.append('NormalizeCdcDlq')
    current_ref = 'NormalizeCdc.good'

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

  dst_cfg = dict(sink_cfg)
  if sink_type in dlq.TRANSFORMS_SUPPORTING_ERROR_HANDLING or (
      sink_type in ('CliWriteToBigQuery', 'CliWriteToBigQueryCDC')):
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
          src_java or sink_java or dlq_ep.is_java_xlang),
      is_unbounded=src_ep.is_unbounded)


def to_yaml(spec: dict[str, Any], argv: list[str]) -> str:
  """Renders a pipeline as a Beam YAML file to save and run with `beam run`."""
  import yaml
  header = '# Generated by: beam pipe ' + shlex.join(argv)
  dumper = getattr(yaml, 'CSafeDumper', yaml.SafeDumper)
  return header + '\n' + yaml.dump(spec, Dumper=dumper, sort_keys=False)
