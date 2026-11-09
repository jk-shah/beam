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

"""Runs small local file-to-file `beam pipe` jobs in-process.

Starting a Beam runner costs about two seconds, most of it importing
`apache_beam` and bringing up the runner, which dominates small local jobs.
When a `beam pipe` reads and writes local CSV, JSONL, Parquet, or Avro files
below `MAX_INPUT_BYTES`, with no explicit `--runner`, this module evaluates
the same Beam YAML pipeline (the source, the compiled `--where` Filter, the
compiled `--select` MapToFields, and the sink) directly in Python.

It reuses the CLI's own readers (`dlq.iter_csv_records`,
`dlq.parse_json_record`), so typing, DLQ records, and fail-fast messages match
the runner path, and it never imports `apache_beam`. Anything it can't run
exactly the same way (remote paths, sharded or templated output, CDC,
`--verify`, `--if-exists append`, extra reader or writer options) falls back
to the Beam runner. Set `BEAM_CLI_NO_FAST_PATH=1` to always use the runner.

Known differences from the runner path, all caused by Beam core:
- Nested structs are written to Parquet (as structs) and CSV (as JSON text);
  the runner path fails on them (apache/beam#41100, apache/beam#41101).
- JSONL floats keep full precision; Beam's WriteToJson rounds them to 10
  decimal places (apache/beam#41102).
- A run with zero output rows still writes the file (a CSV header only).
"""

from __future__ import annotations

import builtins
import csv
import json
import os
from typing import Any
from typing import Iterable
from typing import NamedTuple

from . import dlq
from . import uris

MAX_INPUT_BYTES = 64 * 1024 * 1024
DISABLE_ENV_VAR = 'BEAM_CLI_NO_FAST_PATH'
RUNNER_NAME = 'local_fast'

_SOURCE_FORMATS = frozenset(['csv', 'json', 'parquet', 'avro'])
_SINK_FORMATS = frozenset(['csv', 'json', 'parquet'])
_SINK_EXTENSIONS = {
    'csv': ('.csv', ),
    'json': ('.json', '.jsonl', '.ndjson'),
    'parquet': ('.parquet', '.pq'),
}
# Source config keys the fast path understands; any other reader option means
# the user tuned the Beam reader, so the runner path handles it.
_SOURCE_KEYS = {
    'csv': frozenset(['path', 'delimiter', 'null_values']),
    'json': frozenset(['path']),
    'parquet': frozenset(['path']),
    'avro': frozenset(['path']),
}
_MAX_ERROR_LEN = 4096


class FastPipeError(Exception):
  """A record or expression failure, reported like a failed Beam pipeline."""


class FastPipeResult(NamedTuple):
  files: list[str]
  dlq_files: list[str]
  metrics: dict[str, int]
  columns: list[str]


class Row:
  """A minimal attribute-access row, standing in for `beam.Row`.

  Compiled `--where`/`--select` callables read fields with
  `getattr(__row__, name)`, so this supports exactly that, plus `_asdict` for
  DLQ payloads. Field names that are Python keywords work too.
  """
  __slots__ = ('_row_fields', )

  def __init__(self, **fields):
    object.__setattr__(self, '_row_fields', fields)

  def __getattr__(self, name):
    try:
      return self._row_fields[name]
    except KeyError:
      raise AttributeError(
          f"'Row' object has no attribute {name!r}") from None

  def __setattr__(self, name, value):
    raise AttributeError('Row is immutable')

  def _asdict(self):
    return dict(self._row_fields)

  def __eq__(self, other):
    return isinstance(other, Row) and self._row_fields == other._row_fields

  def __hash__(self):
    return hash(tuple(self._row_fields.items()))

  def __repr__(self):
    inner = ', '.join(f'{k}={v!r}' for k, v in self._row_fields.items())
    return f'Row({inner})'


def _rowify(value: Any) -> Any:
  if isinstance(value, dict):
    return Row(**{str(k): _rowify(v) for k, v in value.items()})
  if isinstance(value, list):
    return [_rowify(v) for v in value]
  return value


def _plain(value: Any) -> Any:
  if isinstance(value, Row):
    return {k: _plain(v) for k, v in value._row_fields.items()}
  if isinstance(value, list):
    return [_plain(v) for v in value]
  return value


def _is_local(path: str | None) -> bool:
  return bool(path) and '://' not in path and '{{' not in path


def _exact_sink_path(ep: uris.ParsedEndpoint | None) -> str | None:
  if ep is None or ep.kind != 'file' or ep.format not in _SINK_FORMATS:
    return None
  target = ep.output_prefix
  if not _is_local(target) or any(ch in target for ch in '*?['):
    return None
  if set(ep.config) - {'path'}:
    return None
  if not target.lower().endswith(_SINK_EXTENSIONS[ep.format]):
    return None
  return target


def plan(
    pipe,
    *,
    explicit_runner: str | None,
    dry_run: bool,
    cdc: bool,
    zero_copy: bool,
    verify: bool,
    if_exists: str,
    wait_timeout_sec: float | None,
    max_input_bytes: int | None = None,
) -> list[str] | None:
  """Returns the matched input files if `pipe` can run on the fast path."""
  if os.environ.get(DISABLE_ENV_VAR, '').strip().lower() not in ('', '0',
                                                                 'false'):
    return None
  if (explicit_runner or dry_run or cdc or zero_copy or verify or
      wait_timeout_sec or if_exists not in ('fail', 'replace') or
      pipe.is_unbounded):
    return None
  src = pipe.source_endpoint
  if (src is None or src.kind != 'file' or src.format not in _SOURCE_FORMATS or
      not _is_local(src.config.get('path'))):
    return None
  if set(src.config) - _SOURCE_KEYS[src.format]:
    return None
  if _exact_sink_path(pipe.sink_endpoint) is None:
    return None
  if pipe.dlq_endpoint is not None and (
      _exact_sink_path(pipe.dlq_endpoint) is None or
      pipe.dlq_endpoint.format == 'parquet'):
    return None
  if _transform_configs(pipe.spec) is None:
    return None
  files = dlq._match_paths(src.config['path'])
  if not files:
    return None
  try:
    total = sum(os.path.getsize(f) for f in files)
  except OSError:
    return None
  if total > (MAX_INPUT_BYTES if max_input_bytes is None else max_input_bytes):
    return None
  return files


def _transform_configs(spec: dict[str, Any]):
  """Extracts `(where_cfg, select_cfg)` from the spec, or None if unknown.

  Returns None when the spec contains a step the fast path doesn't model, so
  new pipeline features fall back to the runner instead of being skipped.
  """
  where_cfg = None
  select_cfg = None
  known = {
      'ReadFromCsv', 'ReadCsvWithDlq', 'ReadFromJson', 'ReadJsonWithDlq',
      'ReadFromParquet', 'ReadFromAvro', 'CliRowifyNested', 'CliCountRows',
      'WriteToCsv', 'WriteToJson', 'WriteToParquet', 'Flatten'
  }
  for t in spec.get('pipeline', {}).get('transforms', []):
    t_type = t.get('type')
    name = str(t.get('name', ''))
    if t_type == 'Filter':
      where_cfg = t.get('config', {})
    elif t_type == 'MapToFields' and name.startswith('Normalize'):
      continue
    elif t_type == 'MapToFields':
      select_cfg = t.get('config', {})
    elif t_type not in known:
      return None
  for cfg in (where_cfg, select_cfg):
    if cfg is not None and cfg.get('language', 'python') != 'python':
      return None
  return where_cfg, select_cfg


def _compile_callable(src: str):
  namespace: dict[str, Any] = {'__builtins__': builtins}
  exec(compile(src, '<beam-cli-fast-path>', 'exec'), namespace)  # pylint: disable=exec-used
  fns = [
      v for k, v in namespace.items()
      if callable(v) and not k.startswith('__')
  ]
  if len(fns) != 1:
    raise FastPipeError('expected exactly one function in compiled expression')
  return fns[0]


def _compile_expression(expr: str):
  code = compile(expr, '<beam-cli-fast-path>', 'eval')

  def fn(row):
    return eval(code, {'__builtins__': builtins}, row._asdict())  # pylint: disable=eval-used

  return fn


def _compile_field(spec_value: Any):
  if isinstance(spec_value, dict):
    if 'callable' in spec_value:
      return _compile_callable(spec_value['callable'])
    if 'expression' in spec_value:
      return _compile_expression(spec_value['expression'])
    raise FastPipeError(f'unsupported field spec {spec_value!r}')
  name = str(spec_value)
  if name.isidentifier():
    return lambda row: getattr(row, name)
  return _compile_expression(name)


def _avro_arrow_type(avro_type: Any):
  import pyarrow as pa
  if isinstance(avro_type, list):
    non_null = [t for t in avro_type if t != 'null']
    return _avro_arrow_type(non_null[0]) if len(non_null) == 1 else None
  if isinstance(avro_type, dict):
    t = avro_type.get('type')
    if t == 'record':
      fields = []
      for f in avro_type.get('fields', []):
        sub = _avro_arrow_type(f.get('type'))
        if sub is None:
          return None
        fields.append(pa.field(f['name'], sub))
      return pa.struct(fields)
    if t == 'array':
      sub = _avro_arrow_type(avro_type.get('items'))
      return pa.list_(sub) if sub is not None else None
    if avro_type.get('logicalType'):
      return None
    return _avro_arrow_type(t)
  return {
      'string': pa.string(),
      'long': pa.int64(),
      'int': pa.int64(),
      'double': pa.float64(),
      'float': pa.float64(),
      'boolean': pa.bool_(),
      'bytes': pa.binary(),
  }.get(avro_type)


def _kind_arrow_type(kind: Any):
  import pyarrow as pa
  if isinstance(kind, dict):
    return pa.struct([pa.field(k, _kind_arrow_type(v)) for k, v in kind.items()])
  return {
      'int': pa.int64(),
      'float': pa.float64(),
      'bool': pa.bool_(),
  }.get(kind, pa.string())


def _read_source(
    src: uris.ParsedEndpoint, files: list[str], route_errors: bool,
    dlq_records: list[dict[str, Any]]):
  """Returns `(columns, rows, rows_read, arrow_types)` for the source."""
  fmt = src.format
  if fmt == 'csv':
    delim = str(src.config.get('delimiter', ',') or ',')
    nulls = dlq.DEFAULT_CSV_NULL_VALUES | frozenset(
        src.config.get('null_values') or ())
    header, kinds = dlq._preflight_csv_schema(
        src.config['path'], delimiter=delim, null_values=nulls)
    rows = []
    rows_read = 0
    for fp in files:
      with dlq._open_auto(fp) as raw_f:
        for parsed, err in dlq.iter_csv_records(fp, raw_f, header, kinds,
                                                delim, nulls):
          rows_read += 1
          if err is None:
            rows.append(Row(**parsed))
          elif route_errors:
            dlq_records.append(err)
          else:
            raise FastPipeError(
                dlq.csv_fail_fast_message(
                    err, " [while running 'ReadCsvWithDlq']"))
    return header, rows, rows_read, {
        c: _kind_arrow_type(kinds[c])
        for c in header
    }

  if fmt == 'json':
    kinds = dlq.infer_json_kinds(src.config['path'])
    rows = []
    rows_read = 0
    for fp in files:
      with dlq._open_auto(fp) as raw_f:
        for raw_line in raw_f:
          stripped = raw_line.decode('utf-8', errors='replace').strip()
          if not stripped:
            continue
          rows_read += 1
          try:
            rows.append(dlq.parse_json_record(stripped, kinds, Row))
          except Exception as exc:  # pylint: disable=broad-except
            if not route_errors:
              raise FastPipeError(
                  dlq.json_fail_fast_message(
                      stripped, exc,
                      " [while running 'ReadJsonWithDlq']")) from None
            dlq_records.append(dlq.json_dlq_record(stripped, exc))
    return list(kinds), rows, rows_read, {
        k: _kind_arrow_type(v)
        for k, v in kinds.items()
    }

  if fmt == 'parquet':
    import pyarrow as pa
    import pyarrow.parquet as pq
    tables = [pq.read_table(fp) for fp in files]
    table = pa.concat_tables(tables, promote_options='default')
    rows = [_rowify(r) for r in table.to_pylist()]
    return (
        table.column_names, rows, len(rows),
        {f.name: f.type for f in table.schema})

  # Avro.
  import fastavro
  rows = []
  columns: list[str] = []
  types: dict[str, Any] = {}
  for fp in files:
    with open(fp, 'rb') as bf:
      reader = fastavro.reader(bf)
      schema = reader.writer_schema or {}
      for f in schema.get('fields', []):
        if f['name'] not in types:
          columns.append(f['name'])
          types[f['name']] = _avro_arrow_type(f.get('type'))
      for rec in reader:
        rows.append(_rowify(dict(rec)))
  return columns, rows, len(rows), {k: v for k, v in types.items() if v}


def _error_record(step: str, exc: Exception, row: Row) -> dict[str, Any]:
  return {
      'step': step,
      'error_type': type(exc).__name__,
      'error': repr(exc)[:_MAX_ERROR_LEN],
      'raw_payload': dlq.safe_json_dumps(row),
  }


def _cell(value: Any) -> Any:
  if value is None:
    return ''
  if isinstance(value, (Row, dict, list)):
    return json.dumps(_plain(value), default=str, sort_keys=False)
  return value


def _arrow_table(columns: list[str], rows: list[dict[str, Any]],
                 known_types: dict[str, Any]):
  import pyarrow as pa
  arrays = []
  fields = []
  for col in columns:
    values = [r.get(col) for r in rows]
    typ = known_types.get(col)
    arr = None
    if typ is not None:
      try:
        arr = pa.array(values, type=typ)
      except (pa.ArrowInvalid, pa.ArrowTypeError):
        arr = None
    if arr is None:
      arr = pa.array(values)
      if pa.types.is_null(arr.type):
        arr = pa.array(values, type=pa.string())
    arrays.append(arr)
    fields.append(pa.field(col, arr.type))
  return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _write(
    ep: uris.ParsedEndpoint, columns: list[str], rows: Iterable[dict[str, Any]],
    known_types: dict[str, Any]) -> str:
  target = ep.output_prefix
  parent = os.path.dirname(os.path.abspath(target))
  os.makedirs(parent, exist_ok=True)
  tmp_path = f'{target}.tmp.{os.getpid()}'
  rows = list(rows)
  try:
    if ep.format == 'parquet':
      import pyarrow.parquet as pq
      pq.write_table(_arrow_table(columns, rows, known_types), tmp_path)
    elif ep.format == 'csv':
      with open(tmp_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, lineterminator='\n')
        writer.writerow(columns)
        for r in rows:
          writer.writerow([_cell(r.get(c)) for c in columns])
    else:
      with open(tmp_path, 'w', encoding='utf-8') as f:
        for r in rows:
          f.write(json.dumps(r, default=str) + '\n')
    os.replace(tmp_path, target)
  finally:
    if os.path.exists(tmp_path):
      try:
        os.unlink(tmp_path)
      except OSError:
        pass
  return target


def run(pipe, files: list[str]) -> FastPipeResult:
  """Runs `pipe` in-process. Raises FastPipeError like a failed pipeline."""
  where_cfg, select_cfg = _transform_configs(pipe.spec)
  route_errors = pipe.dlq_endpoint is not None
  dlq_records: list[dict[str, Any]] = []

  columns, rows, rows_read, types = _read_source(
      pipe.source_endpoint, files, route_errors, dlq_records)

  filtered_out = 0
  if where_cfg is not None:
    keep = _compile_field(where_cfg.get('keep'))
    kept = []
    for row in rows:
      try:
        ok = keep(row)
      except Exception as exc:  # pylint: disable=broad-except
        if not route_errors:
          raise FastPipeError(
              f"{type(exc).__name__}: {exc} [while running 'Filter']"
          ) from None
        dlq_records.append(_error_record('Filter', exc, row))
        continue
      if ok:
        kept.append(row)
      else:
        filtered_out += 1
    rows = kept

  if select_cfg is not None:
    fields = {
        name: _compile_field(spec)
        for name, spec in select_cfg.get('fields', {}).items()
    }
    out_types = {
        name: types[str(spec)]
        for name, spec in select_cfg.get('fields', {}).items()
        if not isinstance(spec, dict) and str(spec) in types
    }
    projected = []
    for row in rows:
      try:
        projected.append(Row(**{name: fn(row) for name, fn in fields.items()}))
      except Exception as exc:  # pylint: disable=broad-except
        if not route_errors:
          raise FastPipeError(
              f"{type(exc).__name__}: {exc} [while running 'MapToFields']"
          ) from None
        dlq_records.append(_error_record('MapToFields', exc, row))
    rows = projected
    columns = list(fields)
    types = out_types

  out_rows = [_plain(r) for r in rows]
  written = [_write(pipe.sink_endpoint, columns, out_rows, types)]
  dlq_files: list[str] = []
  if route_errors:
    dlq_files.append(
        _write(
            pipe.dlq_endpoint, ['step', 'error_type', 'error', 'raw_payload'],
            dlq_records, {}))

  metrics = {'rows_read': rows_read, 'rows_written': len(out_rows)}
  if where_cfg is not None:
    metrics['rows_filtered_out'] = filtered_out
  if route_errors:
    metrics['dlq_rows'] = len(dlq_records)
  return FastPipeResult(
      files=written, dlq_files=dlq_files, metrics=metrics, columns=columns)
