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
    'csv': frozenset(['path', 'delimiter', 'null_values', 'types']),
    'json': frozenset(['path', 'types', 'sample_rows', 'on_unknown_fields']),
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
  warnings: list[str] = []


def _text_number_columns(exc: Exception, row: Row, columns) -> list[str]:
  """Columns that hold text when a comparison mixed text and numbers."""
  if not (isinstance(exc, TypeError) and
          'not supported between instances of' in str(exc) and
          "'str'" in str(exc)):
    return []
  return [
      c for c in columns
      if isinstance(row._row_fields.get(c), str) and _looks_numeric(
          row._row_fields.get(c))
  ]


def _looks_numeric(value: str) -> bool:
  try:
    float(value)
    return True
  except ValueError:
    return False


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
  extra = set(ep.config) - {'path'}
  if extra and not (ep.format == 'json' and extra <= {'key', 'value'} and
                    target.lower().endswith('.json')):
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
  del cdc  # Materialize-to-file CDC is checked via `_transform_configs`.
  if os.environ.get(DISABLE_ENV_VAR, '').strip().lower() not in ('', '0',
                                                                 'false'):
    return None
  if (explicit_runner or dry_run or zero_copy or verify or
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
  """Extracts `(cdc_norm_cfg, cdc_mat_cfg, where_cfg, select_cfg)` or None.

  Returns None when the spec contains a step the fast path doesn't model, so
  new pipeline features fall back to the runner instead of being skipped.
  """
  cdc_norm_cfg = None
  cdc_mat_cfg = None
  where_cfg = None
  select_cfg = None
  expect_cfg = None
  known = {
      'ReadFromCsv', 'ReadCsvWithDlq', 'ReadFromJson', 'ReadJsonWithDlq',
      'ReadFromText', 'ReadFromParquet', 'ReadFromAvro', 'CliRowifyNested',
      'CliCountRows', 'WriteToCsv', 'WriteToJson', 'WriteToParquet', 'Flatten',
      'CliExpect',
  }
  for t in spec.get('pipeline', {}).get('transforms', []):
    t_type = t.get('type')
    name = str(t.get('name', ''))
    if t_type == 'CliNormalizeCdc':
      cdc_norm_cfg = t.get('config', {})
    elif t_type == 'CliMaterializeCdc':
      cdc_mat_cfg = t.get('config', {})
    elif t_type == 'Filter':
      where_cfg = t.get('config', {})
    elif t_type == 'MapToFields' and name.startswith('Normalize'):
      continue
    elif t_type == 'MapToFields':
      select_cfg = t.get('config', {})
    elif t_type == 'CliExpect':
      expect_cfg = t.get('config', {})
    elif t_type not in known:
      return None
  if (cdc_norm_cfg is not None) != (cdc_mat_cfg is not None):
    return None
  for cfg in (where_cfg, select_cfg):
    if cfg is not None and cfg.get('language', 'python') != 'python':
      return None
  return cdc_norm_cfg, cdc_mat_cfg, where_cfg, select_cfg, expect_cfg


def _safe_row_getattr(obj: Any, attr: str):
  if not isinstance(attr, str) or attr.startswith('_'):
    raise AttributeError(f'private attribute {attr!r} is not allowed')
  return getattr(obj, attr)


_SAFE_FAST_BUILTINS: dict[str, Any] = {
    'abs': abs,
    'bool': bool,
    'float': float,
    'getattr': _safe_row_getattr,
    'int': int,
    'isinstance': isinstance,
    'len': len,
    'list': list,
    'max': max,
    'min': min,
    'round': round,
    'str': str,
    'tuple': tuple,
    'bytes': bytes,
    'TypeError': TypeError,
    'ValueError': ValueError,
    'None': None,
    'True': True,
    'False': False,
}


def _compile_callable(src: str):
  namespace: dict[str, Any] = {'__builtins__': _SAFE_FAST_BUILTINS}
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
    return eval(code, {'__builtins__': _SAFE_FAST_BUILTINS}, row._asdict())  # pylint: disable=eval-used

  return fn


def _compile_field(spec_value: Any):
  if isinstance(spec_value, dict):
    if 'callable' in spec_value:
      return _compile_callable(spec_value['callable'])
    if 'expression' in spec_value:
      return _compile_expression(spec_value['expression'])
    raise FastPipeError(f'unsupported field spec {spec_value!r}')
  name = str(spec_value)
  if name.isidentifier() and not name.startswith('_'):
    return lambda row: _safe_row_getattr(row, name)
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
    src: uris.ParsedEndpoint,
    files: list[str],
    route_errors: bool,
    dlq_records: list[dict[str, Any]],
    drift_state: dict[str, Any] | None = None,
):
  """Returns `(columns, rows, rows_read, arrow_types)` for the source."""
  fmt = src.format
  if fmt == 'csv':
    delim = str(src.config.get('delimiter', ',') or ',')
    nulls = dlq.DEFAULT_CSV_NULL_VALUES | frozenset(
        src.config.get('null_values') or ())
    header, kinds = dlq._preflight_csv_schema(
        src.config['path'],
        delimiter=delim,
        null_values=nulls,
        scan_all_for_leading_zeros=True)
    kinds = dlq.apply_type_overrides(
        kinds, src.config.get('types'), str(src.raw))
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
    eff_sample = dlq.resolve_json_sample_rows(
        files, src.config.get('sample_rows'))
    allow_unknown = (
        dlq.parse_unknown_fields_opt(src.config.get('on_unknown_fields')) ==
        'ignore')
    kinds = dlq.apply_type_overrides(
        dlq.infer_json_kinds(src.config['path'], sample_rows=eff_sample),
        src.config.get('types'),
        str(src.raw),
        allow_new_columns=True)
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
            rows.append(
                dlq.parse_json_record(
                    stripped,
                    kinds,
                    Row,
                    allow_unknown_fields=allow_unknown))
          except Exception as exc:  # pylint: disable=broad-except
            if not route_errors:
              raise FastPipeError(
                  dlq.json_fail_fast_message(
                      stripped, exc,
                      " [while running 'ReadJsonWithDlq']")) from None
            if isinstance(exc, dlq.SchemaDriftError) and drift_state is not None:
              drift_state['count'] = int(drift_state.get('count', 0)) + 1
              drift_state.setdefault('fields', set()).update(exc.extra_fields)
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


def _merge_arrow_type(a, b):
  """Common Arrow type of one column read from two formats, or None."""
  import pyarrow as pa
  if a is None or b is None or a == b:
    return a if b is None else b if a is None else a
  if pa.types.is_integer(a) and pa.types.is_floating(b):
    return b
  if pa.types.is_floating(a) and pa.types.is_integer(b):
    return a
  return None


def _typed_columns_outside_csv(groups) -> dict[str, str]:
  """`{column: kind}` for int/float/bool columns of the non-CSV files."""
  if 'csv' not in groups:
    return {}
  kinds: dict[str, str] = {}
  if 'json' in groups:
    json_files = list(groups['json'])
    for col, kind in dlq.infer_json_kinds(
        json_files,
        sample_rows=dlq.resolve_json_sample_rows(json_files)).items():
      if kind in ('int', 'float', 'bool'):
        kinds[col] = kind
  if 'parquet' in groups:
    import pyarrow as pa
    import pyarrow.parquet as pq
    for field in pq.read_schema(groups['parquet'][0]):
      t = field.type
      kind = ('bool' if pa.types.is_boolean(t) else
              'int' if pa.types.is_integer(t) else
              'float' if pa.types.is_floating(t) else None)
      if kind:
        kinds[field.name] = (
            'float' if {kinds.get(field.name), kind} == {'int', 'float'} else
            kind)
  # Only columns the CSV files have; extra columns are just NULL there.
  csv_header, _ = dlq._preflight_csv_schema(list(groups['csv']))
  return {c: k for c, k in kinds.items() if c in csv_header}


def _read_mixed(src, groups, route_errors, dlq_records, drift_state=None):
  """Reads a glob that matched several formats, each file with its own reader.

  Rows are unified by column name: columns appear in first-seen order and a
  column missing from one format is NULL there. A column whose type differs
  between formats keeps its values as they are and lets Arrow pick the type.
  """
  columns: list[str] = []
  rows: list[Row] = []
  rows_read = 0
  types: dict[str, Any] = {}
  conflicts: set[str] = set()
  csv_types = _typed_columns_outside_csv(groups)
  for fmt, fmt_files in groups.items():
    keys = _SOURCE_KEYS[fmt] - {'path'}
    cfg = {
        'path': list(fmt_files),
        **{k: v for k, v in src.config.items() if k in keys}
    }
    if fmt == 'csv' and csv_types:
      # CSV has no types: a column that is numeric/boolean in the JSONL or
      # Parquet files is read with that type, so "100.0" becomes 100.0 and
      # a value such as "oops" goes to the DLQ instead of turning the whole
      # column into text. Explicit --from-opt fields still win.
      cfg['types'] = {**csv_types, **(cfg.get('types') or {})}
    sub = src._replace(format=fmt, config=cfg)
    cols, fmt_rows, n, fmt_types = _read_source(
        sub, fmt_files, route_errors, dlq_records, drift_state=drift_state)
    for c in cols:
      if c not in columns:
        columns.append(c)
    for c, t in fmt_types.items():
      if c in conflicts:
        continue
      if c in types:
        merged = _merge_arrow_type(types[c], t)
        if merged is None:
          conflicts.add(c)
          types.pop(c)
        else:
          types[c] = merged
      else:
        types[c] = t
    rows.extend(fmt_rows)
    rows_read += n
  rows = [
      Row(**{c: r._row_fields.get(c) for c in columns}) for r in rows
  ]
  return columns, rows, rows_read, types


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


def _new_stage_path(target: str, tag: str) -> str:
  import tempfile
  abs_target = os.path.abspath(target)
  parent = os.path.dirname(abs_target) or '.'
  os.makedirs(parent, exist_ok=True)
  base = os.path.basename(abs_target) or 'out'
  fd, tmp_path = tempfile.mkstemp(
      prefix=f'.{base}.{tag}.', suffix='.tmp', dir=parent)
  os.close(fd)
  try:
    os.chmod(tmp_path, 0o644)
  except OSError:
    pass
  return tmp_path


def _stage_write(
    ep: uris.ParsedEndpoint,
    columns: list[str],
    rows: Iterable[dict[str, Any]],
    known_types: dict[str, Any],
    *,
    tag: str = 'out',
) -> tuple[str, str]:
  """Writes `rows` to an exclusive sibling `.tmp` file without touching `target`."""
  target = ep.output_prefix
  tmp_path = _new_stage_path(target, tag)
  try:
    rows = list(rows)
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
  except BaseException:
    if os.path.exists(tmp_path):
      try:
        os.unlink(tmp_path)
      except OSError:
        pass
    raise
  return tmp_path, target


def _stage_json_document(target: str, doc: Any) -> tuple[str, str]:
  tmp_path = _new_stage_path(target, 'doc')
  try:
    with open(tmp_path, 'w', encoding='utf-8') as f:
      json.dump(doc, f, indent=2, default=str)
      f.write('\n')
  except BaseException:
    if os.path.exists(tmp_path):
      try:
        os.unlink(tmp_path)
      except OSError:
        pass
    raise
  return tmp_path, target


def _arrow_type_to_kind(arrow_type: Any) -> str:
  if arrow_type is None:
    return 'str'
  import pyarrow as pa
  if pa.types.is_boolean(arrow_type):
    return 'bool'
  if pa.types.is_integer(arrow_type):
    return 'int'
  if pa.types.is_floating(arrow_type):
    return 'float'
  if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
    return 'bytes'
  return 'str'


def run(
    pipe,
    files: list[str],
    mixed_groups=None,
    *,
    assert_counts: bool = False,
) -> FastPipeResult:
  """Runs `pipe` in-process. Raises FastPipeError like a failed pipeline.

  `mixed_groups` (`{format: files}`) reads a glob that matched several file
  formats, each file with its own reader. Existing outputs are kept untouched
  until every transform, count assertion, and staged write succeeds.
  """
  cdc_norm_cfg, cdc_mat_cfg, where_cfg, select_cfg, expect_cfg = _transform_configs(
      pipe.spec)
  route_errors = pipe.dlq_endpoint is not None
  dlq_records: list[dict[str, Any]] = []
  drift_state: dict[str, Any] = {'count': 0, 'fields': set()}
  cdc_superseded = 0
  cdc_preimage_skipped = 0

  if cdc_norm_cfg is not None:
    from . import cdc as cdc_mod
    pks = [str(k) for k in (cdc_norm_cfg.get('primary_key') or ())]
    op_col = cdc_norm_cfg.get('op_col')
    seq_col = cdc_norm_cfg.get('seq_col') or (
        (cdc_mat_cfg or {}).get('sequence_column'))
    if pipe.source_endpoint.format == 'json':
      raw_records: list[Any] = []
      for fp in files:
        with dlq._open_auto(fp) as raw_f:
          for raw_line in raw_f:
            stripped = raw_line.decode('utf-8', errors='replace').strip()
            if stripped:
              raw_records.append(stripped)
      rows_read = len(raw_records)
      col_specs = [
          (str(p[0]), str(p[1]))
          for p in (cdc_norm_cfg.get('fields_spec') or ())
      ]
    else:
      src_cols, src_rows, rows_read, src_types = _read_source(
          pipe.source_endpoint,
          files,
          route_errors,
          dlq_records,
          drift_state=drift_state)
      raw_records = src_rows
      if cdc_norm_cfg.get('fields_spec'):
        col_specs = [
            (str(p[0]), str(p[1])) for p in cdc_norm_cfg['fields_spec']
        ]
      else:
        col_specs = cdc_mod.resolve_cdc_source_columns(
            [(c, _arrow_type_to_kind(src_types.get(c))) for c in src_cols],
            primary_key=pks,
            op_col=op_col,
            seq_col=seq_col)
    if not col_specs:
      raise FastPipeError(
          'Unable to infer CDC payload columns; pass --from-opt '
          'fields="id:int,col:str,..." or target an existing table')
    try:
      (
          columns, rows, cdc_superseded, cdc_preimage_skipped, cdc_drift_rows,
          cdc_drift_fields) = cdc_mod.materialize_cdc_records(
              raw_records,
              col_specs=col_specs,
              primary_key=pks,
              cdc_format=str(cdc_norm_cfg.get('cdc_format', 'auto')),
              op_col=op_col,
              seq_col=seq_col,
              strict_inferred_fields=bool(
                  cdc_norm_cfg.get('strict_inferred_fields')),
              on_unknown_fields=str(
                  cdc_norm_cfg.get('on_unknown_fields', 'error')),
              route_errors=route_errors,
              row_factory=Row,
              dlq_list=dlq_records)
    except ValueError as exc:
      raise FastPipeError(
          f"{exc} [while running 'CliMaterializeCdc']") from None
    if cdc_drift_rows:
      drift_state['count'] = int(drift_state.get('count', 0)) + cdc_drift_rows
      drift_state.setdefault('fields', set()).update(cdc_drift_fields)
    types = {c: _kind_arrow_type(k) for c, k in col_specs}
  elif mixed_groups:
    columns, rows, rows_read, types = _read_mixed(
        pipe.source_endpoint,
        mixed_groups,
        route_errors,
        dlq_records,
        drift_state=drift_state)
  else:
    columns, rows, rows_read, types = _read_source(
        pipe.source_endpoint,
        files,
        route_errors,
        dlq_records,
        drift_state=drift_state)

  filtered_out = 0
  text_number_cols: set[str] = set()
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
        text_number_cols.update(_text_number_columns(exc, row, columns))
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

  exp_metrics: dict[str, int] = {}
  if expect_cfg is not None:
    from . import expect as expect_mod
    raw_rules = expect_cfg.get('rules', [])
    parsed_rules = [
        expect_mod.parse_expectation_rule(r) if not isinstance(r, expect_mod.ExpectationRule) else r
        for r in raw_rules
    ]
    compiled_rules = [
        (r, compile(r.expression, f'<fast_expectation:{r.name}>', 'eval'))
        for r in parsed_rules
    ]
    kept_exp = []
    for row in rows:
      failed_rule = None
      for rule, code in compiled_rules:
        if expect_mod.evaluate_record(code, row):
          exp_metrics[f'expectation_passed_{rule.name}'] = (
              exp_metrics.get(f'expectation_passed_{rule.name}', 0) + 1)
        else:
          failed_rule = rule
          break
      if failed_rule is None:
        kept_exp.append(row)
        continue

      exp_metrics['expectation_violations_total'] = (
          exp_metrics.get('expectation_violations_total', 0) + 1)
      action = failed_rule.on_violation
      if action == 'fail':
        exp_metrics[f'expectation_failed_{failed_rule.name}'] = (
            exp_metrics.get(f'expectation_failed_{failed_rule.name}', 0) + 1)
        raise FastPipeError(
            f"Expectation rule {failed_rule.name!r} ({failed_rule.expression}) "
            f"violated by row: {row._asdict() if hasattr(row, '_asdict') else row}")
      elif action == 'dlq':
        exp_metrics[f'expectation_quarantined_{failed_rule.name}'] = (
            exp_metrics.get(f'expectation_quarantined_{failed_rule.name}', 0) + 1)
        exp_metrics['expectation_quarantined_rows'] = (
            exp_metrics.get('expectation_quarantined_rows', 0) + 1)
        if not route_errors:
          raise FastPipeError(
              f"Expectation rule {failed_rule.name!r} violated and no DLQ route configured; "
              f"row: {row._asdict() if hasattr(row, '_asdict') else row}")
        dlq_records.append(_error_record('CliExpect', expect_mod.ExpectationViolation(f"Rule {failed_rule.name!r} violated: {failed_rule.expression}"), row))
      elif action == 'drop':
        exp_metrics[f'expectation_dropped_{failed_rule.name}'] = (
            exp_metrics.get(f'expectation_dropped_{failed_rule.name}', 0) + 1)
        exp_metrics['expectation_dropped_rows'] = (
            exp_metrics.get('expectation_dropped_rows', 0) + 1)
        filtered_out += 1
    rows = kept_exp

  out_rows = [_plain(r) for r in rows]
  metrics: dict[str, int] = {'rows_read': rows_read, 'rows_written': len(out_rows), **exp_metrics}
  if cdc_superseded:
    metrics['cdc_superseded_rows'] = cdc_superseded
  if (where_cfg is not None or cdc_superseded > 0 or cdc_preimage_skipped > 0 or expect_cfg is not None):
    metrics['rows_filtered_out'] = (
        filtered_out + cdc_superseded + cdc_preimage_skipped)
  if route_errors:
    metrics['dlq_rows'] = len(dlq_records)
  drift_count = int(drift_state.get('count', 0))
  if drift_count > 0:
    metrics['schema_drift_rows'] = drift_count

  if assert_counts:
    from . import jobs
    err_msg = jobs.check_assert_counts(metrics)
    if err_msg:
      raise FastPipeError(err_msg)

  from . import pipe_sql
  keyed = pipe_sql.keyed_json_options(pipe.sink_endpoint)
  staged_pairs: list[tuple[str, str]] = []
  try:
    if keyed is not None:
      try:
        doc = pipe_sql.keyed_json_document(
            columns, out_rows, keyed['key'], keyed.get('value'))
      except pipe_sql.SqlPipeError as e:
        raise FastPipeError(str(e)) from None
      sink_pair = _stage_json_document(pipe.sink_endpoint.output_prefix, doc)
    else:
      sink_pair = _stage_write(
          pipe.sink_endpoint, columns, out_rows, types, tag='sink')
    staged_pairs.append(sink_pair)

    dlq_pair: tuple[str, str] | None = None
    if route_errors:
      dlq_pair = _stage_write(
          pipe.dlq_endpoint,
          ['step', 'error_type', 'error', 'raw_payload'],
          dlq_records,
          {},
          tag='dlq')
      staged_pairs.append(dlq_pair)

    for tmp_p, final_p in staged_pairs:
      os.replace(tmp_p, final_p)
  finally:
    for tmp_p, _ in staged_pairs:
      if os.path.exists(tmp_p):
        try:
          os.unlink(tmp_p)
        except OSError:
          pass

  written = [sink_pair[1]]
  dlq_files = [dlq_pair[1]] if dlq_pair is not None else []
  warnings = []
  if drift_count > 0:
    warnings.append(
        dlq.schema_drift_warning(drift_count, drift_state.get('fields', ())))
  if text_number_cols:
    cols = ', '.join(sorted(text_number_cols))
    spec = ','.join(f'{c}:float' for c in sorted(text_number_cols))
    warnings.append(
        f'rows went to the DLQ because --where compared text with a number: '
        f'column(s) {cols} were read as text (some sampled values are not '
        f'numbers); set the type with --from-opt fields={spec} so valid '
        'numbers are kept and only bad values go to the DLQ')
  return FastPipeResult(
      files=written,
      dlq_files=dlq_files,
      metrics=metrics,
      columns=columns,
      warnings=warnings)
