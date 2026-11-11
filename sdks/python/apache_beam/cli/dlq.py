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

"""RFC-4180 CSV dead-letter source, DLQ normalizer, and fusion-safe counters.

Key guarantees:
1. `read_csv_with_dlq` uses `fileio.MatchFiles` -> `Reshuffle` ->
   `fileio.ReadMatches` + per-shard `csv.reader(..., newline='')` so quoted
   multiline fields work across sharded globs without line-splitting corruption
   or treating shard headers as data rows.
2. Pre-flight 100-row sample locks a static nullable `RowTypeConstraint` at
   graph construction time so downstream transforms (`Filter`, `MapToFields`,
   `WriteToParquet`, `WriteToBigQuery`) receive exact Beam schemas.
3. Every transform error output (`element`, `failed_row`, `record`) is
   normalized via `_safe_json` into `Row(step, error_type, error, raw_payload)`
   before `Flatten` so heterogeneous error schemas never fail schema unioning.
"""

import base64
import csv
import decimal
import io
import json
import math
import os
import re
from typing import Any
from typing import Iterable
from typing import Optional

# Transforms in Beam YAML's standard library that accept `error_handling` in
# their `config:` block.
TRANSFORMS_SUPPORTING_ERROR_HANDLING = frozenset([
    'Filter',
    'MapToFields',
    'CliExpect',
    'ReadCsvWithDlq',
    'ReadFromJsonWithDlq',
    'ReadFromPubSub',
    'WriteToPubSub',
    'ReadFromKafka',
    'WriteToKafka',
    'WriteToBigQuery',
    'WriteToBigQueryCDC',
    'ReadFromSpanner',
    'WriteToSpanner',
])

# Embedded source for `MapToFields(language: python)` DLQ normalization so
# `--emit-yaml > pipeline.yaml && beam run pipeline.yaml` is 100% portable!
_SAFE_JSON_HELPERS_SRC = """\
  import base64, decimal, json, math
  def _to_jsonable(v, depth=8):
    if depth <= 0:
      return repr(v)
    if v is None or isinstance(v, (bool, int, str)):
      return v
    if isinstance(v, float):
      return str(v) if (math.isnan(v) or math.isinf(v)) else v
    if isinstance(v, (bytes, bytearray, memoryview)):
      return {'_encoding': 'base64', 'data': base64.b64encode(bytes(v)).decode('ascii')}
    if isinstance(v, decimal.Decimal):
      return str(v)
    if hasattr(v, '_asdict'):
      return {str(k): _to_jsonable(val, depth - 1) for k, val in v._asdict().items()}
    if isinstance(v, dict):
      return {str(k): _to_jsonable(val, depth - 1) for k, val in v.items()}
    if isinstance(v, (list, tuple)):
      return [_to_jsonable(x, depth - 1) for x in v]
    if hasattr(v, 'to_rfc3339'):
      try:
        return v.to_rfc3339()
      except Exception:
        pass
    if hasattr(v, 'isoformat'):
      try:
        return v.isoformat()
      except Exception:
        pass
    return repr(v)
  def _safe_json_str(v):
    try:
      return json.dumps(_to_jsonable(v), allow_nan=False, sort_keys=True)
    except Exception:
      return json.dumps({'repr': repr(v)})
"""


def safe_json_dumps(value: Any) -> str:
  """Serializes any Beam `Row`, `Timestamp`, `Decimal`, `bytes`, or `NaN`."""
  def _conv(v: Any, depth: int = 8) -> Any:
    if depth <= 0:
      return repr(v)
    if v is None or isinstance(v, (bool, int, str)):
      return v
    if isinstance(v, float):
      return str(v) if (math.isnan(v) or math.isinf(v)) else v
    if isinstance(v, (bytes, bytearray, memoryview)):
      return {
          '_encoding': 'base64',
          'data': base64.b64encode(bytes(v)).decode('ascii'),
      }
    if isinstance(v, decimal.Decimal):
      return str(v)
    if hasattr(v, '_asdict'):
      return {str(k): _conv(val, depth - 1) for k, val in v._asdict().items()}
    if isinstance(v, dict):
      return {str(k): _conv(val, depth - 1) for k, val in v.items()}
    if isinstance(v, (list, tuple)):
      return [_conv(x, depth - 1) for x in v]
    if hasattr(v, 'to_rfc3339'):
      try:
        return v.to_rfc3339()
      except Exception:  # pylint: disable=broad-except
        pass
    if hasattr(v, 'isoformat'):
      try:
        return v.isoformat()
      except Exception:  # pylint: disable=broad-except
        pass
    return repr(v)

  try:
    return json.dumps(_conv(value), allow_nan=False, sort_keys=True)
  except Exception:  # pylint: disable=broad-except
    return json.dumps({'repr': repr(value)})


def build_dlq_normalizer_transform(
    name: str, input_ref: str, step_label: str) -> dict[str, Any]:
  """Builds a Beam YAML `MapToFields` stage normalizing error rows for Flatten."""
  step_lit = repr(step_label)
  err_type_src = (
      'def normalize_dlq(err_row):\n'
      f"  t = getattr(err_row, 'error_type', None)\n"
      '  if t:\n'
      '    return str(t)\n'
      "  msg = str(getattr(err_row, 'msg', None) or "
      "getattr(err_row, 'error_message', None) or "
      "getattr(err_row, 'error', None) or 'TransformError')\n"
      "  first = msg.splitlines()[0] if msg else ''\n"
      "  head = first.split('(', 1)[0].split(':', 1)[0].strip()\n"
      "  return head if head.isidentifier() else 'TransformError'\n")
  err_msg_src = (
      'def normalize_dlq(err_row):\n'
      "  msg = (getattr(err_row, 'error', None) or "
      "getattr(err_row, 'msg', None) or "
      "getattr(err_row, 'error_message', None) or str(err_row))\n"
      '  return str(msg)[:4096]\n')
  payload_src = (
      'def normalize_dlq(err_row):\n' + _SAFE_JSON_HELPERS_SRC +
      "  raw = getattr(err_row, 'raw_payload', None)\n"
      '  if isinstance(raw, str):\n'
      '    return raw\n'
      "  for attr in ('element', 'failed_row', 'record'):\n"
      '    if hasattr(err_row, attr):\n'
      '      return _safe_json_str(getattr(err_row, attr))\n'
      '  return _safe_json_str(err_row)\n')

  return {
      'name': name,
      'type': 'MapToFields',
      'input': input_ref,
      'config': {
          'language': 'python',
          'fields': {
              'step': {
                  'output_type': 'string',
                  'callable': f'def normalize_dlq(err_row):\n  return {step_lit}\n',
              },
              'error_type': {
                  'output_type': 'string',
                  'callable': err_type_src,
              },
              'error': {
                  'output_type': 'string',
                  'callable': err_msg_src,
              },
              'raw_payload': {
                  'output_type': 'string',
                  'callable': payload_src,
              },
          },
      },
  }


def cli_yaml_provider_spec() -> dict[str, Any]:
  """Provider block enabling built-in pure-Python CLI sources, CDC, & sinks."""
  return {
      'type': 'python',
      'config': {},
      'transforms': {
          'ReadCsvWithDlq': 'apache_beam.cli.dlq.read_csv_with_dlq',
          'ReadJsonWithDlq': 'apache_beam.cli.dlq.read_json_with_dlq',
          'CliCountRows': 'apache_beam.cli.dlq.cli_count_rows',
          'CliWriteToBigQuery': 'apache_beam.cli.dlq.cli_write_to_bigquery',
          'ReadFromDeltaPy': 'apache_beam.cli.lakehouse.cli_read_from_delta',
          'ReadFromDeltaCdcPy': (
              'apache_beam.cli.lakehouse.cli_read_from_delta_cdc'
          ),
          'ReadFromIcebergPy': (
              'apache_beam.cli.lakehouse.cli_read_from_iceberg'
          ),
          'ReadFromIcebergCdcPy': (
              'apache_beam.cli.lakehouse.cli_read_from_iceberg_cdc'
          ),
          'CliWriteToIceberg': 'apache_beam.cli.lakehouse.cli_write_to_iceberg',
          'CliNormalizeCdc': 'apache_beam.cli.cdc.cli_normalize_cdc',
          'CliMaterializeCdc': 'apache_beam.cli.cdc.cli_materialize_cdc',
          'CliRowifyNested': 'apache_beam.cli.dlq.cli_rowify_nested',
          'CliExpect': 'apache_beam.cli.expect.cli_expect',
          'CliWriteToBigQueryCDC': (
              'apache_beam.cli.cdc.cli_write_to_bigquery_cdc'
          ),
          'CliIntervalJoin': (
              'apache_beam.cli.interval_join.cli_interval_join'
          ),
          'IntervalJoin': (
              'apache_beam.cli.interval_join.cli_interval_join'
          ),
      },
  }


# Compressions Beam's CompressionTypes.AUTO handles that we open natively;
# anything else on a local path still goes through FileSystems.
_LOCAL_COMPRESSION_SUFFIXES = ('.gz', '.bz2')
_OTHER_COMPRESSION_SUFFIXES = ('.zst', '.zstd', '.deflate', '.lzma', '.xz',
                               '.snappy')


def _match_paths(path) -> list[str]:
  """Returns the sorted files matching `path` (a file, glob, URI, or list).

  Local paths are matched with `glob` so callers such as the CSV schema
  preflight and the fast local path don't import `apache_beam`. A list or
  tuple is an explicit set of files (one format group of a mixed glob).
  """
  if isinstance(path, (list, tuple)):
    return sorted(str(p) for p in path)
  if '://' not in path:
    import glob
    import os
    if any(ch in path for ch in '*?['):
      return sorted(p for p in glob.glob(path) if os.path.isfile(p))
    return [path] if os.path.isfile(path) else []
  from apache_beam.io.filesystems import FileSystems
  return sorted(m.path for m in FileSystems.match([path])[0].metadata_list)


def _open_auto(path: str):
  """Opens `path` for binary reading, decompressing like Beam's AUTO mode."""
  low = path.lower()
  if '://' not in path and not low.endswith(_OTHER_COMPRESSION_SUFFIXES):
    if low.endswith('.gz'):
      import gzip
      return gzip.open(path, 'rb')
    if low.endswith('.bz2'):
      import bz2
      return bz2.open(path, 'rb')
    return open(path, 'rb')
  from apache_beam.io.filesystem import CompressionTypes
  from apache_beam.io.filesystems import FileSystems
  return FileSystems.open(path, compression_type=CompressionTypes.AUTO)


def _has_leading_zero(v: str) -> bool:
  digits = v[1:] if v.startswith(('+', '-')) else v
  return len(digits) > 1 and digits.startswith('0') and digits.isdigit()


def _is_int_cell(v: str) -> bool:
  digits = v[1:] if v.startswith(('+', '-')) else v
  if len(digits) > 1 and digits.startswith('0') and digits.isdigit():
    return False
  try:
    int(v)
    return True
  except ValueError:
    return False


def _is_float_cell(v: str) -> bool:
  if _is_int_cell(v):
    return True
  digits = v[1:] if v.startswith(('+', '-')) else v
  if len(digits) > 1 and digits.startswith('0') and digits.isdigit():
    return False
  try:
    fval = float(v)
    return not (math.isnan(fval) or math.isinf(fval))
  except ValueError:
    return False


# Cells read as NULL. Like core ReadFromCsv, an empty field is NULL; extra
# markers such as NA or NULL are opt-in (`--null-values`) because they can be
# real values (NA is also North America).
DEFAULT_CSV_NULL_VALUES = frozenset([''])


def _is_null_cell(raw: str, null_values=DEFAULT_CSV_NULL_VALUES) -> bool:
  return raw.strip() == '' or raw in null_values or raw.strip() in null_values


def _infer_scalar_kind(
    samples: list[str],
    *,
    allow_dirty: bool = False,
    null_values=DEFAULT_CSV_NULL_VALUES) -> str:
  """Infers `'int'`, `'float'`, `'bool'`, or `'str'` across non-null cells."""
  non_empty = [s.strip() for s in samples if not _is_null_cell(s, null_values)]
  n = len(non_empty)
  if n == 0:
    return 'str'
  if any(_has_leading_zero(v) for v in non_empty):
    # Zip codes and IDs like "00501": typing the column as int would drop
    # the zeros from every value, so keep it as text.
    return 'str'
  n_bool = sum(1 for v in non_empty if v.lower() in ('true', 'false'))
  n_int = sum(1 for v in non_empty if _is_int_cell(v))
  n_float = sum(1 for v in non_empty if _is_float_cell(v))
  if n_bool == n or (allow_dirty and n >= 3 and n_bool / n >= 0.7):
    return 'bool'
  if n_int == n or (
      allow_dirty and n >= 3 and n_int / n >= 0.7 and n_float == n_int):
    return 'int'
  if n_float == n or (allow_dirty and n >= 3 and n_float / n >= 0.7):
    return 'float'
  return 'str'


def _parse_cell(
    raw: str, kind: str, null_values=DEFAULT_CSV_NULL_VALUES) -> Any:
  if _is_null_cell(raw, null_values):
    return None
  if kind == 'str':
    return raw
  stripped = raw.strip()
  if kind in ('int', 'float') and _has_leading_zero(stripped):
    # The column was typed from a sample without leading zeros; casting would
    # silently turn "00123" into 123. Fail this value instead (DLQ or error).
    raise ValueError(
        f'value {stripped} has a leading zero but the column was typed {kind} '
        'from the first rows; keep it as text with --from-opt '
        'fields=COLUMN:str (or route such rows with --dlq)')
  if kind == 'int':
    return int(stripped)
  if kind == 'float':
    return float(stripped)
  if kind == 'bool':
    low = stripped.lower()
    if low == 'true':
      return True
    if low == 'false':
      return False
    raise ValueError(f'expected boolean value, got {raw!r}')
  return raw


def _preflight_csv_schema(
    path: str,
    delimiter: str = ',',
    sample_rows: int = 100,
    allow_dirty: bool = True,
    null_values=DEFAULT_CSV_NULL_VALUES,
    scan_all_for_leading_zeros: bool = False,
) -> tuple[list[str], dict[str, str]]:
  """Reads header + up to `sample_rows` well-formed rows to lock CSV schema.

  With `scan_all_for_leading_zeros` (small local inputs), every row is also
  checked, and a numeric column with a leading-zero value anywhere ("00123")
  is typed as text so no value loses its zeros.
  """
  matches = _match_paths(path)
  if not matches:
    raise ValueError(f'no CSV files matched {path!r}')

  header: list[str] | None = None
  col_samples: dict[str, list[str]] = {}
  collected = 0

  for match_path in matches:
    with _open_auto(match_path) as raw_stream:
      text_stream = io.TextIOWrapper(
          raw_stream, encoding='utf-8-sig', errors='replace', newline='')
      reader = csv.reader(text_stream, delimiter=delimiter)
      try:
        first_row = next(reader, None)
      except csv.Error:
        continue
      if not first_row:
        continue
      norm = [col.strip() or f'col_{i}' for i, col in enumerate(first_row)]
      order = None
      if header is None:
        header = norm
        col_samples = {c: [] for c in header}
      else:
        order = header_order(norm, header)
      while collected < sample_rows:
        try:
          row = next(reader, None)
        except csv.Error:
          # Skip malformed row during schema inference; runtime DoFn routes it
          # to the DLQ output!
          break
        if row is None:
          break
        if len(row) != len(header):
          continue
        if order is not None:
          row = [row[i] for i in order]
        for col_name, cell in zip(header, row):
          col_samples[col_name].append(cell)
        collected += 1
    if header is not None and collected >= sample_rows:
      break

  if not header:
    raise ValueError(f'CSV source {path!r} is empty or has no header row')
  kinds = {
      c: _infer_scalar_kind(
          col_samples[c], allow_dirty=allow_dirty, null_values=null_values)
      for c in header
  }
  if scan_all_for_leading_zeros:
    numeric = [c for c in header if kinds[c] in ('int', 'float')]
    if numeric:
      for c in _columns_with_leading_zeros(
          matches, header, numeric, delimiter):
        kinds[c] = 'str'
  return header, kinds


def _columns_with_leading_zeros(files, header, columns, delimiter):
  """Columns (of `columns`) with a leading-zero value in any row of `files`."""
  found: set[str] = set()
  for fp in files:
    with _open_auto(fp) as raw_stream:
      reader = csv.reader(
          io.TextIOWrapper(
              raw_stream, encoding='utf-8-sig', errors='replace', newline=''),
          delimiter=delimiter)
      try:
        first = next(reader, None)
      except csv.Error:
        continue
      if not first:
        continue
      norm = [col.strip() or f'col_{i}' for i, col in enumerate(first)]
      positions = {c: norm.index(c) for c in columns if c in norm}
      try:
        for row in reader:
          for c, i in positions.items():
            if c not in found and i < len(row) and _has_leading_zero(
                row[i].strip()):
              found.add(c)
          if len(found) == len(positions):
            break
      except csv.Error:
        pass
  return found


def header_order(file_header: list[str], header: list[str]):
  """Column positions that reorder `file_header` into `header`, or None.

  Files in one glob may list the same columns in a different order; their
  rows are matched by column name. None means same order, or not the same set
  of columns (then rows are read by position, as before).
  """
  if file_header == header or len(file_header) != len(header):
    return None
  if sorted(file_header) != sorted(header) or len(set(header)) != len(header):
    return None
  return [file_header.index(h) for h in header]


_TYPE_KINDS = ('int', 'float', 'bool', 'str')


def apply_type_overrides(
    kinds: dict[str, Any],
    types,
    source: str,
    *,
    allow_new_columns: bool = False,
):
  """Applies explicit column types (`--from-opt fields=amount:float`).

  Values that don't parse as the given type go to the DLQ (or fail the run
  without `--dlq`) instead of the whole column falling back to text. When
  `allow_new_columns=True` (JSONL), explicit fields may also declare sparse
  fields absent from the initial schema sample.
  """
  if not types:
    return kinds
  out = dict(kinds)
  for col, kind in dict(types).items():
    if col not in out and not allow_new_columns:
      raise ValueError(
          f'--from-opt fields: column {col!r} is not in {source} (columns: '
          f'{", ".join(out)})')
    if kind not in _TYPE_KINDS:
      raise ValueError(
          f'--from-opt fields: unsupported type {kind!r} for {col!r}; use '
          'int, float, bool, or str')
    out[col] = kind
  return out


def install_keyword_safe_row_hooks() -> None:
  """Idempotently enables keyword/special column names in Beam Row schemas."""
  import keyword
  import operator
  from apache_beam.typehints import schemas as schemas_mod
  from apache_beam.yaml import yaml_mapping as yaml_map_mod

  st_cls = getattr(schemas_mod, 'SchemaTranslation', None)
  if st_cls is not None and not getattr(st_cls, '_cli_preview_patched', False):
    orig_nt = st_cls.named_tuple_from_schema

    def _make_kw_tuple(type_name: str, subfields) -> type:
      fnames = tuple(name for name, _ in subfields)
      fidx = {name: i for i, name in enumerate(fnames)}
      nfields = len(fnames)

      def _new(cls, *args, **kwargs):
        if args and kwargs:
          if len(args) + len(kwargs) != nfields:
            raise TypeError(f'{type_name}() expected {nfields} args')
          vals = list(args) + [None] * (nfields - len(args))
          for k, v in kwargs.items():
            idx = fidx.get(k)
            if idx is None or idx < len(args):
              raise TypeError(f'{type_name}() unexpected keyword {k!r}')
            vals[idx] = v
          return tuple.__new__(cls, vals)
        if kwargs:
          if len(kwargs) != nfields or set(kwargs) != set(fnames):
            raise TypeError(f'{type_name}() keyword mismatch')
          return tuple.__new__(cls, (kwargs[n] for n in fnames))
        if len(args) != nfields:
          raise TypeError(f'{type_name}() expected {nfields} args')
        return tuple.__new__(cls, args)

      cls = type(
          type_name,
          (tuple, ),
          {
              '__slots__': (),
              '_fields': fnames,
              '__annotations__': dict(subfields),
              '__new__': _new,
              '_asdict': lambda self: dict(zip(fnames, self)),
              '_replace': lambda self, **kw: type(self)(
                  *(kw.get(f, v) for f, v in zip(fnames, self))),
              '__repr__': lambda self: (
                  f'{type_name}(' +
                  ', '.join(f'{f}={v!r}' for f, v in zip(fnames, self)) + ')'),
          })
      for idx, fname in enumerate(fnames):
        if fname not in ('count', 'index') and not fname.startswith('__'):
          setattr(cls, fname, property(operator.itemgetter(idx)))
      return cls

    def _safe_nt(self_st, schema):
      try:
        return orig_nt(self_st, schema)
      except (ValueError, TypeError, SyntaxError):
        from apache_beam import coders
        from apache_beam.typehints import row_type
        type_name = 'BeamSchema_{}'.format(schema.id.replace('-', '_'))
        if schema.id in self_st.schema_registry.by_id:
          return self_st.schema_registry.by_id[schema.id][0]
        subfields = []
        descriptions = {}
        for field in schema.fields:
          f_py = self_st.typing_from_runner_api(field.type)
          if isinstance(f_py, row_type.RowTypeConstraint):
            f_py = f_py.user_type
          descriptions[field.name] = field.description
          subfields.append((field.name, f_py))
        ut = _make_kw_tuple(type_name, subfields)
        setattr(
            ut,
            '__reduce__',
            schemas_mod._named_tuple_reduce_method(schema.SerializeToString()))
        setattr(ut, '_field_descriptions', descriptions)
        setattr(ut, row_type._BEAM_SCHEMA_ID, schema.id)
        self_st.schema_registry.add(ut, schema)
        coders.registry.register_coder(ut, coders.RowCoder)
        return ut

    st_cls.named_tuple_from_schema = _safe_nt
    st_cls._cli_preview_patched = True

  orig_exp = getattr(yaml_map_mod, '_expand_python_mapping_func', None)
  py_call = getattr(yaml_map_mod, 'python_callable', None)
  if (orig_exp is not None and py_call is not None and
      not getattr(orig_exp, '_cli_preview_patched', False)):

    def _safe_exp(
        original_fields, expression=None, callable=None, path=None, name=None):
      if expression and not (path and name):
        if expression in original_fields:
          return py_call.PythonCallableWithSource(
              f'def fn(__row__):\n  return getattr(__row__, {expression!r})')
        safe_f = [
            f for f in original_fields
            if f.isidentifier() and not keyword.iskeyword(f)
        ]
        return orig_exp(
            safe_f,
            expression=expression,
            callable=callable,
            path=path,
            name=name)
      return orig_exp(
          original_fields,
          expression=expression,
          callable=callable,
          path=path,
          name=name)

    _safe_exp._cli_preview_patched = True
    yaml_map_mod._expand_python_mapping_func = _safe_exp

  _install_nested_row_attr_inference()


def _nested_row_plan(fields) -> dict[str, Any]:
  """Maps each field to a sub-plan if it is a (nullable) nested row, else None.

  Used by `cli_rowify_nested` to know which values must become rows.
  """
  import typing
  from apache_beam.typehints import native_type_compatibility as ntc
  from apache_beam.typehints import schemas
  from apache_beam.typehints.row_type import RowTypeConstraint
  plan: dict[str, Any] = {}
  for name, typ in fields:
    args = [a for a in typing.get_args(typ) if a is not type(None)]
    inner = args[0] if len(args) == 1 else typ
    is_row = isinstance(inner, RowTypeConstraint) or ntc.match_is_named_tuple(
        inner)
    plan[name] = (
        _nested_row_plan(schemas.named_fields_from_element_type(inner))
        if is_row else None)
  return plan


def _rowify_value(value: Any, plan: dict[str, Any] | None) -> Any:
  """Builds a beam.Row from a dict or row-like `value` following `plan`."""
  import apache_beam as beam
  if value is None or plan is None:
    return value
  get = value.get if isinstance(value, dict) else (
      lambda n: getattr(value, n, None))
  return beam.Row(**{n: _rowify_value(get(n), sub) for n, sub in plan.items()})


def cli_rowify_nested():
  """Converts nested struct values to rows that match the declared schema.

  `beam pipe` adds this right after a Parquet or Avro source when `--where` or
  `--select` is used. Beam's `ReadFromParquet(as_rows=True)` declares struct
  columns as nested rows but yields plain dicts for them (apache/beam#41100), so
  an expression such as `address.city` fails at runtime with
  "'dict' object has no attribute 'city'". This is a no-op when the schema has
  no nested rows. Structs inside lists are left unchanged.
  """
  import apache_beam as beam
  from apache_beam.typehints import schemas

  class _RowifyNested(beam.PTransform):
    def expand(self, pcoll):
      try:
        fields = schemas.named_fields_from_element_type(pcoll.element_type)
      except Exception:  # pylint: disable=broad-except
        return pcoll
      plan = _nested_row_plan(fields)
      if not any(sub is not None for sub in plan.values()):
        return pcoll
      return pcoll | 'RowifyNested' >> beam.Map(_rowify_value, plan).with_output_types(
          pcoll.element_type)

  return _RowifyNested()


def _install_nested_row_attr_inference() -> None:
  """Lets type inference see through nested rows, e.g. `user.id`.

  Called from `install_keyword_safe_row_hooks`. Released Beam's
  `opcodes._getattr` only resolves fields on a RowTypeConstraint, so
  `--select "user.id AS user_id"` on a nested JSON/Parquet field is inferred as
  Any and WriteToParquet fails at construction. This mirrors the core fix
  proposed upstream (apache/beam#41099): resolve
  NamedTuple-typed rows and Optional[row] fields.
  Idempotent, and a no-op if the installed Beam already resolves them.
  """
  import inspect
  from apache_beam.typehints import native_type_compatibility as ntc
  from apache_beam.typehints import opcodes
  from apache_beam.typehints import row_type
  from apache_beam.typehints import typehints

  orig = opcodes._getattr
  if getattr(orig, '_cli_preview_patched', False):
    return

  def _row_type_of(t):
    if isinstance(t, row_type.RowTypeConstraint):
      return t
    if inspect.isclass(t) and ntc.match_is_named_tuple(t):
      return row_type.RowTypeConstraint.from_user_type(t)
    return None

  def _nullable_row(t):
    if not isinstance(t, typehints.UnionConstraint):
      return None
    non_none = [u for u in t.union_types if u is not type(None)]
    return _row_type_of(non_none[0]) if len(non_none) == 1 else None

  def _getattr(o, name):
    result = orig(o, name)
    if result is not typehints.Any:
      return result
    row = None
    if isinstance(o, row_type.RowTypeConstraint) or (
        inspect.isclass(o) and ntc.match_is_named_tuple(o)):
      row = _row_type_of(o)
      nullable = False
    else:
      row = _nullable_row(ntc.convert_to_beam_type(o))
      nullable = True
    if row is None or name not in dict(row._fields):
      return result
    field_type = row.get_type_for(name)
    if not nullable:
      return field_type
    return typehints.Optional[ntc.convert_to_beam_type(field_type)]

  _getattr._cli_preview_patched = True
  opcodes._getattr = _getattr


def read_csv_with_dlq(
    path: str,
    *,
    delimiter: str = ',',
    null_values: Optional[list[str]] = None,
    types: Optional[dict[str, str]] = None,
    error_handling: Optional[dict[str, Any]] = None,
):
  """RFC-4180 multi-shard CSV Beam YAML source with structured DLQ output.

  Empty fields, and any `null_values`, are read as NULL.
  """
  import apache_beam as beam
  from apache_beam.io import fileio
  from apache_beam.metrics import Metrics
  from apache_beam.typehints.row_type import RowTypeConstraint

  install_keyword_safe_row_hooks()
  nulls = DEFAULT_CSV_NULL_VALUES | frozenset(null_values or ())
  header, kinds = _preflight_csv_schema(
      path, delimiter=delimiter, null_values=nulls)
  kinds = apply_type_overrides(kinds, types, path)
  py_type_map = {
      'int': Optional[int],
      'float': Optional[float],
      'bool': Optional[bool],
      'str': Optional[str],
  }
  good_row_type = RowTypeConstraint.from_fields([
      (col, py_type_map[kinds[col]]) for col in header
  ])
  dlq_row_type = RowTypeConstraint.from_fields([
      ('step', str),
      ('error_type', str),
      ('error', str),
      ('raw_payload', str),
  ])
  err_tag = (error_handling or {}).get('output', 'errors')

  route_errors = bool(error_handling and 'output' in error_handling)

  class _ParseCsvShardDoFn(beam.DoFn):
    def __init__(self):
      self._rows_read = Metrics.counter('beam_cli', 'rows_read')
      self._dlq_rows = Metrics.counter('beam_cli', 'source_dlq_rows')

    def process(self, readable_file):
      file_path = readable_file.metadata.path
      with readable_file.open() as raw_f:
        for parsed, err in iter_csv_records(
            file_path, raw_f, header, kinds, delimiter, nulls):
          self._rows_read.inc()
          if err is None:
            yield beam.Row(**parsed)
            continue
          if not route_errors:
            raise ValueError(csv_fail_fast_message(err))
          self._dlq_rows.inc()
          yield beam.pvalue.TaggedOutput('errors', beam.Row(**err))

  class _ReadCsvWithDlqTransform(beam.PTransform):
    def expand(self, pbegin):
      outputs = (
          pbegin
          | 'MatchFiles' >> fileio.MatchFiles(path)
          | 'Reshuffle' >> beam.Reshuffle()
          | 'ReadMatches' >> fileio.ReadMatches()
          | 'ParseShards' >> beam.ParDo(_ParseCsvShardDoFn()).with_outputs(
              'errors', main='good'))
      good = outputs['good']
      bad = outputs['errors']
      good.element_type = good_row_type
      bad.element_type = dlq_row_type
      if route_errors:
        return {'good': good, err_tag: bad}
      return good

  return _ReadCsvWithDlqTransform()


def iter_csv_records(
    file_path,
    raw_f,
    header,
    kinds,
    delimiter=',',
    null_values=DEFAULT_CSV_NULL_VALUES):
  """Parses one CSV file into `(row_dict, None)` or `(None, dlq_record)`.

  Shared by the `ReadCsvWithDlq` DoFn and the fast local path so both apply
  the same header handling, typing, and DLQ records. Every yielded item is
  one record read; a header row matching `header` and blank lines are skipped.
  """
  wrapper = io.TextIOWrapper(
      raw_f, encoding='utf-8-sig', errors='replace', newline='')
  reader = csv.reader(wrapper, delimiter=delimiter)
  row_index = 0
  order = None
  while True:
    try:
      cells = next(reader)
    except StopIteration:
      break
    except csv.Error as exc:
      yield None, {
          'step': 'ReadCsvWithDlq',
          'error_type': 'CsvSyntaxError',
          'error': str(exc),
          'raw_payload': safe_json_dumps({
              'file': file_path,
              'row_index': row_index,
          }),
      }
      break

    if row_index == 0:
      norm = [c.strip() or f'col_{i}' for i, c in enumerate(cells)]
      order = header_order(norm, header)
      if norm == header or order is not None:
        row_index += 1
        continue
    row_index += 1
    if not cells or (len(cells) == 1 and cells[0] == '' and len(header) > 1):
      continue

    if len(cells) != len(header):
      yield None, {
          'step': 'ReadCsvWithDlq',
          'error_type': 'ColumnCountMismatch',
          'error': f'expected {len(header)} columns, got {len(cells)}',
          'raw_payload': safe_json_dumps({
              'file': file_path,
              'row_index': row_index,
              'cells': cells,
          }),
      }
      continue

    if order is not None:
      cells = [cells[i] for i in order]
    parsed_vals: dict[str, Any] = {}
    conv_err: Exception | None = None
    bad_col = ''
    for col_name, raw_cell in zip(header, cells):
      try:
        parsed_vals[col_name] = _parse_cell(
            raw_cell, kinds[col_name], null_values)
      except Exception as exc:  # pylint: disable=broad-except
        conv_err = exc
        bad_col = col_name
        break
    if conv_err is not None:
      yield None, {
          'step': 'ReadCsvWithDlq',
          'error_type': type(conv_err).__name__,
          'error': f'column {bad_col!r}: {conv_err}',
          'raw_payload': safe_json_dumps(dict(zip(header, cells))),
      }
      continue
    yield parsed_vals, None


def csv_fail_fast_message(err: dict[str, Any], step: str = '') -> str:
  """The error for a bad CSV record when no `--dlq` is configured."""
  return (
      f"{err['error_type']}: {err['error']}; CSV record: "
      f"{err['raw_payload'][:200]}{step} (pass --dlq PATH to route bad "
      'records instead of failing)')


MAX_JSON_NESTING_DEPTH = 32


def _infer_json_kind(values: list[Any], depth: int = 0) -> Any:
  """Infers the schema "kind" of one JSON field from sampled non-null values.

  Used by `read_json_with_dlq` at pipeline construction. A kind is either a
  scalar name ('bool', 'int', 'float', 'str', 'json') or, for JSON objects, a
  dict mapping field names to kinds. Kinds are plain strings/dicts rather than
  typing objects so the parsing DoFn can compare them after it is pickled to a
  worker (typing objects lose identity when unpickled). Mixed numeric/string
  fields follow the CSV reader's rule: if at least 70% of 3+ samples are
  numeric the field is numeric and the other values go to the DLQ.
  """
  if depth > MAX_JSON_NESTING_DEPTH:
    return 'json'
  n = len(values)
  if n == 0:
    return 'str'
  objects = [v for v in values if isinstance(v, dict)]
  if objects and len(objects) == n:
    keys: dict[str, None] = {}
    for obj in objects:
      keys.update(dict.fromkeys(obj))
    if not keys:
      return 'json'
    return {
        k: _infer_json_kind(
            [o[k] for o in objects if o.get(k) is not None], depth + 1)
        for k in keys
    }
  if objects or any(isinstance(v, list) for v in values):
    return 'json'
  n_bool = sum(1 for v in values if isinstance(v, bool))
  n_int = sum(1 for v in values if isinstance(v, int) and not isinstance(v, bool))
  n_num = n_int + sum(1 for v in values if isinstance(v, float))
  dirty_ok = n >= 3
  if n_bool == n or (dirty_ok and n_bool / n >= 0.7):
    return 'bool'
  if n_int == n or (dirty_ok and n_int / n >= 0.7 and n_num == n_int):
    return 'int'
  if n_num == n or (dirty_ok and n_num / n >= 0.7):
    return 'float'
  return 'str'


def _json_kind_type(kind: Any) -> Any:
  """Maps a kind from `_infer_json_kind` to a nullable Beam field type."""
  from apache_beam.typehints.row_type import RowTypeConstraint
  if isinstance(kind, dict):
    return Optional[RowTypeConstraint.from_fields([
        (name, _json_kind_type(sub)) for name, sub in kind.items()
    ])]
  return {
      'bool': Optional[bool],
      'int': Optional[int],
      'float': Optional[float],
  }.get(kind, Optional[str])


class SchemaDriftError(ValueError):
  """A JSON record contains field(s) absent from the sampled schema."""

  def __init__(self, message: str, extra_fields: Iterable[str] = ()):
    super().__init__(message)
    self.extra_fields = [str(f) for f in extra_fields]


def parse_sample_rows_opt(raw: Any) -> Optional[int]:
  """Parses `--from-opt sample_rows=all|<INT>` (default `100`, `None` = all)."""
  if raw is None or raw == '':
    return 100
  if isinstance(raw, bool):
    raise ValueError(
        "--from-opt sample_rows must be a positive integer or 'all'")
  s = str(raw).strip().lower()
  if s == 'all':
    return None
  try:
    n = int(s)
  except ValueError:
    raise ValueError(
        f"--from-opt sample_rows={raw!r}: expected a positive integer or 'all'"
    ) from None
  if n < 1:
    raise ValueError(
        f"--from-opt sample_rows={raw!r}: expected a positive integer or 'all'")
  return n


JSON_AUTO_SCAN_MAX_BYTES = 64 * 1024 * 1024


def should_scan_all_local_json(path_or_files) -> bool:
  """True when `path_or_files` are local files whose total size <= 64 MB."""
  raw_cap = os.environ.get('BEAM_CLI_JSON_AUTO_SCAN_MAX_BYTES', '').strip()
  try:
    cap = int(raw_cap) if raw_cap else JSON_AUTO_SCAN_MAX_BYTES
  except ValueError:
    cap = JSON_AUTO_SCAN_MAX_BYTES
  if isinstance(path_or_files, str):
    if '://' in path_or_files or '{{' in path_or_files:
      return False
    files = _match_paths(path_or_files)
  else:
    files = list(path_or_files)
  if not files or any('://' in str(f) for f in files):
    return False
  try:
    return sum(os.path.getsize(f) for f in files) <= cap
  except OSError:
    return False


def resolve_json_sample_rows(
    path_or_files, raw_sample_rows: Any = None) -> Optional[int]:
  """Effective `sample_rows`: explicit user value, else full scan if local <=64MB."""
  if raw_sample_rows is not None and raw_sample_rows != '':
    return parse_sample_rows_opt(raw_sample_rows)
  if should_scan_all_local_json(path_or_files):
    return None
  return 100


def parse_unknown_fields_opt(raw: Any) -> str:
  """Parses `--from-opt on_unknown_fields=error|ignore` (default `'error'`)."""
  if raw is None or raw == '':
    return 'error'
  s = str(raw).strip().lower()
  if s in ('error', 'ignore'):
    return s
  raise ValueError(
      f"--from-opt on_unknown_fields={raw!r}: expected 'error' or 'ignore'")


def schema_drift_warning(count: int, extra_fields: Iterable[str] = ()) -> str:
  """Warning emitted when `--dlq` quarantines records with new JSON fields."""
  safe_fields = [
      f for f in sorted({str(x) for x in extra_fields if x})
      if re.fullmatch(r'[A-Za-z_][A-Za-z0-9_.]*', f)
  ]
  detail = f" ({', '.join(safe_fields[:8])})" if safe_fields else ''
  return (
      f'{count} JSON record(s) contained new field(s){detail} absent from '
      'the sampled schema and were quarantined to --dlq for review instead of '
      'dropping fields silently; pass --from-opt sample_rows=all (or '
      '--from-opt fields=COL:TYPE) to include them in the output schema, or '
      '--from-opt on_unknown_fields=ignore to drop extra fields')


def _coerce_json_value(
    value: Any,
    kind: Any,
    path: str,
    row_factory=None,
    *,
    allow_unknown_fields: bool = False,
    depth: int = 0,
) -> Any:
  """Converts one parsed JSON value to `kind`, raising ValueError on mismatch.

  Runs inside the parsing DoFn *before* the row is yielded: a value that does
  not match the schema would otherwise fail later in the row coder, outside
  the DoFn's try block, where it can't be routed to the DLQ. `row_factory`
  builds nested rows from keyword arguments (default: `beam.Row`).
  """
  if depth > MAX_JSON_NESTING_DEPTH:
    raise ValueError(
        f'field {path!r}: JSON nesting exceeds maximum depth '
        f'({MAX_JSON_NESTING_DEPTH})')
  if value is None:
    return None
  if isinstance(kind, dict):
    if not isinstance(value, dict):
      raise ValueError(
          f'field {path!r}: expected a JSON object, got {type(value).__name__}')
    if not allow_unknown_fields:
      extra = [k for k in value if k not in kind]
      if extra:
        qualified = [f'{path}.{k}' for k in extra]
        shown = ', '.join(repr(q) for q in qualified[:8])
        known = ', '.join(kind)
        raise SchemaDriftError(
            f'new field(s) {shown} appeared after schema sampling '
            f'(inferred fields for {path!r}: {known})',
            extra_fields=qualified)
    if row_factory is None:
      import apache_beam as beam
      row_factory = beam.Row
    return row_factory(
        **{
            name: _coerce_json_value(
                value.get(name),
                sub,
                f'{path}.{name}',
                row_factory,
                allow_unknown_fields=allow_unknown_fields,
                depth=depth + 1)
            for name, sub in kind.items()
        })
  if kind == 'bool':
    if isinstance(value, bool):
      return value
  elif kind == 'int':
    if isinstance(value, int) and not isinstance(value, bool):
      return value
  elif kind == 'float':
    if isinstance(value, (int, float)) and not isinstance(value, bool):
      return float(value)
  else:  # 'str' or 'json'
    return value if isinstance(value, str) else json.dumps(value)
  raise ValueError(
      f'field {path!r}: expected {kind}, got {type(value).__name__} {value!r}')


def read_json_with_dlq(
    path: str,
    *,
    types: Optional[dict[str, str]] = None,
    sample_rows: Any = None,
    on_unknown_fields: str = 'error',
    error_handling: Optional[dict[str, Any]] = None,
):
  """JSONL Beam YAML source with a sampled schema and an optional DLQ.

  The schema is inferred from all records on local files <= 64 MB (or up to
  `sample_rows` valid objects when set or on larger/remote inputs; `'all'`
  scans every record); nested objects become nested rows so
  `--where`/`--select` can use `user.id`. Any record with fields absent from
  the inferred schema raises `SchemaDriftError` (routed to `error_handling`
  when `--dlq` is set, or failing fast otherwise) unless
  `on_unknown_fields='ignore'`.
  """
  import apache_beam as beam
  from apache_beam.io import ReadFromText
  from apache_beam.metrics import Metrics
  from apache_beam.typehints.row_type import RowTypeConstraint

  eff_sample = resolve_json_sample_rows(path, sample_rows)
  allow_unknown = (parse_unknown_fields_opt(on_unknown_fields) == 'ignore')
  kinds = apply_type_overrides(
      infer_json_kinds(path, sample_rows=eff_sample),
      types,
      path,
      allow_new_columns=True)
  install_keyword_safe_row_hooks()
  good_row_type = RowTypeConstraint.from_fields([
      (name, _json_kind_type(kind)) for name, kind in kinds.items()
  ])
  dlq_row_type = RowTypeConstraint.from_fields([
      ('step', str),
      ('error_type', str),
      ('error', str),
      ('raw_payload', str),
  ])
  route_errors = bool(error_handling and 'output' in error_handling)
  err_tag = (error_handling or {}).get('output', 'errors')

  class _ParseJsonLineDoFn(beam.DoFn):
    def __init__(self):
      self._rows_read = Metrics.counter('beam_cli', 'rows_read')
      self._dlq_rows = Metrics.counter('beam_cli', 'source_dlq_rows')
      self._drift_rows = Metrics.counter('beam_cli', 'schema_drift_rows')

    def process(self, line: str):
      stripped = line.strip()
      if not stripped:
        return
      self._rows_read.inc()
      try:
        row = parse_json_record(
            stripped, kinds, beam.Row, allow_unknown_fields=allow_unknown)
      except Exception as exc:  # pylint: disable=broad-except
        if not route_errors:
          raise ValueError(json_fail_fast_message(stripped, exc)) from exc
        self._dlq_rows.inc()
        if isinstance(exc, SchemaDriftError):
          self._drift_rows.inc()
        yield beam.pvalue.TaggedOutput(
            'errors', beam.Row(**json_dlq_record(stripped, exc)))
        return
      yield row

  class _ReadJsonWithDlqTransform(beam.PTransform):
    def expand(self, pbegin):
      outputs = (
          pbegin
          | 'ReadLines' >> ReadFromText(path)
          | 'ParseJson' >> beam.ParDo(_ParseJsonLineDoFn()).with_outputs(
              'errors', main='good'))
      good = outputs['good']
      bad = outputs['errors']
      good.element_type = good_row_type
      bad.element_type = dlq_row_type
      if route_errors:
        return {'good': good, err_tag: bad}
      return good

  return _ReadJsonWithDlqTransform()


def infer_json_kinds(
    path: str | list[str], *, sample_rows: Optional[int] = 100
) -> dict[str, Any]:
  """Infers field kinds from up to `sample_rows` valid JSON objects (`None` = all)."""
  matches = _match_paths(path)
  if not matches:
    raise ValueError(f'no JSON files matched {path!r}')

  samples: list[dict[str, Any]] = []
  for match_path in matches:
    with _open_auto(match_path) as raw_s, io.TextIOWrapper(
        raw_s, encoding='utf-8', errors='replace') as text_s:
      for raw_line in text_s:
        line = raw_line.strip()
        if not line:
          continue
        try:
          obj = json.loads(line)
        except ValueError:
          continue
        if isinstance(obj, dict):
          samples.append(obj)
        if sample_rows is not None and len(samples) >= sample_rows:
          break
    if sample_rows is not None and len(samples) >= sample_rows:
      break

  if not samples:
    raise ValueError(
        f'could not infer JSON object schema from {path!r} (no valid objects)')
  kinds = _infer_json_kind(samples)
  if not isinstance(kinds, dict):
    raise ValueError(
        f'could not infer JSON object schema from {path!r} (objects have no '
        'fields)')
  return kinds


def parse_json_record(
    stripped: str,
    kinds: dict[str, Any],
    row_factory,
    *,
    allow_unknown_fields: bool = False,
):
  """Parses one stripped JSONL line into a row, raising on any mismatch."""
  obj = json.loads(stripped)
  if not isinstance(obj, dict):
    raise ValueError('expected JSON object per line')
  if not allow_unknown_fields:
    extra = [k for k in obj if k not in kinds]
    if extra:
      shown = ', '.join(repr(k) for k in extra[:8])
      known = ', '.join(kinds)
      raise SchemaDriftError(
          f'new field(s) {shown} appeared after schema sampling '
          f'(inferred fields: {known})',
          extra_fields=extra)
  return row_factory(
      **{
          name: _coerce_json_value(
              obj.get(name),
              kind,
              name,
              row_factory,
              allow_unknown_fields=allow_unknown_fields)
          for name, kind in kinds.items()
      })


def json_dlq_record(stripped: str, exc: Exception) -> dict[str, Any]:
  """The DLQ record for a JSONL line that failed `parse_json_record`."""
  return {
      'step': 'ReadJsonWithDlq',
      'error_type': type(exc).__name__,
      'error': str(exc),
      'raw_payload': safe_json_dumps({'raw_line': stripped[:4096]}),
  }


def json_fail_fast_message(
    stripped: str, exc: Exception, step: str = '') -> str:
  """The error for a bad JSONL line when no `--dlq` is configured."""
  if isinstance(exc, SchemaDriftError):
    return (
        f'{type(exc).__name__}: {exc} in JSON line {stripped[:200]!r}{step} '
        '(pass --dlq PATH to quarantine these records for review, '
        '--from-opt sample_rows=all to scan all records for schema, '
        '--from-opt fields=COL:TYPE to declare sparse fields, or '
        '--from-opt on_unknown_fields=ignore to drop extra fields)')
  return (
      f'{type(exc).__name__}: {exc} in JSON line {stripped[:200]!r}{step} '
      '(pass --dlq PATH to route bad records instead of failing)')


def cli_count_rows(counter: str = 'rows_read'):
  """Fusion-safe pass-through row counter preserving exact `element_type`."""
  import apache_beam as beam
  from apache_beam.metrics import Metrics

  class _CountDoFn(beam.DoFn):
    def __init__(self):
      self._counter = Metrics.counter('beam_cli', counter)

    def process(self, element):
      self._counter.inc()
      yield element

  class _CliCountRowsTransform(beam.PTransform):
    def expand(self, pcoll):
      elem_type = pcoll.element_type
      return pcoll | f'Count_{counter}' >> beam.ParDo(
          _CountDoFn()).with_output_types(elem_type)

  return _CliCountRowsTransform()


def _beam_row_type_to_bq_schema_dict(element_type) -> dict[str, Any]:
  from apache_beam.portability.api import schema_pb2
  from apache_beam.typehints import schemas

  field_type = schemas.typing_to_runner_api(element_type)
  if not field_type.HasField('row_type'):
    raise ValueError(
        f'BigQuery sink requires a schema-aware Row input, got {element_type}')

  atomic_map = {
      schema_pb2.BYTE: 'INTEGER',
      schema_pb2.INT16: 'INTEGER',
      schema_pb2.INT32: 'INTEGER',
      schema_pb2.INT64: 'INTEGER',
      schema_pb2.FLOAT: 'FLOAT',
      schema_pb2.DOUBLE: 'FLOAT',
      schema_pb2.BOOLEAN: 'BOOLEAN',
      schema_pb2.STRING: 'STRING',
      schema_pb2.BYTES: 'BYTES',
  }
  fields = []
  for f in field_type.row_type.schema.fields:
    ft = f.type
    mode = 'NULLABLE' if ft.nullable else 'REQUIRED'
    bq_t = 'STRING'
    if ft.WhichOneof('type_info') == 'atomic_type':
      bq_t = atomic_map.get(ft.atomic_type, 'STRING')
    elif ft.WhichOneof('type_info') == 'logical_type':
      urn = ft.logical_type.urn
      if 'decimal' in urn:
        bq_t = 'NUMERIC'
      elif 'millis_instant' in urn or 'micros_instant' in urn or 'timestamp' in urn:
        bq_t = 'TIMESTAMP'
    elif ft.WhichOneof('type_info') == 'array_type':
      mode = 'REPEATED'
      elem_ft = ft.array_type.element_type
      if elem_ft.WhichOneof('type_info') == 'atomic_type':
        bq_t = atomic_map.get(elem_ft.atomic_type, 'STRING')
    # Streaming inserts reject REQUIRED when nullable cells appear; NULLABLE is
    # safe for auto-created tables unless REPEATED.
    if mode == 'REQUIRED':
      mode = 'NULLABLE'
    fields.append({'name': f.name, 'type': bq_t, 'mode': mode})
  return {'fields': fields}


def cli_write_to_bigquery(
    table: str,
    *,
    create_disposition: str = 'CREATE_IF_NEEDED',
    write_disposition: str = 'WRITE_EMPTY',
    error_handling: Optional[dict[str, Any]] = None,
):
  """Pure-Python BigQuery sink (zero Java boot; supports STREAMING & FILE_LOADS)."""
  import apache_beam as beam
  from apache_beam.io.gcp.bigquery import WriteToBigQuery
  from apache_beam.metrics import Metrics
  from apache_beam.options.pipeline_options import GoogleCloudOptions
  from apache_beam.typehints.row_type import RowTypeConstraint

  err_tag = (error_handling or {}).get('output', 'errors')
  dlq_row_type = RowTypeConstraint.from_fields([
      ('step', str),
      ('error_type', str),
      ('error', str),
      ('raw_payload', str),
  ])

  class _RowToBqDictDoFn(beam.DoFn):
    def process(self, element):
      d = element._asdict() if hasattr(element, '_asdict') else dict(element)
      out = {}
      for k, v in d.items():
        if isinstance(v, decimal.Decimal):
          out[k] = str(v)
        elif hasattr(v, 'to_rfc3339'):
          out[k] = v.to_rfc3339()
        elif hasattr(v, 'isoformat'):
          out[k] = v.isoformat()
        else:
          out[k] = v
      yield out

  class _FormatBqErrorDoFn(beam.DoFn):
    def __init__(self):
      self._sink_dlq = Metrics.counter('beam_cli', 'sink_dlq_rows')

    def process(self, bq_err):
      self._sink_dlq.inc()
      # BigQuery STREAMING_INSERTS failed_rows_with_errors yields
      # `(destination, row_dict, errors_list)`.
      if isinstance(bq_err, tuple) and len(bq_err) >= 3:
        _, row_dict, err_list = bq_err[0], bq_err[1], bq_err[2]
        err_str = json.dumps(err_list)
      else:
        row_dict = getattr(bq_err, 'row', bq_err)
        err_str = str(getattr(bq_err, 'error_message', bq_err))
      yield beam.Row(
          step='WriteToBigQuery',
          error_type='BigQueryWriteError',
          error=err_str[:4096],
          raw_payload=safe_json_dumps(row_dict))

  class _CliWriteBqTransform(beam.PTransform):
    def expand(self, pcoll):
      bq_schema = _beam_row_type_to_bq_schema_dict(pcoll.element_type)
      temp_loc = pcoll.pipeline.options.view_as(GoogleCloudOptions).temp_location

      if write_disposition == 'WRITE_TRUNCATE' and not temp_loc:
        raise ValueError(
            'BigQuery --if-exists=replace (WRITE_TRUNCATE) requires '
            '--temp_location=gs://<bucket>/tmp for atomic file loads')

      if write_disposition == 'WRITE_EMPTY' and (not temp_loc or error_handling):
        from google.api_core import exceptions as gcp_exceptions
        from google.cloud import bigquery as gcp_bq
        from . import uris
        _, proj, ds, tbl_name = uris.normalize_bq_table_spec(
            table,
            default_project=pcoll.pipeline.options.view_as(
                GoogleCloudOptions).project)
        client = gcp_bq.Client(project=proj)
        fq = f'{proj or client.project}.{ds}.{tbl_name}'
        try:
          existing_tbl = client.get_table(fq)
          if (existing_tbl.num_rows or 0) > 0:
            raise ValueError(
                f'BigQuery table {fq!r} already exists and has '
                f'{existing_tbl.num_rows} rows; pass --if-exists=append to add '
                'rows or choose a new destination table')
        except gcp_exceptions.NotFound:
          pass

      use_streaming = bool(error_handling) or not temp_loc
      method = (
          WriteToBigQuery.Method.STREAMING_INSERTS
          if use_streaming else WriteToBigQuery.Method.FILE_LOADS)
      effective_disp = (
          'WRITE_APPEND'
          if (use_streaming and write_disposition == 'WRITE_EMPTY') else
          write_disposition)

      dicts = pcoll | 'ToBqDict' >> beam.ParDo(_RowToBqDictDoFn())
      write_res = dicts | 'WriteBq' >> WriteToBigQuery(
          table,
          schema=bq_schema,
          create_disposition=create_disposition,
          write_disposition=effective_disp,
          method=method)

      if error_handling and 'output' in error_handling and use_streaming:
        errs = (
            write_res.failed_rows_with_errors
            | 'FormatBqErrors' >>
            beam.ParDo(_FormatBqErrorDoFn()).with_output_types(dlq_row_type))
        return {
            'post_write': errs | beam.FlatMap(lambda _: ()),
            err_tag: errs,
        }
      return None

  return _CliWriteBqTransform()

