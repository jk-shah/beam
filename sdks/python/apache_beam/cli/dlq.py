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
from typing import Any
from typing import Optional

# Transforms in Beam YAML's standard library that accept `error_handling` in
# their `config:` block.
TRANSFORMS_SUPPORTING_ERROR_HANDLING = frozenset([
    'Filter',
    'MapToFields',
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
      "  head = msg.splitlines()[0].split(':', 1)[0].strip()\n"
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
          'CliWriteToBigQueryCDC': (
              'apache_beam.cli.cdc.cli_write_to_bigquery_cdc'
          ),
      },
  }


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


def _infer_scalar_kind(samples: list[str], *, allow_dirty: bool = False) -> str:
  """Infers `'int'`, `'float'`, `'bool'`, or `'str'` across non-empty cells."""
  non_empty = [s.strip() for s in samples if s.strip() != '']
  n = len(non_empty)
  if n == 0:
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


def _parse_cell(raw: str, kind: str) -> Any:
  if kind == 'str':
    return raw
  stripped = raw.strip()
  if stripped == '':
    return None
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
) -> tuple[list[str], dict[str, str]]:
  """Reads header + up to `sample_rows` well-formed rows to lock CSV schema."""
  from apache_beam.io.filesystem import CompressionTypes
  from apache_beam.io.filesystems import FileSystems

  matches = FileSystems.match([path])[0].metadata_list
  if not matches:
    raise ValueError(f'no CSV files matched {path!r}')

  header: list[str] | None = None
  col_samples: dict[str, list[str]] = {}
  collected = 0

  for meta in sorted(matches, key=lambda m: m.path):
    with FileSystems.open(
        meta.path, compression_type=CompressionTypes.AUTO) as raw_stream:
      text_stream = io.TextIOWrapper(
          raw_stream, encoding='utf-8-sig', errors='replace', newline='')
      reader = csv.reader(text_stream, delimiter=delimiter)
      try:
        first_row = next(reader, None)
      except csv.Error:
        continue
      if not first_row:
        continue
      if header is None:
        header = [col.strip() or f'col_{i}' for i, col in enumerate(first_row)]
        col_samples = {c: [] for c in header}
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
        for col_name, cell in zip(header, row):
          col_samples[col_name].append(cell)
        collected += 1
    if header is not None and collected >= sample_rows:
      break

  if not header:
    raise ValueError(f'CSV source {path!r} is empty or has no header row')
  kinds = {
      c: _infer_scalar_kind(col_samples[c], allow_dirty=allow_dirty)
      for c in header
  }
  return header, kinds


def read_csv_with_dlq(
    path: str,
    *,
    delimiter: str = ',',
    error_handling: Optional[dict[str, Any]] = None,
):
  """RFC-4180 multi-shard CSV Beam YAML source with structured DLQ output."""
  import apache_beam as beam
  from apache_beam.io import fileio
  from apache_beam.metrics import Metrics
  from apache_beam.typehints.row_type import RowTypeConstraint

  header, kinds = _preflight_csv_schema(path, delimiter=delimiter)
  py_type_map = {
      'int': Optional[int],
      'float': Optional[float],
      'bool': Optional[bool],
      'str': str,
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

  class _ParseCsvShardDoFn(beam.DoFn):
    def __init__(self):
      self._rows_read = Metrics.counter('beam_cli', 'rows_read')
      self._dlq_rows = Metrics.counter('beam_cli', 'source_dlq_rows')

    def process(self, readable_file):
      file_path = readable_file.metadata.path
      with readable_file.open() as raw_f:
        wrapper = io.TextIOWrapper(
            raw_f, encoding='utf-8-sig', errors='replace', newline='')
        reader = csv.reader(wrapper, delimiter=delimiter)
        row_index = 0
        while True:
          try:
            cells = next(reader)
          except StopIteration:
            break
          except csv.Error as exc:
            self._rows_read.inc()
            self._dlq_rows.inc()
            yield beam.pvalue.TaggedOutput(
                'errors',
                beam.Row(
                    step='ReadCsvWithDlq',
                    error_type='CsvSyntaxError',
                    error=str(exc),
                    raw_payload=safe_json_dumps({
                        'file': file_path,
                        'row_index': row_index,
                    })))
            break

          if row_index == 0 and [
              c.strip() or f'col_{i}' for i, c in enumerate(cells)
          ] == header:
            row_index += 1
            continue
          row_index += 1
          if not cells or (len(cells) == 1 and cells[0] == '' and
                           len(header) > 1):
            continue

          self._rows_read.inc()
          if len(cells) != len(header):
            self._dlq_rows.inc()
            yield beam.pvalue.TaggedOutput(
                'errors',
                beam.Row(
                    step='ReadCsvWithDlq',
                    error_type='ColumnCountMismatch',
                    error=(
                        f'expected {len(header)} columns, got {len(cells)}'),
                    raw_payload=safe_json_dumps({
                        'file': file_path,
                        'row_index': row_index,
                        'cells': cells,
                    })))
            continue

          parsed_vals: dict[str, Any] = {}
          conv_err: Exception | None = None
          bad_col = ''
          for col_name, raw_cell in zip(header, cells):
            try:
              parsed_vals[col_name] = _parse_cell(raw_cell, kinds[col_name])
            except Exception as exc:  # pylint: disable=broad-except
              conv_err = exc
              bad_col = col_name
              break
          if conv_err is not None:
            self._dlq_rows.inc()
            yield beam.pvalue.TaggedOutput(
                'errors',
                beam.Row(
                    step='ReadCsvWithDlq',
                    error_type=type(conv_err).__name__,
                    error=f'column {bad_col!r}: {conv_err}',
                    raw_payload=safe_json_dumps(dict(zip(header, cells)))))
            continue

          yield beam.Row(**parsed_vals)

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
      if error_handling and 'output' in error_handling:
        return {'good': good, err_tag: bad}
      return good

  return _ReadCsvWithDlqTransform()


def read_json_with_dlq(
    path: str,
    *,
    error_handling: Optional[dict[str, Any]] = None,
):
  """JSONL Beam YAML source routing corrupt JSON lines to DLQ."""
  import apache_beam as beam
  from apache_beam.io import ReadFromText
  from apache_beam.io.filesystem import CompressionTypes
  from apache_beam.io.filesystems import FileSystems
  from apache_beam.metrics import Metrics
  from apache_beam.typehints.row_type import RowTypeConstraint

  matches = FileSystems.match([path])[0].metadata_list
  if not matches:
    raise ValueError(f'no JSON files matched {path!r}')

  col_types: dict[str, type] = {}
  sampled = 0
  for meta in sorted(matches, key=lambda m: m.path):
    with FileSystems.open(
        meta.path, compression_type=CompressionTypes.AUTO) as raw_s:
      for raw_line in io.TextIOWrapper(raw_s, encoding='utf-8', errors='replace'):
        line = raw_line.strip()
        if not line:
          continue
        try:
          obj = json.loads(line)
        except ValueError:
          continue
        if isinstance(obj, dict):
          for k, v in obj.items():
            if v is not None and k not in col_types:
              if isinstance(v, bool):
                col_types[k] = Optional[bool]
              elif isinstance(v, int):
                col_types[k] = Optional[int]
              elif isinstance(v, float):
                col_types[k] = Optional[float]
              else:
                col_types[k] = Optional[str]
            elif k not in col_types:
              col_types[k] = Optional[str]
          sampled += 1
        if sampled >= 100:
          break
    if sampled >= 100:
      break

  if not col_types:
    raise ValueError(
        f'could not infer JSON object schema from {path!r} (no valid objects)')

  cols = list(col_types.keys())
  good_row_type = RowTypeConstraint.from_fields([(c, col_types[c]) for c in cols])
  dlq_row_type = RowTypeConstraint.from_fields([
      ('step', str),
      ('error_type', str),
      ('error', str),
      ('raw_payload', str),
  ])
  err_tag = (error_handling or {}).get('output', 'errors')

  class _ParseJsonLineDoFn(beam.DoFn):
    def __init__(self):
      self._rows_read = Metrics.counter('beam_cli', 'rows_read')
      self._dlq_rows = Metrics.counter('beam_cli', 'source_dlq_rows')

    def process(self, line: str):
      stripped = line.strip()
      if not stripped:
        return
      self._rows_read.inc()
      try:
        obj = json.loads(stripped)
        if not isinstance(obj, dict):
          raise ValueError('expected JSON object per line')
        row_kwargs = {}
        for c in cols:
          val = obj.get(c)
          if val is not None and col_types[c] is Optional[str] and not isinstance(
              val, str):
            val = json.dumps(val)
          elif val is not None and col_types[c] is Optional[float] and isinstance(
              val, int) and not isinstance(val, bool):
            val = float(val)
          row_kwargs[c] = val
        yield beam.Row(**row_kwargs)
      except Exception as exc:  # pylint: disable=broad-except
        self._dlq_rows.inc()
        yield beam.pvalue.TaggedOutput(
            'errors',
            beam.Row(
                step='ReadJsonWithDlq',
                error_type=type(exc).__name__,
                error=str(exc),
                raw_payload=safe_json_dumps({'raw_line': stripped[:4096]})))

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
      if error_handling and 'output' in error_handling:
        return {'good': good, err_tag: bad}
      return good

  return _ReadJsonWithDlqTransform()


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

