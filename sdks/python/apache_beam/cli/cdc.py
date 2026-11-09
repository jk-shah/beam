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

"""Cross-engine CDC envelope normalizer & BigQuery Storage Write API CDC sink.

Normalizes heterogeneous CDC streams:
- Debezium / Kafka Connect / Datastream nested JSON envelopes (`before`,
  `after`, `op` in `c|r|u|d`, `ts_ms`, `source.lsn`)
- Delta Lake Change Data Feed (`_change_type` in
  `insert|update_preimage|update_postimage|delete`, `_commit_version`)
- Apache Iceberg incremental snapshots (`_change_type`,
  `_commit_snapshot_sequence_number`)
- Flat CDC rows with configurable `--cdc-op-col` and `--cdc-seq-col`

Delivers idempotent ordered `UPSERT` and `DELETE` mutations to:
- `bigquery://` via BigQuery Storage Write API `_default` stream (`_CHANGE_TYPE`
  + `/`-segmented 16-digit hex `_CHANGE_SEQUENCE_NUMBER`, zero JVM required)
- `iceberg://` via Iceberg v2 merge-on-read equality deletes (`lakehouse.py` or
  managed `WriteToIceberg`)
"""

import datetime
import decimal
import io
import json
import re
from typing import Any
from typing import Iterable
from typing import Optional

from . import dlq

_HEX_SEGMENTS_RE = re.compile(r'^[0-9A-Fa-f]{1,16}(?:/[0-9A-Fa-f]{1,16}){0,3}$')
_SAFE_BQ_IDENT_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

_DEFAULT_OP_COLS = (
    'beam_cdc_op',
    '_change_type',
    'op',
    '__op',
    'operation',
    'mutation_type',
)
_DEFAULT_SEQ_COLS = (
    'beam_cdc_seq',
    '_commit_version',
    '_commit_snapshot_sequence_number',
    'lsn',
    'ts_ms',
    'sequence_number',
    'seq',
    'updated_at',
)
# Sequence columns that belong to a CDC envelope rather than to the row data.
_ENVELOPE_SEQ_COLS = frozenset(_DEFAULT_SEQ_COLS) - {'updated_at'}

_UPSERT_OPS = frozenset([
    'c',
    'r',
    'u',
    'i',
    'insert',
    'read',
    'snapshot',
    'update',
    'upsert',
    'update_after',
    'update_postimage',
])
_DELETE_OPS = frozenset(['d', 'delete'])
_SKIP_PREIMAGE_OPS = frozenset(['update_before', 'update_preimage'])


def to_bq_hex_sequence(raw_seq: Any, fallback_int: int = 0) -> tuple[int, str]:
  """Converts a sequence/LSN/timestamp into `(int64_seq, bq_hex_sequence_str)`.

  BigQuery Storage Write API `_CHANGE_SEQUENCE_NUMBER` requires 1..4 `/`-joined
  hexadecimal segments of 1..16 hex digits (`^[0-9A-Fa-f]{1,16}(/...){0,3}$`).
  Zero-padding each segment to 16 uppercase hex digits guarantees both BigQuery
  segment-numeric ordering AND lexicographical ordering match identically.
  """
  if raw_seq is None or raw_seq == '':
    v = max(int(fallback_int), 0)
    return v, f'{v & 0xFFFFFFFFFFFFFFFF:016X}'

  if isinstance(raw_seq, bool):
    v = 1 if raw_seq else 0
    return v, f'{v:016X}'

  if isinstance(raw_seq, int):
    v = max(raw_seq, 0)
    if v <= 0xFFFFFFFFFFFFFFFF:
      return v, f'{v:016X}'
    hi = (v >> 64) & 0xFFFFFFFFFFFFFFFF
    lo = v & 0xFFFFFFFFFFFFFFFF
    return (v & 0x7FFFFFFFFFFFFFFF), f'{hi:016X}/{lo:016X}'

  if isinstance(raw_seq, float):
    v = max(int(raw_seq * 1000), 0)
    return (v & 0x7FFFFFFFFFFFFFFF), f'{v & 0xFFFFFFFFFFFFFFFF:016X}'

  s = str(raw_seq).strip()
  if s.isdigit():
    return to_bq_hex_sequence(int(s), fallback_int=fallback_int)

  if s.lower().startswith('0x') and all(c in '0123456789abcdefABCDEF' for c in s[2:]):
    return to_bq_hex_sequence(int(s[2:], 16), fallback_int=fallback_int)

  # Postgres WAL LSN (`0/16B3748`) or explicit BigQuery hex sequence (`AAA/2`).
  if _HEX_SEGMENTS_RE.match(s):
    segs = [seg.upper().zfill(16) for seg in s.split('/')]
    combined_int = 0
    for seg in segs[-2:]:
      combined_int = ((combined_int << 32) ^ int(seg, 16)) & 0x7FFFFFFFFFFFFFFF
    return combined_int, '/'.join(segs)

  # ISO-8601 timestamp string (`2026-11-08T16:45:00.123456Z`).
  try:
    iso = s[:-1] + '+00:00' if s.endswith('Z') else s
    dt = datetime.datetime.fromisoformat(iso)
    if dt.tzinfo is None:
      dt = dt.replace(tzinfo=datetime.timezone.utc)
    us = max(int(dt.timestamp() * 1_000_000), 0)
    return (us & 0x7FFFFFFFFFFFFFFF), f'{us & 0xFFFFFFFFFFFFFFFF:016X}'
  except ValueError:
    pass

  # Deterministic big-endian hex encoding of arbitrary string (up to 32 bytes).
  raw_b = s.encode('utf-8')[:32]
  segs_out = []
  for idx in range(0, len(raw_b), 8):
    chunk = raw_b[idx:idx + 8].ljust(8, b'\x00')
    segs_out.append(chunk.hex().upper())
  int_approx = int(segs_out[0], 16) & 0x7FFFFFFFFFFFFFFF if segs_out else 0
  return int_approx, '/'.join(segs_out or ['0000000000000000'])


def parse_cdc_event(
    raw_element: Any,
    *,
    cdc_format: str = 'auto',
    op_col: Optional[str] = None,
    seq_col: Optional[str] = None,
    primary_keys: Iterable[str] = (),
) -> Optional[tuple[str, int, str, dict[str, Any]]]:
  """Parses one CDC event into `(op, int_seq, hex_seq, payload_dict)` or `None`.

  Returns `None` when a pre-image row (`UPDATE_BEFORE` / `update_preimage`)
  should be skipped so only the post-image upsert is applied.
  Raises `ValueError` on corrupt JSON, unknown op, or null primary key.
  """
  if hasattr(raw_element, '_asdict'):
    d = dict(raw_element._asdict())
  elif isinstance(raw_element, dict):
    d = dict(raw_element)
  elif isinstance(raw_element, (bytes, bytearray)):
    d = json.loads(bytes(raw_element).decode('utf-8'))
  elif isinstance(raw_element, str):
    d = json.loads(raw_element)
  else:
    raise ValueError(f'unsupported CDC element type: {type(raw_element).__name__}')

  # Unwrap single-string wrapper rows from `ReadFromPubSub(format='STRING')` or
  # `ReadFromText` (`Row(payload='{"op": ...}')` or `Row(line='...')`).
  if set(d.keys()) in ({'payload'}, {'line'}):
    inner_raw = d.get('payload') if 'payload' in d else d.get('line')
    if isinstance(inner_raw, (bytes, bytearray)):
      inner_raw = bytes(inner_raw).decode('utf-8')
    if isinstance(inner_raw, str):
      parsed_inner = json.loads(inner_raw)
      if not isinstance(parsed_inner, dict):
        raise ValueError('CDC JSON message must be a JSON object')
      d = parsed_inner

  fmt = cdc_format.lower()
  is_debezium = (
      fmt == 'debezium' or (
          fmt == 'auto' and 'op' in d and ('after' in d or 'before' in d) and
          isinstance(d.get('after') or d.get('before'), dict)))

  if is_debezium:
    raw_op = str(d.get('op') or '').strip().lower()
    if raw_op in _SKIP_PREIMAGE_OPS:
      return None
    if raw_op in _DELETE_OPS:
      canon_op = 'DELETE'
      img = d.get('before')
      if not isinstance(img, dict):
        raise ValueError("Debezium delete ('d') event is missing 'before' object")
    elif raw_op in _UPSERT_OPS:
      canon_op = 'UPSERT'
      img = d.get('after')
      if not isinstance(img, dict):
        raise ValueError(
            f"Debezium {raw_op!r} event is missing 'after' object")
    else:
      raise ValueError(f'unrecognized Debezium op {raw_op!r}')

    src_meta = d.get('source') if isinstance(d.get('source'), dict) else {}
    if seq_col and seq_col in d:
      seq_raw = d[seq_col]
    elif seq_col and seq_col in img:
      seq_raw = img[seq_col]
    else:
      seq_raw = (
          src_meta.get('lsn') or src_meta.get('sequence') or
          src_meta.get('ts_us') or src_meta.get('ts_ms') or d.get('ts_us') or
          d.get('ts_ms'))
    int_seq, hex_seq = to_bq_hex_sequence(seq_raw)
    payload = dict(img)
  else:
    eff_op_col = op_col
    if not eff_op_col:
      eff_op_col = next((c for c in _DEFAULT_OP_COLS if c in d), None)
    raw_op = (
        str(d.get(eff_op_col)).strip().lower()
        if eff_op_col and d.get(eff_op_col) is not None else 'upsert')
    if raw_op in _SKIP_PREIMAGE_OPS:
      return None
    if raw_op in _DELETE_OPS:
      canon_op = 'DELETE'
    elif raw_op in _UPSERT_OPS:
      canon_op = 'UPSERT'
    else:
      raise ValueError(f'unrecognized CDC operation {raw_op!r}')

    eff_seq_col = seq_col
    if not eff_seq_col:
      eff_seq_col = next((c for c in _DEFAULT_SEQ_COLS if c in d), None)
    seq_raw = d.get(eff_seq_col) if eff_seq_col else None
    int_seq, hex_seq = to_bq_hex_sequence(seq_raw)
    drop_meta = {
        c for c in (
            eff_op_col,
            # A sequence column that is ordinary data (e.g. updated_at) stays
            # in the row; only CDC envelope sequence columns are dropped.
            eff_seq_col if eff_seq_col in _ENVELOPE_SEQ_COLS else None,
            'beam_cdc_op',
            'beam_cdc_seq',
            'beam_cdc_hex_seq',
            '_change_type',
            '_commit_version',
            '_commit_snapshot_sequence_number',
        ) if c and c not in primary_keys
    }
    payload = {k: v for k, v in d.items() if k not in drop_meta}

  for pk in primary_keys:
    if payload.get(pk) is None:
      raise ValueError(f'CDC event has null/missing primary key column {pk!r}')

  return canon_op, int_seq, hex_seq, payload


def parse_fields_shorthand(spec_str: str) -> list[tuple[str, str]]:
  """Parses `'id:int,customer:str,amount:float'` -> `[('id', 'int'), ...]`."""
  type_aliases = {
      'int': 'int',
      'int64': 'int',
      'integer': 'int',
      'long': 'int',
      'float': 'float',
      'float64': 'float',
      'double': 'float',
      'bool': 'bool',
      'boolean': 'bool',
      'bytes': 'bytes',
      'str': 'str',
      'string': 'str',
  }
  out: list[tuple[str, str]] = []
  for part in spec_str.split(','):
    p = part.strip()
    if not p:
      continue
    if ':' not in p:
      out.append((p, 'str'))
      continue
    cname, ctype = [x.strip() for x in p.split(':', 1)]
    norm_t = type_aliases.get(ctype.lower())
    if not norm_t:
      raise ValueError(
          f'unsupported type {ctype!r} for column {cname!r}; use int, float, bool, str, or bytes'
      )
    out.append((cname, norm_t))
  return out


def infer_cdc_jsonl_fields(
    path: str,
    *,
    cdc_format: str = 'auto',
    op_col: Optional[str] = None,
    seq_col: Optional[str] = None,
    primary_keys: Iterable[str] = (),
    sample_rows: Optional[int] = 100,
) -> list[tuple[str, str]]:
  """Samples up to `sample_rows` JSONL records (`None` = all) for CDC schema."""
  from . import lakehouse

  matched = lakehouse._fs_list_files(
      path if any(c in path for c in '*?[') or '://' in path and lakehouse._fs_exists(path)
      else (path if lakehouse._fs_exists(path) else path + '*'))
  if not matched:
    return []

  col_order: list[str] = []
  col_types: dict[str, str] = {}
  seen = 0
  for fpath, _ in matched:
    with lakehouse._fs_open_read(fpath) as fobj, io.TextIOWrapper(
        fobj, encoding='utf-8', errors='replace') as text_s:
      for line in text_s:
        s = line.strip()
        if not s:
          continue
        try:
          parsed = parse_cdc_event(
              s,
              cdc_format=cdc_format,
              op_col=op_col,
              seq_col=seq_col,
              primary_keys=primary_keys)
        except ValueError:
          continue
        if parsed is None:
          continue
        _, _, _, rec = parsed
        for k, v in rec.items():
          if k not in col_types:
            col_order.append(k)
            col_types[k] = 'str'
          if v is not None:
            if isinstance(v, bool):
              col_types[k] = 'bool'
            elif isinstance(v, int):
              if col_types[k] != 'float':
                col_types[k] = 'int'
            elif isinstance(v, float):
              col_types[k] = 'float'
            else:
              col_types[k] = 'str'
        seen += 1
        if sample_rows is not None and seen >= sample_rows:
          break
    if sample_rows is not None and seen >= sample_rows:
      break
  return [(c, col_types[c]) for c in col_order]


def infer_target_table_fields(
    sink_ep, default_project: Optional[str] = None
) -> list[tuple[str, str]]:
  """Fetches existing target schema from BigQuery (`$0`) or Iceberg if present."""
  if sink_ep.kind == 'bigquery':
    try:
      from google.cloud import bigquery
      from . import uris
      client = bigquery.Client(project=default_project)
      _, proj, ds, tbl_name = uris.normalize_bq_table_spec(
          sink_ep.config['table'],
          default_project=default_project or client.project)
      fq = f'{proj or client.project}.{ds}.{tbl_name}'
      tbl = client.get_table(fq)
      bq_map = {
          'INTEGER': 'int',
          'INT64': 'int',
          'FLOAT': 'float',
          'FLOAT64': 'float',
          'NUMERIC': 'float',
          'BIGNUMERIC': 'float',
          'BOOLEAN': 'bool',
          'BOOL': 'bool',
          'BYTES': 'bytes',
      }
      return [(f.name, bq_map.get(f.field_type.upper(), 'str'))
              for f in tbl.schema]
    except Exception:  # pylint: disable=broad-except
      return []

  if sink_ep.kind == 'iceberg':
    try:
      from . import lakehouse
      tdir = lakehouse.resolve_iceberg_table_dir(
          sink_ep.config, default_project=default_project)
      st = lakehouse.load_iceberg_state(tdir)
      ic_map = {
          'int': 'int',
          'long': 'int',
          'float': 'float',
          'double': 'float',
          'boolean': 'bool',
          'binary': 'bytes',
      }
      return [(f['name'], ic_map.get(str(f['type']).lower(), 'str'))
              for f in st['schema']]
    except Exception:  # pylint: disable=broad-except
      return []
  return []


def _kind_to_py_type(kind_str: str) -> Any:
  return {
      'int': int,
      'float': float,
      'bool': bool,
      'bytes': bytes,
      'str': str,
  }.get(kind_str, str)


def _coerce_value(val: Any, kind_str: str) -> Any:
  if val is None:
    return None
  if kind_str == 'int':
    if isinstance(val, bool):
      raise ValueError(f'expected int, got bool {val!r}')
    return int(val)
  if kind_str == 'float':
    if isinstance(val, bool):
      raise ValueError(f'expected float, got bool {val!r}')
    return float(val)
  if kind_str == 'bool':
    if isinstance(val, bool):
      return val
    low = str(val).strip().lower()
    if low in ('true', '1'):
      return True
    if low in ('false', '0'):
      return False
    raise ValueError(f'expected bool, got {val!r}')
  if kind_str == 'bytes':
    return val if isinstance(val, bytes) else str(val).encode('utf-8')
  if isinstance(val, decimal.Decimal):
    return str(val)
  if hasattr(val, 'isoformat'):
    return val.isoformat()
  return str(val)


def cli_normalize_cdc(
    primary_key: list[str],
    *,
    cdc_format: str = 'auto',
    op_col: Optional[str] = None,
    seq_col: Optional[str] = None,
    fields_spec: Optional[list[list[str]]] = None,
    on_unknown_fields: str = 'error',
    strict_inferred_fields: bool = False,
    error_handling: Optional[dict[str, Any]] = None,
):
  """Beam PTransform normalizing CDC inputs into typed rows + `_beam_cdc_*`."""
  import apache_beam as beam
  from apache_beam import pvalue
  from apache_beam.metrics import Metrics
  from apache_beam.portability.api import schema_pb2
  from apache_beam.typehints import schemas
  from apache_beam.typehints.row_type import RowTypeConstraint

  pks = [str(k) for k in primary_key]
  check_unknown = bool(
      strict_inferred_fields and
      dlq.parse_unknown_fields_opt(on_unknown_fields) != 'ignore')
  err_tag = (error_handling or {}).get('output')
  dlq_row_type = RowTypeConstraint.from_fields([
      ('step', str),
      ('error_type', str),
      ('error', str),
      ('raw_payload', str),
  ])

  class _NormalizeCdcDoFn(beam.DoFn):
    def __init__(self, col_specs: list[tuple[str, str]]):
      self._cols = col_specs
      self._known = {c for c, _ in col_specs}
      self._err_ctr = Metrics.counter('beam_cli', 'cdc_dlq_rows')
      self._drift_ctr = Metrics.counter('beam_cli', 'schema_drift_rows')
      self._skip_ctr = Metrics.counter('beam_cli', 'cdc_preimage_skipped')

    def process(self, element, ts=beam.DoFn.TimestampParam):
      try:
        fallback_us = max(int(float(ts) * 1_000_000), 0) if ts else 0
        parsed = parse_cdc_event(
            element,
            cdc_format=cdc_format,
            op_col=op_col,
            seq_col=seq_col,
            primary_keys=pks)
        if parsed is None:
          self._skip_ctr.inc()
          return
        canon_op, int_seq, hex_seq, payload = parsed
        if check_unknown:
          extra = [k for k in payload if k not in self._known]
          if extra:
            shown = ', '.join(repr(k) for k in extra[:8])
            known = ', '.join(c for c, _ in self._cols)
            raise dlq.SchemaDriftError(
                f'new CDC field(s) {shown} appeared after schema sampling '
                f'(inferred fields: {known})',
                extra_fields=extra)
        if int_seq == 0 and fallback_us > 0:
          int_seq, hex_seq = to_bq_hex_sequence(fallback_us)
        out_dict: dict[str, Any] = {}
        for cname, ckind in self._cols:
          out_dict[cname] = _coerce_value(payload.get(cname), ckind)
        out_dict['beam_cdc_op'] = canon_op
        out_dict['beam_cdc_seq'] = int(int_seq)
        out_dict['beam_cdc_hex_seq'] = str(hex_seq)
        yield beam.Row(**out_dict)
      except Exception as exc:  # pylint: disable=broad-except
        if not err_tag:
          if isinstance(exc, dlq.SchemaDriftError):
            raise ValueError(
                f'{type(exc).__name__}: {exc} (pass --dlq PATH to quarantine '
                'these records for review, --from-opt sample_rows=all to scan '
                'all records for schema, or --from-opt on_unknown_fields=ignore '
                'to drop extra fields)') from exc
          raise
        self._err_ctr.inc()
        if isinstance(exc, dlq.SchemaDriftError):
          self._drift_ctr.inc()
        err_row = beam.Row(
            step='NormalizeCdc',
            error_type=type(exc).__name__,
            error=str(exc)[:4096],
            raw_payload=dlq.safe_json_dumps(element))
        yield pvalue.TaggedOutput(err_tag, err_row)

  class _NormalizeCdcTransform(beam.PTransform):
    def expand(self, pcoll):
      resolved_cols: list[tuple[str, str]] = []
      if fields_spec:
        resolved_cols = [(str(pair[0]), str(pair[1])) for pair in fields_spec]
      else:
        beam_schema = schemas.schema_from_element_type(pcoll.element_type)
        fnames = [f.name for f in beam_schema.fields]
        if set(fnames) not in ({'payload'}, {'line'}):
          meta_skip = {
              c for c in (
                  op_col,
                  # Like parse_cdc_event: a sequence column that is ordinary
                  # data (--cdc-seq-col updated_at) stays in the output; only
                  # CDC envelope sequence columns (lsn, ts_ms, ...) are dropped.
                  seq_col if seq_col in _ENVELOPE_SEQ_COLS else None,
                  'beam_cdc_op',
                  'beam_cdc_seq',
                  'beam_cdc_hex_seq',
                  '_change_type',
                  '_commit_version',
                  '_commit_snapshot_sequence_number',
              ) if c and c not in pks
          }
          for f in beam_schema.fields:
            if f.name in meta_skip:
              continue
            atom = f.type.atomic_type
            if atom == schema_pb2.BOOLEAN:
              k = 'bool'
            elif atom in (schema_pb2.INT16, schema_pb2.INT32, schema_pb2.INT64):
              k = 'int'
            elif atom in (schema_pb2.FLOAT, schema_pb2.DOUBLE):
              k = 'float'
            elif atom == schema_pb2.BYTES:
              k = 'bytes'
            else:
              k = 'str'
            resolved_cols.append((f.name, k))

      if not resolved_cols:
        raise ValueError(
            'Unable to infer CDC payload columns; pass --from-opt '
            'fields="id:int,col:str,..." or target an existing table')

      row_fields = [
          (cname, Optional[_kind_to_py_type(ckind)])
          for cname, ckind in resolved_cols
      ] + [
          ('beam_cdc_op', str),
          ('beam_cdc_seq', int),
          ('beam_cdc_hex_seq', str),
      ]
      out_row_type = RowTypeConstraint.from_fields(row_fields)

      if err_tag:
        res = (
            pcoll
            | 'ParseCdcWithDlq' >> beam.ParDo(
                _NormalizeCdcDoFn(resolved_cols)).with_outputs(
                    err_tag, main='good'))
        res['good'].element_type = out_row_type
        res[err_tag].element_type = dlq_row_type
        return {'good': res['good'], err_tag: res[err_tag]}

      return (
          pcoll
          | 'ParseCdc' >> beam.ParDo(
              _NormalizeCdcDoFn(resolved_cols)).with_output_types(out_row_type))

  return _NormalizeCdcTransform()


_CDC_META_COLUMNS = ('beam_cdc_op', 'beam_cdc_seq', 'beam_cdc_hex_seq')


def cli_materialize_cdc(
    primary_key: list[str], sequence_column: Optional[str] = None):
  """Collapses normalized CDC rows to the latest live row per primary key.

  `beam pipe --cdc` inserts this after CliNormalizeCdc when the sink is a plain
  file (Parquet, CSV, JSON, Avro). Unlike Iceberg or BigQuery CDC sinks, files
  can't apply upserts and deletes, so writing the change rows directly would
  produce every version of every key plus internal `beam_cdc_*` columns. For
  each key this keeps the row with the highest `beam_cdc_seq`, drops the key
  if that row is a DELETE, and removes the `beam_cdc_*` columns. If all
  changes for a key share one sequence number (no ordering column was found)
  it fails rather than guess. Superseded rows and deletes are counted in
  `cdc_superseded_rows`.
  """
  import apache_beam as beam
  from apache_beam.metrics import Metrics
  from apache_beam.typehints import schemas
  from apache_beam.typehints.row_type import RowTypeConstraint

  pks = [str(k) for k in primary_key]

  class _LatestLiveRowDoFn(beam.DoFn):
    def __init__(self, names):
      self._names = names
      self._superseded = Metrics.counter('beam_cli', 'cdc_superseded_rows')

    def process(self, keyed):
      key, rows = keyed
      rows = list(rows)
      top = max(r.beam_cdc_seq for r in rows)
      tied = [r for r in rows if r.beam_cdc_seq == top]
      # Exact duplicates (same values and operation) are one change.
      distinct = {
          tuple(getattr(r, n) for n in self._names) + (r.beam_cdc_op, )
          for r in tied
      }
      if len(distinct) > 1:
        if sequence_column:
          hint = (
              f'they have the same {sequence_column} value, so the latest '
              'cannot be chosen; use a finer ordering column (for example a '
              'timestamp with seconds or an LSN) or deduplicate the input '
              'first')
        else:
          hint = (
              'no ordering column was found, so the latest cannot be chosen; '
              'pass --cdc-seq-col COLUMN naming an ordering column (for '
              'example updated_at or lsn)')
        raise ValueError(
            f'--cdc: {len(distinct)} different changes for primary key '
            f'{key!r} share the latest sequence value: {hint}')
      latest = tied[0]
      self._superseded.inc(len(rows) - 1)
      if latest.beam_cdc_op == 'DELETE':
        self._superseded.inc()
        return
      yield beam.Row(**{n: getattr(latest, n) for n in self._names})

  class _MaterializeCdcTransform(beam.PTransform):
    def expand(self, pcoll):
      fields = [(name, typ)
                for name, typ in schemas.named_fields_from_element_type(
                    pcoll.element_type) if name not in _CDC_META_COLUMNS]
      names = [name for name, _ in fields]
      return (
          pcoll
          | 'KeyByPrimaryKey' >> beam.Map(
              lambda r: (tuple(getattr(r, k) for k in pks), r))
          | 'GroupVersions' >> beam.GroupByKey()
          | 'LatestLiveRow' >> beam.ParDo(_LatestLiveRowDoFn(names))
          .with_output_types(RowTypeConstraint.from_fields(fields)))

  return _MaterializeCdcTransform()


# ---------------------------------------------------------------------------
# Pure-Python BigQuery Storage Write API CDC Sink (`_CHANGE_TYPE` + Hex Seq)
# ---------------------------------------------------------------------------


def _ensure_bq_cdc_table(
    table_spec: str,
    col_specs: list[tuple[str, str]],
    primary_keys: list[str],
    write_disposition: str,
    default_project: Optional[str] = None,
) -> tuple[str, str, str]:
  """Ensures destination BigQuery table exists with `PRIMARY KEY NOT ENFORCED`."""
  from google.api_core import exceptions as gcp_exceptions
  from google.cloud import bigquery as gcp_bq
  from . import uris

  _, proj, ds, tbl_name = uris.normalize_bq_table_spec(
      table_spec, default_project=default_project)
  client = gcp_bq.Client(project=proj)
  eff_proj = proj or client.project
  fq = f'{eff_proj}.{ds}.{tbl_name}'

  for ident in [ds, tbl_name] + list(primary_keys) + [c for c, _ in col_specs]:
    if not _SAFE_BQ_IDENT_RE.match(ident):
      raise ValueError(f'invalid BigQuery SQL identifier: {ident!r}')

  exists = False
  try:
    existing_tbl = client.get_table(fq)
    exists = True
    if write_disposition == 'WRITE_EMPTY' and (existing_tbl.num_rows or 0) > 0:
      raise ValueError(
          f'BigQuery table {fq!r} already exists and has '
          f'{existing_tbl.num_rows} rows; pass --if-exists=append or '
          '--if-exists=replace')
  except gcp_exceptions.NotFound:
    exists = False

  sql_type_map = {
      'int': 'INT64',
      'float': 'FLOAT64',
      'bool': 'BOOL',
      'bytes': 'BYTES',
      'str': 'STRING',
  }
  pk_set = set(primary_keys)
  col_defs = []
  for cname, ckind in col_specs:
    not_null = ' NOT NULL' if cname in pk_set else ''
    col_defs.append(f'`{cname}` {sql_type_map.get(ckind, "STRING")}{not_null}')
  pk_clause = ', '.join(f'`{k}`' for k in primary_keys)

  if write_disposition == 'WRITE_TRUNCATE' or not exists:
    verb = (
        'CREATE OR REPLACE TABLE'
        if write_disposition == 'WRITE_TRUNCATE' else 'CREATE TABLE IF NOT EXISTS'
    )
    ddl = (
        f'{verb} `{eff_proj}.{ds}.{tbl_name}` '
        f'({", ".join(col_defs)}, PRIMARY KEY ({pk_clause}) NOT ENFORCED)')
    client.query(ddl).result()

  return eff_proj, ds, tbl_name


def _build_bq_cdc_proto_bundle(col_specs: list[tuple[str, str]]):
  """Builds `(DescriptorProto, MessageClass)` including `_CHANGE_*` fields."""
  from google.protobuf import descriptor_pb2
  from google.protobuf import descriptor_pool
  from google.protobuf import message_factory

  proto_type_map = {
      'int': descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
      'float': descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE,
      'bool': descriptor_pb2.FieldDescriptorProto.TYPE_BOOL,
      'bytes': descriptor_pb2.FieldDescriptorProto.TYPE_BYTES,
      'str': descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
  }

  file_proto = descriptor_pb2.FileDescriptorProto()
  file_proto.name = f'beam_cli_bq_cdc_{id(col_specs)}.proto'
  file_proto.syntax = 'proto2'
  msg_proto = file_proto.message_type.add()
  msg_proto.name = 'BqCdcRow'

  all_fields = list(col_specs) + [
      ('_CHANGE_TYPE', 'str'),
      ('_CHANGE_SEQUENCE_NUMBER', 'str'),
  ]
  for idx, (cname, ckind) in enumerate(all_fields, start=1):
    f = msg_proto.field.add()
    f.name = cname
    f.number = idx
    f.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    f.type = proto_type_map.get(ckind, descriptor_pb2.FieldDescriptorProto.TYPE_STRING)

  pool = descriptor_pool.DescriptorPool()
  file_desc = pool.Add(file_proto)
  msg_desc = file_desc.message_types_by_name['BqCdcRow']
  msg_cls = message_factory.GetMessageClass(msg_desc)

  clean_msg_proto = descriptor_pb2.DescriptorProto()
  clean_msg_proto.CopyFrom(msg_proto)
  return clean_msg_proto, msg_cls


def cli_write_to_bigquery_cdc(
    table: str,
    primary_key: list[str],
    *,
    write_disposition: str = 'WRITE_APPEND',
    error_handling: Optional[dict[str, Any]] = None,
):
  """Pure-Python BigQuery Storage Write API CDC sink (`_default` stream)."""
  import apache_beam as beam
  from apache_beam import pvalue
  from apache_beam.metrics import Metrics
  from apache_beam.options.pipeline_options import GoogleCloudOptions
  from apache_beam.portability.api import schema_pb2
  from apache_beam.transforms import trigger
  from apache_beam.transforms import window
  from apache_beam.typehints import schemas
  from apache_beam.typehints.row_type import RowTypeConstraint

  pks = [str(k) for k in primary_key]
  if not pks:
    raise ValueError('BigQuery CDC sink requires --primary-key')
  err_tag = (error_handling or {}).get('output')
  dlq_row_type = RowTypeConstraint.from_fields([
      ('step', str),
      ('error_type', str),
      ('error', str),
      ('raw_payload', str),
  ])

  class _AppendBqCdcBatchDoFn(beam.DoFn):
    def __init__(
        self,
        stream_name: str,
        col_specs: list[tuple[str, str]],
    ):
      self._stream_name = stream_name
      self._col_specs = col_specs
      self._client = None
      self._desc_proto = None
      self._msg_cls = None
      self._sink_dlq = Metrics.counter('beam_cli', 'sink_dlq_rows')

    def setup(self):
      from google.cloud import bigquery_storage_v1
      self._client = bigquery_storage_v1.BigQueryWriteClient()
      self._desc_proto, self._msg_cls = _build_bq_cdc_proto_bundle(
          self._col_specs)

    def process(self, kv):
      from google.cloud.bigquery_storage_v1 import types as bqs_types

      _, rows_iter = kv
      batch = [
          r._asdict() if hasattr(r, '_asdict') else dict(r) for r in rows_iter
      ]
      if not batch:
        return

      # Send in chunks of up to 500 rows to stay well under the 10 MB gRPC limit.
      for start in range(0, len(batch), 500):
        chunk = batch[start:start + 500]
        serialized: list[bytes] = []
        valid_rows: list[dict[str, Any]] = []
        for r in chunk:
          try:
            msg = self._msg_cls()
            for cname, ckind in self._col_specs:
              val = r.get(cname)
              if val is not None:
                setattr(msg, cname, _coerce_value(val, ckind))
            op_val = str(r.get('beam_cdc_op') or 'UPSERT').upper()
            hex_seq = str(r.get('beam_cdc_hex_seq') or '')
            if not hex_seq:
              _, hex_seq = to_bq_hex_sequence(r.get('beam_cdc_seq', 0))
            setattr(msg, '_CHANGE_TYPE', op_val)
            setattr(msg, '_CHANGE_SEQUENCE_NUMBER', hex_seq)
            serialized.append(msg.SerializeToString())
            valid_rows.append(r)
          except Exception as exc:  # pylint: disable=broad-except
            if not err_tag:
              raise
            self._sink_dlq.inc()
            yield pvalue.TaggedOutput(
                err_tag,
                beam.Row(
                    step='WriteToBigQueryCDC',
                    error_type=type(exc).__name__,
                    error=str(exc)[:4096],
                    raw_payload=dlq.safe_json_dumps(r)))

        if not serialized:
          continue

        import time as _time
        from google.api_core import exceptions as _gcp_exc

        proto_schema = bqs_types.ProtoSchema(proto_descriptor=self._desc_proto)
        proto_rows = bqs_types.ProtoRows(serialized_rows=serialized)
        responses = None
        for attempt in range(5):
          req = bqs_types.AppendRowsRequest(
              write_stream=self._stream_name,
              proto_rows=bqs_types.AppendRowsRequest.ProtoData(
                  writer_schema=proto_schema, rows=proto_rows))
          try:
            responses = list(self._client.append_rows(iter([req])))
            break
          except (
              _gcp_exc.NotFound,
              _gcp_exc.ServiceUnavailable,
              _gcp_exc.DeadlineExceeded,
          ):
            if attempt >= 4:
              raise
            _time.sleep(0.5 * (2**attempt))

        for resp in responses or ():
          if resp.row_errors:
            for r_err in resp.row_errors:
              bad_idx = int(r_err.index)
              bad_row = (
                  valid_rows[bad_idx] if 0 <= bad_idx < len(valid_rows) else {})
              if not err_tag:
                raise RuntimeError(
                    f'BigQuery CDC row error at index {bad_idx}: {r_err.message}'
                )
              self._sink_dlq.inc()
              yield pvalue.TaggedOutput(
                  err_tag,
                  beam.Row(
                      step='WriteToBigQueryCDC',
                      error_type='BigQueryRowError',
                      error=str(r_err.message)[:4096],
                      raw_payload=dlq.safe_json_dumps(bad_row)))
          elif resp.error and resp.error.code != 0:
            raise RuntimeError(
                f'BigQuery Storage Write API error ({resp.error.code}): '
                f'{resp.error.message}')

  class _CliWriteBqCdcTransform(beam.PTransform):
    def expand(self, pcoll):
      beam_schema = schemas.schema_from_element_type(pcoll.element_type)
      meta_cols = {'beam_cdc_op', 'beam_cdc_seq', 'beam_cdc_hex_seq'}
      col_specs: list[tuple[str, str]] = []
      for f in beam_schema.fields:
        if f.name in meta_cols:
          continue
        atom = f.type.atomic_type
        if atom == schema_pb2.BOOLEAN:
          k = 'bool'
        elif atom in (schema_pb2.INT16, schema_pb2.INT32, schema_pb2.INT64):
          k = 'int'
        elif atom in (schema_pb2.FLOAT, schema_pb2.DOUBLE):
          k = 'float'
        elif atom == schema_pb2.BYTES:
          k = 'bytes'
        else:
          k = 'str'
        col_specs.append((f.name, k))

      default_proj = pcoll.pipeline.options.view_as(GoogleCloudOptions).project
      eff_proj, ds, tbl_name = _ensure_bq_cdc_table(
          table,
          col_specs=col_specs,
          primary_keys=pks,
          write_disposition=write_disposition,
          default_project=default_proj)
      stream_name = (
          f'projects/{eff_proj}/datasets/{ds}/tables/{tbl_name}/streams/_default'
      )

      # Partition batches across 4 deterministic shards for throughput.
      keyed = pcoll | 'ShardBqCdc' >> beam.Map(
          lambda r: (hash(getattr(r, pks[0], 0)) % 4, r))
      if pcoll.is_bounded:
        grouped = keyed | 'BatchBqCdc' >> beam.GroupByKey()
      else:
        grouped = (
            keyed
            | 'WindowBqCdc' >> beam.WindowInto(
                window.GlobalWindows(),
                trigger=trigger.Repeatedly(trigger.AfterProcessingTime(1)),
                accumulation_mode=trigger.AccumulationMode.DISCARDING)
            | 'StreamBatchBqCdc' >> beam.GroupByKey())

      dofn = _AppendBqCdcBatchDoFn(stream_name, col_specs)
      if err_tag:
        res = grouped | 'AppendBqCdc' >> beam.ParDo(dofn).with_outputs(
            err_tag, main='post_write')
        res[err_tag].element_type = dlq_row_type
        return {'post_write': res['post_write'], err_tag: res[err_tag]}

      _ = grouped | 'AppendBqCdc' >> beam.ParDo(dofn)
      return None

  return _CliWriteBqCdcTransform()
