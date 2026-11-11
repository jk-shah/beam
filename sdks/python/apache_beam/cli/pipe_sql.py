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

"""Multi-input SQL joins & aggregations (`--sql` / `--join`) for `beam pipe`.

Supports:
1. Local `<100ms` zero-JVM execution over CSV, Parquet, JSON/JSONL, Avro,
   `delta://`, and `iceberg://` via an authorizer-sandboxed `:memory:` SQLite
   engine (`LEFT JOIN`, `INNER JOIN`, `FULL OUTER JOIN`, `COALESCE`, `GROUP BY`,
   `HAVING`, `ORDER BY`, CTEs `WITH`, and window functions).
2. Portable Beam YAML `type: Sql` compilation (`--emit-yaml` / `--dry-run` and
   remote `DataflowRunner` execution).
"""

from __future__ import annotations

import csv
import decimal
import glob
import io
import json
import math
import os
import re
import sqlite3
from typing import Any
from typing import Iterable
from typing import NamedTuple

from . import dlq
from . import uris

_ALIAS_PREFIX_RE = re.compile(r'^(?P<alias>[A-Za-z_][A-Za-z0-9_]*)=(?P<uri>.+)$')
_SAFE_IDENT_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


class SqlPipeError(ValueError):
  """Raised on invalid `--sql` / `--join` arguments or SQL query errors."""


class NamedSource(NamedTuple):
  alias: str
  uri: str
  endpoint: uris.ParsedEndpoint


def _derive_alias_from_uri(raw_uri: str, used: set[str]) -> str:
  cleaned = raw_uri.split('?', 1)[0].rstrip('/')
  if '://' in cleaned:
    cleaned = cleaned.split('://', 1)[1]
  parts = [p for p in cleaned.replace('\\', '/').split('/') if p]
  stem = parts[-1] if parts else 'source'
  if any(ch in stem for ch in '*?['):
    stem = parts[-2] if len(parts) >= 2 else 'source'
  # Strip known extensions or Iceberg namespace prefix (`analytics.events` -> `events`).
  lower_stem = stem.lower()
  for ext in (
      '.parquet',
      '.pq',
      '.jsonl',
      '.ndjson',
      '.json',
      '.csv',
      '.tsv',
      '.avro',
      '.txt',
  ):
    if lower_stem.endswith(ext):
      stem = stem[:-len(ext)]
      break
  else:
    if '.' in stem:
      stem = stem.rsplit('.', 1)[-1]
  ident = re.sub(r'[^A-Za-z0-9_]', '_', stem).strip('_')
  if not ident or ident[0].isdigit():
    ident = 'tbl_' + (ident or '1')
  candidate = ident
  idx = 2
  used_lower = {u.lower() for u in used}
  while candidate.lower() in used_lower:
    candidate = f'{ident}_{idx}'
    idx += 1
  return candidate


def parse_named_sources(
    raw_sources: Iterable[str],
    raw_joins: Iterable[str] | None,
    *,
    from_format: str | None = None,
    from_opts: dict[str, Any] | None = None,
    default_project: str | None = None,
    allow_sniff: bool = True,
    allow_jinja: bool = False,
) -> list[NamedSource]:
  """Parses `--from [ALIAS=]URI` and `--join [ALIAS=]URI` into `NamedSource`s."""
  combined = list(raw_sources or []) + list(raw_joins or [])
  if not combined:
    raise SqlPipeError('--from requires at least one source URI')

  out: list[NamedSource] = []
  used_aliases: set[str] = set()
  for entry in combined:
    entry_s = (entry or '').strip()
    if not entry_s:
      raise SqlPipeError('source URI cannot be empty')
    explicit_alias: str | None = None
    target_uri = entry_s
    m = _ALIAS_PREFIX_RE.match(entry_s)
    if m and not os.path.exists(entry_s):
      cand_uri = m.group('uri').strip()
      if cand_uri:
        explicit_alias = m.group('alias')
        target_uri = cand_uri
    if explicit_alias is not None:
      if explicit_alias.lower() in {a.lower() for a in used_aliases}:
        raise SqlPipeError(
            f'duplicate SQL source alias {explicit_alias!r}; each --from/--join '
            'source must have a unique alias')
      alias = explicit_alias
    else:
      alias = _derive_alias_from_uri(target_uri, used_aliases)
    used_aliases.add(alias)

    has_jinja_syntax = uris._has_jinja_syntax(target_uri)
    mixed_groups = (
        uris.local_format_groups(target_uri)
        if (not from_format and (allow_sniff or not has_jinja_syntax)) else {})
    eff_format = from_format or (next(iter(mixed_groups), None) if mixed_groups else None)

    ep = uris.parse_endpoint(
        target_uri,
        is_read=True,
        explicit_format=eff_format,
        options=dict(from_opts or {}),
        default_project=default_project,
        allow_sniff=allow_sniff,
        allow_jinja=allow_jinja)
    if mixed_groups:
      ep.config['_mixed_groups'] = mixed_groups
    elif from_format:
      ep.config['_explicit_format'] = True
    out.append(NamedSource(alias=alias, uri=target_uri, endpoint=ep))
  return out


def validate_readonly_select_sql(
    sql_query: str, *, allow_jinja: bool = False) -> str:
  """Ensures `--sql` is a single read-only `SELECT` or `WITH ... SELECT`."""
  q = (sql_query or '').strip()
  if not q:
    raise SqlPipeError('--sql query cannot be empty')
  if uris._has_jinja_syntax(q):
    if not allow_jinja:
      raise SqlPipeError(
          'Jinja2 template placeholders in --sql require --emit-yaml '
          '(or expand template variables before running)')
    try:
      uris._validate_jinja_uri(q)
    except uris.UriError as e:
      raise SqlPipeError(str(e)) from None
  # Strip optional single trailing semicolon.
  if q.endswith(';'):
    q = q[:-1].strip()
  if not q:
    raise SqlPipeError('--sql query cannot be empty')
  if not sqlite3.complete_statement(q + ';'):
    raise SqlPipeError(f'incomplete --sql query: {sql_query!r}')
  lead = q.lstrip('(').lstrip().split(None, 1)[0].upper() if q.strip('(').strip() else ''
  if lead not in ('SELECT', 'WITH'):
    raise SqlPipeError(
        f'--sql only accepts read-only SELECT or WITH queries (got {lead!r})')
  return q


def build_sql_yaml_spec(
    named_sources: list[NamedSource],
    sql_query: str,
    sink_ep: uris.ParsedEndpoint | None,
    *,
    allow_jinja: bool = False,
) -> dict[str, Any]:
  """Builds portable Beam YAML `type: Sql` spec for `--emit-yaml` / remote runs."""
  clean_q = validate_readonly_select_sql(sql_query, allow_jinja=allow_jinja)
  transforms: list[dict[str, Any]] = []
  sql_inputs: dict[str, str] = {}
  for ns in named_sources:
    read_name = f'Read_{ns.alias}'
    mixed_groups = ns.endpoint.config.get('_mixed_groups')
    if mixed_groups:
      sub_inputs = []
      for sub_fmt, sub_files in mixed_groups.items():
        sub_read_name = f'Read_{ns.alias}_{sub_fmt}'
        sub_yaml_type = f'ReadFrom{uris.FILE_FORMATS.get(sub_fmt, sub_fmt.capitalize())}'
        sub_cfg = {
            k: v for k, v in ns.endpoint.config.items()
            if not k.startswith('_')
        }
        sub_cfg['path'] = list(sub_files)
        transforms.append({
            'name': sub_read_name,
            'type': sub_yaml_type,
            'config': sub_cfg,
        })
        sub_inputs.append(sub_read_name)
      transforms.append({
          'name': read_name,
          'type': 'Flatten',
          'input': sub_inputs,
      })
    else:
      cfg = {
          k: v for k, v in ns.endpoint.config.items()
          if not k.startswith('_')
      }
      transforms.append({
          'name': read_name,
          'type': ns.endpoint.yaml_type,
          'config': cfg,
      })
    sql_inputs[ns.alias] = read_name

  sql_input_val: Any = (
      next(iter(sql_inputs.values()))
      if len(sql_inputs) == 1 and 'pcollection' in clean_q.lower() else
      sql_inputs)
  transforms.append({
      'name': 'SqlQuery',
      'type': 'Sql',
      'input': sql_input_val,
      'config': {
          'query': clean_q
      },
  })
  if sink_ep is not None:
    transforms.append({
        'name': 'WriteSink',
        'type': sink_ep.yaml_type,
        'input': 'SqlQuery',
        'config': dict(sink_ep.config),
    })
  return {
      'pipeline': {
          'transforms': transforms,
      }
  }


def _match_source_files(
    path_pattern: str | list[str] | tuple[str, ...]) -> list[str]:
  if isinstance(path_pattern, (list, tuple)):
    return sorted(str(p) for p in path_pattern if os.path.isfile(str(p)))
  if '://' not in path_pattern:
    if os.path.isfile(path_pattern):
      return [path_pattern]
    if os.path.isdir(path_pattern):
      files = sorted(
          os.path.join(path_pattern, f) for f in os.listdir(path_pattern)
          if not f.startswith('.') and
          os.path.isfile(os.path.join(path_pattern, f)))
      if files:
        return files
    matched = sorted(p for p in glob.glob(path_pattern) if os.path.isfile(p))
    if matched:
      return matched
    shard_matched = sorted(
        p for p in glob.glob(glob.escape(path_pattern) + '-*-of-*')
        if os.path.isfile(p))
    if shard_matched:
      return shard_matched
    raise FileNotFoundError(f'no input files matched {path_pattern!r}')

  from apache_beam.io.filesystems import FileSystems
  matches = FileSystems.match([path_pattern])[0].metadata_list
  if not matches and not any(ch in path_pattern for ch in '*?['):
    matches = FileSystems.match([path_pattern + '-*-of-*'])[0].metadata_list
  if not matches:
    raise FileNotFoundError(f'no input files matched {path_pattern!r}')
  return sorted(m.path for m in matches)


def _open_binary(path: str):
  if '://' not in path:
    return open(path, 'rb')
  from apache_beam.io.filesystems import FileSystems
  return FileSystems.open(path)


def _coerce_csv_val(
    val: str, kind: str, null_values=dlq.DEFAULT_CSV_NULL_VALUES) -> Any:
  s = val.strip()
  if s == '' or val in null_values or s in null_values:
    return None
  if kind == 'int':
    return int(s)
  if kind == 'float':
    v = float(s)
    if math.isnan(v) or math.isinf(v):
      raise ValueError(f'non-finite float {val!r}')
    return v
  if kind == 'bool':
    low = s.lower()
    if low in ('true', '1', 't', 'yes'):
      return True
    if low in ('false', '0', 'f', 'no'):
      return False
    raise ValueError(f'invalid boolean {val!r}')
  return val


def load_endpoint_rows(
    ep: uris.ParsedEndpoint,
    *,
    default_project: str | None = None,
    dlq_records: list[dict[str, Any]] | None = None,
    warnings: list[str] | None = None,
) -> tuple[list[tuple[str, str]], list[dict[str, Any]], int]:
  """Reads `(columns_with_sqlite_types, rows, raw_rows_read)` from `ep`."""
  from . import lakehouse

  if ep.kind == 'delta':
    table_root = ep.config.get('table') or ep.config.get('path')
    ver = ep.config.get('version')
    snap, rows = lakehouse.read_delta_rows_local(
        table_root,
        version=int(ver) if ver is not None else None,
        limit=None)
    cols = []
    for f in snap.get('schema', []):
      ft = str(f.get('type', 'string')).lower()
      sq_t = (
          'INTEGER' if ft in ('long', 'integer', 'short', 'byte', 'boolean')
          else 'REAL' if ft in ('double', 'float') or ft.startswith('decimal')
          else 'TEXT')
      cols.append((str(f['name']), sq_t))
    return cols, rows, len(rows)

  if ep.kind == 'iceberg':
    tdir = lakehouse.resolve_iceberg_table_dir(
        ep.config, default_project=default_project)
    state = lakehouse.load_iceberg_state(tdir)
    _, rows = lakehouse.read_iceberg_rows_local(state, limit=None)
    cols = []
    for f in state.get('schema', []):
      ft = str(f.get('type', 'string')).lower()
      sq_t = (
          'INTEGER' if ft in ('long', 'int', 'boolean') else 'REAL'
          if ft in ('double', 'float') or ft.startswith('decimal') else 'TEXT')
      cols.append((str(f['name']), sq_t))
    return cols, rows, len(rows)

  if ep.kind != 'file':
    raise SqlPipeError(
        f'local --sql currently supports file (csv, parquet, json, avro), '
        f'delta://, and iceberg:// sources (got {ep.kind!r}); use --emit-yaml '
        'or --runner=DataflowRunner for remote connectors')

  mixed_groups = ep.config.get('_mixed_groups')
  if not mixed_groups and not ep.config.get('_explicit_format'):
    p_val = ep.config.get('path')
    if isinstance(p_val, str):
      mixed_groups = uris.local_format_groups(p_val)

  if mixed_groups:
    merged_cols: list[tuple[str, str]] = []
    seen_col_names: set[str] = set()
    col_type_map: dict[str, str] = {}
    rows: list[dict[str, Any]] = []
    total_raw_count = 0

    for sub_fmt, sub_files in mixed_groups.items():
      sub_cfg = dict(ep.config)
      sub_cfg['path'] = list(sub_files)
      sub_cfg['_explicit_format'] = True
      sub_cfg.pop('_mixed_groups', None)
      sub_ep = ep._replace(format=sub_fmt, config=sub_cfg)
      sub_cols, sub_rows, sub_cnt = load_endpoint_rows(
          sub_ep,
          default_project=default_project,
          dlq_records=dlq_records,
          warnings=warnings)
      total_raw_count += sub_cnt
      rows.extend(sub_rows)
      for c_name, c_type in sub_cols:
        if c_name not in seen_col_names:
          seen_col_names.add(c_name)
          merged_cols.append((c_name, c_type))
          col_type_map[c_name] = c_type
        else:
          curr_type = col_type_map[c_name]
          if curr_type != c_type:
            if {curr_type, c_type} == {'INTEGER', 'REAL'}:
              col_type_map[c_name] = 'REAL'
            else:
              col_type_map[c_name] = 'TEXT'

    final_cols = [(cn, col_type_map[cn]) for cn, _ in merged_cols]
    return final_cols, rows, total_raw_count

  files = _match_source_files(ep.config['path'])
  fmt = ep.format

  if fmt == 'parquet':
    import pyarrow as pa
    import pyarrow.parquet as pq
    tables = []
    for fp in files:
      with _open_binary(fp) as bf:
        tables.append(pq.read_table(bf))
    if not tables:
      return [], [], 0
    full = pa.concat_tables(tables, promote_options='default')
    cols = []
    for field in full.schema:
      t = field.type
      if pa.types.is_integer(t) or pa.types.is_boolean(t):
        sq_t = 'INTEGER'
      elif pa.types.is_floating(t) or pa.types.is_decimal(t):
        sq_t = 'REAL'
      else:
        sq_t = 'TEXT'
      cols.append((field.name, sq_t))
    rows = full.to_pylist()
    return cols, rows, len(rows)

  if fmt == 'csv':
    delim = str(ep.config.get('delimiter', ',') or ',')
    nulls = dlq.DEFAULT_CSV_NULL_VALUES | frozenset(
        ep.config.get('null_values') or ())
    header, kinds = dlq._preflight_csv_schema(
        ep.config['path'],
        delimiter=delim,
        allow_dirty=bool(dlq_records is not None),
        null_values=nulls,
        scan_all_for_leading_zeros=('://' not in str(ep.config['path'])))
    if ep.config.get('fields'):
      from . import cdc as cdc_mod
      try:
        parsed_types = dict(
            cdc_mod.parse_fields_shorthand(str(ep.config['fields'])))
        kinds = dlq.apply_type_overrides(
            kinds, parsed_types, str(ep.raw), allow_new_columns=True)
      except ValueError as e:
        raise SqlPipeError(str(e)) from None
    cols = [
        (
            c,
            'INTEGER' if kinds.get(c) in ('int', 'bool') else
            'REAL' if kinds.get(c) == 'float' else 'TEXT',
        ) for c in header
    ]
    rows = []
    raw_count = 0
    for fp in files:
      with _open_binary(fp) as bf:
        text = bf.read().decode('utf-8-sig', errors='replace')
      reader = csv.reader(io.StringIO(text, newline=''), delimiter=delim)
      first = True
      for rec in reader:
        if first:
          first = False
          continue
        if not rec or (len(rec) == 1 and rec[0] == ''):
          continue
        raw_count += 1
        if len(rec) != len(header):
          err_msg = f'expected {len(header)} columns, got {len(rec)}'
          if dlq_records is not None:
            dlq_records.append({
                'step': 'ReadCsvWithDlq',
                'error_type': 'CsvColumnCountMismatch',
                'error': err_msg,
                'raw_payload': json.dumps(rec),
            })
            continue
          raise SqlPipeError(f'malformed CSV row in {fp}: {err_msg}')
        try:
          row_d = {
              col_n: _coerce_csv_val(val, kinds[col_n], nulls)
              for col_n, val in zip(header, rec)
          }
          rows.append(row_d)
        except Exception as e:  # pylint: disable=broad-except
          if dlq_records is not None:
            dlq_records.append({
                'step': 'ReadCsvWithDlq',
                'error_type': type(e).__name__,
                'error': str(e),
                'raw_payload': json.dumps(dict(zip(header, rec))),
            })
            continue
          raise SqlPipeError(f'corrupt CSV value in {fp}: {e}') from None
    return cols, rows, raw_count

  if fmt == 'json':
    rows = []
    raw_count = 0
    col_types: dict[str, str] = {}
    try:
      eff_sample = dlq.resolve_json_sample_rows(
          files, ep.config.get('sample_rows'))
      allow_unknown = (
          dlq.parse_unknown_fields_opt(ep.config.get('on_unknown_fields')) ==
          'ignore')
    except ValueError as e:
      raise SqlPipeError(str(e)) from None
    # JSONL is typed from a sample like `beam pipe` does, so a value that
    # doesn't fit its column (amount "abc") or a late field absent from the
    # sample goes to --dlq or fails with a hint, instead of silently dropping
    # fields or reaching SQLite as text.
    kinds = _jsonl_kinds(files, sample_rows=eff_sample)
    if kinds is not None and ep.config.get('fields'):
      from . import cdc as cdc_mod
      try:
        parsed_types = dict(
            cdc_mod.parse_fields_shorthand(str(ep.config['fields'])))
        kinds = dlq.apply_type_overrides(
            kinds, parsed_types, str(ep.raw), allow_new_columns=True)
      except ValueError as e:
        raise SqlPipeError(str(e)) from None
    drift_count = 0
    drift_fields: set[str] = set()
    for fp in files:
      with _open_binary(fp) as bf:
        text = bf.read().decode('utf-8', errors='replace')
      stripped_all = text.strip()
      if stripped_all.startswith('['):
        parsed_arr = json.loads(stripped_all)
        if isinstance(parsed_arr, list):
          for obj in parsed_arr:
            raw_count += 1
            if isinstance(obj, dict):
              rows.append(obj)
              for k, v in obj.items():
                if v is not None and k not in col_types:
                  col_types[k] = (
                      'INTEGER' if isinstance(v, (bool, int)) else
                      'REAL' if isinstance(v, float) else 'TEXT')
                elif k not in col_types:
                  col_types[k] = 'TEXT'
          continue
      for line in text.splitlines():
        s = line.strip()
        if not s:
          continue
        raw_count += 1
        if kinds is not None:
          try:
            rows.append(
                dlq.parse_json_record(
                    s, kinds, _dict_row, allow_unknown_fields=allow_unknown))
          except Exception as e:  # pylint: disable=broad-except
            if dlq_records is None:
              raise SqlPipeError(dlq.json_fail_fast_message(s, e)) from None
            if isinstance(e, dlq.SchemaDriftError):
              drift_count += 1
              drift_fields.update(e.extra_fields)
            dlq_records.append(dlq.json_dlq_record(s, e))
          continue
        try:
          obj = json.loads(s)
          if not isinstance(obj, dict):
            raise ValueError('expected JSON object per line')
          rows.append(obj)
          for k, v in obj.items():
            if v is not None and (k not in col_types or col_types[k] == 'TEXT'):
              if isinstance(v, (bool, int)):
                col_types.setdefault(k, 'INTEGER')
              elif isinstance(v, float):
                col_types[k] = 'REAL'
              else:
                col_types.setdefault(k, 'TEXT')
            elif k not in col_types:
              col_types[k] = 'TEXT'
        except Exception as e:  # pylint: disable=broad-except
          if dlq_records is not None:
            dlq_records.append({
                'step': 'ReadFromJsonWithDlq',
                'error_type': type(e).__name__,
                'error': str(e),
                'raw_payload': s,
            })
            continue
          raise SqlPipeError(f'invalid JSON record in {fp}: {e}') from None
    if drift_count > 0 and warnings is not None:
      warnings.append(dlq.schema_drift_warning(drift_count, drift_fields))
    if kinds is not None:
      col_types = {
          k: ('INTEGER' if v in ('int', 'bool') else
              'REAL' if v == 'float' else 'TEXT')
          for k, v in kinds.items()
      }
    cols = list(col_types.items())
    return cols, rows, raw_count

  if fmt == 'avro':
    import fastavro
    rows = []
    col_types = {}
    for fp in files:
      with _open_binary(fp) as bf:
        for obj in fastavro.reader(bf):
          d = dict(obj)
          rows.append(d)
          for k, v in d.items():
            if k not in col_types and v is not None:
              col_types[k] = (
                  'INTEGER' if isinstance(v, (bool, int)) else
                  'REAL' if isinstance(v, float) else 'TEXT')
            elif k not in col_types:
              col_types[k] = 'TEXT'
    return list(col_types.items()), rows, len(rows)

  raise SqlPipeError(f'unsupported local --sql input format {fmt!r}')


def _dict_row(**fields):
  return dict(fields)


def _jsonl_kinds(files, *, sample_rows: int | None = 100):
  """Sampled JSONL field kinds, or None for JSON array files or no sample."""
  for fp in files:
    try:
      with _open_binary(fp) as bf:
        head = bf.read(256).decode('utf-8', errors='replace').lstrip()
    except OSError:
      return None
    if head.startswith('['):
      return None
  try:
    kinds = dlq.infer_json_kinds(list(files), sample_rows=sample_rows)
  except Exception:  # pylint: disable=broad-except
    return None
  return kinds or None


def _quote_ident(name: str) -> str:
  return '"' + name.replace('"', '""') + '"'


def _normalize_sqlite_param(v: Any) -> Any:
  if v is None or isinstance(v, (int, float, str, bytes)):
    return v
  if isinstance(v, bool):
    return 1 if v else 0
  return json.dumps(v, default=str)


def _register_scalar_functions(conn: sqlite3.Connection) -> None:
  conn.create_function(
      'CONCAT',
      -1,
      lambda *args: ''.join('' if a is None else str(a) for a in args),
      deterministic=True)
  conn.create_function(
      'FLOOR',
      1,
      lambda x: None if x is None else math.floor(float(x)),
      deterministic=True)
  conn.create_function(
      'CEIL',
      1,
      lambda x: None if x is None else math.ceil(float(x)),
      deterministic=True)
  conn.create_function(
      'CEILING',
      1,
      lambda x: None if x is None else math.ceil(float(x)),
      deterministic=True)
  conn.create_function(
      'POWER',
      2,
      lambda x, y: None if (x is None or y is None) else math.pow(
          float(x), float(y)),
      deterministic=True)
  conn.create_function(
      'SQRT',
      1,
      lambda x: None if x is None else math.sqrt(float(x)),
      deterministic=True)

  def _split_part(s: Any, delim: Any, idx: Any) -> str | None:
    if s is None or delim is None or idx is None:
      return None
    parts = str(s).split(str(delim))
    pos = int(idx)
    if pos <= 0 or pos > len(parts):
      return ''
    return parts[pos - 1]

  conn.create_function('SPLIT_PART', 3, _split_part, deterministic=True)
  def _to_utc(val: Any) -> str | None:
    """Normalizes ISO-8601 (Z / offset / naive) or epoch sec/ms to ISO UTC."""
    if val is None:
      return None
    s = str(val).strip()
    if not s:
      return None
    from datetime import datetime
    from datetime import timezone
    try:
      num = float(s)
      if s.lstrip('+-').replace('.', '', 1).isdigit():
        if abs(num) >= 1e11:
          num /= 1000.0
        dt = datetime.fromtimestamp(num, tz=timezone.utc)
        return (
            dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            if dt.microsecond == 0 else dt.isoformat().replace('+00:00', 'Z'))
    except (ValueError, OverflowError, OSError):
      pass
    iso = s[:-1] + '+00:00' if s.endswith(('Z', 'z')) else s
    dt = datetime.fromisoformat(iso)
    dt = (
        dt.replace(tzinfo=timezone.utc)
        if dt.tzinfo is None else dt.astimezone(timezone.utc))
    return (
        dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        if dt.microsecond == 0 else dt.isoformat().replace('+00:00', 'Z'))

  conn.create_function('TO_UTC', 1, _to_utc, deterministic=True)
  _register_aggregate_functions(conn)


class _Quantile:
  """Exact quantile with linear interpolation (numpy/pandas default).

  Registered by `_register_aggregate_functions`. `scale` is 1 for fractions
  (quantile(x, 0.95)) and 100 for percentages (percentile(x, 95)).
  """
  scale = 1.0

  def __init__(self):
    self._values: list[float] = []
    self._q_dec: decimal.Decimal | None = None

  def step(self, value, q=None):
    if q is not None:
      self._q_dec = (
          decimal.Decimal(str(q)) / decimal.Decimal(str(self.scale)))
    if value is not None:
      self._values.append(float(value))

  def finalize(self):
    if not self._values or self._q_dec is None:
      return None
    if not decimal.Decimal(0) <= self._q_dec <= decimal.Decimal(1):
      raise ValueError(
          f'quantile must be between 0 and 1 (percentile between 0 and 100), '
          f'got {float(self._q_dec) * self.scale}')
    vals = sorted(self._values)
    pos_dec = decimal.Decimal(len(vals) - 1) * self._q_dec
    lo = int(pos_dec)
    hi = min(lo + 1, len(vals) - 1)
    frac = float(pos_dec - lo)
    return vals[lo] + (vals[hi] - vals[lo]) * frac


class _Percentile(_Quantile):
  scale = 100.0


class _Median(_Quantile):
  def step(self, value):  # pylint: disable=arguments-differ
    self._q_dec = decimal.Decimal('0.5')
    super().step(value)


class _Stddev:
  """Sample standard deviation (STDDEV / STDDEV_SAMP)."""
  def __init__(self):
    self._values: list[float] = []

  def step(self, value):
    if value is not None:
      try:
        self._values.append(float(value))
      except (ValueError, TypeError):
        pass

  def finalize(self):
    if len(self._values) < 2:
      return None
    mean = sum(self._values) / len(self._values)
    return math.sqrt(
        sum((v - mean)**2 for v in self._values) / (len(self._values) - 1))


class _StddevPop:
  """Population standard deviation (STDDEV_POP)."""
  def __init__(self):
    self._values: list[float] = []

  def step(self, value):
    if value is not None:
      try:
        self._values.append(float(value))
      except (ValueError, TypeError):
        pass

  def finalize(self):
    if not self._values:
      return None
    mean = sum(self._values) / len(self._values)
    return math.sqrt(sum((v - mean)**2 for v in self._values) / len(self._values))


class _VarianceSamp:
  """Sample variance (VARIANCE / VAR_SAMP)."""
  def __init__(self):
    self._values: list[float] = []

  def step(self, value):
    if value is not None:
      try:
        self._values.append(float(value))
      except (ValueError, TypeError):
        pass

  def finalize(self):
    if len(self._values) < 2:
      return None
    mean = sum(self._values) / len(self._values)
    return sum((v - mean)**2 for v in self._values) / (len(self._values) - 1)


class _VariancePop:
  """Population variance (VAR_POP)."""
  def __init__(self):
    self._values: list[float] = []

  def step(self, value):
    if value is not None:
      try:
        self._values.append(float(value))
      except (ValueError, TypeError):
        pass

  def finalize(self):
    if not self._values:
      return None
    mean = sum(self._values) / len(self._values)
    return sum((v - mean)**2 for v in self._values) / len(self._values)


class _RegrAccumulator:
  """Numerically stable two-variable accumulator for linear regression (OLS).

  Maintains Welford running sums for (y, x) pairs where neither y nor x is null:
    n: count of pairs
    mean_x, mean_y: running sample means
    m2_x: sum of squared deviations for x
    m2_y: sum of squared deviations for y
    c_xy: sum of cross-products of deviations
  """
  def __init__(self):
    self.n = 0
    self.mean_x = 0.0
    self.mean_y = 0.0
    self.m2_x = 0.0
    self.m2_y = 0.0
    self.c_xy = 0.0

  def step(self, y, x):
    if y is None or x is None:
      return
    try:
      yf = float(y)
      xf = float(x)
    except (ValueError, TypeError):
      return
    self.n += 1
    dx = xf - self.mean_x
    self.mean_x += dx / self.n
    dy = yf - self.mean_y
    self.mean_y += dy / self.n
    self.m2_x += dx * (xf - self.mean_x)
    self.m2_y += dy * (yf - self.mean_y)
    self.c_xy += dx * (yf - self.mean_y)


class _RegrSlope(_RegrAccumulator):
  def finalize(self):
    if self.n < 2 or self.m2_x <= 0:
      return None
    return self.c_xy / self.m2_x


class _RegrIntercept(_RegrAccumulator):
  def finalize(self):
    if self.n < 2 or self.m2_x <= 0:
      return None
    slope = self.c_xy / self.m2_x
    return self.mean_y - slope * self.mean_x


class _RegrR2(_RegrAccumulator):
  def finalize(self):
    if self.n < 2 or self.m2_x <= 0 or self.m2_y <= 0:
      return None
    r = self.c_xy / math.sqrt(self.m2_x * self.m2_y)
    return r * r


class _Corr(_RegrAccumulator):
  def finalize(self):
    if self.n < 2 or self.m2_x <= 0 or self.m2_y <= 0:
      return None
    val = self.c_xy / math.sqrt(self.m2_x * self.m2_y)
    return max(-1.0, min(1.0, val))


class _CovarPop(_RegrAccumulator):
  def finalize(self):
    if self.n < 1:
      return None
    return self.c_xy / self.n


class _CovarSamp(_RegrAccumulator):
  def finalize(self):
    if self.n < 2:
      return None
    return self.c_xy / (self.n - 1)


class _RegrCount(_RegrAccumulator):
  def finalize(self):
    return self.n


class _RegrAvgX(_RegrAccumulator):
  def finalize(self):
    return self.mean_x if self.n > 0 else None


class _RegrAvgY(_RegrAccumulator):
  def finalize(self):
    return self.mean_y if self.n > 0 else None


class _RegrSXX(_RegrAccumulator):
  def finalize(self):
    return self.m2_x if self.n >= 2 else None


class _RegrSYY(_RegrAccumulator):
  def finalize(self):
    return self.m2_y if self.n >= 2 else None


class _RegrSXY(_RegrAccumulator):
  def finalize(self):
    return self.c_xy if self.n >= 2 else None


def _register_aggregate_functions(conn: sqlite3.Connection) -> None:
  """Statistical aggregates agents reach for that plain SQLite lacks.

  They are defined here (rather than relying on SQLite's optional percentile
  extension) so results are identical on every platform.
  """
  conn.create_aggregate('MEDIAN', 1, _Median)
  conn.create_aggregate('PERCENTILE', 2, _Percentile)
  conn.create_aggregate('QUANTILE', 2, _Quantile)
  conn.create_aggregate('QUANTILE_CONT', 2, _Quantile)
  conn.create_aggregate('STDDEV', 1, _Stddev)
  conn.create_aggregate('STDDEV_SAMP', 1, _Stddev)
  conn.create_aggregate('STDDEV_POP', 1, _StddevPop)
  conn.create_aggregate('VARIANCE', 1, _VarianceSamp)
  conn.create_aggregate('VAR_SAMP', 1, _VarianceSamp)
  conn.create_aggregate('VAR_POP', 1, _VariancePop)
  conn.create_aggregate('REGR_SLOPE', 2, _RegrSlope)
  conn.create_aggregate('REGR_INTERCEPT', 2, _RegrIntercept)
  conn.create_aggregate('REGR_R2', 2, _RegrR2)
  conn.create_aggregate('CORR', 2, _Corr)
  conn.create_aggregate('COVAR_POP', 2, _CovarPop)
  conn.create_aggregate('COVAR_SAMP', 2, _CovarSamp)
  conn.create_aggregate('REGR_COUNT', 2, _RegrCount)
  conn.create_aggregate('REGR_AVGX', 2, _RegrAvgX)
  conn.create_aggregate('REGR_AVGY', 2, _RegrAvgY)
  conn.create_aggregate('REGR_SXX', 2, _RegrSXX)
  conn.create_aggregate('REGR_SYY', 2, _RegrSYY)
  conn.create_aggregate('REGR_SXY', 2, _RegrSXY)


# Functions and syntax from other dialects mapped to what local SQL supports.
_SQL_DIALECT_HINTS = (
    (re.compile(r'within\s+group', re.I),
     'WITHIN GROUP is not supported by the default SQLite engine; use '
     'percentile(col, 95), quantile(col, 0.95) or median(col), or pass '
     '--sql-engine duckdb'),
    (re.compile(r'no such function: (percentile_cont|percentile_disc|'
                r'approx_quantiles|approx_percentile)', re.I),
     'use percentile(col, 95), quantile(col, 0.95) or median(col) (exact, '
     'linear interpolation)'),
    (re.compile(r'no such function: approx_count_distinct', re.I),
     'use COUNT(DISTINCT col)'),
    (re.compile(r'no such function: (date_trunc|format_timestamp|extract)',
                re.I),
     "use strftime(), e.g. strftime('%Y-%m', ts)"),
    (re.compile(
        r'no such function: (arg_max|arg_min|'
        r'max_by|min_by|mode|bool_and|bool_or|string_agg|array_agg)',
        re.I),
     'advanced warehouse aggregates require --sql-engine duckdb'),
    (re.compile(r'no such table|table .* does not exist|replacement scan', re.I),
     'name each input with --from ALIAS=PATH / --join ALIAS=PATH (inputs are '
     'pre-loaded as tables, never file paths; without an alias use the file '
     'name or FROM source)'),
)

_SQLITE_CAST_TYPE_MAP = {
    'string': 'TEXT',
    'bytes': 'BLOB',
    'int64': 'INTEGER',
    'float64': 'REAL',
    'bignumeric': 'REAL',
}


def _scan_sql_outside_literals(sql_text: str):
  """Yields `(is_code, segment)` preserving SQL quotes and comments."""
  n = len(sql_text)
  i = 0
  code_start = 0
  while i < n:
    ch = sql_text[i]
    if ch in ("'", '"', '`'):
      if i > code_start:
        yield True, sql_text[code_start:i]
      q = ch
      j = i + 1
      while j < n:
        if sql_text[j] == q:
          if j + 1 < n and sql_text[j + 1] == q:
            j += 2
            continue
          j += 1
          break
        j += 1
      yield False, sql_text[i:j]
      i = code_start = j
      continue
    if sql_text[i:i + 2] == '--':
      if i > code_start:
        yield True, sql_text[code_start:i]
      nl = sql_text.find('\n', i + 2)
      end = n if nl == -1 else nl + 1
      yield False, sql_text[i:end]
      i = code_start = end
      continue
    if sql_text[i:i + 2] == '/*':
      if i > code_start:
        yield True, sql_text[code_start:i]
      end_c = sql_text.find('*/', i + 2)
      end = n if end_c == -1 else end_c + 2
      yield False, sql_text[i:end]
      i = code_start = end
      continue
    i += 1
  if code_start < n:
    yield True, sql_text[code_start:]


def _normalize_sqlite_cast_types(sql_query: str) -> str:
  """Maps BigQuery/ANSI `CAST(... AS STRING|INT64|FLOAT64|BYTES)` to SQLite types.

  SQLite treats unknown type names without `CHAR`/`CLOB`/`TEXT` (including
  `STRING`) as `NUMERIC` affinity, which would silently coerce `'u1'` -> `0`!
  """
  def _sub_cast_type(m: re.Match) -> str:
    mapped = _SQLITE_CAST_TYPE_MAP.get(m.group(2).lower())
    return f'{m.group(1)}{mapped}{m.group(3)}' if mapped else m.group(0)

  cast_as_re = re.compile(
      r'(\bAS\s+)(STRING|BYTES|INT64|FLOAT64|BIGNUMERIC)(\s*\))', re.I)
  parts: list[str] = []
  for is_code, seg in _scan_sql_outside_literals(sql_query):
    parts.append(cast_as_re.sub(_sub_cast_type, seg) if is_code else seg)
  return ''.join(parts)


def _rewrite_source_uri_table_refs(
    sql_query: str, named_sources: list[NamedSource]) -> str:
  """Rewrites `FROM "delta://..."` / `FROM "iceberg://..."` matching a lakehouse input to its alias."""
  uri_to_alias: dict[str, str] = {}
  for ns in named_sources:
    if ns.endpoint.kind not in ('delta', 'iceberg'):
      continue
    quoted = _quote_ident(ns.alias)
    for cand in (ns.uri, ns.endpoint.raw):
      if isinstance(cand, str) and cand.strip():
        uri_to_alias[cand.strip()] = quoted
  if not uri_to_alias:
    return sql_query

  segs = list(_scan_sql_outside_literals(sql_query))
  out: list[str] = []
  for idx, (is_code, seg) in enumerate(segs):
    if (not is_code and len(seg) >= 2 and seg[0] in ('"', '`') and
        seg[-1] == seg[0] and idx > 0 and segs[idx - 1][0]):
      prev_code = segs[idx - 1][1]
      if re.search(r'\b(?:FROM|JOIN)\s*$', prev_code, re.I):
        unq = seg[1:-1].replace(seg[0] * 2, seg[0]).strip()
        if unq in uri_to_alias:
          out.append(uri_to_alias[unq])
          continue
    out.append(seg)
  return ''.join(out)


def _sql_error_hint(message: str, query: str) -> str:
  """Returns a '; hint: ...' suffix for a local SQL error, or ''."""
  for pattern, hint in _SQL_DIALECT_HINTS:
    if pattern.search(message) or (
        pattern.pattern.startswith('within') and pattern.search(query)):
      return f'; hint: {hint}'
  if 'no such function' in message.lower():
    return (
        '; hint: local SQL is SQLite plus to_utc, median, percentile, '
        'quantile, stddev, concat, split_part, floor, ceil, power and sqrt; '
        'for ANSI analytics pass --sql-engine duckdb')
  return ''


# Functions and clauses that local `--sql` engines (SQLite / DuckDB) accept
# but Beam's distributed SQL dialect (Apache Calcite) does not.
_LOCAL_ONLY_SQL_FUNCTIONS = (
    'median',
    'percentile',
    'quantile',
    'quantile_cont',
    'percentile_cont',
    'percentile_disc',
    'approx_quantiles',
    'split_part',
    'to_utc',
    'strftime',
    'julianday',
    'unixepoch',
    'ifnull',
    'iif',
    'group_concat',
    'string_agg',
    'printf',
    'instr',
    'typeof',
    'json_extract',
    'json_group_array',
    'json_object',
)

_LOCAL_ONLY_SQL_KEYWORDS = ('qualify', 'glob', 'regexp', 'ilike')


def analyze_sql_portability(
    query: str, *, sql_engine: str = 'sqlite') -> list[str]:
  """Returns non-portable SQL functions/clauses used outside string literals."""
  del sql_engine
  code_only = ' '.join(
      seg for is_code, seg in _scan_sql_outside_literals(query)
      if is_code).lower()
  found: list[str] = []
  for fn in _LOCAL_ONLY_SQL_FUNCTIONS:
    if re.search(r'\b' + fn + r'\s*\(', code_only):
      found.append(f'{fn}()')
  for kw in _LOCAL_ONLY_SQL_KEYWORDS:
    if re.search(r'\b' + kw + r'\b', code_only):
      found.append(kw.upper())
  return found


def local_only_sql_functions(query: str) -> list[str]:
  """Names of local-only constructs used in `query`."""
  return [item.rstrip('()').lower() for item in analyze_sql_portability(query)]


def _normalize_sqlite_bool_comparisons(
    sql_query: str, bool_cols: set[str]) -> str:
  """Coerces `bool_col = 'true'|'false'` to `1|0` for SQLite INTEGER booleans."""
  if not bool_cols:
    return sql_query
  segs = list(_scan_sql_outside_literals(sql_query))
  out: list[str] = []
  lhs_re = re.compile(
      r'(?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)?([A-Za-z_][A-Za-z0-9_]*)'
      r'\s*(?:=|==|!=|<>|\bIS(?:\s+NOT)?)\s*$',
      re.I)
  op_only_re = re.compile(r'^\s*(?:=|==|!=|<>|\bIS(?:\s+NOT)?)\s*$', re.I)
  rhs_re = re.compile(
      r'^\s*(?:=|==|!=|<>)\s*(?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)?'
      r'([A-Za-z_][A-Za-z0-9_]*)\b',
      re.I)
  rhs_op_re = re.compile(
      r'^\s*(?:=|==|!=|<>)\s*(?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)?$')

  for idx, (is_code, seg) in enumerate(segs):
    if (not is_code and len(seg) >= 2 and seg[0] == "'" and seg[-1] == "'"):
      inner = seg[1:-1].replace("''", "'").strip().lower()
      if inner in ('true', 'false'):
        bit = '1' if inner == 'true' else '0'
        matched = False
        if idx > 0 and segs[idx - 1][0]:
          m_l = lhs_re.search(segs[idx - 1][1])
          if m_l and m_l.group(1).lower() in bool_cols:
            matched = True
          elif (idx >= 2 and not segs[idx - 2][0] and
                segs[idx - 2][1][:1] in ('"', '`') and
                op_only_re.fullmatch(segs[idx - 1][1])):
            unq = segs[idx - 2][1][1:-1].strip().lower()
            if unq in bool_cols:
              matched = True
        if not matched and idx + 1 < len(segs) and segs[idx + 1][0]:
          m_r = rhs_re.match(segs[idx + 1][1])
          if m_r and m_r.group(1).lower() in bool_cols:
            matched = True
          elif (idx + 2 < len(segs) and not segs[idx + 2][0] and
                segs[idx + 2][1][:1] in ('"', '`') and
                rhs_op_re.fullmatch(segs[idx + 1][1])):
            unq = segs[idx + 2][1][1:-1].strip().lower()
            if unq in bool_cols:
              matched = True
        if matched:
          out.append(bit)
          continue
    out.append(seg)
  return ''.join(out)


# Local `--sql` engines. SQLite ships with Python; DuckDB is optional
# (`pip install duckdb`) and adds ANSI analytics such as
# `PERCENTILE_CONT ... WITHIN GROUP`, `QUALIFY`, struct field access and
# `list` functions.
SQL_ENGINES = ('sqlite', 'duckdb')
DUCKDB_INSTALL_HINT = "pip install 'duckdb>=1.1'"


class SqlEngineMissingError(SqlPipeError):
  """The requested `--sql-engine` is not installed."""


def duckdb_version() -> str | None:
  """Installed DuckDB version, without importing it, or None."""
  try:
    from importlib import metadata
    return metadata.version('duckdb')
  except Exception:  # pylint: disable=broad-except
    return None


_ARROW_TYPES_FOR_SQLITE = {'INTEGER': 'int64', 'REAL': 'float64'}


def _arrow_table(cols, rows):
  """Builds a pyarrow Table from loaded rows, keeping nested values."""
  import pyarrow as pa
  names = [c for c, _ in cols] or (list(rows[0]) if rows else ['_empty'])
  types = dict(cols)
  arrays = []
  for name in names:
    values = [r.get(name) for r in rows]
    declared = _ARROW_TYPES_FOR_SQLITE.get(types.get(name, ''), None)
    try:
      if declared and not any(isinstance(v, (dict, list)) for v in values):
        arrays.append(pa.array(values, type=declared))
      else:
        arrays.append(pa.array(values) if values else pa.array(
            [], type=pa.string()))
    except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError):
      # Mixed types in one column: keep the values as text.
      arrays.append(
          pa.array([
              None if v is None else
              (json.dumps(v, default=str) if isinstance(v, (dict, list)) else
               str(v)) for v in values
          ],
                   type=pa.string()))
  return pa.Table.from_arrays(arrays, names=names)


def _run_duckdb(
    named_sources, clean_q, *, default_project, dlq_records, warnings=None):
  """Runs one read-only SELECT in a sandboxed in-memory DuckDB.

  Inputs are loaded by the same readers as the SQLite engine (typing, NULLs,
  DLQ) and registered as in-memory Arrow tables. The connection then has
  file system access, extension install/load and configuration changes
  disabled, so SQL can't read or write files (`read_csv`, `COPY`, `ATTACH`).
  """
  try:
    import duckdb
  except ImportError:
    raise SqlEngineMissingError(
        f'--sql-engine duckdb needs the duckdb package: {DUCKDB_INSTALL_HINT} '
        '(check with: beam doctor)') from None
  clean_q = _rewrite_source_uri_table_refs(clean_q, named_sources)
  total_read = 0
  con = duckdb.connect(
      ':memory:',
      config={
          'enable_external_access': False,
          'autoinstall_known_extensions': False,
          'autoload_known_extensions': False,
      })
  try:
    statements = con.extract_statements(clean_q)
    if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
      raise SqlPipeError('--sql only accepts a single read-only SELECT query')
    first_alias = None
    used_lower = set()
    for ns in named_sources:
      cols, rows, raw_cnt = load_endpoint_rows(
          ns.endpoint,
          default_project=default_project,
          dlq_records=dlq_records,
          warnings=warnings)
      total_read += raw_cnt
      con.register(ns.alias, _arrow_table(cols, rows))
      used_lower.add(ns.alias.lower())
      first_alias = first_alias or ns.alias
    if first_alias is not None:
      extra_views = (
          ('PCOLLECTION', 'source', 'data', 'tbl', 'input')
          if len(named_sources) == 1 else ('PCOLLECTION', 'source'))
      for view_name in extra_views:
        if view_name.lower() not in used_lower:
          con.execute(
              f'CREATE VIEW {_quote_ident(view_name)} AS '
              f'SELECT * FROM {_quote_ident(first_alias)}')
    con.execute("SET TimeZone = 'UTC'")
    con.execute(r'''
      CREATE MACRO to_utc(x) AS (
        CASE
          WHEN x IS NULL OR trim(CAST(x AS VARCHAR)) = '' THEN NULL
          WHEN regexp_matches(trim(CAST(x AS VARCHAR)), '^[+-]?[0-9]+(\.[0-9]+)?$') THEN
            strftime(
              to_timestamp(
                CASE WHEN abs(TRY_CAST(x AS DOUBLE)) >= 1e11
                     THEN TRY_CAST(x AS DOUBLE) / 1000.0
                     ELSE TRY_CAST(x AS DOUBLE) END
              ) AT TIME ZONE 'UTC',
              '%Y-%m-%dT%H:%M:%SZ'
            )
          ELSE
            strftime(
              CAST(trim(CAST(x AS VARCHAR)) AS TIMESTAMPTZ) AT TIME ZONE 'UTC',
              '%Y-%m-%dT%H:%M:%SZ'
            )
        END
      )
    ''')
    raw_mem = os.environ.get('BEAM_CLI_DUCKDB_MAX_MEMORY', '').strip() or '512MB'
    if not re.fullmatch(r'\d+\s*(?:KB|MB|GB|KiB|MiB|GiB)', raw_mem, re.I):
      raw_mem = '512MB'
    raw_max_rows = os.environ.get('BEAM_CLI_SQL_MAX_RESULT_ROWS', '').strip()
    max_sql_rows = int(raw_max_rows) if raw_max_rows else 1_000_000
    con.execute(f"SET memory_limit = '{raw_mem}'")
    con.execute("SET max_temp_directory_size = '0B'")
    con.execute('SET threads = 2')
    con.execute('SET lock_configuration = true')
    try:
      cur = con.execute(clean_q)
      out_cols = [d[0] for d in (cur.description or [])]
      fetched: list[Any] = []
      while True:
        chunk = cur.fetchmany(4096)
        if not chunk:
          break
        fetched.extend(chunk)
        if len(fetched) > max_sql_rows:
          raise SqlPipeError(
              f'SQL query exceeded local result limit ({max_sql_rows:,} rows); '
              'add LIMIT or run distributed SQL with --runner=DataflowRunner')
    except duckdb.Error as e:
      avail_list = [ns.alias for ns in named_sources]
      if len(named_sources) == 1 and 'source' not in used_lower:
        avail_list.append('source')
      avail = ', '.join(avail_list)
      first_line = str(e).splitlines()[0]
      raise SqlPipeError(
          f'SQL execution error (DuckDB): {first_line} '
          f'(available tables: {avail})'
          f'{_sql_error_hint(first_line, clean_q)}') from None
    return out_cols, [dict(zip(out_cols, t)) for t in fetched], total_read
  finally:
    con.close()


def run_in_memory_sql(
    named_sources: list[NamedSource],
    sql_query: str,
    *,
    default_project: str | None = None,
    enable_dlq: bool = False,
    engine: str = 'sqlite',
    warnings: list[str] | None = None,
) -> tuple[list[str], list[dict[str, Any]], list[dict[str, Any]], int]:
  """Executes `sql_query` in a sandboxed in-memory SQLite or DuckDB database.

  Returns `(out_columns, out_rows, dlq_records, total_rows_read)`.
  """
  if engine not in SQL_ENGINES:
    raise SqlPipeError(
        f'unknown --sql-engine {engine!r}; use one of {", ".join(SQL_ENGINES)}')
  clean_q = validate_readonly_select_sql(sql_query)
  dlq_records: list[dict[str, Any]] | None = [] if enable_dlq else None
  if engine == 'duckdb':
    out_cols, out_rows, total_read = _run_duckdb(
        named_sources,
        clean_q,
        default_project=default_project,
        dlq_records=dlq_records,
        warnings=warnings)
    return out_cols, out_rows, (dlq_records or []), total_read
  clean_q = _normalize_sqlite_cast_types(
      _rewrite_source_uri_table_refs(clean_q, named_sources))
  total_read = 0

  conn = sqlite3.connect(':memory:')
  try:
    _register_scalar_functions(conn)
    used_lower: set[str] = set()
    first_alias: str | None = None
    bool_cols: set[str] = set()
    non_bool_cols: set[str] = set()

    for ns in named_sources:
      cols, rows, raw_cnt = load_endpoint_rows(
          ns.endpoint,
          default_project=default_project,
          dlq_records=dlq_records,
          warnings=warnings)
      total_read += raw_cnt
      if not cols and rows:
        cols = [(k, 'TEXT') for k in rows[0].keys()]
      if not cols:
        # Empty dataset with no schema columns -> dummy placeholder column
        cols = [('_empty', 'TEXT')]
      col_defs = ', '.join(f'{_quote_ident(cn)} {ct}' for cn, ct in cols)
      t_ident = _quote_ident(ns.alias)
      conn.execute(f'CREATE TABLE {t_ident} ({col_defs})')
      if rows:
        col_names = [cn for cn, _ in cols]
        for cn in col_names:
          non_null_vals = [r.get(cn) for r in rows if r.get(cn) is not None]
          if non_null_vals and all(isinstance(v, bool) for v in non_null_vals):
            bool_cols.add(cn.lower())
          elif non_null_vals:
            non_bool_cols.add(cn.lower())
        placeholders = ', '.join('?' for _ in col_names)
        col_list_sql = ', '.join(_quote_ident(cn) for cn in col_names)
        batch = [
            tuple(_normalize_sqlite_param(r.get(cn)) for cn in col_names)
            for r in rows
        ]
        conn.executemany(
            f'INSERT INTO {t_ident} ({col_list_sql}) VALUES ({placeholders})',
            batch)
      used_lower.add(ns.alias.lower())
      if first_alias is None:
        first_alias = ns.alias

    pure_bool_cols = bool_cols - non_bool_cols
    if pure_bool_cols:
      clean_q = _normalize_sqlite_bool_comparisons(clean_q, pure_bool_cols)

    # Register Beam SQL `PCOLLECTION` and `source` views on the primary input
    # (plus common single-table aliases `data`, `t`, `tbl`, `input` when unary).
    if first_alias is not None:
      extra_views = (
          ('PCOLLECTION', 'source', 'data', 'tbl', 'input')
          if len(named_sources) == 1 else ('PCOLLECTION', 'source'))
      for view_name in extra_views:
        if view_name.lower() not in used_lower:
          conn.execute(
              f'CREATE VIEW {_quote_ident(view_name)} AS '
              f'SELECT * FROM {_quote_ident(first_alias)}')

    # Lock down SQLite authorizer BEFORE executing user SQL: strictly read-only
    # SELECT/FUNCTION/READ, blocking ATTACH, PRAGMA, DDL, and DML!
    allowed_actions = {
        sqlite3.SQLITE_SELECT,
        sqlite3.SQLITE_READ,
        sqlite3.SQLITE_FUNCTION,
    }
    if hasattr(sqlite3, 'SQLITE_RECURSIVE'):
      allowed_actions.add(sqlite3.SQLITE_RECURSIVE)

    def _readonly_authorizer(action_code, arg1, arg2, dbname, source):
      del arg1, arg2, dbname, source
      if action_code in allowed_actions:
        return sqlite3.SQLITE_OK
      return sqlite3.SQLITE_DENY

    import time as _time
    raw_max_sec = os.environ.get('BEAM_CLI_SQL_MAX_SECONDS', '').strip()
    max_sql_sec = float(raw_max_sec) if raw_max_sec else 30.0
    raw_max_rows = os.environ.get('BEAM_CLI_SQL_MAX_RESULT_ROWS', '').strip()
    max_sql_rows = int(raw_max_rows) if raw_max_rows else 1_000_000
    sql_deadline = _time.monotonic() + max_sql_sec
    timed_out = False

    def _progress_guard() -> int:
      nonlocal timed_out
      if _time.monotonic() > sql_deadline:
        timed_out = True
        return 1
      return 0

    conn.set_progress_handler(_progress_guard, 10_000)
    conn.set_authorizer(_readonly_authorizer)
    try:
      cur = conn.execute(clean_q)
      out_cols = [d[0] for d in (cur.description or [])]
      fetched: list[Any] = []
      while True:
        chunk = cur.fetchmany(4096)
        if not chunk:
          break
        fetched.extend(chunk)
        if len(fetched) > max_sql_rows:
          raise SqlPipeError(
              f'SQL query exceeded local result limit ({max_sql_rows:,} rows); '
              'add LIMIT or run distributed SQL with --runner=DataflowRunner')
    except sqlite3.Error as e:
      if timed_out or 'interrupted' in str(e).lower():
        raise SqlPipeError(
            f'SQL query exceeded local execution time limit '
            f'({max_sql_sec:g}s); check for unbounded recursive CTEs or '
            'cross joins') from None
      avail_list = [ns.alias for ns in named_sources]
      if len(named_sources) == 1 and 'source' not in used_lower:
        avail_list.append('source')
      avail = ', '.join(avail_list)
      raise SqlPipeError(
          f'SQL execution error: {e} (available tables: {avail})'
          f'{_sql_error_hint(str(e), clean_q)}') from None

    out_rows = [dict(zip(out_cols, tup)) for tup in fetched]
    return out_cols, out_rows, (dlq_records or []), total_read
  finally:
    conn.close()


# `--to out.json --to-opt key=COL [--to-opt value=COL]`: one JSON document
# keyed by COL instead of JSON lines, for answers such as
# {"West": {"events": 6, ...}, "East": {...}}.
KEYED_JSON_OPTIONS = frozenset(['key', 'value'])


def keyed_json_options(sink_ep) -> dict[str, str] | None:
  """The key/value options of a keyed JSON document sink, or None."""
  if sink_ep is None or 'key' not in (sink_ep.config or {}):
    return None
  target = str(sink_ep.output_prefix or sink_ep.config.get('path') or '')
  if sink_ep.format != 'json' or not target.lower().endswith('.json'):
    raise SqlPipeError(
        '--to-opt key= writes one JSON document, so --to must be a local '
        '.json file (not .jsonl or another format)')
  return {k: str(sink_ep.config[k]) for k in KEYED_JSON_OPTIONS
          if k in sink_ep.config}


def _embedded_json(value):
  """JSON text from json_object()/json_group_array() is embedded as JSON."""
  if isinstance(value, str) and value[:1] in ('[', '{'):
    try:
      return json.loads(value)
    except ValueError:
      return value
  return value


def keyed_json_document(columns, rows, key: str, value: str | None = None):
  """`{row[key]: {other columns} | row[value]}`; fails on missing/dup keys."""
  if key not in columns:
    raise SqlPipeError(
        f'--to-opt key={key}: no such output column (columns: '
        f'{", ".join(columns)})')
  if value is not None and value not in columns:
    raise SqlPipeError(
        f'--to-opt value={value}: no such output column (columns: '
        f'{", ".join(columns)})')
  doc: dict[str, Any] = {}
  for r in rows:
    k = r.get(key)
    if k is None:
      raise SqlPipeError(
          f'--to-opt key={key}: a row has NULL {key}; replace it first, for '
          f"example COALESCE({key}, 'UNMAPPED')")
    k = str(k)
    if k in doc:
      raise SqlPipeError(
          f'--to-opt key={key}: value {k!r} appears more than once; '
          f'aggregate to one row per {key} first (GROUP BY {key})')
    if value is not None:
      doc[k] = _embedded_json(r.get(value))
    else:
      doc[k] = {c: _embedded_json(r.get(c)) for c in columns if c != key}
  return doc


def _new_sql_stage_path(target: str, tag: str) -> str:
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


def write_json_document(path: str, doc: Any) -> str:
  tmp_path = _new_sql_stage_path(path, 'doc')
  try:
    with open(tmp_path, 'w', encoding='utf-8') as f:
      json.dump(doc, f, indent=2, default=str)
      f.write('\n')
    os.replace(tmp_path, path)
  finally:
    if os.path.exists(tmp_path):
      try:
        os.unlink(tmp_path)
      except OSError:
        pass
  return path


def _mixed_columns_as_text(columns, rows, warnings=None):
  import pyarrow as pa
  arrays = {}
  for col in columns:
    values = [r.get(col) for r in rows]
    try:
      arrays[col] = pa.array(values)
    except (pa.ArrowInvalid, pa.ArrowTypeError):
      if warnings is not None:
        warnings.append(
            f'column {col!r} mixes numbers and text, so it was written as '
            f'text; use CAST({col} AS REAL) in --sql (non-numbers become '
            'NULL) or fix the source values')
      arrays[col] = pa.array([
          None if v is None else
          json.dumps(v, default=str) if isinstance(v, (dict, list)) else str(v)
          for v in values
      ],
                             type=pa.string())
  return pa.table(arrays)


def stage_rows_to_local_file(
    out_cols: list[str],
    out_rows: list[dict[str, Any]],
    sink_ep: uris.ParsedEndpoint,
    *,
    if_exists: str = 'fail',
    warnings: list[str] | None = None,
    tag: str = 'sql',
) -> tuple[str, str]:
  """Writes `out_rows` to an exclusive sibling `.tmp` file, returning `(tmp_path, final_path)`."""
  target = sink_ep.output_prefix or sink_ep.config.get('path')
  if not target:
    raise SqlPipeError(f'missing target path for sink {sink_ep.raw!r}')

  keyed = keyed_json_options(sink_ep)
  if keyed is not None:
    if if_exists == 'append':
      raise SqlPipeError('--to-opt key= cannot be combined with --if-exists append')
    doc = keyed_json_document(out_cols, out_rows, keyed['key'], keyed.get('value'))
    tmp_path = _new_sql_stage_path(target, tag)
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

  ext = os.path.splitext(target)[1].lower()
  has_explicit_ext = ext in (
      '.parquet',
      '.pq',
      '.json',
      '.jsonl',
      '.ndjson',
      '.csv',
      '.tsv',
      '.txt',
  )
  final_path = target if has_explicit_ext else f'{target}-00000-of-00001'
  rows_to_write = list(out_rows)
  if if_exists == 'append' and os.path.isfile(final_path):
    prev_ep = uris.parse_endpoint(
        final_path, is_read=True, explicit_format=sink_ep.format)
    _, prev_rows, _ = load_endpoint_rows(prev_ep)
    rows_to_write = prev_rows + rows_to_write
  tmp_path = _new_sql_stage_path(final_path, tag)

  fmt = sink_ep.format
  try:
    if fmt == 'parquet':
      import pyarrow as pa
      import pyarrow.parquet as pq
      if rows_to_write:
        try:
          tbl = pa.Table.from_pylist(rows_to_write)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
          # A column holds mixed types (SQLite is dynamically typed): write
          # those columns as text rather than failing the whole run.
          tbl = _mixed_columns_as_text(out_cols, rows_to_write, warnings)
      else:
        tbl = pa.table({c: pa.array([], type=pa.string()) for c in out_cols})
      pq.write_table(tbl, tmp_path)
    elif fmt == 'csv':
      delim = str(sink_ep.config.get('delimiter', ',') or ',')
      with open(tmp_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(
            f, fieldnames=out_cols, delimiter=delim, extrasaction='ignore')
        writer.writeheader()
        for r in rows_to_write:
          writer.writerow({
              k: ('' if v is None else json.dumps(v, default=str)
                  if isinstance(v, (dict, list)) else v)
              for k, v in r.items()
          })
    else:
      # Default JSONL (`json` / `text`).
      with open(tmp_path, 'w', encoding='utf-8') as f:
        for r in rows_to_write:
          f.write(json.dumps(r, default=str) + '\n')
  except BaseException:
    if os.path.exists(tmp_path):
      try:
        os.unlink(tmp_path)
      except OSError:
        pass
    raise
  return tmp_path, final_path


def write_rows_to_local_file(
    out_cols: list[str],
    out_rows: list[dict[str, Any]],
    sink_ep: uris.ParsedEndpoint,
    *,
    if_exists: str = 'fail',
    warnings: list[str] | None = None,
) -> list[str]:
  """Writes `out_rows` directly to a local file (`parquet`, `json`, `csv`)."""
  tmp_path, final_path = stage_rows_to_local_file(
      out_cols, out_rows, sink_ep, if_exists=if_exists, warnings=warnings)
  try:
    os.replace(tmp_path, final_path)
  finally:
    if os.path.exists(tmp_path):
      try:
        os.unlink(tmp_path)
      except OSError:
        pass
  return [final_path]
