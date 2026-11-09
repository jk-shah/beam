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

    ep = uris.parse_endpoint(
        target_uri,
        is_read=True,
        explicit_format=from_format,
        options=dict(from_opts or {}),
        default_project=default_project,
        allow_sniff=allow_sniff,
        allow_jinja=allow_jinja)
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
    transforms.append({
        'name': read_name,
        'type': ns.endpoint.yaml_type,
        'config': dict(ns.endpoint.config),
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


def _match_source_files(path_pattern: str) -> list[str]:
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
) -> tuple[list[tuple[str, str]], list[dict[str, Any]], int]:
  """Reads `(columns_with_sqlite_types, rows, raw_rows_read)` from `ep`."""
  from . import lakehouse

  if ep.kind == 'delta':
    snap, rows = lakehouse.read_delta_rows_local(ep.config['path'], limit=None)
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
        null_values=nulls)
    cols = [
        (
            c,
            'INTEGER' if kinds[c] in ('int', 'bool') else
            'REAL' if kinds[c] == 'float' else 'TEXT',
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
  _register_aggregate_functions(conn)


class _Quantile:
  """Exact quantile with linear interpolation (numpy/pandas default).

  Registered by `_register_aggregate_functions`. `scale` is 1 for fractions
  (quantile(x, 0.95)) and 100 for percentages (percentile(x, 95)).
  """
  scale = 1.0

  def __init__(self):
    self._values: list[float] = []
    self._q: float | None = None

  def step(self, value, q=None):
    if q is not None:
      self._q = float(q) / self.scale
    if value is not None:
      self._values.append(float(value))

  def finalize(self):
    if not self._values or self._q is None:
      return None
    if not 0.0 <= self._q <= 1.0:
      raise ValueError(
          f'quantile must be between 0 and 1 (percentile between 0 and 100), '
          f'got {self._q * self.scale}')
    vals = sorted(self._values)
    pos = (len(vals) - 1) * self._q
    lo = math.floor(pos)
    hi = min(lo + 1, len(vals) - 1)
    return vals[lo] + (vals[hi] - vals[lo]) * (pos - lo)


class _Percentile(_Quantile):
  scale = 100.0


class _Median(_Quantile):
  def step(self, value):  # pylint: disable=arguments-differ
    self._q = 0.5
    super().step(value)


class _Stddev:
  """Sample standard deviation (STDDEV / STDDEV_SAMP)."""
  def __init__(self):
    self._values: list[float] = []

  def step(self, value):
    if value is not None:
      self._values.append(float(value))

  def finalize(self):
    if len(self._values) < 2:
      return None
    mean = sum(self._values) / len(self._values)
    return math.sqrt(
        sum((v - mean)**2 for v in self._values) / (len(self._values) - 1))


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


# Functions and syntax from other dialects mapped to what local SQL supports.
_SQL_DIALECT_HINTS = (
    (re.compile(r'within\s+group', re.I),
     'WITHIN GROUP is not supported locally; use percentile(col, 95) or '
     'quantile(col, 0.95) (exact, linear interpolation) or median(col)'),
    (re.compile(r'no such function: (percentile_cont|percentile_disc|'
                r'approx_quantiles|approx_percentile)', re.I),
     'use percentile(col, 95), quantile(col, 0.95) or median(col) (exact, '
     'linear interpolation)'),
    (re.compile(r'no such function: approx_count_distinct', re.I),
     'use COUNT(DISTINCT col)'),
    (re.compile(r'no such function: (date_trunc|format_timestamp|extract)',
                re.I),
     "use strftime(), e.g. strftime('%Y-%m', ts)"),
    (re.compile(r'no such table', re.I),
     'name each input with --from ALIAS=PATH / --join ALIAS=PATH and use the '
     'alias in FROM; without an alias the table is named after the file'),
)


def _sql_error_hint(message: str, query: str) -> str:
  """Returns a '; hint: ...' suffix for a local SQL error, or ''."""
  for pattern, hint in _SQL_DIALECT_HINTS:
    if pattern.search(message) or (
        pattern.pattern.startswith('within') and pattern.search(query)):
      return f'; hint: {hint}'
  if 'no such function' in message.lower():
    return (
        '; hint: local SQL is SQLite plus median, percentile, quantile, '
        'stddev, concat, split_part, floor, ceil, power and sqrt')
  return ''


# Functions that local `--sql` runs (SQLite + `_register_*_functions`) accept
# but Beam's remote SQL dialect (Calcite) does not.
_LOCAL_ONLY_SQL_FUNCTIONS = (
    'median', 'percentile', 'quantile', 'quantile_cont', 'split_part',
    'strftime', 'julianday', 'ifnull', 'group_concat', 'printf', 'instr')


def local_only_sql_functions(query: str) -> list[str]:
  """Names of local-only functions used in `query`.

  `beam pipe --sql` warns with these when it exports Beam YAML (`--emit-yaml`)
  or runs remotely, because the same query would fail in the remote dialect.
  """
  low = query.lower()
  return [
      fn for fn in _LOCAL_ONLY_SQL_FUNCTIONS
      if re.search(r'\b' + fn + r'\s*\(', low)
  ]


def run_in_memory_sql(
    named_sources: list[NamedSource],
    sql_query: str,
    *,
    default_project: str | None = None,
    enable_dlq: bool = False,
) -> tuple[list[str], list[dict[str, Any]], list[dict[str, Any]], int]:
  """Executes `sql_query` in a sandboxed `:memory:` SQLite DB.

  Returns `(out_columns, out_rows, dlq_records, total_rows_read)`.
  """
  clean_q = validate_readonly_select_sql(sql_query)
  dlq_records: list[dict[str, Any]] | None = [] if enable_dlq else None
  total_read = 0

  conn = sqlite3.connect(':memory:')
  try:
    _register_scalar_functions(conn)
    used_lower: set[str] = set()
    first_alias: str | None = None

    for ns in named_sources:
      cols, rows, raw_cnt = load_endpoint_rows(
          ns.endpoint, default_project=default_project, dlq_records=dlq_records)
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

    # Register Beam SQL `PCOLLECTION` and `source` views on the primary input.
    if first_alias is not None:
      for view_name in ('PCOLLECTION', 'source'):
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

    conn.set_authorizer(_readonly_authorizer)
    try:
      cur = conn.execute(clean_q)
      out_cols = [d[0] for d in (cur.description or [])]
      fetched = cur.fetchall()
    except sqlite3.Error as e:
      avail = ', '.join(ns.alias for ns in named_sources)
      raise SqlPipeError(
          f'SQL execution error: {e} (available tables: {avail})'
          f'{_sql_error_hint(str(e), clean_q)}') from None

    out_rows = [dict(zip(out_cols, tup)) for tup in fetched]
    return out_cols, out_rows, (dlq_records or []), total_read
  finally:
    conn.close()


def write_rows_to_local_file(
    out_cols: list[str],
    out_rows: list[dict[str, Any]],
    sink_ep: uris.ParsedEndpoint,
    *,
    if_exists: str = 'fail',
) -> list[str]:
  """Writes `out_rows` directly to a local file (`parquet`, `json`, `csv`)."""
  target = sink_ep.output_prefix or sink_ep.config.get('path')
  if not target:
    raise SqlPipeError(f'missing target path for sink {sink_ep.raw!r}')
  parent = os.path.dirname(os.path.abspath(target))
  if parent:
    os.makedirs(parent, exist_ok=True)

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
  tmp_path = f'{final_path}.tmp.{os.getpid()}'

  fmt = sink_ep.format
  try:
    if fmt == 'parquet':
      import pyarrow as pa
      import pyarrow.parquet as pq
      if rows_to_write:
        tbl = pa.Table.from_pylist(rows_to_write)
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
          writer.writerow({k: ('' if v is None else v) for k, v in r.items()})
    else:
      # Default JSONL (`json` / `text`).
      with open(tmp_path, 'w', encoding='utf-8') as f:
        for r in rows_to_write:
          f.write(json.dumps(r, default=str) + '\n')
    os.replace(tmp_path, final_path)
  finally:
    if os.path.exists(tmp_path):
      try:
        os.unlink(tmp_path)
      except OSError:
        pass
  return [final_path]
