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

"""Pre-flight `--max-bytes` cost & scan guardrail (`EXIT_COST_LIMIT = 8`).

Estimators:
- Local / cloud files: sums matched object byte sizes, short-circuiting as soon
  as `estimated_bytes > max_bytes`.
- BigQuery: `$0` `tables.get` (`table.num_bytes`) for plain `TABLE` reads with
  no column/row pruning; `$0` `jobs.insert(dryRun=True)` (`total_bytes_processed`)
  for `VIEW`, `MATERIALIZED_VIEW`, `EXTERNAL`, `SNAPSHOT`, or when `query`,
  `fields`, or `row_restriction` is present!
- Postgres: `SELECT COALESCE(SUM(pg_relation_size(relid)), 0) FROM
  pg_partition_tree(%s::regclass)` when `psycopg` / `psycopg2` is available so
  partitioned parent tables report real child partition sizes rather than `0`.
"""

import re
from typing import Any

from . import uris

EXIT_COST_LIMIT = 8

_SIZE_RE = re.compile(
    r'^\s*(?P<num>\d+(?:\.\d+)?)\s*(?P<unit>[KMGTPE]?i?B?)?\s*$',
    re.IGNORECASE)

_UNIT_MULTIPLIERS = {
    '': 1,
    'b': 1,
    'k': 1000,
    'kb': 1000,
    'kib': 1024,
    'm': 1000**2,
    'mb': 1000**2,
    'mib': 1024**2,
    'g': 1000**3,
    'gb': 1000**3,
    'gib': 1024**3,
    't': 1000**4,
    'tb': 1000**4,
    'tib': 1024**4,
    'p': 1000**5,
    'pb': 1000**5,
    'pib': 1024**5,
}


class CostLimitError(Exception):
  """Raised when estimated scan bytes exceed `--max-bytes`."""
  def __init__(self, message: str, estimated_bytes: int, limit_bytes: int):
    super().__init__(message)
    self.exit_code = EXIT_COST_LIMIT
    self.estimated_bytes = estimated_bytes
    self.limit_bytes = limit_bytes


def parse_byte_size(raw: str | int) -> int:
  """Parses human sizes like `'500MB'`, `'10GiB'`, or `'1048576'` into bytes."""
  if isinstance(raw, int):
    if raw < 0:
      raise ValueError('--max-bytes must be non-negative')
    return raw
  m = _SIZE_RE.match(str(raw))
  if not m:
    raise ValueError(
        f'invalid byte size {raw!r}; examples: 1048576, 500MB, 10GB, 1GiB')
  num = float(m.group('num'))
  unit = (m.group('unit') or '').lower()
  mult = _UNIT_MULTIPLIERS.get(unit)
  if mult is None:
    raise ValueError(
        f'unsupported byte unit in {raw!r}; use B, KB, MB, GB, TB, KiB, MiB, GiB')
  return int(num * mult)


def format_bytes(num_bytes: int) -> str:
  if num_bytes < 1024:
    return f'{num_bytes} B'
  for unit in ('KiB', 'MiB', 'GiB', 'TiB'):
    num_bytes_f = num_bytes / 1024.0
    if num_bytes_f < 1024.0 or unit == 'TiB':
      return f'{num_bytes_f:.2f} {unit}'
    num_bytes = int(num_bytes_f)
  return f'{num_bytes} B'


def estimate_endpoint_bytes(
    endpoint: uris.ParsedEndpoint,
    *,
    default_project: str | None = None,
    stop_after_bytes: int | None = None,
) -> dict[str, Any]:
  """Returns `{'estimated_bytes': int|None, 'method': str, ...}` for a source."""
  if endpoint.kind == 'file':
    return _estimate_file_bytes(endpoint.raw, stop_after_bytes=stop_after_bytes)
  if endpoint.kind == 'delta':
    from . import lakehouse
    ver = endpoint.config.get('version')
    snap = lakehouse.parse_delta_log(
        endpoint.config['table'],
        target_version=int(ver) if ver is not None else None)
    return {
        'estimated_bytes': int(snap['estimated_bytes']),
        'file_count': len(snap['active_files']),
        'row_count': snap['row_count'],
        'method': 'delta_log',
    }
  if endpoint.kind == 'iceberg':
    from . import lakehouse
    try:
      tdir = lakehouse.resolve_iceberg_table_dir(
          endpoint.config, default_project=default_project)
      st = lakehouse.load_iceberg_state(tdir)
      return {
          'estimated_bytes': int(st['estimated_bytes']),
          'file_count': len(st['data_files']),
          'method': 'iceberg_metadata',
      }
    except (FileNotFoundError, lakehouse.LakehouseError):
      return {'estimated_bytes': None, 'method': 'iceberg_catalog'}
  if endpoint.kind == 'bigquery':
    return _estimate_bigquery_bytes(endpoint, default_project=default_project)
  if endpoint.kind == 'postgres':
    return _estimate_postgres_bytes(endpoint)
  return {'estimated_bytes': None, 'method': 'unsupported_unbounded_or_catalog'}


def check_max_bytes(
    endpoint: uris.ParsedEndpoint,
    max_bytes_raw: str | int,
    *,
    default_project: str | None = None,
) -> dict[str, Any]:
  """Enforces `--max-bytes` pre-flight before running any Beam pipeline."""
  limit = parse_byte_size(max_bytes_raw)
  est = estimate_endpoint_bytes(
      endpoint, default_project=default_project, stop_after_bytes=limit)
  estimated = est.get('estimated_bytes')
  if estimated is not None and estimated > limit:
    at_least = 'at least ' if est.get('short_circuited') else ''
    raise CostLimitError(
        f'estimated input scan is {at_least}{format_bytes(estimated)} '
        f'({estimated:,} bytes via {est.get("method")}), exceeding '
        f'--max-bytes={max_bytes_raw} ({limit:,} bytes)',
        estimated_bytes=estimated,
        limit_bytes=limit)
  return est


def _estimate_file_bytes(
    path: str, stop_after_bytes: int | None = None) -> dict[str, Any]:
  total = 0
  count = 0
  if '://' not in path:
    import glob as _glob
    import os
    matched = (
        _glob.iglob(path)
        if any(c in path for c in '*?[') else ([path] if os.path.isfile(path) else
                                               _glob.iglob(_glob.escape(path) +
                                                           '*')))
    for p in matched:
      if os.path.isfile(p):
        total += os.path.getsize(p)
        count += 1
        if stop_after_bytes is not None and count >= 1000 and total > stop_after_bytes:
          return {
              'estimated_bytes': total,
              'file_count': count,
              'short_circuited': True,
              'method': 'local_stat',
          }
    return {
        'estimated_bytes': total,
        'file_count': count,
        'short_circuited': False,
        'method': 'local_stat',
    }

  from apache_beam.io.filesystems import FileSystems
  matches = FileSystems.match([path])[0].metadata_list
  for meta in matches:
    total += int(meta.size_in_bytes or 0)
    count += 1
    if stop_after_bytes is not None and total > stop_after_bytes:
      return {
          'estimated_bytes': total,
          'file_count': count,
          'short_circuited': True,
          'method': 'filesystem_match',
      }
  return {
      'estimated_bytes': total,
      'file_count': count,
      'short_circuited': False,
      'method': 'filesystem_match',
  }


_SAFE_BQ_COL_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def _estimate_bigquery_bytes(
    endpoint: uris.ParsedEndpoint, default_project: str | None = None
) -> dict[str, Any]:
  from google.cloud import bigquery

  cfg = endpoint.config
  client = bigquery.Client(project=default_project)

  # Direct SQL query in `--from-opt query="..."`: always $0 dryRun.
  if 'query' in cfg and cfg['query']:
    job_cfg = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = client.query(str(cfg['query']), job_config=job_cfg)
    return {
        'estimated_bytes': int(job.total_bytes_processed or 0),
        'method': 'bigquery_dry_run',
    }

  _, proj, dataset, table = uris.normalize_bq_table_spec(
      cfg['table'], default_project=default_project or client.project)
  fq_dot = f'{proj}.{dataset}.{table}' if proj else f'{dataset}.{table}'
  tbl = client.get_table(fq_dot)
  table_type = str(getattr(tbl, 'table_type', 'TABLE') or 'TABLE').upper()
  fields = cfg.get('fields')
  row_restriction = cfg.get('row_restriction')

  # Plain physical TABLE with no column/partition filter: $0 `tables.get`!
  if table_type == 'TABLE' and not fields and not row_restriction:
    return {
        'estimated_bytes': int(tbl.num_bytes or 0),
        'num_rows': int(tbl.num_rows or 0),
        'table_type': table_type,
        'method': 'bigquery_tables_get',
    }

  # VIEW / MATERIALIZED_VIEW / EXTERNAL / SNAPSHOT or pruned scan: $0 dryRun!
  if fields:
    col_list = [fields] if isinstance(fields, str) else list(fields)
    for c in col_list:
      if not _SAFE_BQ_COL_RE.match(str(c)):
        raise ValueError(f'unsafe BigQuery column identifier {c!r}')
    select_cols = ', '.join(f'`{c}`' for c in col_list)
  else:
    select_cols = '*'
  where_sql = f' WHERE {row_restriction}' if row_restriction else ''
  sql_text = f'SELECT {select_cols} FROM `{fq_dot}`{where_sql}'
  job_cfg = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
  job = client.query(sql_text, job_config=job_cfg)
  return {
      'estimated_bytes': int(job.total_bytes_processed or 0),
      'table_type': table_type,
      'method': 'bigquery_dry_run',
  }


def _estimate_postgres_bytes(endpoint: uris.ParsedEndpoint) -> dict[str, Any]:
  table = endpoint.config.get('table')
  if not table:
    return {'estimated_bytes': None, 'method': 'postgres_custom_query'}
  # Optional best-effort check if psycopg / psycopg2 is installed locally.
  driver = None
  for mod_name in ('psycopg', 'psycopg2'):
    try:
      driver = __import__(mod_name)
      break
    except ImportError:
      continue
  if driver is None:
    return {'estimated_bytes': None, 'method': 'postgres_driver_unavailable'}

  jdbc_url = endpoint.config['url']
  pg_dsn = jdbc_url[len('jdbc:'):] if jdbc_url.startswith('jdbc:') else jdbc_url
  conn_kwargs: dict[str, Any] = {}
  if endpoint.config.get('username'):
    conn_kwargs['user'] = endpoint.config['username']
  if endpoint.config.get('password'):
    conn_kwargs['password'] = endpoint.config['password']
  try:
    with driver.connect(pg_dsn, **conn_kwargs) as conn:
      with conn.cursor() as cur:
        cur.execute(
            'SELECT COALESCE(SUM(pg_relation_size(relid)), 0) '
            'FROM pg_partition_tree(%s::regclass)',
            (table, ))
        row = cur.fetchone()
        return {
            'estimated_bytes': int(row[0] or 0) if row else 0,
            'method': 'postgres_pg_partition_tree',
        }
  except Exception:  # pylint: disable=broad-except
    return {'estimated_bytes': None, 'method': 'postgres_probe_skipped'}
