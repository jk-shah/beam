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

"""Pure-Python Delta Lake & Apache Iceberg v2 lakehouse bridges.

Provides `<150 ms` zero-JVM metadata inspection, snapshot & CDC change-feed
readers, Apache Iceberg v2 writers (append & merge-on-read equality-delete
upserts/deletes), and zero-copy Parquet/Delta -> Iceberg v2 manifest
registration using only `FileSystems`, `pyarrow`, and `fastavro`.
"""

import datetime
import decimal
import io
import json
import os
import re
import time
import urllib.parse
import urllib.request
import uuid
from typing import Any
from typing import Iterable
from typing import Optional

from . import output


class LakehouseError(ValueError):
  """Raised on invalid Delta Lake or Apache Iceberg table state or config."""


# ---------------------------------------------------------------------------
# Filesystem helpers (local paths + `gs://`, `s3://`, `azfs://`, `hdfs://`)
# ---------------------------------------------------------------------------


def _normalize_fs_root(raw_path: str) -> str:
  if raw_path.lower().startswith('delta://'):
    raw_path = raw_path[len('delta://'):]
  if raw_path.lower().startswith('file://'):
    parsed = urllib.parse.urlsplit(raw_path)
    raw_path = urllib.parse.unquote(parsed.path)
  if '://' not in raw_path:
    return os.path.abspath(raw_path).rstrip('/\\')
  return raw_path.rstrip('/')


def _join_fs(base: str, *parts: str) -> str:
  clean_parts = [p.strip('/\\').replace('\\', '/') for p in parts if p]
  if '://' not in base:
    return os.path.join(base, *clean_parts)
  suffix = '/'.join(clean_parts)
  return f'{base.rstrip("/")}/{suffix}' if suffix else base.rstrip('/')


def _fs_exists(path: str) -> bool:
  if '://' not in path:
    return os.path.exists(path)
  from apache_beam.io.filesystems import FileSystems
  return FileSystems.exists(path)


def _fs_list_files(pattern: str) -> list[tuple[str, int]]:
  if '://' not in pattern:
    import glob as _glob
    matched = sorted(p for p in _glob.glob(pattern) if os.path.isfile(p))
    return [(p, os.path.getsize(p)) for p in matched]
  from apache_beam.io.filesystems import FileSystems
  metas = FileSystems.match([pattern])[0].metadata_list
  return [(m.path, int(m.size_in_bytes or 0))
          for m in sorted(metas, key=lambda x: x.path)]


def _fs_read_bytes(path: str) -> bytes:
  if '://' not in path:
    with open(path, 'rb') as f:
      return f.read()
  from apache_beam.io.filesystems import FileSystems
  with FileSystems.open(path) as f:
    return f.read()


def _fs_open_read(path: str):
  if '://' not in path:
    return open(path, 'rb')
  from apache_beam.io.filesystems import FileSystems
  return FileSystems.open(path)


def _fs_write_bytes(path: str, data: bytes, mime_type: str = 'application/octet-stream') -> None:
  if '://' not in path:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    tmp_path = f'{path}.tmp-{uuid.uuid4().hex[:8]}'
    with open(tmp_path, 'wb') as f:
      f.write(data)
    os.replace(tmp_path, path)
    return
  from apache_beam.io.filesystems import FileSystems
  with FileSystems.create(path, mime_type=mime_type) as f:
    f.write(data)


def _to_iceberg_uri(path: str) -> str:
  """Formats a path as a URI suitable for Iceberg v2 manifests/metadata."""
  if '://' in path:
    return path
  abs_p = os.path.abspath(path).replace('\\', '/')
  if not abs_p.startswith('/'):
    abs_p = '/' + abs_p
  return f'file://{abs_p}' if os.environ.get('BEAM_ICEBERG_FILE_URI') else abs_p


def _from_iceberg_uri(uri_or_path: str) -> str:
  if uri_or_path.lower().startswith('file://'):
    parsed = urllib.parse.urlsplit(uri_or_path)
    return urllib.parse.unquote(parsed.path)
  return uri_or_path


# ---------------------------------------------------------------------------
# Type conversions (Delta / Iceberg / PyArrow / Python / Beam Row)
# ---------------------------------------------------------------------------


def _delta_type_to_py_type(dtype: Any) -> Any:
  if isinstance(dtype, str):
    low = dtype.lower()
    if low in ('byte', 'short', 'integer', 'int', 'long'):
      return int
    if low in ('float', 'double') or low.startswith('decimal'):
      return float
    if low == 'boolean':
      return bool
    if low == 'binary':
      return bytes
    return str
  return str


def _iceberg_type_to_py_type(itype: Any) -> Any:
  if isinstance(itype, str):
    low = itype.lower()
    if low in ('int', 'long'):
      return int
    if low in ('float', 'double') or low.startswith('decimal'):
      return float
    if low == 'boolean':
      return bool
    if low in ('binary', 'fixed'):
      return bytes
    return str
  return str


def _arrow_type_to_iceberg_type(pa_type) -> str:
  import pyarrow as pa
  if pa.types.is_boolean(pa_type):
    return 'boolean'
  if pa.types.is_int8(pa_type) or pa.types.is_int16(pa_type) or pa.types.is_int32(pa_type):
    return 'int'
  if pa.types.is_integer(pa_type):
    return 'long'
  if pa.types.is_float32(pa_type):
    return 'float'
  if pa.types.is_floating(pa_type):
    return 'double'
  if pa.types.is_decimal(pa_type):
    return f'decimal({pa_type.precision}, {pa_type.scale})'
  if pa.types.is_date(pa_type):
    return 'date'
  if pa.types.is_timestamp(pa_type):
    return 'timestamptz' if getattr(pa_type, 'tz', None) else 'timestamp'
  if pa.types.is_binary(pa_type) or pa.types.is_large_binary(pa_type):
    return 'binary'
  return 'string'


def _iceberg_type_to_arrow_type(itype: Any):
  import pyarrow as pa
  if not isinstance(itype, str):
    return pa.string()
  low = itype.strip().lower()
  if low == 'boolean':
    return pa.bool_()
  if low == 'int':
    return pa.int64()
  if low == 'long':
    return pa.int64()
  if low == 'float':
    return pa.float32()
  if low == 'double':
    return pa.float64()
  if low.startswith('decimal'):
    m = re.match(r'decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)', low)
    if m:
      return pa.decimal128(int(m.group(1)), int(m.group(2)))
    return pa.float64()
  if low == 'date':
    return pa.date32()
  if low == 'timestamptz':
    return pa.timestamp('us', tz='UTC')
  if low == 'timestamp':
    return pa.timestamp('us')
  if low in ('binary', 'fixed'):
    return pa.binary()
  return pa.string()


def _coerce_scalar_for_py(val: Any, py_t: Any) -> Any:
  if val is None:
    return None
  if isinstance(val, decimal.Decimal):
    return float(val) if py_t is float else str(val)
  if isinstance(val, (datetime.datetime, datetime.date)):
    return val.isoformat()
  if py_t is int and not isinstance(val, bool):
    try:
      return int(val)
    except (ValueError, TypeError):
      return val
  if py_t is float and not isinstance(val, bool):
    try:
      return float(val)
    except (ValueError, TypeError):
      return val
  if py_t is bool:
    if isinstance(val, str):
      return val.strip().lower() in ('true', '1')
    return bool(val)
  if py_t is str and not isinstance(val, str):
    return str(val)
  return val


def _row_matches_search(row: dict[str, Any], needle_lower: str) -> bool:
  for v in row.values():
    if v is not None and needle_lower in str(v).lower():
      return True
  return False


# ---------------------------------------------------------------------------
# 1. Delta Lake (`delta://`) Protocol Parser, Inspector & Readers
# ---------------------------------------------------------------------------

_DELTA_COMMIT_RE = re.compile(r'^(\d{20})\.json$')
_DELTA_CHECKPOINT_RE = re.compile(r'^(\d{20})\.checkpoint(?:\.\d+\.\d+)?\.parquet$')


def parse_delta_log(
    table_root: str,
    *,
    target_version: Optional[int] = None,
) -> dict[str, Any]:
  """Replays `_delta_log` up to `target_version` (or latest) in `<100ms`."""
  root = _normalize_fs_root(table_root)
  log_dir = _join_fs(root, '_delta_log')
  entries = _fs_list_files(_join_fs(log_dir, '*'))
  if not entries:
    raise FileNotFoundError(
        f'Delta Lake log directory not found or empty at {log_dir!r}')

  commits: dict[int, str] = {}
  checkpoints: dict[int, list[str]] = {}
  for fpath, _ in entries:
    bname = os.path.basename(fpath.replace('\\', '/'))
    m_c = _DELTA_COMMIT_RE.match(bname)
    if m_c:
      ver = int(m_c.group(1))
      if target_version is None or ver <= target_version:
        commits[ver] = fpath
      continue
    m_cp = _DELTA_CHECKPOINT_RE.match(bname)
    if m_cp:
      ver = int(m_cp.group(1))
      if target_version is None or ver <= target_version:
        checkpoints.setdefault(ver, []).append(fpath)

  if not commits and not checkpoints:
    raise FileNotFoundError(
        f'no Delta Lake commits found at or before version {target_version} in {log_dir!r}')

  active_files: dict[str, dict[str, Any]] = {}
  schema_dict: dict[str, Any] = {'fields': []}
  partition_cols: list[str] = []
  table_config: dict[str, str] = {}
  start_ver = 0

  # Fast-forward from latest checkpoint <= target_version if present.
  if checkpoints:
    cp_ver = max(checkpoints.keys())
    import pyarrow.parquet as pq
    for cp_file in sorted(checkpoints[cp_ver]):
      with _fs_open_read(cp_file) as fobj:
        pf = pq.ParquetFile(fobj)
        for batch in pf.iter_batches():
          for row in batch.to_pylist():
            md = row.get('metaData')
            if md and md.get('schemaString'):
              schema_dict = json.loads(md['schemaString'])
              partition_cols = list(md.get('partitionColumns') or [])
              table_config = dict(md.get('configuration') or {})
            add = row.get('add')
            if add and add.get('path'):
              rel = urllib.parse.unquote(add['path'])
              active_files[rel] = {
                  'rel_path': rel,
                  'path': _join_fs(root, rel),
                  'size': int(add.get('size') or 0),
                  'partition_values': dict(add.get('partitionValues') or {}),
                  'stats': add.get('stats'),
                  'version': cp_ver,
              }
            rem = row.get('remove')
            if rem and rem.get('path'):
              active_files.pop(urllib.parse.unquote(rem['path']), None)
    start_ver = cp_ver + 1

  max_ver = max(list(commits.keys()) + list(checkpoints.keys()))
  for ver in sorted(v for v in commits if v >= start_ver):
    raw_lines = _fs_read_bytes(commits[ver]).decode('utf-8').splitlines()
    for line in raw_lines:
      if not line.strip():
        continue
      act = json.loads(line)
      md = act.get('metaData')
      if md and md.get('schemaString'):
        schema_dict = json.loads(md['schemaString'])
        partition_cols = list(md.get('partitionColumns') or [])
        table_config = dict(md.get('configuration') or {})
      add = act.get('add')
      if add and add.get('path'):
        rel = urllib.parse.unquote(add['path'])
        active_files[rel] = {
            'rel_path': rel,
            'path': _join_fs(root, rel),
            'size': int(add.get('size') or 0),
            'partition_values': dict(add.get('partitionValues') or {}),
            'stats': add.get('stats'),
            'version': ver,
        }
      rem = act.get('remove')
      if rem and rem.get('path'):
        active_files.pop(urllib.parse.unquote(rem['path']), None)

  schema_fields = [{
      'name': str(f['name']),
      'type': str(f['type']) if isinstance(f.get('type'), str) else 'struct',
      'nullable': bool(f.get('nullable', True)),
  } for f in schema_dict.get('fields', [])]

  # Estimate total rows from stats or Parquet footers when lightweight.
  total_rows: Optional[int] = 0
  for info in active_files.values():
    st_raw = info.get('stats')
    if isinstance(st_raw, str) and st_raw:
      try:
        st_obj = json.loads(st_raw)
        if 'numRecords' in st_obj and total_rows is not None:
          total_rows += int(st_obj['numRecords'])
          continue
      except ValueError:
        pass
    total_rows = None
    break

  return {
      'root': root,
      'version': max_ver,
      'commit_versions': sorted(commits.keys()),
      'schema': schema_fields,
      'partition_columns': partition_cols,
      'configuration': table_config,
      'cdc_enabled': str(
          table_config.get('delta.enableChangeDataFeed', 'false')).lower() == 'true',
      'active_files': list(active_files.values()),
      'estimated_bytes': sum(f['size'] for f in active_files.values()),
      'row_count': total_rows,
  }


def list_delta_cdc_actions(
    table_root: str,
    *,
    start_version: int = 0,
    end_version: Optional[int] = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
  """Extracts Delta Change Data Feed (`cdc`) or commit `add`/`remove` actions."""
  root = _normalize_fs_root(table_root)
  log_dir = _join_fs(root, '_delta_log')
  entries = _fs_list_files(_join_fs(log_dir, '*.json'))
  if not entries:
    raise FileNotFoundError(f'no Delta commits found in {log_dir!r}')

  schema_fields: list[dict[str, Any]] = []
  actions: list[dict[str, Any]] = []
  for fpath, _ in entries:
    bname = os.path.basename(fpath.replace('\\', '/'))
    m_c = _DELTA_COMMIT_RE.match(bname)
    if not m_c:
      continue
    ver = int(m_c.group(1))
    raw_lines = _fs_read_bytes(fpath).decode('utf-8').splitlines()
    ver_objs = [json.loads(ln) for ln in raw_lines if ln.strip()]
    for obj in ver_objs:
      md = obj.get('metaData')
      if md and md.get('schemaString'):
        sd = json.loads(md['schemaString'])
        schema_fields = [{
            'name': str(f['name']),
            'type': str(f['type']) if isinstance(f.get('type'), str) else 'string',
            'nullable': bool(f.get('nullable', True)),
        } for f in sd.get('fields', [])]
    if ver < start_version or (end_version is not None and ver > end_version):
      continue

    has_explicit_cdc = any('cdc' in o for o in ver_objs)
    if has_explicit_cdc:
      for obj in ver_objs:
        cdc = obj.get('cdc')
        if cdc and cdc.get('path'):
          rel = urllib.parse.unquote(cdc['path'])
          actions.append({
              'kind': 'cdc_file',
              'path': _join_fs(root, rel),
              'partition_values': dict(cdc.get('partitionValues') or {}),
              'version': ver,
          })
    else:
      for obj in ver_objs:
        add = obj.get('add')
        if add and add.get('path') and add.get('dataChange', True):
          rel = urllib.parse.unquote(add['path'])
          actions.append({
              'kind': 'added_file',
              'path': _join_fs(root, rel),
              'partition_values': dict(add.get('partitionValues') or {}),
              'version': ver,
          })
        rem = obj.get('remove')
        if rem and rem.get('path') and rem.get('dataChange', True):
          rel = urllib.parse.unquote(rem['path'])
          actions.append({
              'kind': 'removed_file',
              'path': _join_fs(root, rel),
              'partition_values': dict(rem.get('partitionValues') or {}),
              'version': ver,
          })
  if not schema_fields:
    snap = parse_delta_log(table_root, target_version=end_version)
    schema_fields = snap['schema']
  return schema_fields, actions


def read_delta_rows_local(
    table_root: str,
    *,
    version: Optional[int] = None,
    limit: Optional[int] = None,
    search_term: Optional[str] = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
  """Reads up to `limit` rows from a Delta snapshot without booting a runner."""
  import pyarrow.parquet as pq
  snap = parse_delta_log(table_root, target_version=version)
  type_map = {f['name']: _delta_type_to_py_type(f['type']) for f in snap['schema']}
  needle = search_term.lower() if search_term else None
  rows: list[dict[str, Any]] = []
  counted_rows = 0
  need_exact_count = snap['row_count'] is None and len(snap['active_files']) <= 64

  for finfo in snap['active_files']:
    with _fs_open_read(finfo['path']) as fobj:
      pf = pq.ParquetFile(fobj)
      counted_rows += pf.metadata.num_rows
      if limit is not None and len(rows) >= limit and not need_exact_count:
        break
      if limit is None or len(rows) < limit:
        pvals = finfo.get('partition_values') or {}
        for batch in pf.iter_batches(batch_size=max((limit or 64) * 2, 64)):
          for raw_r in batch.to_pylist():
            rec: dict[str, Any] = {}
            for f in snap['schema']:
              col = f['name']
              v = raw_r.get(col, pvals.get(col))
              rec[col] = _coerce_scalar_for_py(v, type_map[col])
            if needle is None or _row_matches_search(rec, needle):
              rows.append(rec)
              if limit is not None and len(rows) >= limit:
                break
          if limit is not None and len(rows) >= limit:
            break
  if snap['row_count'] is None and need_exact_count:
    snap['row_count'] = counted_rows
  return snap, rows


def inspect_delta_table(
    ep,
    *,
    sample_count: int = 5,
    search_term: Optional[str] = None,
) -> dict[str, Any]:
  table_path = ep.config['table']
  ver = ep.config.get('version')
  if ver is not None:
    ver = int(ver)
  snap, rows = read_delta_rows_local(
      table_path,
      version=ver,
      limit=sample_count if sample_count > 0 else 0,
      search_term=search_term)
  return {
      'uri': ep.raw,
      'kind': 'delta',
      'format': 'delta',
      'snapshot_version': snap['version'],
      'file_count': len(snap['active_files']),
      'estimated_bytes': snap['estimated_bytes'],
      'row_count': snap['row_count'],
      'partition_columns': snap['partition_columns'],
      'cdc_enabled': snap['cdc_enabled'],
      'schema': snap['schema'],
      'untrusted_data': output.wrap_untrusted_rows(rows)
      if sample_count > 0 else None,
  }


# ---------------------------------------------------------------------------
# 2. Apache Iceberg v2 Spec: Avro Manifest / Manifest-List Schemas & Helpers
# ---------------------------------------------------------------------------

_ICEBERG_V2_MANIFEST_SCHEMA: dict[str, Any] = {
    'type': 'record',
    'name': 'manifest_entry',
    'fields': [
        {'name': 'status', 'type': 'int', 'field-id': 0},
        {'name': 'snapshot_id', 'type': ['null', 'long'], 'default': None, 'field-id': 1},
        {'name': 'sequence_number', 'type': ['null', 'long'], 'default': None, 'field-id': 3},
        {'name': 'file_sequence_number', 'type': ['null', 'long'], 'default': None, 'field-id': 4},
        {
            'name': 'data_file',
            'field-id': 2,
            'type': {
                'type': 'record',
                'name': 'r2',
                'fields': [
                    {'name': 'content', 'type': 'int', 'field-id': 134},
                    {'name': 'file_path', 'type': 'string', 'field-id': 100},
                    {'name': 'file_format', 'type': 'string', 'field-id': 101},
                    {
                        'name': 'partition',
                        'type': {'type': 'record', 'name': 'r102', 'fields': []},
                        'field-id': 102,
                    },
                    {'name': 'record_count', 'type': 'long', 'field-id': 103},
                    {'name': 'file_size_in_bytes', 'type': 'long', 'field-id': 104},
                    {
                        'name': 'equality_ids',
                        'type': [
                            'null',
                            {'type': 'array', 'items': 'int', 'element-id': 136},
                        ],
                        'default': None,
                        'field-id': 135,
                    },
                    {
                        'name': 'sort_order_id',
                        'type': ['null', 'int'],
                        'default': None,
                        'field-id': 140,
                    },
                ],
            },
        },
    ],
}

_ICEBERG_V2_MANIFEST_LIST_SCHEMA: dict[str, Any] = {
    'type': 'record',
    'name': 'manifest_file',
    'fields': [
        {'name': 'manifest_path', 'type': 'string', 'field-id': 500},
        {'name': 'manifest_length', 'type': 'long', 'field-id': 501},
        {'name': 'partition_spec_id', 'type': 'int', 'field-id': 502},
        {'name': 'content', 'type': 'int', 'field-id': 517},
        {'name': 'sequence_number', 'type': 'long', 'field-id': 515},
        {'name': 'min_sequence_number', 'type': 'long', 'field-id': 516},
        {'name': 'added_snapshot_id', 'type': 'long', 'field-id': 503},
        {'name': 'added_files_count', 'type': 'int', 'field-id': 504},
        {'name': 'existing_files_count', 'type': 'int', 'field-id': 505},
        {'name': 'deleted_files_count', 'type': 'int', 'field-id': 506},
        {'name': 'added_rows_count', 'type': 'long', 'field-id': 512},
        {'name': 'existing_rows_count', 'type': 'long', 'field-id': 513},
        {'name': 'deleted_rows_count', 'type': 'long', 'field-id': 514},
    ],
}


def resolve_iceberg_table_dir(
    config: dict[str, Any], default_project: Optional[str] = None
) -> str:
  """Resolves an `iceberg://` config (`table` + `catalog_properties`) to its root dir."""
  cat_props = dict(config.get('catalog_properties') or {})
  cat_type = str(cat_props.get('type', 'hadoop')).lower()
  raw_table = str(config.get('table') or '').rstrip('/')
  table_ident = raw_table.lstrip('/')

  # Explicit direct table location if passed.
  if config.get('location'):
    return _normalize_fs_root(str(config['location']))

  # REST / BigLake catalog: query `/v1/.../namespaces/{ns}/tables/{tbl}` if URI set.
  if cat_type == 'rest' and cat_props.get('uri') and table_ident:
    parts = [p for p in table_ident.split('.') if p]
    if len(parts) >= 2:
      ns = '%1F'.join(parts[:-1])
      tbl = parts[-1]
      rest_uri = str(cat_props['uri']).rstrip('/')
      warehouse = str(cat_props.get('warehouse') or '')
      prefix = ''
      if 'biglake.googleapis.com' in rest_uri and warehouse.startswith('gs://'):
        bkt = warehouse[len('gs://'):].split('/', 1)[0]
        proj = (
            cat_props.get('header.x-goog-user-project') or default_project or '')
        if proj:
          prefix = f'/projects/{proj}/catalogs/{bkt}'
      url = f'{rest_uri}/v1{prefix}/namespaces/{ns}/tables/{tbl}'
      headers = {'Accept': 'application/json'}
      if cat_props.get('header.x-goog-user-project'):
        headers['x-goog-user-project'] = str(
            cat_props['header.x-goog-user-project'])
      if cat_props.get('token'):
        headers['Authorization'] = f"Bearer {cat_props['token']}"
      elif str(cat_props.get('rest.auth.type', '')).lower() == 'google':
        try:
          import google.auth
          import google.auth.transport.requests
          creds, _ = google.auth.default(
              scopes=['https://www.googleapis.com/auth/cloud-platform'])
          creds.refresh(google.auth.transport.requests.Request())
          if creds.token:
            headers['Authorization'] = f'Bearer {creds.token}'
        except Exception:  # pylint: disable=broad-except
          pass
      try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
          payload = json.loads(resp.read().decode('utf-8'))
          if payload.get('metadata-location'):
            meta_loc = str(payload['metadata-location'])
            # Strip `/metadata/vN.metadata.json`.
            return meta_loc.rsplit('/metadata/', 1)[0]
          if isinstance(payload.get('metadata'), dict) and payload['metadata'].get('location'):
            return str(payload['metadata']['location']).rstrip('/')
      except Exception:  # pylint: disable=broad-except
        pass

  warehouse = str(cat_props.get('warehouse') or '').strip()
  if not warehouse:
    # Allow direct filesystem path in `table` (including `iceberg:///abs/wh/ns.tbl`
    # and `iceberg://rel/wh/ns.tbl`) without stripping leading `/`!
    if ('/' in raw_table or os.sep in raw_table or
        raw_table.startswith('.')):
      norm_path = _normalize_fs_root(raw_table)
      # If literal `<norm_path>/metadata` already exists locally, keep it as-is.
      if '://' not in norm_path and os.path.isdir(
          os.path.join(norm_path, 'metadata')):
        return norm_path
      if '/' in norm_path:
        parent_dir, last_seg = norm_path.rsplit('/', 1)
        if '.' in last_seg and not last_seg.startswith('.'):
          seg_parts = [p for p in last_seg.split('.') if p]
          if len(seg_parts) >= 2:
            return _join_fs(parent_dir, *seg_parts)
      return norm_path
    raise LakehouseError(
        f'Iceberg table {table_ident!r} requires a warehouse path '
        '(e.g. iceberg://namespace.table?warehouse=gs://my-bucket/warehouse '
        'or --from-opt warehouse=/path/to/warehouse)')

  wh_norm = _normalize_fs_root(warehouse)
  parts = [p for p in table_ident.split('.') if p]
  # If a 3-part `catalog.namespace.table` was supplied alongside `catalog_name`,
  # strip the leading catalog prefix when matching `catalog_name`.
  cat_name = config.get('catalog_name')
  if len(parts) >= 3 and cat_name and parts[0] == cat_name:
    parts = parts[1:]
  return _join_fs(wh_norm, *parts)


def find_latest_iceberg_metadata_path(table_dir: str) -> tuple[int, str]:
  """Finds `(version_num, metadata_json_path)` for an Iceberg table directory."""
  meta_dir = _join_fs(table_dir, 'metadata')
  hint_path = _join_fs(meta_dir, 'version-hint.text')
  if _fs_exists(hint_path):
    try:
      hint = _fs_read_bytes(hint_path).decode('utf-8').strip()
      if hint.isdigit():
        ver = int(hint)
        cand = _join_fs(meta_dir, f'v{ver}.metadata.json')
        if _fs_exists(cand):
          return ver, cand
    except Exception:  # pylint: disable=broad-except
      pass

  files = _fs_list_files(_join_fs(meta_dir, '*.metadata.json'))
  if not files:
    raise FileNotFoundError(
        f'no Iceberg metadata files (*.metadata.json) found in {meta_dir!r}')

  best_ver = -1
  best_path = files[-1][0]
  for fpath, _ in files:
    bname = os.path.basename(fpath.replace('\\', '/'))
    m = re.match(r'^v?(\d+)(?:-[A-Za-z0-9-]+)?\.metadata\.json$', bname)
    if m:
      v = int(m.group(1))
      if v >= best_ver:
        best_ver = v
        best_path = fpath
  return max(best_ver, 1), best_path


def load_iceberg_state(
    table_dir: str, *, snapshot_id: Optional[int] = None
) -> dict[str, Any]:
  """Loads Iceberg v1/v2 metadata, active data files, and delete files."""
  import fastavro

  meta_ver, meta_path = find_latest_iceberg_metadata_path(table_dir)
  meta = json.loads(_fs_read_bytes(meta_path).decode('utf-8'))

  schemas_list = meta.get('schemas') or (
      [meta['schema']] if 'schema' in meta else [])
  cur_schema_id = meta.get('current-schema-id', 0)
  cur_schema = next(
      (s for s in schemas_list if s.get('schema-id') == cur_schema_id),
      schemas_list[-1] if schemas_list else {'fields': [], 'schema-id': 0})

  id_to_field = {int(f['id']): f for f in cur_schema.get('fields', [])}
  schema_fields = [{
      'id': int(f['id']),
      'name': str(f['name']),
      'type': str(f['type']) if isinstance(f.get('type'), str) else 'string',
      'nullable': not bool(f.get('required', False)),
  } for f in cur_schema.get('fields', [])]
  identifier_ids = list(cur_schema.get('identifier-field-ids') or [])
  identifier_cols = [
      id_to_field[fid]['name'] for fid in identifier_ids if fid in id_to_field
  ]

  snapshots = list(meta.get('snapshots') or [])
  eff_snap_id = (
      snapshot_id
      if snapshot_id is not None else meta.get('current-snapshot-id'))
  cur_snap = next(
      (s for s in snapshots if s.get('snapshot-id') == eff_snap_id), None)

  data_files: list[dict[str, Any]] = []
  eq_delete_files: list[dict[str, Any]] = []
  pos_delete_files: list[dict[str, Any]] = []
  manifest_list_entries: list[dict[str, Any]] = []

  if cur_snap and cur_snap.get('manifest-list'):
    ml_path = _from_iceberg_uri(str(cur_snap['manifest-list']))
    with _fs_open_read(ml_path) as ml_f:
      manifest_list_entries = [dict(r) for r in fastavro.reader(ml_f)]

    for m_entry in manifest_list_entries:
      m_path = _from_iceberg_uri(str(m_entry['manifest_path']))
      inh_seq = int(m_entry.get('sequence_number') or 0)
      inh_snap = int(m_entry.get('added_snapshot_id') or 0)
      m_content = int(m_entry.get('content') or 0)
      with _fs_open_read(m_path) as mf:
        for entry in fastavro.reader(mf):
          status = int(entry.get('status') or 0)
          if status == 2:  # DELETED manifest entry
            continue
          df = entry['data_file']
          content_kind = int(df.get('content', m_content) or 0)
          seq = int(
              entry.get('sequence_number')
              if entry.get('sequence_number') is not None else inh_seq)
          rec_info = {
              'path': _from_iceberg_uri(str(df['file_path'])),
              'uri': str(df['file_path']),
              'record_count': int(df.get('record_count') or 0),
              'file_size_in_bytes': int(df.get('file_size_in_bytes') or 0),
              'sequence_number': seq,
              'snapshot_id': int(entry.get('snapshot_id') or inh_snap),
              'equality_ids': list(df.get('equality_ids') or []),
              'partition': dict(df.get('partition') or {}),
          }
          if content_kind == 0:
            data_files.append(rec_info)
          elif content_kind == 1:
            pos_delete_files.append(rec_info)
          elif content_kind == 2:
            eq_delete_files.append(rec_info)

  return {
      'table_dir': table_dir,
      'metadata_version': meta_ver,
      'metadata_path': meta_path,
      'metadata': meta,
      'current_schema': cur_schema,
      'schema': schema_fields,
      'id_to_field': id_to_field,
      'identifier_columns': identifier_cols,
      'current_snapshot_id': eff_snap_id,
      'snapshots': snapshots,
      'manifest_list_entries': manifest_list_entries,
      'data_files': data_files,
      'eq_delete_files': eq_delete_files,
      'pos_delete_files': pos_delete_files,
      'estimated_bytes': sum(f['file_size_in_bytes'] for f in data_files),
      'raw_record_count': sum(f['record_count'] for f in data_files),
  }


def _load_iceberg_deletes(
    state: dict[str, Any]
) -> tuple[dict[str, set[int]], list[tuple[int, tuple[str, ...], set[tuple[Any, ...]]]]]:
  """Loads position deletes and `(delete_seq, eq_cols, deleted_key_tuples)`."""
  import pyarrow.parquet as pq

  pos_deletes: dict[str, set[int]] = {}
  for pf_info in state['pos_delete_files']:
    with _fs_open_read(pf_info['path']) as fobj:
      pf = pq.ParquetFile(fobj)
      for batch in pf.iter_batches():
        for r in batch.to_pylist():
          fp = _from_iceberg_uri(str(r.get('file_path', '')))
          pos = int(r.get('pos', -1))
          if fp and pos >= 0:
            pos_deletes.setdefault(fp, set()).add(pos)

  eq_deletes: list[tuple[int, tuple[str, ...], set[tuple[Any, ...]]]] = []
  id_to_field = state['id_to_field']
  for ef_info in state['eq_delete_files']:
    eq_ids = ef_info.get('equality_ids') or []
    eq_cols = tuple(
        id_to_field[fid]['name'] for fid in eq_ids if fid in id_to_field)
    if not eq_cols:
      eq_cols = tuple(state['identifier_columns'])
    with _fs_open_read(ef_info['path']) as fobj:
      pf = pq.ParquetFile(fobj)
      if not eq_cols:
        eq_cols = tuple(pf.schema_arrow.names)
      key_set: set[tuple[Any, ...]] = set()
      for batch in pf.iter_batches():
        for r in batch.to_pylist():
          key_set.add(tuple(r.get(c) for c in eq_cols))
      if key_set:
        eq_deletes.append((int(ef_info['sequence_number']), eq_cols, key_set))

  return pos_deletes, eq_deletes


def read_iceberg_rows_local(
    state: dict[str, Any],
    *,
    limit: Optional[int] = None,
    search_term: Optional[str] = None,
) -> tuple[int, list[dict[str, Any]]]:
  """Reads live rows from an Iceberg state, applying position & equality deletes."""
  import pyarrow.parquet as pq

  pos_deletes, eq_deletes = _load_iceberg_deletes(state)
  type_map = {
      f['name']: _iceberg_type_to_py_type(f['type']) for f in state['schema']
  }
  needle = search_term.lower() if search_term else None
  rows: list[dict[str, Any]] = []
  live_count = 0
  has_deletes = bool(pos_deletes or eq_deletes)

  for df_info in state['data_files']:
    if not has_deletes and limit is not None and len(rows) >= limit:
      live_count = state['raw_record_count']
      break
    fpath = df_info['path']
    data_seq = int(df_info['sequence_number'])
    applicable_eq = [
        (cols, kset) for del_seq, cols, kset in eq_deletes if data_seq < del_seq
    ]
    file_pos_del = pos_deletes.get(fpath, set())

    with _fs_open_read(fpath) as fobj:
      pf = pq.ParquetFile(fobj)
      row_idx = 0
      for batch in pf.iter_batches():
        for raw_r in batch.to_pylist():
          cur_pos = row_idx
          row_idx += 1
          if cur_pos in file_pos_del:
            continue
          rec = {
              f['name']: _coerce_scalar_for_py(
                  raw_r.get(f['name']), type_map[f['name']])
              for f in state['schema']
          }
          deleted = False
          for cols, kset in applicable_eq:
            if tuple(rec.get(c) for c in cols) in kset:
              deleted = True
              break
          if deleted:
            continue
          live_count += 1
          if (limit is None or len(rows) < limit) and (
              needle is None or _row_matches_search(rec, needle)):
            rows.append(rec)
          if not has_deletes and limit is not None and len(rows) >= limit:
            break
        if not has_deletes and limit is not None and len(rows) >= limit:
          break
  if not has_deletes:
    live_count = state['raw_record_count']
  return live_count, rows


def inspect_iceberg_table(
    ep,
    *,
    sample_count: int = 5,
    search_term: Optional[str] = None,
    default_project: Optional[str] = None,
) -> dict[str, Any]:
  table_dir = resolve_iceberg_table_dir(ep.config, default_project=default_project)
  state = load_iceberg_state(table_dir)
  live_rows, sample_rows = read_iceberg_rows_local(
      state,
      limit=sample_count if sample_count > 0 else 0,
      search_term=search_term)
  return {
      'uri': ep.raw,
      'kind': 'iceberg',
      'format': 'iceberg',
      'table_location': state['table_dir'],
      'metadata_path': state['metadata_path'],
      'format_version': state['metadata'].get('format-version', 2),
      'current_snapshot_id': state['current_snapshot_id'],
      'snapshot_count': len(state['snapshots']),
      'data_file_count': len(state['data_files']),
      'delete_file_count': (
          len(state['eq_delete_files']) + len(state['pos_delete_files'])),
      'identifier_columns': state['identifier_columns'],
      'estimated_bytes': state['estimated_bytes'],
      'row_count': live_rows,
      'schema': [
          {k: v for k, v in col.items() if k != 'id'} for col in state['schema']
      ],
      'untrusted_data': output.wrap_untrusted_rows(sample_rows)
      if sample_count > 0 else None,
  }


def list_iceberg_cdc_changes(
    table_dir: str,
    *,
    from_snapshot: Optional[int] = None,
    to_snapshot: Optional[int] = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
  """Walks Iceberg snapshots in order and returns `(schema, change_records)`."""
  import fastavro
  import pyarrow.parquet as pq

  state = load_iceberg_state(table_dir)
  snaps = list(state['snapshots'])
  start_idx = 0
  if from_snapshot is not None:
    for idx, s in enumerate(snaps):
      if int(s.get('snapshot-id')) == int(from_snapshot):
        start_idx = idx + 1
        break
  selected_snaps: list[dict[str, Any]] = []
  for s in snaps[start_idx:]:
    selected_snaps.append(s)
    if to_snapshot is not None and int(s.get('snapshot-id')) == int(to_snapshot):
      break

  type_map = {
      f['name']: _iceberg_type_to_py_type(f['type']) for f in state['schema']
  }
  id_to_field = state['id_to_field']
  changes: list[dict[str, Any]] = []

  for snap in selected_snaps:
    snap_id = int(snap['snapshot-id'])
    snap_seq = int(snap.get('sequence-number') or 1)
    ml_path = _from_iceberg_uri(str(snap['manifest-list']))
    with _fs_open_read(ml_path) as ml_f:
      ml_entries = [
          dict(r) for r in fastavro.reader(ml_f)
          if int(r.get('added_snapshot_id') or 0) == snap_id
      ]
    # Process equality deletes added in this snapshot first, then added data.
    ml_entries.sort(key=lambda e: (0 if int(e.get('content') or 0) == 1 else 1))
    for m_entry in ml_entries:
      m_path = _from_iceberg_uri(str(m_entry['manifest_path']))
      m_content = int(m_entry.get('content') or 0)
      with _fs_open_read(m_path) as mf:
        for entry in fastavro.reader(mf):
          if int(entry.get('status') or 0) != 1:  # Only ADDED in this snapshot
            continue
          df = entry['data_file']
          ckind = int(df.get('content', 2 if m_content == 1 else 0))
          fpath = _from_iceberg_uri(str(df['file_path']))
          if ckind == 0:
            with _fs_open_read(fpath) as fobj:
              pf = pq.ParquetFile(fobj)
              for batch in pf.iter_batches():
                for raw_r in batch.to_pylist():
                  rec = {
                      f['name']: _coerce_scalar_for_py(
                          raw_r.get(f['name']), type_map[f['name']])
                      for f in state['schema']
                  }
                  rec['_change_type'] = 'INSERT'
                  rec['_commit_snapshot_sequence_number'] = snap_seq
                  changes.append(rec)
          elif ckind == 2:
            eq_ids = df.get('equality_ids') or []
            eq_cols = [
                id_to_field[fid]['name'] for fid in eq_ids if fid in id_to_field
            ] or list(state['identifier_columns'])
            with _fs_open_read(fpath) as fobj:
              pf = pq.ParquetFile(fobj)
              for batch in pf.iter_batches():
                for raw_r in batch.to_pylist():
                  rec = {
                      f['name']: (
                          _coerce_scalar_for_py(
                              raw_r.get(f['name']), type_map[f['name']])
                          if f['name'] in eq_cols else None)
                      for f in state['schema']
                  }
                  rec['_change_type'] = 'DELETE'
                  rec['_commit_snapshot_sequence_number'] = snap_seq
                  changes.append(rec)
  return state['schema'], changes


# ---------------------------------------------------------------------------
# 3. Iceberg v2 Metadata & Manifest Commit Engine (Append, CDC & Zero-Copy)
# ---------------------------------------------------------------------------


def _write_avro_bytes(
    schema: dict[str, Any],
    records: list[dict[str, Any]],
    metadata: dict[str, str],
) -> bytes:
  import fastavro
  buf = io.BytesIO()
  parsed = fastavro.parse_schema(schema)
  fastavro.writer(buf, parsed, records, metadata=metadata)
  return buf.getvalue()


def commit_iceberg_snapshot(
    table_dir: str,
    *,
    iceberg_fields: list[dict[str, Any]],
    identifier_columns: Optional[Iterable[str]] = None,
    new_data_files: list[dict[str, Any]],
    new_eq_delete_files: Optional[list[dict[str, Any]]] = None,
    replace_table: bool = False,
) -> dict[str, Any]:
  """Atomically writes Iceberg v2 manifests, manifest-list, and `vN.metadata.json`."""
  table_dir = _normalize_fs_root(table_dir)
  meta_dir = _join_fs(table_dir, 'metadata')
  now_ms = int(time.time() * 1000)

  existing: Optional[dict[str, Any]] = None
  if not replace_table:
    try:
      existing = load_iceberg_state(table_dir)
    except FileNotFoundError:
      existing = None

  if existing is not None:
    meta_ver = existing['metadata_version'] + 1
    old_meta = existing['metadata']
    table_uuid = old_meta['table-uuid']
    seq_num = int(old_meta.get('last-sequence-number') or 0) + 1
    parent_snap_id = old_meta.get('current-snapshot-id')
    prior_ml_entries = list(existing['manifest_list_entries'])
    prior_snapshots = list(old_meta.get('snapshots') or [])
    prior_snap_log = list(old_meta.get('snapshot-log') or [])
    prior_meta_log = list(old_meta.get('metadata-log') or [])
    prior_meta_log.append({
        'timestamp-ms': int(old_meta.get('last-updated-ms') or now_ms),
        'metadata-file': _to_iceberg_uri(existing['metadata_path']),
    })
    cur_schema = existing['current_schema']
  else:
    meta_ver = 1
    table_uuid = str(uuid.uuid4())
    seq_num = 1
    parent_snap_id = None
    prior_ml_entries = []
    prior_snapshots = []
    prior_snap_log = []
    prior_meta_log = []
    id_col_set = set(identifier_columns or ())
    schema_fields_json = []
    id_field_ids = []
    for idx, f in enumerate(iceberg_fields, start=1):
      fid = int(f.get('id') or idx)
      is_id = f['name'] in id_col_set
      if is_id:
        id_field_ids.append(fid)
      schema_fields_json.append({
          'id': fid,
          'name': f['name'],
          'required': bool(is_id or not f.get('nullable', True)),
          'type': f['type'],
      })
    cur_schema = {
        'type': 'struct',
        'schema-id': 0,
        'fields': schema_fields_json,
    }
    if id_field_ids:
      cur_schema['identifier-field-ids'] = id_field_ids

  name_to_id = {str(f['name']): int(f['id']) for f in cur_schema['fields']}
  name_mapping = [{
      'field-id': int(f['id']), 'names': [str(f['name'])]
  } for f in cur_schema['fields']]
  part_spec = {'spec-id': 0, 'fields': []}

  snap_id = (int(time.time() * 1000000) ^ uuid.uuid4().int) & 0x7FFFFFFFFFFFFFFF
  if snap_id == 0:
    snap_id = 1

  new_ml_entries: list[dict[str, Any]] = []
  commit_uuid = uuid.uuid4().hex

  # 1. Write data manifest (`content = 0`) if any data files were added.
  if new_data_files:
    m_data_records = []
    total_added_rows = 0
    for df in new_data_files:
      rc = int(df['record_count'])
      total_added_rows += rc
      m_data_records.append({
          'status': 1,
          'snapshot_id': snap_id,
          'sequence_number': seq_num,
          'file_sequence_number': seq_num,
          'data_file': {
              'content': 0,
              'file_path': _to_iceberg_uri(df['path']),
              'file_format': 'PARQUET',
              'partition': {},
              'record_count': rc,
              'file_size_in_bytes': int(df['file_size_in_bytes']),
              'equality_ids': None,
              'sort_order_id': 0,
          },
      })
    m_data_bytes = _write_avro_bytes(
        _ICEBERG_V2_MANIFEST_SCHEMA,
        m_data_records,
        metadata={
            'schema': json.dumps(cur_schema),
            'schema-id': str(cur_schema.get('schema-id', 0)),
            'partition-spec': json.dumps(part_spec['fields']),
            'partition-spec-id': '0',
            'format-version': '2',
            'content': 'data',
        })
    m_data_path = _join_fs(meta_dir, f'{commit_uuid}-m0.avro')
    _fs_write_bytes(m_data_path, m_data_bytes)
    new_ml_entries.append({
        'manifest_path': _to_iceberg_uri(m_data_path),
        'manifest_length': len(m_data_bytes),
        'partition_spec_id': 0,
        'content': 0,
        'sequence_number': seq_num,
        'min_sequence_number': seq_num,
        'added_snapshot_id': snap_id,
        'added_files_count': len(new_data_files),
        'existing_files_count': 0,
        'deleted_files_count': 0,
        'added_rows_count': total_added_rows,
        'existing_rows_count': 0,
        'deleted_rows_count': 0,
    })

  # 2. Write equality-delete manifest (`content = 1` in manifest-list, `2` in entry)
  eq_files = list(new_eq_delete_files or [])
  if eq_files:
    eq_col_names = list(identifier_columns or [
        k for k, fid in name_to_id.items()
        if fid in (cur_schema.get('identifier-field-ids') or [])
    ])
    eq_ids = [name_to_id[c] for c in eq_col_names if c in name_to_id]
    del_schema = {
        'type': 'struct',
        'schema-id': cur_schema.get('schema-id', 0),
        'fields': [f for f in cur_schema['fields'] if int(f['id']) in eq_ids],
    }
    m_del_records = []
    total_del_rows = 0
    for ef in eq_files:
      rc = int(ef['record_count'])
      total_del_rows += rc
      m_del_records.append({
          'status': 1,
          'snapshot_id': snap_id,
          'sequence_number': seq_num,
          'file_sequence_number': seq_num,
          'data_file': {
              'content': 2,
              'file_path': _to_iceberg_uri(ef['path']),
              'file_format': 'PARQUET',
              'partition': {},
              'record_count': rc,
              'file_size_in_bytes': int(ef['file_size_in_bytes']),
              'equality_ids': eq_ids,
              'sort_order_id': None,
          },
      })
    m_del_bytes = _write_avro_bytes(
        _ICEBERG_V2_MANIFEST_SCHEMA,
        m_del_records,
        metadata={
            'schema': json.dumps(del_schema),
            'schema-id': str(cur_schema.get('schema-id', 0)),
            'partition-spec': '[]',
            'partition-spec-id': '0',
            'format-version': '2',
            'content': 'deletes',
        })
    m_del_path = _join_fs(meta_dir, f'{commit_uuid}-m1.avro')
    _fs_write_bytes(m_del_path, m_del_bytes)
    new_ml_entries.append({
        'manifest_path': _to_iceberg_uri(m_del_path),
        'manifest_length': len(m_del_bytes),
        'partition_spec_id': 0,
        'content': 1,
        'sequence_number': seq_num,
        'min_sequence_number': seq_num,
        'added_snapshot_id': snap_id,
        'added_files_count': len(eq_files),
        'existing_files_count': 0,
        'deleted_files_count': 0,
        'added_rows_count': total_del_rows,
        'existing_rows_count': 0,
        'deleted_rows_count': 0,
    })

  all_ml = prior_ml_entries + new_ml_entries
  ml_bytes = _write_avro_bytes(
      _ICEBERG_V2_MANIFEST_LIST_SCHEMA,
      all_ml,
      metadata={
          'format-version': '2',
          'snapshot-id': str(snap_id),
          'parent-snapshot-id': (
              str(parent_snap_id) if parent_snap_id is not None else 'null'),
          'sequence-number': str(seq_num),
      })
  ml_path = _join_fs(meta_dir, f'snap-{snap_id}-1-{commit_uuid}.avro')
  _fs_write_bytes(ml_path, ml_bytes)

  snap_obj: dict[str, Any] = {
      'snapshot-id': snap_id,
      'sequence-number': seq_num,
      'timestamp-ms': now_ms,
      'summary': {
          'operation': 'overwrite' if eq_files else 'append',
          'added-data-files': str(len(new_data_files)),
          'added-delete-files': str(len(eq_files)),
      },
      'manifest-list': _to_iceberg_uri(ml_path),
      'schema-id': cur_schema.get('schema-id', 0),
  }
  if parent_snap_id is not None:
    snap_obj['parent-snapshot-id'] = parent_snap_id

  last_col_id = max((int(f['id']) for f in cur_schema['fields']), default=0)
  meta_json_obj = {
      'format-version': 2,
      'table-uuid': table_uuid,
      'location': _to_iceberg_uri(table_dir),
      'last-sequence-number': seq_num,
      'last-updated-ms': now_ms,
      'last-column-id': last_col_id,
      'current-schema-id': cur_schema.get('schema-id', 0),
      'schemas': [cur_schema],
      'default-spec-id': 0,
      'partition-specs': [part_spec],
      'last-partition-id': 999,
      'default-sort-order-id': 0,
      'sort-orders': [{'order-id': 0, 'fields': []}],
      'properties': {
          'write.format.default': 'parquet',
          'schema.name-mapping.default': json.dumps(name_mapping),
      },
      'current-snapshot-id': snap_id,
      'snapshots': prior_snapshots + [snap_obj],
      'snapshot-log': prior_snap_log + [{
          'timestamp-ms': now_ms, 'snapshot-id': snap_id
      }],
      'metadata-log': prior_meta_log,
  }

  new_meta_path = _join_fs(meta_dir, f'v{meta_ver}.metadata.json')
  _fs_write_bytes(
      new_meta_path,
      json.dumps(meta_json_obj, indent=2).encode('utf-8'),
      mime_type='application/json')
  _fs_write_bytes(
      _join_fs(meta_dir, 'version-hint.text'),
      f'{meta_ver}\n'.encode('utf-8'),
      mime_type='text/plain')

  return {
      'table_location': table_dir,
      'metadata_path': new_meta_path,
      'metadata_version': meta_ver,
      'snapshot_id': snap_id,
      'sequence_number': seq_num,
      'added_data_files': len(new_data_files),
      'added_delete_files': len(eq_files),
  }


def write_parquet_with_field_ids(
    dest_path: str,
    rows: list[dict[str, Any]],
    iceberg_fields: list[dict[str, Any]],
) -> dict[str, Any]:
  """Writes `rows` to a Parquet file carrying `PARQUET:field_id` column metadata."""
  import pyarrow as pa
  import pyarrow.parquet as pq

  pa_fields = []
  cols_data: dict[str, list[Any]] = {f['name']: [] for f in iceberg_fields}
  for idx, f in enumerate(iceberg_fields, start=1):
    fid = int(f.get('id') or idx)
    pa_t = _iceberg_type_to_arrow_type(f['type'])
    pa_fields.append(
        pa.field(
            f['name'],
            pa_t,
            nullable=bool(f.get('nullable', True)),
            metadata={b'PARQUET:field_id': str(fid).encode('ascii')}))

  for r in rows:
    for f in iceberg_fields:
      cols_data[f['name']].append(r.get(f['name']))

  pa_schema = pa.schema(pa_fields)
  arrays = [
      pa.array(cols_data[f['name']], type=_iceberg_type_to_arrow_type(f['type']))
      for f in iceberg_fields
  ]
  tbl = pa.Table.from_arrays(arrays, schema=pa_schema)
  buf = io.BytesIO()
  pq.write_table(tbl, buf, compression='snappy')
  raw = buf.getvalue()
  _fs_write_bytes(dest_path, raw)
  return {
      'path': dest_path,
      'record_count': len(rows),
      'file_size_in_bytes': len(raw),
  }


def register_zero_copy_to_iceberg(
    source_ep,
    sink_ep,
    *,
    if_exists: str = 'fail',
    default_project: Optional[str] = None,
) -> dict[str, Any]:
  """Registers existing Parquet or `delta://` active Parquet files into Iceberg v2."""
  import pyarrow.parquet as pq

  if sink_ep.kind != 'iceberg':
    raise LakehouseError(
        f'--zero-copy requires an iceberg:// destination, got {sink_ep.raw!r}')

  parquet_files: list[tuple[str, int]] = []
  if source_ep.kind == 'delta':
    ver = source_ep.config.get('version')
    snap = parse_delta_log(
        source_ep.config['table'],
        target_version=int(ver) if ver is not None else None)
    if snap['partition_columns']:
      raise LakehouseError(
          'Delta table uses Hive directory partitioning where partition column '
          'values live in folder paths rather than Parquet columns; run '
          'standard `beam pipe` without --zero-copy to materialize partition '
          'columns into Iceberg Parquet files')
    parquet_files = [(f['path'], int(f['size'])) for f in snap['active_files']]
  elif source_ep.kind == 'file' and source_ep.format == 'parquet':
    parquet_files = _fs_list_files(source_ep.config['path'])
  else:
    raise LakehouseError(
        '--zero-copy supports delta:// tables and Parquet file/glob sources; '
        f'got {source_ep.raw!r}')

  if not parquet_files:
    raise FileNotFoundError(
        f'no active Parquet files found to register from {source_ep.raw!r}')

  table_dir = resolve_iceberg_table_dir(
      sink_ep.config, default_project=default_project)
  if if_exists == 'fail':
    try:
      find_latest_iceberg_metadata_path(table_dir)
      raise LakehouseError(
          f'Iceberg table already exists at {table_dir!r}; pass '
          '--if-exists=append or --if-exists=replace')
    except FileNotFoundError:
      pass

  iceberg_fields: list[dict[str, Any]] = []
  data_entries: list[dict[str, Any]] = []
  total_rows = 0
  for fpath, fsize in parquet_files:
    with _fs_open_read(fpath) as fobj:
      pf = pq.ParquetFile(fobj)
      if not iceberg_fields:
        for idx, field in enumerate(pf.schema_arrow, start=1):
          iceberg_fields.append({
              'id': idx,
              'name': field.name,
              'type': _arrow_type_to_iceberg_type(field.type),
              'nullable': bool(field.nullable),
          })
      nrows = int(pf.metadata.num_rows)
      total_rows += nrows
      data_entries.append({
          'path': fpath,
          'record_count': nrows,
          'file_size_in_bytes': fsize,
      })

  commit_res = commit_iceberg_snapshot(
      table_dir,
      iceberg_fields=iceberg_fields,
      new_data_files=data_entries,
      replace_table=(if_exists == 'replace'))
  commit_res['registered_files'] = [p for p, _ in parquet_files]
  commit_res['rows_registered'] = total_rows
  commit_res['bytes_copied'] = 0
  commit_res['zero_copy'] = True
  return commit_res


# ---------------------------------------------------------------------------
# 4. Beam PTransforms (`CliReadFromDelta*`, `CliReadFromIceberg*`, `CliWriteToIceberg`)
# ---------------------------------------------------------------------------


def _fields_to_row_type(schema_fields: list[dict[str, Any]], is_delta: bool = False):
  from apache_beam.typehints.row_type import RowTypeConstraint
  pairs = []
  for f in schema_fields:
    py_t = (
        _delta_type_to_py_type(f['type'])
        if is_delta else _iceberg_type_to_py_type(f['type']))
    pairs.append((f['name'], Optional[py_t] if f.get('nullable', True) else py_t))
  return RowTypeConstraint.from_fields(pairs)


def cli_read_from_delta(
    table: str,
    version: Optional[int] = None,
    **_extra_kwargs: Any,
):
  """Pure-Python distributed Delta Lake snapshot reader."""
  import apache_beam as beam

  snap = parse_delta_log(
      table, target_version=int(version) if version is not None else None)
  row_type = _fields_to_row_type(snap['schema'], is_delta=True)
  schema_spec = list(snap['schema'])
  active_files = list(snap['active_files'])

  class _ReadDeltaShardDoFn(beam.DoFn):
    def process(self, finfo):
      import pyarrow.parquet as pq
      type_map = {f['name']: _delta_type_to_py_type(f['type']) for f in schema_spec}
      pvals = finfo.get('partition_values') or {}
      with _fs_open_read(finfo['path']) as fobj:
        pf = pq.ParquetFile(fobj)
        for batch in pf.iter_batches():
          for raw_r in batch.to_pylist():
            rec = {
                f['name']: _coerce_scalar_for_py(
                    raw_r.get(f['name'], pvals.get(f['name'])),
                    type_map[f['name']])
                for f in schema_spec
            }
            yield beam.Row(**rec)

  class _ReadDeltaTransform(beam.PTransform):
    def expand(self, pbegin):
      return (
          pbegin
          | 'DeltaFiles' >> beam.Create(active_files)
          | 'ReshuffleDelta' >> beam.Reshuffle()
          | 'ReadDeltaParquet' >>
          beam.ParDo(_ReadDeltaShardDoFn()).with_output_types(row_type))

  return _ReadDeltaTransform()


def cli_read_from_delta_cdc(
    table: str,
    start_version: int = 0,
    end_version: Optional[int] = None,
    **_extra_kwargs: Any,
):
  """Pure-Python distributed Delta Change Data Feed / commit-diff reader."""
  import apache_beam as beam

  base_fields, actions = list_delta_cdc_actions(
      table,
      start_version=int(start_version),
      end_version=int(end_version) if end_version is not None else None)
  cdc_fields = list(base_fields) + [
      {'name': 'beam_cdc_op', 'type': 'string', 'nullable': False},
      {'name': 'beam_cdc_seq', 'type': 'long', 'nullable': False},
  ]
  row_type = _fields_to_row_type(cdc_fields, is_delta=True)

  class _ReadDeltaCdcDoFn(beam.DoFn):
    def process(self, act):
      import pyarrow.parquet as pq
      type_map = {f['name']: _delta_type_to_py_type(f['type']) for f in base_fields}
      pvals = act.get('partition_values') or {}
      ver = int(act['version'])
      with _fs_open_read(act['path']) as fobj:
        pf = pq.ParquetFile(fobj)
        for batch in pf.iter_batches():
          for raw_r in batch.to_pylist():
            rec = {
                f['name']: _coerce_scalar_for_py(
                    raw_r.get(f['name'], pvals.get(f['name'])),
                    type_map[f['name']])
                for f in base_fields
            }
            if act['kind'] == 'cdc_file':
              raw_op = str(raw_r.get('_change_type', 'insert'))
            elif act['kind'] == 'removed_file':
              raw_op = 'DELETE'
            else:
              raw_op = 'INSERT'
            rec['beam_cdc_op'] = raw_op
            rec['beam_cdc_seq'] = int(raw_r.get('_commit_version', ver))
            yield beam.Row(**rec)

  class _ReadDeltaCdcTransform(beam.PTransform):
    def expand(self, pbegin):
      return (
          pbegin
          | 'DeltaCdcActions' >> beam.Create(actions)
          | 'ReshuffleDeltaCdc' >> beam.Reshuffle()
          | 'ReadDeltaCdcRows' >>
          beam.ParDo(_ReadDeltaCdcDoFn()).with_output_types(row_type))

  return _ReadDeltaCdcTransform()


def cli_read_from_iceberg(
    table: str,
    catalog_name: Optional[str] = None,
    catalog_properties: Optional[dict[str, Any]] = None,
    **_extra_kwargs: Any,
):
  """Pure-Python Apache Iceberg v2 snapshot reader (honoring v2 deletes)."""
  import apache_beam as beam

  cfg = {
      'table': table,
      'catalog_name': catalog_name,
      'catalog_properties': catalog_properties or {},
  }
  table_dir = resolve_iceberg_table_dir(cfg)
  state = load_iceberg_state(table_dir)
  row_type = _fields_to_row_type(state['schema'], is_delta=False)
  schema_spec = list(state['schema'])
  pos_deletes, eq_deletes = _load_iceberg_deletes(state)
  data_files = list(state['data_files'])

  class _ReadIcebergDataDoFn(beam.DoFn):
    def process(self, df_info):
      import pyarrow.parquet as pq
      type_map = {
          f['name']: _iceberg_type_to_py_type(f['type']) for f in schema_spec
      }
      fpath = df_info['path']
      data_seq = int(df_info['sequence_number'])
      applicable_eq = [
          (cols, kset)
          for del_seq, cols, kset in eq_deletes if data_seq < del_seq
      ]
      file_pos = pos_deletes.get(fpath, set())
      with _fs_open_read(fpath) as fobj:
        pf = pq.ParquetFile(fobj)
        pos = 0
        for batch in pf.iter_batches():
          for raw_r in batch.to_pylist():
            cur_pos = pos
            pos += 1
            if cur_pos in file_pos:
              continue
            rec = {
                f['name']: _coerce_scalar_for_py(
                    raw_r.get(f['name']), type_map[f['name']])
                for f in schema_spec
            }
            if any(
                tuple(rec.get(c) for c in cols) in kset
                for cols, kset in applicable_eq):
              continue
            yield beam.Row(**rec)

  class _ReadIcebergTransform(beam.PTransform):
    def expand(self, pbegin):
      return (
          pbegin
          | 'IcebergFiles' >> beam.Create(data_files)
          | 'ReshuffleIceberg' >> beam.Reshuffle()
          | 'ReadIcebergParquet' >>
          beam.ParDo(_ReadIcebergDataDoFn()).with_output_types(row_type))

  return _ReadIcebergTransform()


def cli_read_from_iceberg_cdc(
    table: str,
    catalog_name: Optional[str] = None,
    catalog_properties: Optional[dict[str, Any]] = None,
    from_snapshot: Optional[int] = None,
    to_snapshot: Optional[int] = None,
    **_extra_kwargs: Any,
):
  """Pure-Python Apache Iceberg v2 incremental snapshot CDC reader."""
  import apache_beam as beam

  cfg = {
      'table': table,
      'catalog_name': catalog_name,
      'catalog_properties': catalog_properties or {},
  }
  table_dir = resolve_iceberg_table_dir(cfg)
  base_schema, changes = list_iceberg_cdc_changes(
      table_dir, from_snapshot=from_snapshot, to_snapshot=to_snapshot)
  cdc_fields = list(base_schema) + [
      {'name': 'beam_cdc_op', 'type': 'string', 'nullable': False},
      {'name': 'beam_cdc_seq', 'type': 'long', 'nullable': False},
  ]
  row_type = _fields_to_row_type(cdc_fields, is_delta=False)
  row_items = []
  for r in changes:
    clean_r = {f['name']: r.get(f['name']) for f in base_schema}
    clean_r['beam_cdc_op'] = str(r.get('_change_type', 'INSERT'))
    clean_r['beam_cdc_seq'] = int(r.get('_commit_snapshot_sequence_number', 0))
    row_items.append(beam.Row(**clean_r))

  class _ReadIcebergCdcTransform(beam.PTransform):
    def expand(self, pbegin):
      return (
          pbegin
          | 'IcebergCdcRows' >>
          beam.Create(row_items).with_output_types(row_type))

  return _ReadIcebergCdcTransform()


def _beam_element_type_to_iceberg_fields(
    element_type, drop_cols: set[str]
) -> list[dict[str, Any]]:
  from apache_beam.portability.api import schema_pb2
  from apache_beam.typehints import schemas

  beam_schema = schemas.schema_from_element_type(element_type)
  out: list[dict[str, Any]] = []
  idx = 1
  for f in beam_schema.fields:
    if f.name in drop_cols:
      continue
    ft = f.type
    nullable = bool(ft.nullable)
    atom = ft.atomic_type
    if atom == schema_pb2.BOOLEAN:
      itype = 'boolean'
    elif atom in (schema_pb2.INT16, schema_pb2.INT32):
      itype = 'int'
    elif atom == schema_pb2.INT64:
      itype = 'long'
    elif atom == schema_pb2.FLOAT:
      itype = 'float'
    elif atom == schema_pb2.DOUBLE:
      itype = 'double'
    elif atom == schema_pb2.BYTES:
      itype = 'binary'
    else:
      itype = 'string'
    out.append({
        'id': idx,
        'name': f.name,
        'type': itype,
        'nullable': nullable,
    })
    idx += 1
  return out


def cli_write_to_iceberg(
    table: str,
    *,
    catalog_name: Optional[str] = None,
    catalog_properties: Optional[dict[str, Any]] = None,
    mode: str = 'append',
    equality_columns: Optional[list[str]] = None,
    change_type_column: Optional[str] = None,
    sequence_number_column: Optional[str] = None,
    if_exists: str = 'append',
    triggering_frequency_seconds: Optional[int] = None,
    **_extra_kwargs: Any,
):
  """Pure-Python Apache Iceberg v2 sink supporting append & merge-on-read CDC."""
  import apache_beam as beam
  from apache_beam.transforms import trigger
  from apache_beam.transforms import window

  cfg = {
      'table': table,
      'catalog_name': catalog_name,
      'catalog_properties': catalog_properties or {},
  }
  table_dir = resolve_iceberg_table_dir(cfg)
  if if_exists == 'fail':
    try:
      find_latest_iceberg_metadata_path(table_dir)
      raise ValueError(
          f'Iceberg table already exists at {table_dir!r}; pass '
          '--if-exists=append or --if-exists=replace')
    except FileNotFoundError:
      pass

  meta_drop = {
      c for c in (
          change_type_column,
          sequence_number_column,
          'beam_cdc_op',
          'beam_cdc_seq',
          'beam_cdc_hex_seq',
      ) if c
  }
  eq_cols = list(equality_columns or [])
  is_mor = (mode == 'merge-on-read')

  class _CommitIcebergBatchDoFn(beam.DoFn):
    def __init__(self, fields_spec: list[dict[str, Any]], replace_first: bool):
      self._fields = fields_spec
      self._replace_first = replace_first

    def process(self, kv):
      _, rows_iter = kv
      raw_list = [
          r._asdict() if hasattr(r, '_asdict') else dict(r) for r in rows_iter
      ]
      if not raw_list:
        return

      # Check if table already has field IDs on disk so IDs remain stable!
      eff_fields = list(self._fields)
      table_exists = False
      if not self._replace_first:
        try:
          st = load_iceberg_state(table_dir)
          table_exists = True
          existing_id_by_name = {f['name']: f['id'] for f in st['schema']}
          eff_fields = [{
              **f, 'id': existing_id_by_name.get(f['name'], f['id'])
          } for f in eff_fields]
        except FileNotFoundError:
          table_exists = False

      batch_id = uuid.uuid4().hex
      data_dir = _join_fs(table_dir, 'data')
      new_data: list[dict[str, Any]] = []
      new_eq_del: list[dict[str, Any]] = []

      if not is_mor:
        clean_rows = [{
            f['name']: r.get(f['name']) for f in eff_fields
        } for r in raw_list]
        d_path = _join_fs(data_dir, f'00000-0-{batch_id}.parquet')
        new_data.append(
            write_parquet_with_field_ids(d_path, clean_rows, eff_fields))
      else:
        if not eq_cols:
          raise ValueError(
              'Iceberg merge-on-read CDC requires --primary-key / equality_columns')
        # Deduplicate per primary key within this commit window by max sequence.
        latest_by_key: dict[tuple[Any, ...], tuple[int, int, dict[str, Any]]] = {}
        for arrival_idx, r in enumerate(raw_list):
          op_raw = str(
              r.get(change_type_column or 'beam_cdc_op', 'UPSERT')).upper()
          if op_raw in ('UPDATE_BEFORE', 'UPDATE_PREIMAGE'):
            continue
          seq_val = int(r.get(sequence_number_column or 'beam_cdc_seq') or 0)
          k_tup = tuple(r.get(c) for c in eq_cols)
          prev = latest_by_key.get(k_tup)
          if prev is None or (seq_val, arrival_idx) >= (prev[0], prev[1]):
            latest_by_key[k_tup] = (seq_val, arrival_idx, r)

        upserts: list[dict[str, Any]] = []
        eq_delete_keys: list[dict[str, Any]] = []
        for k_tup, (_, _, winner) in latest_by_key.items():
          op_w = str(
              winner.get(change_type_column or 'beam_cdc_op', 'UPSERT')).upper()
          key_rec = {c: val for c, val in zip(eq_cols, k_tup)}
          if op_w == 'DELETE':
            eq_delete_keys.append(key_rec)
          else:
            upserts.append({
                f['name']: winner.get(f['name']) for f in eff_fields
            })
            if table_exists:
              # Equality delete removes any older snapshot's version of this key
              # (`data_seq < current_snap_seq`) while keeping this snapshot's row!
              eq_delete_keys.append(key_rec)

        if upserts:
          d_path = _join_fs(data_dir, f'00000-0-{batch_id}.parquet')
          new_data.append(
              write_parquet_with_field_ids(d_path, upserts, eff_fields))
        if eq_delete_keys:
          eq_field_specs = [f for f in eff_fields if f['name'] in set(eq_cols)]
          del_path = _join_fs(data_dir, f'00000-0-{batch_id}-eq-del.parquet')
          new_eq_del.append(
              write_parquet_with_field_ids(
                  del_path, eq_delete_keys, eq_field_specs))

      if new_data or new_eq_del:
        commit_iceberg_snapshot(
            table_dir,
            iceberg_fields=eff_fields,
            identifier_columns=eq_cols if is_mor else None,
            new_data_files=new_data,
            new_eq_delete_files=new_eq_del,
            replace_table=self._replace_first)
        self._replace_first = False

  class _CliWriteIcebergTransform(beam.PTransform):
    def expand(self, pcoll):
      fields_spec = _beam_element_type_to_iceberg_fields(
          pcoll.element_type, meta_drop)
      keyed = pcoll | 'KeySingleCommitter' >> beam.Map(lambda r: (0, r))
      if pcoll.is_bounded:
        grouped = keyed | 'GroupBatchCommit' >> beam.GroupByKey()
      else:
        freq = max(int(triggering_frequency_seconds or 5), 1)
        grouped = (
            keyed
            | 'IcebergCommitWindows' >> beam.WindowInto(
                window.GlobalWindows(),
                trigger=trigger.Repeatedly(
                    trigger.AfterProcessingTime(freq)),
                accumulation_mode=trigger.AccumulationMode.DISCARDING)
            | 'GroupStreamCommit' >> beam.GroupByKey())
      _ = grouped | 'CommitIcebergSnapshot' >> beam.ParDo(
          _CommitIcebergBatchDoFn(
              fields_spec, replace_first=(if_exists == 'replace')))
      return None

  return _CliWriteIcebergTransform()
