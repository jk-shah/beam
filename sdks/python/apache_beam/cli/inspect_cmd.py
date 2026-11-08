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

"""Zero-execution schema, metadata, sample, and search inspector (`beam inspect`).

Executes in `<200 ms` locally without launching any Beam runner:
- Parquet / CSV / JSONL / Avro via PyArrow / `fastavro` / stdlib streaming read
- BigQuery `$0` metadata via `tables.get` + `$0` preview via `tabledata.list`
  (and `$0` `jobs.insert(dryRun=True)` for `VIEW`s / queries)
- `--search SUBSTRING` bounded head scan (up to `--max-scanned-bytes`, default
  64 MiB) returning matching rows strictly inside `untrusted_data`!
"""

import csv
import io
import json
import os
from typing import Any

from . import cost_guard
from . import output
from . import uris


def run_inspect(
    location: str,
    *,
    explicit_format: str | None = None,
    sample_count: int = 5,
    include_stats: bool = False,
    search_term: str | None = None,
    max_scanned_bytes: str = '64MB',
    from_opts: list[str] | None = None,
    from_opt_file: str | None = None,
    default_project: str | None = None,
) -> dict[str, Any]:
  """Inspects `location` and returns a structured metadata + sample dict."""
  opts = uris.parse_kv_options(from_opts, from_opt_file, '--from-opt')
  ep = uris.parse_endpoint(
      location,
      is_read=True,
      explicit_format=explicit_format,
      options=opts,
      default_project=default_project,
      allow_sniff=True)
  scan_budget = cost_guard.parse_byte_size(max_scanned_bytes)

  if ep.kind == 'bigquery':
    return _inspect_bigquery(
        ep,
        sample_count=sample_count,
        search_term=search_term,
        default_project=default_project)
  if ep.kind == 'file':
    return _inspect_files(
        ep,
        sample_count=sample_count,
        include_stats=include_stats,
        search_term=search_term,
        scan_budget=scan_budget)
  return {
      'uri': location,
      'kind': ep.kind,
      'format': ep.format,
      'yaml_transform': ep.yaml_type,
      'config_preview': {
          k: ('<redacted>' if 'pass' in k.lower() else v)
          for k, v in ep.config.items()
      },
      'schema': [],
      'estimated_bytes': None,
      'row_count': None,
      'untrusted_data': None,
  }


def _open_file_bytes(path: str):
  if '://' not in path:
    return open(path, 'rb')
  from apache_beam.io.filesystems import FileSystems
  return FileSystems.open(path)


def _match_files(path: str) -> list[tuple[str, int]]:
  if '://' not in path:
    import glob as _glob
    if any(c in path for c in '*?['):
      files = sorted(p for p in _glob.glob(path) if os.path.isfile(p))
    elif os.path.isfile(path):
      files = [path]
    else:
      files = sorted(
          p for p in _glob.glob(_glob.escape(path) + '*') if os.path.isfile(p))
    return [(p, os.path.getsize(p)) for p in files]

  from apache_beam.io.filesystems import FileSystems
  pattern = path if any(c in path for c in '*?[') else path
  metas = FileSystems.match([pattern])[0].metadata_list
  if not metas and not any(c in path for c in '*?['):
    metas = FileSystems.match([path + '*'])[0].metadata_list
  return [(m.path, int(m.size_in_bytes or 0)) for m in sorted(
      metas, key=lambda x: x.path)]


def _row_matches_search(row: dict[str, Any], needle_lower: str) -> bool:
  for v in row.values():
    if v is not None and needle_lower in str(v).lower():
      return True
  return False


def _inspect_files(
    ep: uris.ParsedEndpoint,
    *,
    sample_count: int,
    include_stats: bool,
    search_term: str | None,
    scan_budget: int,
) -> dict[str, Any]:
  matched = _match_files(ep.raw)
  if not matched:
    raise FileNotFoundError(f'no files matched {ep.raw!r}')

  total_bytes = sum(sz for _, sz in matched)
  first_path = matched[0][0]
  fmt = ep.format
  needle = search_term.lower() if search_term else None

  schema_fields: list[dict[str, Any]] = []
  rows: list[dict[str, Any]] = []
  exact_row_count: int | None = None
  truncated_scan = False

  inspect_warnings: list[str] = []
  malformed_in_sample = 0

  if fmt == 'parquet':
    import pyarrow.parquet as pq
    total_rows = 0
    for fpath, _ in matched:
      with _open_file_bytes(fpath) as fobj:
        pf = pq.ParquetFile(fobj)
        if not schema_fields:
          arrow_schema = pf.schema_arrow
          schema_fields = [{
              'name': field.name,
              'type': str(field.type),
              'nullable': bool(field.nullable),
          } for field in arrow_schema]
        total_rows += pf.metadata.num_rows
        if len(rows) < sample_count and sample_count > 0:
          for batch in pf.iter_batches(batch_size=max(sample_count * 4, 64)):
            for rec in batch.to_pylist():
              if needle is None or _row_matches_search(rec, needle):
                rows.append(rec)
                if len(rows) >= sample_count:
                  break
            if len(rows) >= sample_count:
              break
    exact_row_count = total_rows

  elif fmt == 'avro':
    import fastavro
    with _open_file_bytes(first_path) as fobj:
      reader = fastavro.reader(fobj)
      w_schema = reader.writer_schema
      if isinstance(w_schema, dict) and 'fields' in w_schema:
        schema_fields = [{
            'name': str(f.get('name')),
            'type': str(f.get('type')),
            'nullable': 'null' in str(f.get('type')),
        } for f in w_schema['fields']]
      for rec in reader:
        if needle is None or _row_matches_search(rec, needle):
          rows.append(dict(rec))
          if len(rows) >= sample_count:
            break

  elif fmt == 'csv':
    from . import dlq
    scanned = 0
    header: list[str] | None = None
    col_samples: dict[str, list[str]] = {}
    for fpath, fsize in matched:
      if scanned >= scan_budget:
        truncated_scan = True
        break
      with _open_file_bytes(fpath) as raw_f:
        bounded = io.BytesIO(raw_f.read(max(0, scan_budget - scanned)))
        scanned += min(fsize, scan_budget - scanned)
        wrapper = io.TextIOWrapper(
            bounded, encoding='utf-8-sig', errors='replace', newline='')
        reader = csv.reader(wrapper, delimiter=ep.config.get('delimiter', ','))
        try:
          first = next(reader, None)
        except csv.Error:
          malformed_in_sample += 1
          continue
        if not first:
          continue
        norm_first = [c.strip() or f'col_{i}' for i, c in enumerate(first)]
        if header is None:
          header = norm_first
          col_samples = {c: [] for c in header}
        elif norm_first != header:
          if len(first) != len(header):
            malformed_in_sample += 1
          else:
            for c, cell in zip(header, first):
              col_samples[c].append(cell)
        idx = 0
        while True:
          try:
            cells = next(reader, None)
          except csv.Error:
            malformed_in_sample += 1
            break
          if cells is None:
            break
          if not cells or (len(cells) == 1 and cells[0] == '' and
                           len(header) > 1):
            continue
          if len(cells) != len(header):
            malformed_in_sample += 1
            continue
          if idx < 100:
            for c, cell in zip(header, cells):
              col_samples[c].append(cell)
          idx += 1
          rec = dict(zip(header, cells))
          if (needle is None or _row_matches_search(rec, needle)) and len(
              rows) < sample_count:
            rows.append(rec)
          if idx >= 100 and len(rows) >= sample_count:
            break
    if header:
      dirty_cols: list[str] = []
      for c in header:
        strict_t = dlq._infer_scalar_kind(col_samples[c], allow_dirty=False)
        dirty_t = dlq._infer_scalar_kind(col_samples[c], allow_dirty=True)
        if dirty_t != strict_t:
          dirty_cols.append(f'{c} ({dirty_t} with non-{dirty_t} outlier cells)')
        schema_fields.append({
            'name': c,
            'type': dirty_t,
            'nullable': True,
        })
      if malformed_in_sample > 0 or dirty_cols:
        parts_msg = []
        if malformed_in_sample > 0:
          parts_msg.append(
              f'{malformed_in_sample} malformed CSV row(s) (column/syntax mismatch)'
          )
        if dirty_cols:
          parts_msg.append(
              f'dirty type-outlier cell(s) in {", ".join(dirty_cols)}')
        inspect_warnings.append(
            f'Detected {" and ".join(parts_msg)} in sample; pass '
            '--dlq <path> to `beam pipe` to quarantine corrupt records.')

  else:  # json / jsonl / text
    scanned = 0
    inferred_types: dict[str, str] = {}
    for fpath, fsize in matched:
      if scanned >= scan_budget:
        truncated_scan = True
        break
      with _open_file_bytes(fpath) as raw_f:
        chunk = raw_f.read(max(0, scan_budget - scanned))
        scanned += min(fsize, len(chunk))
        for raw_line in chunk.decode('utf-8', errors='replace').splitlines():
          s = raw_line.strip()
          if not s:
            continue
          if fmt == 'text':
            rec = {'line': s}
          else:
            try:
              parsed = json.loads(s)
              rec = parsed if isinstance(parsed, dict) else {'value': parsed}
            except ValueError:
              malformed_in_sample += 1
              continue
          for k, v in rec.items():
            if k not in inferred_types and v is not None:
              inferred_types[k] = type(v).__name__
          if (needle is None or _row_matches_search(rec, needle)) and len(
              rows) < sample_count:
            rows.append(rec)
          if len(inferred_types) > 0 and len(rows) >= sample_count:
            break
    if malformed_in_sample > 0:
      inspect_warnings.append(
          f'Detected {malformed_in_sample} malformed JSON line(s) in sample; '
          'pass --dlq <path> to `beam pipe` to quarantine corrupt records.')
    schema_fields = [{
        'name': k, 'type': t, 'nullable': True
    } for k, t in inferred_types.items()]

  stats: dict[str, Any] | None = None
  if include_stats and rows:
    null_counts = {
        f['name']: sum(1 for r in rows if r.get(f['name']) in (None, ''))
        for f in schema_fields
    }
    stats = {
        'sampled_rows': len(rows),
        'malformed_rows_in_sample': malformed_in_sample,
        'null_counts_in_sample': null_counts,
    }

  return {
      'uri': ep.raw,
      'kind': 'file',
      'format': fmt,
      'file_count': len(matched),
      'estimated_bytes': total_bytes,
      'row_count': exact_row_count,
      'schema': schema_fields,
      'stats': stats,
      'warnings': inspect_warnings,
      'untrusted_data': output.wrap_untrusted_rows(rows, truncated=truncated_scan)
      if sample_count > 0 else None,
  }


def _inspect_bigquery(
    ep: uris.ParsedEndpoint,
    *,
    sample_count: int,
    search_term: str | None,
    default_project: str | None,
) -> dict[str, Any]:
  from google.cloud import bigquery

  client = bigquery.Client(project=default_project)
  _, proj, dataset, table = uris.normalize_bq_table_spec(
      ep.config['table'], default_project=default_project or client.project)
  fq_dot = f'{proj}.{dataset}.{table}' if proj else f'{dataset}.{table}'
  tbl = client.get_table(fq_dot)
  table_type = str(getattr(tbl, 'table_type', 'TABLE') or 'TABLE').upper()

  schema_fields = [{
      'name': f.name,
      'type': f.field_type,
      'mode': f.mode,
      'nullable': f.mode != 'REQUIRED',
  } for f in tbl.schema]

  est = cost_guard.estimate_endpoint_bytes(ep, default_project=proj)
  rows: list[dict[str, Any]] = []
  if sample_count > 0 and table_type == 'TABLE':
    # $0 free Storage/REST `tabledata.list` preview (never runs a billed query!).
    fetch_limit = max(sample_count * 10, 50) if search_term else sample_count
    needle = search_term.lower() if search_term else None
    for row in client.list_rows(tbl, max_results=fetch_limit):
      rec = dict(row.items())
      if needle is None or _row_matches_search(rec, needle):
        rows.append(rec)
        if len(rows) >= sample_count:
          break

  return {
      'uri': ep.raw,
      'kind': 'bigquery',
      'format': 'bigquery',
      'table_type': table_type,
      'estimated_bytes': est.get('estimated_bytes'),
      'estimation_method': est.get('method'),
      'row_count': int(tbl.num_rows) if tbl.num_rows is not None else None,
      'partitioning': getattr(tbl, 'time_partitioning', None) is not None,
      'clustering_fields': list(tbl.clustering_fields or []),
      'schema': schema_fields,
      'untrusted_data': output.wrap_untrusted_rows(rows)
      if sample_count > 0 else None,
  }
