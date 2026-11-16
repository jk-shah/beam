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

  if ep.kind == 'delta':
    from . import lakehouse
    return lakehouse.inspect_delta_table(
        ep, sample_count=sample_count, search_term=search_term)
  if ep.kind == 'iceberg':
    from . import lakehouse
    return lakehouse.inspect_iceberg_table(
        ep,
        sample_count=sample_count,
        search_term=search_term,
        default_project=default_project)
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
          k: ('<redacted>' if uris._SECRET_KEY_RE.search(str(k)) else v)
          for k, v in ep.config.items()
      },
      'schema': [],
      'estimated_bytes': None,
      'row_count': None,
      'untrusted_data': None,
  }


def _open_file_bytes(path: str, *, decompress: bool = False):
  if decompress:
    from . import dlq
    return dlq._open_auto(path)
  if '://' not in path:
    return open(path, 'rb')
  from apache_beam.io.filesystems import FileSystems
  return FileSystems.open(path)


def _match_files(path: str) -> list[tuple[str, int]]:
  has_wild = any(c in path for c in '*?[')
  if '://' not in path:
    import glob as _glob
    if has_wild:
      files = sorted(p for p in _glob.glob(path) if os.path.isfile(p))
    elif os.path.isfile(path):
      files = [path]
    elif os.path.isdir(path):
      files = sorted(
          os.path.join(path, fn) for fn in os.listdir(path)
          if not fn.startswith('.') and os.path.isfile(os.path.join(path, fn)))
    else:
      files = sorted(
          p for p in _glob.glob(_glob.escape(path) + '*')
          if os.path.isfile(p) and cost_guard.is_owned_by_prefix(p, path))
    return [(p, os.path.getsize(p)) for p in files]

  from apache_beam.io.filesystems import FileSystems
  metas = FileSystems.match([path])[0].metadata_list
  if not metas and not has_wild:
    metas = [
        m for m in FileSystems.match([path + '*'])[0].metadata_list
        if cost_guard.is_owned_by_prefix(m.path, path)
    ]
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
  from . import dlq
  dlq.ensure_csv_field_size_limit()

  matched = _match_files(ep.raw)
  if not matched:
    raise FileNotFoundError(f'no files matched {ep.raw!r}')

  total_bytes = sum(sz for _, sz in matched)
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
    scanned = 0
    scanned_all_footers = True
    for fpath, fsize in matched:
      if scanned >= scan_budget and schema_fields:
        truncated_scan = True
        scanned_all_footers = False
        break
      with _open_file_bytes(fpath) as fobj:
        pf = pq.ParquetFile(fobj)
        if not schema_fields:
          arrow_schema = pf.schema_arrow
          schema_fields = [{
              'name': output.sanitize_untrusted_text(
                  str(field.name), max_chars=256),
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
      scanned += min(max(fsize, 1), 16384)
    exact_row_count = total_rows if scanned_all_footers else None

  elif fmt == 'avro':
    import fastavro
    scanned = 0
    for fpath, _ in matched:
      if scanned >= scan_budget or (len(rows) >= sample_count and schema_fields):
        if scanned >= scan_budget:
          truncated_scan = True
        break
      with _open_file_bytes(fpath) as fobj:
        reader = fastavro.reader(fobj)
        w_schema = reader.writer_schema
        if not schema_fields and isinstance(w_schema, dict) and 'fields' in w_schema:
          schema_fields = [{
              'name': output.sanitize_untrusted_text(
                  str(f.get('name')), max_chars=256),
              'type': str(f.get('type')),
              'nullable': 'null' in str(f.get('type')),
          } for f in w_schema['fields']]
        start_pos = 0
        for rec in reader:
          if needle is None or _row_matches_search(rec, needle):
            rows.append(dict(rec))
            if len(rows) >= sample_count:
              break
          try:
            pos = int(fobj.tell())
          except Exception:  # pylint: disable=broad-except
            pos = start_pos + 256
          if scanned + (pos - start_pos) >= scan_budget:
            scanned = scan_budget
            truncated_scan = True
            break
        try:
          scanned += int(fobj.tell())
        except Exception:  # pylint: disable=broad-except
          pass

  elif fmt == 'csv':
    scanned = 0
    header: list[str] | None = None
    col_samples: dict[str, list[str]] = {}
    delim = str(ep.config.get('delimiter', ',') or ',')
    for fpath, fsize in matched:
      if scanned >= scan_budget:
        truncated_scan = True
        break
      with _open_file_bytes(fpath, decompress=True) as raw_f:
        chunk_bytes = raw_f.read(max(0, scan_budget - scanned))
        scanned += min(max(fsize, len(chunk_bytes)), scan_budget - scanned)
        bounded = io.BytesIO(chunk_bytes)
        wrapper = io.TextIOWrapper(
            bounded, encoding='utf-8-sig', errors='replace', newline='')
        try:
          rec_iter = dlq.iter_csv_rows_recovering(wrapper, delimiter=delim)
          first = None
          for cells, csv_err, _ in rec_iter:
            if csv_err is not None:
              malformed_in_sample += 1
              continue
            if cells:
              first = cells
              break
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
          for cells, csv_err, _ in rec_iter:
            if csv_err is not None:
              malformed_in_sample += 1
              continue
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
        finally:
          try:
            wrapper.detach()
          except Exception:  # pylint: disable=broad-except
            pass
    if header:
      dirty_cols: list[str] = []
      for c in header:
        strict_t = dlq._infer_scalar_kind(col_samples[c], allow_dirty=False)
        dirty_t = dlq._infer_scalar_kind(col_samples[c], allow_dirty=True)
        safe_c = output.sanitize_untrusted_text(str(c), max_chars=256)
        if dirty_t != strict_t:
          shown_c = (
              safe_c if output.SAFE_IDENTIFIER_RE.match(safe_c) else
              '<unsafe-column-name>')
          dirty_cols.append(f'{shown_c} ({dirty_t} with non-{dirty_t} outlier cells)')
        schema_fields.append({
            'name': safe_c,
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
    seen_keys: list[str] = []
    seen_key_set: set[str] = set()
    inferred_types: dict[str, str] = {}
    min_schema_records = max(100, sample_count)
    records_scanned = 0

    def _observe_record(rec_obj: dict[str, Any]) -> None:
      nonlocal records_scanned
      records_scanned += 1
      for k, v in rec_obj.items():
        if k not in seen_key_set:
          seen_key_set.add(k)
          seen_keys.append(k)
        if v is not None:
          val_t = type(v).__name__
          prev_t = inferred_types.get(k)
          if prev_t is None:
            inferred_types[k] = val_t
          elif {prev_t, val_t} == {'int', 'float'}:
            inferred_types[k] = 'float'
      if (needle is None or _row_matches_search(rec_obj, needle)) and len(
          rows) < sample_count:
        rows.append(rec_obj)

    for fpath, fsize in matched:
      if scanned >= scan_budget:
        truncated_scan = True
        break
      with _open_file_bytes(fpath, decompress=True) as raw_f:
        chunk = raw_f.read(max(0, scan_budget - scanned))
        scanned += min(max(fsize, len(chunk)), scan_budget - scanned)
        text_chunk = chunk.decode('utf-8-sig', errors='replace')
        stripped_whole = text_chunk.lstrip()
        if fmt != 'text' and stripped_whole.startswith('['):
          try:
            top_parsed = json.loads(stripped_whole)
            if isinstance(top_parsed, list):
              for item in top_parsed:
                rec = item if isinstance(item, dict) else {'value': item}
                _observe_record(rec)
                if (records_scanned >= min_schema_records and
                    len(rows) >= sample_count):
                  break
              if (records_scanned >= min_schema_records and
                  len(rows) >= sample_count):
                break
              continue
          except ValueError:
            pass

        for raw_line in text_chunk.splitlines():
          s = raw_line.strip()
          if not s:
            continue
          if fmt == 'text':
            _observe_record({'line': s})
          else:
            try:
              parsed = json.loads(s)
            except ValueError:
              malformed_in_sample += 1
              continue
            if isinstance(parsed, list) and all(
                isinstance(el, dict) for el in parsed):
              for el in parsed:
                _observe_record(el)
            else:
              rec = parsed if isinstance(parsed, dict) else {'value': parsed}
              _observe_record(rec)
          if (records_scanned >= min_schema_records and
              len(rows) >= sample_count):
            break
      if records_scanned >= min_schema_records and len(rows) >= sample_count:
        break

    if malformed_in_sample > 0:
      inspect_warnings.append(
          f'Detected {malformed_in_sample} malformed JSON line(s) in sample; '
          'pass --dlq <path> to `beam pipe` to quarantine corrupt records.')
    schema_fields = [{
        'name': output.sanitize_untrusted_text(str(k), max_chars=256),
        'type': inferred_types.get(k, 'str'),
        'nullable': True,
    } for k in seen_keys]

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
  proj = uris.validate_bq_project_id(proj)
  fq_dot = f'{proj}.{dataset}.{table}' if proj else f'{dataset}.{table}'
  tbl = client.get_table(fq_dot)
  table_type = str(getattr(tbl, 'table_type', 'TABLE') or 'TABLE').upper()

  schema_fields = [{
      'name': output.sanitize_untrusted_text(str(f.name), max_chars=256),
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
