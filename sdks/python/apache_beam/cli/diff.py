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

"""Cross-source distributed dataset reconciliation (`beam diff`).

Compares two bounded sources (`--left` / `--from` vs `--right` / `--to`) across
local/cloud files (`csv`, `json`, `parquet`, `avro`, extensionless shards) and
`bigquery://` tables:
- Uses tagged `(int, bytes)` primitive key tuples so Beam's `CoGroupByKey` /
  `CombinePerKey` deterministic key coder checks always succeed.
- Uses map-side `CombinePerKey` ($O(1)$ memory per key even under millions of
  duplicate or `NULL` keys) before `CoGroupByKey`.
- Handles `NaN == NaN`, exact `Decimal` comparison (`rtol == 0 and atol == 0`),
  and float tolerance (`--rtol`, `--atol`), isolating any sampled drift keys
  inside `untrusted_data`.
"""

import decimal
import hashlib
import json
import math
from typing import Any
from typing import Iterable

from . import cost_guard
from . import output
from . import uris

EXIT_OK = 0
EXIT_DIFF_MISMATCH = 6


def _canonical_number_bytes(val: int | float | decimal.Decimal) -> bytes:
  if isinstance(val, float):
    if math.isnan(val):
      return b'NaN'
    if math.isinf(val):
      return b'+Inf' if val > 0 else b'-Inf'
    dec = decimal.Decimal(str(val))
  elif isinstance(val, int):
    dec = decimal.Decimal(val)
  else:
    dec = val
  if dec.is_nan():
    return b'NaN'
  if dec.is_infinite():
    return b'+Inf' if dec > 0 else b'-Inf'
  norm = dec.normalize()
  # Treat -0 and 0 identically.
  if norm == 0:
    return b'0'
  return format(norm, 'f').encode('ascii')


def _encode_key_atom(val: Any) -> tuple[int, bytes]:
  """Encodes a key value into a deterministic `(tag, bytes)` primitive pair."""
  if val is None:
    return (0, b'')
  if isinstance(val, bool):
    return (1, b'\x01' if val else b'\x00')
  if isinstance(val, (int, float, decimal.Decimal)):
    return (2, _canonical_number_bytes(val))
  if isinstance(val, str):
    return (3, val.encode('utf-8', errors='surrogatepass'))
  if isinstance(val, (bytes, bytearray, memoryview)):
    return (4, bytes(val))
  if hasattr(val, 'to_rfc3339'):
    return (5, val.to_rfc3339().encode('utf-8'))
  if hasattr(val, 'isoformat'):
    return (5, val.isoformat().encode('utf-8'))
  return (6, repr(val).encode('utf-8'))


def _row_to_plain_dict(row: Any) -> dict[str, Any]:
  if hasattr(row, '_asdict'):
    return dict(row._asdict())
  if isinstance(row, dict):
    return dict(row)
  return {'value': row}


def _canonical_row_digest(
    row_dict: dict[str, Any], float_sig_digits: int | None = None) -> bytes:
  parts: list[tuple[str, int, str]] = []
  for k in sorted(row_dict.keys()):
    v = row_dict[k]
    if v is None:
      parts.append((k, 0, ''))
    elif isinstance(v, bool):
      parts.append((k, 1, '1' if v else '0'))
    elif isinstance(v, (int, float, decimal.Decimal)):
      if float_sig_digits is not None and not (
          isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        parts.append((k, 2, f'{float(v):.{float_sig_digits}g}'))
      else:
        parts.append((k, 2, _canonical_number_bytes(v).decode('ascii')))
    elif isinstance(v, (bytes, bytearray, memoryview)):
      parts.append((k, 4, bytes(v).hex()))
    elif hasattr(v, 'to_rfc3339'):
      parts.append((k, 5, v.to_rfc3339()))
    elif hasattr(v, 'isoformat'):
      parts.append((k, 5, v.isoformat()))
    else:
      parts.append((k, 3, str(v)))
  raw = json.dumps(parts, separators=(',', ':')).encode('utf-8')
  return hashlib.sha256(raw).digest()


def _values_equal(a: Any, b: Any, rtol: float, atol: float) -> bool:
  if a is None and b is None:
    return True
  if a is None or b is None:
    return False
  if isinstance(a, bool) or isinstance(b, bool):
    return isinstance(a, bool) and isinstance(b, bool) and a == b
  if isinstance(a, (int, float, decimal.Decimal)) and isinstance(
      b, (int, float, decimal.Decimal)):
    fa_nan = (isinstance(a, float) and math.isnan(a)) or (
        isinstance(a, decimal.Decimal) and a.is_nan())
    fb_nan = (isinstance(b, float) and math.isnan(b)) or (
        isinstance(b, decimal.Decimal) and b.is_nan())
    if fa_nan or fb_nan:
      return fa_nan and fb_nan
    if rtol == 0.0 and atol == 0.0:
      return _canonical_number_bytes(a) == _canonical_number_bytes(b)
    return math.isclose(float(a), float(b), rel_tol=rtol, abs_tol=atol)
  if hasattr(a, 'to_rfc3339') and hasattr(b, 'to_rfc3339'):
    return a.to_rfc3339() == b.to_rfc3339()
  return a == b


def _read_endpoint_pcoll(pipeline, label: str, ep: uris.ParsedEndpoint):
  """Reads `ep` into a `PCollection` of `Row`s or `dict`s using pure Python."""
  import apache_beam as beam

  if ep.kind == 'bigquery':
    from apache_beam.io.gcp.bigquery import ReadFromBigQuery
    from apache_beam.options.pipeline_options import GoogleCloudOptions
    from apache_beam.yaml import yaml_io

    bq_cfg = dict(ep.config)
    if bq_cfg.get('query'):
      return pipeline | label >> ReadFromBigQuery(
          query=str(bq_cfg['query']),
          use_standard_sql=True,
          method=ReadFromBigQuery.Method.DIRECT_READ)

    needs_sql_view = False
    sql_query = None
    try:
      from google.cloud import bigquery as gcp_bq
      def_proj = pipeline.options.view_as(GoogleCloudOptions).project
      _, proj, ds, tbl_name = uris.normalize_bq_table_spec(
          str(bq_cfg['table']), default_project=def_proj)
      client = gcp_bq.Client(project=proj)
      eff_proj = proj or client.project
      tbl = client.get_table(f'{eff_proj}.{ds}.{tbl_name}')
      t_type = str(getattr(tbl, 'table_type', 'TABLE') or 'TABLE').upper()
      constraints = getattr(tbl, 'table_constraints', None)
      has_pk = getattr(constraints, 'primary_key', None) is not None
      if t_type != 'TABLE' or has_pk:
        needs_sql_view = True
        sql_query = f'SELECT * FROM `{eff_proj}.{ds}.{tbl_name}`'
        if bq_cfg.get('row_restriction'):
          sql_query += f" WHERE {bq_cfg['row_restriction']}"
    except Exception:  # pylint: disable=broad-except
      needs_sql_view = False

    if needs_sql_view and sql_query:
      return pipeline | label >> ReadFromBigQuery(
          query=sql_query,
          use_standard_sql=True,
          method=ReadFromBigQuery.Method.DIRECT_READ)
    return pipeline | label >> yaml_io.read_from_bigquery(**bq_cfg)

  if ep.kind == 'delta':
    from . import lakehouse
    return pipeline | label >> lakehouse.cli_read_from_delta(**ep.config)

  if ep.kind == 'iceberg':
    from . import lakehouse
    return pipeline | label >> lakehouse.cli_read_from_iceberg(**ep.config)

  if ep.kind == 'file':
    path = ep.config['path']
    if ep.format == 'parquet':
      return pipeline | label >> beam.io.ReadFromParquet(path)
    if ep.format == 'avro':
      return pipeline | label >> beam.io.ReadFromAvro(path)
    if ep.format == 'csv':
      from . import dlq
      return pipeline | label >> dlq.read_csv_with_dlq(
          path, delimiter=ep.config.get('delimiter', ','))
    if ep.format == 'json':
      return (
          pipeline
          | f'{label}_Lines' >> beam.io.ReadFromText(path)
          | f'{label}_Json' >> beam.FlatMap(
              lambda line: [json.loads(line)] if line.strip() else []))

  raise uris.UriError(
      f'beam diff currently supports file paths/globs, delta://, iceberg://, '
      f'and bigquery:// URIs; got {ep.raw!r}')


def run_diff(
    left_uri: str,
    right_uri: str,
    *,
    keys: Iterable[str] | None = None,
    left_format: str | None = None,
    right_format: str | None = None,
    left_opts: Iterable[str] | None = None,
    right_opts: Iterable[str] | None = None,
    rtol: float = 0.0,
    atol: float = 0.0,
    max_bytes: str | None = None,
    pipeline_args: list[str] | None = None,
    default_project: str | None = None,
) -> tuple[int, dict[str, Any]]:
  """Runs distributed reconciliation and returns `(exit_code, report_dict)`."""
  import apache_beam as beam
  from apache_beam.options.pipeline_options import GoogleCloudOptions
  from apache_beam.options.pipeline_options import PipelineOptions
  from apache_beam.options.pipeline_options import StandardOptions

  p_args = list(pipeline_args or [])
  opts = PipelineOptions(p_args)
  proj = default_project or opts.view_as(GoogleCloudOptions).project

  # Use in-process FnApiRunner when no explicit remote runner was requested so
  # local/GCS/BQ diffs run deterministically without Prism port/sandbox issues.
  std_opts = opts.view_as(StandardOptions)
  if not std_opts.runner or std_opts.runner.lower() in (
      'direct', 'directrunner'):
    std_opts.runner = 'FnApiRunner'

  # Ensure FileSystems sees options (`--project`, S3, etc.) before sniffing!
  from apache_beam.io.filesystems import FileSystems
  FileSystems.set_options(opts)

  parsed_left_opts = uris.parse_kv_options(left_opts, None, '--left-opt')
  parsed_right_opts = uris.parse_kv_options(right_opts, None, '--right-opt')

  left_ep = uris.parse_endpoint(
      left_uri,
      is_read=True,
      explicit_format=left_format,
      options=parsed_left_opts,
      default_project=proj,
      allow_sniff=True)
  right_ep = uris.parse_endpoint(
      right_uri,
      is_read=True,
      explicit_format=right_format,
      options=parsed_right_opts,
      default_project=proj,
      allow_sniff=True)

  for ep_chk in (left_ep, right_ep):
    if ep_chk.kind == 'file':
      if not FileSystems.match([ep_chk.config['path']])[0].metadata_list:
        raise uris.UriError(
            f'no files matched {ep_chk.raw!r} (checked {ep_chk.config["path"]!r})')

  if max_bytes is not None:
    limit = cost_guard.parse_byte_size(max_bytes)
    left_est = cost_guard.estimate_endpoint_bytes(
        left_ep, default_project=proj, stop_after_bytes=limit)
    right_est = cost_guard.estimate_endpoint_bytes(
        right_ep, default_project=proj, stop_after_bytes=limit)
    combined = (left_est.get('estimated_bytes') or 0) + (
        right_est.get('estimated_bytes') or 0)
    if combined > limit:
      raise cost_guard.CostLimitError(
          f'combined diff scan is {cost_guard.format_bytes(combined)} '
          f'({combined:,} bytes), exceeding --max-bytes={max_bytes} '
          f'({limit:,} bytes)',
          estimated_bytes=combined,
          limit_bytes=limit)

  key_cols = [k.strip() for k in (keys or ()) if k.strip()]
  sig_digits = (
      max(1, int(-math.log10(rtol))) if (rtol and rtol > 0) else None)

  class _KeySummaryCombineFn(beam.CombineFn):
    """O(1)-memory per-key summary: count, multiset hash, and single-row."""
    def create_accumulator(self):
      return [0, 0, 0, None, None]  # [count, xor_h, sum_h, single_row, key_repr]

    def add_input(self, acc, item):
      row_dict, key_display = item
      digest_int = int.from_bytes(
          _canonical_row_digest(row_dict, sig_digits), 'big')
      new_count = acc[0] + 1
      new_xor = acc[1] ^ digest_int
      new_sum = (acc[2] + digest_int) & ((1 << 256) - 1)
      single = row_dict if new_count == 1 else None
      disp = acc[4] if acc[4] is not None else key_display
      return [new_count, new_xor, new_sum, single, disp]

    def merge_accumulators(self, accumulators):
      total_c = 0
      xor_h = 0
      sum_h = 0
      single = None
      disp = None
      for a in accumulators:
        if a[0] == 0:
          continue
        if total_c == 0 and a[0] == 1:
          single = a[3]
        else:
          single = None
        total_c += a[0]
        xor_h ^= a[1]
        sum_h = (sum_h + a[2]) & ((1 << 256) - 1)
        if disp is None and a[4] is not None:
          disp = a[4]
      if total_c != 1:
        single = None
      return [total_c, xor_h, sum_h, single, disp]

    def extract_output(self, acc):
      return tuple(acc)

  class _GlobalDiffReportCombineFn(beam.CombineFn):
    """Aggregates per-key reconciliation statuses into one bounded summary."""
    def create_accumulator(self):
      return {
          'left_rows': 0,
          'right_rows': 0,
          'matched_rows': 0,
          'only_in_left': 0,
          'only_in_right': 0,
          'value_mismatches': 0,
          'duplicate_key_mismatches': 0,
          'column_mismatch_counts': {},
          'sample_drifts': [],
      }

    def add_input(self, acc, event: dict[str, Any]):
      acc['left_rows'] += event['left_rows']
      acc['right_rows'] += event['right_rows']
      acc['matched_rows'] += event['matched_rows']
      acc['only_in_left'] += event['only_in_left']
      acc['only_in_right'] += event['only_in_right']
      acc['value_mismatches'] += event['value_mismatches']
      acc['duplicate_key_mismatches'] += event['duplicate_key_mismatches']
      for col, cnt in event.get('col_mismatches', {}).items():
        acc['column_mismatch_counts'][col] = (
            acc['column_mismatch_counts'].get(col, 0) + cnt)
      if event.get('sample') is not None and len(acc['sample_drifts']) < 10:
        acc['sample_drifts'].append(event['sample'])
      return acc

    def merge_accumulators(self, accumulators):
      merged = self.create_accumulator()
      for a in accumulators:
        merged['left_rows'] += a['left_rows']
        merged['right_rows'] += a['right_rows']
        merged['matched_rows'] += a['matched_rows']
        merged['only_in_left'] += a['only_in_left']
        merged['only_in_right'] += a['only_in_right']
        merged['value_mismatches'] += a['value_mismatches']
        merged['duplicate_key_mismatches'] += a['duplicate_key_mismatches']
        for col, cnt in a['column_mismatch_counts'].items():
          merged['column_mismatch_counts'][col] = (
              merged['column_mismatch_counts'].get(col, 0) + cnt)
        for s in a['sample_drifts']:
          if len(merged['sample_drifts']) < 10:
            merged['sample_drifts'].append(s)
      return merged

    def extract_output(self, acc):
      return acc

  def _key_row(row):
    d = _row_to_plain_dict(row)
    if key_cols:
      missing = [k for k in key_cols if k not in d]
      if missing:
        raise ValueError(
            f'key column(s) {missing} not found in row columns {sorted(d.keys())}')
      k_tuple = tuple(_encode_key_atom(d[k]) for k in key_cols)
      k_disp = {k: d[k] for k in key_cols}
    else:
      digest = _canonical_row_digest(d, sig_digits)
      k_tuple = ((4, digest), )
      k_disp = d
    return (k_tuple, (d, k_disp))

  def _compare_grouped(item):
    _, grouped = item
    l_list = grouped['left']
    r_list = grouped['right']
    l_sum = l_list[0] if l_list else (0, 0, 0, None, None)
    r_sum = r_list[0] if r_list else (0, 0, 0, None, None)
    l_cnt, l_xor, l_add, l_row, l_disp = l_sum
    r_cnt, r_xor, r_add, r_row, r_disp = r_sum

    if l_cnt == 0:
      return {
          'left_rows': 0,
          'right_rows': r_cnt,
          'matched_rows': 0,
          'only_in_left': 0,
          'only_in_right': r_cnt,
          'value_mismatches': 0,
          'duplicate_key_mismatches': 0,
          'col_mismatches': {},
          'sample': {
              'kind': 'only_in_right', 'key': r_disp, 'count': r_cnt
          },
      }
    if r_cnt == 0:
      return {
          'left_rows': l_cnt,
          'right_rows': 0,
          'matched_rows': 0,
          'only_in_left': l_cnt,
          'only_in_right': 0,
          'value_mismatches': 0,
          'duplicate_key_mismatches': 0,
          'col_mismatches': {},
          'sample': {
              'kind': 'only_in_left', 'key': l_disp, 'count': l_cnt
          },
      }

    if not key_cols:
      common = min(l_cnt, r_cnt)
      extra_l = max(0, l_cnt - r_cnt)
      extra_r = max(0, r_cnt - l_cnt)
      sample = None
      if extra_l:
        sample = {'kind': 'only_in_left', 'row': l_disp, 'extra_count': extra_l}
      elif extra_r:
        sample = {
            'kind': 'only_in_right', 'row': r_disp, 'extra_count': extra_r
        }
      return {
          'left_rows': l_cnt,
          'right_rows': r_cnt,
          'matched_rows': common,
          'only_in_left': extra_l,
          'only_in_right': extra_r,
          'value_mismatches': 0,
          'duplicate_key_mismatches': 0,
          'col_mismatches': {},
          'sample': sample,
      }

    if l_cnt != r_cnt:
      return {
          'left_rows': l_cnt,
          'right_rows': r_cnt,
          'matched_rows': 0,
          'only_in_left': 0,
          'only_in_right': 0,
          'value_mismatches': 0,
          'duplicate_key_mismatches': 1,
          'col_mismatches': {},
          'sample': {
              'kind': 'duplicate_key_count_mismatch',
              'key': l_disp,
              'left_count': l_cnt,
              'right_count': r_cnt,
          },
      }

    if l_cnt == 1 and r_cnt == 1 and l_row is not None and r_row is not None:
      all_cols = sorted(set(l_row.keys()) | set(r_row.keys()))
      bad_cols: dict[str, int] = {}
      diff_fields: dict[str, dict[str, Any]] = {}
      for c in all_cols:
        if c in key_cols:
          continue
        lv = l_row.get(c)
        rv = r_row.get(c)
        if not _values_equal(lv, rv, rtol=rtol, atol=atol):
          bad_cols[c] = 1
          diff_fields[c] = {'left': lv, 'right': rv}
      if not bad_cols:
        return {
            'left_rows': 1,
            'right_rows': 1,
            'matched_rows': 1,
            'only_in_left': 0,
            'only_in_right': 0,
            'value_mismatches': 0,
            'duplicate_key_mismatches': 0,
            'col_mismatches': {},
            'sample': None,
        }
      return {
          'left_rows': 1,
          'right_rows': 1,
          'matched_rows': 0,
          'only_in_left': 0,
          'only_in_right': 0,
          'value_mismatches': 1,
          'duplicate_key_mismatches': 0,
          'col_mismatches': bad_cols,
          'sample': {
              'kind': 'value_mismatch',
              'key': l_disp,
              'differing_columns': diff_fields,
          },
      }

    # Multi-row duplicate key (`l_cnt == r_cnt > 1`): compare O(1) multiset hash!
    if l_xor == r_xor and l_add == r_add:
      return {
          'left_rows': l_cnt,
          'right_rows': r_cnt,
          'matched_rows': l_cnt,
          'only_in_left': 0,
          'only_in_right': 0,
          'value_mismatches': 0,
          'duplicate_key_mismatches': 0,
          'col_mismatches': {},
          'sample': None,
      }
    return {
        'left_rows': l_cnt,
        'right_rows': r_cnt,
        'matched_rows': 0,
        'only_in_left': 0,
        'only_in_right': 0,
        'value_mismatches': l_cnt,
        'duplicate_key_mismatches': 0,
        'col_mismatches': {},
        'sample': {
            'kind': 'duplicate_key_multiset_mismatch',
            'key': l_disp,
            'count': l_cnt,
        },
    }

  import os
  import shutil
  import tempfile
  import uuid

  remote_runner = (std_opts.runner or '').lower() not in (
      'fnapirunner', 'directrunner', 'direct', 'prismrunner', 'prism',
      'bundlebaseddirectrunner')
  temp_loc = opts.view_as(GoogleCloudOptions).temp_location
  if remote_runner and not temp_loc:
    raise uris.UriError(
        f'beam diff on remote runner {std_opts.runner!r} requires '
        '--temp_location=gs://<bucket>/tmp so workers can write the summary')
  tmp_dir: str | None = None
  if remote_runner and temp_loc:
    out_file_prefix = (
        temp_loc.rstrip('/') + f'/beam-diff-{uuid.uuid4().hex}/summary')
  else:
    tmp_dir = tempfile.mkdtemp(prefix='beam-diff-')
    out_file_prefix = os.path.join(tmp_dir, 'summary')
  job_id: str | None = None
  final_state = 'DONE'
  try:
    pipeline = beam.Pipeline(options=opts)
    left_pcoll = _read_endpoint_pcoll(pipeline, 'ReadLeft', left_ep)
    right_pcoll = _read_endpoint_pcoll(pipeline, 'ReadRight', right_ep)

    left_sum = (
        left_pcoll
        | 'KeyLeft' >> beam.Map(_key_row)
        | 'CombineLeft' >> beam.CombinePerKey(_KeySummaryCombineFn()))
    right_sum = (
        right_pcoll
        | 'KeyRight' >> beam.Map(_key_row)
        | 'CombineRight' >> beam.CombinePerKey(_KeySummaryCombineFn()))

    _ = (
        {
            'left': left_sum, 'right': right_sum
        }
        | 'CoGroup' >> beam.CoGroupByKey()
        | 'CompareKeys' >> beam.Map(_compare_grouped)
        | 'SummarizeDiff' >> beam.CombineGlobally(
            _GlobalDiffReportCombineFn()).without_defaults()
        | 'SerializeSummary' >> beam.Map(
            lambda d: json.dumps(
                output.sanitize_untrusted_value(d), allow_nan=False))
        | 'WriteSummary' >> beam.io.WriteToText(
            out_file_prefix, num_shards=1, shard_name_template=''))
    res = pipeline.run()
    wait_st = res.wait_until_finish()
    if wait_st:
      final_state = str(wait_st)
    for attr in ('job_id', '_job_id'):
      val = getattr(res, attr, None)
      if callable(val):
        try:
          val = val()
        except Exception:  # pylint: disable=broad-except
          val = None
      if isinstance(val, str) and val.strip():
        job_id = val.strip()
        break

    summary: dict[str, Any] = {
        'left_rows': 0,
        'right_rows': 0,
        'matched_rows': 0,
        'only_in_left': 0,
        'only_in_right': 0,
        'value_mismatches': 0,
        'duplicate_key_mismatches': 0,
        'column_mismatch_counts': {},
        'sample_drifts': [],
    }
    if tmp_dir is not None:
      if os.path.isfile(out_file_prefix):
        with open(out_file_prefix, encoding='utf-8') as f:
          text = f.read().strip()
          if text:
            summary = json.loads(text.splitlines()[0])
    else:
      matches = FileSystems.match([out_file_prefix + '*'])[0].metadata_list
      if matches:
        with FileSystems.open(matches[0].path) as f:
          text = f.read().decode('utf-8').strip()
          if text:
            summary = json.loads(text.splitlines()[0])
  finally:
    if tmp_dir is not None:
      shutil.rmtree(tmp_dir, ignore_errors=True)

  samples = summary.pop('sample_drifts', [])
  identical = (
      summary['only_in_left'] == 0 and summary['only_in_right'] == 0 and
      summary['value_mismatches'] == 0 and
      summary['duplicate_key_mismatches'] == 0 and
      summary['left_rows'] == summary['right_rows'])
  exit_code = EXIT_OK if identical else EXIT_DIFF_MISMATCH
  report = {
      'match': identical,
      'left': left_uri,
      'right': right_uri,
      'keys': key_cols,
      'rtol': rtol,
      'atol': atol,
      'summary': summary,
      'job_id': job_id,
      'state': final_state,
      'untrusted_data': output.wrap_untrusted_rows(
          samples, truncated=len(samples) >= 10) if samples else None,
  }
  return exit_code, report
