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
import collections
import csv
import decimal
import io
import json
import math
import os
import re
import threading
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
    'ReadJsonWithDlq',
    'ReadFromJsonWithDlq',
    'ReadFromPubSub',
    'WriteToPubSub',
    'ReadFromKafka',
    'WriteToKafka',
    'WriteToBigQuery',
    'CliWriteToBigQuery',
    'WriteToBigQueryCDC',
    'CliWriteToBigQueryCDC',
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


TRANSFORM_DLQ_FIELDS = (
    ('step', str),
    ('error_type', str),
    ('error', str),
    ('raw_payload', str),
    ('element', str),
)


def transform_dlq_row_type():
  """Returns the standard 5-column `RowTypeConstraint` for transform DLQ outputs."""
  from apache_beam.typehints.row_type import RowTypeConstraint
  return RowTypeConstraint.from_fields(list(TRANSFORM_DLQ_FIELDS))


_RAW_EMAIL_PII_RE = (
    r'(?<![A-Za-z0-9._%+-])[A-Za-z0-9._%+-]{1,64}@[A-Za-z0-9.-]{1,255}\.[A-Za-z]{2,24}'
)
_RAW_PAN_PII_RE = r'(?<!\d)(?:\d[ -]?){12,18}\d(?!\d)'
_RAW_SSN_PII_RE = r'\b\d{3}-\d{2}-\d{4}\b'
_UNPARSEABLE_VAL_PAT = (
    r'(?:"(?:\\.|[^"\\])*(?:"|$)|\'(?:\\.|[^\'\\])*(?:\'|$)|[^,\s)\]}]+)')
_INLINE_KV_SECRET_PAT = (
    r'(\b(?:password|passwd|pwd|passphrase|secret|token|api[-_]?key|'
    r'x-api-key|access_key|private_key|signing_key|encryption_key|'
    r'client_secret|auth|aws_secret_access_key|sasl\.jaas\.config)\b\s*[:=]\s*)'
    r'(?![\'"]?<(?:redacted|REDACTED))'
    r'(?:"(?:\\.|[^"\\])*(?:"|$)|\'(?:\\.|[^\'\\])*(?:\'|$)|[^\s,\'";)\]}]+)')


def scrub_unparseable_dlq_text(
    raw_text: str,
    redact_fields: Iterable[str] = (),
) -> str:
  """Scrubs PII from unparseable/corrupt CSV or JSON strings when `redact_fields` is set."""
  from . import output as out_mod
  if not raw_text:
    return raw_text
  out_s = out_mod.redact_inline_secrets(
      str(raw_text), placeholder='<REDACTED_SECRET>')
  rk = tuple(str(f) for f in (redact_fields or ()) if str(f).strip())
  if not rk:
    return out_s
  for col in rk:
    esc_col = re.escape(col)
    out_s = re.sub(
        r'([\'"]?' + esc_col + r'[\'"]?\s*[:=]\s*)' + _UNPARSEABLE_VAL_PAT,
        r'\1"<REDACTED>"',
        out_s)
    out_s = re.sub(
        r'(\bcolumn\s+[\'"]?' + esc_col + r'[\'"]?\s*:\s*).*',
        r'\1<REDACTED>',
        out_s)
  if '@' in out_s:
    out_s = re.sub(_RAW_EMAIL_PII_RE, '<REDACTED>', out_s)
  out_s = re.sub(_RAW_SSN_PII_RE, '<REDACTED>', out_s)
  out_s = re.sub(_RAW_PAN_PII_RE, '<REDACTED>', out_s)
  return out_s


def redact_nested_fields(
    value: Any,
    redact_fields: Iterable[str] = (),
    *,
    max_depth: int = 8,
) -> Any:
  """Recursively replaces keys in `redact_fields` with `'<REDACTED>'` across dicts/lists."""
  rk = frozenset(str(f) for f in (redact_fields or ()) if str(f).strip())
  if not rk or max_depth <= 0:
    return value
  if hasattr(value, '_asdict'):
    value = dict(value._asdict())
  if isinstance(value, dict):
    out_dict: dict[Any, Any] = {}
    for k, v in value.items():
      ks = str(k)
      if ks in rk:
        out_dict[k] = '<REDACTED>'
      elif ks == 'cells' and isinstance(v, (list, tuple)):
        out_dict[k] = ['<REDACTED>' for _ in v]
      elif ks == 'raw_line' and isinstance(v, str):
        if 'file' in value:
          out_dict[k] = '<REDACTED>'
        else:
          try:
            parsed_rl = json.loads(v)
            if isinstance(parsed_rl, (dict, list)):
              dumped_rl = json.dumps(
                  redact_nested_fields(parsed_rl, rk, max_depth=max_depth - 1),
                  allow_nan=False,
                  sort_keys=True)
              out_dict[k] = scrub_unparseable_dlq_text(dumped_rl, rk)
            elif isinstance(parsed_rl, str):
              out_dict[k] = json.dumps(
                  scrub_unparseable_dlq_text(parsed_rl, rk), allow_nan=False)
            else:
              out_dict[k] = scrub_unparseable_dlq_text(v, rk)
          except Exception:  # pylint: disable=broad-except
            out_dict[k] = scrub_unparseable_dlq_text(v, rk)
      else:
        out_dict[k] = redact_nested_fields(v, rk, max_depth=max_depth - 1)
    return out_dict
  if isinstance(value, (list, tuple)):
    return [
        redact_nested_fields(item, rk, max_depth=max_depth - 1)
        for item in value
    ]
  if isinstance(value, str):
    return scrub_unparseable_dlq_text(value, rk)
  return value


def make_transform_dlq_row(
    step: str,
    error_type: str,
    error_msg: str,
    record: Any,
    *,
    redact_fields: Iterable[str] = (),
):
  """Constructs a sanitized, PII-scrubbed 5-column `beam.Row` for transform DLQs."""
  import apache_beam as beam
  from . import output as out_mod
  rk = tuple(str(f) for f in (redact_fields or ()) if str(f).strip())
  clean_record = redact_nested_fields(record, rk) if rk else record
  raw_json = safe_json_dumps(clean_record)
  clean_err = out_mod.sanitize_untrusted_text(str(error_msg), max_chars=4096)
  clean_err = out_mod.redact_inline_secrets(
      clean_err, placeholder='<REDACTED_SECRET>')
  if rk:
    clean_err = scrub_unparseable_dlq_text(clean_err, rk)
  for col in rk:
    esc_col = re.escape(col)
    clean_err = re.sub(
        r'([\'"]?' + esc_col + r'[\'"]?\s*[:=]\s*)' + _UNPARSEABLE_VAL_PAT,
        r'\1<REDACTED>',
        clean_err)
    clean_err = re.sub(
        r'(\bcolumn\s+[\'"]?' + esc_col + r'[\'"]?\s*:\s*).*',
        r'\1<REDACTED>',
        clean_err)
  return beam.Row(
      step=str(step),
      error_type=str(error_type),
      error=clean_err[:4096],
      raw_payload=raw_json,
      element=raw_json,
  )


def build_dlq_normalizer_transform(
    name: str,
    input_ref: str,
    step_label: str,
    *,
    redact_fields: Iterable[str] = (),
) -> dict[str, Any]:
  """Builds a Beam YAML `MapToFields` stage normalizing error rows for Flatten."""
  from . import output as out_mod
  step_lit = repr(step_label)
  redact_tuple = tuple(str(f) for f in (redact_fields or ()) if str(f).strip())
  ansi_pat = repr(out_mod._ANSI_RE.pattern)
  ctrl_pat = repr(out_mod._CONTROL_AND_INJECTION_RE.pattern)
  uri_sec_pat = repr(out_mod._URI_USERINFO_SECRET_RE.pattern)
  auth_sec_pat = repr(out_mod._AUTH_HEADER_SECRET_RE.pattern)
  kv_sec_pat = repr(_INLINE_KV_SECRET_PAT)
  tok_sec_pat = repr(out_mod._KNOWN_TOKEN_RE.pattern)
  val_pat_lit = repr(_UNPARSEABLE_VAL_PAT)
  inline_secret_snip = (
      f'  _m = re.sub({ansi_pat}, "", _m)\n'
      f'  _m = re.sub({ctrl_pat}, "", _m)\n'
      f'  _m = re.sub({uri_sec_pat}, r"\\1<REDACTED_SECRET>\\2", _m)\n'
      f'  _m = re.sub({auth_sec_pat}, r"\\1 <REDACTED_SECRET>", _m, flags=re.I)\n'
      f'  _m = re.sub({kv_sec_pat}, r"\\1<REDACTED_SECRET>", _m, flags=re.I)\n'
      f'  _m = re.sub({tok_sec_pat}, "<REDACTED_SECRET>", _m)\n')
  err_type_src = (
      'def normalize_dlq(err_row):\n'
      "  t = getattr(err_row, 'error_type', None)\n"
      '  if t:\n'
      '    return str(t)\n'
      "  msg = str(getattr(err_row, 'msg', None) or "
      "getattr(err_row, 'error_message', None) or "
      "getattr(err_row, 'error', None) or 'TransformError')\n"
      "  first = msg.splitlines()[0] if msg else ''\n"
      "  head = first.split('(', 1)[0].split(':', 1)[0].strip()\n"
      "  return head if head.isidentifier() else 'TransformError'\n")
  if redact_tuple:
    redact_lit = repr(redact_tuple)
    err_msg_src = (
        'def normalize_dlq(err_row):\n'
        '  import re\n'
        f'  _rk = {redact_lit}\n'
        "  _m = str(getattr(err_row, 'error', None) or "
        "getattr(err_row, 'msg', None) or "
        "getattr(err_row, 'error_message', None) or str(err_row))[:4096]\n" +
        inline_secret_snip + '  for _col in _rk:\n'
        '    _ec = re.escape(_col)\n'
        f'    _m = re.sub(r\'([\\\'"]?\' + _ec + r\'[\\\'"]?\\s*[:=]\\s*)\' + {val_pat_lit}, r\'\\1<REDACTED>\', _m)\n'
        '    _m = re.sub(r\'(\\bcolumn\\s+[\\\'"]?\' + _ec + r\'[\\\'"]?\\s*:\\s*).*\', r\'\\1<REDACTED>\', _m)\n'
        '  if "@" in _m:\n'
        f'    _m = re.sub({_RAW_EMAIL_PII_RE!r}, "<REDACTED>", _m)\n'
        f'  _m = re.sub({_RAW_SSN_PII_RE!r}, "<REDACTED>", _m)\n'
        f'  _m = re.sub({_RAW_PAN_PII_RE!r}, "<REDACTED>", _m)\n'
        '  return _m[:4096]\n')
    redact_tail = (
        '  import re\n'
        f'  _rk = {redact_lit}\n'
        '  def _scrub_str(s):\n'
        f'    s = re.sub({uri_sec_pat}, r"\\1<REDACTED_SECRET>\\2", s)\n'
        f'    s = re.sub({auth_sec_pat}, r"\\1 <REDACTED_SECRET>", s, flags=re.I)\n'
        f'    s = re.sub({kv_sec_pat}, r"\\1<REDACTED_SECRET>", s, flags=re.I)\n'
        f'    s = re.sub({tok_sec_pat}, "<REDACTED_SECRET>", s)\n'
        '    for _col in _rk:\n'
        '      _ec = re.escape(_col)\n'
        f'      s = re.sub(r\'([\\\'"]?\' + _ec + r\'[\\\'"]?\\s*[:=]\\s*)\' + {val_pat_lit}, r\'\\1"<REDACTED>"\', s)\n'
        '      s = re.sub(r\'(\\bcolumn\\s+[\\\'"]?\' + _ec + r\'[\\\'"]?\\s*:\\s*).*\', r\'\\1<REDACTED>\', s)\n'
        '    if "@" in s:\n'
        f'      s = re.sub({_RAW_EMAIL_PII_RE!r}, "<REDACTED>", s)\n'
        f'    s = re.sub({_RAW_SSN_PII_RE!r}, "<REDACTED>", s)\n'
        f'    s = re.sub({_RAW_PAN_PII_RE!r}, "<REDACTED>", s)\n'
        '    return s\n'
        '  def _scrub(v, d=8):\n'
        '    if d <= 0:\n'
        '      return v\n'
        '    if isinstance(v, dict):\n'
        '      _res = {}\n'
        '      for k, val in v.items():\n'
        '        _ks = str(k)\n'
        '        if _ks in _rk:\n'
        "          _res[k] = '<REDACTED>'\n"
        "        elif _ks == 'cells' and isinstance(val, list):\n"
        "          _res[k] = ['<REDACTED>' for _ in val]\n"
        "        elif _ks == 'raw_line' and isinstance(val, str):\n"
        "          if 'file' in v:\n"
        "            _res[k] = '<REDACTED>'\n"
        '          else:\n'
        '            try:\n'
        '              _rl = json.loads(val)\n'
        '              if isinstance(_rl, (dict, list)):\n'
        '                _res[k] = _scrub_str(json.dumps(_scrub(_rl, d - 1), allow_nan=False, sort_keys=True))\n'
        '              elif isinstance(_rl, str):\n'
        '                _res[k] = json.dumps(_scrub_str(_rl), allow_nan=False)\n'
        '              else:\n'
        '                _res[k] = _scrub_str(val)\n'
        '            except Exception:\n'
        '              _res[k] = _scrub_str(val)\n'
        '        else:\n'
        '          _res[k] = _scrub(val, d - 1)\n'
        '      return _res\n'
        '    if isinstance(v, list):\n'
        '      return [_scrub(x, d - 1) for x in v]\n'
        '    if isinstance(v, str):\n'
        '      return _scrub_str(v)\n'
        '    return v\n'
        '  try:\n'
        '    _obj = json.loads(_out)\n'
        '    if isinstance(_obj, (dict, list)):\n'
        '      return json.dumps(_scrub(_obj), allow_nan=False, sort_keys=True)\n'
        '    if isinstance(_obj, str):\n'
        '      return json.dumps(_scrub_str(_obj), allow_nan=False)\n'
        '  except Exception:\n'
        '    pass\n'
        '  return _scrub_str(str(_out))\n')
    payload_src = (
        'def normalize_dlq(err_row):\n' + _SAFE_JSON_HELPERS_SRC +
        "  raw = getattr(err_row, 'raw_payload', None)\n"
        '  if isinstance(raw, str):\n'
        '    _out = raw\n'
        "  elif hasattr(err_row, 'element'):\n"
        "    _out = _safe_json_str(getattr(err_row, 'element'))\n"
        "  elif hasattr(err_row, 'failed_row'):\n"
        "    _out = _safe_json_str(getattr(err_row, 'failed_row'))\n"
        "  elif hasattr(err_row, 'record'):\n"
        "    _out = _safe_json_str(getattr(err_row, 'record'))\n"
        '  else:\n'
        '    _out = _safe_json_str(err_row)\n' + redact_tail)
  else:
    err_msg_src = (
        'def normalize_dlq(err_row):\n'
        '  import re\n'
        "  _m = str(getattr(err_row, 'error', None) or "
        "getattr(err_row, 'msg', None) or "
        "getattr(err_row, 'error_message', None) or str(err_row))[:4096]\n" +
        inline_secret_snip + '  return _m[:4096]\n')
    payload_src = (
        'def normalize_dlq(err_row):\n' + _SAFE_JSON_HELPERS_SRC +
        "  raw = getattr(err_row, 'raw_payload', None)\n"
        '  if isinstance(raw, str):\n'
        '    return raw\n'
        "  if hasattr(err_row, 'element'):\n"
        "    return _safe_json_str(getattr(err_row, 'element'))\n"
        "  if hasattr(err_row, 'failed_row'):\n"
        "    return _safe_json_str(getattr(err_row, 'failed_row'))\n"
        "  if hasattr(err_row, 'record'):\n"
        "    return _safe_json_str(getattr(err_row, 'record'))\n"
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
          'CliDropRows': 'apache_beam.cli.dlq.cli_drop_rows',
          'CliWriteToBigQuery': 'apache_beam.cli.dlq.cli_write_to_bigquery',
          'ReadFromDelta': 'apache_beam.cli.lakehouse.cli_read_from_delta',
          'ReadFromDeltaPy': 'apache_beam.cli.lakehouse.cli_read_from_delta',
          'ReadFromDeltaCDC': (
              'apache_beam.cli.lakehouse.cli_read_from_delta_cdc'),
          'ReadFromDeltaCdcPy': (
              'apache_beam.cli.lakehouse.cli_read_from_delta_cdc'),
          'ReadFromIcebergPy': (
              'apache_beam.cli.lakehouse.cli_read_from_iceberg'),
          'ReadFromIcebergCdcPy': (
              'apache_beam.cli.lakehouse.cli_read_from_iceberg_cdc'),
          'CliWriteToIceberg': 'apache_beam.cli.lakehouse.cli_write_to_iceberg',
          'CliNormalizeCdc': 'apache_beam.cli.cdc.cli_normalize_cdc',
          'CliMaterializeCdc': 'apache_beam.cli.cdc.cli_materialize_cdc',
          'CliRowifyNested': 'apache_beam.cli.dlq.cli_rowify_nested',
          'CliExpect': 'apache_beam.cli.expect.cli_expect',
          'WriteToBigQueryCDC': (
              'apache_beam.cli.cdc.cli_write_to_bigquery_cdc'),
          'CliWriteToBigQueryCDC': (
              'apache_beam.cli.cdc.cli_write_to_bigquery_cdc'),
          'CliIntervalJoin': (
              'apache_beam.cli.interval_join.cli_interval_join'),
          'IntervalJoin': ('apache_beam.cli.interval_join.cli_interval_join'),
          'CliAssignTimestamps': (
              'apache_beam.cli.interval_join.cli_assign_timestamps'),
          'CliDeduplicate': 'apache_beam.cli.dedup.cli_deduplicate',
          'Deduplicate': 'apache_beam.cli.dedup.cli_deduplicate',
          'CliEnrich': 'apache_beam.cli.enrich.cli_enrich',
          'Enrich': 'apache_beam.cli.enrich.cli_enrich',
          'CliInfer': 'apache_beam.cli.infer.cli_infer',
          'Infer': 'apache_beam.cli.infer.cli_infer',
          'CliWindowInto': 'apache_beam.cli.windowing.cli_window_into',
          'CliExtractPaneInfo': (
              'apache_beam.cli.windowing.cli_extract_pane_info'),
          'CliAsofJoin': 'apache_beam.cli.asof_join.cli_asof_join',
          'AsofJoin': 'apache_beam.cli.asof_join.cli_asof_join',
          'CliPatternMatch': 'apache_beam.cli.cep.cli_pattern_match',
          'PatternMatch': 'apache_beam.cli.cep.cli_pattern_match',
          'CliLimitRows': 'apache_beam.cli.dlq.cli_limit_rows',
      },
  }


DEFAULT_CSV_FIELD_SIZE_LIMIT = 64 * 1024 * 1024


def ensure_csv_field_size_limit(
    limit_bytes: int = DEFAULT_CSV_FIELD_SIZE_LIMIT,
) -> int:
  """Raises Python's `csv.field_size_limit` up to `limit_bytes` safely."""
  current = csv.field_size_limit()
  target = int(limit_bytes)
  if current >= target:
    return current
  while target > current:
    try:
      csv.field_size_limit(target)
      return csv.field_size_limit()
    except OverflowError:
      target //= 2
  return csv.field_size_limit()


ensure_csv_field_size_limit()


def iter_csv_rows_recovering(
    text_stream,
    delimiter: str = ',',
    *,
    max_record_bytes: int = DEFAULT_CSV_FIELD_SIZE_LIMIT,
):
  """Yields `(cells, csv_err, raw_line)` without truncating remaining rows.

  Preserves RFC-4180 multiline quoted fields while replaying buffered physical
  lines if a malformed row (unclosed quote at EOF, field size overflow, or
  invalid quote syntax) raises `csv.Error`.
  """
  ensure_csv_field_size_limit(max_record_bytes)
  replay: collections.deque[str] = collections.deque()
  attempt: list[str] = []

  def _make_line_iter():
    acc_bytes = 0
    while True:
      if replay:
        ln = replay.popleft()
      else:
        ln = text_stream.readline()
        if ln == '':
          return
      attempt.append(ln)
      acc_bytes += len(ln)
      if len(attempt) > 1 and acc_bytes > max_record_bytes:
        raise csv.Error(
            f'multiline CSV record exceeds {max_record_bytes} bytes '
            '(likely unclosed quote)')
      yield ln

  reader = csv.reader(_make_line_iter(), delimiter=delimiter, strict=True)
  while True:
    attempt.clear()
    try:
      cells = next(reader)
      yield cells, None, ''.join(attempt)
    except StopIteration:
      break
    except csv.Error as exc:
      bad_line = attempt[0] if attempt else ''
      for extra_ln in reversed(attempt[1:]):
        replay.appendleft(extra_ln)
      reader = csv.reader(_make_line_iter(), delimiter=delimiter, strict=True)
      yield None, exc, bad_line


# Compressions Beam's CompressionTypes.AUTO handles that we open natively;
# anything else on a local path still goes through FileSystems.
_LOCAL_COMPRESSION_SUFFIXES = ('.gz', '.bz2')
_OTHER_COMPRESSION_SUFFIXES = (
    '.zst', '.zstd', '.deflate', '.lzma', '.xz', '.snappy')


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


MAX_SCHEMA_DECOMPRESSED_BYTES = 64 * 1024 * 1024


class _BoundedDecompressedRawReader(io.RawIOBase):
  """Caps decompressed bytes read from `.gz`/`.bz2` streams against zip bombs."""
  def __init__(self, inner, max_bytes: int, path: str):
    self._inner = inner
    self._max_bytes = int(max_bytes)
    self._path = path
    self._read_bytes = 0

  def readable(self) -> bool:
    return True

  def readinto(self, b) -> int:
    chunk = self._inner.read(len(b))
    if not chunk:
      return 0
    n = len(chunk)
    self._read_bytes += n
    if self._read_bytes > self._max_bytes:
      raise ValueError(
          f'decompressed stream for {self._path!r} exceeds maximum scan size '
          f'({self._max_bytes:,} bytes)')
    b[:n] = chunk
    return n

  def close(self) -> None:
    try:
      self._inner.close()
    finally:
      super().close()


def _open_auto(
    path: str,
    *,
    max_decompressed_bytes: Optional[int] = MAX_SCHEMA_DECOMPRESSED_BYTES,
    streaming_rows: bool = False,
):
  """Opens `path` for binary reading, decompressing like Beam's AUTO mode."""
  low = path.lower()
  is_compressed = low.endswith(
      _LOCAL_COMPRESSION_SUFFIXES + _OTHER_COMPRESSION_SUFFIXES)
  eff_cap = None if streaming_rows else max_decompressed_bytes
  if '://' not in path and not low.endswith(_OTHER_COMPRESSION_SUFFIXES):
    if low.endswith('.gz'):
      import gzip
      raw_s: Any = gzip.open(path, 'rb')
    elif low.endswith('.bz2'):
      import bz2
      raw_s = bz2.open(path, 'rb')
    else:
      return open(path, 'rb')
    if eff_cap is not None:
      return io.BufferedReader(
          _BoundedDecompressedRawReader(raw_s, eff_cap, path))
    return raw_s
  from apache_beam.io.filesystem import CompressionTypes
  from apache_beam.io.filesystems import FileSystems
  raw_fs = FileSystems.open(path, compression_type=CompressionTypes.AUTO)
  if is_compressed and eff_cap is not None:
    return io.BufferedReader(
        _BoundedDecompressedRawReader(raw_fs, eff_cap, path))
  return raw_fs


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
  if n_int == n or (allow_dirty and n >= 3 and n_int / n >= 0.7 and
                    n_float == n_int):
    return 'int'
  if n_float == n or (allow_dirty and n >= 3 and n_float / n >= 0.7):
    return 'float'
  return 'str'


def _parse_cell(
    raw: str, kind: str, null_values=DEFAULT_CSV_NULL_VALUES) -> Any:
  if kind == 'str':
    if raw == '' or raw in null_values or (raw.strip() and
                                           raw.strip() in null_values):
      return None
    return raw
  if _is_null_cell(raw, null_values):
    return None
  stripped = raw.strip()
  if kind in ('int', 'float') and _has_leading_zero(stripped):
    # The column was typed from a sample without leading zeros; casting would
    # silently turn "00123" into 123. Fail this value instead (DLQ or error).
    raise ValueError(
        f'cell value (len={len(stripped)}) has a leading zero but the column '
        f'was typed {kind} from the first rows; keep it as text with '
        '--from-opt fields=COLUMN:str (or route such rows with --dlq)')
  if kind == 'int':
    try:
      return int(stripped)
    except ValueError:
      raise ValueError(
          f'expected integer value, got non-integer text (len={len(stripped)})'
      ) from None
  if kind == 'float':
    try:
      return float(stripped)
    except ValueError:
      raise ValueError(
          f'expected float value, got non-numeric text (len={len(stripped)})'
      ) from None
  if kind == 'bool':
    low = stripped.lower()
    if low == 'true':
      return True
    if low == 'false':
      return False
    raise ValueError(
        f'expected boolean value, got invalid token (len={len(stripped)})')
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
  ensure_csv_field_size_limit()
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
      try:
        rec_iter = iter_csv_rows_recovering(text_stream, delimiter=delimiter)
        first_row = None
        for cells, csv_err, _ in rec_iter:
          if csv_err is not None:
            continue
          if cells:
            first_row = cells
            break
        if not first_row:
          continue
        norm = [col.strip() or f'col_{i}' for i, col in enumerate(first_row)]
        order = None
        if header is None:
          header = norm
          col_samples = {c: [] for c in header}
        else:
          order = header_order(norm, header)
        for row, csv_err, _ in rec_iter:
          if collected >= sample_rows:
            break
          if csv_err is not None or row is None:
            continue
          if len(row) != len(header):
            continue
          if order is not None:
            row = [row[i] for i in order]
          for col_name, cell in zip(header, row):
            col_samples[col_name].append(cell)
          collected += 1
      finally:
        try:
          text_stream.detach()
        except Exception:  # pylint: disable=broad-except
          pass
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
      for c in _columns_with_leading_zeros(matches, header, numeric, delimiter):
        kinds[c] = 'str'
  return header, kinds


def _columns_with_leading_zeros(files, header, columns, delimiter):
  """Columns (of `columns`) with a leading-zero value in any row of `files`."""
  ensure_csv_field_size_limit()
  found: set[str] = set()
  for fp in files:
    with _open_auto(fp) as raw_stream:
      text_stream = io.TextIOWrapper(
          raw_stream, encoding='utf-8-sig', errors='replace', newline='')
      try:
        rec_iter = iter_csv_rows_recovering(text_stream, delimiter=delimiter)
        first = None
        for cells, csv_err, _ in rec_iter:
          if csv_err is not None:
            continue
          if cells:
            first = cells
            break
        if not first:
          continue
        norm = [col.strip() or f'col_{i}' for i, col in enumerate(first)]
        positions = {c: norm.index(c) for c in columns if c in norm}
        for row, csv_err, _ in rec_iter:
          if csv_err is not None or row is None:
            continue
          for c, i in positions.items():
            if c not in found and i < len(row) and _has_leading_zero(
                row[i].strip()):
              found.add(c)
          if len(found) == len(positions):
            break
      finally:
        try:
          text_stream.detach()
        except Exception:  # pylint: disable=broad-except
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


_ROW_HOOKS_LOCK = threading.RLock()


def install_keyword_safe_row_hooks() -> None:
  """Idempotently enables keyword/special column names in Beam Row schemas."""
  import keyword
  import operator
  from apache_beam.typehints import schemas as schemas_mod
  from apache_beam.yaml import yaml_mapping as yaml_map_mod

  with _ROW_HOOKS_LOCK:
    st_cls = getattr(schemas_mod, 'SchemaTranslation', None)
    if st_cls is not None and not getattr(st_cls, '_cli_row_hooks_v2', False):
      orig_nt = st_cls.named_tuple_from_schema
      reserved_tuple_attrs = frozenset((
          '_asdict', '_fields', '_replace', '_field_defaults', 'count',
          'index'))

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
            type_name, (tuple, ),
            {
                '__slots__': (),
                '_fields': fnames,
                '__annotations__': dict(subfields),
                '__new__': _new,
                '_asdict': lambda self: dict(zip(fnames, self)),
                '_replace': lambda self, **kw: type(self)
                (*(kw.get(f, v) for f, v in zip(fnames, self))),
                '__repr__': lambda self: (
                    f'{type_name}(' + ', '.join(
                        f'{f}={v!r}' for f, v in zip(fnames, self)) + ')'),
            })
        for idx, fname in enumerate(fnames):
          if fname not in ('count', 'index') and not fname.startswith('_'):
            setattr(cls, fname, property(operator.itemgetter(idx)))
        return cls

      def _needs_kw_tuple(schema) -> bool:
        seen_names: set[str] = set()
        for f in schema.fields:
          fn = f.name
          if (not fn.isidentifier() or keyword.iskeyword(fn) or
              fn.startswith('__') or fn in reserved_tuple_attrs or
              fn in seen_names):
            return True
          seen_names.add(fn)
        return False

      def _safe_nt(self_st, schema):
        try:
          if _needs_kw_tuple(schema):
            raise ValueError('keyword or reserved tuple field name')
          with _ROW_HOOKS_LOCK:
            return orig_nt(self_st, schema)
        except (ValueError, TypeError, SyntaxError):
          # Workaround for apache/beam#40698 (keyword / non-identifier Row cols).
          from apache_beam import coders
          from apache_beam.typehints import row_type
          type_name = 'BeamSchema_{}'.format(schema.id.replace('-', '_'))
          with _ROW_HOOKS_LOCK:
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
          with _ROW_HOOKS_LOCK:
            if schema.id in self_st.schema_registry.by_id:
              return self_st.schema_registry.by_id[schema.id][0]
            ut = _make_kw_tuple(type_name, subfields)
            setattr(
                ut,
                '__reduce__',
                schemas_mod._named_tuple_reduce_method(
                    schema.SerializeToString()))
            setattr(ut, '_field_descriptions', descriptions)
            setattr(ut, row_type._BEAM_SCHEMA_ID, schema.id)
            self_st.schema_registry.add(ut, schema)
            coders.registry.register_coder(ut, coders.RowCoder)
            return ut

      st_cls.named_tuple_from_schema = _safe_nt
      st_cls._cli_preview_patched = True
      st_cls._cli_row_hooks_v2 = True

    # Workaround for apache/beam#41077 (keyword column names in yaml_mapping).
    orig_exp = getattr(yaml_map_mod, '_expand_python_mapping_func', None)
    py_call = getattr(yaml_map_mod, 'python_callable', None)
    if (orig_exp is not None and py_call is not None and
        not getattr(orig_exp, '_cli_preview_patched', False)):

      def _safe_exp(
          original_fields,
          expression=None,
          callable=None,
          path=None,
          name=None):
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

    # Workaround for apache/beam#41373 (_validator rejects None on output_type).
    orig_val = getattr(yaml_map_mod, '_validator', None)
    if orig_val is not None and not getattr(
        orig_val, '_cli_preview_patched', False):

      def _nullable_validator(beam_type):
        inner_v = orig_val(beam_type)
        try:
          beam_type.nullable = True
        except Exception:  # pylint: disable=broad-except
          pass
        return lambda x: x is None or inner_v(x)

      _nullable_validator._cli_preview_patched = True
      yaml_map_mod._validator = _nullable_validator

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
    is_row = isinstance(inner,
                        RowTypeConstraint) or ntc.match_is_named_tuple(inner)
    plan[name] = (
        _nested_row_plan(schemas.named_fields_from_element_type(inner))
        if is_row else None)
  return plan


def _rowify_value(value: Any, plan: dict[str, Any] | None) -> Any:
  """Builds a beam.Row from a dict or row-like `value` following `plan`."""
  import apache_beam as beam
  if value is None or plan is None:
    return value
  get = value.get if isinstance(value,
                                dict) else (lambda n: getattr(value, n, None))
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
      return pcoll | 'RowifyNested' >> beam.Map(
          _rowify_value, plan).with_output_types(pcoll.element_type)

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
    if isinstance(o,
                  row_type.RowTypeConstraint) or (inspect.isclass(o) and
                                                  ntc.match_is_named_tuple(o)):
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


def should_scan_local_csv_leading_zeros(path_or_files) -> bool:
  """True when local CSV files <= 64 MB should be scanned for leading zeros."""
  if os.environ.get('BEAM_CLI_NO_FAST_PATH'):
    return False
  return should_scan_all_local_json(path_or_files)


def _normalize_redact_tuple(raw: Any) -> tuple[str, ...]:
  if not raw:
    return ()
  if isinstance(raw, str):
    return tuple(p.strip() for p in raw.split(',') if p.strip())
  return tuple(str(p).strip() for p in raw if str(p).strip())


def read_csv_with_dlq(
    path: str,
    *,
    delimiter: str = ',',
    null_values: Optional[list[str]] = None,
    types: Optional[dict[str, str]] = None,
    dlq_redact_fields: Any = None,
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
  redact_cols = _normalize_redact_tuple(
      dlq_redact_fields if dlq_redact_fields is not None else (
          error_handling or {}).get('redact_fields'))
  header, kinds = _preflight_csv_schema(
      path,
      delimiter=delimiter,
      null_values=nulls,
      scan_all_for_leading_zeros=should_scan_local_csv_leading_zeros(path))
  kinds = apply_type_overrides(kinds, types, path)
  py_type_map = {
      'int': Optional[int],
      'float': Optional[float],
      'bool': Optional[bool],
      'str': Optional[str],
  }
  good_row_type = RowTypeConstraint.from_fields([(col, py_type_map[kinds[col]])
                                                 for col in header])
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

    def start_bundle(self):
      self._rows_read.inc(0)

    def process(self, readable_file):
      file_path = readable_file.metadata.path
      with readable_file.open() as raw_f:
        for parsed, err in iter_csv_records(
            file_path,
            raw_f,
            header,
            kinds,
            delimiter,
            nulls,
            redact_fields=redact_cols):
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
    null_values=DEFAULT_CSV_NULL_VALUES,
    *,
    redact_fields: Iterable[str] = ()):
  """Parses one CSV file into `(row_dict, None)` or `(None, dlq_record)`.

  Shared by the `ReadCsvWithDlq` DoFn and the fast local path so both apply
  the same header handling, typing, and DLQ records. Every yielded item is
  one record read; a header row matching `header` and blank lines are skipped.
  """
  ensure_csv_field_size_limit()
  rk = _normalize_redact_tuple(redact_fields)
  wrapper = io.TextIOWrapper(
      raw_f, encoding='utf-8-sig', errors='replace', newline='')
  try:
    row_index = 0
    order = None
    for cells, csv_err, bad_line in iter_csv_rows_recovering(
        wrapper, delimiter=delimiter):
      if csv_err is not None:
        row_index += 1
        raw_obj: dict[str, Any] = {
            'file': file_path,
            'row_index': row_index,
            'raw_line': '<REDACTED>' if rk else (bad_line or '')[:4096],
        }
        err_s = f'file {file_path!r} row {row_index}: {csv_err}'
        if rk:
          err_s = scrub_unparseable_dlq_text(err_s, rk)
        yield None, {
            'step': 'ReadCsvWithDlq',
            'error_type': 'CsvSyntaxError',
            'error': err_s,
            'raw_payload': safe_json_dumps(raw_obj),
        }
        continue

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
            'error': (
                f'file {file_path!r} row {row_index}: '
                f'expected {len(header)} columns, got {len(cells)}'),
            'raw_payload': safe_json_dumps({
                'file': file_path,
                'row_index': row_index,
                'cells': ['<REDACTED>' for _ in cells] if rk else cells,
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
        row_map = dict(zip(header, cells))
        err_s = (
            f'file {file_path!r} row {row_index} '
            f'column {bad_col!r}: {conv_err}')
        if rk:
          row_map = redact_nested_fields(row_map, rk)
          err_s = scrub_unparseable_dlq_text(err_s, rk)
        yield None, {
            'step': 'ReadCsvWithDlq',
            'error_type': type(conv_err).__name__,
            'error': err_s,
            'raw_payload': safe_json_dumps(row_map),
        }
        continue
      yield parsed_vals, None
  finally:
    try:
      wrapper.detach()
    except Exception:  # pylint: disable=broad-except
      pass


def csv_fail_fast_message(err: dict[str, Any], step: str = '') -> str:
  """The error for a bad CSV record when no `--dlq` is configured."""
  return (
      f"{err['error_type']}: {err['error']}{step} (pass --dlq PATH to route "
      'bad records or run `beam inspect` to review input)')


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
        k: _infer_json_kind([o[k] for o in objects if o.get(k) is not None],
                            depth + 1)
        for k in keys
    }
  if objects or any(isinstance(v, list) for v in values):
    return 'json'
  n_bool = sum(1 for v in values if isinstance(v, bool))
  n_int = sum(
      1 for v in values if isinstance(v, int) and not isinstance(v, bool))
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


def resolve_json_sample_rows(path_or_files,
                             raw_sample_rows: Any = None) -> Optional[int]:
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
      f for f in sorted({str(x)
                         for x in extra_fields
                         if x}) if re.fullmatch(r'[A-Za-z_][A-Za-z0-9_.]*', f)
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
      f'field {path!r}: expected {kind}, got {type(value).__name__}')


def read_json_with_dlq(
    path: str,
    *,
    types: Optional[dict[str, str]] = None,
    sample_rows: Any = None,
    on_unknown_fields: str = 'error',
    dlq_redact_fields: Any = None,
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
  redact_cols = _normalize_redact_tuple(
      dlq_redact_fields if dlq_redact_fields is not None else (
          error_handling or {}).get('redact_fields'))
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

    def start_bundle(self):
      self._rows_read.inc(0)

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
          raise ValueError(
              json_fail_fast_message(stripped, exc, file_path=path)) from exc
        self._dlq_rows.inc()
        if isinstance(exc, SchemaDriftError):
          self._drift_rows.inc()
        yield beam.pvalue.TaggedOutput(
            'errors',
            beam.Row(
                **json_dlq_record(stripped, exc, redact_fields=redact_cols)))
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
    path: str | list[str],
    *,
    sample_rows: Optional[int] = 100) -> dict[str, Any]:
  """Infers field kinds from up to `sample_rows` valid JSON objects (`None` = all)."""
  matches = _match_paths(path)
  if not matches:
    raise ValueError(f'no JSON files matched {path!r}')

  samples: list[dict[str, Any]] = []
  has_non_empty_line = False
  for match_path in matches:
    with _open_auto(match_path) as raw_s, io.TextIOWrapper(
        raw_s, encoding='utf-8', errors='replace') as text_s:
      for raw_line in text_s:
        line = raw_line.strip()
        if not line:
          continue
        has_non_empty_line = True
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
    if not has_non_empty_line:
      return {}
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


def json_dlq_record(
    stripped: str,
    exc: Exception,
    *,
    redact_fields: Iterable[str] = (),
) -> dict[str, Any]:
  """The DLQ record for a JSONL line that failed `parse_json_record`."""
  from . import output as out_mod
  rk = _normalize_redact_tuple(redact_fields)
  err_s = out_mod.sanitize_untrusted_text(str(exc), max_chars=4096)
  err_s = out_mod.redact_inline_secrets(err_s, placeholder='<REDACTED_SECRET>')
  payload_obj: dict[str, Any] = {'raw_line': stripped[:4096]}
  if rk:
    err_s = scrub_unparseable_dlq_text(err_s, rk)
    payload_obj = redact_nested_fields(payload_obj, rk)
  return {
      'step': 'ReadJsonWithDlq',
      'error_type': type(exc).__name__,
      'error': err_s[:4096],
      'raw_payload': safe_json_dumps(payload_obj),
  }


def json_fail_fast_message(
    stripped: str,
    exc: Exception,
    step: str = '',
    *,
    line_number: Optional[int] = None,
    file_path: Optional[str] = None,
) -> str:
  """The error for a bad JSONL line when no `--dlq` is configured."""
  del stripped  # Never interpolate untrusted raw payload into error.message.
  loc_parts = []
  if file_path:
    loc_parts.append(f'file {file_path!r}')
  if line_number is not None:
    loc_parts.append(f'line {line_number}')
  loc = f" ({', '.join(loc_parts)})" if loc_parts else ''
  if isinstance(exc, SchemaDriftError):
    return (
        f'{type(exc).__name__}: {exc} in JSON input{loc}{step} '
        '(pass --dlq PATH to quarantine these records for review, '
        '--from-opt sample_rows=all to scan all records for schema, '
        '--from-opt fields=COL:TYPE to declare sparse fields, or '
        '--from-opt on_unknown_fields=ignore to drop extra fields)')
  return (
      f'{type(exc).__name__}: {exc} in JSON input{loc}{step} '
      '(pass --dlq PATH to route bad records or run `beam inspect` to review '
      'input)')


def cli_count_rows(counter: str = 'rows_read'):
  """Fusion-safe pass-through row counter preserving exact `element_type`."""
  import apache_beam as beam
  from apache_beam.metrics import Metrics

  class _CountDoFn(beam.DoFn):
    def __init__(self):
      self._counter = Metrics.counter('beam_cli', counter)

    def start_bundle(self):
      self._counter.inc(0)

    def process(self, element):
      self._counter.inc()
      yield element

  class _CliCountRowsTransform(beam.PTransform):
    def expand(self, pcoll):
      elem_type = pcoll.element_type
      return pcoll | f'Count_{counter}' >> beam.ParDo(
          _CountDoFn()).with_output_types(elem_type)

  return _CliCountRowsTransform()


def cli_drop_rows(counter: str = 'stage_dropped_rows'):
  """Consumes bad records under `--drop-bad`, counting without emitting rows."""
  import apache_beam as beam
  from apache_beam.metrics import Metrics

  class _DropDoFn(beam.DoFn):
    def __init__(self):
      self._counter = Metrics.counter('beam_cli', counter)

    def start_bundle(self):
      self._counter.inc(0)

    def process(self, element):
      del element
      self._counter.inc()
      if False:  # pylint: disable=using-constant-test
        yield None

  class _CliDropRowsTransform(beam.PTransform):
    def expand(self, pcoll):
      elem_type = pcoll.element_type
      return pcoll | f'Drop_{counter}' >> beam.ParDo(
          _DropDoFn()).with_output_types(elem_type)

  return _CliDropRowsTransform()


def cli_limit_rows(limit: int):
  """Caps output at `limit` rows per window while preserving timestamps and tracking `stage_dropped_rows`."""
  import apache_beam as beam
  from apache_beam.metrics import Metrics
  from apache_beam.transforms.window import TimestampedValue

  n = int(limit)
  if isinstance(limit, bool) or n < 0:
    raise ValueError(
        f'CliLimitRows limit must be an integer >= 0, got {limit!r}')

  class _TagTsDoFn(beam.DoFn):
    def process(self, element, ts=beam.DoFn.TimestampParam):
      yield (ts, element)

  class _LimitCombineFn(beam.CombineFn):
    def create_accumulator(self):
      return (0, [])

    def add_input(self, acc, element):
      cnt, kept = acc
      new_kept = kept if len(kept) >= n else kept + [element]
      return (cnt + 1, new_kept)

    def merge_accumulators(self, accumulators):
      total_cnt = 0
      merged_kept = []
      for cnt, kept in accumulators:
        total_cnt += cnt
        if len(merged_kept) < n and kept:
          need = n - len(merged_kept)
          merged_kept.extend(kept[:need])
      return (total_cnt, merged_kept)

    def extract_output(self, acc):
      cnt, kept = acc
      dropped = max(0, cnt - len(kept))
      if dropped:
        Metrics.counter('beam_cli', 'stage_dropped_rows').inc(dropped)
      return kept

  class _UnpackLimitDoFn(beam.DoFn):
    def process(self, items):
      for ts, elem in items:
        yield TimestampedValue(elem, ts)

  class _CliLimitRowsTransform(beam.PTransform):
    def expand(self, pcoll):
      elem_type = pcoll.element_type
      return (
          pcoll
          | 'TagTs' >> beam.ParDo(_TagTsDoFn())
          | 'CombineLimit' >> beam.CombineGlobally(
              _LimitCombineFn()).without_defaults()
          | 'UnpackLimit' >> beam.ParDo(
              _UnpackLimitDoFn()).with_output_types(elem_type))

  return _CliLimitRowsTransform()


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


def _bq_table_has_streaming_buffer(tbl: Any) -> bool:
  if hasattr(tbl, '_mock_children') and (
      'streaming_buffer' not in getattr(tbl, '__dict__', {}) and
      'streaming_buffer' not in (getattr(tbl, '_mock_children', None) or {})):
    return False
  return getattr(tbl, 'streaming_buffer', None) is not None


def cli_write_to_bigquery(
    table: str,
    *,
    create_disposition: str = 'CREATE_IF_NEEDED',
    write_disposition: str = 'WRITE_EMPTY',
    dlq_redact_fields: Any = None,
    error_handling: Optional[dict[str, Any]] = None,
):
  """Pure-Python BigQuery sink (zero Java boot; supports STREAMING & FILE_LOADS)."""
  import apache_beam as beam
  from apache_beam.io.gcp.bigquery import WriteToBigQuery
  from apache_beam.metrics import Metrics
  from apache_beam.options.pipeline_options import GoogleCloudOptions
  from apache_beam.typehints.row_type import RowTypeConstraint
  from . import output as out_mod

  err_tag = (error_handling or {}).get('output', 'errors')
  redact_cols = _normalize_redact_tuple(
      dlq_redact_fields if dlq_redact_fields is not None else (
          error_handling or {}).get('redact_fields'))
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
      clean_err = out_mod.sanitize_untrusted_text(err_str, max_chars=4096)
      clean_err = out_mod.redact_inline_secrets(
          clean_err, placeholder='<REDACTED_SECRET>')
      clean_row = redact_nested_fields(
          row_dict, redact_cols) if redact_cols else row_dict
      if redact_cols:
        clean_err = scrub_unparseable_dlq_text(clean_err, redact_cols)
      yield beam.Row(
          step='WriteToBigQuery',
          error_type='BigQueryWriteError',
          error=clean_err[:4096],
          raw_payload=safe_json_dumps(clean_row))

  class _CliWriteBqTransform(beam.PTransform):
    def expand(self, pcoll):
      bq_schema = _beam_row_type_to_bq_schema_dict(pcoll.element_type)
      temp_loc = pcoll.pipeline.options.view_as(
          GoogleCloudOptions).temp_location

      if write_disposition == 'WRITE_TRUNCATE' and not temp_loc:
        raise ValueError(
            'BigQuery --if-exists=replace (WRITE_TRUNCATE) requires '
            '--temp_location=gs://<bucket>/tmp for atomic file loads')

      use_streaming = bool(error_handling) or not temp_loc
      if write_disposition == 'WRITE_EMPTY' and use_streaming:
        from google.api_core import exceptions as gcp_exceptions
        from google.cloud import bigquery as gcp_bq
        from . import uris
        _, proj, ds, tbl_name = uris.normalize_bq_table_spec(
            table,
            default_project=pcoll.pipeline.options.view_as(
                GoogleCloudOptions).project)
        client = gcp_bq.Client(project=proj)
        eff_proj = uris.validate_bq_project_id(proj or client.project)
        fq = f'{eff_proj}.{ds}.{tbl_name}'
        try:
          existing_tbl = client.get_table(fq)
          has_rows = (existing_tbl.num_rows or 0) > 0
          has_sbuf = _bq_table_has_streaming_buffer(existing_tbl)
          if has_rows or has_sbuf:
            detail = (
                f'has {existing_tbl.num_rows} rows'
                if has_rows else 'has active streaming buffer rows')
            raise ValueError(
                f'BigQuery table {fq!r} already exists and {detail}; pass '
                '--if-exists=append to add rows or choose a new destination '
                'table')
        except gcp_exceptions.NotFound:
          if create_disposition != 'CREATE_NEVER':
            bq_fields = [
                gcp_bq.SchemaField(
                    f['name'], f['type'], mode=f.get('mode', 'NULLABLE'))
                for f in bq_schema.get('fields', [])
            ]
            try:
              client.create_table(
                  gcp_bq.Table(fq, schema=bq_fields), exists_ok=False)
            except gcp_exceptions.Conflict:
              raise ValueError(
                  f'BigQuery table {fq!r} was concurrently created by another '
                  'writer (--if-exists=fail); pass --if-exists=append to add '
                  'rows or choose a new destination table') from None

      method = (
          WriteToBigQuery.Method.STREAMING_INSERTS
          if use_streaming else WriteToBigQuery.Method.FILE_LOADS)
      effective_disp = (
          'WRITE_APPEND' if
          (use_streaming and
           write_disposition == 'WRITE_EMPTY') else write_disposition)

      bq_write_kwargs: dict[str, Any] = {
          'schema': bq_schema,
          'create_disposition': create_disposition,
          'write_disposition': effective_disp,
          'method': method,
      }
      if use_streaming:
        bq_write_kwargs['insert_retry_strategy'] = 'RETRY_ON_TRANSIENT_ERROR'

      dicts = pcoll | 'ToBqDict' >> beam.ParDo(_RowToBqDictDoFn())
      write_res = dicts | 'WriteBq' >> WriteToBigQuery(table, **bq_write_kwargs)

      if error_handling and 'output' in error_handling and use_streaming:
        errs = (
            write_res.failed_rows_with_errors
            | 'FormatBqErrors' >> beam.ParDo(
                _FormatBqErrorDoFn()).with_output_types(dlq_row_type))
        return {
            'post_write': errs | beam.FlatMap(lambda _: ()),
            err_tag: errs,
        }
      return None

  return _CliWriteBqTransform()
