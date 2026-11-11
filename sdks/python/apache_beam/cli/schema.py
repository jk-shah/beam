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

"""Pre-flight schema contract and compatibility guard (`beam schema diff`).

Compares schemas between source and target datasets, detecting breaking changes,
dropped non-nullable fields, and unsafe type coercions before submitting compute
to Google Cloud Dataflow or portable runners.
"""

from typing import Any, Iterable, Optional

EXIT_OK = 0
EXIT_PIPELINE_FAILED = 1
EXIT_USAGE = 2

# Canonical types recognized across engines
CANONICAL_TYPES = frozenset([
    'INT32',
    'INT64',
    'FLOAT32',
    'FLOAT64',
    'STRING',
    'BYTES',
    'BOOLEAN',
    'TIMESTAMP',
    'DATE',
    'TIME',
    'RECORD',
    'ARRAY',
    'UNKNOWN',
])

# Safe widening conversions (Source -> Target)
_SAFE_WIDENINGS = {
    ('INT32', 'INT64'),
    ('INT32', 'FLOAT64'),
    ('FLOAT32', 'FLOAT64'),
    ('DATE', 'TIMESTAMP'),
}

# Type narrowing / loss-of-precision conversions (Source -> Target)
_TYPE_NARROWINGS = {
    ('INT64', 'INT32'): 'INTEGER_OVERFLOW_RISK',
    ('FLOAT64', 'FLOAT32'): 'PRECISION_LOSS',
    ('FLOAT64', 'INT64'): 'FRACTIONAL_TRUNCATION',
    ('FLOAT64', 'INT32'): 'FRACTIONAL_TRUNCATION_AND_OVERFLOW',
    ('FLOAT32', 'INT64'): 'FRACTIONAL_TRUNCATION',
    ('FLOAT32', 'INT32'): 'FRACTIONAL_TRUNCATION',
    ('TIMESTAMP', 'DATE'): 'TEMPORAL_COMPONENT_LOSS',
    ('STRING', 'INT64'): 'PARSE_CAST_FAILURE_RISK',
    ('STRING', 'INT32'): 'PARSE_CAST_FAILURE_RISK',
    ('STRING', 'FLOAT64'): 'PARSE_CAST_FAILURE_RISK',
    ('STRING', 'FLOAT32'): 'PARSE_CAST_FAILURE_RISK',
    ('STRING', 'BOOLEAN'): 'PARSE_CAST_FAILURE_RISK',
    ('STRING', 'TIMESTAMP'): 'PARSE_CAST_FAILURE_RISK',
    ('STRING', 'DATE'): 'PARSE_CAST_FAILURE_RISK',
    ('INT64', 'STRING'): 'NUMERIC_TO_TEXT_COERCION',
    ('INT32', 'STRING'): 'NUMERIC_TO_TEXT_COERCION',
    ('FLOAT64', 'STRING'): 'NUMERIC_TO_TEXT_COERCION',
    ('FLOAT32', 'STRING'): 'NUMERIC_TO_TEXT_COERCION',
    ('BOOLEAN', 'STRING'): 'BOOLEAN_TO_TEXT_COERCION',
}


def normalize_type(raw: Any) -> str:
  """Normalizes types from Arrow, BigQuery, Avro, Iceberg, Delta into a canonical name."""
  if raw is None:
    return 'UNKNOWN'
  s = str(raw).strip().lower()

  # Strip wrapper affixes like nullable(int64) or int64[pyarrow]
  if s.startswith('nullable(') and s.endswith(')'):
    s = s[9:-1].strip()
  if '[' in s or '<' in s or '(' in s:
    delim = '[' if '[' in s else ('<' if '<' in s else '(')
    base = s.split(delim, 1)[0].strip()
    if base in ('timestamp', 'datetime'):
      return 'TIMESTAMP'
    if base in ('date', 'date32', 'date64'):
      return 'DATE'
    if base in ('time', 'time32', 'time64'):
      return 'TIME'
    if base in ('list', 'array'):
      return 'ARRAY'
    if base in ('struct', 'map', 'record'):
      return 'RECORD'
    s = base

  # Integer mappings
  if s in (
      'int', 'integer', 'int32', 'int16', 'int8', 'uint8', 'uint16',
      'uint32', 'smallint', 'tinyint', 'short', 'byte'):
    return 'INT32'
  if s in ('int64', 'uint64', 'bigint', 'long'):
    return 'INT64'

  # Floating / numeric mappings
  if s in ('float', 'float32', 'real'):
    return 'FLOAT32'
  if s in (
      'float64', 'double', 'numeric', 'bignumeric', 'decimal', 'number'):
    return 'FLOAT64'

  # Text mappings
  if s in (
      'str', 'string', 'text', 'varchar', 'char', 'utf8', 'large_utf8',
      'large_string'):
    return 'STRING'

  # Binary mappings
  if s in ('bytes', 'binary', 'large_binary', 'blob', 'bytearray'):
    return 'BYTES'

  # Boolean mappings
  if s in ('bool', 'boolean'):
    return 'BOOLEAN'

  # Temporal mappings
  if s in ('timestamp', 'datetime', 'timestamptz'):
    return 'TIMESTAMP'
  if s in ('date', 'date32', 'date64'):
    return 'DATE'
  if s in ('time', 'time32', 'time64'):
    return 'TIME'

  # Complex mappings
  if s in ('struct', 'record', 'map'):
    return 'RECORD'
  if s in ('list', 'array'):
    return 'ARRAY'

  return 'UNKNOWN'


def extract_endpoint_schema(
    location_or_schema: Any,
    *,
    explicit_format: Optional[str] = None,
    options: Optional[list[str]] = None,
    default_project: Optional[str] = None,
) -> list[dict[str, Any]]:
  """Extracts and normalizes schema fields from an endpoint URI, dict, or list."""
  if isinstance(location_or_schema, list):
    raw_fields = location_or_schema
  elif isinstance(location_or_schema, dict) and 'schema' in location_or_schema:
    raw_fields = location_or_schema['schema']
  elif isinstance(location_or_schema, dict):
    raw_fields = [
        {'name': k, 'type': v, 'nullable': True}
        for k, v in location_or_schema.items()
    ]
  elif isinstance(location_or_schema, str):
    from . import inspect_cmd
    insp = inspect_cmd.run_inspect(
        location_or_schema,
        explicit_format=explicit_format,
        sample_count=0,
        from_opts=options,
        default_project=default_project)
    raw_fields = insp.get('schema') or []
  else:
    raise ValueError(f'unsupported schema input: {location_or_schema!r}')

  normalized = []
  for f in raw_fields:
    if isinstance(f, dict):
      name = str(f.get('name') or f.get('field_name') or '')
      raw_t = f.get('type') or f.get('field_type') or 'UNKNOWN'
      mode = str(f.get('mode') or '').upper()
      is_nullable = f.get('nullable', mode != 'REQUIRED')
      norm_t = normalize_type(raw_t)
      normalized.append({
          'name': name,
          'type': norm_t,
          'raw_type': str(raw_t),
          'nullable': bool(is_nullable),
          'mode': 'NULLABLE' if is_nullable else 'REQUIRED',
      })
    elif isinstance(f, (list, tuple)) and len(f) >= 2:
      name, raw_t = str(f[0]), f[1]
      norm_t = normalize_type(raw_t)
      normalized.append({
          'name': name,
          'type': norm_t,
          'raw_type': str(raw_t),
          'nullable': True,
          'mode': 'NULLABLE',
      })
  return normalized


def diff_schemas(
    source_schema: list[dict[str, Any]],
    target_schema: list[dict[str, Any]],
    *,
    mode: str = 'BACKWARD',
    strict: bool = False,
    source_label: str = 'source',
    target_label: str = 'target',
) -> dict[str, Any]:
  """Compares source and target schemas against compatibility mode rules."""
  eff_mode = str(mode).strip().upper()
  if eff_mode not in ('BACKWARD', 'FORWARD', 'FULL'):
    raise ValueError(
        f'unsupported compatibility mode {mode!r}; expected BACKWARD, FORWARD, or FULL')

  src_fields = extract_endpoint_schema(source_schema)
  tgt_fields = extract_endpoint_schema(target_schema)

  src_map = {f['name']: f for f in src_fields}
  tgt_map = {f['name']: f for f in tgt_fields}

  violations: list[dict[str, Any]] = []
  warnings: list[dict[str, Any]] = []
  columns_added: list[str] = []
  columns_missing: list[str] = []
  type_mismatches: list[dict[str, Any]] = []
  nullability_mismatches: list[dict[str, Any]] = []
  next_actions: list[dict[str, Any]] = []

  # 1. Evaluate Target Requirements against Source (BACKWARD / FULL)
  if eff_mode in ('BACKWARD', 'FULL'):
    for tgt_name, tgt_col in tgt_map.items():
      if tgt_name not in src_map:
        columns_missing.append(f"{tgt_name} ({tgt_col['type']}, {tgt_col['mode']})")
        if not tgt_col['nullable']:
          violations.append({
              'field': tgt_name,
              'issue': 'DROPPED_REQUIRED_COLUMN',
              'severity': 'ERROR',
              'target_type': tgt_col['type'],
              'target_mode': tgt_col['mode'],
              'description': (
                  f"Target requires non-null column '{tgt_name}' ({tgt_col['type']}), "
                  f"but it is missing from {source_label}."
              ),
          })
          next_actions.append({
              'action': 'provide_default_column',
              'field': tgt_name,
              'command_hint': f"--select \"'DEFAULT_VAL' AS {tgt_name}, *\"",
              'description': f"Supply a fallback or default value for required column '{tgt_name}'.",
          })
        else:
          warnings.append({
              'field': tgt_name,
              'issue': 'SAFE_COLUMN_MISSING',
              'severity': 'INFO',
              'description': (
                  f"Target column '{tgt_name}' ({tgt_col['type']}) is missing in {source_label} "
                  "and will be populated with NULL."
              ),
          })
      else:
        src_col = src_map[tgt_name]
        # Nullability check
        if not tgt_col['nullable'] and src_col['nullable']:
          nullability_mismatches.append({
              'field': tgt_name,
              'source_mode': src_col['mode'],
              'target_mode': tgt_col['mode'],
          })
          violations.append({
              'field': tgt_name,
              'issue': 'NULLABILITY_MISMATCH',
              'severity': 'ERROR',
              'source_mode': src_col['mode'],
              'target_mode': tgt_col['mode'],
              'description': (
                  f"Column '{tgt_name}' is NULLABLE in {source_label}, but REQUIRED in {target_label}. "
                  "Potential NULL values will violate target database constraints."
              ),
          })
          next_actions.append({
              'action': 'filter_or_coalesce_nulls',
              'field': tgt_name,
              'command_hint': (
                  f"--where \"{tgt_name} IS NOT NULL\" or "
                  f"--select \"COALESCE({tgt_name}, 'DEFAULT') AS {tgt_name}, *\""
              ),
              'description': f"Filter out null records or apply COALESCE() to column '{tgt_name}'.",
          })

        # Type compatibility check
        st, tt = src_col['type'], tgt_col['type']
        if st != tt:
          mismatch_info = {
              'field': tgt_name,
              'source_type': st,
              'target_type': tt,
              'source_raw': src_col['raw_type'],
              'target_raw': tgt_col['raw_type'],
          }
          type_mismatches.append(mismatch_info)

          if (st, tt) in _SAFE_WIDENINGS:
            if strict:
              violations.append({
                  'field': tgt_name,
                  'issue': 'STRICT_TYPE_WIDENING_VIOLATION',
                  'severity': 'ERROR',
                  'source_type': st,
                  'target_type': tt,
                  'description': (
                      f"Safe widening from {st} to {tt} on column '{tgt_name}' is rejected under --strict."
                  ),
              })
            else:
              warnings.append({
                  'field': tgt_name,
                  'issue': 'SAFE_TYPE_WIDENING',
                  'severity': 'INFO',
                  'source_type': st,
                  'target_type': tt,
                  'description': f"Column '{tgt_name}' safely widens from {st} to {tt}.",
              })
          elif (st, tt) in _TYPE_NARROWINGS:
            narrow_reason = _TYPE_NARROWINGS[(st, tt)]
            violations.append({
                'field': tgt_name,
                'issue': 'TYPE_NARROWING_ERROR',
                'severity': 'ERROR',
                'source_type': st,
                'target_type': tt,
                'narrowing_category': narrow_reason,
                'description': (
                    f"Column '{tgt_name}' type {st} cannot be safely written to target {tt} "
                    f"without potential runtime cast failure or precision loss ({narrow_reason})."
                ),
            })
            next_actions.append({
                'action': 'explicit_type_cast',
                'field': tgt_name,
                'command_hint': f"--select \"CAST({tgt_name} AS {tt}) AS {tgt_name}, *\"",
                'description': f"Explicitly cast column '{tgt_name}' to {tt} before writing.",
            })
          else:
            violations.append({
                'field': tgt_name,
                'issue': 'INCOMPATIBLE_TYPE_ERROR',
                'severity': 'ERROR',
                'source_type': st,
                'target_type': tt,
                'description': (
                    f"Column '{tgt_name}' type {st} is completely incompatible with target type {tt}."
                ),
            })

    # Evaluate extra columns in Source
    for src_name, src_col in src_map.items():
      if src_name not in tgt_map:
        columns_added.append(f"{src_name} ({src_col['type']}, {src_col['mode']})")
        if strict:
          violations.append({
              'field': src_name,
              'issue': 'EXTRA_SOURCE_COLUMN_REJECTED',
              'severity': 'ERROR',
              'source_type': src_col['type'],
              'description': (
                  f"Extra source column '{src_name}' does not exist in target and is rejected under --strict."
              ),
          })
        else:
          warnings.append({
              'field': src_name,
              'issue': 'EXTRA_SOURCE_COLUMN',
              'severity': 'INFO',
              'description': (
                  f"Source column '{src_name}' ({src_col['type']}) does not exist in target; "
                  "it will be dropped or ignored unless target schema evolution is enabled."
              ),
          })

  # 2. Evaluate FORWARD / FULL Compatibility
  if eff_mode in ('FORWARD', 'FULL'):
    for src_name, src_col in src_map.items():
      if src_name not in tgt_map and not src_col['nullable']:
        violations.append({
            'field': src_name,
            'issue': 'FORWARD_REQUIRED_COLUMN_ADDED',
            'severity': 'ERROR',
            'source_type': src_col['type'],
            'description': (
                f"Source column '{src_name}' is REQUIRED, which breaks forward compatibility for readers "
                "expecting older schema versions without this field."
            ),
        })

  compatible = len(violations) == 0

  return {
      'compatible': compatible,
      'compatibility_mode': eff_mode,
      'strict': strict,
      'violation_count': len(violations),
      'warning_count': len(warnings),
      'violations': violations,
      'warnings': warnings,
      'diff': {
          'columns_added': columns_added,
          'columns_missing': columns_missing,
          'type_mismatches': type_mismatches,
          'nullability_mismatches': nullability_mismatches,
      },
      'source_columns': [f['name'] for f in src_fields],
      'target_columns': [f['name'] for f in tgt_fields],
      'source_schema': src_fields,
      'target_schema': tgt_fields,
      'next_actions': next_actions,
  }


def run_schema_diff(
    source: str,
    target: str,
    *,
    mode: str = 'BACKWARD',
    strict: bool = False,
    source_format: Optional[str] = None,
    target_format: Optional[str] = None,
    source_opts: Optional[list[str]] = None,
    target_opts: Optional[list[str]] = None,
    default_project: Optional[str] = None,
) -> dict[str, Any]:
  """Extracts schemas from source and target endpoints and runs compatibility diff."""
  src_fields = extract_endpoint_schema(
      source,
      explicit_format=source_format,
      options=source_opts,
      default_project=default_project)
  tgt_fields = extract_endpoint_schema(
      target,
      explicit_format=target_format,
      options=target_opts,
      default_project=default_project)

  return diff_schemas(
      src_fields,
      tgt_fields,
      mode=mode,
      strict=strict,
      source_label=source,
      target_label=target)
