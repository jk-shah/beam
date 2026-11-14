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

"""Online Feature & Table Enrichment for Beam CLI & Beam YAML (`B1.3`).

Provides `CliEnrich` (`cli_enrich`), a fail-closed enrichment transform with:
1. Lazy worker-side client setup (`DoFn.setup`) so `--dry-run` and `beam check`
   never execute network RPCs or require optional ML/FeatureStore packages.
2. Parameterized BigQuery SQL (`QueryJobConfig` + `IN UNNEST(@keys)`) eliminating
   `.format()` SQL injection, capped `max_batch_size <= 500`, and auto-included
   join keys stripped post-fetch.
3. Hardened Bigtable, Vertex AI Feature Store (`googleapis.com` endpoint SSRF
   guard + `ExceptionLevel.WARN`), Feast (`SafeLoader` allowlist + temp-file
   cleanup + all-`None` miss detection), and local `<100ms` `file:` handlers.
4. TLS-enforced Redis caching (`BEAM_REDIS_PASSWORD`, JSON values, SHA-256 keys)
   with cache-poisoning prevention and graceful degradation on cache outages.
5. Batch RPC execution with `AdaptiveThrottler(window_ms=120_000, bucket_ms=1000)`
   and automatic singleton fallback so 1 bad key routes to `--dlq` while
   healthy sibling rows in the batch succeed and preserve event timestamps.
"""

from __future__ import annotations

import concurrent.futures
import csv
import decimal
import hashlib
import json
import logging
import math
import os
import re
import tempfile
import time
import urllib.parse
from typing import Any
from typing import Iterable
from typing import Mapping
from typing import NamedTuple
from typing import Optional

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io.components.adaptive_throttler import AdaptiveThrottler
from apache_beam.metrics import Metrics
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.typehints.schemas import named_fields_from_element_type
from apache_beam.utils.windowed_value import WindowedValue

from . import dlq
from . import interval_join
from . import output
from . import uris

_LOGGER = logging.getLogger(__name__)

split_bracket_tokens = interval_join.split_bracket_tokens
parse_column_list = interval_join.parse_column_list
encode_canonical_key_values = interval_join.encode_canonical_key_values

MAX_BQ_BATCH_SIZE = 500
DEFAULT_BQ_BATCH_SIZE = 100
DEFAULT_FILE_MAX_BYTES = 256 * 1024 * 1024
MAX_FEAST_YAML_BYTES = 64 * 1024

_LOOPBACK_HOSTS = frozenset(['localhost', '127.0.0.1', '::1', '[::1]'])
_GCP_REGION_RE = re.compile(r'^[a-z]+-[a-z0-9]+$')
_RESOURCE_SLUG_RE = re.compile(r'^[A-Za-z0-9_.-]{1,128}$')
_VERTEX_ENDPOINT_RE = re.compile(
    r'^(?:(?P<loc>[a-z0-9-]+)-)?aiplatform\.googleapis\.com(?::443)?$')

_SAFE_FEAST_PROVIDERS = frozenset(['local', 'gcp', 'aws', 'azure'])
_SAFE_FEAST_ONLINE_STORES = frozenset([
    'sqlite',
    'redis',
    'datastore',
    'bigtable',
    'dynamodb',
    'postgres',
    'snowflake',
    'null',
])

_HANDLER_ALIASES = {
    'file': 'file',
    'local': 'file',
    'side': 'file',
    'bigquery': 'bigquery',
    'bq': 'bigquery',
    'bigtable': 'bigtable',
    'bt': 'bigtable',
    'vertexai': 'vertexai',
    'vertex_ai': 'vertexai',
    'vertexfs': 'vertexai',
    'vertexaifeaturestore': 'vertexai',
    'feast': 'feast',
    'feastfeaturestore': 'feast',
}

_DECLARED_TYPE_MAP = {
    'str': 'str',
    'string': 'str',
    'int': 'int',
    'integer': 'int',
    'int64': 'int',
    'float': 'float',
    'double': 'float',
    'float64': 'float',
    'bool': 'bool',
    'boolean': 'bool',
}

_PY_TYPE_MAP: dict[str, type] = {
    'str': str,
    'int': int,
    'float': float,
    'bool': bool,
}

_FORBIDDEN_CALLABLE_KEYS = frozenset([
    'query_fn',
    'condition_value_fn',
    'row_restriction_template',
    'row_key_fn',
    'entity_row_fn',
    'join_fn',
])

_COMMON_ENRICH_KEYS = frozenset([
    'handler',
    'enrichment_handler',
    'provider',
    'key',
    'row_key',
    'fields',
    'entity_id',
    'lookup_key',
    'right_key',
    'table_fields',
    'columns',
    'column_names',
    'features',
    'feature_ids',
    'prefix',
    'on_miss',
    'timeout',
    'min_batch_size',
    'max_batch_size',
    'max_batch_duration_secs',
    'rate_limit',
    'redis',
    'redis_host',
    'redis_port',
    'redis_db',
    'redis_tls',
    'redis_ttl',
    'redis_password',
    'redis_password_env',
    'dlq_redact_fields',
    'trust_providers',
    'error_handling',
])

_HANDLER_SPECIFIC_KEYS: dict[str, frozenset[str]] = {
    'file': frozenset(['path', 'uri', 'file', 'max_file_bytes']),
    'bigquery': frozenset(
        ['project', 'project_id', 'table', 'table_name', 'row_restriction']),
    'bigtable': frozenset([
        'project',
        'project_id',
        'instance',
        'instance_id',
        'table',
        'table_id',
        'app_profile',
        'app_profile_id',
        'encoding',
        'column_family',
    ]),
    'vertexai': frozenset([
        'project',
        'project_id',
        'location',
        'region',
        'api_endpoint',
        'feature_store',
        'feature_store_name',
        'feature_view',
        'feature_view_name',
    ]),
    'feast': frozenset([
        'path',
        'yaml_path',
        'feature_store_yaml_path',
        'feature_names',
        'feature_service',
        'feature_service_name',
        'full_feature_names',
    ]),
}


class EnrichConfig(NamedTuple):
  handler: str
  fields: tuple[str, ...]
  lookup_fields: tuple[str, ...]
  columns: tuple[str, ...]
  column_types: dict[str, str]
  column_sources: dict[str, str]
  prefix: str
  on_miss: str
  timeout: float
  min_batch_size: int
  max_batch_size: int
  max_batch_duration_secs: float | None
  rate_limit: float | None
  redis_host: str | None
  redis_port: int | None
  redis_db: int
  redis_tls: bool
  redis_ttl: int
  redis_password_env: str
  dlq_redact_fields: tuple[str, ...]
  trust_providers: bool
  handler_params: dict[str, Any]
  explicit_types: frozenset[str] = frozenset()
  redis_password: str | None = None


def _parse_bool(val: Any, flag_label: str) -> bool:
  if isinstance(val, bool):
    return val
  s = str(val).strip().lower()
  if s in ('1', 'true', 'yes', 'on'):
    return True
  if s in ('0', 'false', 'no', 'off'):
    return False
  raise ValueError(f'expected boolean for {flag_label}, got {val!r}')


def _parse_positive_float(val: Any, label: str) -> float:
  if isinstance(val, str) and any(c.isalpha() for c in val):
    sec = interval_join.parse_interval_duration(val)
  else:
    try:
      sec = float(val)
    except (TypeError, ValueError):
      raise ValueError(
          f'{label} must be a positive number or duration, got {val!r}'
      ) from None
  if not math.isfinite(sec) or sec <= 0:
    raise ValueError(f'{label} must be > 0, got {val!r}')
  return sec


def _parse_positive_int(val: Any, label: str) -> int:
  if isinstance(val, bool):
    raise ValueError(f'{label} must be a positive integer, got {val!r}')
  try:
    iv = int(val)
  except (TypeError, ValueError):
    raise ValueError(
        f'{label} must be a positive integer, got {val!r}') from None
  if iv < 1:
    raise ValueError(f'{label} must be >= 1, got {val!r}')
  return iv


def _validate_ident_list(cols: Iterable[str], label: str) -> tuple[str, ...]:
  out: list[str] = []
  for c in cols:
    cs = str(c).strip()
    if not output.SAFE_IDENTIFIER_RE.match(cs):
      raise ValueError(f'invalid column identifier {cs!r} in {label}')
    if cs not in out:
      out.append(cs)
  return tuple(out)


def _parse_typed_columns(
    raw_cols: Any,
    *,
    handler: str,
    full_feature_names: bool = False,
) -> tuple[
    tuple[str, ...], dict[str, str], dict[str, str], list[str], frozenset[str]]:
  """Parses `columns` / `feature_names` into `(out_cols, types, sources, raw_specs, explicit_types)`."""
  items = parse_column_list(raw_cols)
  if not items:
    return (), {}, {}, [], frozenset()

  out_cols: list[str] = []
  col_types: dict[str, str] = {}
  col_sources: dict[str, str] = {}
  clean_raw_specs: list[str] = []
  explicit_set: set[str] = set()

  # First pass to detect duplicate qualifiers across Bigtable column families.
  raw_parsed: list[tuple[str | None, str, str, str, bool]] = []
  qual_counts: dict[str, int] = {}
  for item in items:
    parts = [p.strip() for p in item.split(':')]
    if handler == 'feast':
      # Feast features are `view:feature` or `view:feature:type` (or `feature[:type]`).
      if len(parts) == 3 and parts[2].lower() in _DECLARED_TYPE_MAP:
        view_name, feat_name, dtype, is_exp = (
            parts[0], parts[1], _DECLARED_TYPE_MAP[parts[2].lower()], True)
        raw_spec = f'{view_name}:{feat_name}'
      elif len(parts) == 2 and parts[1].lower() in _DECLARED_TYPE_MAP and (
          parts[0] and output.SAFE_IDENTIFIER_RE.match(parts[0])):
        view_name, feat_name, dtype, is_exp = parts[0], parts[1], 'str', False
        raw_spec = f'{view_name}:{feat_name}'
      elif len(parts) == 2:
        view_name, feat_name, dtype, is_exp = parts[0], parts[1], 'str', False
        raw_spec = f'{view_name}:{feat_name}'
      elif len(parts) == 1:
        view_name, feat_name, dtype, is_exp = None, parts[0], 'str', False
        raw_spec = feat_name
      else:
        raise ValueError(f'invalid Feast feature specification {item!r}')
      feast_out = (
          f'{view_name}__{feat_name}' if
          (full_feature_names and view_name) else feat_name)
      if not output.SAFE_IDENTIFIER_RE.match(feast_out):
        raise ValueError(
            f'invalid Feast feature output column {feast_out!r} in {item!r}')
      raw_parsed.append((view_name, feast_out, dtype, raw_spec, is_exp))
      qual_counts[feast_out] = qual_counts.get(feast_out, 0) + 1
    elif handler == 'bigtable':
      if len(parts) == 3:
        cf, qual, raw_t = parts
        if raw_t.lower() not in _DECLARED_TYPE_MAP:
          raise ValueError(
              f'unknown column type {raw_t!r} in {item!r}; '
              'expected str, int, float, or bool')
        dtype, is_exp = _DECLARED_TYPE_MAP[raw_t.lower()], True
        raw_spec = f'{cf}:{qual}'
      elif len(parts) == 2:
        if parts[1].lower() in _DECLARED_TYPE_MAP:
          cf, qual, dtype, is_exp = (
              None, parts[0], _DECLARED_TYPE_MAP[parts[1].lower()], True)
          raw_spec = qual
        else:
          cf, qual, dtype, is_exp = parts[0], parts[1], 'str', False
          raw_spec = f'{cf}:{qual}'
      elif len(parts) == 1:
        cf, qual, dtype, is_exp = None, parts[0], 'str', False
        raw_spec = qual
      else:
        raise ValueError(f'invalid Bigtable column specification {item!r}')
      if cf is not None and not _RESOURCE_SLUG_RE.match(cf):
        raise ValueError(f'invalid Bigtable column family {cf!r} in {item!r}')
      if not output.SAFE_IDENTIFIER_RE.match(qual):
        raise ValueError(f'invalid column identifier {qual!r} in {item!r}')
      raw_parsed.append((cf, qual, dtype, raw_spec, is_exp))
      qual_counts[qual] = qual_counts.get(qual, 0) + 1
    else:
      if len(parts) == 2:
        col_name, raw_t = parts
        if raw_t.lower() not in _DECLARED_TYPE_MAP:
          raise ValueError(
              f'unknown column type {raw_t!r} in {item!r}; '
              'expected str, int, float, or bool')
        dtype, is_exp = _DECLARED_TYPE_MAP[raw_t.lower()], True
      elif len(parts) == 1:
        col_name, dtype, is_exp = parts[0], 'str', False
      else:
        raise ValueError(f'invalid column specification {item!r}')
      if not output.SAFE_IDENTIFIER_RE.match(col_name):
        raise ValueError(f'invalid column identifier {col_name!r} in {item!r}')
      raw_parsed.append((None, col_name, dtype, col_name, is_exp))
      qual_counts[col_name] = qual_counts.get(col_name, 0) + 1

  for ns_prefix, base_col, dtype, raw_spec, is_exp in raw_parsed:
    if handler == 'bigtable' and ns_prefix and qual_counts.get(base_col, 0) > 1:
      safe_cf = re.sub(r'[^A-Za-z0-9_]', '_', ns_prefix)
      out_col = f'{safe_cf}_{base_col}'
    else:
      out_col = base_col
    if not output.SAFE_IDENTIFIER_RE.match(out_col):
      raise ValueError(f'invalid enriched output column {out_col!r}')
    if out_col in col_types:
      raise ValueError(f'duplicate enriched column {out_col!r}')
    out_cols.append(out_col)
    col_types[out_col] = dtype
    col_sources[out_col] = raw_spec
    clean_raw_specs.append(raw_spec)
    if is_exp:
      explicit_set.add(out_col)

  return (
      tuple(out_cols),
      col_types,
      col_sources,
      clean_raw_specs,
      frozenset(explicit_set),
  )


def _parse_redis_endpoint(
    redis_val: str | None,
    *,
    redis_host_opt: str | None = None,
    redis_port_opt: Any = None,
    redis_db_opt: Any = None,
    redis_tls_opt: Any = None,
) -> tuple[str | None, int | None, int, bool]:
  if not redis_val and not redis_host_opt:
    if redis_port_opt is not None or redis_tls_opt is not None:
      raise ValueError('redis_port / redis_tls requires redis=HOST:PORT')
    return None, None, 0, False

  raw = str(redis_val or '').strip()
  scheme_ssl: bool | None = None
  db = int(redis_db_opt) if redis_db_opt is not None else 0
  if db < 0:
    raise ValueError(f'redis_db must be >= 0, got {db}')

  if raw:
    if '://' in raw:
      uris.reject_inline_credentials(raw)
      parsed = urllib.parse.urlsplit(raw)
      scheme = parsed.scheme.lower()
      if scheme not in ('redis', 'rediss'):
        raise ValueError(
            f'unsupported Redis URI scheme {parsed.scheme!r}; '
            'use rediss://HOST:PORT (or redis://127.0.0.1:6379 for loopback)')
      scheme_ssl = (scheme == 'rediss')
      host = parsed.hostname or ''
      port = parsed.port or 6379
      if parsed.path and parsed.path.strip('/'):
        try:
          db = int(parsed.path.strip('/'))
        except ValueError:
          raise ValueError(
              f'invalid Redis DB path {parsed.path!r} in {raw!r}') from None
    else:
      if '@' in raw:
        raise ValueError(
            'inline credentials in Redis address are not allowed; '
            'set BEAM_REDIS_PASSWORD in the environment')
      if raw.startswith('['):
        # IPv6 `[::1]:6379`
        m6 = re.match(r'^(\[[^\]]+\])(?::(\d+))?$', raw)
        if not m6:
          raise ValueError(f'invalid Redis address {raw!r}')
        host = m6.group(1)
        port = int(m6.group(2)) if m6.group(2) else 6379
      elif ':' in raw:
        h_part, p_part = raw.rsplit(':', 1)
        host = h_part.strip()
        try:
          port = int(p_part.strip())
        except ValueError:
          raise ValueError(
              f'invalid Redis port {p_part!r} in {raw!r}') from None
      else:
        host = raw
        port = int(redis_port_opt) if redis_port_opt is not None else 6379
  else:
    host = str(redis_host_opt or '').strip()
    if '@' in host or '://' in host:
      raise ValueError(f'invalid redis_host {host!r}')
    port = int(redis_port_opt) if redis_port_opt is not None else 6379

  if not host:
    raise ValueError('Redis host cannot be empty')
  if not (1 <= port <= 65535):
    raise ValueError(f'Redis port out of range (1..65535): {port}')

  norm_host = host.strip('[]').lower()
  is_loopback = norm_host in ('localhost', '127.0.0.1', '::1')

  if redis_tls_opt is not None:
    explicit_tls = _parse_bool(redis_tls_opt, 'redis_tls')
    if scheme_ssl is True and not explicit_tls:
      raise ValueError('conflicting rediss:// URI with redis_tls=false')
    use_tls = explicit_tls
  elif scheme_ssl is not None:
    use_tls = scheme_ssl
  else:
    use_tls = not is_loopback

  if not is_loopback and not use_tls:
    raise ValueError(
        f'non-loopback Redis host {host!r} requires TLS encryption; '
        'use rediss://HOST:PORT or redis_tls=true')

  return host.strip('[]'), port, db, use_tls


def parse_enrich_spec_str(
    spec_str: str,
    *,
    extra_opts: Mapping[str, Any] | None = None,
    default_project: str | None = None,
    single_stage: bool = True,
) -> dict[str, Any]:
  """Parses CLI `--enrich HANDLER:k=v,...` shorthand into a YAML config dict."""
  if not isinstance(spec_str, str) or not spec_str.strip():
    raise ValueError('--enrich specification cannot be empty')
  s = spec_str.strip()
  if ':' not in s:
    raise ValueError(
        f'invalid --enrich specification {spec_str!r}; expected '
        'HANDLER:key=val,... (handlers: file, bigquery, bigtable, vertexai, feast)'
    )
  raw_handler, rest = s.split(':', 1)
  h_key = raw_handler.strip().lower().replace('-', '_')
  handler = _HANDLER_ALIASES.get(h_key)
  if handler is None:
    raise ValueError(
        f'unknown --enrich handler {raw_handler!r}; supported handlers are '
        'file, bigquery, bigtable, vertexai, and feast')

  kv: dict[str, Any] = {'handler': handler}
  if rest.strip():
    for tok in split_bracket_tokens(rest.strip()):
      if '=' not in tok:
        raise ValueError(
            f'expected key=value token in --enrich {raw_handler}:..., got {tok!r}'
        )
      k, v = tok.split('=', 1)
      key = k.strip().lower().replace('-', '_')
      val = v.strip()
      if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
        val = val[1:-1]
      if uris._SECRET_KEY_RE.search(key):
        raise ValueError(
            f'refusing inline secret option {key!r} in --enrich; '
            'use environment variables (e.g. BEAM_REDIS_PASSWORD) or ADC')
      kv[key] = val

  if extra_opts:
    allowed_for_handler = _COMMON_ENRICH_KEYS | _HANDLER_SPECIFIC_KEYS[handler]
    for ek, ev in extra_opts.items():
      norm_k = str(ek).strip().lower().replace('-', '_')
      if norm_k in _FORBIDDEN_CALLABLE_KEYS:
        raise ValueError(
            f'option {ek!r} is disallowed in CliEnrich for security')
      if norm_k not in allowed_for_handler:
        if single_stage:
          raise ValueError(
              f'unknown option {ek!r} for --enrich handler {handler!r}')
        continue
      kv.setdefault(norm_k, ev)

  # Validate eagerly and normalize to clean portable YAML config.
  parsed = parse_enrich_config(kv, default_project=default_project)
  return enrich_config_to_yaml_dict(parsed)


def parse_enrich_config(
    raw_cfg: Mapping[str, Any],
    *,
    default_project: str | None = None,
) -> EnrichConfig:
  """Validates a `CliEnrich` config mapping and returns `EnrichConfig`."""
  if not isinstance(raw_cfg, Mapping):
    raise ValueError('CliEnrich config must be a mapping')

  norm_cfg: dict[str, Any] = {}
  for k, v in raw_cfg.items():
    nk = str(k).strip().lower().replace('-', '_')
    if nk in _FORBIDDEN_CALLABLE_KEYS:
      raise ValueError(
          f'CliEnrich forbids {k!r} to prevent code/SQL injection; '
          'use declarative fields/columns instead')
    if nk not in ('redis_password_env',
                  'redis_password') and (uris._SECRET_KEY_RE.search(nk)):
      raise ValueError(
          f'CliEnrich forbids inline secret key {k!r}; '
          'configure credentials via environment variables or ADC')
    norm_cfg[nk] = v

  raw_h = str(
      norm_cfg.get('handler') or norm_cfg.get('enrichment_handler') or
      norm_cfg.get('provider') or '').strip()
  if not raw_h:
    raise ValueError(
        'CliEnrich requires handler: one of file, bigquery, bigtable, '
        'vertexai, or feast')
  handler = _HANDLER_ALIASES.get(raw_h.lower().replace('-', '_'))
  if handler is None:
    raise ValueError(
        f'unknown CliEnrich handler {raw_h!r}; expected file, bigquery, '
        'bigtable, vertexai, or feast')

  allowed_keys = _COMMON_ENRICH_KEYS | _HANDLER_SPECIFIC_KEYS[handler]
  unknown = sorted(set(norm_cfg) - allowed_keys)
  if unknown:
    raise ValueError(
        f'unknown option(s) for CliEnrich ({handler}): {", ".join(unknown)}')

  # Input join fields.
  raw_key = (
      norm_cfg.get('fields') or norm_cfg.get('row_key') or
      norm_cfg.get('key') or norm_cfg.get('entity_id'))
  fields = _validate_ident_list(
      parse_column_list(raw_key), 'CliEnrich key/fields')
  if not fields:
    raise ValueError(
        f'CliEnrich ({handler}) requires key/row_key/fields specifying the '
        'input join column(s)')

  raw_lookup = (
      norm_cfg.get('lookup_key') or norm_cfg.get('right_key') or
      norm_cfg.get('table_fields'))
  lookup_fields = (
      _validate_ident_list(parse_column_list(raw_lookup), 'lookup_key')
      if raw_lookup is not None else fields)
  if len(lookup_fields) != len(fields):
    raise ValueError(
        f'lookup_key count ({len(lookup_fields)}) must match input key count '
        f'({len(fields)})')

  prefix = str(norm_cfg.get('prefix') or '').strip()
  if prefix and not output.SAFE_IDENTIFIER_RE.match(prefix.rstrip('_') or 'p'):
    raise ValueError(f'invalid column prefix {prefix!r}')
  if prefix and not re.match(r'^[A-Za-z_][A-Za-z0-9_]{0,31}$', prefix):
    raise ValueError(f'invalid column prefix {prefix!r}')

  on_miss = str(norm_cfg.get('on_miss') or 'null').strip().lower()
  if on_miss not in ('null', 'dlq', 'drop', 'fail'):
    raise ValueError(
        f'invalid on_miss={on_miss!r}; expected null, dlq, drop, or fail')

  timeout = (
      _parse_positive_float(norm_cfg['timeout'], 'timeout')
      if norm_cfg.get('timeout') is not None else 30.0)

  default_max_batch = (
      DEFAULT_BQ_BATCH_SIZE if handler in ('bigquery', 'file') else 1)
  min_batch = (
      _parse_positive_int(norm_cfg['min_batch_size'], 'min_batch_size')
      if norm_cfg.get('min_batch_size') is not None else 1)
  max_batch = (
      _parse_positive_int(norm_cfg['max_batch_size'], 'max_batch_size')
      if norm_cfg.get('max_batch_size') is not None else max(
          min_batch, default_max_batch))
  if min_batch > max_batch:
    raise ValueError(
        f'min_batch_size ({min_batch}) cannot exceed max_batch_size ({max_batch})'
    )
  if handler == 'bigquery' and max_batch > MAX_BQ_BATCH_SIZE:
    raise ValueError(
        f'BigQuery CliEnrich max_batch_size ({max_batch}) exceeds FinOps/parameter '
        f'cap ({MAX_BQ_BATCH_SIZE})')

  max_batch_dur = (
      _parse_positive_float(
          norm_cfg['max_batch_duration_secs'], 'max_batch_duration_secs')
      if norm_cfg.get('max_batch_duration_secs') is not None else None)
  rate_limit = (
      _parse_positive_float(norm_cfg['rate_limit'], 'rate_limit')
      if norm_cfg.get('rate_limit') is not None else None)

  r_host, r_port, r_db, r_tls = _parse_redis_endpoint(
      norm_cfg.get('redis'),
      redis_host_opt=norm_cfg.get('redis_host'),
      redis_port_opt=norm_cfg.get('redis_port'),
      redis_db_opt=norm_cfg.get('redis_db'),
      redis_tls_opt=norm_cfg.get('redis_tls'))
  r_ttl = (
      int(_parse_positive_float(norm_cfg['redis_ttl'], 'redis_ttl'))
      if norm_cfg.get('redis_ttl') is not None else 3600)
  r_pass_env = str(norm_cfg.get('redis_password_env') or
                   'BEAM_REDIS_PASSWORD').strip()
  if not re.match(r'^[A-Za-z_][A-Za-z0-9_]{1,63}$', r_pass_env):
    raise ValueError(f'invalid redis_password_env {r_pass_env!r}')
  raw_r_pass = norm_cfg.get('redis_password')
  r_pass = (
      str(raw_r_pass) if raw_r_pass is not None and
      str(raw_r_pass) != '<REDACTED_SECRET>' else None)

  redact_cols = _validate_ident_list(
      parse_column_list(norm_cfg.get('dlq_redact_fields')), 'dlq_redact_fields')
  trust_providers = _parse_bool(
      norm_cfg.get('trust_providers', False), 'trust_providers')

  full_feat_names = (
      _parse_bool(
          norm_cfg.get('full_feature_names', False), 'full_feature_names')
      if handler == 'feast' else False)
  raw_col_spec = (
      norm_cfg.get('columns') or norm_cfg.get('column_names') or
      norm_cfg.get('feature_names') or norm_cfg.get('features') or
      norm_cfg.get('feature_ids'))
  cols, col_types, col_sources, clean_raw_specs, explicit_types = (
      _parse_typed_columns(
          raw_col_spec, handler=handler, full_feature_names=full_feat_names))

  handler_params: dict[str, Any] = {}
  if handler == 'file':
    fpath = str(
        norm_cfg.get('path') or norm_cfg.get('uri') or norm_cfg.get('file') or
        '').strip()
    if not fpath:
      raise ValueError('CliEnrich (file) requires path=<CSV|JSONL|PARQUET>')
    uris.reject_inline_credentials(fpath)
    max_bytes = (
        _parse_positive_int(norm_cfg['max_file_bytes'], 'max_file_bytes') if
        norm_cfg.get('max_file_bytes') is not None else DEFAULT_FILE_MAX_BYTES)
    handler_params = {'path': fpath, 'max_file_bytes': max_bytes}

  elif handler == 'bigquery':
    raw_table = str(norm_cfg.get('table') or norm_cfg.get('table_name') or
                    '').strip()
    if raw_table.lower().startswith('bigquery://'):
      raw_table = raw_table[len('bigquery://'):]
    if not raw_table:
      raise ValueError(
          'CliEnrich (bigquery) requires table=PROJECT.DATASET.TABLE')
    m_tbl = uris._BQ_TABLE_ID_RE.match(raw_table)
    if not m_tbl:
      raise ValueError(
          f'invalid BigQuery table identifier {raw_table!r}; expected '
          'PROJECT.DATASET.TABLE')
    tbl_proj = m_tbl.group('project')
    raw_proj = str(
        norm_cfg.get('project') or norm_cfg.get('project_id') or tbl_proj or
        default_project or '').strip()
    if not raw_proj:
      raise ValueError(
          'CliEnrich (bigquery) requires project=PROJECT_ID (or fully qualified '
          'table=PROJECT.DATASET.TABLE)')
    proj = uris.validate_bq_project_id(raw_proj)
    eff_tbl_proj = uris.validate_bq_project_id(tbl_proj) if tbl_proj else proj
    dataset = m_tbl.group('dataset')
    table_id = m_tbl.group('table')
    fq_table = f'{eff_tbl_proj}.{dataset}.{table_id}'
    restriction = uris.validate_bq_row_restriction(
        str(norm_cfg.get('row_restriction') or ''))
    handler_params = {
        'project': proj,
        'table': fq_table,
    }
    if restriction:
      handler_params['row_restriction'] = restriction

  elif handler == 'bigtable':
    if len(fields) != 1:
      raise ValueError(
          'CliEnrich (bigtable) supports a single row_key column; '
          'use --pre-select to concatenate composite keys first')
    raw_proj = str(
        norm_cfg.get('project') or norm_cfg.get('project_id') or
        default_project or '').strip()
    if not raw_proj:
      raise ValueError('CliEnrich (bigtable) requires project=PROJECT_ID')
    proj = uris.validate_bq_project_id(raw_proj)
    inst = str(norm_cfg.get('instance') or norm_cfg.get('instance_id') or
               '').strip()
    tbl = str(norm_cfg.get('table') or norm_cfg.get('table_id') or '').strip()
    if not inst or not _RESOURCE_SLUG_RE.match(inst):
      raise ValueError(
          f'CliEnrich (bigtable) requires valid instance=<ID>, got {inst!r}')
    if not tbl or not _RESOURCE_SLUG_RE.match(tbl):
      raise ValueError(
          f'CliEnrich (bigtable) requires valid table=<ID>, got {tbl!r}')
    app_prof = norm_cfg.get('app_profile') or norm_cfg.get('app_profile_id')
    if app_prof is not None and not _RESOURCE_SLUG_RE.match(str(app_prof)):
      raise ValueError(f'invalid Bigtable app_profile_id {app_prof!r}')
    enc = str(norm_cfg.get('encoding') or 'utf-8').strip()
    cf = norm_cfg.get('column_family')
    if cf is not None and not _RESOURCE_SLUG_RE.match(str(cf)):
      raise ValueError(f'invalid Bigtable column_family {cf!r}')
    handler_params = {
        'project_id': proj,
        'instance_id': inst,
        'table_id': tbl,
        'encoding': enc,
    }
    if app_prof:
      handler_params['app_profile_id'] = str(app_prof)
    if cf:
      handler_params['column_family'] = str(cf)

  elif handler == 'vertexai':
    if len(fields) != 1:
      raise ValueError(
          'CliEnrich (vertexai) supports a single row_key/entity_id column')
    raw_proj = str(
        norm_cfg.get('project') or norm_cfg.get('project_id') or
        default_project or '').strip()
    if not raw_proj:
      raise ValueError('CliEnrich (vertexai) requires project=PROJECT_ID')
    proj = uris.validate_bq_project_id(raw_proj)
    loc = str(norm_cfg.get('location') or norm_cfg.get('region') or '').strip()
    if not loc or not _GCP_REGION_RE.match(loc):
      raise ValueError(
          f'CliEnrich (vertexai) requires valid GCP location (e.g. us-central1), got {loc!r}'
      )
    endpoint = str(
        norm_cfg.get('api_endpoint') or
        f'{loc}-aiplatform.googleapis.com').strip()
    m_ep = _VERTEX_ENDPOINT_RE.match(endpoint)
    if not m_ep:
      raise ValueError(
          f'unsafe Vertex AI api_endpoint {endpoint!r}; must match '
          '<region>-aiplatform.googleapis.com')
    if m_ep.group('loc') and m_ep.group('loc') != loc:
      raise ValueError(
          f'Vertex AI api_endpoint region {m_ep.group("loc")!r} does not match '
          f'location={loc!r}')
    fs_name = str(
        norm_cfg.get('feature_store') or norm_cfg.get('feature_store_name') or
        '').strip()
    fv_name = str(
        norm_cfg.get('feature_view') or norm_cfg.get('feature_view_name') or
        '').strip()
    if not fs_name or not _RESOURCE_SLUG_RE.match(fs_name):
      raise ValueError(
          'CliEnrich (vertexai) requires valid feature_store=<NAME>')
    if not fv_name or not _RESOURCE_SLUG_RE.match(fv_name):
      raise ValueError(
          'CliEnrich (vertexai) requires valid feature_view=<NAME>')
    handler_params = {
        'project': proj,
        'location': loc,
        'api_endpoint': endpoint,
        'feature_store_name': fs_name,
        'feature_view_name': fv_name,
    }

  elif handler == 'feast':
    if len(fields) != 1:
      raise ValueError('CliEnrich (feast) supports a single entity_id column')
    fs_yaml = str(
        norm_cfg.get('feature_store_yaml_path') or norm_cfg.get('yaml_path') or
        norm_cfg.get('path') or '').strip()
    if not fs_yaml:
      raise ValueError('CliEnrich (feast) requires path=<feature_store.yaml>')
    uris.reject_inline_credentials(fs_yaml)
    f_svc = str(
        norm_cfg.get('feature_service_name') or
        norm_cfg.get('feature_service') or '').strip()
    has_f_names = bool(clean_raw_specs)
    if has_f_names == bool(f_svc):
      raise ValueError(
          'CliEnrich (feast) requires exactly one of features=[...] or '
          'feature_service=<NAME>')
    if f_svc and not _RESOURCE_SLUG_RE.match(f_svc):
      raise ValueError(f'invalid Feast feature_service {f_svc!r}')
    handler_params = {
        'feature_store_yaml_path': fs_yaml,
        'feature_names': clean_raw_specs if has_f_names else None,
        'feature_service_name': f_svc,
        'full_feature_names': full_feat_names,
    }

  return EnrichConfig(
      handler=handler,
      fields=fields,
      lookup_fields=lookup_fields,
      columns=cols,
      column_types=col_types,
      column_sources=col_sources,
      prefix=prefix,
      on_miss=on_miss,
      timeout=timeout,
      min_batch_size=min_batch,
      max_batch_size=max_batch,
      max_batch_duration_secs=max_batch_dur,
      rate_limit=rate_limit,
      redis_host=r_host,
      redis_port=r_port,
      redis_db=r_db,
      redis_tls=r_tls,
      redis_ttl=r_ttl,
      redis_password_env=r_pass_env,
      dlq_redact_fields=redact_cols,
      trust_providers=trust_providers,
      handler_params=handler_params,
      explicit_types=explicit_types,
      redis_password=r_pass,
  )


def enrich_config_to_yaml_dict(cfg: EnrichConfig) -> dict[str, Any]:
  """Serializes `EnrichConfig` into a clean portable Beam YAML `config:` dict."""
  out: dict[str, Any] = {'handler': cfg.handler}
  if len(cfg.fields) == 1:
    out['row_key'] = cfg.fields[0]
  else:
    out['fields'] = list(cfg.fields)
  if cfg.lookup_fields != cfg.fields:
    out['lookup_key'] = list(cfg.lookup_fields)

  for k, v in cfg.handler_params.items():
    if v is not None and v != '' and v is not False:
      if cfg.handler == 'feast' and k == 'feature_names':
        continue
      if cfg.handler == 'file' and k == 'max_file_bytes' and (
          v == DEFAULT_FILE_MAX_BYTES):
        continue
      out[k] = v

  if cfg.columns:
    col_specs: list[str] = []
    for c in cfg.columns:
      raw_src = cfg.column_sources.get(c, c)
      dtype = cfg.column_types.get(c, 'str')
      use_type = (c in cfg.explicit_types) or (dtype != 'str')
      col_specs.append(f'{raw_src}:{dtype}' if use_type else raw_src)
    key_name = 'feature_names' if cfg.handler == 'feast' else 'columns'
    out[key_name] = col_specs

  if cfg.prefix:
    out['prefix'] = cfg.prefix
  if cfg.on_miss != 'null':
    out['on_miss'] = cfg.on_miss
  if cfg.timeout != 30.0:
    out['timeout'] = cfg.timeout
  default_max = DEFAULT_BQ_BATCH_SIZE if cfg.handler in (
      'bigquery', 'file') else 1
  if cfg.min_batch_size != 1:
    out['min_batch_size'] = cfg.min_batch_size
  if cfg.max_batch_size != default_max:
    out['max_batch_size'] = cfg.max_batch_size
  if cfg.max_batch_duration_secs is not None:
    out['max_batch_duration_secs'] = cfg.max_batch_duration_secs
  if cfg.rate_limit is not None:
    out['rate_limit'] = cfg.rate_limit
  if cfg.redis_host and cfg.redis_port:
    scheme = 'rediss' if cfg.redis_tls else 'redis'
    db_suffix = f'/{cfg.redis_db}' if cfg.redis_db else ''
    out['redis'] = f'{scheme}://{cfg.redis_host}:{cfg.redis_port}{db_suffix}'
    if cfg.redis_ttl != 3600:
      out['redis_ttl'] = cfg.redis_ttl
    if cfg.redis_password_env != 'BEAM_REDIS_PASSWORD':
      out['redis_password_env'] = cfg.redis_password_env
    if cfg.redis_password is not None:
      out['redis_password'] = cfg.redis_password
  if cfg.dlq_redact_fields:
    out['dlq_redact_fields'] = list(cfg.dlq_redact_fields)
  if cfg.trust_providers:
    out['trust_providers'] = True
  return out


def _coerce_typed_value(val: Any, dtype: str | None) -> Any:
  if val is None or dtype is None:
    return val
  if dtype == 'str':
    if isinstance(val, str):
      return val
    if isinstance(val, (dict, list)):
      return json.dumps(val, default=str, sort_keys=True)
    return str(val)
  if dtype == 'int':
    if isinstance(val, bool):
      return int(val)
    return int(val)
  if dtype == 'float':
    return float(val)
  if dtype == 'bool':
    if isinstance(val, bool):
      return val
    return _parse_bool(val, 'enriched bool field')
  return val


def _sanitize_enrich_error(exc: Exception, handler: str) -> str:
  raw = f'{type(exc).__name__}: {exc}'
  scrubbed = output.redact_inline_secrets(raw, placeholder='<REDACTED_SECRET>')
  scrubbed = output._LONG_QUOTED_RE.sub("'<REDACTED>'", scrubbed)
  # Strip any embedded SQL query fragments from upstream BQ exceptions.
  scrubbed = re.sub(
      r'(?is)\bSELECT\b.+\bFROM\b.+', 'SELECT <redacted_query>', scrubbed)
  return f'CliEnrich ({handler}) {scrubbed}'[:1024]


def _make_enrich_dlq_row(
    step: str,
    err_type: str,
    msg: str,
    elem: Any,
    redact_fields: tuple[str, ...] = (),
) -> beam.Row:
  try:
    plain = interval_join._to_plain_dict(elem)
    if isinstance(plain, dict) and redact_fields:
      plain = {
          k: ('<REDACTED>' if k in redact_fields else v)
          for k, v in plain.items()
      }
    raw_json = json.dumps(plain, default=str, sort_keys=True)
  except Exception:  # pylint: disable=broad-except
    raw_json = '<unserializable_row>'
  return beam.Row(
      step=step,
      error_type=err_type,
      error=msg[:4096],
      raw_payload=raw_json,
      element=raw_json,
  )


def _infer_local_file_schema(
    cfg: EnrichConfig) -> tuple[tuple[str, ...], dict[str, str]] | None:
  """Sniffs local `file:` lookup columns & types in `<5ms` when file exists."""
  if cfg.handler != 'file':
    return None
  path = str(cfg.handler_params.get('path') or '')
  if not path or '://' in path or not os.path.isfile(path):
    return None
  fmt = uris.EXTENSIONS.get(
      uris.extension_of(path)) or uris.sniff_file_format(path)
  lookup_set = set(cfg.lookup_fields)
  requested = list(cfg.columns) if cfg.columns else None
  try:
    if fmt == 'parquet':
      import pyarrow as pa
      import pyarrow.parquet as pq
      schema = pq.read_schema(path)
      cols: list[str] = []
      types: dict[str, str] = {}
      for field in schema:
        if requested is not None:
          if field.name not in requested:
            continue
        elif field.name in lookup_set:
          continue
        if not output.SAFE_IDENTIFIER_RE.match(field.name):
          continue
        cols.append(field.name)
        if field.name in cfg.column_types and (
            f'{field.name}:{cfg.column_types[field.name]}' != field.name):
          # Preserve explicit `:type` annotation if user provided one.
          pass
        if pa.types.is_boolean(field.type):
          inferred = 'bool'
        elif pa.types.is_integer(field.type):
          inferred = 'int'
        elif pa.types.is_floating(field.type):
          inferred = 'float'
        else:
          inferred = 'str'
        types[field.name] = inferred
      if requested is not None:
        cols = [c for c in requested if c in types or c in cfg.column_types]
        for c in cols:
          if c in cfg.column_sources and ':' in str(
              raw := c):  # pragma: no cover
            _ = raw
      return (tuple(cols), types) if cols else None

    if fmt in ('json', 'jsonl'):
      cols = []
      types = {}
      with dlq._open_auto(path) as f:
        for _ in range(100):
          line = f.readline()
          if not line:
            break
          s = line.decode('utf-8', errors='replace').strip()
          if not s:
            continue
          obj = json.loads(s)
          if not isinstance(obj, dict):
            continue
          for k, v in obj.items():
            if requested is not None:
              if k not in requested:
                continue
            elif k in lookup_set:
              continue
            if not output.SAFE_IDENTIFIER_RE.match(str(k)):
              continue
            if k not in cols:
              cols.append(k)
            if v is None:
              continue
            v_t = (
                'bool' if isinstance(v, bool) else 'int' if isinstance(v, int)
                else 'float' if isinstance(v, float) else 'str')
            prev = types.get(k)
            if prev is None:
              types[k] = v_t
            elif prev != v_t:
              types[k] = ('float' if {prev, v_t} == {'int', 'float'} else 'str')
      for c in cols:
        types.setdefault(c, 'str')
      if requested is not None:
        cols = [c for c in requested]
        for c in cols:
          types.setdefault(c, cfg.column_types.get(c, 'str'))
      return (tuple(cols), types) if cols else None

    if fmt == 'csv':
      with dlq._open_auto(path) as bf:
        text_lines = [
            bf.readline().decode('utf-8-sig', errors='replace')
            for _ in range(101)
        ]
      non_empty = [ln for ln in text_lines if ln]
      if not non_empty:
        return None
      rdr = csv.DictReader(non_empty)
      if not rdr.fieldnames:
        return None
      cols = [
          c for c in rdr.fieldnames
          if output.SAFE_IDENTIFIER_RE.match(str(c)) and
          (c in requested if requested is not None else c not in lookup_set)
      ]
      types = {}
      for row in rdr:
        for c in cols:
          cell = (row.get(c) or '').strip()
          if not cell:
            continue
          low = cell.lower()
          if low in ('true', 'false'):
            ct = 'bool'
          elif dlq._is_int_cell(cell):
            ct = 'int'
          elif dlq._is_float_cell(cell):
            ct = 'float'
          else:
            ct = 'str'
          prev = types.get(c)
          if prev is None:
            types[c] = ct
          elif prev != ct:
            types[c] = 'float' if {prev, ct} == {'int', 'float'} else 'str'
      for c in cols:
        types.setdefault(c, 'str')
      if requested is not None:
        cols = list(requested)
        for c in cols:
          types.setdefault(c, cfg.column_types.get(c, 'str'))
      return (tuple(cols), types) if cols else None
  except Exception:  # pylint: disable=broad-except
    return None
  return None


def _coerce_csv_cell(raw_val: str | None) -> Any:
  if raw_val is None:
    return None
  s = raw_val.strip()
  if s == '':
    return None
  low = s.lower()
  if low == 'true':
    return True
  if low == 'false':
    return False
  if dlq._is_int_cell(s):
    return int(s)
  if dlq._is_float_cell(s):
    return float(s)
  return s


class _FileEnrichmentCaller:
  """Deterministic in-memory lookup caller for `file:` (`csv`/`jsonl`/`parquet`)."""
  def __init__(self, cfg: EnrichConfig):
    self._cfg = cfg
    self._index: dict[bytes, dict[str, Any]] = {}
    self.discovered_columns: tuple[str, ...] = cfg.columns

  def __enter__(self):
    path = str(self._cfg.handler_params['path'])
    max_bytes = int(
        self._cfg.handler_params.get('max_file_bytes', DEFAULT_FILE_MAX_BYTES))
    matched = dlq._match_paths(path)
    if not matched:
      raise FileNotFoundError(
          f'CliEnrich (file): lookup path not found or empty: {path!r}')
    total_size = 0
    for mp in matched:
      if '://' not in mp:
        if not os.path.isfile(mp):
          raise ValueError(
              f'CliEnrich (file): lookup path {mp!r} is not a regular file')
        total_size += os.path.getsize(mp)
    if total_size > max_bytes:
      raise ValueError(
          f'CliEnrich (file): lookup table {path!r} ({total_size:,} bytes) '
          f'exceeds max_file_bytes ({max_bytes:,}); use bigquery:/bigtable: '
          'or --interval-join for large tables')

    lookup_fields = self._cfg.lookup_fields
    lookup_set = set(lookup_fields)
    req_cols = self._cfg.columns
    seen_cols: list[str] = list(req_cols)
    index: dict[bytes, dict[str, Any]] = {}

    for mp in matched:
      fmt = uris.EXTENSIONS.get(
          uris.extension_of(mp)) or uris.sniff_file_format(mp)
      if fmt == 'parquet':
        import pyarrow.parquet as pq
        with dlq._open_auto(mp) as pf:
          tbl = pq.read_table(pf)
        rows = tbl.to_pylist()
      elif fmt in ('json', 'jsonl'):
        rows = []
        with dlq._open_auto(mp) as jf:
          raw_bytes = jf.read(max_bytes + 1)
        if len(raw_bytes) > max_bytes:
          raise ValueError(
              f'CliEnrich (file): lookup file {mp!r} exceeds {max_bytes:,} bytes'
          )
        text = raw_bytes.decode('utf-8')
        stripped = text.lstrip()
        if stripped.startswith('['):
          loaded = json.loads(stripped)
          if not isinstance(loaded, list):
            raise ValueError(f'expected JSON array or JSONL in {mp!r}')
          rows = loaded
        else:
          for ln in text.splitlines():
            if ln.strip():
              rows.append(json.loads(ln))
      else:
        rows = []
        with dlq._open_auto(mp) as cf:
          raw_bytes = cf.read(max_bytes + 1)
        if len(raw_bytes) > max_bytes:
          raise ValueError(
              f'CliEnrich (file): lookup file {mp!r} exceeds {max_bytes:,} bytes'
          )
        rdr = csv.DictReader(raw_bytes.decode('utf-8-sig').splitlines())
        for r in rdr:
          rows.append({k: _coerce_csv_cell(v) for k, v in r.items() if k})

      for r in rows:
        if not isinstance(r, dict):
          continue
        if any(lf not in r for lf in lookup_fields):
          raise KeyError(
              f'lookup key {list(lookup_fields)} not found in {mp!r}')
        k_vals = [r[lf] for lf in lookup_fields]
        if any(v is None for v in k_vals):
          continue
        ckey = encode_canonical_key_values(k_vals)
        if req_cols:
          payload = {c: r.get(c) for c in req_cols}
        else:
          payload = {
              k: v
              for k, v in r.items() if k not in lookup_set and
              output.SAFE_IDENTIFIER_RE.match(str(k))
          }
          for k in payload:
            if k not in seen_cols:
              seen_cols.append(k)
        # First match wins (consistent with SQL lookup / dimension tables).
        index.setdefault(ckey, payload)

    self._index = index
    self.discovered_columns = tuple(seen_cols)
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    self._index = {}
    return None

  def lookup_batch(
      self, requests: list[tuple[bytes, tuple[Any, ...], beam.Row]]
  ) -> dict[bytes, dict[str, Any] | None]:
    return {ckey: self._index.get(ckey) for ckey, _, _ in requests}


def _bq_scalar_type(val: Any) -> tuple[str, Any]:
  if isinstance(val, bool):
    return 'BOOL', val
  if isinstance(val, int):
    return 'INT64', val
  if isinstance(val, float):
    return 'FLOAT64', val
  if isinstance(val, decimal.Decimal):
    return 'NUMERIC', val
  if isinstance(val, (bytes, bytearray)):
    return 'BYTES', bytes(val)
  return 'STRING', str(val)


class _ParameterizedBigQueryCaller:
  """SQL-injection-proof BigQuery batch caller using `QueryJobConfig`."""
  def __init__(self, cfg: EnrichConfig, client_factory=None):
    self._cfg = cfg
    self._client_factory = client_factory
    self._client = None
    self.discovered_columns: tuple[str, ...] = cfg.columns

  def __enter__(self):
    if self._client_factory is not None:
      self._client = self._client_factory(self._cfg.handler_params['project'])
    else:
      from google.cloud import bigquery
      self._client = bigquery.Client(
          project=self._cfg.handler_params['project'])
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    if self._client is not None and hasattr(self._client, 'close'):
      try:
        self._client.close()
      except Exception:  # pylint: disable=broad-except
        pass
    self._client = None
    return None

  def _build_query_and_params(
      self, key_tuples: list[tuple[Any,
                                   ...]]) -> tuple[str, list[Any], set[str]]:
    from google.cloud import bigquery

    hp = self._cfg.handler_params
    fq_table = hp['table']
    lookup_fields = self._cfg.lookup_fields
    req_cols = self._cfg.columns

    stripped_join_cols: set[str] = set()
    if req_cols:
      select_cols = list(req_cols)
      for lf in lookup_fields:
        if lf not in select_cols:
          select_cols.append(lf)
          stripped_join_cols.add(lf)
      select_sql = ', '.join(f'`{c}`' for c in select_sql_idents(select_cols))
    else:
      select_sql = '*'
      stripped_join_cols = set(lookup_fields)

    params: list[Any] = []
    if len(lookup_fields) == 1:
      col = lookup_fields[0]
      typed = [_bq_scalar_type(kt[0]) for kt in key_tuples]
      distinct_types = {t for t, _ in typed}
      if len(distinct_types) == 1:
        arr_type = next(iter(distinct_types))
        arr_vals = [v for _, v in typed]
      else:
        arr_type = 'STRING'
        arr_vals = [str(kt[0]) for kt in key_tuples]
      where_sql = f'`{col}` IN UNNEST(@keys)'
      params.append(bigquery.ArrayQueryParameter('keys', arr_type, arr_vals))
    else:
      disjuncts: list[str] = []
      for i, kt in enumerate(key_tuples):
        conjuncts: list[str] = []
        for j, (col, kv) in enumerate(zip(lookup_fields, kt)):
          ptype, pval = _bq_scalar_type(kv)
          pname = f'k_{i}_{j}'
          conjuncts.append(f'`{col}` = @{pname}')
          params.append(bigquery.ScalarQueryParameter(pname, ptype, pval))
        disjuncts.append('(' + ' AND '.join(conjuncts) + ')')
      where_sql = '(' + ' OR '.join(disjuncts) + ')'

    if hp.get('row_restriction'):
      where_sql = f'{where_sql} AND ({hp["row_restriction"]})'

    sql = f'SELECT {select_sql} FROM `{fq_table}` WHERE {where_sql}'
    return sql, params, stripped_join_cols

  def lookup_batch(
      self, requests: list[tuple[bytes, tuple[Any, ...], beam.Row]]
  ) -> dict[bytes, dict[str, Any] | None]:
    if not requests:
      return {}
    from google.cloud import bigquery

    key_tuples = [kt for _, kt, _ in requests]
    sql, params, stripped_join_cols = self._build_query_and_params(key_tuples)
    job_cfg = bigquery.QueryJobConfig(query_parameters=params)
    rows_iter = self._client.query(sql, job_config=job_cfg).result()

    lookup_fields = self._cfg.lookup_fields
    req_cols = self._cfg.columns
    found: dict[bytes, dict[str, Any]] = {}
    dyn_cols: list[str] = list(req_cols)
    for row in rows_iter:
      rdict = dict(row.items())
      k_vals = [rdict.get(lf) for lf in lookup_fields]
      if any(v is None for v in k_vals):
        continue
      ckey = encode_canonical_key_values(k_vals)
      if req_cols:
        payload = {c: rdict.get(c) for c in req_cols}
      else:
        payload = {
            k: v
            for k, v in rdict.items() if k not in stripped_join_cols and
            output.SAFE_IDENTIFIER_RE.match(str(k))
        }
        for k in payload:
          if k not in dyn_cols:
            dyn_cols.append(k)
      found.setdefault(ckey, payload)

    if dyn_cols and not self.discovered_columns:
      self.discovered_columns = tuple(dyn_cols)

    return {ckey: found.get(ckey) for ckey, _, _ in requests}


def select_sql_idents(cols: Iterable[str]) -> list[str]:
  out: list[str] = []
  for c in cols:
    if not output.SAFE_IDENTIFIER_RE.match(c):
      raise ValueError(f'unsafe SQL identifier {c!r}')
    out.append(c)
  return out


def _flatten_bigtable_response(raw_dict: Mapping[str, Any],
                               cfg: EnrichConfig) -> dict[str, Any]:
  if not raw_dict:
    return {}
  default_cf = cfg.handler_params.get('column_family')
  if cfg.columns:
    out: dict[str, Any] = {}
    for col in cfg.columns:
      src = cfg.column_sources.get(col, col)
      if ':' in src:
        cf_name, qual = src.split(':', 1)
        cf_map = raw_dict.get(cf_name)
        out[col] = cf_map.get(qual) if isinstance(cf_map, Mapping) else None
      elif col in raw_dict and not isinstance(raw_dict[col], Mapping):
        out[col] = raw_dict[col]
      elif default_cf and isinstance(raw_dict.get(default_cf), Mapping):
        out[col] = raw_dict[default_cf].get(src)
      else:
        val = None
        for v in raw_dict.values():
          if isinstance(v, Mapping) and src in v:
            val = v[src]
            break
        out[col] = val
    return out

  # Dynamic flattening across column families.
  flat: dict[str, Any] = {}
  qual_Collision: set[str] = set()
  seen_quals: dict[str, int] = {}
  for cf_k, cf_v in raw_dict.items():
    if isinstance(cf_v, Mapping):
      for q in cf_v:
        seen_quals[str(q)] = seen_quals.get(str(q), 0) + 1
        if seen_quals[str(q)] > 1:
          qual_Collision.add(str(q))

  for cf_k, cf_v in raw_dict.items():
    if isinstance(cf_v, Mapping):
      safe_cf = re.sub(r'[^A-Za-z0-9_]', '_', str(cf_k))
      for q, val in cf_v.items():
        qs = str(q)
        out_name = f'{safe_cf}_{qs}' if qs in qual_Collision else qs
        if output.SAFE_IDENTIFIER_RE.match(out_name):
          flat[out_name] = val
    elif output.SAFE_IDENTIFIER_RE.match(str(cf_k)):
      flat[str(cf_k)] = cf_v
  return flat


class _BigTableCaller:
  """Lazy worker-side wrapper around `BigTableEnrichmentHandler`."""
  def __init__(self, cfg: EnrichConfig, handler_factory=None):
    self._cfg = cfg
    self._handler_factory = handler_factory
    self._handler = None
    self.discovered_columns: tuple[str, ...] = cfg.columns

  def __enter__(self):
    hp = self._cfg.handler_params
    if self._handler_factory is not None:
      self._handler = self._handler_factory(hp, self._cfg.fields[0])
    else:
      from apache_beam.transforms.enrichment_handlers.bigtable import BigTableEnrichmentHandler
      from apache_beam.transforms.enrichment_handlers.utils import ExceptionLevel
      self._handler = BigTableEnrichmentHandler(
          project_id=hp['project_id'],
          instance_id=hp['instance_id'],
          table_id=hp['table_id'],
          row_key=self._cfg.fields[0],
          app_profile_id=hp.get('app_profile_id'),
          encoding=hp.get('encoding', 'utf-8'),
          exception_level=ExceptionLevel.WARN)
    self._handler.__enter__()
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    if self._handler is not None:
      self._handler.__exit__(exc_type, exc_val, exc_tb)
      self._handler = None
    return None

  def lookup_batch(
      self, requests: list[tuple[bytes, tuple[Any, ...], beam.Row]]
  ) -> dict[bytes, dict[str, Any] | None]:
    results: dict[bytes, dict[str, Any] | None] = {}
    for ckey, _, row in requests:
      _, resp_row = self._handler(row)
      raw_dict = resp_row._asdict() if hasattr(resp_row, '_asdict') else dict(
          resp_row or {})
      if not raw_dict:
        results[ckey] = None
        continue
      flat = _flatten_bigtable_response(raw_dict, self._cfg)
      if not flat or (self._cfg.columns and all(v is None
                                                for v in flat.values())):
        results[ckey] = None
      else:
        if not self.discovered_columns and flat:
          self.discovered_columns = tuple(flat.keys())
        results[ckey] = flat
    return results


class _VertexFSCaller:
  """Lazy worker-side wrapper around `VertexAIFeatureStoreEnrichmentHandler`."""
  def __init__(self, cfg: EnrichConfig, handler_factory=None):
    self._cfg = cfg
    self._handler_factory = handler_factory
    self._handler = None
    self.discovered_columns: tuple[str, ...] = cfg.columns

  def __enter__(self):
    hp = self._cfg.handler_params
    if self._handler_factory is not None:
      self._handler = self._handler_factory(hp, self._cfg.fields[0])
    else:
      from apache_beam.transforms.enrichment_handlers.utils import ExceptionLevel
      from apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store import VertexAIFeatureStoreEnrichmentHandler
      # Never pass ExceptionLevel.QUIET (upstream UnboundLocalError at L181).
      self._handler = VertexAIFeatureStoreEnrichmentHandler(
          project=hp['project'],
          location=hp['location'],
          api_endpoint=hp['api_endpoint'],
          feature_store_name=hp['feature_store_name'],
          feature_view_name=hp['feature_view_name'],
          row_key=self._cfg.fields[0],
          exception_level=ExceptionLevel.WARN)
    self._handler.__enter__()
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    if self._handler is not None:
      self._handler.__exit__(exc_type, exc_val, exc_tb)
      self._handler = None
    return None

  def lookup_batch(
      self, requests: list[tuple[bytes, tuple[Any, ...], beam.Row]]
  ) -> dict[bytes, dict[str, Any] | None]:
    results: dict[bytes, dict[str, Any] | None] = {}
    req_cols = self._cfg.columns
    for ckey, _, row in requests:
      _, resp_row = self._handler(row)
      raw_dict = resp_row._asdict() if hasattr(resp_row, '_asdict') else dict(
          resp_row or {})
      if not raw_dict:
        results[ckey] = None
        continue
      payload = ({
          c: raw_dict.get(c)
          for c in req_cols
      } if req_cols else {
          k: v
          for k, v in raw_dict.items()
          if output.SAFE_IDENTIFIER_RE.match(str(k))
      })
      if not payload or all(v is None for v in payload.values()):
        results[ckey] = None
      else:
        if not self.discovered_columns:
          self.discovered_columns = tuple(payload.keys())
        results[ckey] = payload
    return results


def validate_feast_yaml_content(
    yaml_text: str, *, trust_providers: bool = False) -> dict[str, Any]:
  """Validates Feast `feature_store.yaml` safely before `FeatureStore()` load."""
  if len(yaml_text.encode('utf-8', errors='replace')) > MAX_FEAST_YAML_BYTES:
    raise ValueError(
        f'Feast feature_store.yaml exceeds {MAX_FEAST_YAML_BYTES:,} byte limit')
  doc = output.safe_yaml_load(yaml_text)
  if not isinstance(doc, dict):
    raise ValueError('Feast feature_store.yaml must be a YAML mapping')
  if not trust_providers:
    provider = str(doc.get('provider') or '').strip()
    if not provider or provider not in _SAFE_FEAST_PROVIDERS:
      raise ValueError(
          f'untrusted Feast provider {provider!r} in feature_store.yaml '
          f'(allowed: {sorted(_SAFE_FEAST_PROVIDERS)}; pass --trust-providers '
          'to allow custom Feast provider modules)')
    online_store = doc.get('online_store')
    os_type: str | None = None
    if isinstance(online_store, str):
      os_type = online_store.strip()
    elif isinstance(online_store, Mapping):
      os_type = str(online_store.get('type') or 'sqlite').strip()
    if os_type is not None and os_type not in _SAFE_FEAST_ONLINE_STORES:
      raise ValueError(
          f'untrusted Feast online_store.type {os_type!r} in feature_store.yaml '
          f'(allowed: {sorted(_SAFE_FEAST_ONLINE_STORES)}; pass '
          '--trust-providers to allow custom Feast online store classes)')
  return doc


class _FeastCaller:
  """Safe Feast caller with YAML allowlist, temp cleanup, and miss detection."""
  def __init__(self, cfg: EnrichConfig, store_factory=None):
    self._cfg = cfg
    self._store_factory = store_factory
    self._store = None
    self._features = None
    self._temp_yaml_path: str | None = None
    self.discovered_columns: tuple[str, ...] = cfg.columns

  def __enter__(self):
    hp = self._cfg.handler_params
    fs_yaml_path = hp['feature_store_yaml_path']
    with dlq._open_auto(fs_yaml_path) as f:
      raw_bytes = f.read(MAX_FEAST_YAML_BYTES + 1)
    if len(raw_bytes) > MAX_FEAST_YAML_BYTES:
      raise ValueError(
          f'Feast feature_store.yaml exceeds {MAX_FEAST_YAML_BYTES:,} bytes')
    yaml_text = raw_bytes.decode('utf-8')
    validate_feast_yaml_content(
        yaml_text, trust_providers=self._cfg.trust_providers)

    fd, tmp_path = tempfile.mkstemp(prefix='beam_cli_feast_', suffix='.yaml')
    try:
      os.chmod(tmp_path, 0o600)
      with os.fdopen(fd, 'w', encoding='utf-8') as tf:
        tf.write(yaml_text)
      self._temp_yaml_path = tmp_path
      if self._store_factory is not None:
        self._store = self._store_factory(tmp_path)
      else:
        from pathlib import Path
        from feast import FeatureStore
        self._store = FeatureStore(fs_yaml_file=Path(tmp_path))
      if hp.get('feature_service_name'):
        self._features = self._store.get_feature_service(
            hp['feature_service_name'])
      else:
        self._features = list(hp.get('feature_names') or ())
    except BaseException:
      self._cleanup_temp()
      raise
    return self

  def _cleanup_temp(self) -> None:
    if self._temp_yaml_path and os.path.exists(self._temp_yaml_path):
      try:
        os.unlink(self._temp_yaml_path)
      except OSError:
        pass
    self._temp_yaml_path = None

  def __exit__(self, exc_type, exc_val, exc_tb):
    self._store = None
    self._features = None
    self._cleanup_temp()
    return None

  def lookup_batch(
      self, requests: list[tuple[bytes, tuple[Any, ...], beam.Row]]
  ) -> dict[bytes, dict[str, Any] | None]:
    if not requests:
      return {}
    entity_col = self._cfg.fields[0]
    lookup_entity = self._cfg.lookup_fields[0]
    full_names = bool(self._cfg.handler_params.get('full_feature_names', False))
    entity_rows = [{lookup_entity: kt[0]} for _, kt, _ in requests]
    feat_dict = self._store.get_online_features(
        features=self._features,
        entity_rows=entity_rows,
        full_feature_names=full_names).to_dict()

    results: dict[bytes, dict[str, Any] | None] = {}
    req_cols = self._cfg.columns
    for idx, (ckey, _, _) in enumerate(requests):
      row_vals: dict[str, Any] = {}
      for k, vals in feat_dict.items():
        if k in (entity_col, lookup_entity) and k not in req_cols:
          continue
        if output.SAFE_IDENTIFIER_RE.match(str(k)):
          row_vals[str(k)] = vals[idx] if idx < len(vals) else None
      if req_cols:
        row_vals = {c: row_vals.get(c) for c in req_cols}
      # Feast returns all None feature values when an entity is missing.
      if not row_vals or all(v is None for v in row_vals.values()):
        results[ckey] = None
      else:
        if not self.discovered_columns:
          self.discovered_columns = tuple(row_vals.keys())
        results[ckey] = row_vals
    return results


class _SafeRedisCache:
  """JSON-serialized, SHA-256-keyed Redis cache with non-fatal degradation."""
  def __init__(self, cfg: EnrichConfig, stage_fingerprint: str, client=None):
    self._cfg = cfg
    self._prefix = f'beam_cli:enrich:{stage_fingerprint}:'
    self._ttl = cfg.redis_ttl
    self._injected_client = client
    self._client = None
    self._warned_unavailable = False

  def __enter__(self):
    if self._injected_client is not None:
      self._client = self._injected_client
      return self
    try:
      import redis
    except ImportError as e:
      raise ImportError(
          "Redis enrichment cache requires the 'redis' package "
          "(install with `pip install redis`).") from e
    pwd = (
        os.environ.get(self._cfg.redis_password_env) or
        self._cfg.redis_password or None)
    self._client = redis.Redis(
        host=self._cfg.redis_host,
        port=self._cfg.redis_port,
        db=self._cfg.redis_db,
        password=pwd,
        ssl=self._cfg.redis_tls,
        socket_connect_timeout=2.0,
        socket_timeout=2.0)
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    if self._client is not None and self._injected_client is None:
      try:
        self._client.close()
      except Exception:  # pylint: disable=broad-except
        pass
    self._client = None
    return None

  def _rkey(self, canon_key: bytes) -> str:
    return self._prefix + hashlib.sha256(canon_key).hexdigest()

  def get_many(self, canon_keys: list[bytes]) -> dict[bytes, dict[str, Any]]:
    if not canon_keys or self._client is None:
      return {}
    try:
      rkeys = [self._rkey(ck) for ck in canon_keys]
      raw_vals = self._client.mget(rkeys)
      hits: dict[bytes, dict[str, Any]] = {}
      for ck, raw in zip(canon_keys, raw_vals):
        if raw is None:
          continue
        text = raw.decode('utf-8') if isinstance(raw,
                                                 (bytes,
                                                  bytearray)) else str(raw)
        loaded = json.loads(text)
        if isinstance(loaded, dict):
          hits[ck] = loaded
      return hits
    except Exception as e:  # pylint: disable=broad-except
      if not self._warned_unavailable:
        self._warned_unavailable = True
        _LOGGER.warning(
            'CliEnrich Redis cache read unavailable (%s); '
            'degrading cleanly to direct source lookup.',
            type(e).__name__)
      return {}

  def put_many(self, items: Mapping[bytes, dict[str, Any]]) -> None:
    if not items or self._client is None:
      return
    try:
      pipe = self._client.pipeline(transaction=False)
      count = 0
      for ck, payload in items.items():
        if not payload:
          continue
        encoded = json.dumps(payload, default=str, allow_nan=False)
        pipe.setex(self._rkey(ck), self._ttl, encoded)
        count += 1
      if count:
        pipe.execute()
    except Exception as e:  # pylint: disable=broad-except
      if not self._warned_unavailable:
        self._warned_unavailable = True
        _LOGGER.warning(
            'CliEnrich Redis cache write unavailable (%s); continuing.',
            type(e).__name__)


def _is_retryable_rpc_error(exc: Exception) -> bool:
  if isinstance(exc, (TimeoutError, concurrent.futures.TimeoutError)):
    return True
  cls_name = type(exc).__name__
  return cls_name in (
      'TooManyRequests',
      'ServiceUnavailable',
      'DeadlineExceeded',
      'InternalServerError',
      'Aborted',
      'UserCodeQuotaException',
      'UserCodeTimeoutException',
  )


class _CliEnrichDoFn(beam.DoFn):
  """Bundle-buffered enrichment DoFn with singleton fallback & timestamp preservation."""
  def __init__(
      self,
      cfg: EnrichConfig,
      *,
      has_error_handling: bool = False,
      static_columns: tuple[str, ...] = (),
      static_column_types: Mapping[str, str] | None = None,
      caller_factory=None,
      redis_client=None,
  ):
    self._cfg = cfg
    self._has_error_handling = has_error_handling
    self._static_columns = static_columns
    self._static_col_types = dict(static_column_types or cfg.column_types)
    self._caller_factory = caller_factory
    self._injected_redis = redis_client

    fp_src = json.dumps({
        'h': cfg.handler,
        'p': cfg.handler_params,
        'l': cfg.lookup_fields,
        'c': cfg.columns,
    },
                        sort_keys=True)
    self._stage_fp = hashlib.sha256(fp_src.encode('utf-8')).hexdigest()[:16]

    self._hits_counter = Metrics.counter('beam_cli', 'enrich_hits')
    self._misses_counter = Metrics.counter('beam_cli', 'enrich_misses')
    self._misses_dropped_counter = Metrics.counter(
        'beam_cli', 'enrich_misses_dropped')
    self._errors_counter = Metrics.counter('beam_cli', 'enrich_errors')

    self._executor: concurrent.futures.ThreadPoolExecutor | None = None
    self._throttler: AdaptiveThrottler | None = None
    self._caller: Any = None
    self._cache: _SafeRedisCache | None = None
    self._buffer: list[tuple[beam.Row, Any, Any]] = []
    self._bundle_start_t: float = 0.0
    self._last_rpc_t: float = 0.0

  def setup(self):
    self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    self._throttler = AdaptiveThrottler(
        window_ms=120_000, bucket_ms=1000, overload_ratio=2.0)
    if self._caller_factory is not None:
      self._caller = self._caller_factory(self._cfg)
    elif self._cfg.handler == 'file':
      self._caller = _FileEnrichmentCaller(self._cfg)
    elif self._cfg.handler == 'bigquery':
      self._caller = _ParameterizedBigQueryCaller(self._cfg)
    elif self._cfg.handler == 'bigtable':
      self._caller = _BigTableCaller(self._cfg)
    elif self._cfg.handler == 'vertexai':
      self._caller = _VertexFSCaller(self._cfg)
    elif self._cfg.handler == 'feast':
      self._caller = _FeastCaller(self._cfg)
    else:  # pragma: no cover
      raise ValueError(f'unsupported handler {self._cfg.handler!r}')
    self._caller.__enter__()

    if self._cfg.redis_host and self._cfg.redis_port:
      self._cache = _SafeRedisCache(
          self._cfg, self._stage_fp, client=self._injected_redis)
      self._cache.__enter__()

  def teardown(self):
    if self._cache is not None:
      self._cache.__exit__(None, None, None)
      self._cache = None
    if self._caller is not None:
      self._caller.__exit__(None, None, None)
      self._caller = None
    if self._executor is not None:
      self._executor.shutdown(wait=False)
      self._executor = None

  def start_bundle(self):
    self._buffer = []
    self._bundle_start_t = time.monotonic()

  def process(
      self,
      element: beam.Row,
      ts=beam.DoFn.TimestampParam,
      win=beam.DoFn.WindowParam,
  ):
    self._buffer.append((element, ts, win))
    now = time.monotonic()
    dur_exceeded = (
        self._cfg.max_batch_duration_secs is not None and
        (now - self._bundle_start_t) >= self._cfg.max_batch_duration_secs)
    if len(self._buffer) >= self._cfg.max_batch_size or dur_exceeded:
      yield from self._flush_buffer()
      self._bundle_start_t = time.monotonic()

  def finish_bundle(self):
    if self._buffer:
      yield from self._flush_buffer()

  def _call_with_retry_and_timeout(
      self, req_list: list[tuple[bytes, tuple[Any, ...], beam.Row]]
  ) -> dict[bytes, dict[str, Any] | None]:
    assert self._executor is not None
    assert self._throttler is not None

    if self._cfg.rate_limit:
      min_interval = 1.0 / self._cfg.rate_limit
      wait_s = min_interval - (time.monotonic() - self._last_rpc_t)
      if wait_s > 0:
        time.sleep(min(wait_s, 2.0))

    throttle_deadline = time.monotonic() + min(self._cfg.timeout, 2.0)
    while self._throttler.throttle_request(int(time.time() * 1000)):
      if time.monotonic() >= throttle_deadline:
        break
      time.sleep(0.05)

    attempt = 0
    backoff = 0.05
    while True:
      req_ms = int(time.time() * 1000)
      self._last_rpc_t = time.monotonic()
      fut = self._executor.submit(self._caller.lookup_batch, req_list)
      try:
        res = fut.result(timeout=self._cfg.timeout)
        self._throttler.successful_request(req_ms)
        return res
      except Exception as exc:  # pylint: disable=broad-except
        if attempt < 2 and _is_retryable_rpc_error(exc):
          attempt += 1
          time.sleep(backoff)
          backoff *= 2.0
          continue
        raise

  def _join_row(
      self, left_row: beam.Row,
      right_dict: Mapping[str, Any] | None) -> beam.Row:
    merged = dict(left_row._asdict())
    prefix = self._cfg.prefix
    known_cols = (
        self._static_columns or getattr(self._caller, 'discovered_columns',
                                        ()) or self._cfg.columns)

    if right_dict is None:
      # `on_miss == 'null'` padding
      for c in known_cols:
        out_k = f'{prefix}{c}' if prefix else c
        if prefix or out_k not in merged:
          merged[out_k] = None
      return beam.Row(**merged)

    iter_cols = self._static_columns if self._static_columns else tuple(
        right_dict.keys())
    for c in iter_cols:
      raw_v = right_dict.get(c)
      coerced = _coerce_typed_value(raw_v, self._static_col_types.get(c))
      out_k = f'{prefix}{c}' if prefix else c
      if prefix or out_k not in merged:
        merged[out_k] = coerced
    return beam.Row(**merged)

  def _emit_error_or_raise(
      self,
      row: beam.Row,
      ts: Any,
      win: Any,
      err_type: str,
      msg: str,
      *,
      count_error: bool = True,
  ):
    if count_error:
      self._errors_counter.inc(1)
    if self._has_error_handling:
      dlq_row = _make_enrich_dlq_row(
          'CliEnrich',
          err_type,
          msg,
          row,
          redact_fields=self._cfg.dlq_redact_fields)
      return pvalue.TaggedOutput('errors', WindowedValue(dlq_row, ts, (win, )))
    raise RuntimeError(msg)

  def _flush_buffer(self):
    batch = self._buffer
    self._buffer = []
    if not batch:
      return

    # 1. Extract canonical lookup keys per buffered item.
    item_states: list[tuple[str, Any]] = [('', None)] * len(batch)
    unique_reqs: dict[bytes, tuple[bytes, tuple[Any, ...], beam.Row]] = {}
    for idx, (row, _ts, _win) in enumerate(batch):
      try:
        rdict = row._asdict()
      except AttributeError:
        rdict = dict(row) if isinstance(row, Mapping) else {}
      missing = [f for f in self._cfg.fields if f not in rdict]
      if missing:
        item_states[idx] = (
            'error',
            (
                'KeyError',
                f'CliEnrich ({self._cfg.handler}) missing join key column(s): '
                f'{missing}',
            ))
        continue
      k_vals = tuple(rdict[f] for f in self._cfg.fields)
      if any(v is None for v in k_vals):
        item_states[idx] = ('miss', b'null')
        continue
      ckey = encode_canonical_key_values(k_vals)
      item_states[idx] = ('lookup', ckey)
      if ckey not in unique_reqs:
        unique_reqs[ckey] = (ckey, k_vals, row)

    resolved: dict[bytes, dict[str, Any] | None] = {}
    key_errors: dict[bytes, tuple[str, str]] = {}

    # 2. Check Redis cache first for unique keys.
    pending_keys = list(unique_reqs.keys())
    if self._cache is not None and pending_keys:
      cache_hits = self._cache.get_many(pending_keys)
      for ck, payload in cache_hits.items():
        resolved[ck] = payload
      pending_keys = [ck for ck in pending_keys if ck not in resolved]

    # 3. Execute batch RPC for cache misses, with singleton fallback on error!
    if pending_keys:
      req_list = [unique_reqs[ck] for ck in pending_keys]
      fresh_hits_to_cache: dict[bytes, dict[str, Any]] = {}
      try:
        batch_out = self._call_with_retry_and_timeout(req_list)
        for ck in pending_keys:
          val = batch_out.get(ck)
          resolved[ck] = val
          if val is not None:
            fresh_hits_to_cache[ck] = val
      except Exception as batch_exc:  # pylint: disable=broad-except
        if len(req_list) == 1:
          only_ck = pending_keys[0]
          key_errors[only_ck] = (
              type(batch_exc).__name__,
              _sanitize_enrich_error(batch_exc, self._cfg.handler))
        else:
          # Singleton fallback: isolate poison element(s) without failing N-1 good rows!
          for single_req in req_list:
            s_ck = single_req[0]
            try:
              s_out = self._call_with_retry_and_timeout([single_req])
              s_val = s_out.get(s_ck)
              resolved[s_ck] = s_val
              if s_val is not None:
                fresh_hits_to_cache[s_ck] = s_val
            except Exception as single_exc:  # pylint: disable=broad-except
              key_errors[s_ck] = (
                  type(single_exc).__name__,
                  _sanitize_enrich_error(single_exc, self._cfg.handler))

      if self._cache is not None and fresh_hits_to_cache:
        self._cache.put_many(fresh_hits_to_cache)

    # 4. Emit hydrated rows / misses / DLQ records in input order with exact timestamps!
    for idx, (row, ts, win) in enumerate(batch):
      kind, info = item_states[idx]
      if kind == 'error':
        err_t, err_m = info
        yield self._emit_error_or_raise(row, ts, win, err_t, err_m)
        continue

      if kind == 'miss':
        ck_bytes = info
        is_miss = True
        payload = None
      else:
        ck_bytes = info
        if ck_bytes in key_errors:
          err_t, err_m = key_errors[ck_bytes]
          yield self._emit_error_or_raise(row, ts, win, err_t, err_m)
          continue
        payload = resolved.get(ck_bytes)
        is_miss = payload is None

      if not is_miss and payload is not None:
        self._hits_counter.inc(1)
        try:
          joined = self._join_row(row, payload)
        except Exception as coerce_exc:  # pylint: disable=broad-except
          yield self._emit_error_or_raise(
              row,
              ts,
              win,
              type(coerce_exc).__name__,
              _sanitize_enrich_error(coerce_exc, self._cfg.handler))
          continue
        yield WindowedValue(joined, ts, (win, ))
      else:
        self._misses_counter.inc(1)
        if self._cfg.on_miss == 'null':
          joined_null = self._join_row(row, None)
          yield WindowedValue(joined_null, ts, (win, ))
        elif self._cfg.on_miss == 'drop':
          self._misses_dropped_counter.inc(1)
        elif self._cfg.on_miss == 'dlq':
          k_hash = hashlib.sha256(ck_bytes).hexdigest()[:12]
          msg = (
              f'CliEnrich ({self._cfg.handler}) lookup miss on key '
              f'{list(self._cfg.fields)} (key_sha256={k_hash})')
          yield self._emit_error_or_raise(
              row, ts, win, 'EnrichmentMissError', msg, count_error=False)
        else:
          k_hash = hashlib.sha256(ck_bytes).hexdigest()[:12]
          raise RuntimeError(
              f'CliEnrich ({self._cfg.handler}) lookup miss on key '
              f'{list(self._cfg.fields)} (key_sha256={k_hash})')


def _infer_enriched_row_type(
    input_element_type: Any,
    static_cols: tuple[str, ...],
    static_col_types: Mapping[str, str],
    prefix: str,
) -> Any:
  if not static_cols:
    return None
  try:
    in_fields = list(named_fields_from_element_type(input_element_type))
  except Exception:  # pylint: disable=broad-except
    return None
  if not in_fields:
    return None

  field_map: dict[str, Any] = dict(in_fields)
  field_order: list[str] = [n for n, _ in in_fields]
  for c in static_cols:
    out_k = f'{prefix}{c}' if prefix else c
    py_t = _PY_TYPE_MAP.get(static_col_types.get(c, 'str'), str)
    opt_t = Optional[py_t]
    if out_k not in field_map:
      field_order.append(out_k)
      field_map[out_k] = opt_t
    elif prefix:
      field_map[out_k] = opt_t
  return RowTypeConstraint.from_fields([(n, field_map[n]) for n in field_order])


class CliEnrich(beam.PTransform):
  """Composite transform executing online/local feature & table enrichment."""
  def __init__(
      self,
      config: Mapping[str, Any],
      *,
      has_error_handling: bool = False,
      caller_factory=None,
      redis_client=None,
  ):
    super().__init__()
    self._cfg = parse_enrich_config(config)
    self._has_error_handling = has_error_handling
    self._caller_factory = caller_factory
    self._redis_client = redis_client

  def expand(self, pcoll: beam.PCollection):
    static_cols = self._cfg.columns
    static_types = dict(self._cfg.column_types)
    sniffed = _infer_local_file_schema(self._cfg)
    if sniffed is not None:
      sniff_cols, sniff_types = sniffed
      if not static_cols:
        static_cols = sniff_cols
      for c, st in sniff_types.items():
        if c not in self._cfg.explicit_types:
          static_types[c] = st

    out_row_type = _infer_enriched_row_type(
        pcoll.element_type, static_cols, static_types, self._cfg.prefix)

    dofn = _CliEnrichDoFn(
        self._cfg,
        has_error_handling=self._has_error_handling,
        static_columns=static_cols,
        static_column_types=static_types,
        caller_factory=self._caller_factory,
        redis_client=self._redis_client)

    if self._has_error_handling or self._cfg.on_miss == 'dlq':
      pardo = beam.ParDo(dofn).with_outputs('errors', main='good')
      if out_row_type is not None:
        pardo = pardo.with_output_types(out_row_type)
      res = pcoll | 'Enrich' >> pardo
      good = res['good']
      if out_row_type is not None:
        good.element_type = out_row_type
      return {'good': good, 'errors': res['errors']}

    pardo = beam.ParDo(dofn)
    if out_row_type is not None:
      pardo = pardo.with_output_types(out_row_type)
    out = pcoll | 'Enrich' >> pardo
    if out_row_type is not None:
      out.element_type = out_row_type
    return out


def cli_enrich(
    handler: Optional[str] = None,
    enrichment_handler: Optional[str] = None,
    provider: Optional[str] = None,
    key: Optional[Any] = None,
    row_key: Optional[Any] = None,
    fields: Optional[Any] = None,
    entity_id: Optional[Any] = None,
    lookup_key: Optional[Any] = None,
    columns: Optional[Any] = None,
    column_names: Optional[Any] = None,
    features: Optional[Any] = None,
    feature_names: Optional[Any] = None,
    feature_service: Optional[str] = None,
    feature_service_name: Optional[str] = None,
    full_feature_names: bool = False,
    prefix: str = '',
    on_miss: str = 'null',
    timeout: float = 30.0,
    min_batch_size: Optional[int] = None,
    max_batch_size: Optional[int] = None,
    max_batch_duration_secs: Optional[float] = None,
    rate_limit: Optional[float] = None,
    redis: Optional[str] = None,
    redis_host: Optional[str] = None,
    redis_port: Optional[int] = None,
    redis_db: Optional[int] = None,
    redis_tls: Optional[bool] = None,
    redis_ttl: Optional[int] = None,
    redis_password: Optional[str] = None,
    redis_password_env: Optional[str] = None,
    dlq_redact_fields: Optional[Any] = None,
    trust_providers: bool = False,
    path: Optional[str] = None,
    max_file_bytes: Optional[int] = None,
    project: Optional[str] = None,
    project_id: Optional[str] = None,
    table: Optional[str] = None,
    table_name: Optional[str] = None,
    table_id: Optional[str] = None,
    row_restriction: Optional[str] = None,
    instance: Optional[str] = None,
    instance_id: Optional[str] = None,
    app_profile_id: Optional[str] = None,
    encoding: Optional[str] = None,
    column_family: Optional[str] = None,
    location: Optional[str] = None,
    region: Optional[str] = None,
    api_endpoint: Optional[str] = None,
    feature_store: Optional[str] = None,
    feature_store_name: Optional[str] = None,
    feature_view: Optional[str] = None,
    feature_view_name: Optional[str] = None,
    feature_store_yaml_path: Optional[str] = None,
    error_handling: Optional[dict[str, Any]] = None,
) -> beam.PTransform:
  """Beam YAML provider constructor for `type: CliEnrich`."""
  raw_locals = {
      k: v
      for k, v in locals().items()
      if k != 'error_handling' and v is not None and v is not False
  }
  if full_feature_names:
    raw_locals['full_feature_names'] = True
  if trust_providers:
    raw_locals['trust_providers'] = True
  if redis_tls is False:
    raw_locals['redis_tls'] = False
  return CliEnrich(raw_locals, has_error_handling=bool(error_handling))
