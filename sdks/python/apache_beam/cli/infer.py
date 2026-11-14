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

"""Streaming & batch ML inference (`CliInfer`) for `beam pipe --infer`.

Implements backlog item `B1.4` purely inside `apache_beam.cli`:
- Lazy worker-side `ModelHandler` proxy (`load_model()`) so `--dry-run`,
  `beam check --plan`, and private-VPC job submissions never perform eager
  endpoint RPCs or require local `torch` / `transformers` / `aiplatform`.
- Hardened HuggingFace supply-chain defaults: `trust_remote_code=False`,
  `use_safetensors=True`, `large_model=True`, `model_copies=1`, blocked
  `env_vars` / arbitrary callables, and `@<commit_sha>` revision pinning.
- Hardened Vertex AI JSON defaults: `< 1.4 MB` pre-RPC payload guard (both per
  row and cumulative per batch), streaming `max_batch_size=64` /
  `max_batch_duration_secs=0.2`, `private=true` VPC network validation, and
  optional `rate_limit` token-bucket pacing.
- Deterministic `<10ms` offline `mock:` handler with configurable static,
  linear score, echo, poison-item, and transient batch-failure modes.
- Timestamp-preserving bundle batching (`min_ts` watermark preservation +
  exact per-element `WindowedValue` restoration) avoiding `BatchElements`
  `GlobalWindow.max_timestamp()` clobbering.
- In-worker singleton bisect on batch inference errors (preserving healthy
  `N - 1` rows without deleting/reloading multi-GB models) plus outer
  `RunInference.with_exception_handling()` `FlatMap` DLQ explosion for exact
  `--assert-counts` row conservation.
- Concrete `RowTypeConstraint` output schemas (nested `beam.Row` or
  `--infer-flatten` scalar columns — never untyped `Any` that breaks Parquet).
"""

from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
import json
import math
import os
import re
import threading
import time
from typing import Any
from typing import NamedTuple
from typing import Optional

import apache_beam as beam
from apache_beam.io.components import rate_limiter as beam_rate_limiter
from apache_beam.metrics import Metrics
from apache_beam.ml.inference import RunInference
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.pvalue import TaggedOutput
from apache_beam.transforms.window import WindowedValue
from apache_beam.typehints import schemas
from apache_beam.typehints.row_type import RowTypeConstraint

from . import enrich as enrich_mod
from . import interval_join
from . import output
from . import uris

_PROVIDER_ALIASES: dict[str, str] = {
    'vertexai': 'vertexai',
    'vertex': 'vertexai',
    'vertex_ai': 'vertexai',
    'vertexai_json': 'vertexai',
    'vertexaimodelhandlerjson': 'vertexai',
    'hf': 'hf',
    'huggingface': 'hf',
    'hf_pipeline': 'hf',
    'huggingfacepipelinemodelhandler': 'hf',
    'mock': 'mock',
}

_FORBIDDEN_CALLABLE_KEYS = frozenset([
    'preprocess',
    'postprocess',
    'inference_fn',
    'fn',
    'callable',
    'code',
    'env_vars',
    'element_size_fn',
    'batch_length_fn',
    'on_failure_callback',
])

_COMMON_INFER_KEYS = frozenset([
    'provider',
    'handler',
    'tag',
    'inference_tag',
    'flatten',
    'output_type',
    'output_columns',
    'columns',
    'fields',
    'infer_fields',
    'input_col',
    'min_batch_size',
    'max_batch_size',
    'max_batch_duration_secs',
    'rate_limit',
    'num_retries',
    'dlq_redact_fields',
    'inference_args',
    'parameters',
])

_PROVIDER_SPECIFIC_KEYS: dict[str, frozenset[str]] = {
    'vertexai': frozenset([
        'endpoint_id',
        'endpoint',
        'project',
        'location',
        'region',
        'private',
        'network',
        'experiment',
        'invoke_route',
        'max_payload_bytes',
    ]),
    'hf': frozenset([
        'task',
        'model',
        'revision',
        'device',
        'large_model',
        'model_copies',
        'trust_remote_code',
        'use_safetensors',
        'load_pipeline_args',
        'model_kwargs',
        'hf_token',
        'hf_token_env',
    ]),
    'mock': frozenset([
        'score',
        'label',
        'model_id',
        'mode',
        'score_col',
        'scale',
        'threshold',
        'pos_label',
        'neg_label',
        'response_json',
        'fail_on',
        'fail_batch_once',
    ]),
}

_SAFE_GCP_TOKEN_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$')
_SAFE_LOCATION_RE = re.compile(r'^[a-z0-9][a-z0-9-]{1,63}$')
_SAFE_ENDPOINT_ID_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$')
_SAFE_VPC_NETWORK_RE = re.compile(
    r'^projects/[A-Za-z0-9._:-]+/global/networks/[A-Za-z0-9._-]+$')
_SAFE_INVOKE_ROUTE_RE = re.compile(r'^/[A-Za-z0-9._/-]{1,255}$')
_SAFE_HF_TASK_RE = re.compile(r'^[a-z0-9][a-z0-9-]{1,63}$')
_SAFE_HF_REPO_RE = re.compile(
    r'^[A-Za-z0-9][A-Za-z0-9._-]*(?:/[A-Za-z0-9._-]+)?$')
_SAFE_HF_REVISION_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$')
_PINNED_SHA_RE = re.compile(r'^[0-9a-f]{40}$')
_AS_SPLIT_RE = re.compile(r'^(?P<src>.+?)\s+[Aa][Ss]\s+(?P<dst>.+)$')

_DEFAULT_VERTEX_MAX_PAYLOAD_BYTES = 1_400_000  # < 1.5 MB public Vertex limit
_MAX_INFER_BATCH_SIZE = 1024

_SUPPORTED_OUTPUT_TYPES = frozenset([
    'row',
    'float',
    'str',
    'string',
    'int',
    'integer',
    'bool',
    'boolean',
    'json'
])

_TYPED_COL_PY_MAP: dict[str, type] = {
    'str': str,
    'int': int,
    'float': float,
    'bool': bool,
}

_NESTED_PREDICTION_SCHEMA = RowTypeConstraint.from_fields([
    ('score', Optional[float]),
    ('label', Optional[str]),
    ('model_id', Optional[str]),
    ('raw_json', Optional[str]),
])

_DLQ_ROW_SCHEMA = RowTypeConstraint.from_fields([
    ('step', str),
    ('error_type', str),
    ('error', str),
    ('raw_payload', str),
    ('element', str),
])


class InferFieldMapping(NamedTuple):
  source_path: tuple[str, ...]
  target_key: str


class InferConfig(NamedTuple):
  provider: str
  tag: str
  flatten: bool
  output_type: str
  output_columns: tuple[tuple[str, str], ...]
  fields: tuple[InferFieldMapping, ...]
  min_batch_size: int
  max_batch_size: int
  max_batch_duration_secs: float
  rate_limit: float | None
  num_retries: int
  dlq_redact_fields: tuple[str, ...]
  inference_args: dict[str, Any]
  # Vertex AI options
  endpoint_id: str | None = None
  project: str | None = None
  location: str | None = None
  private: bool = False
  network: str | None = None
  experiment: str | None = None
  invoke_route: str | None = None
  max_payload_bytes: int = _DEFAULT_VERTEX_MAX_PAYLOAD_BYTES
  # HuggingFace options
  task: str | None = None
  model: str | None = None
  revision: str | None = None
  is_pinned_revision: bool = True
  device: str | None = None
  large_model: bool = True
  model_copies: int = 1
  hf_token_env: str = 'HF_TOKEN'
  hf_token: str | None = None
  # Mock options
  mock_score: float = 0.9
  mock_label: str = 'POSITIVE'
  mock_model_id: str = 'mock-v1'
  mock_mode: str = 'static'
  mock_score_col: str | None = None
  mock_scale: float = 1.0
  mock_threshold: float = 0.5
  mock_pos_label: str = 'POSITIVE'
  mock_neg_label: str = 'NEGATIVE'
  mock_response_json: str | None = None
  mock_fail_on: str | None = None
  mock_fail_batch_once: bool = False


class _InferErrorSentinel(NamedTuple):
  err_type: str
  msg: str


def _parse_bool(val: Any, label: str) -> bool:
  if isinstance(val, bool):
    return val
  s = str(val).strip().lower()
  if s in ('true', '1', 'yes'):
    return True
  if s in ('false', '0', 'no'):
    return False
  raise ValueError(
      f'invalid boolean {val!r} for {label}; expected true or false')


def _parse_positive_int(val: Any, label: str) -> int:
  if isinstance(val, bool):
    raise ValueError(f'{label} must be a positive integer, got {val!r}')
  try:
    iv = int(str(val).strip())
  except (TypeError, ValueError):
    raise ValueError(
        f'{label} must be a positive integer, got {val!r}') from None
  if iv <= 0:
    raise ValueError(f'{label} must be > 0, got {iv}')
  return iv


def _parse_non_negative_float(val: Any, label: str) -> float:
  if isinstance(val, bool):
    raise ValueError(f'{label} must be numeric, got {val!r}')
  try:
    fv = float(str(val).strip())
  except (TypeError, ValueError):
    raise ValueError(f'{label} must be numeric, got {val!r}') from None
  if not math.isfinite(fv) or fv < 0:
    raise ValueError(f'{label} must be a finite number >= 0, got {val!r}')
  return fv


def _parse_finite_float(val: Any, label: str) -> float:
  if isinstance(val, bool):
    raise ValueError(f'{label} must be numeric, got {val!r}')
  try:
    fv = float(str(val).strip())
  except (TypeError, ValueError):
    raise ValueError(f'{label} must be numeric, got {val!r}') from None
  if not math.isfinite(fv):
    raise ValueError(f'{label} must be a finite number, got {val!r}')
  return fv


def _validate_dotted_path(raw_src: str, label: str) -> tuple[str, ...]:
  parts = [p.strip() for p in raw_src.strip().split('.')]
  if not parts or any(not p for p in parts):
    raise ValueError(f'empty field reference {raw_src!r} in {label}')
  for seg in parts:
    if seg.startswith('_'):
      raise ValueError(
          f'underscore-prefixed field {seg!r} is not allowed in {label}')
    if not output.SAFE_IDENTIFIER_RE.match(seg):
      raise ValueError(f'invalid column identifier {seg!r} in {label}')
  return tuple(parts)


def parse_infer_fields(raw: Any,
                       label: str = '--infer-fields'
                       ) -> tuple[InferFieldMapping, ...]:
  """Parses SQL-style `col [AS alias], nested.col AS feat` mappings."""
  if raw is None or raw == '' or raw == [] or raw == ():
    return ()

  tokens: list[str] = []
  if isinstance(raw, Mapping):
    out_map: list[InferFieldMapping] = []
    seen_targets: set[str] = set()
    for dst_k, src_v in raw.items():
      dst = str(dst_k).strip()
      if dst.startswith('_') or not output.SAFE_IDENTIFIER_RE.match(dst):
        raise ValueError(f'invalid target alias {dst!r} in {label}')
      if dst in seen_targets:
        raise ValueError(f'duplicate target field {dst!r} in {label}')
      seen_targets.add(dst)
      src_path = _validate_dotted_path(str(src_v), label)
      out_map.append(InferFieldMapping(source_path=src_path, target_key=dst))
    return tuple(out_map)

  if isinstance(raw, str):
    s = raw.strip()
    if s.startswith('[') and s.endswith(']'):
      s = s[1:-1].strip()
    if not s:
      return ()
    tokens = enrich_mod.split_bracket_tokens(s)
  elif isinstance(raw, Iterable):
    for item in raw:
      if isinstance(item, InferFieldMapping):
        tokens.append(
            f'{".".join(item.source_path)} AS {item.target_key}' if item.
            target_key != item.source_path[-1] else '.'.join(item.source_path))
      else:
        tokens.append(str(item).strip())
  else:
    raise ValueError(f'unsupported {label} value {raw!r}')

  mappings: list[InferFieldMapping] = []
  seen: set[str] = set()
  for tok in tokens:
    clean = tok.strip()
    if len(clean) >= 2 and clean[0] == clean[-1] and clean[0] in ("'", '"'):
      clean = clean[1:-1].strip()
    if not clean:
      continue
    if any(ch in clean for ch in ('(', ')', ';', '=', '[', ']', '{', '}')):
      raise ValueError(
          f'invalid field mapping {tok!r} in {label}; expected '
          '"col AS feature_name" or "nested.col AS feature_name" '
          '(use --pre-select for computed expressions)')
    m = _AS_SPLIT_RE.match(clean)
    if m:
      src_raw = m.group('src').strip()
      dst_raw = m.group('dst').strip()
    else:
      src_raw = clean
      dst_raw = ''
    src_path = _validate_dotted_path(src_raw, label)
    target = dst_raw if dst_raw else src_path[-1]
    if target.startswith('_') or not output.SAFE_IDENTIFIER_RE.match(target):
      raise ValueError(f'invalid target field alias {target!r} in {label}')
    if target in seen:
      raise ValueError(f'duplicate target field {target!r} in {label}')
    seen.add(target)
    mappings.append(InferFieldMapping(source_path=src_path, target_key=target))
  return tuple(mappings)


def format_infer_fields(fields: Sequence[InferFieldMapping]) -> list[str]:
  out: list[str] = []
  for f in fields:
    src = '.'.join(f.source_path)
    if f.target_key == f.source_path[-1] and len(f.source_path) == 1:
      out.append(src)
    else:
      out.append(f'{src} AS {f.target_key}')
  return out


def _normalize_output_type(raw: Any) -> str:
  s = str(raw or 'row').strip().lower()
  if s not in _SUPPORTED_OUTPUT_TYPES:
    raise ValueError(
        f'invalid --infer output_type {raw!r}; expected one of '
        'row, float, str, int, bool, or json')
  if s == 'string':
    return 'str'
  if s == 'integer':
    return 'int'
  if s == 'boolean':
    return 'bool'
  return s


def _validate_hf_security_dict(d: Any, label: str) -> None:
  if d is None:
    return
  if not isinstance(d, Mapping):
    raise ValueError(f'{label} must be a mapping')
  for k, v in d.items():
    ks = str(k).strip()
    kl = ks.lower()
    if kl == 'trust_remote_code' and _parse_bool(v,
                                                 f'{label}.trust_remote_code'):
      raise ValueError(
          'trust_remote_code=True is blocked in CliInfer to prevent arbitrary '
          'remote Hub code execution; only trust_remote_code=False is allowed')
    if kl == 'use_safetensors' and not _parse_bool(v,
                                                   f'{label}.use_safetensors'):
      raise ValueError(
          'use_safetensors=False is blocked in CliInfer to prevent unsafe '
          'pickle (.bin) weight deserialization')
    if kl in _FORBIDDEN_CALLABLE_KEYS:
      raise ValueError(f'{ks!r} is disallowed inside {label} for security')
    if isinstance(v, Mapping):
      _validate_hf_security_dict(v, f'{label}.{ks}')


def parse_infer_config(
    raw_cfg: Mapping[str, Any],
    *,
    default_project: str | None = None,
) -> InferConfig:
  """Validates a `CliInfer` config mapping and returns `InferConfig`."""
  if not isinstance(raw_cfg, Mapping):
    raise ValueError('CliInfer config must be a mapping')

  norm: dict[str, Any] = {}
  for k, v in raw_cfg.items():
    nk = str(k).strip().lower().replace('-', '_')
    if nk in _FORBIDDEN_CALLABLE_KEYS:
      raise ValueError(
          f'option {k!r} is disallowed in CliInfer; arbitrary Python callables '
          'and worker env_vars overrides are blocked for security (use '
          '--pre-select / --infer-fields / --select instead)')
    if nk not in ('hf_token',
                  'hf_token_env') and uris._SECRET_KEY_RE.search(nk):
      raise ValueError(
          f'secret option {k!r} is not supported inline in CliInfer; use '
          'ADC, HF_TOKEN environment variable, or hf_token in a 0600 '
          '--infer-opt-file')
    norm[nk] = v

  raw_prov = norm.get('provider') or norm.get('handler')
  if not raw_prov and isinstance(norm.get('model_handler'), Mapping):
    mh_map = norm['model_handler']
    raw_prov = mh_map.get('type')
    if isinstance(mh_map.get('config'), Mapping):
      for mk, mv in mh_map['config'].items():
        mnk = str(mk).strip().lower().replace('-', '_')
        if mnk in _FORBIDDEN_CALLABLE_KEYS:
          raise ValueError(f'option {mk!r} is disallowed in CliInfer')
        norm.setdefault(mnk, mv)
      norm.pop('model_handler', None)

  if not raw_prov:
    raise ValueError(
        'CliInfer requires provider (supported: vertexai, hf, mock)')
  prov_key = str(raw_prov).strip().lower().replace('-', '_')
  provider = _PROVIDER_ALIASES.get(prov_key)
  if provider is None:
    raise ValueError(
        f'unknown --infer provider {raw_prov!r}; supported providers are '
        'vertexai, hf, and mock')

  allowed_keys = _COMMON_INFER_KEYS | _PROVIDER_SPECIFIC_KEYS[provider]
  unknown = [k for k in norm if k not in allowed_keys]
  if unknown:
    raise ValueError(
        f'unknown option(s) {sorted(unknown)} for --infer provider {provider!r}'
    )

  tag = str(norm.get('tag') or norm.get('inference_tag') or
            'prediction').strip()
  if tag.startswith('_') or not output.SAFE_IDENTIFIER_RE.match(tag):
    raise ValueError(f'invalid --infer-tag column name {tag!r}')

  flatten = _parse_bool(norm.get('flatten', False), 'flatten')
  output_type = _normalize_output_type(norm.get('output_type', 'row'))

  raw_out_cols = norm.get('output_columns') or norm.get('columns')
  output_columns: tuple[tuple[str, str], ...] = ()
  if raw_out_cols:
    parsed_oc: list[tuple[str, str]] = []
    seen_oc: set[str] = set()
    for raw_oc in interval_join.parse_column_list(raw_out_cols):
      if ':' in raw_oc:
        oc_name, oc_type = raw_oc.split(':', 1)
        oc_name = oc_name.strip()
        oc_dt = _normalize_output_type(oc_type.strip())
        if oc_dt in ('row', 'json'):
          oc_dt = 'str'
      else:
        oc_name = raw_oc.strip()
        oc_dt = 'str'
      if oc_name.startswith(
          '_') or not output.SAFE_IDENTIFIER_RE.match(oc_name):
        raise ValueError(f'invalid output_columns identifier {oc_name!r}')
      if oc_name in seen_oc:
        raise ValueError(f'duplicate column {oc_name!r} in output_columns')
      seen_oc.add(oc_name)
      parsed_oc.append((oc_name, oc_dt))
    output_columns = tuple(parsed_oc)

  raw_fields = norm.get('fields')
  if raw_fields is None:
    raw_fields = norm.get('infer_fields')
  if raw_fields is None and norm.get('input_col'):
    raw_fields = str(norm['input_col'])
  fields = parse_infer_fields(raw_fields, '--infer-fields')

  default_max_batch = 64 if provider == 'vertexai' else 32
  max_batch_size = _parse_positive_int(
      norm.get('max_batch_size', default_max_batch), 'max_batch_size')
  if max_batch_size > _MAX_INFER_BATCH_SIZE:
    raise ValueError(
        f'max_batch_size={max_batch_size} exceeds maximum {_MAX_INFER_BATCH_SIZE}'
    )
  min_batch_size = _parse_positive_int(
      norm.get('min_batch_size', 1), 'min_batch_size')
  if min_batch_size > max_batch_size:
    min_batch_size = max_batch_size

  max_batch_duration_secs = _parse_non_negative_float(
      norm.get('max_batch_duration_secs', 0.2), 'max_batch_duration_secs')

  rate_limit: float | None = None
  if norm.get('rate_limit') is not None and str(norm.get('rate_limit')).strip():
    rate_limit = _parse_non_negative_float(norm['rate_limit'], 'rate_limit')
    if rate_limit <= 0:
      raise ValueError(f'rate_limit must be > 0, got {rate_limit}')

  num_retries = int(norm.get('num_retries', 3))
  if num_retries < 0 or num_retries > 20:
    raise ValueError(f'num_retries must be between 0 and 20, got {num_retries}')

  dlq_redact_fields: tuple[str, ...] = ()
  if norm.get('dlq_redact_fields'):
    dlq_redact_fields = enrich_mod._validate_ident_list(
        interval_join.parse_column_list(norm['dlq_redact_fields']),
        'dlq_redact_fields')

  raw_inf_args = norm.get('inference_args') or norm.get('parameters') or {}
  if isinstance(raw_inf_args, str):
    loaded_ia = output.safe_yaml_load(raw_inf_args)
    if not isinstance(loaded_ia, Mapping):
      raise ValueError('inference_args / parameters must be a JSON/YAML object')
    raw_inf_args = loaded_ia
  if not isinstance(raw_inf_args, Mapping):
    raise ValueError('inference_args / parameters must be a mapping')
  inference_args = {str(k): v for k, v in raw_inf_args.items()}

  if provider == 'vertexai':
    endpoint_id = str(norm.get('endpoint_id') or norm.get('endpoint') or
                      '').strip()
    if not endpoint_id:
      raise ValueError(
          '--infer vertexai:... requires endpoint_id=<ID> (plus project and location)'
      )
    try:
      uris.reject_inline_credentials(endpoint_id)
    except uris.UriError as e:
      raise ValueError(str(e)) from None
    if '://' in endpoint_id or not _SAFE_ENDPOINT_ID_RE.match(endpoint_id):
      raise ValueError(f'invalid Vertex AI endpoint_id {endpoint_id!r}')

    project = str(norm.get('project') or default_project or '').strip()
    if not project:
      raise ValueError(
          '--infer vertexai:... requires project=<GCP_PROJECT> (or --project)')
    if not _SAFE_GCP_TOKEN_RE.match(project):
      raise ValueError(f'invalid GCP project {project!r} in --infer vertexai')

    location = str(norm.get('location') or norm.get('region') or '').strip()
    if not location:
      raise ValueError(
          '--infer vertexai:... requires location=<REGION> (e.g. us-central1)')
    if not _SAFE_LOCATION_RE.match(location):
      raise ValueError(f'invalid GCP location {location!r} in --infer vertexai')

    private = _parse_bool(norm.get('private', False), 'private')
    network = (
        str(norm['network']).strip() if norm.get('network') is not None and
        str(norm['network']).strip() else None)
    if private and not network:
      raise ValueError(
          '--infer vertexai private=true requires network=projects/<NUM>/global/networks/<VPC>'
      )
    if network is not None and not _SAFE_VPC_NETWORK_RE.match(network):
      raise ValueError(
          f'invalid Vertex AI VPC network {network!r}; expected '
          'projects/<PROJECT_NUMBER>/global/networks/<NETWORK>')

    experiment = (
        str(norm['experiment']).strip()
        if norm.get('experiment') is not None and
        str(norm['experiment']).strip() else None)
    if experiment is not None and not _SAFE_GCP_TOKEN_RE.match(experiment):
      raise ValueError(f'invalid Vertex AI experiment {experiment!r}')

    invoke_route = (
        str(norm['invoke_route']).strip()
        if norm.get('invoke_route') is not None and
        str(norm['invoke_route']).strip() else None)
    if invoke_route is not None:
      if '..' in invoke_route or not _SAFE_INVOKE_ROUTE_RE.match(invoke_route):
        raise ValueError(
            f'invalid Vertex AI invoke_route {invoke_route!r}; expected leading '
            'slash path such as /predict/v1 without ..')

    max_payload_bytes = _parse_positive_int(
        norm.get('max_payload_bytes', _DEFAULT_VERTEX_MAX_PAYLOAD_BYTES),
        'max_payload_bytes')

    return InferConfig(
        provider='vertexai',
        tag=tag,
        flatten=flatten,
        output_type=output_type,
        output_columns=output_columns,
        fields=fields,
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size,
        max_batch_duration_secs=max_batch_duration_secs,
        rate_limit=rate_limit,
        num_retries=num_retries,
        dlq_redact_fields=dlq_redact_fields,
        inference_args=inference_args,
        endpoint_id=endpoint_id,
        project=project,
        location=location,
        private=private,
        network=network,
        experiment=experiment,
        invoke_route=invoke_route,
        max_payload_bytes=max_payload_bytes,
    )

  if provider == 'hf':
    if 'trust_remote_code' in norm and _parse_bool(norm['trust_remote_code'],
                                                   'trust_remote_code'):
      raise ValueError(
          'trust_remote_code=True is blocked in CliInfer to prevent arbitrary '
          'remote Hub code execution; only trust_remote_code=False is allowed')
    if 'use_safetensors' in norm and not _parse_bool(norm['use_safetensors'],
                                                     'use_safetensors'):
      raise ValueError(
          'use_safetensors=False is blocked in CliInfer to prevent unsafe '
          'pickle (.bin) weight deserialization')
    _validate_hf_security_dict(
        norm.get('load_pipeline_args'), 'load_pipeline_args')
    _validate_hf_security_dict(norm.get('model_kwargs'), 'model_kwargs')

    task = (
        str(norm['task']).strip().lower()
        if norm.get('task') is not None and str(norm['task']).strip() else None)
    if task is not None and not _SAFE_HF_TASK_RE.match(task):
      raise ValueError(f'invalid HuggingFace task {task!r}')

    raw_model = (
        str(norm['model']).strip() if norm.get('model') is not None and
        str(norm['model']).strip() else None)
    if not task and not raw_model:
      raise ValueError(
          '--infer hf:... requires at least task=<TASK> or model=<ORG/REPO[@SHA]>'
      )
    if not fields:
      raise ValueError(
          '--infer hf:... requires --infer-fields <COL> (or inline fields=[col]) '
          'specifying the input feature/text column(s)')

    revision = (
        str(norm['revision']).strip() if norm.get('revision') is not None and
        str(norm['revision']).strip() else None)
    model_id = raw_model
    is_pinned = True
    if model_id is not None:
      try:
        uris.reject_inline_credentials(model_id)
      except uris.UriError as e:
        raise ValueError(str(e)) from None
      if model_id.startswith(('http://', 'https://')):
        raise ValueError(
            f'direct HTTP URL {model_id!r} is not allowed for --infer hf model; '
            'specify a HuggingFace repo org/model@<40-char-sha>, gs:// path, '
            'or local directory')
      if '@' in model_id and not model_id.startswith('gs://'):
        base_m, inline_rev = model_id.rsplit('@', 1)
        if not base_m or not inline_rev:
          raise ValueError(f'invalid HuggingFace model@revision {model_id!r}')
        if revision and revision != inline_rev:
          raise ValueError(
              f'conflicting revision in model={model_id!r} vs revision={revision!r}'
          )
        model_id = base_m
        revision = inline_rev

      if model_id.startswith(('gs://', 's3://')):
        if '..' in model_id:
          raise ValueError(f'path traversal disallowed in model {model_id!r}')
        is_pinned = True
      elif model_id.startswith(('/', './')) or os.path.exists(model_id):
        if '..' in model_id.replace('\\', '/').split('/'):
          raise ValueError(f'path traversal disallowed in model {model_id!r}')
        is_pinned = True
      else:
        if '..' in model_id or not _SAFE_HF_REPO_RE.match(model_id):
          raise ValueError(f'invalid HuggingFace model identifier {model_id!r}')
        is_pinned = bool(revision and _PINNED_SHA_RE.match(revision))

    if revision is not None and not _SAFE_HF_REVISION_RE.match(revision):
      raise ValueError(f'invalid HuggingFace revision {revision!r}')

    device: str | None = None
    if norm.get('device') is not None and str(norm['device']).strip():
      dev_up = str(norm['device']).strip().upper()
      if dev_up not in ('CPU', 'GPU'):
        raise ValueError(
            f'invalid --infer hf device {norm["device"]!r}; expected CPU or GPU'
        )
      device = dev_up

    large_model = _parse_bool(norm.get('large_model', True), 'large_model')
    model_copies = _parse_positive_int(
        norm.get('model_copies', 1), 'model_copies')
    if model_copies > 16:
      raise ValueError(f'model_copies={model_copies} exceeds safe bound (16)')

    hf_token_env = str(norm.get('hf_token_env') or 'HF_TOKEN').strip()
    if not output.SAFE_IDENTIFIER_RE.match(hf_token_env):
      raise ValueError(f'invalid hf_token_env {hf_token_env!r}')
    raw_hf_tok = norm.get('hf_token')
    hf_token = (
        str(raw_hf_tok) if raw_hf_tok is not None and
        str(raw_hf_tok) not in ('<redacted>', '<REDACTED_SECRET>') else None)

    return InferConfig(
        provider='hf',
        tag=tag,
        flatten=flatten,
        output_type=output_type,
        output_columns=output_columns,
        fields=fields,
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size,
        max_batch_duration_secs=max_batch_duration_secs,
        rate_limit=rate_limit,
        num_retries=num_retries,
        dlq_redact_fields=dlq_redact_fields,
        inference_args=inference_args,
        task=task,
        model=model_id,
        revision=revision,
        is_pinned_revision=is_pinned,
        device=device,
        large_model=large_model,
        model_copies=model_copies,
        hf_token_env=hf_token_env,
        hf_token=hf_token,
    )

  # provider == 'mock'
  mock_score = _parse_finite_float(norm.get('score', 0.9), 'score')
  mock_label = str(norm.get('label', 'POSITIVE'))
  mock_model_id = str(norm.get('model_id', 'mock-v1')).strip() or 'mock-v1'
  mock_mode = str(norm.get('mode', 'static')).strip().lower()
  if mock_mode not in ('static', 'echo', 'linear'):
    raise ValueError(
        f'invalid mock mode {mock_mode!r}; expected static, echo, or linear')
  mock_score_col = (
      str(norm['score_col']).strip() if norm.get('score_col') is not None and
      str(norm['score_col']).strip() else None)
  if mock_score_col is not None:
    if not output.SAFE_IDENTIFIER_RE.match(mock_score_col):
      raise ValueError(f'invalid mock score_col {mock_score_col!r}')
    if mock_mode == 'static':
      mock_mode = 'linear'
  mock_scale = _parse_finite_float(norm.get('scale', 1.0), 'scale')
  mock_threshold = _parse_finite_float(norm.get('threshold', 0.5), 'threshold')
  mock_pos_label = str(norm.get('pos_label', 'POSITIVE'))
  mock_neg_label = str(norm.get('neg_label', 'NEGATIVE'))
  mock_response_json: str | None = None
  if norm.get('response_json') is not None:
    if isinstance(norm['response_json'], (Mapping, list)):
      mock_response_json = json.dumps(norm['response_json'], sort_keys=True)
    else:
      raw_rj = str(norm['response_json']).strip()
      parsed_rj = json.loads(raw_rj)
      mock_response_json = json.dumps(parsed_rj, sort_keys=True)
  mock_fail_on = (
      str(norm['fail_on']) if norm.get('fail_on') is not None and
      str(norm['fail_on']) != '' else None)
  mock_fail_batch_once = _parse_bool(
      norm.get('fail_batch_once', False), 'fail_batch_once')

  return InferConfig(
      provider='mock',
      tag=tag,
      flatten=flatten,
      output_type=output_type,
      output_columns=output_columns,
      fields=fields,
      min_batch_size=min_batch_size,
      max_batch_size=max_batch_size,
      max_batch_duration_secs=max_batch_duration_secs,
      rate_limit=rate_limit,
      num_retries=num_retries,
      dlq_redact_fields=dlq_redact_fields,
      inference_args=inference_args,
      mock_score=mock_score,
      mock_label=mock_label,
      mock_model_id=mock_model_id,
      mock_mode=mock_mode,
      mock_score_col=mock_score_col,
      mock_scale=mock_scale,
      mock_threshold=mock_threshold,
      mock_pos_label=mock_pos_label,
      mock_neg_label=mock_neg_label,
      mock_response_json=mock_response_json,
      mock_fail_on=mock_fail_on,
      mock_fail_batch_once=mock_fail_batch_once,
  )


def infer_config_to_yaml_dict(cfg: InferConfig) -> dict[str, Any]:
  """Serializes `InferConfig` to a clean YAML dict for `PipeSpec`."""
  out: dict[str, Any] = {'provider': cfg.provider}
  if cfg.tag != 'prediction':
    out['tag'] = cfg.tag
  if cfg.flatten:
    out['flatten'] = True
  if cfg.output_type != 'row':
    out['output_type'] = cfg.output_type
  if cfg.output_columns:
    out['output_columns'] = [f'{c}:{t}' for c, t in cfg.output_columns]
  if cfg.fields:
    out['fields'] = format_infer_fields(cfg.fields)

  default_max_b = 64 if cfg.provider == 'vertexai' else 32
  if cfg.min_batch_size != 1:
    out['min_batch_size'] = cfg.min_batch_size
  if cfg.max_batch_size != default_max_b:
    out['max_batch_size'] = cfg.max_batch_size
  if cfg.max_batch_duration_secs != 0.2:
    out['max_batch_duration_secs'] = cfg.max_batch_duration_secs
  if cfg.rate_limit is not None:
    out['rate_limit'] = cfg.rate_limit
  if cfg.num_retries != 3:
    out['num_retries'] = cfg.num_retries
  if cfg.dlq_redact_fields:
    out['dlq_redact_fields'] = list(cfg.dlq_redact_fields)
  if cfg.inference_args:
    out['inference_args'] = dict(cfg.inference_args)

  if cfg.provider == 'vertexai':
    out['endpoint_id'] = cfg.endpoint_id
    out['project'] = cfg.project
    out['location'] = cfg.location
    if cfg.private:
      out['private'] = True
    if cfg.network:
      out['network'] = cfg.network
    if cfg.experiment:
      out['experiment'] = cfg.experiment
    if cfg.invoke_route:
      out['invoke_route'] = cfg.invoke_route
    if cfg.max_payload_bytes != _DEFAULT_VERTEX_MAX_PAYLOAD_BYTES:
      out['max_payload_bytes'] = cfg.max_payload_bytes
  elif cfg.provider == 'hf':
    if cfg.task:
      out['task'] = cfg.task
    if cfg.model:
      out['model'] = cfg.model
    if cfg.revision:
      out['revision'] = cfg.revision
    if cfg.device:
      out['device'] = cfg.device
    out['large_model'] = cfg.large_model
    if cfg.model_copies != 1:
      out['model_copies'] = cfg.model_copies
    if cfg.hf_token_env != 'HF_TOKEN':
      out['hf_token_env'] = cfg.hf_token_env
    if cfg.hf_token is not None:
      out['hf_token'] = cfg.hf_token
  else:  # mock
    if cfg.mock_mode != 'static':
      out['mode'] = cfg.mock_mode
    if cfg.mock_score != 0.9:
      out['score'] = cfg.mock_score
    if cfg.mock_label != 'POSITIVE':
      out['label'] = cfg.mock_label
    if cfg.mock_model_id != 'mock-v1':
      out['model_id'] = cfg.mock_model_id
    if cfg.mock_score_col:
      out['score_col'] = cfg.mock_score_col
    if cfg.mock_scale != 1.0:
      out['scale'] = cfg.mock_scale
    if cfg.mock_threshold != 0.5:
      out['threshold'] = cfg.mock_threshold
    if cfg.mock_pos_label != 'POSITIVE':
      out['pos_label'] = cfg.mock_pos_label
    if cfg.mock_neg_label != 'NEGATIVE':
      out['neg_label'] = cfg.mock_neg_label
    if cfg.mock_response_json is not None:
      out['response_json'] = cfg.mock_response_json
    if cfg.mock_fail_on is not None:
      out['fail_on'] = cfg.mock_fail_on
    if cfg.mock_fail_batch_once:
      out['fail_batch_once'] = True
  return out


def parse_infer_spec_str(
    spec_str: str,
    *,
    extra_opts: Mapping[str, Any] | None = None,
    infer_fields: str | None = None,
    infer_tag: str | None = None,
    infer_flatten: bool = False,
    default_project: str | None = None,
    single_stage: bool = True,
) -> dict[str, Any]:
  """Parses CLI `--infer PROVIDER[:k=v,...]` shorthand into a YAML config dict."""
  if not isinstance(spec_str, str) or not spec_str.strip():
    raise ValueError('--infer specification cannot be empty')
  s = spec_str.strip()
  if ':' in s:
    raw_prov, rest = s.split(':', 1)
  else:
    raw_prov, rest = s, ''
  p_key = raw_prov.strip().lower().replace('-', '_')
  provider = _PROVIDER_ALIASES.get(p_key)
  if provider is None:
    raise ValueError(
        f'unknown --infer provider {raw_prov!r}; expected '
        'PROVIDER[:key=val,...] with provider vertexai, hf, or mock')

  kv: dict[str, Any] = {'provider': provider}
  if rest.strip():
    for tok in enrich_mod.split_bracket_tokens(rest.strip()):
      if '=' not in tok:
        raise ValueError(
            f'expected key=value token in --infer {raw_prov}:..., got {tok!r}')
      k, v = tok.split('=', 1)
      key = k.strip().lower().replace('-', '_')
      val = v.strip()
      if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
        val = val[1:-1]
      if uris._SECRET_KEY_RE.search(key):
        raise ValueError(
            f'refusing inline secret option {key!r} in --infer; '
            'use environment variables (e.g. HF_TOKEN), ADC, or a 0600 '
            '--infer-opt-file')
      kv[key] = val

  if infer_fields is not None and 'fields' not in kv and 'infer_fields' not in kv:
    kv['fields'] = infer_fields
  if infer_tag is not None and 'tag' not in kv and 'inference_tag' not in kv:
    kv['tag'] = infer_tag
  if infer_flatten and 'flatten' not in kv:
    kv['flatten'] = True

  if extra_opts:
    allowed_for_prov = _COMMON_INFER_KEYS | _PROVIDER_SPECIFIC_KEYS[provider]
    for ek, ev in extra_opts.items():
      norm_k = str(ek).strip().lower().replace('-', '_')
      if norm_k in _FORBIDDEN_CALLABLE_KEYS:
        raise ValueError(
            f'option {ek!r} is disallowed in CliInfer for security')
      if norm_k not in allowed_for_prov:
        if single_stage:
          raise ValueError(
              f'unknown option {ek!r} for --infer provider {provider!r}')
        continue
      kv.setdefault(norm_k, ev)

  parsed = parse_infer_config(kv, default_project=default_project)
  return infer_config_to_yaml_dict(parsed)


def _to_json_compatible(val: Any) -> Any:
  if val is None or isinstance(val, (str, int, bool)):
    return val
  if isinstance(val, float):
    if not math.isfinite(val):
      raise ValueError(f'non-finite float {val!r} is not JSON serializable')
    return val
  if isinstance(val, bytes):
    return val.decode('utf-8', errors='replace')
  if hasattr(val, '_asdict'):
    return {str(k): _to_json_compatible(v) for k, v in val._asdict().items()}
  if isinstance(val, Mapping):
    return {str(k): _to_json_compatible(v) for k, v in val.items()}
  if isinstance(val, (list, tuple)):
    return [_to_json_compatible(x) for x in val]
  if hasattr(val, 'tolist') and callable(val.tolist):
    return _to_json_compatible(val.tolist())
  return str(val)


def _extract_field_path(
    row_dict: Mapping[str, Any], path: tuple[str, ...]) -> Any:
  cur: Any = row_dict
  for idx, seg in enumerate(path):
    if cur is None:
      return None
    if hasattr(cur, '_asdict'):
      cur = cur._asdict()
    if isinstance(cur, Mapping):
      if idx == 0 and seg not in cur:
        raise KeyError(f'missing required inference input column {seg!r}')
      cur = cur.get(seg)
    else:
      if not hasattr(cur, seg):
        raise KeyError(f'missing nested attribute {seg!r}')
      cur = getattr(cur, seg)
  return cur


def _build_model_input(row_dict: Mapping[str, Any], cfg: InferConfig) -> Any:
  if cfg.provider == 'hf':
    if len(cfg.fields) == 1:
      val = _extract_field_path(row_dict, cfg.fields[0].source_path)
      if val is None:
        raise ValueError(
            f'null input in column {".".join(cfg.fields[0].source_path)!r} '
            'for HuggingFace text inference')
      return val if isinstance(val, str) else str(val)
    out_hf: dict[str, Any] = {}
    for fm in cfg.fields:
      val = _extract_field_path(row_dict, fm.source_path)
      if val is None:
        raise ValueError(
            f'null input in column {".".join(fm.source_path)!r} '
            'for HuggingFace inference')
      out_hf[fm.target_key] = _to_json_compatible(val)
    return out_hf

  if cfg.fields:
    mapped: dict[str, Any] = {}
    for fm in cfg.fields:
      val = _extract_field_path(row_dict, fm.source_path)
      mapped[fm.target_key] = _to_json_compatible(val)
    return mapped

  return {
      str(k): _to_json_compatible(v)
      for k, v in row_dict.items() if not str(k).startswith('_')
  }


def _sanitize_infer_error(
    exc_or_str: BaseException | str, provider: str) -> str:
  if isinstance(exc_or_str, BaseException):
    raw = f'{type(exc_or_str).__name__}: {exc_or_str}'
  else:
    lines = [ln.strip() for ln in str(exc_or_str).splitlines() if ln.strip()]
    raw = lines[-1] if lines else 'InferenceError'
  scrubbed = output.redact_inline_secrets(raw, placeholder='<REDACTED_SECRET>')
  scrubbed = output._LONG_QUOTED_RE.sub("'<REDACTED>'", scrubbed)
  # Scrub dict/JSON payload echoes so raw row PII never leaks into error logs.
  scrubbed = re.sub(r'\{[^{}]{24,}\}', '{<redacted_payload>}', scrubbed)
  return f'CliInfer ({provider}) {scrubbed}'[:1024]


def _make_infer_dlq_row(
    step: str,
    err_type: str,
    msg: str,
    row_dict: Mapping[str, Any] | Any,
    redact_fields: tuple[str, ...] = (),
) -> beam.Row:
  try:
    plain = (
        dict(row_dict) if isinstance(row_dict, Mapping) else
        interval_join._to_plain_dict(row_dict))
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


def _normalize_prediction(
    pred: Any,
    default_model_id: str,
) -> tuple[float | None, str | None, str | None, dict[str, Any] | None, str]:
  """Extracts `(score, label, model_id, pred_dict, raw_json)` from a result."""
  if isinstance(pred, PredictionResult):
    raw_inf = pred.inference
    raw_mid = pred.model_id
  else:
    raw_inf = pred
    raw_mid = None

  model_id = str(raw_mid) if raw_mid is not None else default_model_id
  compat = _to_json_compatible(raw_inf)
  raw_json = json.dumps(compat, sort_keys=True, separators=(',', ':'))

  unwrapped = compat
  if (isinstance(unwrapped, list) and len(unwrapped) == 1 and
      isinstance(unwrapped[0], Mapping)):
    unwrapped = unwrapped[0]

  score: float | None = None
  label: str | None = None
  pred_dict: dict[str, Any] | None = None

  if isinstance(unwrapped, bool):
    label = 'true' if unwrapped else 'false'
    score = 1.0 if unwrapped else 0.0
  elif isinstance(unwrapped, (int, float)):
    score = float(unwrapped)
  elif isinstance(unwrapped, str):
    label = unwrapped
  elif isinstance(unwrapped, Mapping):
    pred_dict = dict(unwrapped)
    for sk in ('score', 'probability', 'confidence'):
      sv = pred_dict.get(sk)
      if isinstance(sv, (int, float)) and not isinstance(sv, bool):
        score = float(sv)
        break
    for lk in (
        'label',
        'class',
        'prediction',
        'generated_text',
        'summary_text',
        'translation_text',
        'answer',
    ):
      lv = pred_dict.get(lk)
      if lv is not None and not isinstance(lv, (Mapping, list)):
        label = str(lv)
        break
    if score is None and isinstance(pred_dict.get('scores'), list):
      scores_list = [
          float(x) for x in pred_dict['scores']
          if isinstance(x, (int, float)) and not isinstance(x, bool)
      ]
      if scores_list:
        best_idx = max(range(len(scores_list)), key=lambda i: scores_list[i])
        score = scores_list[best_idx]
        classes_list = pred_dict.get('classes')
        if (label is None and isinstance(classes_list, list) and
            len(classes_list) == len(scores_list)):
          label = str(classes_list[best_idx])

  return score, label, model_id, pred_dict, raw_json


class _TokenBucketRateLimiter(beam_rate_limiter.RateLimiter):
  """Thread-safe local token-bucket `RateLimiter` for `RemoteModelHandler`."""
  def __init__(self, rate_per_sec: float, namespace: str = 'CliInfer'):
    super().__init__(namespace=namespace)
    self._rate = max(0.01, float(rate_per_sec))
    self._lock = threading.Lock()
    self._next_allowed = 0.0

  def __getstate__(self):
    return {'rate': self._rate}

  def __setstate__(self, state):
    super().__init__(namespace='CliInfer')
    self._rate = state['rate']
    self._lock = threading.Lock()
    self._next_allowed = 0.0

  def allow(self, **kwargs) -> bool:
    hits = max(1, int(kwargs.get('hits_added', 1) or 1))
    self.requests_counter.inc(hits)
    step = float(hits) / self._rate
    with self._lock:
      now = time.monotonic()
      if self._next_allowed <= now:
        self._next_allowed = now + step
        wait_s = 0.0
      else:
        wait_s = self._next_allowed - now
        self._next_allowed += step
    if wait_s > 0:
      self.requests_throttled.inc()
      time.sleep(min(wait_s, 5.0))
    self.requests_allowed.inc(hits)
    return True


class _LoadedMockModel:
  """Weak-referenceable model object required by `apache_beam.utils.shared`."""
  def __init__(self, model_id: str, mode: str):
    self.model_id = model_id
    self.mode = mode


class _MockModelHandler(ModelHandler[Any, PredictionResult, _LoadedMockModel]):
  """Deterministic `<1ms` local `ModelHandler` for offline CLI dev & tests."""
  def __init__(self, cfg: InferConfig):
    super().__init__()
    self._cfg = cfg
    self._failed_multi_batch_once = False

  def load_model(self) -> _LoadedMockModel:
    return _LoadedMockModel(self._cfg.mock_model_id, self._cfg.mock_mode)

  def run_inference(
      self,
      batch: Sequence[Any],
      model: _LoadedMockModel,
      inference_args: Optional[dict[str, Any]] = None,
  ) -> Iterable[PredictionResult]:
    del inference_args
    if (self._cfg.mock_fail_batch_once and len(batch) > 1 and
        not self._failed_multi_batch_once):
      self._failed_multi_batch_once = True
      raise RuntimeError('simulated transient multi-element batch RPC failure')

    results: list[PredictionResult] = []
    for item in batch:
      if self._cfg.mock_fail_on is not None:
        rendered = (
            json.dumps(item, sort_keys=True, default=str) if isinstance(
                item, (Mapping, list)) else str(item))
        if self._cfg.mock_fail_on in rendered:
          raise ValueError('mock model rejected poison input record')

      if self._cfg.mock_response_json is not None:
        inf_val = json.loads(self._cfg.mock_response_json)
      elif self._cfg.mock_mode == 'echo':
        inf_val = {
            'score': self._cfg.mock_score,
            'label': (
                json.dumps(item, sort_keys=True, default=str) if isinstance(
                    item, (Mapping, list)) else str(item)),
        }
      elif self._cfg.mock_mode == 'linear':
        raw_num: Any = None
        if isinstance(item, Mapping):
          if self._cfg.mock_score_col and self._cfg.mock_score_col in item:
            raw_num = item[self._cfg.mock_score_col]
          elif len(item) == 1:
            raw_num = next(iter(item.values()))
        else:
          raw_num = item
        num_val = float(raw_num) if raw_num is not None else 0.0
        sc = round(num_val * self._cfg.mock_scale, 6)
        lbl = (
            self._cfg.mock_pos_label
            if sc >= self._cfg.mock_threshold else self._cfg.mock_neg_label)
        inf_val = {'score': sc, 'label': lbl}
      else:
        inf_val = {
            'score': self._cfg.mock_score,
            'label': self._cfg.mock_label,
        }
      results.append(PredictionResult(item, inf_val, model.model_id))
    return results

  def get_metrics_namespace(self) -> str:
    return 'BeamML_MockModelHandler'


class _LazyCliModelHandler(ModelHandler[Any, Any, Any]):
  """Picklable lazy proxy deferring SDK/model imports strictly to `load_model()`.

  Also performs in-worker singleton bisect on batch inference exceptions when
  `catch_singleton_errors=True`, isolating per-record poison pills to DLQ
  without discarding healthy batch peers or triggering `RunInference` model
  eviction/reloads.
  """
  def __init__(
      self,
      cfg: InferConfig,
      *,
      catch_singleton_errors: bool = False,
      handler_factory=None,
  ):
    super().__init__()
    self._cfg = cfg
    self._catch_singleton_errors = catch_singleton_errors
    self._handler_factory = handler_factory
    self._inner: ModelHandler[Any, Any, Any] | None = None
    self._rate_limiter = (
        _TokenBucketRateLimiter(cfg.rate_limit)
        if cfg.rate_limit is not None and cfg.provider != 'vertexai' else None)
    self._batches_counter = Metrics.counter('beam_cli', 'inference_batches')
    self._latency_dist = Metrics.distribution(
        'beam_cli', 'inference_latency_ms')

  def __getstate__(self):
    return {
        'cfg': self._cfg,
        'catch_singleton_errors': self._catch_singleton_errors,
        'handler_factory': self._handler_factory,
    }

  def __setstate__(self, state):
    super().__init__()
    self._cfg = state['cfg']
    self._catch_singleton_errors = state['catch_singleton_errors']
    self._handler_factory = state['handler_factory']
    self._inner = None
    self._rate_limiter = (
        _TokenBucketRateLimiter(self._cfg.rate_limit)
        if self._cfg.rate_limit is not None and self._cfg.provider != 'vertexai'
        else None)
    self._batches_counter = Metrics.counter('beam_cli', 'inference_batches')
    self._latency_dist = Metrics.distribution(
        'beam_cli', 'inference_latency_ms')

  def _ensure_inner_handler(self) -> ModelHandler[Any, Any, Any]:
    if self._inner is not None:
      return self._inner

    if self._handler_factory is not None:
      self._inner = self._handler_factory(self._cfg)
      return self._inner

    if self._cfg.provider == 'mock':
      self._inner = _MockModelHandler(self._cfg)
      return self._inner

    if self._cfg.provider == 'vertexai':
      try:
        from apache_beam.ml.inference.vertex_ai_inference import VertexAIModelHandlerJSON
      except ImportError as e:
        raise RuntimeError(
            'Vertex AI --infer requires google-cloud-aiplatform; install with: '
            'pip install "apache-beam[gcp]" google-cloud-aiplatform') from e

      v_kwargs: dict[str, Any] = {
          'num_retries': self._cfg.num_retries,
      }
      if self._cfg.rate_limit is not None:
        v_kwargs['rate_limiter'] = _TokenBucketRateLimiter(
            self._cfg.rate_limit, namespace='VertexAIModelHandlerJSON')
      self._inner = VertexAIModelHandlerJSON(
          endpoint_id=str(self._cfg.endpoint_id),
          project=str(self._cfg.project),
          location=str(self._cfg.location),
          experiment=self._cfg.experiment,
          network=self._cfg.network,
          private=self._cfg.private,
          invoke_route=self._cfg.invoke_route,
          min_batch_size=self._cfg.min_batch_size,
          max_batch_size=self._cfg.max_batch_size,
          **v_kwargs,
      )
      return self._inner

    if self._cfg.provider == 'hf':
      try:
        from apache_beam.ml.inference.huggingface_inference import HuggingFacePipelineModelHandler
      except ImportError as e:
        raise RuntimeError(
            'HuggingFace --infer requires transformers and torch; install with: '
            'pip install transformers torch safetensors') from e

      model_kwargs: dict[str, Any] = {
          'trust_remote_code': False,
          'use_safetensors': True,
      }
      load_pipeline_args: dict[str, Any] = {
          'trust_remote_code': False,
          'model_kwargs': model_kwargs,
      }
      if self._cfg.revision:
        load_pipeline_args['revision'] = self._cfg.revision
        model_kwargs['revision'] = self._cfg.revision
      tok = os.environ.get(self._cfg.hf_token_env) or self._cfg.hf_token
      if tok:
        load_pipeline_args['token'] = tok

      self._inner = HuggingFacePipelineModelHandler(
          task=self._cfg.task or '',
          model=self._cfg.model or '',
          device=self._cfg.device,
          load_pipeline_args=load_pipeline_args,
          min_batch_size=self._cfg.min_batch_size,
          max_batch_size=self._cfg.max_batch_size,
          large_model=self._cfg.large_model,
          model_copies=self._cfg.model_copies,
      )
      return self._inner

    raise ValueError(f'unsupported provider {self._cfg.provider!r}')

  def load_model(self) -> Any:
    return self._ensure_inner_handler().load_model()

  def run_inference(
      self,
      batch: Sequence[Any],
      model: Any,
      inference_args: Optional[dict[str, Any]] = None,
  ) -> Iterable[Any]:
    inner = self._ensure_inner_handler()
    if self._rate_limiter is not None:
      self._rate_limiter.allow(hits_added=len(batch))
    self._batches_counter.inc()
    t0 = time.monotonic()
    try:
      preds = list(inner.run_inference(batch, model, inference_args))
      if len(preds) != len(batch):
        raise RuntimeError(
            f'model returned {len(preds)} predictions for batch of {len(batch)}'
        )
      return preds
    except Exception as batch_exc:  # pylint: disable=broad-except
      if not self._catch_singleton_errors:
        raise RuntimeError(
            _sanitize_infer_error(batch_exc, self._cfg.provider)) from None
      if len(batch) == 1:
        return [
            _InferErrorSentinel(
                err_type=type(batch_exc).__name__,
                msg=_sanitize_infer_error(batch_exc, self._cfg.provider))
        ]
      # Singleton bisect against the already-loaded model so N - 1 healthy rows
      # succeed without triggering a model reload!
      isolated: list[Any] = []
      for single_item in batch:
        try:
          single_preds = list(
              inner.run_inference([single_item], model, inference_args))
          if len(single_preds) != 1:
            raise RuntimeError(
                f'singleton retry returned {len(single_preds)} predictions')
          isolated.append(single_preds[0])
        except Exception as item_exc:  # pylint: disable=broad-except
          isolated.append(
              _InferErrorSentinel(
                  err_type=type(item_exc).__name__,
                  msg=_sanitize_infer_error(item_exc, self._cfg.provider)))
      return isolated
    finally:
      elapsed_ms = int(round(max(0.0, (time.monotonic() - t0) * 1000.0)))
      self._latency_dist.update(elapsed_ms)

  def get_num_bytes(self, batch: Sequence[Any]) -> int:
    try:
      return len(json.dumps(batch, default=str).encode('utf-8'))
    except Exception:  # pylint: disable=broad-except
      return len(batch) * 64

  def get_metrics_namespace(self) -> str:
    if self._cfg.provider == 'vertexai':
      return 'BeamML_VertexAIModelHandlerJSON'
    if self._cfg.provider == 'hf':
      return 'BeamML_HuggingFacePipelineModelHandler'
    return 'BeamML_MockModelHandler'

  def share_model_across_processes(self) -> bool:
    return bool(self._cfg.provider == 'hf' and self._cfg.large_model)

  def model_copies(self) -> int:
    return int(self._cfg.model_copies or 1)


class _PreprocessAndBatchDoFn(beam.DoFn):
  """Extracts model inputs, enforces Vertex `< 1.4 MB` limits, and batches per window.

  Emits each buffered batch stamped with `min(ts)` of its constituent rows so
  watermarks never advance past buffered event timestamps and downstream
  unpacking restores exact per-row `(ts, window)` monotonically (`ts_i >= min_ts`).
  """
  def __init__(self, cfg: InferConfig, has_dlq: bool):
    self._cfg = cfg
    self._has_dlq = has_dlq
    self._err_counter = Metrics.counter('beam_cli', 'inference_errors')

  def start_bundle(self):
    self._win_buffers: dict[Any,
                            list[tuple[tuple[dict[str, Any], Any, Any],
                                       Any]]] = {}
    self._win_bytes: dict[Any, int] = {}
    self._win_started: dict[Any, float] = {}

  def _flush_window(self, win: Any):
    buf = self._win_buffers.get(win)
    if not buf:
      return
    items = list(buf)
    buf.clear()
    self._win_bytes[win] = 0
    self._win_started.pop(win, None)
    min_ts = min(item[0][1] for item in items)
    yield WindowedValue(items, min_ts, (win, ))

  def process(
      self,
      element,
      ts=beam.DoFn.TimestampParam,
      win=beam.DoFn.WindowParam,
  ):
    try:
      row_dict = interval_join._to_plain_dict(element)
      model_input = _build_model_input(row_dict, self._cfg)
    except Exception as exc:  # pylint: disable=broad-except
      self._err_counter.inc()
      msg = _sanitize_infer_error(exc, self._cfg.provider)
      if self._has_dlq:
        yield TaggedOutput(
            'errors',
            WindowedValue(
                _make_infer_dlq_row(
                    'CliInfer',
                    type(exc).__name__,
                    msg,
                    element,
                    redact_fields=self._cfg.dlq_redact_fields),
                ts, (win, )))
        return
      raise ValueError(msg) from None

    elem_bytes = 0
    enforce_payload_cap = (
        self._cfg.provider == 'vertexai' and not self._cfg.private)
    if enforce_payload_cap:
      try:
        encoded = json.dumps(
            model_input, separators=(',', ':'),
            allow_nan=False).encode('utf-8')
      except (TypeError, ValueError) as exc:
        self._err_counter.inc()
        msg = _sanitize_infer_error(exc, self._cfg.provider)
        if self._has_dlq:
          yield TaggedOutput(
              'errors',
              WindowedValue(
                  _make_infer_dlq_row(
                      'CliInfer',
                      'SerializationError',
                      msg,
                      row_dict,
                      redact_fields=self._cfg.dlq_redact_fields),
                  ts, (win, )))
          return
        raise ValueError(msg) from None

      elem_bytes = len(encoded) + 2
      if elem_bytes >= self._cfg.max_payload_bytes:
        self._err_counter.inc()
        msg = (
            f'CliInfer (vertexai) serialized instance size ({elem_bytes} bytes) '
            f'exceeds public endpoint safety limit ({self._cfg.max_payload_bytes} bytes)'
        )
        if self._has_dlq:
          yield TaggedOutput(
              'errors',
              WindowedValue(
                  _make_infer_dlq_row(
                      'CliInfer',
                      'PayloadTooLargeError',
                      msg,
                      row_dict,
                      redact_fields=self._cfg.dlq_redact_fields),
                  ts, (win, )))
          return
        raise ValueError(msg)

    buf = self._win_buffers.setdefault(win, [])
    cur_bytes = self._win_bytes.get(win, 32)
    now = time.monotonic()
    started = self._win_started.get(win)

    should_flush_before = False
    if buf:
      if len(buf) >= self._cfg.max_batch_size:
        should_flush_before = True
      elif (enforce_payload_cap and
            cur_bytes + elem_bytes >= self._cfg.max_payload_bytes):
        should_flush_before = True
      elif (self._cfg.max_batch_duration_secs > 0 and started is not None and
            (now - started) >= self._cfg.max_batch_duration_secs and
            len(buf) >= self._cfg.min_batch_size):
        should_flush_before = True

    if should_flush_before:
      yield from self._flush_window(win)
      buf = self._win_buffers.setdefault(win, [])
      cur_bytes = 32

    if not buf:
      self._win_started[win] = now
    buf.append(((row_dict, ts, win), model_input))
    self._win_bytes[win] = cur_bytes + elem_bytes

    if len(buf) >= self._cfg.max_batch_size:
      yield from self._flush_window(win)

  def finish_bundle(self):
    for win in list(self._win_buffers.keys()):
      yield from self._flush_window(win)


class _UnpackInferResultsDoFn(beam.DoFn):
  """Restores per-element `(ts, win)`, formats typed predictions, and routes sentinels to DLQ."""
  def __init__(self, cfg: InferConfig, has_dlq: bool):
    self._cfg = cfg
    self._has_dlq = has_dlq
    self._default_mid = (
        cfg.endpoint_id or cfg.model or cfg.mock_model_id or cfg.provider)
    self._rows_counter = Metrics.counter('beam_cli', 'inference_rows')
    self._err_counter = Metrics.counter('beam_cli', 'inference_errors')

  def process(self, keyed_result):
    key_meta, pred = keyed_result
    row_dict, orig_ts, orig_win = key_meta

    if isinstance(pred, _InferErrorSentinel):
      self._err_counter.inc()
      if self._has_dlq:
        yield TaggedOutput(
            'errors',
            WindowedValue(
                _make_infer_dlq_row(
                    'CliInfer',
                    pred.err_type,
                    pred.msg,
                    row_dict,
                    redact_fields=self._cfg.dlq_redact_fields),
                orig_ts, (orig_win, )))
        return
      raise RuntimeError(pred.msg)

    try:
      score, label, model_id, pred_dict, raw_json = _normalize_prediction(
          pred, str(self._default_mid))
      merged = dict(row_dict)

      if self._cfg.flatten:
        merged[f'{self._cfg.tag}_score'] = score
        merged[f'{self._cfg.tag}_label'] = label
        merged[f'{self._cfg.tag}_model_id'] = model_id
        for out_col, out_dtype in self._cfg.output_columns:
          raw_v = pred_dict.get(out_col) if pred_dict is not None else None
          merged[out_col] = enrich_mod._coerce_typed_value(raw_v, out_dtype)
      else:
        otype = self._cfg.output_type
        if otype == 'float':
          merged[self._cfg.tag] = (
              score if score is not None else enrich_mod._coerce_typed_value(
                  label, 'float'))
        elif otype == 'str':
          merged[self._cfg.tag] = (
              label if label is not None else
              (str(score) if score is not None else raw_json))
        elif otype == 'json':
          merged[self._cfg.tag] = raw_json
        elif otype == 'int':
          base_v = score if score is not None else label
          merged[self._cfg.tag] = enrich_mod._coerce_typed_value(base_v, 'int')
        elif otype == 'bool':
          base_v = label if label is not None else score
          merged[self._cfg.tag] = enrich_mod._coerce_typed_value(base_v, 'bool')
        else:
          merged[self._cfg.tag] = beam.Row(
              score=score,
              label=label,
              model_id=model_id,
              raw_json=raw_json,
          )
      out_row = beam.Row(**merged)
    except Exception as exc:  # pylint: disable=broad-except
      self._err_counter.inc()
      msg = _sanitize_infer_error(exc, self._cfg.provider)
      if self._has_dlq:
        yield TaggedOutput(
            'errors',
            WindowedValue(
                _make_infer_dlq_row(
                    'CliInfer',
                    type(exc).__name__,
                    msg,
                    row_dict,
                    redact_fields=self._cfg.dlq_redact_fields),
                orig_ts, (orig_win, )))
        return
      raise ValueError(msg) from None

    self._rows_counter.inc()
    yield WindowedValue(out_row, orig_ts, (orig_win, ))


class _ExplodeFailedBatchDoFn(beam.DoFn):
  """Explodes `RunInference.with_exception_handling()` `(failed_batch, exc)` into per-row DLQs."""
  def __init__(self, cfg: InferConfig):
    self._cfg = cfg
    self._err_counter = Metrics.counter('beam_cli', 'inference_errors')

  def process(self, failed_record):
    failed_items, exc_info = failed_record
    msg = _sanitize_infer_error(exc_info, self._cfg.provider)
    items_seq = (
        failed_items if isinstance(failed_items,
                                   (list, tuple)) else [failed_items])
    for item in items_seq:
      self._err_counter.inc()
      if (isinstance(item, tuple) and len(item) == 2 and
          isinstance(item[0], tuple) and len(item[0]) == 3):
        row_dict, orig_ts, orig_win = item[0]
        yield WindowedValue(
            _make_infer_dlq_row(
                'CliInfer',
                'InferenceBatchError',
                msg,
                row_dict,
                redact_fields=self._cfg.dlq_redact_fields),
            orig_ts, (orig_win, ))
      else:
        yield _make_infer_dlq_row(
            'CliInfer',
            'InferenceBatchError',
            msg,
            item,
            redact_fields=self._cfg.dlq_redact_fields)


def _compute_output_schema(
    input_element_type: Any,
    cfg: InferConfig,
) -> RowTypeConstraint | None:
  try:
    in_fields = list(schemas.named_fields_from_element_type(input_element_type))
  except Exception:  # pylint: disable=broad-except
    return None
  if not in_fields:
    return None

  out_map: dict[str, Any] = {name: typ for name, typ in in_fields}
  if cfg.flatten:
    out_map[f'{cfg.tag}_score'] = Optional[float]
    out_map[f'{cfg.tag}_label'] = Optional[str]
    out_map[f'{cfg.tag}_model_id'] = Optional[str]
    for col, dtype in cfg.output_columns:
      out_map[col] = Optional[_TYPED_COL_PY_MAP.get(dtype, str)]
  else:
    if cfg.output_type == 'float':
      out_map[cfg.tag] = Optional[float]
    elif cfg.output_type in ('str', 'json'):
      out_map[cfg.tag] = Optional[str]
    elif cfg.output_type == 'int':
      out_map[cfg.tag] = Optional[int]
    elif cfg.output_type == 'bool':
      out_map[cfg.tag] = Optional[bool]
    else:
      out_map[cfg.tag] = Optional[_NESTED_PREDICTION_SCHEMA]

  return RowTypeConstraint.from_fields(list(out_map.items()))


class CliInfer(beam.PTransform):
  """Hardened `RunInference` composite `PTransform` for `beam pipe --infer`."""
  def __init__(
      self,
      cfg: InferConfig | Mapping[str, Any],
      *,
      error_handling: Mapping[str, Any] | None = None,
      handler_factory=None,
  ):
    super().__init__()
    self._cfg = (
        cfg if isinstance(cfg, InferConfig) else parse_infer_config(cfg))
    self._error_handling = dict(error_handling) if error_handling else None
    self._handler_factory = handler_factory

  def expand(self, pcoll):
    has_dlq = bool(self._error_handling and self._error_handling.get('output'))
    out_schema = _compute_output_schema(pcoll.element_type, self._cfg)

    pre = (
        pcoll
        | 'PreprocessAndBatch' >> beam.ParDo(
            _PreprocessAndBatchDoFn(self._cfg, has_dlq=has_dlq)).with_outputs(
                'errors', main='batches'))

    lazy_handler = _LazyCliModelHandler(
        self._cfg,
        catch_singleton_errors=has_dlq,
        handler_factory=self._handler_factory,
    )
    keyed_prebatched = KeyedModelHandler(lazy_handler).with_no_batching()
    inf_args = dict(
        self._cfg.inference_args) if self._cfg.inference_args else None

    dlq_branches: list[beam.PCollection] = []
    if has_dlq:
      dlq_branches.append(
          pre['errors']
          | 'TypePreErrors' >> beam.Map(lambda r: r).with_output_types(
              _DLQ_ROW_SCHEMA))
      inferred, ri_dlq = (
          pre['batches']
          | 'RunInference' >> RunInference(
              model_handler=keyed_prebatched,
              inference_args=inf_args,
          ).with_exception_handling())
      if ri_dlq.failed_inferences is not None:
        dlq_branches.append(
            ri_dlq.failed_inferences
            | 'ExplodeFailedBatches' >> beam.ParDo(
                _ExplodeFailedBatchDoFn(self._cfg)).with_output_types(
                    _DLQ_ROW_SCHEMA))
      for idx, bad_pre in enumerate(ri_dlq.failed_preprocessing or ()):
        dlq_branches.append(
            bad_pre
            | f'ExplodeFailedPre_{idx}' >> beam.ParDo(
                _ExplodeFailedBatchDoFn(self._cfg)).with_output_types(
                    _DLQ_ROW_SCHEMA))
      for idx, bad_post in enumerate(ri_dlq.failed_postprocessing or ()):
        dlq_branches.append(
            bad_post
            | f'ExplodeFailedPost_{idx}' >> beam.ParDo(
                _ExplodeFailedBatchDoFn(self._cfg)).with_output_types(
                    _DLQ_ROW_SCHEMA))
    else:
      inferred = (
          pre['batches']
          | 'RunInference' >> RunInference(
              model_handler=keyed_prebatched,
              inference_args=inf_args,
          ))

    unpacked = (
        inferred
        | 'UnpackPredictions' >> beam.ParDo(
            _UnpackInferResultsDoFn(self._cfg, has_dlq=has_dlq)).with_outputs(
                'errors', main='good'))

    good = unpacked['good']
    if out_schema is not None:
      good = good | 'TypeOutput' >> beam.Map(lambda r: r).with_output_types(
          out_schema)

    if has_dlq:
      dlq_branches.append(
          unpacked['errors']
          | 'TypePostErrors' >> beam.Map(lambda r: r).with_output_types(
              _DLQ_ROW_SCHEMA))
      err_tag = str(self._error_handling['output'])
      merged_errors = (
          dlq_branches
          | 'FlattenInferDlq' >>
          beam.Flatten().with_output_types(_DLQ_ROW_SCHEMA))
      return {'good': good, err_tag: merged_errors}

    return good


def cli_infer(
    provider: Optional[str] = None,
    handler: Optional[str] = None,
    tag: str = 'prediction',
    inference_tag: Optional[str] = None,
    flatten: bool = False,
    output_type: str = 'row',
    output_columns: Optional[Any] = None,
    columns: Optional[Any] = None,
    fields: Optional[Any] = None,
    infer_fields: Optional[Any] = None,
    input_col: Optional[str] = None,
    min_batch_size: int = 1,
    max_batch_size: Optional[int] = None,
    max_batch_duration_secs: float = 0.2,
    rate_limit: Optional[float] = None,
    num_retries: int = 3,
    dlq_redact_fields: Optional[Any] = None,
    inference_args: Optional[Mapping[str, Any]] = None,
    parameters: Optional[Mapping[str, Any]] = None,
    # Vertex AI
    endpoint_id: Optional[str] = None,
    endpoint: Optional[str] = None,
    project: Optional[str] = None,
    location: Optional[str] = None,
    region: Optional[str] = None,
    private: Optional[bool] = None,
    network: Optional[str] = None,
    experiment: Optional[str] = None,
    invoke_route: Optional[str] = None,
    max_payload_bytes: Optional[int] = None,
    # HuggingFace
    task: Optional[str] = None,
    model: Optional[str] = None,
    revision: Optional[str] = None,
    device: Optional[str] = None,
    large_model: Optional[bool] = None,
    model_copies: Optional[int] = None,
    hf_token: Optional[str] = None,
    hf_token_env: Optional[str] = None,
    # Mock
    score: Optional[float] = None,
    label: Optional[str] = None,
    model_id: Optional[str] = None,
    mode: Optional[str] = None,
    score_col: Optional[str] = None,
    scale: Optional[float] = None,
    threshold: Optional[float] = None,
    pos_label: Optional[str] = None,
    neg_label: Optional[str] = None,
    response_json: Optional[Any] = None,
    fail_on: Optional[str] = None,
    fail_batch_once: Optional[bool] = None,
    # Beam YAML error handling
    error_handling: Optional[Mapping[str, Any]] = None,
) -> CliInfer:
  """Beam YAML provider entrypoint for `type: CliInfer` / `type: Infer`."""
  raw: dict[str, Any] = {
      'tag': inference_tag or tag,
      'flatten': flatten,
      'output_type': output_type,
      'min_batch_size': min_batch_size,
      'max_batch_duration_secs': max_batch_duration_secs,
      'num_retries': num_retries,
  }
  optional_pairs = (
      ('provider', provider),
      ('handler', handler),
      ('output_columns', output_columns),
      ('columns', columns),
      ('fields', fields),
      ('infer_fields', infer_fields),
      ('input_col', input_col),
      ('max_batch_size', max_batch_size),
      ('rate_limit', rate_limit),
      ('dlq_redact_fields', dlq_redact_fields),
      ('inference_args', inference_args),
      ('parameters', parameters),
      ('endpoint_id', endpoint_id),
      ('endpoint', endpoint),
      ('project', project),
      ('location', location),
      ('region', region),
      ('private', private),
      ('network', network),
      ('experiment', experiment),
      ('invoke_route', invoke_route),
      ('max_payload_bytes', max_payload_bytes),
      ('task', task),
      ('model', model),
      ('revision', revision),
      ('device', device),
      ('large_model', large_model),
      ('model_copies', model_copies),
      ('hf_token', hf_token),
      ('hf_token_env', hf_token_env),
      ('score', score),
      ('label', label),
      ('model_id', model_id),
      ('mode', mode),
      ('score_col', score_col),
      ('scale', scale),
      ('threshold', threshold),
      ('pos_label', pos_label),
      ('neg_label', neg_label),
      ('response_json', response_json),
      ('fail_on', fail_on),
      ('fail_batch_once', fail_batch_once),
  )
  for k, v in optional_pairs:
    if v is not None:
      raw[k] = v
  cfg = parse_infer_config(raw)
  return CliInfer(cfg, error_handling=error_handling)
