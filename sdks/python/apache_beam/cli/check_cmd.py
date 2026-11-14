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

"""Offline Beam YAML validator & execution DAG planner (`beam check`).

Security guarantee:
- Default `beam check FILE.yaml` (or `-` stdin) is 100% offline: parses YAML
  with `SafeLoader` (never executing `providers:` code or Jinja SSTI) and
  computes a deterministic `plan_hash` (`sha256` of canonical pipeline spec).
- Full provider/cross-language graph expansion (`--plan`) is explicitly gated
  behind `--trust-providers` when custom `providers:` are declared in the YAML!
"""

import contextlib
import hashlib
import json
import os
import sys
from typing import Any

EXIT_OK = 0
EXIT_USAGE = 2


class CheckError(ValueError):
  """Raised when `beam check` finds a structural or schema error."""


def run_check(
    file_or_dash: str,
    *,
    expand_plan: bool = False,
    trust_providers: bool = False,
    jinja_variables_json: str | None = None,
    pipeline_args: list[str] | None = None,
    portable: bool = False,
    verbose: bool = False,
) -> dict[str, Any]:
  """Validates a Beam YAML pipeline and returns a structured validation report."""
  import yaml

  from . import output as out_mod

  if file_or_dash == '-':
    try:
      raw_text = out_mod.read_bounded_text(sys.stdin, label='<stdin> YAML')
    except ValueError as e:
      raise CheckError(str(e)) from None
    source_label = '<stdin>'
  else:
    if '://' not in file_or_dash and not os.path.isfile(file_or_dash):
      raise CheckError(f'no such file: {file_or_dash!r}')
    try:
      if '://' in file_or_dash:
        from apache_beam.io.filesystems import FileSystems
        with FileSystems.open(file_or_dash) as f:
          raw_text = out_mod.read_bounded_text(f, label=file_or_dash)
      else:
        raw_text = out_mod.read_bounded_text(file_or_dash, label=file_or_dash)
    except ValueError as e:
      raise CheckError(str(e)) from None
    source_label = file_or_dash

  if jinja_variables_json:
    try:
      raw_text = out_mod.safe_expand_jinja(raw_text, jinja_variables_json)
    except ValueError as e:
      raise CheckError(f'{source_label}: {e}') from None
  elif '{{' in raw_text or '{%' in raw_text:
    from . import package as pkg_mod
    try:
      out_mod.validate_safe_jinja_template(raw_text)
      pkg_mod.extract_jinja_parameters(raw_text)
    except (ValueError, pkg_mod.PackageError) as e:
      raise CheckError(f'Jinja2 syntax error in {source_label}: {e}') from None
  try:
    spec = out_mod.safe_yaml_load(raw_text)
  except yaml.YAMLError as e:
    if (not jinja_variables_json and '{{' in raw_text and
        'alias expansion' not in str(e) and 'maximum' not in str(e)):
      from . import package as pkg_mod
      try:
        spec = pkg_mod._validate_yaml_with_jinja_placeholders(raw_text)
      except pkg_mod.PackageError:
        raise CheckError(f'YAML syntax error in {source_label}: {e}') from None
    else:
      raise CheckError(f'YAML syntax error in {source_label}: {e}') from None

  if not isinstance(spec, dict) or 'pipeline' not in spec:
    raise CheckError(
        f'{source_label}: top-level YAML mapping must contain a "pipeline:" key')

  pipeline_block = spec['pipeline']
  if not isinstance(pipeline_block, dict) or 'transforms' not in pipeline_block:
    raise CheckError(
        f'{source_label}: "pipeline:" must be a mapping containing "transforms:"'
    )
  transforms = pipeline_block['transforms']
  if not isinstance(transforms, list) or not transforms:
    raise CheckError(
        f'{source_label}: "pipeline.transforms" must be a non-empty list')

  stages: list[dict[str, Any]] = []
  _check_transform_keys(transforms, source_label)
  for extra in ('source', 'sink', 'extra_transforms'):
    if extra in pipeline_block:
      block = pipeline_block[extra]
      _check_transform_keys(
          block if isinstance(block, list) else [block], source_label)
  for idx, t in enumerate(transforms):
    if not isinstance(t, dict) or 'type' not in t:
      raise CheckError(
          f'{source_label}: transform #{idx + 1} must be a mapping with "type:"'
      )
    if t['type'] in ('CliIntervalJoin', 'IntervalJoin') and isinstance(
        t.get('config'), dict):
      from . import interval_join
      try:
        interval_join.parse_interval_join_config(t['config'])
      except ValueError as e:
        raise CheckError(
            f'{source_label}: invalid {t["type"]} config: {e}') from None
    if t['type'] in ('CliDeduplicate', 'Deduplicate') and isinstance(
        t.get('config'), dict):
      from . import dedup
      try:
        dedup.parse_deduplicate_config(t['config'])
      except ValueError as e:
        raise CheckError(
            f'{source_label}: invalid {t["type"]} config: {e}') from None
    if t['type'] in ('CliEnrich', 'Enrich') and isinstance(t.get('config'),
                                                           dict):
      from . import enrich
      try:
        enrich.parse_enrich_config(t['config'])
      except ValueError as e:
        raise CheckError(
            f'{source_label}: invalid {t["type"]} config: {e}') from None
    stages.append({
        'index': idx,
        'name': t.get('name') or t['type'],
        'type': t['type'],
    })

  from . import dlq
  _builtin_cli_targets = frozenset(
      dlq.cli_yaml_provider_spec()['transforms'].values())
  providers_list = spec.get('providers') or []
  has_custom_providers = bool(providers_list)
  has_untrusted_providers = False
  for prov in providers_list:
    if not isinstance(prov, dict) or prov.get('type') != 'python' or prov.get(
        'config') not in ({}, None):
      has_untrusted_providers = True
      break
    t_map = prov.get('transforms')
    if not isinstance(t_map, dict) or any(
        val not in _builtin_cli_targets for val in t_map.values()):
      has_untrusted_providers = True
      break
  has_untrusted_code = _has_executable_python_transforms(pipeline_block)

  canonical_json = json.dumps(spec, sort_keys=True, separators=(',', ':'))
  plan_hash = 'sha256:' + hashlib.sha256(
      canonical_json.encode('utf-8')).hexdigest()

  expanded_transforms: list[str] | None = None
  plan_skipped_reason: str | None = None
  if expand_plan:
    if (has_untrusted_providers or has_untrusted_code) and not trust_providers:
      raise CheckError(
          f'{source_label} declares custom "providers:" or inline Python '
          'transforms (such as PyTransform or language: python callables) '
          'which execute code during --plan expansion; re-run with '
          '--trust-providers to allow code execution')
    import apache_beam as beam
    from apache_beam.yaml import main as yaml_main
    from . import pipe_spec
    dumper = getattr(yaml, 'CSafeDumper', yaml.SafeDumper)
    clean_yaml = yaml.dump(spec, Dumper=dumper, sort_keys=False)
    options, constructor = (
        yaml_main.build_pipeline_components_from_yaml(
            clean_yaml, list(pipeline_args or []), validate_schema='generic'))
    p = beam.Pipeline(
        options=options,
        display_data={'yaml': pipe_spec.redact_yaml_text(clean_yaml)})
    try:
      with _quiet_expansion(enabled=not verbose):
        constructor(p)
        proto = p.to_runner_api()
      expanded_transforms = sorted(
          t.unique_name for t in proto.components.transforms.values()
          if t.unique_name and '/' not in t.unique_name)
    except Exception as e:  # pylint: disable=broad-except
      # Some transforms list cloud paths or read schemas while the pipeline is
      # built (e.g. ReadFromCsv globs gs://...). Without network access or
      # credentials that is not a defect in the YAML, so report the static
      # result and why the full plan was skipped rather than failing.
      if not _is_remote_access_error(e):
        raise
      plan_skipped_reason = (
          'check passed (offline static validation); full plan expansion was '
          'skipped because it needs access to remote storage or credentials '
          'that are not available here')
      plan_skipped_detail = (
          'informational, not an error (the check passed): '
          f'{str(e).splitlines()[0][:300]}')

  check_warnings: list[str] = _check_parquet_untyped_python_expressions(pipeline_block)
  non_portable: list[str] = _check_portability(pipeline_block)
  if portable and non_portable:
    raise CheckError(
        f'{source_label}: --portable rejected non-portable construct(s): ' +
        ', '.join(non_portable) +
        ' (these constructs run on local engines but fail on distributed '
        'runners Dataflow/Flink/Spark)')
  if non_portable and not portable:
    check_warnings.extend(
        [f'non-portable construct: {np}' for np in non_portable])

  result = {
      'valid': True,
      'source': source_label,
      'mode': 'expanded_plan' if expanded_transforms is not None else
      'offline_static',
      'plan_hash': plan_hash,
      'transform_count': len(stages),
      'transforms': stages,
      'has_custom_providers': has_custom_providers,
      'expanded_top_level_transforms': expanded_transforms,
      'portable_to_runner': len(non_portable) == 0,
      'non_portable_constructs': non_portable,
      'warnings': check_warnings,
  }
  if plan_skipped_reason:
    result['plan_skipped_reason'] = plan_skipped_reason
    result['plan_skipped_detail'] = plan_skipped_detail
  return result


def _check_portability(pipeline_block: dict[str, Any]) -> list[str]:
  """Recursively checks for dialect or provider constructs non-portable to distributed Beam runners.

  Why Pre-Flight Portability Verification:
  Beam's distributed runners (DataflowRunner, FlinkRunner, SparkRunner) execute
  `type: Sql` transforms via Apache Calcite and the Beam Java SQL expansion service.
  Calcite conforms strictly to ANSI SQL rules and rejects engine-specific dialect
  functions such as `strftime`, `to_utc`, `median`, `percentile`, `qualify`, or
  single-document keyed JSON sinks (`key=`).

  Catching non-portable constructs statically in `beam check --portable` prevents
  submitting expensive distributed pipelines that would fail minutes into execution.
  """
  issues: list[str] = []
  from . import pipe_sql

  def _scan_transforms(t_list):
    for t in (t_list or []):
      if not isinstance(t, dict):
        continue
      t_type = str(t.get('type') or '')
      cfg = t.get('config') or {}

      if t_type.lower() in ('sql', 'sqltransform') or 'query' in cfg:
        q = str(cfg.get('query') or '')
        if q:
          non_port = pipe_sql.analyze_sql_portability(q)
          for item in non_port:
            issues.append(f'SQL query uses {item}')

      if 'key' in cfg and t_type.lower() in ('writeasjson', 'writetojson'):
        issues.append('keyed JSON output (key=)')

      if isinstance(t.get('transforms'), list):
        _scan_transforms(t['transforms'])
      for extra in ('source', 'sink', 'extra_transforms'):
        if extra in t:
          b = t[extra]
          _scan_transforms(b if isinstance(b, list) else [b])

  for extra in ('source', 'sink', 'extra_transforms', 'transforms'):
    if extra in pipeline_block:
      b = pipeline_block[extra]
      _scan_transforms(b if isinstance(b, list) else [b])
  return sorted(set(issues))


# Keys Beam YAML accepts on a transform (pipeline.schema.yaml). Anything else,
# such as `path:` next to `type: ReadFromCsv`, is a parameter that belongs
# under `config:`; Beam only reports it at expansion, with a schema dump.
_LEAF_TRANSFORM_KEYS = frozenset([
    'type', 'name', 'input', 'output', 'output_schema', 'windowing',
    'resource_hints', 'config', '__line__', '__uuid__'
])
_NESTED_TRANSFORM_KEYS = frozenset([
    'type', 'name', 'input', 'output', 'windowing', 'resource_hints',
    'transforms', 'extra_transforms', 'source', 'sink', '__line__', '__uuid__'
])


def _check_transform_keys(transforms, source_label: str, path: str = '') -> None:
  """Offline check that each transform only uses keys Beam YAML accepts."""
  for idx, t in enumerate(transforms or []):
    if not isinstance(t, dict) or 'type' not in t:
      continue
    label = f'{path}transform #{idx + 1} ({t.get("name") or t["type"]})'
    nested = t['type'] in ('composite', 'chain')
    allowed = _NESTED_TRANSFORM_KEYS if nested else _LEAF_TRANSFORM_KEYS
    unknown = [k for k in t if k not in allowed]
    if unknown:
      if nested:
        raise CheckError(
            f'{source_label}: {label} has unknown keys {", ".join(unknown)}; '
            f'a {t["type"]} transform takes {", ".join(sorted(allowed - {"__line__", "__uuid__"}))}')
      example = unknown[0]
      raise CheckError(
          f'{source_label}: {label} has {", ".join(unknown)} outside '
          f'config:; transform parameters go under config:, for example\n'
          f'  - type: {t["type"]}\n    config:\n      {example}: ...')
    if 'config' in t and not isinstance(t['config'], dict):
      raise CheckError(f'{source_label}: {label}: config: must be a mapping')
    if nested:
      _check_transform_keys(t.get('transforms'), source_label, f'{label} > ')
      for extra in ('source', 'sink'):
        if isinstance(t.get(extra), dict):
          _check_transform_keys([t[extra]], source_label, f'{label} > ')


def _extract_untyped_python_map_fields(transform: dict[str, Any]) -> list[str]:
  """Returns field names of untyped Python expressions in MapToFields."""
  if not isinstance(transform, dict) or transform.get('type') != 'MapToFields':
    return []
  cfg = transform.get('config')
  if not isinstance(cfg, dict):
    return []
  lang = str(cfg.get('language') or '').lower()
  if lang != 'python':
    return []
  fields = cfg.get('fields')
  if not isinstance(fields, dict):
    return []
  untyped: list[str] = []
  for fname, fval in fields.items():
    if isinstance(fval, str):
      untyped.append(str(fname))
    elif isinstance(fval, dict) and not fval.get('output_type') and not fval.get('type'):
      untyped.append(str(fname))
  return untyped


def _check_parquet_untyped_python_expressions(
    pipeline_block: dict[str, Any]) -> list[str]:
  """Warns when untyped `language: python` in MapToFields feeds WriteToParquet."""
  warnings: list[str] = []
  if not isinstance(pipeline_block, dict):
    return warnings

  def _flatten_sequence(block: dict[str, Any]) -> list[dict[str, Any]]:
    seq: list[dict[str, Any]] = []
    if 'source' in block:
      src = block['source']
      seq.extend(src if isinstance(src, list) else [src])
    if 'transforms' in block and isinstance(block['transforms'], list):
      seq.extend(block['transforms'])
    if 'sink' in block:
      sink = block['sink']
      seq.extend(sink if isinstance(sink, list) else [sink])
    return [t for t in seq if isinstance(t, dict)]

  def _scan_sequence(seq: list[dict[str, Any]]):
    for idx, t in enumerate(seq):
      t_type = t.get('type')
      untyped = _extract_untyped_python_map_fields(t)
      if untyped:
        t_name = t.get('name') or f'MapToFields (stage #{idx + 1})'
        for downstream in seq[idx + 1:]:
          d_type = downstream.get('type')
          if d_type == 'WriteToParquet':
            warnings.append(
                f'{t_name} has untyped Python expression(s) for '
                f'{", ".join(repr(f) for f in untyped[:5])} preceding WriteToParquet; '
                'consider adding output_type: <type> (or using language: sql) '
                'to prevent PyArrow LogicalType Any conversion errors.')
            break
          elif d_type in ('SqlTransform', 'Sql', 'beam:transform:org.apache.beam:sql:v1'):
            break
          elif d_type in ('chain', 'composite'):
            inner = _flatten_sequence(downstream)
            if any(x.get('type') == 'WriteToParquet' for x in inner):
              warnings.append(
                  f'{t_name} has untyped Python expression(s) for '
                  f'{", ".join(repr(f) for f in untyped[:5])} preceding WriteToParquet; '
                  'consider adding output_type: <type> (or using language: sql) '
                  'to prevent PyArrow LogicalType Any conversion errors.')
              break
      elif t_type in ('chain', 'composite'):
        _scan_sequence(_flatten_sequence(t))

  root_seq = _flatten_sequence(pipeline_block)
  _scan_sequence(root_seq)

  named_transforms: dict[str, dict[str, Any]] = {}
  for t in root_seq:
    if 'name' in t and isinstance(t['name'], str):
      named_transforms[t['name']] = t

  for t in root_seq:
    if t.get('type') == 'WriteToParquet':
      inp = t.get('input')
      inputs = inp if isinstance(inp, list) else ([inp] if isinstance(inp, str) else [])
      for ref in inputs:
        producer = named_transforms.get(ref)
        if producer:
          untyped = _extract_untyped_python_map_fields(producer)
          if untyped:
            p_name = producer.get('name') or 'MapToFields'
            msg = (
                f'{p_name} has untyped Python expression(s) for '
                f'{", ".join(repr(f) for f in untyped[:5])} preceding WriteToParquet; '
                'consider adding output_type: <type> (or using language: sql) '
                'to prevent PyArrow LogicalType Any conversion errors.')
            if msg not in warnings:
              warnings.append(msg)

  return warnings


_SAFE_CALLABLE_IMPORTS = frozenset(
    ['json', 'math', 're', 'datetime', 'decimal', 'base64'])
_DANGEROUS_CALLABLE_NAMES = frozenset([
    '__import__', 'eval', 'exec', 'compile', 'open', 'input', 'breakpoint',
    'globals', 'locals', 'vars', 'dir', 'delattr', 'setattr', 'builtins',
    'os', 'sys', 'subprocess', 'shutil', 'socket', 'urllib', 'http', 'ctypes',
    'importlib', 'pathlib', 'pickle', 'marshal',
])


def _is_safe_inline_callable(src: str) -> bool:
  """True only for pure side-effect-free `def fn(row):` or `lambda row:`."""
  import ast
  try:
    mod = ast.parse(src)
  except SyntaxError:
    return False
  if len(mod.body) != 1:
    return False
  stmt = mod.body[0]
  if isinstance(stmt, ast.FunctionDef):
    args = stmt.args
    if (stmt.decorator_list or stmt.returns is not None or args.defaults or
        any(d is not None for d in args.kw_defaults)):
      return False
    all_arg_nodes = (
        list(args.posonlyargs) + list(args.args) + list(args.kwonlyargs) +
        ([args.vararg] if args.vararg else []) +
        ([args.kwarg] if args.kwarg else []))
    if any(a.annotation is not None for a in all_arg_nodes):
      return False
  elif isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Lambda):
    args = stmt.value.args
    if args.defaults or any(d is not None for d in args.kw_defaults):
      return False
  else:
    return False

  for node in ast.walk(stmt):
    if isinstance(node, ast.Import):
      if any(alias.name not in _SAFE_CALLABLE_IMPORTS for alias in node.names):
        return False
    elif isinstance(node, ast.ImportFrom):
      if (node.module or '') not in _SAFE_CALLABLE_IMPORTS:
        return False
    elif isinstance(node, ast.Name):
      if node.id in _DANGEROUS_CALLABLE_NAMES or (node.id.startswith('__') and
                                                  node.id != '__row__'):
        return False
    elif isinstance(node, ast.Attribute):
      if node.attr.startswith('__') or node.attr in ('gi_frame',
                                                     'f_globals',
                                                     'f_builtins',
                                                     'tb_frame',
                                                     'cr_frame',
                                                     'ag_frame'):
        return False
  return True


_UDF_HOOK_KEYS = frozenset([
    'preprocess',
    'postprocess',
    'inference_fn',
    'row_key_fn',
    'join_fn',
])


def _config_has_untrusted_python(obj: Any) -> bool:
  if isinstance(obj, dict):
    if 'path' in obj and isinstance(obj.get('path'), str):
      return True
    if 'callable' in obj:
      c = obj.get('callable')
      if not isinstance(c, str) or not _is_safe_inline_callable(c):
        return True
    return any(_config_has_untrusted_python(v) for v in obj.values())
  if isinstance(obj, list):
    return any(_config_has_untrusted_python(item) for item in obj)
  return False


def _config_has_untrusted_udf_hooks(obj: Any) -> bool:
  """Detects Python callables, UDF hooks, or trust_remote_code outside `language: python`."""
  if isinstance(obj, dict):
    if 'callable' in obj:
      c = obj.get('callable')
      if not isinstance(c, str) or not _is_safe_inline_callable(c):
        return True
    if bool(obj.get('trust_remote_code')):
      return True
    for k, v in obj.items():
      if k in _UDF_HOOK_KEYS and _config_has_untrusted_python(v):
        return True
      if _config_has_untrusted_udf_hooks(v):
        return True
  elif isinstance(obj, list):
    return any(_config_has_untrusted_udf_hooks(item) for item in obj)
  return False


def _has_executable_python_transforms(block: Any) -> bool:
  """Detects `PyTransform` or untrusted Python callables before `--plan`."""
  if isinstance(block, list):
    return any(_has_executable_python_transforms(item) for item in block)
  if not isinstance(block, dict):
    return False
  t_type = str(block.get('type') or '')
  if t_type in ('PyTransform', 'PythonCallable', 'MLTransform'):
    return True
  cfg = block.get('config')
  if isinstance(cfg, dict):
    lang = str(cfg.get('language') or '').lower()
    if lang == 'python' and _config_has_untrusted_python(cfg):
      return True
    if _config_has_untrusted_udf_hooks(cfg):
      return True
    if 'constructor' in cfg and t_type not in ('ReadFromCsv',
                                               'ReadFromJson',
                                               'ReadFromParquet',
                                               'ReadFromAvro',
                                               'ReadFromText'):
      return True
  for child_key in ('transforms', 'extra_transforms', 'source', 'sink'):
    if child_key in block and _has_executable_python_transforms(
        block[child_key]):
      return True
  return False


_REMOTE_ACCESS_MARKERS = (
    'gs://', 's3://', 'azfs://', 'hdfs://', 'list operation failed',
    'operation not permitted', 'defaultcredentialserror',
    'could not automatically determine credentials', 'name resolution',
    'nodename nor servname', 'failed to establish a new connection',
    'connection refused', 'maven', 'http error 403', 'refresherror',
    'mutualtlschannelerror', 'reauthentication', 'invalid_grant',
    'transporterror')


def _is_remote_access_error(exc: BaseException) -> bool:
  """True if `exc` (from --plan expansion) comes from missing network/creds."""
  text = f'{type(exc).__name__}: {exc}'.lower()
  return any(marker in text for marker in _REMOTE_ACCESS_MARKERS)


@contextlib.contextmanager
def _quiet_expansion(enabled: bool = True):
  """Hides log records, warnings and stray stderr while --plan expands.

  Building a pipeline can log credential refresh failures and gRPC/auth
  tracebacks (RefreshError, MutualTLSChannelError) even when `beam check`
  then reports a clean result or an offline fallback. `--verbose` keeps them.
  """
  if not enabled:
    yield
    return
  import io
  import logging
  import warnings
  prev_disable = logging.root.manager.disable
  prev_grpc = os.environ.get('GRPC_VERBOSITY')
  os.environ['GRPC_VERBOSITY'] = 'NONE'
  logging.disable(logging.CRITICAL)
  try:
    with warnings.catch_warnings(), contextlib.redirect_stderr(io.StringIO()):
      warnings.simplefilter('ignore')
      yield
  finally:
    logging.disable(prev_disable)
    if prev_grpc is None:
      os.environ.pop('GRPC_VERBOSITY', None)
    else:
      os.environ['GRPC_VERBOSITY'] = prev_grpc
