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
    verbose: bool = False,
) -> dict[str, Any]:
  """Validates a Beam YAML pipeline and returns a structured validation report."""
  import yaml

  if file_or_dash == '-':
    raw_text = sys.stdin.read()
    source_label = '<stdin>'
    if jinja_variables_json:
      from apache_beam.yaml import yaml_utils
      raw_text = yaml_utils.expand_jinja(
          raw_text, json.loads(jinja_variables_json))
  else:
    if '://' not in file_or_dash and not os.path.isfile(file_or_dash):
      raise CheckError(f'no such file: {file_or_dash!r}')
    if '://' in file_or_dash:
      from apache_beam.io.filesystems import FileSystems
      with FileSystems.open(file_or_dash) as f:
        raw_text = f.read().decode('utf-8')
    else:
      with open(file_or_dash, encoding='utf-8') as f:
        raw_text = f.read()
    source_label = file_or_dash
    if jinja_variables_json:
      from apache_beam.yaml import yaml_utils
      raw_text = yaml_utils.expand_jinja(
          raw_text, json.loads(jinja_variables_json))

  from . import output as out_mod
  if not jinja_variables_json and ('{{' in raw_text or '{%' in raw_text):
    from . import package as pkg_mod
    try:
      pkg_mod.extract_jinja_parameters(raw_text)
    except pkg_mod.PackageError as e:
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
          f'{source_label}: transform #{idx + 1} must be a mapping with "type:"')
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
    dumper = getattr(yaml, 'CSafeDumper', yaml.SafeDumper)
    clean_yaml = yaml.dump(spec, Dumper=dumper, sort_keys=False)
    options, constructor = (
        yaml_main.build_pipeline_components_from_yaml(
            clean_yaml, list(pipeline_args or []), validate_schema='generic'))
    p = beam.Pipeline(options=options, display_data={'yaml': clean_yaml})
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
  }
  if plan_skipped_reason:
    result['plan_skipped_reason'] = plan_skipped_reason
    result['plan_skipped_detail'] = plan_skipped_detail
  return result


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
      if node.id in _DANGEROUS_CALLABLE_NAMES or (
          node.id.startswith('__') and node.id != '__row__'):
        return False
    elif isinstance(node, ast.Attribute):
      if node.attr.startswith('__') or node.attr in (
          'gi_frame', 'f_globals', 'f_builtins', 'tb_frame', 'cr_frame',
          'ag_frame'):
        return False
  return True


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


def _has_executable_python_transforms(block: Any) -> bool:
  """Detects `PyTransform` or untrusted Python callables before `--plan`."""
  if isinstance(block, list):
    return any(_has_executable_python_transforms(item) for item in block)
  if not isinstance(block, dict):
    return False
  t_type = str(block.get('type') or '')
  if t_type in ('PyTransform', 'PythonCallable'):
    return True
  cfg = block.get('config')
  if isinstance(cfg, dict):
    lang = str(cfg.get('language') or '').lower()
    if lang == 'python' and _config_has_untrusted_python(cfg):
      return True
    if 'constructor' in cfg and t_type not in (
        'ReadFromCsv', 'ReadFromJson', 'ReadFromParquet', 'ReadFromAvro',
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
