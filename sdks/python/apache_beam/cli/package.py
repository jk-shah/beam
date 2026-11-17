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

"""Simplified Dataflow Flex Template packager (`beam package`).

Eliminates handwritten `Dockerfile` and `metadata.json` boilerplate across two
modes:
1. **Zero-Docker YAML Flex Templates (`--sdk=yaml`, default for `.yaml`/stdin)**:
   - Safely parses Jinja2 `{{ var }}` and `{{ var | default('...') }}` via AST
     (zero template execution) to synthesize `metadata.parameters`.
   - Resolves Google's official regional `Yaml_Template` launcher image from
     `https://storage.googleapis.com/dataflow-templates-<region>/latest/flex/Yaml_Template`
     in <200ms with zero Docker or Cloud Build step.
   - Emits a ready-to-run Dataflow `ContainerSpec` JSON (`sdkInfo: YAML`,
     `metadata.yamlDefinition`) to `gs://...` or local disk, plus copy-pasteable
     `gcloud dataflow flex-template run` and Cloud Composer / Airflow snippets.
2. **Zero-Boilerplate Python Flex Templates (`--sdk=python`, default for `.py`)**:
   - Safely parses `PipelineOptions._add_argparse_args` via Python `ast` (zero
     code execution) to synthesize `metadata.parameters`.
   - Generates a hermetic single-image `Dockerfile`, `metadata.json`, and
     `ContainerSpec` JSON into `--output-dir` (and optionally runs
     `gcloud builds submit` when `--build-image` is requested).
"""

import ast
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import yaml

EXIT_USAGE = 2
EXIT_MISSING_PACKAGE = 4

DEFAULT_REGION = 'us-central1'
FALLBACK_YAML_TEMPLATE_IMAGE = 'gcr.io/dataflow-templates/latest/yaml-template'
DEFAULT_PYTHON_LAUNCHER_BASE = (
    'gcr.io/dataflow-templates-base/python312-template-launcher-base:latest')
_SAFE_PARAM_NAME_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_JINJA_EXPR_RE = re.compile(r'\{\{.*?\}\}|\{%.*?%\}|\{#.*?#\}', re.DOTALL)
_SECRET_VALUE_TOKEN_RE = re.compile(
    r'(?:'
    r'\bya29\.[A-Za-z0-9._-]{8,}|'
    r'\bsk-[A-Za-z0-9_-]{12,}|'
    r'\bAIza[0-9A-Za-z_-]{20,}|'
    r'\bgh[pousr]_[A-Za-z0-9_]{12,}|'
    r'\bgithub_pat_[A-Za-z0-9_]{12,}|'
    r'\bhf_[A-Za-z0-9]{12,}|'
    r'\bAKIA[0-9A-Z]{16}\b|'
    r'\bxox[baprs]-[A-Za-z0-9-]{10,}|'
    r'\beyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}'
    r')')


class PackageError(Exception):
  """Raised when `beam package` validation or packaging fails."""
  def __init__(self, message: str, exit_code: int = EXIT_USAGE):
    super().__init__(message)
    self.exit_code = exit_code


def _check_no_inline_secret_text(text: str, label: str) -> None:
  """Raises `PackageError` if `text` contains inline URI credentials or tokens."""
  from . import output as out_mod
  from . import uris as uris_mod
  if uris_mod._INLINE_URI_USERINFO_RE.search(text):
    raise PackageError(
        f'refusing to bake inline URI credentials (`user:pass@`) from {label} '
        'into Flex Template; use environment variables, ADC, or Secret Manager.',
        EXIT_USAGE)
  if '://' in text:
    for token in re.findall(r'[A-Za-z][A-Za-z0-9+.-]*://[^\s\'"<>]+', text):
      try:
        uris_mod.reject_inline_credentials(token)
      except uris_mod.UriError as e:
        raise PackageError(
            f'refusing to bake inline credentials from {label}: {e}',
            EXIT_USAGE) from None
  if _SECRET_VALUE_TOKEN_RE.search(text) or (out_mod.redact_inline_secrets(text)
                                             != text):
    raise PackageError(
        f'refusing to bake inline credentials or secret tokens from {label} '
        'into Flex Template; use Google Cloud Secret Manager or runtime parameters.',
        EXIT_USAGE)


def _scan_python_source_for_secrets(py_source: str, label: str) -> None:
  """Scans string literals in `py_source` via AST for inline credentials/tokens."""
  try:
    tree = ast.parse(py_source)
  except SyntaxError as e:
    raise PackageError(
        f'invalid Python syntax in pipeline file (line {e.lineno}): {e.msg}',
        EXIT_USAGE) from None
  for node in ast.walk(tree):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
      _check_no_inline_secret_text(node.value, label)


def _scan_requirements_file_for_secrets(req_path: Path) -> None:
  """Scans `requirements.txt` lines for inline credentials or secret tokens."""
  from . import output as out_mod
  try:
    req_text = out_mod.read_bounded_text(req_path, label=str(req_path))
  except ValueError as e:
    raise PackageError(str(e), EXIT_USAGE) from None
  for lineno, raw_line in enumerate(req_text.splitlines(), start=1):
    stripped = raw_line.strip()
    if not stripped or stripped.startswith('#'):
      continue
    _check_no_inline_secret_text(stripped, f'{req_path.name} (line {lineno})')


def _humanize_label(param_name: str) -> str:
  words = param_name.replace('-', '_').split('_')
  label = ' '.join(w for w in words if w).strip()
  return label[:1].upper() + label[1:] if label else param_name


def extract_jinja_parameters(yaml_text: str) -> list[dict[str, Any]]:
  """Extracts Jinja2 variables & `default(...)` filters via AST (no execution)."""
  from . import uris as uris_mod
  try:
    import jinja2
    from jinja2 import meta
    from jinja2 import nodes
  except ImportError as e:
    raise PackageError(
        f'jinja2 is required to inspect YAML template parameters: {e}',
        EXIT_MISSING_PACKAGE) from None

  env = jinja2.Environment()
  try:
    parsed = env.parse(yaml_text)
  except jinja2.TemplateSyntaxError as e:
    raise PackageError(
        f'invalid Jinja2 syntax in YAML pipeline (line {e.lineno}): {e.message}',
        EXIT_USAGE) from None

  undeclared = meta.find_undeclared_variables(parsed)
  # Preserve first-appearance order in the template while capturing defaults.
  ordered_names: list[str] = []
  defaults_by_name: dict[str, str] = {}

  def _walk(node: Any) -> None:
    if isinstance(node, nodes.Filter) and node.name in ('default', 'd'):
      if isinstance(node.node, nodes.Name) and node.node.name in undeclared:
        var_name = node.node.name
        if var_name not in ordered_names:
          ordered_names.append(var_name)
        if node.args and isinstance(node.args[0], nodes.Const):
          val = node.args[0].value
          defaults_by_name[var_name] = '' if val is None else str(val)
    elif isinstance(node, nodes.Name) and node.ctx == 'load':
      if node.name in undeclared and node.name not in ordered_names:
        ordered_names.append(node.name)
    for child in node.iter_child_nodes():
      _walk(child)

  _walk(parsed)
  for extra_name in sorted(undeclared):
    if extra_name not in ordered_names:
      ordered_names.append(extra_name)

  params: list[dict[str, Any]] = []
  for name in ordered_names:
    if name in ('datetime', ):
      continue
    if not _SAFE_PARAM_NAME_RE.match(name):
      continue
    has_default = name in defaults_by_name
    if has_default:
      def_v = defaults_by_name[name]
      if (def_v.strip() and not name.lower().endswith('_env') and
          uris_mod._SECRET_KEY_RE.search(name)):
        raise PackageError(
            f'refusing to bake non-empty defaultValue for secret Jinja '
            f'parameter {name!r}; omit default(...) or use Secret Manager.',
            EXIT_USAGE)
      if def_v:
        _check_no_inline_secret_text(def_v, f'Jinja default for {name!r}')
    entry: dict[str, Any] = {
        'name': name,
        'label': _humanize_label(name),
        'helpText': f"Runtime value for Jinja variable '{name}'.",
        'isOptional': bool(has_default),
        'paramType': 'TEXT',
    }
    if has_default:
      entry['defaultValue'] = defaults_by_name[name]
    params.append(entry)
  return params


def _validate_region(region: str | None) -> str:
  reg = (region or DEFAULT_REGION).strip()
  if not re.fullmatch(r'[a-z0-9]+(?:-[a-z0-9]+)+', reg):
    raise PackageError(
        f'invalid GCP region {region!r}; expected lowercase region such as '
        "'us-central1'",
        EXIT_USAGE)
  return reg


def _validate_image_uri(image_uri: str, flag_name: str = '--image') -> str:
  cleaned = image_uri.strip()
  if not cleaned or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._/:@-]{0,511}',
                                     cleaned):
    raise PackageError(
        f'invalid container image URI for {flag_name}: {image_uri!r}',
        EXIT_USAGE)
  return cleaned


def _validate_entrypoint_filename(filename: str) -> str:
  if not re.fullmatch(r'[A-Za-z0-9_.-]+\.py', filename) or '..' in filename:
    raise PackageError(
        f'unsafe Python entrypoint filename {filename!r}; expected simple '
        'alphanumeric .py basename',
        EXIT_USAGE)
  return filename


_ALLOWED_TRANSFORM_SPEC_KEYS = frozenset({
    'type',
    'name',
    'input',
    'output',
    'config',
    'transforms',
    'source',
    'sink',
    'extra_transforms',
    'output_schema',
    'error_handling',
    'windowing',
    'resource_hints',
    'environment',
    'annotations',
})


def _validate_yaml_with_jinja_placeholders(yaml_text: str) -> dict[str, Any]:
  """Validates top-level Beam YAML structure even when `{{ vars }}` are present."""
  from . import output as out_mod
  try:
    out_mod.validate_safe_jinja_template(yaml_text)
  except ValueError as e:
    raise PackageError(str(e), EXIT_USAGE) from None
  scrubbed = _JINJA_EXPR_RE.sub('__BEAM_JINJA_PLACEHOLDER__', yaml_text)
  try:
    doc = out_mod.safe_yaml_load(scrubbed)
  except yaml.YAMLError as e:
    raise PackageError(f'invalid YAML syntax: {e}', EXIT_USAGE) from None
  if not isinstance(doc, dict) or 'pipeline' not in doc:
    raise PackageError(
        "Beam YAML file must be a mapping containing a top-level 'pipeline:' key.",
        EXIT_USAGE)
  pipe_obj = doc.get('pipeline')
  if isinstance(pipe_obj, dict):
    transforms = pipe_obj.get('transforms')
    if isinstance(transforms, list):
      for idx, tfm in enumerate(transforms):
        if not isinstance(tfm, dict) or not tfm.get('type'):
          raise PackageError(
              f"pipeline.transforms[{idx}] must be a mapping with a 'type' key.",
              EXIT_USAGE)
        bad_keys = sorted(set(tfm.keys()) - _ALLOWED_TRANSFORM_SPEC_KEYS)
        if bad_keys:
          raise PackageError(
              f"pipeline.transforms[{idx}] ({tfm.get('type')!r}) has unexpected "
              f'top-level key(s) {bad_keys}; connector/transform parameters '
              "belong nested under 'config:'.",
              EXIT_USAGE)
  return doc


def _find_custom_python_providers(doc: dict[str, Any]) -> list[str]:
  """Returns non-stock Python provider packages/constructors in YAML."""
  custom: list[str] = []
  providers = doc.get('providers')
  if isinstance(providers, list):
    for prov in providers:
      if not isinstance(prov, dict):
        continue
      ptype = str(prov.get('type', ''))
      cfg = prov.get('config') or {}
      if ptype == 'pythonPackage':
        pkgs = cfg.get('packages') or []
        custom.extend(str(p) for p in pkgs)
      elif ptype == 'python':
        transforms = prov.get('transforms') or {}
        if isinstance(transforms, dict):
          for fq in transforms.values():
            fq_s = str(fq)
            # Stock yaml-template image has standard apache_beam, but not local
            # user modules or unreleased `apache_beam.cli.*` helpers.
            if not fq_s.startswith('apache_beam.') or fq_s.startswith(
                'apache_beam.cli.'):
              custom.append(fq_s)

  def _walk_py_transforms(node: Any) -> None:
    if isinstance(node, dict):
      if str(node.get('type') or '') == 'PyTransform':
        t_cfg = node.get('config')
        if isinstance(t_cfg, dict):
          ctor = str(t_cfg.get('constructor') or '').strip()
          if ctor.startswith('apache_beam.cli.'):
            custom.append(ctor)
      for val in node.values():
        _walk_py_transforms(val)
    elif isinstance(node, list):
      for item in node:
        _walk_py_transforms(item)

  _walk_py_transforms(doc.get('pipeline'))
  return list(dict.fromkeys(custom))


def _detect_yaml_extra_packages(yaml_doc: Any) -> list[str]:
  """Returns third-party runtime packages implied by YAML stages."""
  needed: list[str] = []

  def _walk(node: Any) -> None:
    if isinstance(node, dict):
      t_type = str(node.get('type') or '')
      cfg = node.get('config') if isinstance(node.get('config'), dict) else {}
      if t_type in ('Enrichment', 'CliEnrich'):
        handler = str(
            cfg.get('enrichment_handler') or cfg.get('handler') or
            cfg.get('provider') or '').lower()
        if 'feast' in handler:
          needed.append('feast[gcp]')
        if 'vertex' in handler:
          needed.append('google-cloud-aiplatform')
        if 'bigtable' in handler or handler == 'bt' or cfg.get('bigtable'):
          needed.append('google-cloud-bigtable')
        if cfg.get('redis') or cfg.get('redis_host'):
          needed.append('redis')
      elif t_type in ('RunInference', 'CliInfer'):
        mh = cfg.get('model_handler')
        mh_type = str((mh.get('type') if isinstance(mh, dict) else None) or
                      cfg.get('provider') or cfg.get('handler') or '').lower()
        if 'huggingface' in mh_type or mh_type == 'hf':
          needed.extend(['transformers', 'torch', 'safetensors'])
        if 'vertex' in mh_type:
          needed.append('google-cloud-aiplatform')
      elif t_type in (
          'ReadFromIcebergPy',
          'ReadFromIcebergCdcPy',
          'CliWriteToIceberg',
          'IcebergAddFiles',
      ):
        needed.append('pyiceberg')
      elif 'bigtable' in t_type.lower() and t_type not in (
          'ReadFromBigTable',
          'WriteToBigTable',
      ):
        needed.append('google-cloud-bigtable')
      for v in node.values():
        _walk(v)
    elif isinstance(node, list):
      for item in node:
        _walk(item)

  _walk(yaml_doc)
  return list(dict.fromkeys(needed))


def extract_python_pipeline_parameters(py_source: str) -> list[dict[str, Any]]:
  """Extracts `add_argument` / `add_value_provider_argument` via Python AST."""
  try:
    tree = ast.parse(py_source)
  except SyntaxError as e:
    raise PackageError(
        f'invalid Python syntax in pipeline file (line {e.lineno}): {e.msg}',
        EXIT_USAGE) from None

  params: list[dict[str, Any]] = []
  seen: set[str] = set()

  for node in ast.walk(tree):
    if not isinstance(node, ast.Call) or not isinstance(node.func,
                                                        ast.Attribute):
      continue
    if node.func.attr not in ('add_argument', 'add_value_provider_argument'):
      continue

    flag_strings: list[str] = []
    for arg in node.args:
      if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
        flag_strings.append(arg.value)
    long_flags = [f for f in flag_strings if f.startswith('--')]
    if not long_flags:
      continue

    raw_name = long_flags[0].lstrip('-').replace('-', '_')
    kw_map: dict[str, Any] = {}
    for kw in node.keywords:
      if kw.arg is not None:
        kw_map[kw.arg] = kw.value

    if 'dest' in kw_map and isinstance(
        kw_map['dest'], ast.Constant) and isinstance(kw_map['dest'].value, str):
      raw_name = kw_map['dest'].value

    if not _SAFE_PARAM_NAME_RE.match(raw_name) or raw_name in seen:
      continue
    seen.add(raw_name)

    help_text = f"Pipeline option '--{raw_name.replace('_', '-')}'."
    if 'help' in kw_map and isinstance(
        kw_map['help'], ast.Constant) and isinstance(kw_map['help'].value, str):
      help_text = kw_map['help'].value

    required = False
    if 'required' in kw_map and isinstance(kw_map['required'], ast.Constant):
      required = bool(kw_map['required'].value)

    has_default = 'default' in kw_map
    default_val: str | None = None
    if has_default and isinstance(kw_map['default'], ast.Constant):
      val = kw_map['default'].value
      if val is not None:
        default_val = str(val)
        from . import uris as uris_mod
        if (default_val.strip() and not raw_name.lower().endswith('_env') and
            uris_mod._SECRET_KEY_RE.search(raw_name)):
          raise PackageError(
              f'refusing to bake non-empty defaultValue for secret parameter '
              f'{raw_name!r}; omit default=... or use Secret Manager.',
              EXIT_USAGE)
        if default_val:
          _check_no_inline_secret_text(default_val, f'default for {raw_name!r}')

    is_optional = (not required)
    entry: dict[str, Any] = {
        'name': raw_name,
        'label': _humanize_label(raw_name),
        'helpText': help_text,
        'isOptional': is_optional,
        'paramType': 'TEXT',
    }
    if default_val is not None:
      entry['defaultValue'] = default_val
    params.append(entry)

  return params


class _HttpsOnlyRedirectHandler(urllib.request.HTTPRedirectHandler):
  """Refuses any redirect from `https://` to a non-`https://` scheme."""
  def redirect_request(self, req, fp, code, msg, headers, newurl):
    if not str(newurl).lower().startswith('https://'):
      raise urllib.error.URLError(
          f'refusing non-HTTPS redirect target {newurl!r}')
    return super().redirect_request(req, fp, code, msg, headers, newurl)


def resolve_yaml_template_image(
    region: str = DEFAULT_REGION,
    explicit_image: str | None = None,
    allow_network: bool = True,
) -> tuple[str, list[str]]:
  """Resolves Google's hosted regional `Yaml_Template` container image."""
  safe_reg = _validate_region(region)
  if explicit_image:
    return _validate_image_uri(explicit_image, '--image'), []

  warnings_out: list[str] = []
  if allow_network:
    opener = urllib.request.build_opener(
        urllib.request.HTTPSHandler(), _HttpsOnlyRedirectHandler())
    for reg in (safe_reg, DEFAULT_REGION):
      url = (
          f'https://storage.googleapis.com/dataflow-templates-{reg}/'
          'latest/flex/Yaml_Template')
      try:
        req = urllib.request.Request(
            url, headers={'User-Agent': 'apache-beam-cli-package'})
        with opener.open(req, timeout=5.0) as resp:
          spec = json.loads(resp.read(65536).decode('utf-8'))
          img = spec.get('image')
          if isinstance(img, str) and img.strip():
            return _validate_image_uri(img.strip(), 'Yaml_Template'), warnings_out
      except Exception:  # pylint: disable=broad-except
        continue
    warnings_out.append(
        'Could not reach regional dataflow-templates bucket; used fallback '
        f'YAML template image {FALLBACK_YAML_TEMPLATE_IMAGE!r}.')
  return FALLBACK_YAML_TEMPLATE_IMAGE, warnings_out


def _build_default_environment(
    *,
    subnetwork: str | None = None,
    network: str | None = None,
    no_use_public_ips: bool = False,
    staging_location: str | None = None,
    temp_location: str | None = None,
    service_account_email: str | None = None,
    worker_machine_type: str | None = None,
    max_workers: int | None = None,
    num_workers: int | None = None,
    worker_region: str | None = None,
    enable_streaming_engine: bool = False,
) -> dict[str, Any]:
  env: dict[str, Any] = {}
  if service_account_email:
    env['serviceAccountEmail'] = service_account_email
  if max_workers is not None:
    env['maxWorkers'] = int(max_workers)
  if num_workers is not None:
    env['numWorkers'] = int(num_workers)
  if network:
    env['network'] = network
  if subnetwork:
    env['subnetwork'] = subnetwork
  if worker_machine_type:
    env['machineType'] = worker_machine_type
  resolved_temp = temp_location or staging_location
  if resolved_temp:
    env['tempLocation'] = resolved_temp
  if staging_location:
    env['stagingLocation'] = staging_location
  if no_use_public_ips:
    env['ipConfiguration'] = 'WORKER_IP_PRIVATE'
  if worker_region:
    env['workerRegion'] = worker_region
  if enable_streaming_engine:
    env['enableStreamingEngine'] = True
  return env


def generate_python_dockerfile(
    entrypoint_filename: str,
    *,
    base_image: str = DEFAULT_PYTHON_LAUNCHER_BASE,
    has_requirements: bool = False,
    extra_wheels: list[str] | None = None,
    extra_files: list[str] | None = None,
) -> str:
  """Generates a single-image Flex Template Dockerfile (launcher + worker)."""
  safe_base = _validate_image_uri(base_image, '--base-image')
  safe_ep = _validate_entrypoint_filename(entrypoint_filename)
  lines = [
      f'FROM {safe_base}',
      '',
      'WORKDIR /template',
  ]
  if extra_wheels:
    safe_wheels: list[str] = []
    for whl in extra_wheels:
      if not re.fullmatch(r'[A-Za-z0-9_.-]+\.whl', whl) or '..' in whl:
        raise PackageError(f'unsafe wheel filename {whl!r}', EXIT_USAGE)
      safe_wheels.append(whl)
      lines.append(f'COPY {whl} /template/{whl}')
    whl_paths = ' '.join(f'/template/{w}' for w in safe_wheels)
    lines.append(f'RUN pip install --no-cache-dir {whl_paths}')
  if has_requirements:
    lines.extend([
        'COPY requirements.txt /template/requirements.txt',
        'RUN pip install --no-cache-dir -r /template/requirements.txt',
        'ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"',
    ])
  if extra_files:
    for ef in extra_files:
      if not re.fullmatch(r'[A-Za-z0-9_.-]+', ef) or '..' in ef:
        raise PackageError(f'unsafe extra filename {ef!r}', EXIT_USAGE)
      lines.append(f'COPY {ef} /template/{ef}')
  lines.extend([
      f'COPY {safe_ep} /template/{safe_ep}',
      f'ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/{safe_ep}"',
      '',
  ])
  return '\n'.join(lines)


def _generate_yaml_python_wrapper(
    yaml_filename: str, param_names: list[str] | None = None) -> str:
  """Generates a minimal Python Flex entrypoint wrapping a Beam YAML file."""
  if not re.fullmatch(r'[A-Za-z0-9_.-]+\.ya?ml',
                      yaml_filename) or '..' in yaml_filename:
    raise PackageError(f'unsafe YAML filename {yaml_filename!r}', EXIT_USAGE)
  clean_params: list[str] = []
  for p in sorted(set(param_names or ())):
    if not _SAFE_PARAM_NAME_RE.match(str(p)):
      raise PackageError(f'invalid parameter name {p!r}', EXIT_USAGE)
    clean_params.append(str(p))
  names_literal = repr(clean_params)
  return (
      '"""Auto-generated Beam YAML Flex Template launcher."""\n'
      'import json\n'
      'import os\n'
      'from pathlib import Path\n'
      'import sys\n\n'
      'try:\n'
      '  from apache_beam_cli_preview import launcher as _prev\n'
      '  _prev.install_runtime_hooks(strict=False)\n'
      'except ImportError:\n'
      '  pass\n\n'
      'try:\n'
      '  from apache_beam.cli import dlq as _cli_dlq\n'
      '  _cli_dlq.install_keyword_safe_row_hooks()\n'
      'except ImportError:\n'
      '  pass\n\n'
      'from apache_beam.yaml import main as yaml_main\n\n'
      f'_JINJA_PARAMS = set({names_literal})\n'
      '_SKIP_STAGE_FLAGS = {"--no-stage-cli-wheel", "--no_stage_cli_wheel"}\n\n'
      'def _split_jinja_argv(raw_argv):\n'
      '  jinja_vars = {}\n'
      '  passthrough = []\n'
      '  skip_stage = os.environ.get(\n'
      '      "BEAM_NO_STAGE_CLI_WHEEL", "").strip().lower() in ("1", "true", "yes")\n'
      '  i = 0\n'
      '  while i < len(raw_argv):\n'
      '    tok = raw_argv[i]\n'
      '    if tok in _SKIP_STAGE_FLAGS:\n'
      '      skip_stage = True\n'
      '      i += 1\n'
      '      continue\n'
      '    if tok.startswith("--") and "=" in tok:\n'
      '      k, v = tok[2:].split("=", 1)\n'
      '      if k in _JINJA_PARAMS:\n'
      '        jinja_vars[k] = v\n'
      '        i += 1\n'
      '        continue\n'
      '    elif tok.startswith("--") and tok[2:] in _JINJA_PARAMS and i + 1 < len(raw_argv):\n'
      '      jinja_vars[tok[2:]] = raw_argv[i + 1]\n'
      '      i += 2\n'
      '      continue\n'
      '    passthrough.append(tok)\n'
      '    i += 1\n'
      '  if jinja_vars:\n'
      '    passthrough.append("--jinja_variables=" + json.dumps(jinja_vars))\n'
      '  if not skip_stage:\n'
      '    tmpl_dir = Path(__file__).resolve().parent\n'
      '    whls = sorted(tmpl_dir.glob("apache_beam_cli_preview-*.whl"))\n'
      '    if whls:\n'
      '      if not any("apache_beam_cli_preview" in a for a in passthrough):\n'
      '        passthrough.append("--extra_package=" + str(whls[0]))\n'
      '      plugin_arg = "--beam_plugin=apache_beam_cli_preview._worker_bootstrap"\n'
      '      if plugin_arg not in passthrough:\n'
      '        passthrough.append(plugin_arg)\n'
      '  return passthrough\n\n'
      'if __name__ == "__main__":\n'
      f'  yaml_path = str(Path(__file__).resolve().with_name({yaml_filename!r}))\n'
      '  forwarded = _split_jinja_argv(sys.argv[1:])\n'
      '  yaml_main.run(argv=["--yaml_pipeline_file=" + yaml_path] + forwarded)\n'
  )


_FALLBACK_LAUNCHER_SRC = (
    '"""Minimal synthesized CLI runtime hook for Flex Template workers."""\n'
    'import importlib.abc\n'
    'import importlib.util\n'
    'import sys\n'
    'from pathlib import Path\n\n'
    '_FAST_PKG = "_apache_beam_cli_pkg"\n\n'
    'class _PreviewCliFinder(importlib.abc.MetaPathFinder):\n'
    '  def __init__(self, cli_dir: Path):\n'
    '    self._cli_dir = cli_dir\n'
    '  def find_spec(self, fullname, path, target=None):\n'
    '    del path, target\n'
    '    if fullname in ("apache_beam.cli", _FAST_PKG):\n'
    '      return importlib.util.spec_from_file_location(\n'
    '          fullname,\n'
    '          self._cli_dir / "__init__.py",\n'
    '          submodule_search_locations=[str(self._cli_dir)])\n'
    '    for pfx in ("apache_beam.cli.", _FAST_PKG + "."):\n'
    '      if fullname.startswith(pfx):\n'
    '        parts = fullname[len(pfx):].split(".")\n'
    '        if not parts or not all(p.isidentifier() for p in parts):\n'
    '          return None\n'
    '        pkg_init = self._cli_dir.joinpath(*parts, "__init__.py")\n'
    '        if pkg_init.is_file():\n'
    '          return importlib.util.spec_from_file_location(\n'
    '              fullname, pkg_init, submodule_search_locations=[str(pkg_init.parent)])\n'
    '        mod_py = self._cli_dir.joinpath(*parts[:-1], f"{parts[-1]}.py")\n'
    '        if mod_py.is_file():\n'
    '          return importlib.util.spec_from_file_location(fullname, mod_py)\n'
    '    return None\n\n'
    'def canonicalize_cli_module_aliases() -> None:\n'
    '  for k, m in list(sys.modules.items()):\n'
    '    if m is not None and (k == _FAST_PKG or k.startswith(_FAST_PKG + ".")):\n'
    '      sys.modules.setdefault("apache_beam.cli" + k[len(_FAST_PKG):], m)\n\n'
    'def install_runtime_hooks(*, strict: bool = True):\n'
    '  cli_dir = (Path(__file__).resolve().parent / "_bundled_cli").resolve()\n'
    '  if not (cli_dir / "main.py").is_file():\n'
    '    if strict:\n'
    '      raise RuntimeError("bundled cli directory missing")\n'
    '    return None\n'
    '  sys.meta_path[:] = [\n'
    '      f for f in sys.meta_path if type(f).__name__ != "_PreviewCliFinder"\n'
    '  ]\n'
    '  sys.meta_path.insert(0, _PreviewCliFinder(cli_dir))\n'
    '  canonicalize_cli_module_aliases()\n'
    '  return cli_dir\n')


def _check_owned_dir_lstat(dir_path: Path) -> None:
  st = os.lstat(dir_path)
  if stat.S_ISLNK(st.st_mode):
    raise RuntimeError(f'refusing symlinked cache directory {str(dir_path)!r}')
  if not stat.S_ISDIR(st.st_mode):
    raise NotADirectoryError(
        f'cache path is not a directory: {str(dir_path)!r}')
  if hasattr(os, 'getuid') and st.st_uid != os.getuid():
    raise RuntimeError(
        f'refusing foreign-owned cache directory {str(dir_path)!r}')
  try:
    os.chmod(dir_path, 0o700)
  except OSError:
    pass


def _ensure_owned_private_dir(path: Path, *, root: Path | None = None) -> Path:
  """Creates missing cache dirs with `0o700` and verifies ownership/no symlinks."""
  target = Path(os.path.abspath(os.fspath(path)))
  root_abs = (
      Path(os.path.abspath(os.fspath(root))) if root is not None else None)
  chain: list[Path] = []
  cur = target
  while True:
    chain.append(cur)
    if root_abs is not None and cur == root_abs:
      break
    parent = cur.parent
    if parent == cur:
      break
    if root_abs is None:
      try:
        parent_st = os.lstat(parent)
        parent_exists = True
      except FileNotFoundError:
        parent_exists = False
        parent_st = None
      if parent_exists:
        p_name = parent.name
        if not (p_name.startswith('beam-cli-cache') or p_name == '.beam' or
                (parent_st is not None and stat.S_ISLNK(parent_st.st_mode) and
                 parent not in
                 (Path('/tmp'), Path('/var'), Path(tempfile.gettempdir())))):
          break
    cur = parent

  chain.reverse()
  base_parent = chain[0].parent
  if base_parent != chain[0] and not base_parent.exists():
    base_parent.mkdir(mode=0o700, parents=True, exist_ok=True)
  for comp in chain:
    try:
      os.lstat(comp)
    except FileNotFoundError:
      try:
        os.mkdir(comp, 0o700)
      except FileExistsError:
        pass
    _check_owned_dir_lstat(comp)
  return target


def _is_valid_owned_wheel(wheel_path: Path) -> bool:
  import zipfile
  try:
    st = os.lstat(wheel_path)
  except FileNotFoundError:
    return False
  if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
    raise RuntimeError(
        f'refusing symlinked or non-regular cached wheel {str(wheel_path)!r}')
  if hasattr(os, 'getuid') and st.st_uid != os.getuid():
    raise RuntimeError(
        f'refusing foreign-owned cached wheel {str(wheel_path)!r}')
  return zipfile.is_zipfile(wheel_path)


def _ensure_cli_bundle_wheel() -> Path:
  """Returns the CLI `.whl`, synthesizing one from `apache_beam/cli` if needed."""
  try:
    from apache_beam_cli_preview import launcher as _prev_launcher
    return Path(_prev_launcher.ensure_preview_wheel())
  except ImportError:
    pass

  import base64
  import hashlib
  import zipfile

  cli_dir = Path(__file__).resolve().parent
  repo_preview_pkg = (
      cli_dir.parents[1] / 'cli-preview' / 'apache_beam_cli_preview')
  version_str = '0.2.0'
  entries: list[tuple[str, bytes]] = []
  if (repo_preview_pkg / 'launcher.py').is_file():
    for fname in ('__init__.py',
                  'launcher.py',
                  '_worker_bootstrap.py',
                  'demo.py'):
      fp = repo_preview_pkg / fname
      if fp.is_file():
        entries.append((f'apache_beam_cli_preview/{fname}', fp.read_bytes()))
  else:
    entries.extend([
        (
            'apache_beam_cli_preview/__init__.py',
            f'__version__ = {version_str!r}\n'.encode('utf-8'),
        ),
        (
            'apache_beam_cli_preview/launcher.py',
            _FALLBACK_LAUNCHER_SRC.encode('utf-8'),
        ),
        (
            'apache_beam_cli_preview/_worker_bootstrap.py',
            (
                'from . import launcher\n'
                'launcher.install_runtime_hooks(strict=False)\n'
                'try:\n'
                '  from apache_beam.cli import dlq\n'
                '  dlq.install_keyword_safe_row_hooks()\n'
                'except Exception:\n'
                '  pass\n').encode('utf-8'),
        ),
    ])

  for py in sorted(cli_dir.rglob('*.py')):
    if not py.name.endswith('_test.py') and '__pycache__' not in py.parts:
      rel = py.relative_to(cli_dir).as_posix()
      entries.append((
          f'apache_beam_cli_preview/_bundled_cli/{rel}',
          py.read_bytes(),
      ))

  pth_line = (
      'import apache_beam_cli_preview.launcher as _bcp; '
      '_bcp.install_runtime_hooks(strict=False)\n')
  entries.append(('apache_beam_cli_preview.pth', pth_line.encode('utf-8')))

  hasher = hashlib.sha256()
  for arcname, data in entries:
    hasher.update(arcname.encode('utf-8'))
    hasher.update(data)
  digest = hasher.hexdigest()[:12]

  explicit_cache = os.environ.get('BEAM_CACHE_DIR')
  beam_home_env = os.environ.get('BEAM_HOME')
  if explicit_cache:
    cache_root = Path(explicit_cache)
    check_root = cache_root
  elif beam_home_env:
    check_root = Path(os.path.expanduser(beam_home_env))
    cache_root = check_root / 'cache'
  else:
    check_root = Path.home() / '.beam'
    cache_root = check_root / 'cache'
  wheel_dir = cache_root / 'wheels' / digest
  wheel_name = f'apache_beam_cli_preview-{version_str}-py3-none-any.whl'
  try:
    wheel_dir = _ensure_owned_private_dir(wheel_dir, root=check_root)
  except OSError:
    if explicit_cache:
      raise
    uid_sfx = f'-{os.getuid()}' if hasattr(os, 'getuid') else ''
    user_tmp_root = Path(tempfile.gettempdir()) / f'beam-cli-cache{uid_sfx}'
    wheel_dir = _ensure_owned_private_dir(
        user_tmp_root / 'wheels' / digest, root=user_tmp_root)

  wheel_path = wheel_dir / wheel_name
  if _is_valid_owned_wheel(wheel_path):
    return wheel_path

  dist_info = f'apache_beam_cli_preview-{version_str}.dist-info'
  all_files = entries + [
      (
          f'{dist_info}/METADATA',
          (
              'Metadata-Version: 2.1\n'
              'Name: apache-beam-cli-preview\n'
              f'Version: {version_str}\n'
              'Summary: Pure-Python preview overlay for Apache Beam CLI\n'
          ).encode('utf-8'),
      ),
      (
          f'{dist_info}/WHEEL',
          (
              'Wheel-Version: 1.0\n'
              'Generator: apache-beam-cli-package\n'
              'Root-Is-Purelib: true\n'
              'Tag: py3-none-any\n').encode('utf-8'),
      ),
      (f'{dist_info}/top_level.txt', b'apache_beam_cli_preview\n'),
  ]
  record_lines: list[str] = []
  for arcname, data in all_files:
    b64 = base64.urlsafe_b64encode(
        hashlib.sha256(data).digest()).rstrip(b'=').decode('ascii')
    record_lines.append(f'{arcname},sha256={b64},{len(data)}')
  record_path = f'{dist_info}/RECORD'
  record_lines.append(f'{record_path},,')
  record_bytes = ('\n'.join(record_lines) + '\n').encode('utf-8')

  fd, tmp_whl = tempfile.mkstemp(
      prefix='.tmp-pkg-', suffix='.whl', dir=str(wheel_dir))
  os.close(fd)
  try:
    with zipfile.ZipFile(tmp_whl, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
      for arcname, data in all_files:
        zf.writestr(arcname, data)
      zf.writestr(record_path, record_bytes)
    try:
      os.replace(tmp_whl, wheel_path)
    except PermissionError:
      if (wheel_path.is_file() and not wheel_path.is_symlink() and
          _is_valid_owned_wheel(wheel_path)):
        return wheel_path
      raise
  finally:
    if os.path.exists(tmp_whl):
      try:
        os.unlink(tmp_whl)
      except OSError:
        pass
  return wheel_path


def _py_source_references_beam_cli(py_source: str) -> bool:
  if 'apache_beam.cli' in py_source:
    return True
  try:
    tree = ast.parse(py_source)
  except SyntaxError:
    return False
  for node in ast.walk(tree):
    if isinstance(node, ast.ImportFrom) and node.module == 'apache_beam':
      if any(alias.name == 'cli' for alias in node.names):
        return True
    elif isinstance(node, ast.Import):
      for alias in node.names:
        if alias.name == 'apache_beam.cli' or alias.name.startswith(
            'apache_beam.cli.'):
          return True
  return False


def _atomic_write_text(target: Path, content: str) -> None:
  target.parent.mkdir(parents=True, exist_ok=True)
  fd, tmp_path = tempfile.mkstemp(
      prefix=f'.tmp_{target.name}_', dir=str(target.parent))
  try:
    with os.fdopen(fd, 'w', encoding='utf-8') as f:
      f.write(content)
    os.replace(tmp_path, target)
  finally:
    if os.path.exists(tmp_path):
      try:
        os.unlink(tmp_path)
      except OSError:
        pass


def _write_text_destination(
    dest: str, content: str, *, project: str | None = None) -> None:
  """Writes UTF-8 text either to `gs://bucket/object` or a local file path."""
  if dest.startswith('gs://'):
    without_scheme = dest[len('gs://'):]
    parts = without_scheme.split('/', 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
      raise PackageError(
          f'invalid GCS destination {dest!r}; expected gs://<bucket>/<object>',
          EXIT_USAGE)
    bucket_name, blob_name = parts
    try:
      from google.cloud import storage
    except ImportError as e:
      raise PackageError(
          f'google-cloud-storage is required to write to {dest}: {e}',
          EXIT_MISSING_PACKAGE) from None
    client = storage.Client(project=project) if project else storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(
        content.encode('utf-8'), content_type='application/json')
    return

  _atomic_write_text(Path(dest), content)


def _read_source_text(
    source: str,
    *,
    stdin_text: str | None = None,
    project: str | None = None) -> tuple[str, str]:
  """Returns `(content, inferred_basename)` from local path, `gs://`, or `-`."""
  from . import output as out_mod
  if source == '-':
    try:
      data = (
          out_mod.read_bounded_text(sys.stdin, label='stdin pipeline')
          if stdin_text is None else stdin_text)
      if len(data.encode('utf-8', errors='replace')) > out_mod.MAX_YAML_BYTES:
        raise ValueError(
            f'stdin pipeline exceeds maximum size ({out_mod.MAX_YAML_BYTES:,} bytes)'
        )
    except ValueError as e:
      raise PackageError(str(e), EXIT_USAGE) from None
    if not data.strip():
      raise PackageError('stdin pipeline content is empty.', EXIT_USAGE)
    return data, 'pipeline.yaml'

  if source.startswith('gs://'):
    without_scheme = source[len('gs://'):]
    parts = without_scheme.split('/', 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
      raise PackageError(f'invalid GCS path: {source!r}', EXIT_USAGE)
    try:
      from google.cloud import storage
    except ImportError as e:
      raise PackageError(
          f'google-cloud-storage is required to read {source}: {e}',
          EXIT_MISSING_PACKAGE) from None
    client = storage.Client(project=project) if project else storage.Client()
    blob = client.bucket(parts[0]).blob(parts[1])
    raw_bytes = blob.download_as_bytes(start=0, end=out_mod.MAX_YAML_BYTES)
    if len(raw_bytes) > out_mod.MAX_YAML_BYTES:
      raise PackageError(
          f'pipeline file {source} exceeds maximum size ({out_mod.MAX_YAML_BYTES:,} bytes)',
          EXIT_USAGE)
    text = raw_bytes.decode('utf-8')
    return text, os.path.basename(parts[1]) or 'pipeline.yaml'

  p = Path(source)
  if p.is_symlink():
    raise PackageError(
        f'refusing symbolic link pipeline file: {source}', EXIT_USAGE)
  if not p.is_file():
    raise PackageError(f'pipeline file not found: {source}', EXIT_USAGE)
  try:
    return out_mod.read_bounded_text(p, label=source), p.name
  except ValueError as e:
    raise PackageError(str(e), EXIT_USAGE) from None


def _write_safe_local_file(target: Path, text: str) -> None:
  if target.is_symlink():
    raise PackageError(
        f'refusing to write through symbolic link {target}', EXIT_USAGE)
  target.write_text(text, encoding='utf-8')


def _build_launch_snippets(
    *,
    template_location: str,
    template_name: str,
    region: str,
    project: str | None,
    parameters: list[dict[str, Any]],
) -> dict[str, str]:
  import shlex
  required_params = [p['name'] for p in parameters if not p.get('isOptional')]
  sample_kv = ','.join(f'{k}=<VALUE>' for k in required_params)
  proj_flag = f' --project={shlex.quote(project)}' if project else ''
  param_flag = f' --parameters={shlex.quote(sample_kv)}' if sample_kv else ''
  if template_location.startswith('gs://'):
    gcs_spec_uri = template_location
    upload_prefix = ''
  else:
    gcs_spec_uri = f'gs://<BUCKET>/templates/{template_name}.json'
    upload_prefix = (
        f'gcloud storage cp {shlex.quote(template_location)} '
        f'{shlex.quote(gcs_spec_uri)} && ')
  gcloud_cmd = (
      f'{upload_prefix}gcloud dataflow flex-template run '
      f'{shlex.quote(template_name + "-job")}'
      f' --template-file-gcs-location={shlex.quote(gcs_spec_uri)}'
      f' --region={shlex.quote(region)}{proj_flag}{param_flag}')

  params_dict_literal = (
      '{' + ', '.join(f'{k!r}: "<VALUE>"' for k in required_params) +
      '}' if required_params else '{}')
  airflow_snippet = (
      'DataflowStartFlexTemplateOperator(\n'
      f'    task_id="run_{template_name.replace("-", "_")}",\n'
      f'    project_id={project or "<PROJECT_ID>"!r},\n'
      f'    location={region!r},\n'
      '    body={\n'
      '        "launchParameter": {\n'
      f'            "jobName": "{template_name}-{{{{ ds_nodash }}}}",\n'
      f'            "containerSpecGcsPath": {gcs_spec_uri!r},\n'
      f'            "parameters": {params_dict_literal},\n'
      '        }\n'
      '    },\n'
      ')')
  return {
      'gcloud': gcloud_cmd,
      'airflow_operator': airflow_snippet,
  }


def run_package(
    source: str,
    *,
    to_location: str | None = None,
    name: str | None = None,
    description: str | None = None,
    sdk: str = 'auto',
    image: str | None = None,
    base_image: str = DEFAULT_PYTHON_LAUNCHER_BASE,
    region: str = DEFAULT_REGION,
    project: str | None = None,
    subnetwork: str | None = None,
    network: str | None = None,
    no_use_public_ips: bool = False,
    staging_location: str | None = None,
    temp_location: str | None = None,
    service_account_email: str | None = None,
    worker_machine_type: str | None = None,
    max_workers: int | None = None,
    num_workers: int | None = None,
    enable_streaming_engine: bool = False,
    requirements_file: str | None = None,
    output_dir: str | None = None,
    build_image: bool = False,
    dry_run: bool = False,
    stdin_text: str | None = None,
) -> dict[str, Any]:
  """Packages a `.yaml` or `.py` pipeline into a Dataflow Flex Template."""
  if project is not None:
    p_str = str(project).strip()
    if (not p_str or p_str.startswith('-') or
        any(ch in p_str for ch in ('\r', '\n', '\x00'))):
      raise PackageError(
          f'invalid GCP project identifier {project!r}', EXIT_USAGE)
    if build_image or not dry_run:
      from . import uris
      try:
        project = uris.validate_bq_project_id(p_str)
      except uris.UriError as e:
        raise PackageError(str(e), EXIT_USAGE) from None
    else:
      project = p_str
  source_text, source_basename = _read_source_text(
      source, stdin_text=stdin_text, project=project)

  sdk_norm = (sdk or 'auto').strip().lower()
  if sdk_norm not in ('auto', 'yaml', 'python'):
    raise PackageError(
        f"unsupported --sdk {sdk!r}; expected 'auto', 'yaml', or 'python'.",
        EXIT_USAGE)

  if requirements_file:
    req_p = Path(requirements_file)
    if req_p.is_symlink():
      raise PackageError(
          f'refusing symbolic link requirements file: {requirements_file}',
          EXIT_USAGE)
    if not req_p.is_file():
      raise PackageError(
          f'requirements file not found: {requirements_file}', EXIT_USAGE)
    _scan_requirements_file_for_secrets(req_p)

  is_py_file = source_basename.endswith('.py')
  if is_py_file:
    _scan_python_source_for_secrets(source_text, source_basename)
  else:
    from . import output as out_mod
    from . import uris as uris_mod
    if (_SECRET_VALUE_TOKEN_RE.search(source_text) or
        uris_mod._INLINE_URI_USERINFO_RE.search(source_text) or
        out_mod.redact_inline_secrets(source_text) != source_text):
      raise PackageError(
          'refusing to bake inline credentials or tokens into Flex Template '
          'YAML; parameterize secrets with Jinja `{{ secret_param }}` or '
          'Google Cloud Secret Manager.',
          EXIT_USAGE)
  if sdk_norm == 'auto':
    resolved_sdk = 'PYTHON' if (is_py_file or requirements_file) else 'YAML'
  else:
    resolved_sdk = sdk_norm.upper()

  if is_py_file and resolved_sdk == 'YAML':
    raise PackageError(
        'Python source files (.py) cannot be packaged with --sdk=yaml; '
        'use --sdk=python.',
        EXIT_USAGE)

  if not to_location and not output_dir and not dry_run:
    raise PackageError(
        'specify --to <gs://bucket/template.json | local.json>, '
        '--output-dir <DIR>, or --dry-run.',
        EXIT_USAGE)

  region = _validate_region(region)
  stem = Path(source_basename).stem or 'beam-pipeline'
  safe_name = re.sub(r'[^a-z0-9-]+', '-', (name or stem).lower()).strip('-')
  if not safe_name:
    safe_name = 'beam-pipeline'

  default_env = _build_default_environment(
      subnetwork=subnetwork,
      network=network,
      no_use_public_ips=no_use_public_ips,
      staging_location=staging_location,
      temp_location=temp_location,
      service_account_email=service_account_email,
      worker_machine_type=worker_machine_type,
      max_workers=max_workers,
      num_workers=num_workers,
      worker_region=region,
      enable_streaming_engine=enable_streaming_engine,
  )

  warnings_out: list[str] = []
  written_files: list[str] = []

  def _mapping_has_literal_secret(obj: Any) -> str | None:
    from . import uris as uris_mod
    if isinstance(obj, dict):
      for k, v in obj.items():
        ks = str(k)
        if not ks.lower().endswith('_env') and uris_mod._SECRET_KEY_RE.search(
            ks):
          sv = '' if v is None else str(v).strip()
          if sv and sv not in (
              '<REDACTED_SECRET>',
              '<redacted>',
              '__BEAM_JINJA_PLACEHOLDER__',
          ):
            return ks
        nested = _mapping_has_literal_secret(v)
        if nested:
          return nested
    elif isinstance(obj, list):
      for item in obj:
        nested = _mapping_has_literal_secret(item)
        if nested:
          return nested
    return None

  if resolved_sdk == 'YAML':
    yaml_doc = _validate_yaml_with_jinja_placeholders(source_text)
    bad_sec_key = _mapping_has_literal_secret(yaml_doc)
    if bad_sec_key:
      raise PackageError(
          f'refusing to bake secret key {bad_sec_key!r} into Flex Template '
          'metadata; parameterize via Jinja `{{ ... }}` or Secret Manager.',
          EXIT_USAGE)
    custom_provs = _find_custom_python_providers(yaml_doc)
    if custom_provs:
      if sdk_norm == 'auto':
        resolved_sdk = 'PYTHON'
        warnings_out.append(
            'Auto-selected --sdk=python because YAML pipeline references '
            f'custom Python providers ({", ".join(custom_provs)}).')
      else:
        raise PackageError(
            'YAML pipeline references custom Python providers '
            f'({", ".join(custom_provs)}) not present in the stock Dataflow '
            'YAML template container. Re-run with `--sdk=python '
            '--output-dir=./flex_bundle --image=<ARTIFACT_REGISTRY_IMAGE>` '
            'to bundle custom Python providers into a single-image Flex Template.',
            EXIT_USAGE)

  if resolved_sdk == 'YAML':
    needed_yaml_pkgs = _detect_yaml_extra_packages(yaml_doc)
    opts_in_yaml = (
        yaml_doc.get('options') if isinstance(yaml_doc, dict) and
        isinstance(yaml_doc.get('options'), dict) else {})
    if (needed_yaml_pkgs and not opts_in_yaml.get('requirements_file') and
        not opts_in_yaml.get('extra_packages')):
      warnings_out.append(
          'Pipeline references transforms requiring extra Python packages '
          f'({", ".join(needed_yaml_pkgs)}) not preinstalled in the stock '
          'Dataflow YAML container; consider `--sdk=python '
          '--requirements=<requirements.txt>` for hermetic packaging.')
    parameters = extract_jinja_parameters(source_text)
    allow_net = bool(
        not dry_run or (to_location and to_location.startswith('gs://')))
    resolved_image, img_warnings = resolve_yaml_template_image(
        region=region, explicit_image=image, allow_network=allow_net)
    warnings_out.extend(img_warnings)

    yaml_def = source_text
    # Workaround for GoogleCloudPlatform/DataflowTemplates#4565 (private-IP).
    if (no_use_public_ips and 's3://' not in source_text and
        'azfs://' not in source_text):
      opts_block = yaml_doc.get('options') if isinstance(
          yaml_doc, dict) else None
      if opts_block is None and not re.search(r'(?m)^options\s*:', yaml_def):
        yaml_def = yaml_def.rstrip(
            '\n') + '\n\noptions:\n  requirements_file: null\n'
      elif isinstance(opts_block,
                      dict) and 'requirements_file' not in opts_block:
        yaml_def = re.sub(
            r'(?m)^options\s*:\s*$',
            'options:\n  requirements_file: null',
            yaml_def,
            count=1)

    metadata: dict[str, Any] = {
        'name': safe_name,
        'description': description or f'Beam YAML Flex Template ({safe_name}).',
        'parameters': parameters,
        'yamlDefinition': yaml_def,
    }
    container_spec: dict[str, Any] = {
        'image': resolved_image,
        'metadata': metadata,
        'sdkInfo': {
            'language': 'YAML'
        },
    }
    if default_env:
      container_spec['defaultEnvironment'] = default_env

    spec_json = json.dumps(container_spec, indent=2, sort_keys=True) + '\n'
    if output_dir and not dry_run:
      out_d = Path(output_dir)
      if out_d.is_symlink():
        raise PackageError(
            f'refusing symbolic link --output-dir {output_dir}', EXIT_USAGE)
      out_d.mkdir(parents=True, exist_ok=True)
      p_spec = out_d / f'{safe_name}.template.json'
      _write_safe_local_file(p_spec, spec_json)
      written_files.append(str(p_spec))
      p_yaml = out_d / 'pipeline.yaml'
      _write_safe_local_file(p_yaml, source_text)
      written_files.append(str(p_yaml))

    if to_location and not dry_run:
      if '://' not in to_location and Path(to_location).is_symlink():
        raise PackageError(
            f'refusing symbolic link --to {to_location}', EXIT_USAGE)
      _write_text_destination(to_location, spec_json, project=project)
      written_files.append(to_location)

    spec_ref = to_location or (
        str(Path(output_dir) / f'{safe_name}.template.json')
        if output_dir else f'gs://<BUCKET>/templates/{safe_name}.json')
    return {
        'name': safe_name,
        'sdk_language': 'YAML',
        'docker_build_required': False,
        'image': resolved_image,
        'template_location': to_location,
        'parameters': parameters,
        'written_files': written_files,
        'container_spec': container_spec,
        'launch': _build_launch_snippets(
            template_location=spec_ref,
            template_name=safe_name,
            region=region,
            project=project,
            parameters=parameters),
        'warnings': warnings_out,
    }

  # `resolved_sdk == 'PYTHON'`

  extra_wheels: list[str] = []
  bundle_preview_wheel_path: Path | None = None
  if is_py_file:
    parameters = extract_python_pipeline_parameters(source_text)
    entrypoint_name = source_basename
    entrypoint_code = source_text
    extra_yaml_name = None
    if _py_source_references_beam_cli(source_text):
      bundle_preview_wheel_path = _ensure_cli_bundle_wheel()
      extra_wheels.append(bundle_preview_wheel_path.name)
  else:
    yaml_doc = _validate_yaml_with_jinja_placeholders(source_text)
    bad_sec_key = _mapping_has_literal_secret(yaml_doc)
    if bad_sec_key:
      raise PackageError(
          f'refusing to bake secret key {bad_sec_key!r} into Flex Template '
          'bundle; parameterize via Jinja `{{ ... }}` or Secret Manager.',
          EXIT_USAGE)
    parameters = extract_jinja_parameters(source_text)
    extra_yaml_name = 'pipeline.yaml'
    entrypoint_name = 'flex_yaml_entrypoint.py'
    entrypoint_code = _generate_yaml_python_wrapper(
        extra_yaml_name, [p['name'] for p in parameters])
    # Always bundle the pure-Python CLI wheel when YAML uses `apache_beam.cli.*`
    # providers/constructors, synthesizing it directly if cli-preview is absent!
    custom_provs = _find_custom_python_providers(yaml_doc)
    if any(p.startswith('apache_beam.cli.') for p in custom_provs):
      bundle_preview_wheel_path = _ensure_cli_bundle_wheel()
      extra_wheels.append(bundle_preview_wheel_path.name)
    if not requirements_file:
      needed_pkgs = _detect_yaml_extra_packages(yaml_doc)
      if needed_pkgs:
        warnings_out.append(
            'Pipeline references transforms requiring extra Python packages '
            f'({", ".join(needed_pkgs)}) not preinstalled in the base Beam '
            'container; pass --requirements <requirements.txt> before building '
            'the Flex Template image.')

  if build_image and not image:
    raise PackageError(
        '--build-image requires --image <REGION-docker.pkg.dev/PROJECT/REPO/IMG:TAG>.',
        EXIT_USAGE)

  resolved_py_image = (
      _validate_image_uri(image, '--image') if image else
      f'{region}-docker.pkg.dev/{project or "<PROJECT>"}/dataflow/{safe_name}:latest'
  )
  if not image:
    warnings_out.append(
        'No --image supplied; generated placeholder image URI '
        f'{resolved_py_image!r}. Pass --image (and optionally --build-image) '
        'before launching on Cloud Dataflow.')

  dockerfile_text = generate_python_dockerfile(
      entrypoint_name,
      base_image=base_image,
      has_requirements=bool(requirements_file),
      extra_wheels=extra_wheels,
      extra_files=[extra_yaml_name] if extra_yaml_name else None)

  # Single-image best practice: align worker `sdk_container_image` with the
  # Flex Template image so Dataflow workers skip runtime `pip install` cold starts.
  py_default_env = dict(default_env)
  metadata = {
      'name': safe_name,
      'description': description or f'Beam Python Flex Template ({safe_name}).',
      'parameters': parameters,
  }
  container_spec = {
      'image': resolved_py_image,
      'metadata': metadata,
      'sdkInfo': {
          'language': 'PYTHON'
      },
  }
  if py_default_env:
    container_spec['defaultEnvironment'] = py_default_env

  spec_json = json.dumps(container_spec, indent=2, sort_keys=True) + '\n'
  metadata_json = json.dumps(metadata, indent=2, sort_keys=True) + '\n'

  def _populate_bundle_dir(target_dir: Path) -> None:
    if target_dir.is_symlink():
      raise PackageError(
          f'refusing symbolic link bundle directory {target_dir}', EXIT_USAGE)
    target_dir.mkdir(parents=True, exist_ok=True)
    _write_safe_local_file(target_dir / 'Dockerfile', dockerfile_text)
    _write_safe_local_file(target_dir / entrypoint_name, entrypoint_code)
    _write_safe_local_file(target_dir / 'metadata.json', metadata_json)
    _write_safe_local_file(target_dir / f'{safe_name}.template.json', spec_json)
    if extra_yaml_name:
      _write_safe_local_file(target_dir / extra_yaml_name, source_text)
    if requirements_file:
      req_dst = target_dir / 'requirements.txt'
      if req_dst.is_symlink():
        raise PackageError(f'refusing symbolic link {req_dst}', EXIT_USAGE)
      shutil.copy2(requirements_file, req_dst)
    if bundle_preview_wheel_path is not None:
      whl_dst = target_dir / bundle_preview_wheel_path.name
      if whl_dst.is_symlink():
        raise PackageError(f'refusing symbolic link {whl_dst}', EXIT_USAGE)
      shutil.copy2(bundle_preview_wheel_path, whl_dst)

  if output_dir and not dry_run:
    out_d = Path(output_dir)
    _populate_bundle_dir(out_d)
    for fname in sorted(os.listdir(out_d)):
      written_files.append(str(out_d / fname))

  if build_image and not dry_run:
    gcloud_bin = shutil.which('gcloud')
    if not gcloud_bin:
      raise PackageError(
          'gcloud CLI is required for --build-image.', EXIT_MISSING_PACKAGE)
    with tempfile.TemporaryDirectory(prefix='beam-flex-build-') as tmp_d:
      build_ctx = Path(output_dir) if output_dir else Path(tmp_d)
      if not output_dir:
        _populate_bundle_dir(build_ctx)
      cmd = [
          gcloud_bin,
          'builds',
          'submit',
          str(build_ctx),
          '--tag',
          resolved_py_image
      ]
      if project:
        cmd.extend(['--project', project])
      proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
      if proc.returncode != 0:
        raise PackageError(
            f'Cloud Build failed (exit {proc.returncode}): '
            f'{(proc.stderr or proc.stdout).strip()[-800:]}',
            1)

  if to_location and not dry_run:
    _write_text_destination(to_location, spec_json, project=project)
    written_files.append(to_location)

  spec_ref = to_location or (
      str(Path(output_dir) / f'{safe_name}.template.json')
      if output_dir else f'gs://<BUCKET>/templates/{safe_name}.json')
  return {
      'name': safe_name,
      'sdk_language': 'PYTHON',
      'docker_build_required': True,
      'image': resolved_py_image,
      'image_built': bool(build_image and not dry_run),
      'template_location': to_location,
      'parameters': parameters,
      'dockerfile': dockerfile_text,
      'written_files': written_files,
      'container_spec': container_spec,
      'launch': _build_launch_snippets(
          template_location=spec_ref,
          template_name=safe_name,
          region=region,
          project=project,
          parameters=parameters),
      'warnings': warnings_out,
  }
