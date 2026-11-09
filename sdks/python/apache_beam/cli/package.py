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
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
from typing import Any
import urllib.error
import urllib.request

import yaml

EXIT_USAGE = 2
EXIT_MISSING_PACKAGE = 4

DEFAULT_REGION = 'us-central1'
FALLBACK_YAML_TEMPLATE_IMAGE = 'gcr.io/dataflow-templates/latest/yaml-template'
DEFAULT_PYTHON_LAUNCHER_BASE = (
    'gcr.io/dataflow-templates-base/python312-template-launcher-base:latest')
_SAFE_PARAM_NAME_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_JINJA_EXPR_RE = re.compile(r'\{\{.*?\}\}|\{%.*?%\}|\{#.*?#\}', re.DOTALL)


class PackageError(Exception):
  """Raised when `beam package` validation or packaging fails."""
  def __init__(self, message: str, exit_code: int = EXIT_USAGE):
    super().__init__(message)
    self.exit_code = exit_code


def _humanize_label(param_name: str) -> str:
  words = param_name.replace('-', '_').split('_')
  label = ' '.join(w for w in words if w).strip()
  return label[:1].upper() + label[1:] if label else param_name


def extract_jinja_parameters(yaml_text: str) -> list[dict[str, Any]]:
  """Extracts Jinja2 variables & `default(...)` filters via AST (no execution)."""
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
  if not cleaned or not re.fullmatch(
      r'[A-Za-z0-9][A-Za-z0-9._/:@-]{0,511}', cleaned):
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


def _validate_yaml_with_jinja_placeholders(yaml_text: str) -> dict[str, Any]:
  """Validates top-level Beam YAML structure even when `{{ vars }}` are present."""
  from . import output as out_mod
  scrubbed = _JINJA_EXPR_RE.sub('__BEAM_JINJA_PLACEHOLDER__', yaml_text)
  try:
    doc = out_mod.safe_yaml_load(scrubbed)
  except yaml.YAMLError as e:
    raise PackageError(f'invalid YAML syntax: {e}', EXIT_USAGE) from None
  if not isinstance(doc, dict) or 'pipeline' not in doc:
    raise PackageError(
        "Beam YAML file must be a mapping containing a top-level 'pipeline:' key.",
        EXIT_USAGE)
  return doc


def _find_custom_python_providers(doc: dict[str, Any]) -> list[str]:
  """Returns non-stock Python provider packages referenced in `providers:`."""
  providers = doc.get('providers')
  if not isinstance(providers, list):
    return []
  custom: list[str] = []
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
  return custom


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
    if not isinstance(node, ast.Call) or not isinstance(
        node.func, ast.Attribute):
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
    for reg in (safe_reg, DEFAULT_REGION):
      url = (
          f'https://storage.googleapis.com/dataflow-templates-{reg}/'
          'latest/flex/Yaml_Template')
      try:
        req = urllib.request.Request(
            url, headers={'User-Agent': 'apache-beam-cli-package'})
        with urllib.request.urlopen(req, timeout=5.0) as resp:
          spec = json.loads(resp.read().decode('utf-8'))
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
    ])
  lines.extend([
      f'COPY {safe_ep} /template/{safe_ep}',
      f'ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/{safe_ep}"',
      '',
  ])
  return '\n'.join(lines)


def _generate_yaml_python_wrapper(
    yaml_filename: str, param_names: list[str] | None = None) -> str:
  """Generates a minimal Python Flex entrypoint wrapping a Beam YAML file."""
  names_literal = repr(sorted(set(param_names or ())))
  return (
      '"""Auto-generated Beam YAML Flex Template launcher."""\n'
      'import json\n'
      'from pathlib import Path\n'
      'import sys\n\n'
      'try:\n'
      '  from apache_beam_cli_preview import launcher as _prev\n'
      '  _prev.install_runtime_hooks()\n'
      'except ImportError:\n'
      '  pass\n\n'
      'from apache_beam.yaml import main as yaml_main\n\n'
      f'_JINJA_PARAMS = set({names_literal})\n\n'
      'def _split_jinja_argv(raw_argv):\n'
      '  jinja_vars = {}\n'
      '  passthrough = []\n'
      '  i = 0\n'
      '  while i < len(raw_argv):\n'
      '    tok = raw_argv[i]\n'
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
      '  return passthrough\n\n'
      'if __name__ == "__main__":\n'
      f'  yaml_path = str(Path(__file__).resolve().with_name({yaml_filename!r}))\n'
      '  forwarded = _split_jinja_argv(sys.argv[1:])\n'
      '  yaml_main.run(argv=["--yaml_pipeline_file=" + yaml_path] + forwarded)\n'
  )


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
    blob.upload_from_string(content.encode('utf-8'), content_type='application/json')
    return

  out_path = Path(dest)
  out_path.parent.mkdir(parents=True, exist_ok=True)
  out_path.write_text(content, encoding='utf-8')


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
  if not p.is_file():
    raise PackageError(f'pipeline file not found: {source}', EXIT_USAGE)
  try:
    return out_mod.read_bounded_text(p, label=source), p.name
  except ValueError as e:
    raise PackageError(str(e), EXIT_USAGE) from None


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
  gcloud_cmd = (
      f'gcloud dataflow flex-template run {shlex.quote(template_name + "-job")}'
      f' --template-file-gcs-location={shlex.quote(template_location)}'
      f' --region={shlex.quote(region)}{proj_flag}{param_flag}')

  params_dict_literal = (
      '{' + ', '.join(f'{k!r}: "<VALUE>"' for k in required_params) + '}'
      if required_params else '{}')
  airflow_snippet = (
      'DataflowStartFlexTemplateOperator(\n'
      f'    task_id="run_{template_name.replace("-", "_")}",\n'
      f'    project_id={project or "<PROJECT_ID>"!r},\n'
      f'    location={region!r},\n'
      '    body={\n'
      '        "launchParameter": {\n'
      f'            "jobName": "{template_name}-{{{{ ds_nodash }}}}",\n'
      f'            "containerSpecGcsPath": {template_location!r},\n'
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
  source_text, source_basename = _read_source_text(
      source, stdin_text=stdin_text, project=project)

  sdk_norm = (sdk or 'auto').strip().lower()
  if sdk_norm not in ('auto', 'yaml', 'python'):
    raise PackageError(
        f"unsupported --sdk {sdk!r}; expected 'auto', 'yaml', or 'python'.",
        EXIT_USAGE)

  is_py_file = source_basename.endswith('.py')
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

  if resolved_sdk == 'YAML':
    yaml_doc = _validate_yaml_with_jinja_placeholders(source_text)
    custom_provs = _find_custom_python_providers(yaml_doc)
    if custom_provs:
      raise PackageError(
          'YAML pipeline references custom Python providers '
          f'({", ".join(custom_provs)}) not present in the stock Dataflow '
          'YAML template container. Re-run with `--sdk=python '
          '--output-dir=./flex_bundle --image=<ARTIFACT_REGISTRY_IMAGE>` '
          'to bundle custom Python providers into a single-image Flex Template.',
          EXIT_USAGE)

    parameters = extract_jinja_parameters(source_text)
    allow_net = bool(not dry_run or (to_location and to_location.startswith('gs://')))
    resolved_image, img_warnings = resolve_yaml_template_image(
        region=region, explicit_image=image, allow_network=allow_net)
    warnings_out.extend(img_warnings)

    yaml_def = source_text
    if (
        no_use_public_ips
        and 's3://' not in source_text
        and 'azfs://' not in source_text
    ):
      opts_block = yaml_doc.get('options') if isinstance(yaml_doc, dict) else None
      if opts_block is None and not re.search(r'(?m)^options\s*:', yaml_def):
        yaml_def = yaml_def.rstrip('\n') + '\n\noptions:\n  requirements_file: null\n'
      elif isinstance(opts_block, dict) and 'requirements_file' not in opts_block:
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
        'sdkInfo': {'language': 'YAML'},
    }
    if default_env:
      container_spec['defaultEnvironment'] = default_env

    spec_json = json.dumps(container_spec, indent=2, sort_keys=True) + '\n'
    if output_dir and not dry_run:
      out_d = Path(output_dir)
      out_d.mkdir(parents=True, exist_ok=True)
      p_spec = out_d / f'{safe_name}.template.json'
      p_spec.write_text(spec_json, encoding='utf-8')
      written_files.append(str(p_spec))
      p_yaml = out_d / 'pipeline.yaml'
      p_yaml.write_text(source_text, encoding='utf-8')
      written_files.append(str(p_yaml))

    if to_location and not dry_run:
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
  if requirements_file and not Path(requirements_file).is_file():
    raise PackageError(
        f'requirements file not found: {requirements_file}', EXIT_USAGE)

  extra_wheels: list[str] = []
  bundle_preview_wheel_path: Path | None = None
  if is_py_file:
    parameters = extract_python_pipeline_parameters(source_text)
    entrypoint_name = source_basename
    entrypoint_code = source_text
    extra_yaml_name = None
  else:
    yaml_doc = _validate_yaml_with_jinja_placeholders(source_text)
    parameters = extract_jinja_parameters(source_text)
    extra_yaml_name = 'pipeline.yaml'
    entrypoint_name = 'flex_yaml_entrypoint.py'
    entrypoint_code = _generate_yaml_python_wrapper(
        extra_yaml_name, [p['name'] for p in parameters])
    # If running inside `apache-beam-cli-preview` and YAML uses `apache_beam.cli.*`
    # providers (such as `ReadCsvWithDlq`), automatically include the pure-Python
    # preview wheel so the Flex container is 100% self-contained!
    custom_provs = _find_custom_python_providers(yaml_doc)
    if any(p.startswith('apache_beam.cli.') for p in custom_provs):
      try:
        from apache_beam_cli_preview import launcher as _prev_launcher
        bundle_preview_wheel_path = Path(_prev_launcher.ensure_preview_wheel())
        extra_wheels.append(bundle_preview_wheel_path.name)
      except ImportError:
        pass

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
      extra_wheels=extra_wheels)

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
      'sdkInfo': {'language': 'PYTHON'},
  }
  if py_default_env:
    container_spec['defaultEnvironment'] = py_default_env

  spec_json = json.dumps(container_spec, indent=2, sort_keys=True) + '\n'
  metadata_json = json.dumps(metadata, indent=2, sort_keys=True) + '\n'

  def _populate_bundle_dir(target_dir: Path) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    p_df = target_dir / 'Dockerfile'
    p_df.write_text(dockerfile_text, encoding='utf-8')
    p_ep = target_dir / entrypoint_name
    p_ep.write_text(entrypoint_code, encoding='utf-8')
    p_meta = target_dir / 'metadata.json'
    p_meta.write_text(metadata_json, encoding='utf-8')
    p_spec = target_dir / f'{safe_name}.template.json'
    p_spec.write_text(spec_json, encoding='utf-8')
    if extra_yaml_name:
      (target_dir / extra_yaml_name).write_text(source_text, encoding='utf-8')
    if requirements_file:
      shutil.copy2(requirements_file, target_dir / 'requirements.txt')
    if bundle_preview_wheel_path is not None:
      shutil.copy2(
          bundle_preview_wheel_path,
          target_dir / bundle_preview_wheel_path.name)

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
      cmd = [gcloud_bin, 'builds', 'submit', str(build_ctx), '--tag', resolved_py_image]
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
