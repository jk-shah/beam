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

  loader = getattr(yaml, 'CSafeLoader', yaml.SafeLoader)
  try:
    spec = yaml.load(raw_text, Loader=loader)
  except yaml.YAMLError as e:
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

  canonical_json = json.dumps(spec, sort_keys=True, separators=(',', ':'))
  plan_hash = 'sha256:' + hashlib.sha256(
      canonical_json.encode('utf-8')).hexdigest()

  expanded_transforms: list[str] | None = None
  if expand_plan:
    if has_untrusted_providers and not trust_providers:
      raise CheckError(
          f'{source_label} declares custom "providers:" which execute code '
          'during --plan expansion; re-run with --trust-providers to allow '
          'provider execution')
    import apache_beam as beam
    from apache_beam.yaml import main as yaml_main
    dumper = getattr(yaml, 'CSafeDumper', yaml.SafeDumper)
    clean_yaml = yaml.dump(spec, Dumper=dumper, sort_keys=False)
    options, constructor = (
        yaml_main.build_pipeline_components_from_yaml(
            clean_yaml, list(pipeline_args or []), validate_schema='generic'))
    p = beam.Pipeline(options=options, display_data={'yaml': clean_yaml})
    constructor(p)
    proto = p.to_runner_api()
    expanded_transforms = sorted(
        t.unique_name for t in proto.components.transforms.values()
        if t.unique_name and '/' not in t.unique_name)

  return {
      'valid': True,
      'source': source_label,
      'mode': 'expanded_plan' if expand_plan else 'offline_static',
      'plan_hash': plan_hash,
      'transform_count': len(stages),
      'transforms': stages,
      'has_custom_providers': has_custom_providers,
      'expanded_top_level_transforms': expanded_transforms,
  }
