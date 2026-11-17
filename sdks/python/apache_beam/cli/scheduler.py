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

"""Enterprise Orchestration Export for Apache Beam pipelines.

This module provides declarative schedule export capabilities (`beam export-schedule`)
that generate production-grade orchestration deployment artifacts:
1. Google Cloud Composer (Apache Airflow) DAGs using `DataflowStartFlexTemplateOperator`.
2. Serverless Google Cloud Scheduler HTTP triggers launching Dataflow Flex Templates.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any
from typing import NamedTuple

VALID_ORCHESTRATORS = frozenset(['composer', 'cloud-scheduler'])

_CRON_MACROS = frozenset([
    '@yearly',
    '@annually',
    '@monthly',
    '@weekly',
    '@daily',
    '@midnight',
    '@hourly',
])


class SchedulerError(Exception):
  """Raised when schedule generation fails due to invalid parameters or templates."""
  def __init__(self, message: str, exit_code: int = 2):
    super().__init__(message)
    self.exit_code = exit_code


ScheduleError = SchedulerError


class ScheduleExportReport(NamedTuple):
  orchestrator: str
  schedule: str
  destination: str | None
  content: str
  dag_id: str | None
  job_name: str | None


_CRON_TOKEN_RE = re.compile(r'^[A-Za-z0-9*,/_#?-]+$')


def validate_cron_expression(expr: str) -> str:
  """Validates standard 5-field cron syntax or standard Airflow macros."""
  raw = str(expr)
  if any(ch in raw for ch in ('\r', '\n', '\x00')):
    raise SchedulerError(
        f'Invalid cron expression {expr!r}; newline and control characters '
        'are not allowed.')
  cleaned = raw.strip(' \t')
  if not cleaned:
    raise SchedulerError('Schedule expression cannot be empty.')
  if cleaned.lower() in _CRON_MACROS:
    return cleaned.lower()
  parts = re.split(r'[ \t]+', cleaned)
  if len(parts) != 5 or not all(_CRON_TOKEN_RE.match(p) for p in parts):
    raise SchedulerError(
        f'Invalid cron expression {expr!r}; expected standard 5-part cron syntax '
        '(minute hour day-of-month month day-of-week) or macro (@daily, @hourly).'
    )
  return ' '.join(parts)


def _sanitize_identifier(name: str) -> str:
  """Sanitizes strings for safe Python identifiers and Airflow DAG IDs."""
  sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name).strip('_').lower()
  if not sanitized:
    return 'beam_pipeline'
  if sanitized[0].isdigit():
    sanitized = 'dag_' + sanitized
  return sanitized


_JINJA_PLACEHOLDER_RE = re.compile(
    r'\{\{.*?\}\}|\{%.*?%\}|\{#.*?#\}', re.DOTALL)
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


def _parse_yaml_options_dict(raw_text: str, label: str) -> dict[str, Any]:
  from . import output
  try:
    data = output.safe_yaml_load(raw_text)
  except Exception as first_exc:  # pylint: disable=broad-except
    if '{{' in raw_text or '{%' in raw_text:
      try:
        output.validate_safe_jinja_template(raw_text)
        scrubbed = _JINJA_PLACEHOLDER_RE.sub(
            '__BEAM_JINJA_PLACEHOLDER__', raw_text)
        data = output.safe_yaml_load(scrubbed)
      except Exception:  # pylint: disable=broad-except
        raise SchedulerError(f'invalid YAML in {label}: {first_exc}') from None
    else:
      raise SchedulerError(f'invalid YAML in {label}: {first_exc}') from None

  if not isinstance(data, dict):
    if label == 'stdin YAML':
      raise SchedulerError('stdin YAML must be a mapping')
    return {}
  opts = data.get('options', {}) or {}
  if not isinstance(opts, dict):
    return {}
  normalized: dict[str, Any] = {}
  for k, v in opts.items():
    if isinstance(v, str) and v.strip() == '__BEAM_JINJA_PLACEHOLDER__':
      continue
    norm_k = str(k).strip().lstrip('-').replace('-', '_')
    normalized[norm_k] = v
  return normalized


def _load_pipeline_options_from_source(source: str) -> dict[str, Any]:
  """Extracts options block from a YAML pipeline file or stdin if provided."""
  import sys

  from . import output

  if not source or '://' in source:
    return {}
  s_trim = source.strip()
  if s_trim.startswith(('beam ', 'apache-beam ')):
    return {}
  if s_trim == '-':
    try:
      raw_text = output.read_bounded_text(sys.stdin, label='stdin YAML')
    except Exception as exc:
      raise SchedulerError(f'invalid YAML on stdin: {exc}') from None
    return _parse_yaml_options_dict(raw_text, 'stdin YAML')

  if s_trim.lower().endswith(('.yaml', '.yml')):
    if os.path.islink(s_trim):
      raise SchedulerError(f'refusing symbolic link pipeline file {s_trim!r}')
    if not os.path.isfile(s_trim):
      raise SchedulerError(f'pipeline file not found: {s_trim}')
    try:
      raw_text = output.read_bounded_text(s_trim, label=s_trim)
    except Exception as exc:
      raise SchedulerError(f'invalid YAML in {s_trim!r}: {exc}') from None
    return _parse_yaml_options_dict(raw_text, repr(s_trim))

  if os.path.isfile(s_trim) and not os.path.islink(s_trim):
    try:
      raw_text = output.read_bounded_text(s_trim, label=s_trim)
      return _parse_yaml_options_dict(raw_text, repr(s_trim))
    except Exception:  # pylint: disable=broad-except
      return {}
  return {}


def _is_truthy_opt(val: Any) -> bool:
  if isinstance(val, bool):
    return val
  if val is None:
    return False
  return str(val).strip().lower() in ('1', 'true', 'yes')


def _validate_param_value_no_secrets(key: str, val: str) -> None:
  from . import output
  from . import uris
  if uris._INLINE_URI_USERINFO_RE.search(val):
    raise SchedulerError(
        f'Refusing to bake inline credentials in parameter {key!r}; '
        'pass a Secret Manager resource name instead')
  try:
    uris.reject_inline_credentials(val)
  except uris.UriError as exc:
    raise SchedulerError(
        f'Refusing to bake inline credentials in parameter {key!r}: {exc}'
    ) from None
  if _SECRET_VALUE_TOKEN_RE.search(val) or (output.redact_inline_secrets(val)
                                            != val):
    raise SchedulerError(
        f'Refusing to bake inline secret or token in parameter {key!r}; '
        'pass a Secret Manager resource name instead')


def _validate_options_no_secrets(obj: Any, key_path: str = 'options') -> None:
  """Recursively scans YAML `options:` for secret keys or inline secret tokens."""
  from . import uris
  if isinstance(obj, dict):
    for k, v in obj.items():
      ks = str(k).strip()
      sub_path = f'{key_path}.{ks}'
      if not ks.lower().endswith('_env') and uris._SECRET_KEY_RE.search(ks):
        sv = '' if v is None else str(v).strip()
        if sv and sv not in (
            '<REDACTED_SECRET>',
            '<redacted>',
            '__BEAM_JINJA_PLACEHOLDER__',
        ):
          raise SchedulerError(
              f'Refusing to bake secret option {ks!r} from pipeline YAML into '
              'exported schedule; pass a Secret Manager resource name instead')
      _validate_options_no_secrets(v, sub_path)
  elif isinstance(obj, (list, tuple)):
    for idx, item in enumerate(obj):
      _validate_options_no_secrets(item, f'{key_path}[{idx}]')
  elif isinstance(obj, str):
    sv = obj.strip()
    if sv and sv not in (
        '<REDACTED_SECRET>',
        '<redacted>',
        '__BEAM_JINJA_PLACEHOLDER__',
    ):
      _validate_param_value_no_secrets(key_path, obj)


def generate_composer_dag(
    *,
    dag_id: str,
    schedule: str,
    project: str,
    location: str,
    template_gcs_path: str,
    job_name_pattern: str,
    parameters: dict[str, str] | None = None,
    environment: dict[str, Any] | None = None,
    owner: str = 'data-engineering',
    retries: int = 2,
    retry_delay_min: int = 5,
    catchup: bool = False,
    max_active_runs: int = 1,
) -> str:
  """Generates Python source code for a Cloud Composer (Airflow) DAG."""
  params = dict(parameters or {})
  env = dict(environment or {})

  import pprint
  params_repr = pprint.pformat(params, width=80, sort_dicts=True)
  env_repr = pprint.pformat(env, width=80, sort_dicts=True)

  code = f'''"""Automated Cloud Composer (Airflow) DAG generated by Apache Beam CLI (`beam export-schedule`)."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator

default_args = {{
    "owner": {repr(owner)},
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": {int(retries)},
    "retry_delay": timedelta(minutes={int(retry_delay_min)}),
}}

with DAG(
    dag_id={repr(dag_id)},
    default_args=default_args,
    schedule_interval={repr(schedule)},
    start_date=datetime(2026, 1, 1),
    catchup={bool(catchup)},
    max_active_runs={int(max_active_runs)},
    tags=["beam", "dataflow", "lakehouse"],
) as dag:

    launch_dataflow_flex_template = DataflowStartFlexTemplateOperator(
        task_id="run_flex_template",
        project_id={repr(project)},
        location={repr(location)},
        body={{
            "launchParameter": {{
                "jobName": {repr(job_name_pattern)},
                "containerSpecGcsPath": {repr(template_gcs_path)},
                "parameters": {params_repr},
                "environment": {env_repr},
            }}
        }},
    )
'''
  return code


def generate_cloud_scheduler_script(
    *,
    job_name: str,
    schedule: str,
    project: str,
    location: str,
    template_gcs_path: str,
    service_account_email: str | None = None,
    parameters: dict[str, str] | None = None,
    environment: dict[str, Any] | None = None,
    time_zone: str = 'Etc/UTC',
) -> str:
  """Generates a bash setup script provisioning a Cloud Scheduler HTTP trigger."""
  import shlex
  sa_flag = (
      f'  --oauth-service-account-email={shlex.quote(service_account_email)} \\\n'
      if service_account_email else
      '  --oauth-service-account-email="${SERVICE_ACCOUNT_EMAIL}" \\\n')

  body = {
      'launchParameter': {
          'jobName': f'{job_name}-$(date +%Y%m%d-%H%M%S)',
          'containerSpecGcsPath': template_gcs_path,
          'parameters': parameters or {},
          'environment': environment or {},
      }
  }
  body_json = json.dumps(body, indent=2)
  eof_delim = '__BEAM_PAYLOAD_EOF__'
  while eof_delim in body_json:
    eof_delim += '_X'

  script = f'''#!/usr/bin/env bash
# Cloud Scheduler HTTP trigger deployment script generated by Apache Beam CLI (`beam export-schedule`).
set -euo pipefail

PROJECT={shlex.quote(project)}
LOCATION={shlex.quote(location)}
JOB_NAME={shlex.quote(job_name)}
SCHEDULE={shlex.quote(schedule)}
TIME_ZONE={shlex.quote(time_zone)}

PAYLOAD_FILE="$(mktemp "${{TMPDIR:-/tmp}}/beam-sched-payload.XXXXXX")"
trap 'rm -f "${{PAYLOAD_FILE}}"' EXIT

cat << '{eof_delim}' > "${{PAYLOAD_FILE}}"
{body_json}
{eof_delim}

echo "Deploying Cloud Scheduler job: ${{JOB_NAME}} in ${{LOCATION}}..."
gcloud scheduler jobs create http "${{JOB_NAME}}" \\
  --project="${{PROJECT}}" \\
  --location="${{LOCATION}}" \\
  --schedule="${{SCHEDULE}}" \\
  --time-zone="${{TIME_ZONE}}" \\
  --uri="https://dataflow.googleapis.com/v1b3/projects/${{PROJECT}}/locations/${{LOCATION}}/flexTemplates:launch" \\
  --http-method=POST \\
{sa_flag}  --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform" \\
  --message-body-from-file="${{PAYLOAD_FILE}}"

echo "Cloud Scheduler job successfully created."
'''
  return script


def run_export_schedule(
    source: str,
    *,
    schedule: str = '@daily',
    orchestrator: str = 'composer',
    to_destination: str | None = None,
    project: str | None = None,
    location: str | None = None,
    template_gcs_path: str | None = None,
    job_name: str | None = None,
    dag_id: str | None = None,
    service_account_email: str | None = None,
    subnetwork: str | None = None,
    network: str | None = None,
    temp_location: str | None = None,
    staging_location: str | None = None,
    no_use_public_ips: bool | None = None,
    worker_machine_type: str | None = None,
    max_num_workers: int | None = None,
    num_workers: int | None = None,
    enable_streaming_engine: bool | None = None,
    parameters: list[str] | None = None,
    catchup: bool = False,
    max_active_runs: int = 1,
    retries: int = 2,
    retry_delay_min: int = 5,
) -> ScheduleExportReport:
  """Main entry point for `beam export-schedule`."""
  from . import uris

  orch = str(orchestrator).strip().lower()
  if orch not in VALID_ORCHESTRATORS:
    raise SchedulerError(
        f'Unsupported orchestrator {orchestrator!r}; must be one of {sorted(VALID_ORCHESTRATORS)}'
    )

  valid_sched = validate_cron_expression(schedule)

  # Extract options from pipeline YAML if present
  pipeline_opts = _load_pipeline_options_from_source(source)
  _validate_options_no_secrets(pipeline_opts)
  eff_project = str(
      project or pipeline_opts.get('project') or
      os.environ.get('GOOGLE_CLOUD_PROJECT') or 'my-gcp-project').strip()
  eff_location = str(
      location or pipeline_opts.get('region') or
      os.environ.get('CLOUDSDK_COMPUTE_REGION') or 'us-central1').strip()
  eff_sa = service_account_email or pipeline_opts.get('service_account_email')
  eff_subnetwork = subnetwork or pipeline_opts.get('subnetwork')
  eff_network = network or pipeline_opts.get('network')
  eff_temp = temp_location or pipeline_opts.get('temp_location')
  eff_staging = staging_location or pipeline_opts.get('staging_location')
  eff_machine = (
      worker_machine_type or pipeline_opts.get('worker_machine_type') or
      pipeline_opts.get('machine_type'))
  eff_max_workers = (
      max_num_workers if max_num_workers is not None else (
          pipeline_opts.get('max_num_workers')
          if pipeline_opts.get('max_num_workers') is not None else
          pipeline_opts.get('max_workers')))
  eff_num_workers = (
      num_workers
      if num_workers is not None else pipeline_opts.get('num_workers'))
  if no_use_public_ips is not None:
    eff_no_public = bool(no_use_public_ips)
  else:
    eff_no_public = _is_truthy_opt(pipeline_opts.get('no_use_public_ips')) or (
        pipeline_opts.get('use_public_ips') is False or
        str(pipeline_opts.get('use_public_ips', '')).strip().lower() == 'false')
  if enable_streaming_engine is not None:
    eff_streaming_engine = bool(enable_streaming_engine)
  else:
    eff_streaming_engine = _is_truthy_opt(
        pipeline_opts.get('enable_streaming_engine'))

  if not re.fullmatch(r'[a-z0-9.:-]+',
                      eff_project) or eff_project.startswith('-'):
    raise SchedulerError(f'Invalid GCP project identifier {eff_project!r}')
  if not re.fullmatch(r'[a-z0-9-]+',
                      eff_location) or eff_location.startswith('-'):
    raise SchedulerError(f'Invalid GCP region/location {eff_location!r}')

  raw_base = (
      os.path.splitext(os.path.basename(source))[0]
      if source and source != '-' else 'beam_pipeline')
  clean_base = _sanitize_identifier(raw_base)
  slug_base = re.sub(r'[^a-zA-Z0-9._-]+', '-',
                     clean_base).strip('-') or 'pipeline'

  # Resolve template GCS path (guaranteed free of spaces)
  eff_template = template_gcs_path
  if not eff_template:
    if source.startswith('gs://') and source.endswith(
        '.json') and ' ' not in source:
      eff_template = source
    else:
      eff_template = f'gs://{eff_project}-templates/{slug_base}.json'

  eff_dag_id = dag_id or f'{clean_base}_dag'
  eff_job_name = job_name or f'{clean_base.replace("_", "-")}-job'
  if not re.fullmatch(r'[a-zA-Z][a-zA-Z0-9_-]{0,127}', eff_job_name):
    raise SchedulerError(
        f'Invalid job name {eff_job_name!r}; must start with a letter and '
        'contain only letters, digits, hyphens, or underscores')

  # Environment block for Flex Template launch
  env_block: dict[str, Any] = {}
  if eff_sa:
    env_block['serviceAccountEmail'] = str(eff_sa)
  if eff_subnetwork:
    env_block['subnetwork'] = str(eff_subnetwork)
  if eff_network:
    env_block['network'] = str(eff_network)
  if eff_temp:
    env_block['tempLocation'] = str(eff_temp)
  if eff_staging:
    env_block['stagingLocation'] = str(eff_staging)
  if eff_no_public:
    env_block['ipConfiguration'] = 'WORKER_IP_PRIVATE'
  if eff_machine:
    env_block['machineType'] = str(eff_machine)
  if eff_max_workers is not None:
    try:
      env_block['maxWorkers'] = int(eff_max_workers)
    except (TypeError, ValueError):
      raise SchedulerError(
          f'Invalid max_num_workers {eff_max_workers!r}; expected integer'
      ) from None
  if eff_num_workers is not None:
    try:
      env_block['numWorkers'] = int(eff_num_workers)
    except (TypeError, ValueError):
      raise SchedulerError(
          f'Invalid num_workers {eff_num_workers!r}; expected integer'
      ) from None
  if eff_streaming_engine:
    env_block['enableStreamingEngine'] = True

  # Parse parameters (rejecting hardcoded secret keys and inline secret values)
  param_dict: dict[str, str] = {}
  if parameters:
    for p in parameters:
      if '=' in p:
        k, v = p.split('=', 1)
        ks = k.strip()
        vs = v.strip()
      else:
        ks = p.strip()
        vs = ''
      if uris._SECRET_KEY_RE.search(ks):
        raise SchedulerError(
            f'Refusing to bake secret parameter {ks!r} into exported schedule; '
            'pass a Secret Manager resource name instead')
      if vs:
        _validate_param_value_no_secrets(ks, vs)
      param_dict[ks] = vs

  if orch == 'composer':
    job_name_pattern = f'{eff_job_name}-{{{{ ds_nodash }}}}'
    content = generate_composer_dag(
        dag_id=eff_dag_id,
        schedule=valid_sched,
        project=eff_project,
        location=eff_location,
        template_gcs_path=eff_template,
        job_name_pattern=job_name_pattern,
        parameters=param_dict,
        environment=env_block,
        retries=retries,
        retry_delay_min=retry_delay_min,
        catchup=catchup,
        max_active_runs=max_active_runs,
    )
  else:
    content = generate_cloud_scheduler_script(
        job_name=eff_job_name,
        schedule=valid_sched,
        project=eff_project,
        location=eff_location,
        template_gcs_path=eff_template,
        service_account_email=str(eff_sa) if eff_sa else None,
        parameters=param_dict,
        environment=env_block,
    )

  if to_destination:
    dest_path = os.path.abspath(to_destination)
    if os.path.islink(dest_path):
      raise SchedulerError(
          f'Refusing to write schedule through symbolic link {dest_path!r}')
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    with open(dest_path, 'w', encoding='utf-8') as f:
      f.write(content)

  return ScheduleExportReport(
      orchestrator=orch,
      schedule=valid_sched,
      destination=to_destination,
      content=content,
      dag_id=eff_dag_id if orch == 'composer' else None,
      job_name=eff_job_name,
  )
