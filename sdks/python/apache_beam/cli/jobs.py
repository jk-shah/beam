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

"""Async job lifecycle, `--wait-timeout`, idempotency ledger, `wait` & `cancel`.

Security & reliability invariants:
1. Strict `SAFE_JOB_ID_RE` (`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`) + resolved
   directory containment check prevents `CWE-22` path traversal when reading or
   writing `~/.beam/jobs/<job_id>.json`.
2. Local/in-process runners (`direct`, `fnapi`, `prism`, `bundlebaseddirect`)
   NEVER detach on `--wait-timeout`: they cancel the in-process job and fail
   closed with `EXIT_TIMEOUT = 7`.
3. Remote runners (`DataflowRunner`, etc.) persist `0600` job state + post-run
   verification specs (`--assert-counts`, `--verify`) in `~/.beam/jobs/` so
   `beam wait <job_id>` can poll metrics via the runner REST client and run
   deferred checks upon completion, while a 30-minute `(argv_hash, plan_hash)`
   idempotency ledger prevents duplicate job submissions on agent retries.
"""

import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import time
from typing import Any

EXIT_OK = 0
EXIT_PIPELINE_FAILED = 1
EXIT_USAGE = 2
EXIT_TIMEOUT = 7

SAFE_JOB_ID_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$')
_DURATION_RE = re.compile(
    r'^\s*(?P<num>\d+(?:\.\d+)?)\s*(?P<unit>ms|s|sec|secs|m|min|mins|h|hr|hrs)?\s*$',
    re.IGNORECASE)

_TERMINAL_SUCCESS_STATES = frozenset(['DONE', 'JOB_STATE_DONE', 'DRAINED', 'JOB_STATE_DRAINED'])
_TERMINAL_FAILURE_STATES = frozenset([
    'FAILED',
    'JOB_STATE_FAILED',
    'CANCELLED',
    'CANCELED',
    'JOB_STATE_CANCELLED',
    'UPDATED',
    'JOB_STATE_UPDATED',
])


class JobError(ValueError):
  """Raised on invalid job IDs, missing job records, or runner API errors."""
  def __init__(self, message: str, exit_code: int = EXIT_USAGE):
    super().__init__(message)
    self.exit_code = exit_code


def parse_duration_seconds(raw: str | int | float) -> float:
  """Parses durations like `'30s'`, `'5m'`, `'2h'`, `'500ms'`, or `'120'`."""
  if isinstance(raw, (int, float)):
    val = float(raw)
    if val <= 0:
      raise ValueError('duration must be positive')
    return val
  m = _DURATION_RE.match(str(raw))
  if not m:
    raise ValueError(
        f'invalid duration {raw!r}; examples: 30s, 5m, 1h, 500ms, 120')
  num = float(m.group('num'))
  unit = (m.group('unit') or 's').lower()
  mult = {
      'ms': 0.001,
      's': 1.0,
      'sec': 1.0,
      'secs': 1.0,
      'm': 60.0,
      'min': 60.0,
      'mins': 60.0,
      'h': 3600.0,
      'hr': 3600.0,
      'hrs': 3600.0,
  }[unit]
  seconds = num * mult
  if seconds <= 0:
    raise ValueError('duration must be > 0')
  return seconds


def _jobs_dir() -> Path:
  beam_home = os.path.expanduser(os.environ.get('BEAM_HOME', '~/.beam'))
  d = Path(beam_home) / 'jobs'
  d.mkdir(parents=True, exist_ok=True)
  try:
    os.chmod(d, 0o700)
  except OSError:
    pass
  return d


def validate_job_id(job_id: str) -> str:
  """Validates `job_id` against `SAFE_JOB_ID_RE` and blocks path traversal."""
  cleaned = (job_id or '').strip()
  if not SAFE_JOB_ID_RE.match(cleaned) or '..' in cleaned:
    raise JobError(
        f'invalid job_id {job_id!r}; expected alphanumeric/dash/underscore/dot '
        'identifier (max 128 chars)',
        EXIT_USAGE)
  base = _jobs_dir().resolve()
  candidate = (base / f'{cleaned}.json').resolve()
  try:
    candidate.relative_to(base)
  except ValueError:
    raise JobError(f'unsafe job_id path {job_id!r}', EXIT_USAGE) from None
  return cleaned


def _atomic_write_json(target: Path, payload: dict[str, Any]) -> None:
  target.parent.mkdir(parents=True, exist_ok=True)
  fd, tmp_path = tempfile.mkstemp(
      prefix='.tmp-job-', suffix='.json', dir=str(target.parent))
  try:
    try:
      os.fchmod(fd, 0o600)
    except (AttributeError, OSError):
      pass
    with os.fdopen(fd, 'w', encoding='utf-8') as f:
      json.dump(payload, f, indent=2, sort_keys=True)
      f.write('\n')
    os.replace(tmp_path, target)
  finally:
    if os.path.exists(tmp_path):
      try:
        os.unlink(tmp_path)
      except OSError:
        pass


def compute_idempotency_key(argv: list[str], plan_hash: str) -> str:
  canonical = json.dumps({'argv': list(argv), 'plan_hash': plan_hash}, sort_keys=True)
  return 'idem:' + hashlib.sha256(canonical.encode('utf-8')).hexdigest()[:32]


def lookup_recent_submission(
    idem_key: str, ttl_seconds: int = 1800) -> dict[str, Any] | None:
  """Returns an active/recent remote job record if submitted within `ttl_seconds`."""
  ledger_path = _jobs_dir() / '_idempotency_ledger.json'
  if not ledger_path.is_file():
    return None
  try:
    ledger = json.loads(ledger_path.read_text(encoding='utf-8'))
  except (OSError, ValueError):
    return None
  entry = ledger.get(idem_key)
  if not isinstance(entry, dict):
    return None
  submitted_at = float(entry.get('submitted_at', 0.0))
  if (time.time() - submitted_at) > ttl_seconds:
    return None
  job_id = entry.get('job_id')
  if not isinstance(job_id, str):
    return None
  try:
    rec = load_job_record(job_id)
  except JobError:
    return None
  state = str(rec.get('state') or '').upper()
  if state in _TERMINAL_FAILURE_STATES:
    return None
  return rec


def record_job_submission(
    job_id: str,
    *,
    runner: str,
    state: str,
    command: str,
    project: str | None = None,
    region: str | None = None,
    idem_key: str | None = None,
    post_checks: dict[str, Any] | None = None,
    outputs: dict[str, Any] | None = None,
) -> dict[str, Any]:
  """Persists `~/.beam/jobs/<job_id>.json` (`0600`) and updates the ledger."""
  safe_id = validate_job_id(job_id)
  now = time.time()
  record = {
      'job_id': safe_id,
      'runner': runner,
      'state': state,
      'command': command,
      'project': project,
      'region': region,
      'submitted_at': now,
      'updated_at': now,
      'post_checks': post_checks or {},
      'outputs': outputs or {},
      'metrics': {},
  }
  job_path = _jobs_dir() / f'{safe_id}.json'
  _atomic_write_json(job_path, record)

  if idem_key:
    ledger_path = _jobs_dir() / '_idempotency_ledger.json'
    ledger: dict[str, Any] = {}
    if ledger_path.is_file():
      try:
        ledger = json.loads(ledger_path.read_text(encoding='utf-8'))
      except (OSError, ValueError):
        ledger = {}
    # Prune entries older than 2 hours.
    cutoff = now - 7200
    ledger = {
        k: v
        for k, v in ledger.items()
        if isinstance(v, dict) and float(v.get('submitted_at', 0)) >= cutoff
    }
    ledger[idem_key] = {'job_id': safe_id, 'submitted_at': now}
    _atomic_write_json(ledger_path, ledger)
  return record


def load_job_record(job_id: str) -> dict[str, Any]:
  safe_id = validate_job_id(job_id)
  job_path = _jobs_dir() / f'{safe_id}.json'
  if not job_path.is_file():
    raise JobError(
        f'no local job record for {safe_id!r} in {_jobs_dir()}; pass '
        '--project and --region if querying a remote Dataflow job directly',
        EXIT_USAGE)
  try:
    return json.loads(job_path.read_text(encoding='utf-8'))
  except (OSError, ValueError) as e:
    raise JobError(f'corrupt job record for {safe_id!r}: {e}', EXIT_USAGE) from None


def extract_beam_cli_counters(metrics_result: Any) -> dict[str, int]:
  """Extracts `Metrics.counter('beam_cli', ...)` from a Beam `MetricResults`."""
  if metrics_result is None:
    return {}
  try:
    from apache_beam.metrics.metric import MetricsFilter
    queried = metrics_result.query(MetricsFilter().with_namespace('beam_cli'))
  except Exception:  # pylint: disable=broad-except
    return {}

  counters: dict[str, int] = {}
  for c in queried.get('counters', ()):
    name = getattr(getattr(c, 'key', None), 'metric', None)
    metric_name = getattr(name, 'name', None)
    committed = getattr(c, 'committed', None)
    attempted = getattr(c, 'attempted', 0)
    val = committed if committed is not None else attempted
    if metric_name:
      counters[str(metric_name)] = counters.get(str(metric_name), 0) + int(val or 0)

  return finalize_cli_row_metrics(counters)


def finalize_cli_row_metrics(raw_counters: dict[str, int]) -> dict[str, int]:
  """Computes `rows_written` and `rows_filtered_out` accurately."""
  if not raw_counters:
    return {}
  out: dict[str, int] = {}
  if 'rows_read' in raw_counters:
    out['rows_read'] = int(raw_counters['rows_read'])
  dlq_rows = int(raw_counters.get('dlq_rows', 0))
  sink_dlq_rows = int(raw_counters.get('sink_dlq_rows', 0))
  filter_dlq_rows = int(raw_counters.get('filter_dlq_rows', 0))
  if 'dlq_rows' in raw_counters or dlq_rows > 0:
    out['dlq_rows'] = dlq_rows
  if 'pre_filter_rows' in raw_counters or 'post_filter_rows' in raw_counters:
    pre_f = int(raw_counters.get('pre_filter_rows', 0))
    post_f = int(raw_counters.get('post_filter_rows', 0))
    out['rows_filtered_out'] = max(0, pre_f - post_f - filter_dlq_rows)
  if 'pre_sink_rows' in raw_counters or 'rows_read' in raw_counters:
    pre_sink = int(raw_counters.get('pre_sink_rows', 0))
    out['rows_written'] = max(0, pre_sink - sink_dlq_rows)
  return out


def check_assert_counts(counters: dict[str, int]) -> str | None:
  """Returns `None` if row conservation holds, or an actionable error message."""
  if 'rows_read' not in counters or 'rows_written' not in counters:
    return (
        '--assert-counts could not read row counters from the runner '
        f'(received metrics: {counters})')
  r_read = int(counters['rows_read'])
  r_written = int(counters['rows_written'])
  r_filtered = int(counters.get('rows_filtered_out', 0))
  r_dlq = int(counters.get('dlq_rows', 0))
  accounted = r_written + r_filtered + r_dlq
  if r_read != accounted:
    return (
        f'--assert-counts failed: rows_read ({r_read}) != '
        f'rows_written ({r_written}) + rows_filtered_out ({r_filtered}) + '
        f'dlq_rows ({r_dlq})')
  return None


def _query_dataflow_job(
    job_id: str, project: str, region: str) -> tuple[str, dict[str, int]]:
  """Queries Dataflow REST API for current job state and `beam_cli` counters."""
  from apache_beam.options.pipeline_options import PipelineOptions
  from apache_beam.runners.dataflow.internal import apiclient

  opts = PipelineOptions([
      f'--project={project}',
      f'--region={region}',
  ])
  client = apiclient.DataflowApplicationClient(opts)
  job_proto = client.get_job(job_id)
  raw_state = getattr(job_proto, 'current_state', None)
  if raw_state is None:
    raw_state = getattr(job_proto, 'currentState', 'JOB_STATE_UNKNOWN')
  state_str = str(getattr(raw_state, 'name', None) or raw_state)

  raw_counters: dict[str, int] = {}
  try:
    metrics_proto = client.get_job_metrics(job_id)
    for m in getattr(metrics_proto, 'metrics', ()) or ():
      m_name = getattr(m, 'name', None)
      if m_name is None:
        continue
      origin = str(getattr(m_name, 'origin', '') or '')
      name_str = str(getattr(m_name, 'name', '') or '')
      ctx_obj = getattr(m_name, 'context', None)
      if hasattr(ctx_obj, 'items'):
        context_entries = {str(k): str(v) for k, v in ctx_obj.items()}
      elif ctx_obj and hasattr(ctx_obj, 'additionalProperties'):
        context_entries = {
            str(kv.key): str(kv.value) for kv in ctx_obj.additionalProperties
        }
      else:
        context_entries = {}
      if 'user' in origin.lower() and context_entries.get(
          'namespace') == 'beam_cli' and not name_str.endswith(
              ('_MIN', '_MAX', '_MEAN', '_COUNT')):
        scalar = getattr(m, 'scalar', None)
        if isinstance(scalar, (int, float)):
          val: int | float | None = scalar
        elif scalar is not None:
          val = getattr(scalar, 'integer_value', None)
          if val is None:
            val = getattr(scalar, 'double_value', None)
        else:
          val = None
        if val is not None and context_entries.get('tentative') != 'true':
          raw_counters[name_str] = raw_counters.get(name_str, 0) + int(val)
  except Exception:  # pylint: disable=broad-except
    pass

  return state_str, finalize_cli_row_metrics(raw_counters)


def wait_for_job(
    job_id: str,
    *,
    timeout_seconds: float | None = None,
    poll_interval: float = 3.0,
    project: str | None = None,
    region: str | None = None,
    query_fn=None,
) -> tuple[int, dict[str, Any]]:
  """Polls a remote job until terminal state or `timeout_seconds` (`EXIT_TIMEOUT=7`)."""
  safe_id = validate_job_id(job_id)
  record: dict[str, Any]
  try:
    record = load_job_record(safe_id)
  except JobError:
    if not (project and region):
      raise
    record = {
        'job_id': safe_id,
        'runner': 'DataflowRunner',
        'state': 'JOB_STATE_UNKNOWN',
        'command': 'external',
        'project': project,
        'region': region,
        'post_checks': {},
        'outputs': {},
        'metrics': {},
    }

  proj = project or record.get('project')
  reg = region or record.get('region') or 'us-central1'
  if not proj:
    raise JobError(
        f'cannot poll job {safe_id!r} without a GCP project; pass --project',
        EXIT_USAGE)

  fetch = query_fn or _query_dataflow_job
  deadline = (time.monotonic() + timeout_seconds) if timeout_seconds else None

  while True:
    state_str, counters = fetch(safe_id, proj, reg)
    record['state'] = state_str
    record['updated_at'] = time.time()
    if counters:
      record['metrics'] = counters
    _atomic_write_json(_jobs_dir() / f'{safe_id}.json', record)

    upper = state_str.upper()
    if upper in _TERMINAL_SUCCESS_STATES:
      outputs_obj = dict(record.get('outputs') or {})
      out_pfx = outputs_obj.get('output_prefix')
      dlq_pfx = outputs_obj.get('dlq_prefix')
      if out_pfx or dlq_pfx:
        try:
          from apache_beam.io.filesystems import FileSystems
          from apache_beam.options.pipeline_options import PipelineOptions
          FileSystems.set_options(PipelineOptions([f'--project={proj}']))
          if out_pfx:
            written = sorted(
                m.path
                for m in FileSystems.match([out_pfx + '*'])[0].metadata_list)
            outputs_obj['files'] = written
            outputs_obj['file_count'] = len(written)
          if dlq_pfx:
            dlq_files = sorted(
                m.path
                for m in FileSystems.match([dlq_pfx + '*'])[0].metadata_list)
            outputs_obj['dlq_files'] = dlq_files
          record['outputs'] = outputs_obj
        except Exception:  # pylint: disable=broad-except
          pass

      # Run deferred post-checks (`assert_counts` / `verify_diff`) if persisted!
      post = record.get('post_checks') or {}
      if post.get('assert_counts'):
        err_msg = check_assert_counts(counters)
        if err_msg:
          _atomic_write_json(_jobs_dir() / f'{safe_id}.json', record)
          return EXIT_PIPELINE_FAILED, {
              **record,
              'post_check_error': err_msg,
          }
      if post.get('verify_diff'):
        from . import diff
        v_cfg = post['verify_diff']
        v_right = (out_pfx + '*' if out_pfx else v_cfg['right'])
        diff_code, diff_report = diff.run_diff(
            v_cfg['left'],
            v_right,
            keys=v_cfg.get('keys'),
            default_project=proj)
        diff_report.pop('untrusted_data', None)
        outputs_obj['verify'] = diff_report
        record['outputs'] = outputs_obj
        record['verify_diff'] = diff_report
        _atomic_write_json(_jobs_dir() / f'{safe_id}.json', record)
        if diff_code != 0:
          return diff_code, record
      _atomic_write_json(_jobs_dir() / f'{safe_id}.json', record)
      return EXIT_OK, record

    if upper in _TERMINAL_FAILURE_STATES:
      return EXIT_PIPELINE_FAILED, record

    if deadline is not None and time.monotonic() >= deadline:
      return EXIT_TIMEOUT, {**record, 'detached': True}

    sleep_s = poll_interval
    if deadline is not None:
      sleep_s = max(0.05, min(poll_interval, deadline - time.monotonic()))
    time.sleep(sleep_s)


def cancel_job(
    job_id: str,
    *,
    drain: bool = False,
    project: str | None = None,
    region: str | None = None,
    cancel_fn=None,
) -> dict[str, Any]:
  """Cancels (or `--drain`s) a running remote job."""
  safe_id = validate_job_id(job_id)
  record: dict[str, Any] = {}
  try:
    record = load_job_record(safe_id)
  except JobError:
    if not (project and region):
      raise

  proj = project or record.get('project')
  reg = region or record.get('region') or 'us-central1'
  if not proj:
    raise JobError(
        f'cannot cancel job {safe_id!r} without a GCP project; pass --project',
        EXIT_USAGE)

  target_state = 'JOB_STATE_DRAINING' if drain else 'JOB_STATE_CANCELLED'
  if cancel_fn is not None:
    ok = bool(cancel_fn(safe_id, proj, reg, target_state))
  else:
    from apache_beam.options.pipeline_options import PipelineOptions
    from apache_beam.runners.dataflow.internal import apiclient
    opts = PipelineOptions([f'--project={proj}', f'--region={reg}'])
    client = apiclient.DataflowApplicationClient(opts)
    ok = bool(client.modify_job_state(safe_id, target_state))

  if not ok:
    raise JobError(
        f'runner rejected state transition to {target_state} for job {safe_id!r}',
        EXIT_PIPELINE_FAILED)

  if record:
    record['state'] = target_state
    record['updated_at'] = time.time()
    _atomic_write_json(_jobs_dir() / f'{safe_id}.json', record)

  return {
      'job_id': safe_id,
      'project': proj,
      'region': reg,
      'requested_state': target_state,
      'mode': 'drain' if drain else 'cancel',
  }
