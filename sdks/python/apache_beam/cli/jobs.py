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

import contextlib
import hashlib
import json
import os
import re
import tempfile
import time
from pathlib import Path
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
  beam_home = Path(os.path.expanduser(os.environ.get('BEAM_HOME', '~/.beam')))
  if os.path.islink(beam_home):
    raise JobError(
        f'refusing symlinked BEAM_HOME directory {str(beam_home)!r}',
        EXIT_USAGE)
  d = beam_home / 'jobs'
  if os.path.islink(d):
    raise JobError(
        f'refusing symlinked jobs directory {str(d)!r}', EXIT_USAGE)
  beam_home.mkdir(mode=0o700, parents=True, exist_ok=True)
  d.mkdir(mode=0o700, parents=True, exist_ok=True)
  if os.path.islink(beam_home) or os.path.islink(d):
    raise JobError(
        f'refusing symlinked jobs directory {str(d)!r}', EXIT_USAGE)
  try:
    os.chmod(beam_home, 0o700)
    os.chmod(d, 0o700)
  except OSError:
    pass
  return d


_INPROC_LOCKS_GUARD = None
_INPROC_LOCKS: dict[str, Any] = {}
_INPROC_LOCK_DEPTH: dict[str, int] = {}


def _get_inproc_lock(key: str):
  import threading
  global _INPROC_LOCKS_GUARD
  if _INPROC_LOCKS_GUARD is None:
    _INPROC_LOCKS_GUARD = threading.Lock()
  with _INPROC_LOCKS_GUARD:
    lk = _INPROC_LOCKS.get(key)
    if lk is None:
      lk = threading.RLock()
      _INPROC_LOCKS[key] = lk
    return lk


@contextlib.contextmanager
def _file_lock(lock_path: Path):
  abs_key = os.path.abspath(str(lock_path))
  inproc_lk = _get_inproc_lock(abs_key)
  with inproc_lk:
    depth = _INPROC_LOCK_DEPTH.get(abs_key, 0)
    if depth > 0:
      _INPROC_LOCK_DEPTH[abs_key] = depth + 1
      try:
        yield
      finally:
        _INPROC_LOCK_DEPTH[abs_key] = depth
      return

    lock_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
    try:
      if os.name == 'nt':
        import msvcrt
        os.lseek(fd, 0, os.SEEK_SET)
        try:
          if os.fstat(fd).st_size == 0:
            os.write(fd, b'\0')
            os.lseek(fd, 0, os.SEEK_SET)
        except OSError:
          pass
        while True:
          try:
            msvcrt.locking(fd, msvcrt.LK_LOCK, 1)
            break
          except OSError:
            time.sleep(0.05)
      else:
        import fcntl
        fcntl.flock(fd, fcntl.LOCK_EX)
      _INPROC_LOCK_DEPTH[abs_key] = 1
      yield
    finally:
      _INPROC_LOCK_DEPTH.pop(abs_key, None)
      try:
        if os.name == 'nt':
          import msvcrt
          os.lseek(fd, 0, os.SEEK_SET)
          msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        else:
          import fcntl
          fcntl.flock(fd, fcntl.LOCK_UN)
      except OSError:
        pass
      os.close(fd)


def atomic_commit_local_file(
    tmp_path: str,
    final_path: str,
    *,
    allow_overwrite: bool = True,
) -> None:
  """Commits `tmp_path` onto `final_path`; refuses TOCTOU overwrite when `allow_overwrite=False`."""
  parent = os.path.dirname(os.path.abspath(final_path)) or '.'
  os.makedirs(parent, exist_ok=True)
  if allow_overwrite:
    os.replace(tmp_path, final_path)
    return
  try:
    os.link(tmp_path, final_path)
  except FileExistsError:
    raise
  except OSError:
    # Fallback for filesystems that do not support hardlinks (e.g. FAT/exFAT).
    lock_p = Path(f'{os.path.abspath(final_path)}.commit.lock')
    with _file_lock(lock_p):
      if os.path.exists(final_path):
        raise FileExistsError(final_path) from None
      os.replace(tmp_path, final_path)
      return
  try:
    os.unlink(tmp_path)
  except OSError:
    pass


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
  target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
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
      f.flush()
      try:
        os.fsync(f.fileno())
      except OSError:
        pass
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
  from . import output as out_mod
  jobs_d = _jobs_dir()
  ledger_path = jobs_d / '_idempotency_ledger.json'
  if not ledger_path.is_file() or os.path.islink(ledger_path):
    return None
  try:
    with _file_lock(jobs_d / '.ledger.lock'):
      ledger = json.loads(
          out_mod.read_bounded_text(ledger_path, label=str(ledger_path)))
  except (OSError, ValueError):
    return None
  if not isinstance(ledger, dict):
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
  val_state = str(rec.get('validation_state') or '').upper()
  if (val_state == 'FAILED' or rec.get('post_check_error') or
      rec.get('validation_exit_code') not in (None, 0)):
    return None
  if (state in _TERMINAL_SUCCESS_STATES and rec.get('post_checks') and
      val_state != 'PASSED'):
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
  from . import output as out_mod
  safe_id = validate_job_id(job_id)
  now = time.time()
  has_post = bool(post_checks)
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
      'validation_state': 'PENDING' if has_post else 'NONE',
      'outputs': outputs or {},
      'metrics': {},
  }
  jobs_d = _jobs_dir()
  job_path = jobs_d / f'{safe_id}.json'
  with _file_lock(jobs_d / f'.{safe_id}.job.lock'):
    _atomic_write_json(job_path, record)

  if idem_key:
    ledger_path = jobs_d / '_idempotency_ledger.json'
    with _file_lock(jobs_d / '.ledger.lock'):
      ledger: dict[str, Any] = {}
      if ledger_path.is_file() and not os.path.islink(ledger_path):
        try:
          ledger = json.loads(
              out_mod.read_bounded_text(ledger_path, label=str(ledger_path)))
        except (OSError, ValueError):
          ledger = {}
      if not isinstance(ledger, dict):
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


def mark_job_validation(
    job_id: str,
    *,
    passed: bool,
    exit_code: int = 0,
    error: str | None = None,
    state: str | None = None,
    metrics: dict[str, Any] | None = None,
    outputs: dict[str, Any] | None = None,
    verify_diff: dict[str, Any] | None = None,
) -> None:
  """Updates persisted job record with post-job validation outcome."""
  try:
    safe_id = validate_job_id(job_id)
    jobs_d = _jobs_dir()
    with _file_lock(jobs_d / f'.{safe_id}.job.lock'):
      rec = load_job_record(safe_id)
      if state is not None:
        rec['state'] = state
      rec['updated_at'] = time.time()
      has_post = bool(rec.get('post_checks'))
      rec['validation_state'] = (('PASSED' if has_post else 'NONE')
                                 if passed else 'FAILED')
      rec['validation_exit_code'] = int(exit_code)
      if error:
        rec['post_check_error'] = error
      else:
        rec.pop('post_check_error', None)
      if metrics is not None:
        rec['metrics'] = dict(metrics)
      if outputs is not None:
        rec['outputs'] = {**dict(rec.get('outputs') or {}), **dict(outputs)}
      if verify_diff is not None:
        rec['verify_diff'] = verify_diff
      _atomic_write_json(jobs_d / f'{safe_id}.json', rec)
  except JobError:
    return


def load_job_record(job_id: str) -> dict[str, Any]:
  from . import output as out_mod
  safe_id = validate_job_id(job_id)
  job_path = _jobs_dir() / f'{safe_id}.json'
  if not job_path.is_file() or os.path.islink(job_path):
    raise JobError(
        f'no local job record for {safe_id!r} in {_jobs_dir()}; pass '
        '--project and --region if querying a remote Dataflow job directly',
        EXIT_USAGE)
  try:
    return json.loads(out_mod.read_bounded_text(job_path, label=str(job_path)))
  except (OSError, ValueError) as e:
    raise JobError(
        f'corrupt job record for {safe_id!r}: {e}', EXIT_USAGE) from None


def extract_beam_cli_counters(metrics_result: Any) -> dict[str, int]:
  """Extracts `Metrics.counter`/`distribution('beam_cli', ...)` from `MetricResults`."""
  if metrics_result is None:
    return {}
  if (hasattr(metrics_result, 'metrics') and
      callable(metrics_result.metrics) and
      not hasattr(metrics_result, 'query')):
    metrics_result = metrics_result.metrics()
  counters: dict[str, int] = {}
  dist_sums: dict[str, tuple[float, int]] = {}
  try:
    from apache_beam.metrics.metric import MetricsFilter
    queried = metrics_result.query(MetricsFilter().with_namespace('beam_cli'))
    if not isinstance(queried, dict):
      return {}
    for c in queried.get('counters', ()):
      name = getattr(getattr(c, 'key', None), 'metric', None)
      metric_name = getattr(name, 'name', None)
      committed = getattr(c, 'committed', None)
      attempted = getattr(c, 'attempted', 0)
      val = committed if committed is not None else attempted
      if metric_name:
        counters[str(metric_name)] = (
            counters.get(str(metric_name), 0) + int(val or 0))
    for d in queried.get('distributions', ()):
      name = getattr(getattr(d, 'key', None), 'metric', None)
      metric_name = getattr(name, 'name', None)
      committed = getattr(d, 'committed', None)
      attempted = getattr(d, 'attempted', None)
      dist = committed if committed is not None else attempted
      cnt = int(getattr(dist, 'count', 0) or 0) if dist is not None else 0
      total = float(
          getattr(dist, 'sum', 0.0) or 0.0) if dist is not None else 0.0
      if metric_name and cnt > 0:
        prev_sum, prev_cnt = dist_sums.get(str(metric_name), (0.0, 0))
        dist_sums[str(metric_name)] = (prev_sum + total, prev_cnt + cnt)
    for m_name, (total, cnt) in dist_sums.items():
      if cnt > 0:
        counters[m_name] = int(round(total / cnt))
  except Exception:  # pylint: disable=broad-except
    return {}

  return finalize_cli_row_metrics(counters)


_TIER1_EXPORTED_METRICS = (
    'duplicates_dropped',
    'skew_dropped',
    'enrich_hits',
    'enrich_misses',
    'enrich_misses_dropped',
    'enrich_errors',
    'inference_batches',
    'inference_rows',
    'inference_errors',
    'inference_latency_ms',
    'asof_matched',
    'asof_unmatched',
    'patterns_matched',
    'patterns_expired',
    'pattern_overflow_dropped',
    'late_rows_dropped',
    'late_panes_emitted',
)


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
  schema_drift_rows = int(raw_counters.get('schema_drift_rows', 0))
  if schema_drift_rows > 0:
    out['schema_drift_rows'] = schema_drift_rows
  cdc_skipped = int(raw_counters.get('cdc_preimage_skipped', 0))
  # Older versions and deletes collapsed by CliMaterializeCdc (--cdc into
  # plain files) are not written, so they count as filtered out.
  cdc_superseded = int(raw_counters.get('cdc_superseded_rows', 0))
  if cdc_superseded:
    out['cdc_superseded_rows'] = cdc_superseded
  exp_dropped = int(raw_counters.get('expectation_dropped_rows', 0))
  dupes_dropped = int(raw_counters.get('duplicates_dropped', 0))
  enrich_dropped = int(raw_counters.get('enrich_misses_dropped', 0))
  skew_dropped = int(raw_counters.get('skew_dropped', 0))
  late_dropped = int(raw_counters.get('late_rows_dropped', 0))
  limit_dropped = int(raw_counters.get('stage_dropped_rows', 0))
  stage_drops = (
      cdc_skipped + cdc_superseded + exp_dropped + dupes_dropped +
      enrich_dropped + skew_dropped + late_dropped + limit_dropped)
  if ('pre_filter_rows' in raw_counters or 'post_filter_rows' in raw_counters or
      stage_drops > 0):
    pre_f = int(raw_counters.get('pre_filter_rows', 0))
    post_f = int(raw_counters.get('post_filter_rows', 0))
    out['rows_filtered_out'] = (
        max(0, pre_f - post_f - filter_dlq_rows) + stage_drops)
  if 'pre_sink_rows' in raw_counters or 'rows_read' in raw_counters:
    pre_sink = int(raw_counters.get('pre_sink_rows', 0))
    out['rows_written'] = max(0, pre_sink - sink_dlq_rows)
  for k, v in raw_counters.items():
    if k.startswith('expectation_') or k.startswith('expectations_'):
      out[k] = int(v)
  for k in _TIER1_EXPORTED_METRICS:
    if k in raw_counters:
      out[k] = int(raw_counters[k])
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


def dataflow_console_url(
    job_id: str | None,
    project: str | None,
    region: str | None = 'us-central1',
) -> str | None:
  """Returns the Cloud Console URL for a Dataflow job when ID and project exist."""
  if not job_id or not project:
    return None
  reg = (region or 'us-central1').strip()
  return (
      f'https://console.cloud.google.com/dataflow/jobs/{reg}/{job_id}'
      f'?project={project}')


class _NoRedirectHandler(object):
  """Lazy factory wrapper around `urllib.request.HTTPRedirectHandler`."""

  @staticmethod
  def build_opener():
    import urllib.request

    class _BlockRedirect(urllib.request.HTTPRedirectHandler):
      def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(
            req.full_url, code, 'redirects disallowed for auth requests',
            headers, fp)

    return urllib.request.build_opener(_BlockRedirect())


def _extract_raw_counters_from_metrics_list(
    metrics_items: Any) -> dict[str, int]:
  """Extracts `beam_cli` counters from either REST dicts or apiclient protos."""
  raw_counters: dict[str, int] = {}
  for m in metrics_items or ():
    if isinstance(m, dict):
      m_name = m.get('name') or {}
      origin = str(m_name.get('origin') or '')
      name_str = str(m_name.get('name') or '')
      ctx_raw = m_name.get('context') or {}
      context_entries = (
          {str(k): str(v) for k, v in ctx_raw.items()}
          if isinstance(ctx_raw, dict) else {})
      scalar = m.get('scalar')
      if isinstance(scalar, dict):
        val = scalar.get(
            'integerValue',
            scalar.get(
                'integer_value',
                scalar.get('doubleValue', scalar.get('double_value'))))
      else:
        val = scalar
    else:
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
      scalar = getattr(m, 'scalar', None)
      if isinstance(scalar, (int, float)):
        val = scalar
      elif scalar is not None:
        val = getattr(scalar, 'integer_value', None)
        if val is None:
          val = getattr(scalar, 'double_value', None)
      else:
        val = None

    if 'user' in origin.lower() and context_entries.get(
        'namespace') == 'beam_cli' and not name_str.endswith(
            ('_MIN', '_MAX', '_MEAN', '_COUNT')):
      if val is not None and context_entries.get('tentative') != 'true':
        try:
          raw_counters[name_str] = raw_counters.get(name_str, 0) + int(
              float(val))
        except (TypeError, ValueError):
          pass
  return raw_counters


class _DataflowPoller:
  """Caches credentials/client across polls so `beam wait` never forks per poll."""
  def __init__(self, project: str, region: str):
    self._project = project
    self._region = region
    self._creds = None
    self._auth_req = None
    self._opener = None
    self._apiclient = None

  def _ensure_rest_creds(self) -> str | None:
    try:
      if self._creds is None:
        import google.auth
        creds, _ = google.auth.default(
            scopes=['https://www.googleapis.com/auth/cloud-platform'])
        self._creds = creds
      if not self._creds.valid or getattr(self._creds, 'expired', False):
        if self._auth_req is None:
          try:
            import google.auth.transport.requests as ga_requests
            self._auth_req = ga_requests.Request()
          except ImportError:
            import google.auth.transport._http_client as ga_http
            self._auth_req = ga_http.Request()
        self._creds.refresh(self._auth_req)
      token = getattr(self._creds, 'token', None)
      if isinstance(token, str) and token:
        if self._opener is None:
          self._opener = _NoRedirectHandler.build_opener()
        return token
    except Exception:  # pylint: disable=broad-except
      return None
    return None

  def _rest_json(
      self,
      url: str,
      token: str,
      *,
      method: str = 'GET',
      body: dict[str, Any] | None = None,
  ) -> dict[str, Any]:
    import urllib.parse
    import urllib.request
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != 'https' or parsed.hostname != 'dataflow.googleapis.com':
      raise JobError(f'refusing Dataflow API request to {url!r}', EXIT_USAGE)
    payload = (
        json.dumps(body).encode('utf-8') if body is not None else None)
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json',
    }
    if payload is not None:
      headers['Content-Type'] = 'application/json'
    req = urllib.request.Request(url, data=payload, headers=headers, method=method)
    with self._opener.open(req, timeout=20) as resp:
      raw = resp.read(16 * 1024 * 1024)
    return json.loads(raw.decode('utf-8')) if raw else {}

  def query(self, job_id: str, project: str, region: str) -> tuple[str, dict[str, int]]:
    import urllib.parse
    token = self._ensure_rest_creds()
    if token is not None:
      q_proj = urllib.parse.quote(project, safe='')
      q_reg = urllib.parse.quote(region, safe='')
      q_job = urllib.parse.quote(job_id, safe='')
      base = (
          f'https://dataflow.googleapis.com/v1b3/projects/{q_proj}'
          f'/locations/{q_reg}/jobs/{q_job}')
      job_doc = self._rest_json(base, token)
      state_str = str(job_doc.get('currentState') or 'JOB_STATE_UNKNOWN')
      raw_counters: dict[str, int] = {}
      try:
        m_doc = self._rest_json(f'{base}/metrics', token)
        raw_counters = _extract_raw_counters_from_metrics_list(
            m_doc.get('metrics'))
      except Exception:  # pylint: disable=broad-except
        pass
      return state_str, finalize_cli_row_metrics(raw_counters)

    # Fallback: instantiate `DataflowApplicationClient` ONCE and reuse it.
    if self._apiclient is None:
      os.environ.setdefault('GRPC_VERBOSITY', 'ERROR')
      os.environ.setdefault('GRPC_ENABLE_FORK_SUPPORT', '0')
      from apache_beam.options.pipeline_options import PipelineOptions
      from apache_beam.runners.dataflow.internal import apiclient
      opts = PipelineOptions([f'--project={project}', f'--region={region}'])
      self._apiclient = apiclient.DataflowApplicationClient(opts)
    job_proto = self._apiclient.get_job(job_id)
    raw_state = getattr(job_proto, 'current_state', None)
    if raw_state is None:
      raw_state = getattr(job_proto, 'currentState', 'JOB_STATE_UNKNOWN')
    state_str = str(getattr(raw_state, 'name', None) or raw_state)
    raw_counters = {}
    try:
      metrics_proto = self._apiclient.get_job_metrics(job_id)
      raw_counters = _extract_raw_counters_from_metrics_list(
          getattr(metrics_proto, 'metrics', None))
    except Exception:  # pylint: disable=broad-except
      pass
    return state_str, finalize_cli_row_metrics(raw_counters)

  def modify_state(self, job_id: str, target_state: str) -> bool:
    import urllib.parse
    token = self._ensure_rest_creds()
    if token is not None:
      q_proj = urllib.parse.quote(self._project, safe='')
      q_reg = urllib.parse.quote(self._region, safe='')
      q_job = urllib.parse.quote(job_id, safe='')
      url = (
          f'https://dataflow.googleapis.com/v1b3/projects/{q_proj}'
          f'/locations/{q_reg}/jobs/{q_job}')
      try:
        self._rest_json(
            url, token, method='PUT', body={'requestedState': target_state})
        return True
      except Exception:  # pylint: disable=broad-except
        return False
    if self._apiclient is None:
      os.environ.setdefault('GRPC_VERBOSITY', 'ERROR')
      os.environ.setdefault('GRPC_ENABLE_FORK_SUPPORT', '0')
      from apache_beam.options.pipeline_options import PipelineOptions
      from apache_beam.runners.dataflow.internal import apiclient
      opts = PipelineOptions([
          f'--project={self._project}', f'--region={self._region}'
      ])
      self._apiclient = apiclient.DataflowApplicationClient(opts)
    return bool(self._apiclient.modify_job_state(job_id, target_state))


def wait_for_job(
    job_id: str,
    *,
    timeout_seconds: float | None = None,
    poll_interval: float = 3.0,
    project: str | None = None,
    region: str | None = None,
    query_fn=None,
    show_progress: bool = False,
) -> tuple[int, dict[str, Any]]:
  """Polls a remote job until terminal state or `timeout_seconds` (`EXIT_TIMEOUT=7`)."""
  import sys
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

  proj, reg = _validate_job_project_and_region(
      safe_id,
      project or record.get('project'),
      region or record.get('region') or 'us-central1',
      verb='poll')

  poller = _DataflowPoller(proj, reg) if query_fn is None else None
  fetch = query_fn or poller.query
  t_start = time.monotonic()
  deadline = (t_start + timeout_seconds) if timeout_seconds else None
  last_logged_state: str | None = None
  last_logged_at = 0.0

  console_url = dataflow_console_url(safe_id, proj, reg)
  if console_url:
    existing_out = dict(record.get('outputs') or {})
    existing_out.setdefault('console_url', console_url)
    record['outputs'] = existing_out

  if show_progress and console_url:
    print(f'[beam] Job console: {console_url}', file=sys.stderr, flush=True)

  while True:
    state_str, counters = fetch(safe_id, proj, reg)
    upper = state_str.upper()
    try:
      latest_disk = load_job_record(safe_id)
      if isinstance(latest_disk, dict):
        disk_state = str(latest_disk.get('state') or '').upper()
        record.update(latest_disk)
        if console_url:
          merged_out = dict(record.get('outputs') or {})
          merged_out.setdefault('console_url', console_url)
          record['outputs'] = merged_out
        if (disk_state in ('JOB_STATE_CANCELLED', 'CANCELLED', 'CANCELED',
                           'JOB_STATE_DRAINING', 'DRAINING') and
            upper not in (_TERMINAL_SUCCESS_STATES | _TERMINAL_FAILURE_STATES)):
          record['state'] = latest_disk['state']
        else:
          record['state'] = state_str
      else:
        record['state'] = state_str
    except JobError:
      record['state'] = state_str

    record['updated_at'] = time.time()
    eff_counters = counters if counters else dict(record.get('metrics') or {})
    if eff_counters:
      record['metrics'] = eff_counters
    _atomic_write_json(_jobs_dir() / f'{safe_id}.json', record)

    now_m = time.monotonic()
    elapsed_s = int(now_m - t_start)
    if show_progress and (
        state_str != last_logged_state or (now_m - last_logged_at) >= 10.0):
      print(
          f'[beam] Waiting for job {safe_id} '
          f'(state={state_str}, elapsed={elapsed_s}s)...',
          file=sys.stderr,
          flush=True)
      last_logged_state = state_str
      last_logged_at = now_m

    if upper in _TERMINAL_SUCCESS_STATES:
      outputs_obj = dict(record.get('outputs') or {})
      out_pfx = outputs_obj.get('output_prefix')
      dlq_pfx = outputs_obj.get('dlq_prefix')
      if out_pfx or dlq_pfx:
        try:
          os.environ.setdefault('GRPC_VERBOSITY', 'ERROR')
          os.environ.setdefault('GRPC_ENABLE_FORK_SUPPORT', '0')
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
        err_msg = check_assert_counts(eff_counters)
        if err_msg:
          record['validation_state'] = 'FAILED'
          record['validation_exit_code'] = EXIT_PIPELINE_FAILED
          record['post_check_error'] = err_msg
          _atomic_write_json(_jobs_dir() / f'{safe_id}.json', record)
          return EXIT_PIPELINE_FAILED, dict(record)
      if post.get('verify_diff'):
        from . import diff
        v_cfg = post['verify_diff']
        v_right = (out_pfx + '*' if out_pfx else v_cfg['right'])
        diff_code, diff_report = diff.run_diff(
            v_cfg['left'],
            v_right,
            keys=v_cfg.get('keys'),
            left_opts=v_cfg.get('left_opts'),
            right_opts=v_cfg.get('right_opts'),
            pipeline_args=v_cfg.get('pipeline_args'),
            default_project=proj)
        diff_report.pop('untrusted_data', None)
        outputs_obj['verify'] = diff_report
        record['outputs'] = outputs_obj
        record['verify_diff'] = diff_report
        if diff_code != 0:
          v_err = (
              f'--verify detected mismatch between '
              f'{v_cfg["left"]} and {v_cfg["right"]}')
          record['validation_state'] = 'FAILED'
          record['validation_exit_code'] = diff_code
          record['post_check_error'] = v_err
          _atomic_write_json(_jobs_dir() / f'{safe_id}.json', record)
          return diff_code, dict(record)
      record['validation_state'] = 'PASSED' if post else 'NONE'
      record['validation_exit_code'] = EXIT_OK
      record.pop('post_check_error', None)
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


def _validate_job_project_and_region(
    safe_id: str,
    raw_proj: Any,
    raw_reg: Any,
    *,
    verb: str,
) -> tuple[str, str]:
  from . import uris
  if not raw_proj:
    raise JobError(
        f'cannot {verb} job {safe_id!r} without a GCP project; pass --project',
        EXIT_USAGE)
  try:
    proj = uris.validate_bq_project_id(str(raw_proj))
  except uris.UriError as e:
    raise JobError(str(e), EXIT_USAGE) from None
  reg = str(raw_reg or 'us-central1').strip()
  if not re.fullmatch(r'[a-z0-9]+(?:-[a-z0-9]+)+', reg):
    raise JobError(f'invalid GCP region {raw_reg!r}', EXIT_USAGE)
  return proj, reg


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

  proj, reg = _validate_job_project_and_region(
      safe_id,
      project or record.get('project'),
      region or record.get('region') or 'us-central1',
      verb='cancel')

  target_state = 'JOB_STATE_DRAINING' if drain else 'JOB_STATE_CANCELLED'
  already_terminal = False
  current_state: str | None = None
  if cancel_fn is not None:
    ok = bool(cancel_fn(safe_id, proj, reg, target_state))
  else:
    poller = _DataflowPoller(proj, reg)
    ok = poller.modify_state(safe_id, target_state)
    if not ok:
      try:
        q_state, _ = poller.query(safe_id)
        current_state = str(q_state or '').upper()
        if current_state in (_TERMINAL_SUCCESS_STATES |
                             _TERMINAL_FAILURE_STATES):
          ok = True
          already_terminal = True
      except Exception:  # pylint: disable=broad-except
        pass

  if not ok:
    hint = f' (current state: {current_state})' if current_state else ''
    raise JobError(
        f'runner rejected state transition to {target_state} for job {safe_id!r}{hint}',
        EXIT_PIPELINE_FAILED)

  if record:
    record['state'] = current_state if already_terminal else target_state
    record['updated_at'] = time.time()
    _atomic_write_json(_jobs_dir() / f'{safe_id}.json', record)

  out_res: dict[str, Any] = {
      'job_id': safe_id,
      'project': proj,
      'region': reg,
      'requested_state': target_state,
      'mode': 'drain' if drain else 'cancel',
  }
  if already_terminal and current_state:
    out_res['already_terminal'] = True
    out_res['current_state'] = current_state
  return out_res
