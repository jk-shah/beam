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

"""Bounded & live follow (`-f`) non-destructive stream/dataset peek (`beam tail`).

Guarantees:
1. Bounded mode (`beam tail <URI>`): samples up to `--limit` (`-n`, default 5)
   records within `--duration` (default `5s`) and exits cleanly (`0`), so AI
   agents never hang synchronous tool calls.
2. Live follow mode (`beam tail -f <URI>`): streams new records continuously
   as they arrive (just like Unix `tail -f`) until `Ctrl-C`, `--limit` (`-n`),
   or `--duration` is reached:
   - `pubsub://projects/<p>/subscriptions/<s>`: **never calls `acknowledge()`**,
     deduplicates redelivered `message_id`s in a bounded ring buffer, and
     releases leases (`ack_deadline_seconds=0`) so downstream consumers see
     every message with zero loss.
   - `pubsub://projects/<p>/topics/<t>`: creates an ephemeral subscription
     `beam-cli-peek-<epoch_hex>-<rand>`, acknowledges messages on that private
     ephemeral subscription so `-f` never duplicates records across ack
     deadlines, and **always deletes** the ephemeral subscription in `finally:`
     (including on `Ctrl-C` / `KeyboardInterrupt`).
   - `kafka://<brokers>/<topic>`: tails from `'latest'` in `-f` mode (or
     `'earliest'` in bounded peek) without committing consumer offsets.
   - Local or `gs://` files/directories: watches appended lines and newly
     landed streaming/DLQ shards live.
3. Enforces `--max-scanned-bytes` (default `64MB`) and sanitizes every record
   via `output.sanitize_untrusted_value(...)` (`untrusted_data`).
"""

import collections
import csv
import datetime
import glob
import io
import json
import os
import re
import signal
import struct
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any
from typing import Callable

from . import cost_guard
from . import inspect_cmd
from . import jobs
from . import output
from . import uris

EXIT_MISSING_PACKAGE = 4

RecordCallback = Callable[[dict[str, Any], int], None]


class TailError(Exception):
  """Raised when `beam tail` encounters a usage or missing-package error."""
  def __init__(self, message: str, exit_code: int = 2):
    super().__init__(message)
    self.exit_code = exit_code


_ANSI_ESCAPE_RE = re.compile(r'\x1b\[[0-9;]*[a-zA-Z]|\x1b\].*?(\x07|\x1b\\)|[\x00-\x08\x0b-\x1f\x7f]')


def _sanitize_terminal_str(s: str) -> str:
  """Strips ANSI, C1, and Unicode BiDi control characters from untrusted values."""
  from . import output
  return output.sanitize_untrusted_text(s, max_chars=4096)


class InteractiveTailFormatter:
  """Human-friendly live terminal TUI with throughput tracking, ANSI colors, and field inspector.

  Provides live rolling rate calculation, colored field formatting, non-destructive
  ephemeral peek banners, and graceful terminal exit summaries. All untrusted record
  keys and values are sanitized to prevent terminal control character / ANSI escape injection.
  """

  def __init__(
      self,
      uri: str,
      follow: bool = False,
      use_color: bool | None = None,
      stream: Any = None,
  ):
    self.uri = _sanitize_terminal_str(str(uri))
    self.follow = follow
    self._stream = stream if stream is not None else sys.stdout
    if use_color is None:
      self.use_color = (
          hasattr(self._stream, 'isatty')
          and self._stream.isatty()
          and 'NO_COLOR' not in os.environ
          and os.environ.get('TERM') != 'dumb'
      )
    else:
      self.use_color = use_color

    self.start_time = time.monotonic()
    self.record_count = 0
    self.total_bytes = 0
    self.timestamps: collections.deque[float] = collections.deque(maxlen=500)
    self.peak_rate = 0.0

  def _style(self, text: str, code: str) -> str:
    if not self.use_color:
      return text
    return f'\033[{code}m{text}\033[0m'

  def bold(self, text: str) -> str:
    return self._style(text, '1')

  def cyan(self, text: str) -> str:
    return self._style(text, '36')

  def green(self, text: str) -> str:
    return self._style(text, '32')

  def yellow(self, text: str) -> str:
    return self._style(text, '33')

  def dim(self, text: str) -> str:
    return self._style(text, '2')

  def magenta(self, text: str) -> str:
    return self._style(text, '35')

  def render_start_banner(self) -> None:
    """Renders the top banner on stderr showing stream URI and non-destructive mode."""
    mode_str = 'Live Follow (Ctrl-C to stop)' if self.follow else 'Sample Peek'
    banner = (
        f"{self.cyan('╭─')} {self.bold('Beam Stream')}: {self.yellow(self.uri)} "
        f"[{self.green(mode_str)}] "
        f"{self.cyan('─' * max(4, 60 - len(self.uri) - len(mode_str)))}╮\n"
        f"{self.cyan('│')} Non-destructive: {self.green('True')} (Zero loss / No offset commits) "
        f"{self.cyan('│')}\n"
        f"{self.cyan('╰' + '─' * 74 + '╯')}"
    )
    print(banner, file=sys.stderr, flush=True)

  def format_record(self, rec: dict[str, Any], idx: int) -> str:
    """Formats a single stream record with timestamps, 5s rolling rate, and sanitized fields."""
    now = time.monotonic()
    self.record_count += 1
    self.timestamps.append(now)
    while len(self.timestamps) > 1 and (now - self.timestamps[0]) > 5.0:
      self.timestamps.popleft()
    rec_bytes = len(json.dumps(rec, default=str))
    self.total_bytes += rec_bytes

    if len(self.timestamps) > 1:
      span = self.timestamps[-1] - self.timestamps[0]
      rate = (len(self.timestamps) - 1) / max(0.001, span)
    else:
      rate = 1.0 / max(0.001, now - self.start_time)
    if rate > self.peak_rate:
      self.peak_rate = rate

    idx_str = self.dim(f'[#{idx}]')
    ts_str = self.green(datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3])

    parts = []
    for k, v in rec.items():
      safe_k = _sanitize_terminal_str(str(k))
      kl = safe_k.lower()
      val_raw = json.dumps(v, default=str) if isinstance(v, (dict, list)) else str(v)
      safe_v = _sanitize_terminal_str(val_raw)
      if len(safe_v) > 60:
        safe_v = safe_v[:57] + '...'
      if isinstance(v, (int, float)) and not isinstance(v, bool):
        styled_v = self.yellow(safe_v)
      elif kl.endswith(('_ts', '_time', 'timestamp', 'created_at', 'updated_at')):
        styled_v = self.green(safe_v)
      elif kl == 'id' or kl.endswith('_id') or kl in ('key', 'partition'):
        styled_v = self.cyan(safe_v)
      else:
        styled_v = self.yellow(safe_v)
      parts.append(f"{self.bold(safe_k)}={styled_v}")

    fields_str = '  '.join(parts)
    rate_str = self.dim(f"({rate:.1f} msg/s)") if self.follow else ""
    return f"{idx_str} {ts_str} {rate_str} {fields_str}"

  def on_stream_record(self, rec: dict[str, Any], idx: int) -> None:
    """Callback invoked on every record arrival to print the formatted line."""
    line = self.format_record(rec, idx)
    print(line, file=self._stream, flush=True)

  def render_exit_summary(self, interrupted: bool = False) -> None:
    """Renders the summary card upon stream completion or Ctrl-C interrupt."""
    elapsed = max(0.001, time.monotonic() - self.start_time)
    avg_rate = self.record_count / elapsed
    bytes_kb = self.total_bytes / 1024.0
    status_str = 'Stopped by user (Ctrl-C)' if interrupted else 'Completed'

    summary = (
        f"\n{self.cyan('╭─')} {self.bold('Stream Summary')} "
        f"[{self.green(status_str)}] {self.cyan('─' * 46)}╮\n"
        f"{self.cyan('│')} Elapsed: {self.bold(f'{elapsed:.1f}s')} | "
        f"Records: {self.bold(str(self.record_count))} | "
        f"Avg Rate: {self.bold(f'{avg_rate:.1f} msg/s')} | "
        f"Peak: {self.bold(f'{self.peak_rate:.1f} msg/s')} {self.cyan('│')}\n"
        f"{self.cyan('│')} Data Scanned: {self.bold(f'{bytes_kb:.1f} KB')} | "
        f"Non-Destructive: {self.green('True')} {self.cyan('│')}\n"
        f"{self.cyan('╰' + '─' * 68 + '╯')}"
    )
    print(summary, file=sys.stderr, flush=True)


def _decode_payload_bytes(raw_bytes: bytes) -> Any:
  text = raw_bytes.decode('utf-8', errors='replace')
  stripped = text.strip()
  if stripped.startswith('{') or stripped.startswith('['):
    try:
      return json.loads(stripped)
    except ValueError:
      pass
  return output.sanitize_untrusted_text(text)


def _emit_record(
    rows: list[dict[str, Any]],
    raw_record: Any,
    on_record: RecordCallback | None,
) -> dict[str, Any]:
  sanitized = output.sanitize_untrusted_value(raw_record)
  if not isinstance(sanitized, dict):
    sanitized = {'value': sanitized}
  rows.append(sanitized)
  if on_record is not None:
    on_record(sanitized, len(rows))
  return sanitized


def run_tail(
    location: str,
    *,
    limit: int | None = 5,
    duration: str | None = '5s',
    follow: bool = False,
    poll_interval: float = 0.5,
    max_scanned_bytes: str = '64MB',
    explicit_format: str | None = None,
    from_opts: list[str] | None = None,
    default_project: str | None = None,
    on_record: RecordCallback | None = None,
) -> dict[str, Any]:
  """Samples or live-follows (`follow=True`) records without consuming."""
  if limit is not None and (limit < 0 or (limit == 0 and not follow)):
    raise TailError('--limit (-n) must be >= 1 (or 0 with -f)', 2)
  duration_sec = (
      jobs.parse_duration_seconds(duration) if duration is not None else None)
  byte_budget = cost_guard.parse_byte_size(max_scanned_bytes)

  try:
    parsed_opts = uris.parse_kv_options(from_opts, None, '--from-opt')
    try:
      ep = uris.parse_endpoint(
          location,
          is_read=True,
          explicit_format=explicit_format,
          options=parsed_opts,
          default_project=default_project,
          allow_sniff=True)
    except uris.UriError as e:
      scheme = location.split('://', 1)[0].lower() if '://' in location else ''
      if (follow and not explicit_format and scheme in ('', 'gs') and
          'cannot tell the format' in str(e)):
        # Allow `beam tail -f` on empty, not-yet-created, or directory targets
        # (for example `/var/log/messages` after rotation or log dirs).
        ep = uris.parse_endpoint(
            location,
            is_read=True,
            explicit_format='text',
            options=parsed_opts,
            default_project=default_project,
            allow_sniff=True)
      else:
        raise
  except uris.UriError as e:
    raise TailError(str(e), 2) from None

  if ep.kind == 'pubsub':
    return _tail_pubsub(
        ep,
        limit=limit,
        duration_sec=duration_sec,
        follow=follow,
        poll_interval=poll_interval,
        byte_budget=byte_budget,
        default_project=default_project,
        on_record=on_record)
  if ep.kind == 'kafka':
    return _tail_kafka(
        ep,
        limit=limit,
        duration_sec=duration_sec,
        follow=follow,
        byte_budget=byte_budget,
        on_record=on_record)
  if ep.kind in ('delta', 'iceberg') and follow:
    return _tail_lakehouse_follow(
        ep,
        limit=limit,
        duration_sec=duration_sec,
        poll_interval=poll_interval,
        byte_budget=byte_budget,
        default_project=default_project,
        on_record=on_record)
  if ep.kind == 'file' and follow:
    return _tail_files_follow(
        location,
        limit=limit,
        duration_sec=duration_sec,
        poll_interval=poll_interval,
        byte_budget=byte_budget,
        default_project=default_project,
        on_record=on_record)
  if ep.kind in ('file', 'bigquery', 'postgres', 'delta', 'iceberg'):
    if follow and ep.kind in ('bigquery', 'postgres'):
      raise TailError(
          'beam tail -f (--follow) is for streaming/file/lakehouse endpoints '
          '(pubsub://, kafka://, delta://, iceberg://, local paths, gs://); '
          f'omit -f to peek at {ep.kind}:// tables.',
          2)
    eff_limit = 5 if limit is None else limit
    inspected = inspect_cmd.run_inspect(
        location,
        explicit_format=explicit_format,
        sample_count=eff_limit,
        max_scanned_bytes=max_scanned_bytes,
        from_opts=from_opts,
        default_project=default_project)
    sampled_rows = list((inspected.get('untrusted_data') or {}).get('rows', ()))
    if on_record is not None:
      for idx, r in enumerate(sampled_rows, 1):
        on_record(r, idx)
    return {
        'uri': location,
        'kind': ep.kind,
        'mode': 'snapshot_peek',
        'follow': False,
        'non_destructive': True,
        'sampled_count': len(sampled_rows),
        'scanned_bytes': inspected.get('estimated_bytes'),
        'untrusted_data': inspected.get('untrusted_data'),
    }
  raise TailError(
      f'beam tail does not support {ep.kind!r} endpoints yet; supported: '
      'pubsub://, kafka://, delta://, iceberg://, bigquery://, and file/gs:// paths',
      2)


_PUBSUB_LEASE_WINDOW_SEC = 900
_PUBSUB_HEARTBEAT_SEC = 300.0
_PUBSUB_FOLLOW_FALLBACK_MAX_AGE_SEC = 86400.0


def _tail_leases_dir() -> Path:
  beam_home = Path(os.path.expanduser(os.environ.get('BEAM_HOME', '~/.beam')))
  if os.path.islink(beam_home):
    raise OSError(f'refusing symlinked BEAM_HOME {str(beam_home)!r}')
  d = beam_home / 'tail_leases'
  if os.path.islink(d):
    raise OSError(f'refusing symlinked tail_leases {str(d)!r}')
  return d


def _write_local_tail_lease(sub_id: str, lease_until: int) -> None:
  try:
    d = _tail_leases_dir()
    d.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    d.mkdir(mode=0o700, parents=True, exist_ok=True)
    jobs._atomic_write_json(
        d / f'{sub_id}.json', {
            'pid': os.getpid(), 'lease_until': int(lease_until)
        })
  except Exception:  # pylint: disable=broad-except
    pass


def _remove_local_tail_lease(sub_id: str | None) -> None:
  if not sub_id:
    return
  try:
    (_tail_leases_dir() / f'{sub_id}.json').unlink(missing_ok=True)
  except Exception:  # pylint: disable=broad-except
    pass


def _is_local_tail_pid_alive(sub_name: str, now: float) -> bool:
  try:
    lease_file = _tail_leases_dir() / f'{sub_name}.json'
    if not lease_file.is_file() or os.path.islink(lease_file):
      return False
    data = json.loads(lease_file.read_text(encoding='utf-8'))
    lease_until = float(data.get('lease_until', 0))
    if lease_until <= 0 or now > (lease_until + 60):
      try:
        lease_file.unlink(missing_ok=True)
      except OSError:
        pass
      return False
    pid = int(data.get('pid', 0))
    if pid <= 0:
      return False
  except Exception:  # pylint: disable=broad-except
    return False
  try:
    os.kill(pid, 0)
    return True
  except ProcessLookupError:
    try:
      lease_file.unlink(missing_ok=True)
    except OSError:
      pass
    return False
  except PermissionError:
    return True
  except Exception:  # pylint: disable=broad-except
    return False


def _gc_stale_peek_subscriptions(
    subscriber, publisher, topic_path: str, max_age_sec: float = 900.0) -> None:
  """Best-effort cleanup of orphaned `beam-cli-peek-*` / `beam-cli-follow-*` subscriptions.

  Never deletes a subscription whose local process is still alive, whose
  `beam_lease_until` heartbeat label has not expired, or whose encoded
  expiration / follow TTL is still active.
  """
  now = time.time()
  try:
    for sub_path in publisher.list_topic_subscriptions(
        request={'topic': topic_path}):
      sub_name = sub_path.rsplit('/', 1)[-1]
      is_peek = sub_name.startswith('beam-cli-peek-')
      is_follow = sub_name.startswith('beam-cli-follow-')
      if not (is_peek or is_follow):
        continue
      if _is_local_tail_pid_alive(sub_name, now):
        continue
      parts = sub_name.split('-')
      if len(parts) < 5:
        continue
      try:
        ts_val = int(parts[3], 16)
      except ValueError:
        continue

      # Check subscription labels (`beam_lease_until` / `beam_mode`) before any
      # deletion so long-running `beam tail -f` sessions (>15m) on any host are
      # never deleted mid-stream!
      lease_until: int | None = None
      label_mode: str | None = None
      try:
        sub_meta = subscriber.get_subscription(
            request={'subscription': sub_path})
        labels = getattr(sub_meta, 'labels', None)
        if labels is not None and hasattr(labels, 'get'):
          label_mode = str(labels.get('beam_mode') or '').lower() or None
          raw_lease = labels.get('beam_lease_until')
          if raw_lease is not None:
            lease_until = int(str(raw_lease))
      except Exception:  # pylint: disable=broad-except
        pass

      if lease_until is not None:
        # Active session heartbeats `beam_lease_until` every 5m (+60s skew grace).
        if now <= (lease_until + 60):
          continue
        try:
          subscriber.delete_subscription(request={'subscription': sub_path})
        except Exception:  # pylint: disable=broad-except
          pass
        continue

      # Fallback when labels are unavailable (e.g., restricted IAM without
      # `pubsub.subscriptions.get`/`update` or legacy subscription names):
      # - `beam-cli-follow-*` (or `beam_mode=follow`) relies on `finally:` +
      #   GCP Pub/Sub native idle `expiration_policy` (24h) so active >15m
      #   follow sessions are never killed by concurrent `beam tail` startups!
      effective_age_limit = (
          _PUBSUB_FOLLOW_FALLBACK_MAX_AGE_SEC if
          (is_follow or label_mode == 'follow') else max_age_sec)
      if (now - ts_val) > effective_age_limit:
        try:
          subscriber.delete_subscription(request={'subscription': sub_path})
        except Exception:  # pylint: disable=broad-except
          pass
  except Exception:  # pylint: disable=broad-except
    pass


def _refresh_pubsub_lease(
    subscriber, ephemeral_sub: str, sub_id: str, mode: str,
    lease_sec: int) -> int:
  lease_until = int(time.time()) + max(_PUBSUB_LEASE_WINDOW_SEC, int(lease_sec))
  _write_local_tail_lease(sub_id, lease_until)
  try:
    subscriber.update_subscription(
        request={
            'subscription': {
                'name': ephemeral_sub,
                'labels': {
                    'beam_cli': 'peek',
                    'beam_mode': mode,
                    'beam_lease_until': str(lease_until),
                },
            },
            'update_mask': {
                'paths': ['labels']
            },
        })
  except Exception:  # pylint: disable=broad-except
    pass
  return lease_until


def _tail_pubsub(
    ep: uris.ParsedEndpoint,
    *,
    limit: int | None,
    duration_sec: float | None,
    follow: bool,
    poll_interval: float,
    byte_budget: int,
    default_project: str | None,
    on_record: RecordCallback | None,
) -> dict[str, Any]:
  try:
    from google.api_core import exceptions as gcp_exceptions
    from google.cloud import pubsub_v1
  except ImportError as e:
    raise TailError(
        f'google-cloud-pubsub is required for pubsub:// tailing: {e}\n'
        "  install: pip install 'apache-beam[gcp]'",
        EXIT_MISSING_PACKAGE) from None

  subscriber = pubsub_v1.SubscriberClient()
  ephemeral_sub: str | None = None
  ephemeral_sub_id: str | None = None
  sub_path = ep.config.get('subscription')
  topic_path = ep.config.get('topic')

  rows: list[dict[str, Any]] = []
  scanned_bytes = 0
  truncated_budget = False
  interrupted = False

  # Bounded deduplication window so non-destructive subscription `tail -f`
  # never prints redelivered unacked messages twice.
  seen_ids_order: collections.deque[str] = collections.deque()
  seen_ids_set: set[str] = set()

  def _remember_msg_id(mid: str) -> bool:
    if not mid:
      return True
    if mid in seen_ids_set:
      return False
    if len(seen_ids_order) >= 8192:
      oldest = seen_ids_order.popleft()
      seen_ids_set.discard(oldest)
    seen_ids_order.append(mid)
    seen_ids_set.add(mid)
    return True

  prev_sigterm = None
  installed_sigterm = False
  if threading.current_thread() is threading.main_thread() and hasattr(
      signal, 'SIGTERM'):
    try:

      def _on_sigterm(_signum, _frame):
        raise KeyboardInterrupt()

      prev_sigterm = signal.getsignal(signal.SIGTERM)
      signal.signal(signal.SIGTERM, _on_sigterm)
      installed_sigterm = True
    except (OSError, ValueError):
      installed_sigterm = False

  try:
    mode_tag = 'follow' if follow else 'peek'
    lease_span = _PUBSUB_LEASE_WINDOW_SEC
    if duration_sec is not None and not follow:
      lease_span = max(
          _PUBSUB_LEASE_WINDOW_SEC, min(86400, int(duration_sec) + 120))
    last_lease_refresh = time.monotonic()

    if topic_path and not sub_path:
      parts = topic_path.split('/')
      proj = parts[1] if len(parts) >= 4 else default_project
      if not proj:
        raise TailError(
            f'cannot determine GCP project for topic {topic_path!r}', 2)
      try:
        proj = uris.validate_bq_project_id(proj)
      except uris.UriError as e:
        raise TailError(str(e), 2) from None
      publisher = pubsub_v1.PublisherClient()
      _gc_stale_peek_subscriptions(subscriber, publisher, topic_path)
      now_int = int(time.time())
      # Encode creation/anchor timestamp; `beam-cli-follow-*` protects long
      # `-f` sessions even when `pubsub.subscriptions.get`/`update` is absent.
      sub_prefix = 'beam-cli-follow' if follow else 'beam-cli-peek'
      anchor_ts = (
          now_int if follow else now_int +
          max(0, lease_span - _PUBSUB_LEASE_WINDOW_SEC))
      epoch_hex = format(anchor_ts, 'x')
      ephemeral_sub_id = f'{sub_prefix}-{epoch_hex}-{uuid.uuid4().hex[:8]}'
      ephemeral_sub = f'projects/{proj}/subscriptions/{ephemeral_sub_id}'
      lease_until = now_int + lease_span
      _write_local_tail_lease(ephemeral_sub_id, lease_until)
      base_sub_req: dict[str, Any] = {
          'name': ephemeral_sub,
          'topic': topic_path,
          'ack_deadline_seconds': 10,
          'message_retention_duration': {
              'seconds': 600
          },
          'expiration_policy': {
              'ttl': {
                  'seconds': 86400
              }
          },
      }
      try:
        subscriber.create_subscription(
            request={
                **base_sub_req,
                'labels': {
                    'beam_cli': 'peek',
                    'beam_mode': mode_tag,
                    'beam_lease_until': str(lease_until),
                },
            })
      except (TypeError, gcp_exceptions.InvalidArgument):
        subscriber.create_subscription(request=base_sub_req)
      sub_path = ephemeral_sub

    deadline = (
        time.monotonic() + duration_sec if duration_sec is not None else None)
    pending_nack_ids: list[str] = []

    try:
      while True:
        if limit is not None and len(rows) >= limit:
          break
        if deadline is not None and time.monotonic() >= deadline:
          break
        if (ephemeral_sub and ephemeral_sub_id and
            (time.monotonic() - last_lease_refresh) >= _PUBSUB_HEARTBEAT_SEC):
          _refresh_pubsub_lease(
              subscriber, ephemeral_sub, ephemeral_sub_id, mode_tag, lease_span)
          last_lease_refresh = time.monotonic()

        remaining = (
            max(0.2, deadline -
                time.monotonic()) if deadline is not None else 5.0)
        batch_max = (min(20, limit - len(rows)) if limit is not None else 20)
        if batch_max <= 0:
          break

        try:
          response = subscriber.pull(
              request={
                  'subscription': sub_path,
                  'max_messages': batch_max,
              },
              timeout=min(remaining, 5.0))
        except gcp_exceptions.DeadlineExceeded:
          if not follow:
            break
          continue

        received = list(getattr(response, 'received_messages', ()) or ())
        if not received:
          if not follow and not ephemeral_sub:
            break
          sleep_s = (
              min(poll_interval, remaining)
              if deadline is not None else poll_interval)
          time.sleep(max(0.05, sleep_s))
          continue

        if ephemeral_sub:
          ephemeral_ack_ids = [
              rm.ack_id for rm in received if getattr(rm, 'ack_id', None)
          ]
          cycle_nack_ids = []
        else:
          ephemeral_ack_ids = []
          cycle_nack_ids = [
              rm.ack_id for rm in received if getattr(rm, 'ack_id', None)
          ]
          pending_nack_ids.extend(cycle_nack_ids)

        for rm in received:
          msg = rm.message
          msg_id = str(getattr(msg, 'message_id', ''))
          if not ephemeral_sub and not _remember_msg_id(msg_id):
            continue

          raw_data = bytes(getattr(msg, 'data', b'') or b'')
          if scanned_bytes + len(raw_data) > byte_budget:
            truncated_budget = True
            break
          scanned_bytes += len(raw_data)
          pub_time = getattr(msg, 'publish_time', None)
          _emit_record(
              rows,
              {
                  'message_id': msg_id,
                  'publish_time': (
                      pub_time.isoformat() if hasattr(pub_time, 'isoformat')
                      else str(pub_time) if pub_time else None),
                  'attributes': dict(getattr(msg, 'attributes', {}) or {}),
                  'payload': _decode_payload_bytes(raw_data),
              },
              on_record)
          if limit is not None and len(rows) >= limit:
            break

        # Private ephemeral subscription: ACK immediately so `-f` never
        # redelivers already-printed messages after 10s ack deadline!
        if ephemeral_sub and ephemeral_ack_ids and sub_path:
          try:
            subscriber.acknowledge(
                request={
                    'subscription': sub_path,
                    'ack_ids': ephemeral_ack_ids,
                })
          except Exception:  # pylint: disable=broad-except
            pass

        # Shared existing subscription in `-f` mode: release lease immediately
        # so real downstream consumers are never delayed, then pause briefly.
        if not ephemeral_sub and follow and cycle_nack_ids and sub_path:
          try:
            subscriber.modify_ack_deadline(
                request={
                    'subscription': sub_path,
                    'ack_ids': list(dict.fromkeys(cycle_nack_ids)),
                    'ack_deadline_seconds': 0,
                })
            pending_nack_ids.clear()
          except Exception:  # pylint: disable=broad-except
            pass

        if truncated_budget:
          break
        if follow and not ephemeral_sub:
          time.sleep(max(0.1, poll_interval))
    except KeyboardInterrupt:
      if not follow:
        raise
      interrupted = True
    finally:
      if not ephemeral_sub and pending_nack_ids and sub_path:
        try:
          subscriber.modify_ack_deadline(
              request={
                  'subscription': sub_path,
                  'ack_ids': list(dict.fromkeys(pending_nack_ids)),
                  'ack_deadline_seconds': 0,
              })
          pending_nack_ids.clear()
        except Exception:  # pylint: disable=broad-except
          pass
  finally:
    if installed_sigterm:
      try:
        signal.signal(signal.SIGTERM, prev_sigterm)
      except (OSError, ValueError):
        pass
    _remove_local_tail_lease(ephemeral_sub_id)
    if ephemeral_sub:
      try:
        subscriber.delete_subscription(request={'subscription': ephemeral_sub})
      except Exception:  # pylint: disable=broad-except
        pass
    try:
      subscriber.close()
    except Exception:  # pylint: disable=broad-except
      pass

  hit_limit = bool(limit is not None and len(rows) >= limit)
  return {
      'uri': ep.raw,
      'kind': 'pubsub',
      'mode': (
          'ephemeral_topic_follow' if (ephemeral_sub and follow) else
          'ephemeral_topic_subscription' if ephemeral_sub else
          'subscription_nack_follow' if follow else 'subscription_nack_peek'),
      'follow': bool(follow),
      'interrupted': interrupted,
      'non_destructive': True,
      'sampled_count': len(rows),
      'scanned_bytes': scanned_bytes,
      'untrusted_data': output.wrap_untrusted_rows(
          rows, truncated=truncated_budget or hit_limit),
  }


_BLOCKED_METADATA_HOSTS = frozenset({
    '169.254.169.254',
    'fd00:ec2::254',
    '[fd00:ec2::254]',
    'metadata.google.internal',
    'metadata',
    '100.100.100.200',
})


def _fetch_confluent_schema_doc(
    registry_url: str,
    schema_id: int,
    cache: dict[int, Any],
    basic_auth: str | None = None,
) -> dict[str, Any]:
  """Fetches and caches `{schema_type, parsed}` from Confluent Schema Registry."""
  if schema_id in cache:
    cached_val = cache[schema_id]
    if isinstance(cached_val, dict) and '_beam_schema_type' in cached_val:
      return cached_val
    return {'_beam_schema_type': 'AVRO', 'parsed': cached_val}
  parsed_u = urllib.parse.urlsplit(registry_url)
  host = (parsed_u.hostname or '').lower()
  if (parsed_u.scheme not in ('http', 'https') or not host or
      host in _BLOCKED_METADATA_HOSTS):
    raise TailError(f'disallowed Schema Registry URL {registry_url!r}', 2)
  endpoint = f"{registry_url.rstrip('/')}/schemas/ids/{int(schema_id)}"
  headers = {'Accept': 'application/json'}
  if basic_auth:
    import base64
    b64_tok = base64.b64encode(basic_auth.encode('utf-8')).decode('ascii')
    headers['Authorization'] = f'Basic {b64_tok}'
  req = urllib.request.Request(endpoint, headers=headers)
  with urllib.request.urlopen(req, timeout=5.0) as resp:  # nosec B310
    raw_body = resp.read(1024 * 1024 + 1)
  if len(raw_body) > 1024 * 1024:
    raise TailError('Schema Registry response exceeded 1 MiB limit', 2)
  doc = json.loads(raw_body.decode('utf-8'))
  schema_type = str(doc.get('schemaType') or 'AVRO').upper()
  schema_json = doc.get('schema')
  schema_obj = (
      json.loads(schema_json) if isinstance(schema_json, str) else schema_json)
  if schema_type == 'AVRO':
    try:
      import fastavro
    except ImportError:
      raise TailError(
          'fastavro is required to decode Confluent Schema Registry Avro '
          "payloads; install: pip install 'fastavro'",
          EXIT_MISSING_PACKAGE) from None
    entry = {
        '_beam_schema_type': 'AVRO',
        'parsed': fastavro.parse_schema(schema_obj),
    }
  else:
    entry = {'_beam_schema_type': schema_type, 'parsed': schema_obj}
  cache[schema_id] = entry
  return entry


def _fetch_confluent_avro_schema(
    registry_url: str,
    schema_id: int,
    cache: dict[int, Any],
) -> Any:
  """Backwards-compatible helper returning a parsed `fastavro` schema."""
  entry = _fetch_confluent_schema_doc(registry_url, schema_id, cache)
  return entry['parsed']


def _decode_kafka_wire_message(
    raw_val: bytes,
    registry_url: str | None,
    schema_cache: dict[int, Any],
    basic_auth: str | None = None,
) -> tuple[Any, int | None, bool | None, str | None]:
  """Returns `(payload, schema_id, schema_valid, schema_error)`."""
  if len(raw_val) >= 5 and raw_val[0] == 0:
    schema_id = struct.unpack('>I', raw_val[1:5])[0]
    body_bytes = raw_val[5:]
    if registry_url:
      try:
        # Preserve backwards compatibility when tests mock `_fetch_confluent_avro_schema`.
        if getattr(
            _fetch_confluent_avro_schema, '__module__', '') != __name__:
          import fastavro
          avro_sc = _fetch_confluent_avro_schema(
              registry_url, schema_id, schema_cache)
          decoded = fastavro.schemaless_reader(io.BytesIO(body_bytes), avro_sc)
          return decoded, schema_id, True, None
        entry = _fetch_confluent_schema_doc(
            registry_url, schema_id, schema_cache, basic_auth=basic_auth)
        st = entry.get('_beam_schema_type', 'AVRO')
        if st == 'AVRO':
          import fastavro
          decoded = fastavro.schemaless_reader(
              io.BytesIO(body_bytes), entry['parsed'])
          return decoded, schema_id, True, None
        if st == 'JSON':
          decoded_json = json.loads(body_bytes.decode('utf-8'))
          try:
            import jsonschema  # type: ignore
            jsonschema.validate(instance=decoded_json, schema=entry['parsed'])
            return decoded_json, schema_id, True, None
          except ImportError:
            return decoded_json, schema_id, None, None
          except Exception as val_exc:  # pylint: disable=broad-except
            return decoded_json, schema_id, False, str(val_exc).splitlines()[0]
      except TailError:
        raise
      except Exception:  # pylint: disable=broad-except
        pass
    # Even without registry_url, Confluent JSON Schema wire payloads are UTF-8 JSON after byte 5!
    try:
      txt_body = body_bytes.decode('utf-8').strip()
      if txt_body and txt_body[0] in ('{', '['):
        return json.loads(txt_body), schema_id, None, None
    except Exception:  # pylint: disable=broad-except
      pass
    return _decode_payload_bytes(raw_val), schema_id, None, None
  return _decode_payload_bytes(raw_val), None, None, None


def _tail_kafka(
    ep: uris.ParsedEndpoint,
    *,
    limit: int | None,
    duration_sec: float | None,
    follow: bool,
    byte_budget: int,
    on_record: RecordCallback | None,
) -> dict[str, Any]:
  bootstrap = ep.config['bootstrap_servers']
  topic = ep.config['topic']
  registry_url = ep.config.get('confluent_schema_registry_url')
  sr_auth = (
      ep.config.get('schema_registry_basic_auth') or
      ep.config.get('schema_registry_user_info') or
      os.environ.get('BEAM_SCHEMA_REGISTRY_USER_INFO'))
  want_raw = bool(
      ep.format in ('raw', 'bytes', 'hex') or ep.config.get('raw_bytes'))
  no_group = bool(ep.config.get('no_group'))
  schema_cache: dict[int, Any] = {}
  try:
    import confluent_kafka  # type: ignore
  except ImportError:
    raise TailError(
        'confluent-kafka is required for local kafka:// tailing; '
        "install: pip install 'confluent-kafka'",
        EXIT_MISSING_PACKAGE) from None

  extra_consumer_cfg = ep.config.get('consumer_config')
  offset_reset = str(
      ep.config.get('auto_offset_reset_config') or
      ('latest' if follow else 'earliest'))
  consumer_cfg: dict[str, Any] = {
      'bootstrap.servers': bootstrap,
      'socket.timeout.ms': 5000,
  }
  if isinstance(extra_consumer_cfg, dict):
    for k, v in extra_consumer_cfg.items():
      if k not in ('group.id', 'enable.auto.commit'):
        consumer_cfg[str(k)] = v
  # Always enforce an ephemeral non-committing `beam-tail-<uuid>` consumer group.
  consumer_cfg['group.id'] = f'beam-tail-{uuid.uuid4().hex[:12]}'
  consumer_cfg['enable.auto.commit'] = False
  consumer_cfg['auto.offset.reset'] = offset_reset

  consumer = confluent_kafka.Consumer(consumer_cfg)
  rows: list[dict[str, Any]] = []
  scanned_bytes = 0
  truncated_budget = False
  interrupted = False
  try:
    if no_group and hasattr(consumer, 'assign') and hasattr(
        confluent_kafka, 'TopicPartition'):
      off_const = (
          getattr(confluent_kafka, 'OFFSET_END', -1)
          if offset_reset == 'latest' else
          getattr(confluent_kafka, 'OFFSET_BEGINNING', -2))
      part_ids = [0]
      if hasattr(consumer, 'list_topics'):
        meta = consumer.list_topics(topic=topic, timeout=5.0)
        t_meta = (getattr(meta, 'topics', None) or {}).get(topic)
        if t_meta is not None and getattr(t_meta, 'partitions', None):
          part_ids = sorted(t_meta.partitions.keys())
      consumer.assign([
          confluent_kafka.TopicPartition(topic, pid, off_const)
          for pid in part_ids
      ])
    else:
      consumer.subscribe([topic])
    deadline = (
        time.monotonic() + duration_sec if duration_sec is not None else None)
    try:
      while True:
        if limit is not None and len(rows) >= limit:
          break
        if deadline is not None and time.monotonic() >= deadline:
          break
        poll_wait = (
            min(0.5, max(0.05, deadline - time.monotonic()))
            if deadline is not None else 0.5)
        msg = consumer.poll(timeout=poll_wait)
        if msg is None or msg.error():
          continue
        raw_val = bytes(msg.value() or b'')
        if scanned_bytes + len(raw_val) > byte_budget:
          truncated_budget = True
          break
        scanned_bytes += len(raw_val)
        raw_key = msg.key()
        decoded_payload, schema_id, schema_valid, schema_err = (
            _decode_kafka_wire_message(
                raw_val,
                registry_url,
                schema_cache,
                basic_auth=str(sr_auth) if sr_auth else None))
        rec_out: dict[str, Any] = {
            'partition': msg.partition(),
            'offset': msg.offset(),
            'key': (
                raw_key.decode('utf-8', errors='replace')
                if raw_key else None),
            'payload': decoded_payload,
        }
        if schema_id is not None:
          rec_out['schema_id'] = schema_id
        if schema_valid is not None:
          rec_out['schema_valid'] = schema_valid
        if schema_err is not None:
          rec_out['schema_error'] = schema_err
        if want_raw:
          rec_out['size_bytes'] = len(raw_val)
          rec_out['raw_hex'] = raw_val[:256].hex()
        if hasattr(msg, 'timestamp'):
          try:
            ts_pair = msg.timestamp()
            if isinstance(ts_pair, tuple) and len(ts_pair) == 2 and ts_pair[0] > 0:
              rec_out['timestamp_ms'] = int(ts_pair[1])
          except Exception:  # pylint: disable=broad-except
            pass
        _emit_record(rows, rec_out, on_record)
    except KeyboardInterrupt:
      if not follow:
        raise
      interrupted = True
  finally:
    consumer.close()

  hit_limit = bool(limit is not None and len(rows) >= limit)
  return {
      'uri': ep.raw,
      'kind': 'kafka',
      'mode': 'uncommitted_follow' if follow else 'uncommitted_ephemeral_consumer',
      'follow': bool(follow),
      'interrupted': interrupted,
      'non_destructive': True,
      'sampled_count': len(rows),
      'scanned_bytes': scanned_bytes,
      'untrusted_data': output.wrap_untrusted_rows(
          rows, truncated=truncated_budget or hit_limit),
  }


def _list_watch_targets(
    location: str, default_project: str | None) -> list[tuple[str, int]]:
  """Lists `(uri_or_path, size_bytes)` for local files/globs or `gs://`."""
  if location.startswith('gs://'):
    try:
      from google.cloud import storage
    except ImportError as e:
      raise TailError(
          f'google-cloud-storage is required to tail {location}: {e}',
          EXIT_MISSING_PACKAGE) from None
    without_scheme = location[len('gs://'):]
    parts = without_scheme.split('/', 1)
    bucket_name = parts[0]
    raw_prefix = parts[1] if len(parts) > 1 else ''
    # Strip trailing wildcard patterns like `*.jsonl` or `*` into GCS prefix.
    prefix = raw_prefix.split('*', 1)[0]
    has_glob = '*' in raw_prefix or '?' in raw_prefix
    client = (
        storage.Client(project=default_project)
        if default_project else storage.Client())
    items: list[tuple[str, int]] = []
    import fnmatch
    for blob in client.list_blobs(bucket_name, prefix=prefix):
      if blob.name.endswith('/'):
        continue
      if has_glob and not fnmatch.fnmatch(blob.name, raw_prefix):
        continue
      if not has_glob and raw_prefix and not raw_prefix.endswith('/'):
        # Exact file OR shard prefix (`out.jsonl-00000-of-00001`).
        if blob.name != raw_prefix and not blob.name.startswith(
            raw_prefix + '-'):
          continue
      items.append((f'gs://{bucket_name}/{blob.name}', int(blob.size or 0)))
    items.sort(key=lambda kv: kv[0])
    return items

  p = Path(location)
  if p.is_dir():
    matches = [
        str(child) for child in sorted(p.iterdir()) if child.is_file()
    ]
  elif any(ch in location for ch in ('*', '?', '[')):
    matches = sorted(glob.glob(location))
  elif p.is_file():
    matches = [str(p)]
  else:
    matches = sorted(glob.glob(location + '-*-of-*'))

  out: list[tuple[str, int]] = []
  for m in matches:
    try:
      st = os.stat(m)
      if os.path.isfile(m):
        out.append((m, int(st.st_size)))
    except OSError:
      continue
  return out


def _read_appended_bytes(
    item_uri: str,
    offset: int,
    default_project: str | None,
    *,
    max_bytes: int = 64 * 1024 * 1024,
) -> bytes:
  cap = max(1, int(max_bytes))
  if item_uri.startswith('gs://'):
    from google.cloud import storage
    without_scheme = item_uri[len('gs://'):]
    bucket_name, blob_name = without_scheme.split('/', 1)
    client = (
        storage.Client(project=default_project)
        if default_project else storage.Client())
    blob = client.bucket(bucket_name).blob(blob_name)
    return blob.download_as_bytes(start=offset, end=offset + cap - 1)

  with open(item_uri, 'rb') as f:
    f.seek(offset)
    return f.read(cap)


def _ensure_csv_field_size_limit() -> None:
  target = 16 * 1024 * 1024
  try:
    if csv.field_size_limit() >= target:
      return
  except Exception:  # pylint: disable=broad-except
    pass
  limit = target
  while limit >= 131072:
    try:
      csv.field_size_limit(limit)
      return
    except OverflowError:
      limit //= 2


def _parse_text_lines_from_chunk(
    item_uri: str,
    chunk_text: str,
    csv_headers: dict[str, list[str]],
) -> list[Any]:
  if not chunk_text or not chunk_text.strip():
    return []
  lower = item_uri.lower()
  is_csv = lower.endswith('.csv') or '.csv-' in lower
  if is_csv:
    _ensure_csv_field_size_limit()
    if item_uri not in csv_headers:
      dict_reader = csv.DictReader(io.StringIO(chunk_text))
      if dict_reader.fieldnames:
        csv_headers[item_uri] = list(dict_reader.fieldnames)
      out_rows: list[dict[str, Any]] = []
      for raw_d in dict_reader:
        clean_d = {str(k): v for k, v in raw_d.items() if k is not None}
        if any(val for val in clean_d.values()):
          out_rows.append(clean_d)
      return out_rows
    reader = [
        row for row in csv.reader(io.StringIO(chunk_text))
        if any(cell.strip() for cell in row)
    ]
    if not reader:
      return []
    hdr = csv_headers[item_uri]
    return [dict(zip(hdr, row)) for row in reader]

  lines = [ln for ln in chunk_text.splitlines() if ln.strip()]
  if not lines:
    return []
  parsed: list[Any] = []
  for ln in lines:
    stripped = ln.strip()
    if stripped.startswith('{') or stripped.startswith('['):
      try:
        parsed.append(json.loads(stripped))
        continue
      except ValueError:
        pass
    parsed.append({'line': stripped, 'source': item_uri})
  return parsed


def _tail_files_follow(
    location: str,
    *,
    limit: int | None,
    duration_sec: float | None,
    poll_interval: float,
    byte_budget: int,
    default_project: str | None,
    on_record: RecordCallback | None,
) -> dict[str, Any]:
  """Follows local or `gs://` files/directories as lines or shards land (`-f`)."""
  rows: list[dict[str, Any]] = []
  scanned_bytes = 0
  truncated_budget = False
  interrupted = False
  csv_headers: dict[str, list[str]] = {}

  # Snapshot existing files at their current end-of-file (just like `tail -f`
  # watches newly appended lines and newly created shards). For CSV files that
  # already exist, capture line 0 header so subsequent appended rows map cleanly.
  offsets: dict[str, int] = {}
  for uri_item, sz in _list_watch_targets(location, default_project):
    offsets[uri_item] = sz
    lower = uri_item.lower()
    if sz > 0 and (lower.endswith('.csv') or '.csv-' in lower):
      try:
        _ensure_csv_field_size_limit()
        head_bytes = _read_appended_bytes(
            uri_item, 0, default_project, max_bytes=65536)
        head_text = head_bytes.decode('utf-8', errors='replace')
        hdr_row = next(csv.reader(io.StringIO(head_text)), None)
        if hdr_row:
          csv_headers[uri_item] = hdr_row
      except Exception:  # pylint: disable=broad-except
        pass

  deadline = (
      time.monotonic() + duration_sec if duration_sec is not None else None)
  eff_poll = max(0.1, poll_interval)

  try:
    while True:
      if limit is not None and len(rows) >= limit:
        break
      if deadline is not None and time.monotonic() >= deadline:
        break

      targets = _list_watch_targets(location, default_project)
      saw_data = False
      for uri_item, sz in targets:
        prev_off = offsets.get(uri_item, 0)
        if sz < prev_off and not uri_item.startswith('gs://'):
          # Handle copytruncate / rotated local log files cleanly.
          prev_off = 0
          offsets[uri_item] = 0
        if sz <= prev_off:
          if sz == 0:
            offsets[uri_item] = 0
          continue
        lower = uri_item.lower()
        is_binary_columnar = (
            lower.endswith('.parquet') or '.parquet-' in lower or
            lower.endswith('.avro') or '.avro-' in lower)
        if is_binary_columnar and prev_off == 0:
          # Newly landed Parquet/Avro shard: sample rows via `inspect_cmd`!
          rem = None if limit is None else max(1, limit - len(rows))
          insp = inspect_cmd.run_inspect(
              uri_item,
              sample_count=min(rem or 50, 50),
              max_scanned_bytes=str(max(1, byte_budget - scanned_bytes)),
              default_project=default_project)
          offsets[uri_item] = sz
          for r in (insp.get('untrusted_data') or {}).get('rows', ()):
            saw_data = True
            _emit_record(rows, r, on_record)
            if limit is not None and len(rows) >= limit:
              break
        else:
          rem_budget = max(1, byte_budget - scanned_bytes)
          delta = _read_appended_bytes(
              uri_item, prev_off, default_project, max_bytes=rem_budget + 1)
          if not delta:
            continue
          if len(delta) > rem_budget:
            truncated_budget = True
            break
          # Only advance up to the last complete newline so partial writes are
          # re-read once completed.
          last_nl = delta.rfind(b'\n')
          if last_nl == -1:
            # On GCS objects (immutable once closed), process full object.
            if not uri_item.startswith('gs://'):
              continue
            complete = delta
            offsets[uri_item] = prev_off + len(delta)
          else:
            complete = delta[:last_nl + 1]
            offsets[uri_item] = prev_off + last_nl + 1

          if scanned_bytes + len(complete) > byte_budget:
            truncated_budget = True
            break
          scanned_bytes += len(complete)
          recs = _parse_text_lines_from_chunk(
              uri_item,
              complete.decode('utf-8', errors='replace'),
              csv_headers)
          for r in recs:
            saw_data = True
            _emit_record(rows, r, on_record)
            if limit is not None and len(rows) >= limit:
              break

        if truncated_budget or (limit is not None and len(rows) >= limit):
          break

      if truncated_budget or (limit is not None and len(rows) >= limit):
        break
      if not saw_data:
        sleep_s = (
            min(eff_poll, max(0.05, deadline - time.monotonic()))
            if deadline is not None else eff_poll)
        time.sleep(sleep_s)
  except KeyboardInterrupt:
    interrupted = True

  hit_limit = bool(limit is not None and len(rows) >= limit)
  return {
      'uri': location,
      'kind': 'file',
      'mode': 'file_follow',
      'follow': True,
      'interrupted': interrupted,
      'non_destructive': True,
      'sampled_count': len(rows),
      'scanned_bytes': scanned_bytes,
      'untrusted_data': output.wrap_untrusted_rows(
          rows, truncated=truncated_budget or hit_limit),
  }


def _tail_lakehouse_follow(
    ep: uris.ParsedEndpoint,
    *,
    limit: int | None,
    duration_sec: float | None,
    poll_interval: float,
    byte_budget: int,
    default_project: str | None,
    on_record: RecordCallback | None,
) -> dict[str, Any]:
  """Polls `delta://` commits or `iceberg://` snapshots in `-f` follow mode."""
  import pyarrow.parquet as pq
  from . import lakehouse

  rows: list[dict[str, Any]] = []
  scanned_bytes = 0
  truncated_budget = False
  interrupted = False
  eff_poll = max(0.1, poll_interval)
  deadline = (
      time.monotonic() + duration_sec if duration_sec is not None else None)

  last_delta_ver: int = -1
  last_iceberg_snap: int | None = None
  iceberg_dir: str | None = None

  if ep.kind == 'delta':
    try:
      last_delta_ver = int(lakehouse.parse_delta_log(ep.config['table'])['version'])
    except FileNotFoundError:
      last_delta_ver = -1
  else:
    iceberg_dir = lakehouse.resolve_iceberg_table_dir(
        ep.config, default_project=default_project)
    try:
      st = lakehouse.load_iceberg_state(iceberg_dir)
      last_iceberg_snap = st['current_snapshot_id']
    except FileNotFoundError:
      last_iceberg_snap = None

  try:
    while True:
      if limit is not None and len(rows) >= limit:
        break
      if deadline is not None and time.monotonic() >= deadline:
        break

      saw_data = False
      if ep.kind == 'delta':
        try:
          cur_snap = lakehouse.parse_delta_log(ep.config['table'])
          cur_ver = int(cur_snap['version'])
        except FileNotFoundError:
          cur_ver = -1
        if cur_ver > last_delta_ver:
          base_fields, actions = lakehouse.list_delta_cdc_actions(
              ep.config['table'],
              start_version=last_delta_ver + 1,
              end_version=cur_ver)
          last_delta_ver = cur_ver
          type_map = {
              f['name']: lakehouse._delta_type_to_py_type(f['type'])
              for f in base_fields
          }
          for act in actions:
            pvals = act.get('partition_values') or {}
            ver = int(act['version'])
            with lakehouse._fs_open_read(act['path']) as fobj:
              pf = pq.ParquetFile(fobj)
              for batch in pf.iter_batches():
                for raw_r in batch.to_pylist():
                  rec = {
                      f['name']: lakehouse._coerce_scalar_for_py(
                          raw_r.get(f['name'], pvals.get(f['name'])),
                          type_map[f['name']])
                      for f in base_fields
                  }
                  if act['kind'] == 'cdc_file':
                    rec['_change_type'] = str(
                        raw_r.get('_change_type', 'insert'))
                  elif act['kind'] == 'removed_file':
                    rec['_change_type'] = 'DELETE'
                  else:
                    rec['_change_type'] = 'INSERT'
                  rec['_commit_version'] = int(raw_r.get('_commit_version', ver))
                  est_sz = len(json.dumps(rec, default=str))
                  if scanned_bytes + est_sz > byte_budget:
                    truncated_budget = True
                    break
                  scanned_bytes += est_sz
                  saw_data = True
                  _emit_record(rows, rec, on_record)
                  if limit is not None and len(rows) >= limit:
                    break
                if truncated_budget or (limit is not None and len(rows) >= limit):
                  break
            if truncated_budget or (limit is not None and len(rows) >= limit):
              break
      else:
        assert iceberg_dir is not None
        try:
          st = lakehouse.load_iceberg_state(iceberg_dir)
          cur_snap_id = st['current_snapshot_id']
        except FileNotFoundError:
          cur_snap_id = None
        if cur_snap_id is not None and cur_snap_id != last_iceberg_snap:
          _, changes = lakehouse.list_iceberg_cdc_changes(
              iceberg_dir,
              from_snapshot=last_iceberg_snap,
              to_snapshot=cur_snap_id)
          last_iceberg_snap = cur_snap_id
          for rec in changes:
            est_sz = len(json.dumps(rec, default=str))
            if scanned_bytes + est_sz > byte_budget:
              truncated_budget = True
              break
            scanned_bytes += est_sz
            saw_data = True
            _emit_record(rows, rec, on_record)
            if limit is not None and len(rows) >= limit:
              break

      if truncated_budget or (limit is not None and len(rows) >= limit):
        break
      if not saw_data:
        sleep_s = (
            min(eff_poll, max(0.05, deadline - time.monotonic()))
            if deadline is not None else eff_poll)
        time.sleep(sleep_s)
  except KeyboardInterrupt:
    interrupted = True

  hit_limit = bool(limit is not None and len(rows) >= limit)
  return {
      'uri': ep.raw,
      'kind': ep.kind,
      'mode': 'lakehouse_commit_follow',
      'follow': True,
      'interrupted': interrupted,
      'non_destructive': True,
      'sampled_count': len(rows),
      'scanned_bytes': scanned_bytes,
      'untrusted_data': output.wrap_untrusted_rows(
          rows, truncated=truncated_budget or hit_limit),
  }
