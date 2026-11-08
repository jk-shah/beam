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
import glob
import io
import json
import os
from pathlib import Path
import time
from typing import Any
from typing import Callable
import uuid

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
    ep = uris.parse_endpoint(
        location,
        is_read=True,
        options=parsed_opts,
        default_project=default_project,
        allow_sniff=True)
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
  if ep.kind in ('file', 'bigquery', 'delta', 'iceberg'):
    if follow and ep.kind == 'bigquery':
      raise TailError(
          'beam tail -f (--follow) is for streaming/file/lakehouse endpoints '
          '(pubsub://, kafka://, delta://, iceberg://, local paths, gs://); '
          'omit -f to peek at bigquery:// tables.',
          2)
    eff_limit = 5 if limit is None else limit
    inspected = inspect_cmd.run_inspect(
        location,
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


def _gc_stale_peek_subscriptions(
    subscriber, publisher, topic_path: str, max_age_sec: float = 900.0) -> None:
  """Best-effort cleanup of orphaned `beam-cli-peek-*` subscriptions."""
  now = time.time()
  try:
    for sub_path in publisher.list_topic_subscriptions(
        request={'topic': topic_path}):
      sub_name = sub_path.rsplit('/', 1)[-1]
      if not sub_name.startswith('beam-cli-peek-'):
        continue
      parts = sub_name.split('-')
      if len(parts) >= 5:
        try:
          created_ts = int(parts[3], 16)
          if (now - created_ts) > max_age_sec:
            subscriber.delete_subscription(request={'subscription': sub_path})
        except Exception:  # pylint: disable=broad-except
          pass
  except Exception:  # pylint: disable=broad-except
    pass


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

  try:
    if topic_path and not sub_path:
      parts = topic_path.split('/')
      proj = parts[1] if len(parts) >= 4 else default_project
      if not proj:
        raise TailError(
            f'cannot determine GCP project for topic {topic_path!r}', 2)
      publisher = pubsub_v1.PublisherClient()
      _gc_stale_peek_subscriptions(subscriber, publisher, topic_path)
      epoch_hex = format(int(time.time()), 'x')
      sub_id = f'beam-cli-peek-{epoch_hex}-{uuid.uuid4().hex[:8]}'
      ephemeral_sub = f'projects/{proj}/subscriptions/{sub_id}'
      subscriber.create_subscription(
          request={
              'name': ephemeral_sub,
              'topic': topic_path,
              'ack_deadline_seconds': 10,
              'message_retention_duration': {'seconds': 600},
              'expiration_policy': {'ttl': {'seconds': 86400}},
          })
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

        remaining = (
            max(0.2, deadline - time.monotonic())
            if deadline is not None else 5.0)
        batch_max = (
            min(20, limit - len(rows)) if limit is not None else 20)
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

        ephemeral_ack_ids: list[str] = []
        cycle_nack_ids: list[str] = []
        for rm in received:
          if ephemeral_sub:
            if rm.ack_id:
              ephemeral_ack_ids.append(rm.ack_id)
          else:
            if rm.ack_id:
              cycle_nack_ids.append(rm.ack_id)
              pending_nack_ids.append(rm.ack_id)

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
                    'ack_ids': cycle_nack_ids,
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

    if not ephemeral_sub and pending_nack_ids and sub_path:
      try:
        subscriber.modify_ack_deadline(
            request={
                'subscription': sub_path,
                'ack_ids': pending_nack_ids,
                'ack_deadline_seconds': 0,
            })
      except Exception:  # pylint: disable=broad-except
        pass
  finally:
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
  try:
    import confluent_kafka  # type: ignore
  except ImportError:
    raise TailError(
        'confluent-kafka is required for local kafka:// tailing; '
        "install: pip install 'confluent-kafka'",
        EXIT_MISSING_PACKAGE) from None

  consumer = confluent_kafka.Consumer({
      'bootstrap.servers': bootstrap,
      'group.id': f'beam-cli-peek-{uuid.uuid4().hex[:10]}',
      'enable.auto.commit': False,
      'auto.offset.reset': 'latest' if follow else 'earliest',
  })
  rows: list[dict[str, Any]] = []
  scanned_bytes = 0
  truncated_budget = False
  interrupted = False
  try:
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
        _emit_record(
            rows,
            {
                'partition': msg.partition(),
                'offset': msg.offset(),
                'key': (
                    raw_key.decode('utf-8', errors='replace')
                    if raw_key else None),
                'payload': _decode_payload_bytes(raw_val),
            },
            on_record)
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
    item_uri: str, offset: int, default_project: str | None) -> bytes:
  if item_uri.startswith('gs://'):
    from google.cloud import storage
    without_scheme = item_uri[len('gs://'):]
    bucket_name, blob_name = without_scheme.split('/', 1)
    client = (
        storage.Client(project=default_project)
        if default_project else storage.Client())
    blob = client.bucket(bucket_name).blob(blob_name)
    return blob.download_as_bytes(start=offset)

  with open(item_uri, 'rb') as f:
    f.seek(offset)
    return f.read()


def _parse_text_lines_from_chunk(
    item_uri: str,
    chunk_text: str,
    csv_headers: dict[str, list[str]],
) -> list[Any]:
  lower = item_uri.lower()
  is_csv = lower.endswith('.csv') or '.csv-' in lower
  lines = [ln for ln in chunk_text.splitlines() if ln.strip()]
  if not lines:
    return []
  if is_csv:
    reader = list(csv.reader(io.StringIO('\n'.join(lines))))
    if not reader:
      return []
    if item_uri not in csv_headers:
      csv_headers[item_uri] = reader[0]
      reader = reader[1:]
    hdr = csv_headers[item_uri]
    return [dict(zip(hdr, row)) for row in reader]

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
        head_bytes = _read_appended_bytes(uri_item, 0, default_project)[:4096]
        first_line = head_bytes.decode('utf-8', errors='replace').splitlines()
        if first_line:
          hdr_rows = list(csv.reader([first_line[0]]))
          if hdr_rows:
            csv_headers[uri_item] = hdr_rows[0]
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
        if sz <= prev_off:
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
          delta = _read_appended_bytes(uri_item, prev_off, default_project)
          if not delta:
            continue
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

