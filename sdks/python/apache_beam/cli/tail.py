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

"""Bounded, non-destructive stream & dataset peek (`beam tail`).

Guarantees:
1. `pubsub://projects/<p>/subscriptions/<s>`: pulls up to `--limit` messages
   within `--duration`, **never calls `acknowledge()`**, and resets
   `ack_deadline_seconds=0` so pulled messages remain immediately visible to
   downstream consumers with zero message loss!
2. `pubsub://projects/<p>/topics/<t>`: creates an ephemeral subscription
   `beam-cli-peek-<rand>`, garbage-collects stale `beam-cli-peek-*`
   subscriptions older than 15 minutes on the topic, pulls up to `--limit`
   messages published during `--duration`, and deletes the ephemeral
   subscription in a `finally:` block.
3. Enforces `--max-scanned-bytes` (default `64MB`) and wraps all sampled
   records inside `output.wrap_untrusted_rows(...)` (`untrusted_data`).
"""

import json
import time
import uuid
from typing import Any

from . import cost_guard
from . import inspect_cmd
from . import jobs
from . import output
from . import uris

EXIT_MISSING_PACKAGE = 4


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


def run_tail(
    location: str,
    *,
    limit: int = 5,
    duration: str = '5s',
    max_scanned_bytes: str = '64MB',
    default_project: str | None = None,
) -> dict[str, Any]:
  """Samples up to `limit` records from a stream or dataset without consuming."""
  if limit <= 0:
    raise TailError('--limit must be >= 1', 2)
  duration_sec = jobs.parse_duration_seconds(duration)
  byte_budget = cost_guard.parse_byte_size(max_scanned_bytes)

  try:
    ep = uris.parse_endpoint(
        location,
        is_read=True,
        default_project=default_project,
        allow_sniff=True)
  except uris.UriError as e:
    raise TailError(str(e), 2) from None

  if ep.kind == 'pubsub':
    return _tail_pubsub(
        ep,
        limit=limit,
        duration_sec=duration_sec,
        byte_budget=byte_budget,
        default_project=default_project)
  if ep.kind == 'kafka':
    return _tail_kafka(
        ep, limit=limit, duration_sec=duration_sec, byte_budget=byte_budget)
  if ep.kind in ('file', 'bigquery'):
    inspected = inspect_cmd.run_inspect(
        location,
        sample_count=limit,
        max_scanned_bytes=max_scanned_bytes,
        default_project=default_project)
    return {
        'uri': location,
        'kind': ep.kind,
        'mode': 'snapshot_peek',
        'non_destructive': True,
        'sampled_count': len(
            (inspected.get('untrusted_data') or {}).get('rows', ())),
        'scanned_bytes': inspected.get('estimated_bytes'),
        'untrusted_data': inspected.get('untrusted_data'),
    }
  raise TailError(
      f'beam tail does not support {ep.kind!r} endpoints yet; supported: '
      'pubsub://, kafka://, bigquery://, and file paths',
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
      # Subscription name format: beam-cli-peek-<epoch_hex>-<rand>
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
    limit: int,
    duration_sec: float,
    byte_budget: int,
    default_project: str | None,
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

  try:
    if topic_path and not sub_path:
      # Topic peek: create a short-lived ephemeral subscription and delete it
      # in `finally:` so no persistent subscription is left behind.
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

    deadline = time.monotonic() + duration_sec
    pending_ack_ids: list[str] = []

    while len(rows) < limit and time.monotonic() < deadline:
      remaining = max(0.2, deadline - time.monotonic())
      need = limit - len(rows)
      try:
        response = subscriber.pull(
            request={
                'subscription': sub_path,
                'max_messages': need,
            },
            timeout=min(remaining, 5.0))
      except gcp_exceptions.DeadlineExceeded:
        break

      received = list(getattr(response, 'received_messages', ()) or ())
      if not received:
        if not ephemeral_sub:
          # Existing subscription had 0 ready messages on pull -> return fast!
          break
        time.sleep(min(0.25, remaining))
        continue

      for rm in received:
        if not ephemeral_sub and rm.ack_id:
          pending_ack_ids.append(rm.ack_id)
        msg = rm.message
        raw_data = bytes(getattr(msg, 'data', b'') or b'')
        if scanned_bytes + len(raw_data) > byte_budget:
          truncated_budget = True
          break
        scanned_bytes += len(raw_data)
        pub_time = getattr(msg, 'publish_time', None)
        rows.append({
            'message_id': str(getattr(msg, 'message_id', '')),
            'publish_time': (
                pub_time.isoformat() if hasattr(pub_time, 'isoformat') else
                str(pub_time) if pub_time else None),
            'attributes': dict(getattr(msg, 'attributes', {}) or {}),
            'payload': _decode_payload_bytes(raw_data),
        })
        if len(rows) >= limit:
          break
      if truncated_budget:
        break

    # Non-destructive guarantee on existing subscriptions: NEVER acknowledge,
    # and release the lease immediately (`ack_deadline_seconds=0`) so real
    # consumers see the messages without waiting for ack deadline expiry!
    if not ephemeral_sub and pending_ack_ids and sub_path:
      try:
        subscriber.modify_ack_deadline(
            request={
                'subscription': sub_path,
                'ack_ids': pending_ack_ids,
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

  return {
      'uri': ep.raw,
      'kind': 'pubsub',
      'mode': 'ephemeral_topic_subscription'
      if ephemeral_sub else 'subscription_nack_peek',
      'non_destructive': True,
      'sampled_count': len(rows),
      'scanned_bytes': scanned_bytes,
      'untrusted_data': output.wrap_untrusted_rows(
          rows, truncated=truncated_budget or len(rows) >= limit),
  }


def _tail_kafka(
    ep: uris.ParsedEndpoint,
    *,
    limit: int,
    duration_sec: float,
    byte_budget: int,
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
      'auto.offset.reset': 'earliest',
  })
  rows: list[dict[str, Any]] = []
  scanned_bytes = 0
  truncated_budget = False
  try:
    consumer.subscribe([topic])
    deadline = time.monotonic() + duration_sec
    while len(rows) < limit and time.monotonic() < deadline:
      msg = consumer.poll(timeout=min(0.5, max(0.05, deadline - time.monotonic())))
      if msg is None:
        continue
      if msg.error():
        continue
      raw_val = bytes(msg.value() or b'')
      if scanned_bytes + len(raw_val) > byte_budget:
        truncated_budget = True
        break
      scanned_bytes += len(raw_val)
      raw_key = msg.key()
      rows.append({
          'partition': msg.partition(),
          'offset': msg.offset(),
          'key': raw_key.decode('utf-8', errors='replace') if raw_key else None,
          'payload': _decode_payload_bytes(raw_val),
      })
  finally:
    consumer.close()

  return {
      'uri': ep.raw,
      'kind': 'kafka',
      'mode': 'uncommitted_ephemeral_consumer',
      'non_destructive': True,
      'sampled_count': len(rows),
      'scanned_bytes': scanned_bytes,
      'untrusted_data': output.wrap_untrusted_rows(
          rows, truncated=truncated_budget or len(rows) >= limit),
  }
