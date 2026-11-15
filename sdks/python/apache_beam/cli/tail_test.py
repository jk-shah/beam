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

import contextlib
import datetime
import io
import os
import shutil
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

from apache_beam.cli import tail


class TailCommandsTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp()
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.beam_home = os.path.join(self.tmp, 'beam_home')
    self._env_patch = mock.patch.dict(
        os.environ, {'BEAM_HOME': self.beam_home}, clear=False
    )
    self._env_patch.start()
    self.addCleanup(self._env_patch.stop)

  def test_interactive_tail_formatter_color_and_sanitization(self):
    for color in (True, False):
      fmt = tail.InteractiveTailFormatter(
          'kafka://broker:9092/events\x1b[31m', follow=True, use_color=color
      )
      stderr = io.StringIO()
      stdout = io.StringIO()
      with (
          contextlib.redirect_stderr(stderr),
          contextlib.redirect_stdout(stdout),
      ):
        fmt.render_start_banner()
        fmt.on_stream_record(
            {
                'id': 1,
                'meta': {'env': 'prod'},
                'msg': 'x' * 80 + '\x1b]0;pwn\x07',
            },
            1,
        )
        fmt.on_stream_record({'id': 2, 'msg': 'second'}, 2)
        fmt.render_exit_summary(interrupted=color)
      self.assertIn('Beam Stream', stderr.getvalue())
      self.assertIn('Stream Summary', stderr.getvalue())
      self.assertNotIn('\x1b]0;pwn', stdout.getvalue())

  def test_local_tail_leases_and_pubsub_gc_protection(self):
    now = time.time()
    tail._write_local_tail_lease('beam-cli-follow-60000000-live', int(now) + 600)
    self.assertTrue(
        tail._is_local_tail_pid_alive('beam-cli-follow-60000000-live', now)
    )
    tail._remove_local_tail_lease('beam-cli-follow-60000000-live')
    self.assertFalse(
        tail._is_local_tail_pid_alive('beam-cli-follow-60000000-live', now)
    )

    # Expired lease file is unlinked automatically
    tail._write_local_tail_lease('beam-cli-peek-60000000-old', int(now) - 3600)
    self.assertFalse(
        tail._is_local_tail_pid_alive('beam-cli-peek-60000000-old', now)
    )

    old_hex = format(int(now) - 3600, 'x')
    deleted: list[str] = []

    class FakeSubscriber:
      def get_subscription(self, request):
        name = request['subscription']
        if 'activelease' in name:
          return types.SimpleNamespace(
              labels={'beam_mode': 'follow', 'beam_lease_until': str(int(now) + 900)}
          )
        if 'expiredlease' in name:
          return types.SimpleNamespace(
              labels={'beam_mode': 'peek', 'beam_lease_until': str(int(now) - 900)}
          )
        raise RuntimeError('IAM denied get_subscription')

      def delete_subscription(self, request):
        deleted.append(request['subscription'])

    fake_pub = types.SimpleNamespace(
        list_topic_subscriptions=lambda request: [
            f'projects/p/subscriptions/user-prod-sub',
            f'projects/p/subscriptions/beam-cli-follow-{old_hex}-activelease',
            f'projects/p/subscriptions/beam-cli-peek-{old_hex}-expiredlease',
            f'projects/p/subscriptions/beam-cli-follow-{old_hex}-nolabel24h',
            f'projects/p/subscriptions/beam-cli-peek-{old_hex}-nolabelpeek',
        ]
    )
    tail._gc_stale_peek_subscriptions(
        FakeSubscriber(), fake_pub, 'projects/p/topics/t', max_age_sec=900.0
    )
    self.assertEqual(
        deleted,
        [
            f'projects/p/subscriptions/beam-cli-peek-{old_hex}-expiredlease',
            f'projects/p/subscriptions/beam-cli-peek-{old_hex}-nolabelpeek',
        ],
    )

  def test_tail_pubsub_ephemeral_topic_and_non_acking_subscription(self):
    fake_exc_mod = types.ModuleType('google.api_core.exceptions')

    class FakeDeadlineExceeded(Exception):
      pass

    class FakeInvalidArgument(Exception):
      pass

    fake_exc_mod.DeadlineExceeded = FakeDeadlineExceeded
    fake_exc_mod.InvalidArgument = FakeInvalidArgument

    acked: list[list[str]] = []
    nacked: list[list[str]] = []
    deleted_subs: list[str] = []

    class FakeSubClient:
      def __init__(self):
        self._pulls = 0

      def create_subscription(self, request):
        self.created = request

      def update_subscription(self, request):
        self.updated = request

      def pull(self, request, timeout=None):
        del request, timeout
        self._pulls += 1
        if self._pulls == 1:
          m1 = types.SimpleNamespace(
              ack_id='ack-1',
              message=types.SimpleNamespace(
                  message_id='m-1',
                  data=b'{"order_id": 10}',
                  publish_time=datetime.datetime(
                      2026, 11, 15, tzinfo=datetime.timezone.utc
                  ),
                  attributes={'src': 'web'},
              ),
          )
          # Duplicate message_id on subscription mode should be deduplicated
          m_dup = types.SimpleNamespace(
              ack_id='ack-2',
              message=types.SimpleNamespace(
                  message_id='m-1',
                  data=b'{"order_id": 10}',
                  publish_time=None,
                  attributes={},
              ),
          )
          return types.SimpleNamespace(received_messages=[m1, m_dup])
        raise FakeDeadlineExceeded()

      def acknowledge(self, request):
        acked.append(list(request['ack_ids']))

      def modify_ack_deadline(self, request):
        if request['ack_deadline_seconds'] == 0:
          nacked.append(list(request['ack_ids']))

      def delete_subscription(self, request):
        deleted_subs.append(request['subscription'])

      def close(self):
        pass

    fake_pubsub = types.ModuleType('google.cloud.pubsub_v1')
    fake_pubsub.SubscriberClient = FakeSubClient
    fake_pubsub.PublisherClient = lambda: types.SimpleNamespace(
        list_topic_subscriptions=lambda request: []
    )
    fake_gc = types.ModuleType('google.cloud')
    fake_gc.pubsub_v1 = fake_pubsub
    fake_api_core = types.ModuleType('google.api_core')
    fake_api_core.exceptions = fake_exc_mod

    with mock.patch.dict(
        sys.modules,
        {
            'google.cloud': fake_gc,
            'google.cloud.pubsub_v1': fake_pubsub,
            'google.api_core': fake_api_core,
            'google.api_core.exceptions': fake_exc_mod,
        },
    ):
      # 1. Topic peek -> ephemeral subscription created, ACKed, and deleted
      res_topic = tail.run_tail(
          'pubsub://projects/my-proj/topics/orders',
          limit=2,
          duration='1s',
      )
      self.assertEqual(res_topic['mode'], 'ephemeral_topic_subscription')
      self.assertEqual(res_topic['sampled_count'], 2)
      self.assertEqual(len(acked), 1)
      self.assertEqual(len(deleted_subs), 1)

      # 2. Existing subscription follow -> NEVER ACKs, deduplicates m-1, releases lease
      acked.clear()
      res_sub = tail.run_tail(
          'pubsub://projects/my-proj/subscriptions/shared-sub',
          limit=1,
          duration='0.3s',
          follow=True,
          poll_interval=0.05,
      )
      self.assertEqual(res_sub['mode'], 'subscription_nack_follow')
      self.assertEqual(res_sub['sampled_count'], 1)
      self.assertEqual(acked, [])
      self.assertTrue(len(nacked) >= 1)

  def test_tail_kafka_uncommitted_ephemeral_consumer(self):
    closed = []
    configs = []

    class FakeMsg:
      def error(self):
        return None

      def value(self):
        return b'{"event": "click"}'

      def key(self):
        return b'u-42'

      def partition(self):
        return 0

      def offset(self):
        return 99

    class FakeConsumer:
      def __init__(self, cfg):
        configs.append(cfg)
        self._n = 0

      def subscribe(self, topics):
        self.topics = topics

      def poll(self, timeout=0.5):
        del timeout
        self._n += 1
        return FakeMsg() if self._n == 1 else None

      def close(self):
        closed.append(True)

    fake_ck = types.ModuleType('confluent_kafka')
    fake_ck.Consumer = FakeConsumer
    with mock.patch.dict(sys.modules, {'confluent_kafka': fake_ck}):
      res = tail.run_tail(
          'kafka://localhost:9092/clicks',
          limit=1,
          duration='0.3s',
      )
    self.assertEqual(res['kind'], 'kafka')
    self.assertEqual(res['sampled_count'], 1)
    self.assertFalse(configs[0]['enable.auto.commit'])
    self.assertEqual(closed, [True])

  def test_tail_files_follow_csv_append_and_new_parquet_shard(self):
    watch_dir = os.path.join(self.tmp, 'stream_out')
    os.makedirs(watch_dir)
    csv_file = os.path.join(watch_dir, 'events.csv')
    with open(csv_file, 'w', encoding='utf-8') as f:
      f.write('id,status\n1,INIT\n')

    def writer():
      time.sleep(0.12)
      with open(csv_file, 'a', encoding='utf-8') as f:
        f.write('2,PAID\n')
      pq.write_table(
          pa.table({'id': [3], 'status': ['SHIPPED']}),
          os.path.join(watch_dir, 'part-00001.parquet'),
      )

    t = threading.Thread(target=writer, daemon=True)
    t.start()
    seen = []
    res = tail.run_tail(
        watch_dir,
        limit=2,
        duration='2s',
        follow=True,
        poll_interval=0.05,
        on_record=lambda r, idx: seen.append((idx, r)),
    )
    t.join(timeout=2)
    self.assertEqual(res['mode'], 'file_follow')
    self.assertEqual(res['sampled_count'], 2)
    self.assertEqual(len(seen), 2)

  def test_tail_lakehouse_follow_delta_and_iceberg_commits(self):
    # Iceberg snapshot commit follow
    ice_calls = {'n': 0}

    def fake_load_state(_tdir):
      ice_calls['n'] += 1
      return {'current_snapshot_id': 1 if ice_calls['n'] == 1 else 2}

    with (
        mock.patch(
            'apache_beam.cli.lakehouse.resolve_iceberg_table_dir',
            return_value=os.path.join(self.tmp, 'ice_tbl'),
        ),
        mock.patch(
            'apache_beam.cli.lakehouse.load_iceberg_state',
            side_effect=fake_load_state,
        ),
        mock.patch(
            'apache_beam.cli.lakehouse.list_iceberg_cdc_changes',
            return_value=([], [{'id': 7, '_change_type': 'INSERT'}]),
        ),
    ):
      res = tail.run_tail(
          f'iceberg://ns.tbl?warehouse={self.tmp}',
          limit=1,
          duration='1s',
          follow=True,
          poll_interval=0.05,
      )
    self.assertEqual(res['mode'], 'lakehouse_commit_follow')
    self.assertEqual(res['sampled_count'], 1)

    # Validation errors on -f with bigquery:// and bad --limit
    with self.assertRaises(tail.TailError):
      tail.run_tail('bigquery://proj.ds.tbl', follow=True)
    with self.assertRaises(tail.TailError):
      tail.run_tail(os.path.join(self.tmp, 'x.csv'), limit=0, follow=False)


if __name__ == '__main__':
  unittest.main()
