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
import io
import json
import os
import shutil
import tempfile
import types
import unittest
from unittest import mock

from apache_beam.cli import jobs


class JobsLifecycleTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp()
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.beam_home = os.path.join(self.tmp, 'beam_home')
    self._env_patch = mock.patch.dict(
        os.environ, {'BEAM_HOME': self.beam_home}, clear=False
    )
    self._env_patch.start()
    self.addCleanup(self._env_patch.stop)

  def test_duration_parsing_and_job_id_validation(self):
    self.assertAlmostEqual(jobs.parse_duration_seconds('500ms'), 0.5)
    self.assertAlmostEqual(jobs.parse_duration_seconds('30s'), 30.0)
    self.assertAlmostEqual(jobs.parse_duration_seconds('2m'), 120.0)
    self.assertAlmostEqual(jobs.parse_duration_seconds('1h'), 3600.0)
    self.assertAlmostEqual(jobs.parse_duration_seconds(15), 15.0)
    for bad in (-1, 0, '0s', 'forever', ''):
      with self.assertRaises(ValueError):
        jobs.parse_duration_seconds(bad)

    self.assertEqual(jobs.validate_job_id('2026-11-15_01-02_abc'), '2026-11-15_01-02_abc')
    for unsafe in ('', '../etc/passwd', 'a..b', 'job/slash', 'a' * 200):
      with self.assertRaises(jobs.JobError):
        jobs.validate_job_id(unsafe)

  def test_idempotency_ledger_validation_states_and_expiry(self):
    key = jobs.compute_idempotency_key(['pipe', 'a.csv', 'b.parquet'], 'hash1')
    rec = jobs.record_job_submission(
        'job-100',
        runner='DataflowRunner',
        state='JOB_STATE_RUNNING',
        command='pipe',
        project='my-proj',
        region='us-central1',
        idem_key=key,
        post_checks={'assert_counts': True},
    )
    self.assertEqual(rec['validation_state'], 'PENDING')
    # Active RUNNING submission hits ledger
    self.assertIsNotNone(jobs.lookup_recent_submission(key, ttl_seconds=60))
    # Expired TTL misses
    self.assertIsNone(jobs.lookup_recent_submission(key, ttl_seconds=-1))

    # DONE with pending post_checks must NOT short-circuit retries
    jobs.record_job_submission(
        'job-100',
        runner='DataflowRunner',
        state='JOB_STATE_DONE',
        command='pipe',
        project='my-proj',
        region='us-central1',
        idem_key=key,
        post_checks={'assert_counts': True},
    )
    self.assertIsNone(jobs.lookup_recent_submission(key, ttl_seconds=60))

    # Marked PASSED -> hits ledger; marked FAILED -> misses ledger
    jobs.mark_job_validation(
        'job-100',
        passed=True,
        exit_code=0,
        state='JOB_STATE_DONE',
        metrics={'rows_read': 5, 'rows_written': 5},
        outputs={'extra': 1},
    )
    self.assertIsNotNone(jobs.lookup_recent_submission(key, ttl_seconds=60))

    jobs.mark_job_validation(
        'job-100',
        passed=False,
        exit_code=1,
        error='conservation mismatch',
    )
    self.assertIsNone(jobs.lookup_recent_submission(key, ttl_seconds=60))

  def test_extract_counters_finalize_metrics_and_assert_counts(self):
    # Test MetricResults with both counters and distributions
    fake_counter = types.SimpleNamespace(
        key=types.SimpleNamespace(metric=types.SimpleNamespace(name='rows_read')),
        committed=10,
        attempted=10,
    )
    fake_sink = types.SimpleNamespace(
        key=types.SimpleNamespace(metric=types.SimpleNamespace(name='pre_sink_rows')),
        committed=None,
        attempted=8,
    )
    fake_dlq = types.SimpleNamespace(
        key=types.SimpleNamespace(metric=types.SimpleNamespace(name='dlq_rows')),
        committed=1,
        attempted=1,
    )
    fake_dupe = types.SimpleNamespace(
        key=types.SimpleNamespace(metric=types.SimpleNamespace(name='duplicates_dropped')),
        committed=1,
        attempted=1,
    )
    fake_dist = types.SimpleNamespace(
        key=types.SimpleNamespace(metric=types.SimpleNamespace(name='inference_latency_ms')),
        committed=types.SimpleNamespace(count=2, sum=50.0),
        attempted=None,
    )
    fake_mr = types.SimpleNamespace(
        query=lambda flt: {
            'counters': [fake_counter, fake_sink, fake_dlq, fake_dupe],
            'distributions': [fake_dist],
        }
    )
    wrapped_res = types.SimpleNamespace(metrics=lambda: fake_mr)
    counters = jobs.extract_beam_cli_counters(wrapped_res)
    self.assertEqual(counters['rows_read'], 10)
    self.assertEqual(counters['rows_written'], 8)
    self.assertEqual(counters['rows_filtered_out'], 1)
    self.assertEqual(counters['dlq_rows'], 1)
    self.assertEqual(counters['inference_latency_ms'], 25)
    self.assertIsNone(jobs.check_assert_counts(counters))
    self.assertIsNotNone(jobs.check_assert_counts({'rows_read': 10, 'rows_written': 7}))
    self.assertIsNotNone(jobs.check_assert_counts({}))

    # REST dict & proto object extraction in _extract_raw_counters_from_metrics_list
    rest_items = [
        {
            'name': {
                'origin': 'user',
                'name': 'rows_read',
                'context': {'namespace': 'beam_cli'},
            },
            'scalar': {'integerValue': '42'},
        },
        {
            'name': {
                'origin': 'user',
                'name': 'rows_read',
                'context': {'namespace': 'beam_cli', 'tentative': 'true'},
            },
            'scalar': 999,
        },
        types.SimpleNamespace(
            name=types.SimpleNamespace(
                origin='user',
                name='pre_sink_rows',
                context=types.SimpleNamespace(
                    additionalProperties=[
                        types.SimpleNamespace(key='namespace', value='beam_cli')
                    ]
                ),
            ),
            scalar=types.SimpleNamespace(integer_value=42, double_value=None),
        ),
    ]
    extracted = jobs._extract_raw_counters_from_metrics_list(rest_items)
    self.assertEqual(extracted, {'rows_read': 42, 'pre_sink_rows': 42})

  def test_dataflow_poller_rest_and_apiclient_fallback(self):
    poller = jobs._DataflowPoller('my-proj', 'us-central1')
    poller._opener = types.SimpleNamespace(
        open=lambda req, timeout=20: contextlib.nullcontext(
            io.BytesIO(
                json.dumps(
                    {'metrics': [{'name': {'origin': 'user', 'name': 'rows_read', 'context': {'namespace': 'beam_cli'}}, 'scalar': 5}]}
                    if req.full_url.endswith('/metrics')
                    else {'currentState': 'JOB_STATE_DONE'}
                ).encode('utf-8')
            )
        )
    )
    # SSRF host guard
    with self.assertRaisesRegex(jobs.JobError, 'refusing Dataflow API request'):
      poller._rest_json('https://evil.example.com/v1b3/jobs/1', 'tok')

    with mock.patch.object(poller, '_ensure_rest_creds', return_value='tok-123'):
      st, m = poller.query('job-1', 'my-proj', 'us-central1')
      self.assertEqual(st, 'JOB_STATE_DONE')
      self.assertEqual(m['rows_read'], 5)
      self.assertTrue(poller.modify_state('job-1', 'JOB_STATE_CANCELLED'))

    # Fallback when REST token is None -> uses _apiclient
    poller2 = jobs._DataflowPoller('my-proj', 'us-central1')
    poller2._apiclient = types.SimpleNamespace(
        get_job=lambda jid: types.SimpleNamespace(current_state='JOB_STATE_RUNNING'),
        get_job_metrics=lambda jid: types.SimpleNamespace(metrics=[]),
        modify_job_state=lambda jid, st: True,
    )
    with mock.patch.object(poller2, '_ensure_rest_creds', return_value=None):
      st2, _ = poller2.query('job-2', 'my-proj', 'us-central1')
      self.assertEqual(st2, 'JOB_STATE_RUNNING')
      self.assertTrue(poller2.modify_state('job-2', 'JOB_STATE_DRAINING'))

  def test_wait_for_job_and_cancel_job_branches(self):
    left_f = os.path.join(self.tmp, 'left.jsonl')
    right_f = os.path.join(self.tmp, 'right.jsonl')
    with open(left_f, 'w', encoding='utf-8') as f:
      f.write('{"id": 1}\n')
    with open(right_f, 'w', encoding='utf-8') as f:
      f.write('{"id": 2}\n')

    jobs.record_job_submission(
        'job-verify',
        runner='DataflowRunner',
        state='JOB_STATE_RUNNING',
        command='pipe',
        project='my-proj',
        region='us-central1',
        post_checks={
            'assert_counts': True,
            'verify_diff': {'left': left_f, 'right': right_f, 'keys': ['id']},
        },
    )
    states = iter([
        ('JOB_STATE_RUNNING', {}),
        ('JOB_STATE_DONE', {'rows_read': 1, 'rows_written': 1}),
    ])
    stderr = io.StringIO()
    with contextlib.redirect_stderr(stderr):
      rc, rec = jobs.wait_for_job(
          'job-verify',
          poll_interval=0.01,
          show_progress=True,
          query_fn=lambda jid, p, r: next(states),
      )
    self.assertEqual(rc, 6)
    self.assertEqual(rec['validation_state'], 'FAILED')
    self.assertIn('Waiting for job', stderr.getvalue())

    # Timeout branch on external job (no prior local record)
    rc_to, rec_to = jobs.wait_for_job(
        'job-timeout',
        timeout_seconds=0.02,
        poll_interval=0.01,
        project='my-proj',
        region='us-central1',
        query_fn=lambda jid, p, r: ('JOB_STATE_RUNNING', {}),
    )
    self.assertEqual(rc_to, jobs.EXIT_TIMEOUT)
    self.assertTrue(rec_to['detached'])

    # Terminal failure branch
    rc_fail, _ = jobs.wait_for_job(
        'job-failed',
        project='my-proj',
        region='us-central1',
        query_fn=lambda jid, p, r: ('JOB_STATE_FAILED', {}),
    )
    self.assertEqual(rc_fail, jobs.EXIT_PIPELINE_FAILED)

    # cancel_job drain + already_terminal fallback + error
    res_drain = jobs.cancel_job(
        'job-verify',
        drain=True,
        cancel_fn=lambda jid, p, r, st: st == 'JOB_STATE_DRAINING',
    )
    self.assertEqual(res_drain['mode'], 'drain')

    with self.assertRaisesRegex(jobs.JobError, 'runner rejected state transition'):
      jobs.cancel_job(
          'job-verify',
          cancel_fn=lambda jid, p, r, st: False,
      )


if __name__ == '__main__':
  unittest.main()
