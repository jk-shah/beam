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

"""Hermetic unit and CLI integration tests for `CliEnrich` (`B1.3`)."""

from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr
from contextlib import redirect_stdout

import pyarrow as pa
import pyarrow.parquet as pq

import apache_beam as beam
from apache_beam.cli import check_cmd
from apache_beam.cli import enrich
from apache_beam.cli import main as cli_main
from apache_beam.cli import package as pkg_mod
from apache_beam.cli import pipe_spec
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import TimestampedValue


def _run_cli(argv: list[str]) -> tuple[int, str, str]:
  out_buf = io.StringIO()
  err_buf = io.StringIO()
  with redirect_stdout(out_buf), redirect_stderr(err_buf):
    rc = cli_main.main(argv)
  return rc, out_buf.getvalue(), err_buf.getvalue()


class _FakeRedisPipeline:
  def __init__(
      self,
      persist_path: str | None,
      store: dict[str, str],
      fail_writes: bool = False,
  ):
    self._persist_path = persist_path
    self._store = store
    self._fail_writes = fail_writes
    self._ops: list[tuple[str, str]] = []

  def setex(self, key: str, _ttl: int, value: str):
    self._ops.append((key, value))
    return self

  def execute(self):
    if self._fail_writes:
      raise ConnectionError('simulated redis write outage')
    if self._persist_path and os.path.exists(self._persist_path):
      with open(self._persist_path, encoding='utf-8') as f:
        data = json.load(f)
    else:
      data = dict(self._store)
    for k, v in self._ops:
      data[k] = v
      self._store[k] = v
    if self._persist_path:
      with open(self._persist_path, 'w', encoding='utf-8') as f:
        json.dump(data, f)
    self._ops.clear()


class _FakeRedisClient:
  def __init__(
      self,
      persist_path: str | None = None,
      *,
      fail_reads: bool = False,
      fail_writes: bool = False,
  ):
    self.persist_path = persist_path
    self.store: dict[str, str] = {}
    self.fail_reads = fail_reads
    self.fail_writes = fail_writes
    self.mget_calls = 0

  def _load(self) -> dict[str, str]:
    if self.persist_path and os.path.exists(self.persist_path):
      with open(self.persist_path, encoding='utf-8') as f:
        return json.load(f)
    return self.store

  def mget(self, keys: list[str]) -> list[bytes | None]:
    self.mget_calls += 1
    if self.fail_reads:
      raise ConnectionError('simulated redis read outage')
    data = self._load()
    return [data[k].encode('utf-8') if k in data else None for k in keys]

  def pipeline(self, transaction: bool = False):
    _ = transaction
    return _FakeRedisPipeline(
        self.persist_path, self.store, fail_writes=self.fail_writes)

  def close(self):
    pass


class _PoisonBatchCaller:
  """Picklable test caller failing batch RPCs when `u_poison` is present."""
  def __init__(self, call_log: list[list[str]] | None = None):
    self._data = {
        enrich.encode_canonical_key_values(['u1']): {
            'tier': 'VIP', 'ltv': 250.5
        },
        enrich.encode_canonical_key_values(['u2']): {
            'tier': 'STD', 'ltv': 40.0
        },
    }
    self._poison_ck = enrich.encode_canonical_key_values(['u_poison'])
    self.discovered_columns = ('tier', 'ltv')
    self._call_log = call_log

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    return None

  def lookup_batch(self, requests):
    if self._call_log is not None:
      self._call_log.append([str(kt[0]) for _, kt, _ in requests])
    for ckey, _, _ in requests:
      if ckey == self._poison_ck:
        raise ValueError(
            "Invalid entity secret_token='super_secret_row_payload_value_123456' "
            "in SELECT * FROM `p.d.t` WHERE id = 'u_poison'")
    return {ckey: self._data.get(ckey) for ckey, _, _ in requests}


def _make_poison_caller(_cfg):
  return _PoisonBatchCaller()


class EnrichConfigAndSecurityTest(unittest.TestCase):
  def test_bracket_aware_spec_parsing_and_yaml_roundtrip(self):
    yaml_cfg = enrich.parse_enrich_spec_str(
        'bigquery:project=my-proj,table=analytics.users,'
        'fields=[tenant_id,user_id],columns=[tier,ltv:float,zip:str],'
        'prefix=u_,on_miss=dlq,max_batch_size=200,'
        'redis=cache.internal:6379,rate_limit=50')
    self.assertEqual(yaml_cfg['handler'], 'bigquery')
    self.assertEqual(yaml_cfg['table'], 'my-proj.analytics.users')
    self.assertEqual(yaml_cfg['fields'], ['tenant_id', 'user_id'])
    self.assertEqual(yaml_cfg['columns'], ['tier', 'ltv:float', 'zip:str'])
    self.assertEqual(yaml_cfg['prefix'], 'u_')
    self.assertEqual(yaml_cfg['on_miss'], 'dlq')
    self.assertEqual(yaml_cfg['max_batch_size'], 200)
    # Non-loopback Redis defaults to TLS (`rediss://`).
    self.assertEqual(yaml_cfg['redis'], 'rediss://cache.internal:6379')

    reparsed = enrich.parse_enrich_config(yaml_cfg)
    self.assertTrue(reparsed.redis_tls)
    self.assertIn('zip', reparsed.explicit_types)
    self.assertNotIn('tier', reparsed.explicit_types)

  def test_rejects_inline_secrets_and_non_tls_remote_redis(self):
    with self.assertRaisesRegex(ValueError, 'inline credentials'):
      enrich.parse_enrich_spec_str(
          'file:path=dims.jsonl,key=id,redis=rediss://:mysecret@cache.internal:6379'
      )
    with self.assertRaisesRegex(ValueError, 'inline credentials'):
      enrich.parse_enrich_spec_str(
          'file:path=dims.jsonl,key=id,redis=pass@cache.internal:6379')
    with self.assertRaisesRegex(ValueError, 'refusing inline secret'):
      enrich.parse_enrich_spec_str(
          'file:path=dims.jsonl,key=id,redis_password=secret')
    with self.assertRaisesRegex(ValueError, 'requires TLS'):
      enrich.parse_enrich_spec_str(
          'file:path=dims.jsonl,key=id,redis=redis://cache.internal:6379')
    with self.assertRaisesRegex(ValueError, 'requires TLS'):
      enrich.parse_enrich_spec_str(
          'file:path=dims.jsonl,key=id,redis=cache.internal:6379,redis_tls=false'
      )
    # Loopback non-TLS is permitted for local dev/testing.
    local_cfg = enrich.parse_enrich_spec_str(
        'file:path=dims.jsonl,key=id,redis=redis://127.0.0.1:6379')
    self.assertEqual(local_cfg['redis'], 'redis://127.0.0.1:6379')

  def test_rejects_forbidden_callables_bq_sqli_and_oversized_batches(self):
    for bad_key in (
        'query_fn',
        'condition_value_fn',
        'row_restriction_template',
        'row_key_fn',
        'join_fn',
    ):
      with self.assertRaisesRegex(ValueError, 'forbids'):
        enrich.parse_enrich_config({
            'handler': 'bigquery',
            'project': 'my-proj',
            'table': 'my-proj.d.t',
            'row_key': 'id',
            bad_key: 'x',
        })

    with self.assertRaisesRegex(ValueError, 'exceeds FinOps'):
      enrich.parse_enrich_spec_str(
          'bigquery:project=my-proj,table=my-proj.d.t,key=id,max_batch_size=5000'
      )

    with self.assertRaises(ValueError):
      enrich.parse_enrich_spec_str(
          'bigquery:project=my-proj,table="my-proj.d.t; DROP TABLE x",key=id')

    with self.assertRaises(ValueError):
      enrich.parse_enrich_spec_str(
          'bigquery:project=my-proj,table=my-proj.d.t,key=id,'
          'row_restriction="1=1 UNION SELECT * FROM secrets"')

  def test_vertexai_endpoint_ssrf_and_region_mismatch_blocked(self):
    with self.assertRaisesRegex(ValueError, 'unsafe Vertex AI api_endpoint'):
      enrich.parse_enrich_spec_str(
          'vertexai:project=my-proj,location=us-central1,'
          'api_endpoint=attacker.example.com,feature_store=fs,feature_view=fv,key=uid'
      )
    with self.assertRaisesRegex(ValueError, 'does not match location'):
      enrich.parse_enrich_spec_str(
          'vertexai:project=my-proj,location=us-central1,'
          'api_endpoint=europe-west1-aiplatform.googleapis.com,'
          'feature_store=fs,feature_view=fv,key=uid')
    ok = enrich.parse_enrich_spec_str(
        'vertexai:project=my-proj,location=us-central1,'
        'feature_store=fs,feature_view=fv,key=uid,columns=[f1:float]')
    self.assertEqual(
        ok['api_endpoint'], 'us-central1-aiplatform.googleapis.com')

  def test_feast_yaml_allowlist_temp_cleanup_and_all_none_miss(self):
    with tempfile.TemporaryDirectory() as tmp:
      evil_yaml = os.path.join(tmp, 'evil_fs.yaml')
      with open(evil_yaml, 'w', encoding='utf-8') as f:
        f.write('project: p\nprovider: attacker.pkg.EvilProvider\n')
      cfg_evil = enrich.parse_enrich_config({
          'handler': 'feast',
          'path': evil_yaml,
          'entity_id': 'user_id',
          'feature_names': ['stats:ltv:float'],
      })
      caller_evil = enrich._FeastCaller(cfg_evil)
      with self.assertRaisesRegex(ValueError, 'untrusted Feast provider'):
        caller_evil.__enter__()

      good_yaml = os.path.join(tmp, 'good_fs.yaml')
      with open(good_yaml, 'w', encoding='utf-8') as f:
        f.write(
            'project: demo\nprovider: local\nonline_store:\n  type: sqlite\n')
      cfg_good = enrich.parse_enrich_config({
          'handler': 'feast',
          'path': good_yaml,
          'entity_id': 'user_id',
          'feature_names': ['stats:tier', 'stats:ltv:float'],
      })

      captured_tmp_paths: list[str] = []

      class _FakeOnlineResp:
        def __init__(self, d):
          self._d = d

        def to_dict(self):
          return self._d

      class _FakeFeastStore:
        def __init__(self, fs_path: str):
          captured_tmp_paths.append(fs_path)
          assert os.path.isfile(fs_path)

        def get_online_features(
            self, features, entity_rows, full_feature_names):
          _ = (features, full_feature_names)
          tiers, ltvs, uids = [], [], []
          for er in entity_rows:
            uid = er['user_id']
            uids.append(uid)
            if uid == 'u1':
              tiers.append('GOLD')
              ltvs.append(99.5)
            else:
              # Feast returns None values for missing entity IDs!
              tiers.append(None)
              ltvs.append(None)
          return _FakeOnlineResp({'user_id': uids, 'tier': tiers, 'ltv': ltvs})

      caller = enrich._FeastCaller(cfg_good, store_factory=_FakeFeastStore)
      with caller:
        self.assertEqual(len(captured_tmp_paths), 1)
        self.assertTrue(os.path.exists(captured_tmp_paths[0]))
        ck1 = enrich.encode_canonical_key_values(['u1'])
        ck2 = enrich.encode_canonical_key_values(['u_missing'])
        out = caller.lookup_batch([
            (ck1, ('u1', ), beam.Row(user_id='u1')),
            (ck2, ('u_missing', ), beam.Row(user_id='u_missing')),
        ])
        self.assertEqual(out[ck1], {'tier': 'GOLD', 'ltv': 99.5})
        self.assertIsNone(out[ck2])
      # Temp YAML file must be deleted on __exit__!
      self.assertFalse(os.path.exists(captured_tmp_paths[0]))


class EnrichHandlersAndResilienceTest(unittest.TestCase):
  def test_parameterized_bigquery_caller_prevents_sqli_and_strips_join_key(
      self):
    executed_queries: list[tuple[str, list]] = []

    class _FakeJob:
      def result(self):
        return [
            {
                'user_id': 'u1', 'tier': 'VIP', 'ltv': 150.0
            },
        ]

    class _FakeBqClient:
      def __init__(self, project):
        self.project = project
        self.closed = False

      def query(self, sql, job_config=None):
        executed_queries.append((sql, list(job_config.query_parameters)))
        return _FakeJob()

      def close(self):
        self.closed = True

    cfg = enrich.parse_enrich_config({
        'handler': 'bigquery',
        'project': 'my-proj',
        'table': 'my-proj:analytics.dim_users',
        'fields': ['user_id'],
        'columns': ['tier', 'ltv:float'],
        'row_restriction': 'is_active = TRUE',
    })
    sqli_key = "u1') OR 1=1 --"
    ck_ok = enrich.encode_canonical_key_values(['u1'])
    ck_sqli = enrich.encode_canonical_key_values([sqli_key])

    caller = enrich._ParameterizedBigQueryCaller(
        cfg, client_factory=_FakeBqClient)
    with caller:
      res = caller.lookup_batch([
          (ck_ok, ('u1', ), beam.Row(user_id='u1')),
          (ck_sqli, (sqli_key, ), beam.Row(user_id=sqli_key)),
      ])
    self.assertEqual(len(executed_queries), 1)
    sql, params = executed_queries[0]
    self.assertIn(
        'SELECT `tier`, `ltv`, `user_id` FROM `my-proj.analytics.dim_users`',
        sql)
    self.assertIn('`user_id` IN UNNEST(@keys)', sql)
    self.assertNotIn('1=1', sql)
    self.assertEqual(len(params), 1)
    self.assertEqual(list(params[0].values), ['u1', sqli_key])
    # Auto-fetched `user_id` is stripped because user requested only `[tier, ltv]`.
    self.assertEqual(res[ck_ok], {'tier': 'VIP', 'ltv': 150.0})
    self.assertIsNone(res[ck_sqli])

  def test_bigtable_and_vertex_lazy_callers_flatten_and_handle_misses(self):
    bt_cfg = enrich.parse_enrich_config({
        'handler': 'bigtable',
        'project': 'my-proj',
        'instance': 'inst-1',
        'table': 'users-bt',
        'row_key': 'user_id',
        'columns': ['profile:tier', 'metrics:tier', 'metrics:score:float'],
    })

    class _FakeBtHandler:
      def __enter__(self):
        return self

      def __exit__(self, *args):
        return None

      def __call__(self, req):
        if req.user_id == 'u1':
          return req, beam.Row(
              profile={'tier': 'PRO'},
              metrics={'tier': 'M1', 'score': '88.5'})
        return req, beam.Row()

    bt_caller = enrich._BigTableCaller(
        bt_cfg, handler_factory=lambda _hp, _rk: _FakeBtHandler())
    with bt_caller:
      ck1 = enrich.encode_canonical_key_values(['u1'])
      ck2 = enrich.encode_canonical_key_values(['u2'])
      out = bt_caller.lookup_batch([
          (ck1, ('u1', ), beam.Row(user_id='u1')),
          (ck2, ('u2', ), beam.Row(user_id='u2')),
      ])
      self.assertEqual(
          out[ck1], {
              'profile_tier': 'PRO', 'metrics_tier': 'M1', 'score': '88.5'
          })
      self.assertIsNone(out[ck2])

    # VertexFS: verify instantiation does NOT run during CliEnrich(config)!
    init_calls = []

    def _vertex_factory(_hp, _rk):
      init_calls.append(1)

      class _FakeVfs:
        def __enter__(self):
          return self

        def __exit__(self, *args):
          return None

        def __call__(self, req):
          if req.user_id == 'u1':
            return req, beam.Row(risk_score=0.12)
          return req, beam.Row()

      return _FakeVfs()

    t_vfs = enrich.CliEnrich({
        'handler': 'vertexai',
        'project': 'my-proj',
        'location': 'us-central1',
        'feature_store': 'fs1',
        'feature_view': 'fv1',
        'row_key': 'user_id',
        'columns': ['risk_score:float'],
    },
                             caller_factory=lambda cfg: enrich._VertexFSCaller(
                                 cfg, handler_factory=_vertex_factory))
    # Zero network/handler init prior to pipeline execution!
    self.assertEqual(init_calls, [])

  def test_singleton_batch_fallback_dlq_scrubbing_and_redis_cache_guard(self):
    with tempfile.TemporaryDirectory() as tmp:
      redis_json = os.path.join(tmp, 'redis_store.json')
      fake_redis = _FakeRedisClient(redis_json)

      cfg = {
          'handler': 'bigquery',
          'project': 'my-proj',
          'table': 'my-proj.d.users',
          'row_key': 'user_id',
          'columns': ['tier', 'ltv:float'],
          'prefix': 'u_',
          'on_miss': 'dlq',
          'max_batch_size': 10,
          'redis': 'redis://127.0.0.1:6379',
          'dlq_redact_fields': ['secret_ssn'],
      }

      with beam.Pipeline() as p:
        inputs = (
            p
            | beam.Create([
                beam.Row(user_id='u1', amount=10.0, secret_ssn='111-11-1111'),
                beam.Row(
                    user_id='u_poison', amount=20.0, secret_ssn='222-22-2222'),
                beam.Row(user_id='u2', amount=30.0, secret_ssn='333-33-3333'),
                beam.Row(
                    user_id='u_miss', amount=40.0, secret_ssn='444-44-4444'),
                beam.Row(user_id='u1', amount=50.0, secret_ssn='555-55-5555'),
            ]))
        res = inputs | enrich.CliEnrich(
            cfg,
            has_error_handling=True,
            caller_factory=_make_poison_caller,
            redis_client=fake_redis)
        good_tuples = res['good'] | 'ToGoodTuples' >> beam.Map(
            lambda r: (r.user_id, r.amount, r.u_tier, r.u_ltv))
        dlq_checks = res['errors'] | 'CheckDlqScrub' >> beam.Map(
            lambda r: (
                r.error_type,
                'super_secret_row_payload' not in r.error,
                'u_poison' not in r.error,
                '222-22-2222' not in r.raw_payload, ))

        assert_that(
            good_tuples,
            equal_to([
                ('u1', 10.0, 'VIP', 250.5),
                ('u2', 30.0, 'STD', 40.0),
                ('u1', 50.0, 'VIP', 250.5),
            ]),
            label='CheckGood')
        assert_that(
            dlq_checks,
            equal_to([
                ('ValueError', True, True, True),
                ('EnrichmentMissError', True, True, True),
            ]),
            label='CheckDlqScrubbed')

      with open(redis_json, encoding='utf-8') as rf:
        shared_redis_store = json.load(rf)
      # Verify Redis cached ONLY the 2 valid hits (`u1`, `u2`), NEVER `u_poison` or `u_miss`!
      self.assertEqual(len(shared_redis_store), 2)
      cached_vals = [json.loads(v) for v in shared_redis_store.values()]
      self.assertCountEqual(
          cached_vals, [{
              'tier': 'VIP', 'ltv': 250.5
          }, {
              'tier': 'STD', 'ltv': 40.0
          }])
      for rkey in shared_redis_store:
        self.assertNotIn('u1', rkey)

      def _never_called(_cfg):
        class _FailIfCalled:
          discovered_columns = ('tier', 'ltv')

          def __enter__(self):
            return self

          def __exit__(self, *args):
            return None

          def lookup_batch(self, _reqs):
            raise AssertionError('should be served 100% from Redis cache!')

        return _FailIfCalled()

      with beam.Pipeline() as p2:
        out2 = (
            p2
            | beam.Create([beam.Row(user_id='u1', amount=99.0)])
            | enrich.CliEnrich({
                'handler': 'bigquery',
                'project': 'my-proj',
                'table': 'my-proj.d.users',
                'row_key': 'user_id',
                'columns': ['tier', 'ltv:float'],
                'prefix': 'u_',
                'redis': 'redis://127.0.0.1:6379',
            },
                               caller_factory=_never_called,
                               redis_client=_FakeRedisClient(redis_json))
            | beam.Map(lambda r: (r.user_id, r.u_tier, r.u_ltv)))
        assert_that(out2, equal_to([('u1', 'VIP', 250.5)]))

      # Verify Redis outage degrades cleanly to direct lookup without failing!
      broken_redis = _FakeRedisClient(fail_reads=True, fail_writes=True)
      with beam.Pipeline() as p3:
        out3 = (
            p3
            | beam.Create([beam.Row(user_id='u2', amount=12.0)])
            | enrich.CliEnrich({
                'handler': 'bigquery',
                'project': 'my-proj',
                'table': 'my-proj.d.users',
                'row_key': 'user_id',
                'columns': ['tier', 'ltv:float'],
                'prefix': 'u_',
                'redis': 'redis://127.0.0.1:6379',
            },
                               caller_factory=_make_poison_caller,
                               redis_client=broken_redis)
            | beam.Map(lambda r: (r.user_id, r.u_tier, r.u_ltv)))
        assert_that(out3, equal_to([('u2', 'STD', 40.0)]))

  def test_preserves_event_timestamps_into_fixed_windows(self):
    with tempfile.TemporaryDirectory() as tmp:
      dims_path = os.path.join(tmp, 'dims.jsonl')
      with open(dims_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps({'uid': 'u1', 'mult': 2.0}) + '\n')

      with beam.Pipeline() as p:
        res = (
            p
            | beam.Create([
                TimestampedValue(beam.Row(uid='u1', v=5.0), 1000),
                TimestampedValue(beam.Row(uid='u1', v=7.0), 1010),
                TimestampedValue(beam.Row(uid='u1', v=3.0), 1090),
            ]).with_output_types(
                beam.typehints.schemas.named_tuple_to_schema(
                    beam.Row(uid='u1', v=1.0).__class__)
                if False else beam.Row)  # fallback dynamic test
            | enrich.CliEnrich({
                'handler': 'file',
                'path': dims_path,
                'key': 'uid',
                'columns': ['mult:float'],
                'max_batch_size': 50,
            })
            | beam.WindowInto(FixedWindows(60))
            | beam.Map(lambda r: (r.uid, r.v * r.mult))
            | beam.CombinePerKey(sum))
        assert_that(res, equal_to([('u1', 24.0), ('u1', 6.0)]))


class EnrichCliPipeIntegrationTest(unittest.TestCase):
  def test_full_pipe_pre_select_multi_enrich_post_where_parquet_dlq_redact_and_assert_counts(
      self):
    with tempfile.TemporaryDirectory() as tmp:
      events_path = os.path.join(tmp, 'events.jsonl')
      users_csv = os.path.join(tmp, 'users.csv')
      fx_parquet = os.path.join(tmp, 'fx.parquet')
      out_parquet = os.path.join(tmp, 'out.parquet')
      dlq_jsonl = os.path.join(tmp, 'dlq.jsonl')

      events = [
          # Duplicate of e1 (dropped by --dedupe-by)
          {
              'event_id': 'e1',
              'raw_uid': '  U1 ',
              'ccy': 'EUR',
              'amount': 100.0,
              'secret_ssn': '999-00-1111',
          },
          {
              'event_id': 'e1',
              'raw_uid': 'U1',
              'ccy': 'EUR',
              'amount': 100.0,
              'secret_ssn': '999-00-1111',
          },
          # Filtered by --where (amount <= 0)
          {
              'event_id': 'e2',
              'raw_uid': 'U1',
              'ccy': 'EUR',
              'amount': -5.0,
              'secret_ssn': '999-00-2222',
          },
          # Missing user in users.csv -> routed to --dlq via on_miss=dlq (with secret_ssn redacted!)
          {
              'event_id': 'e3',
              'raw_uid': 'U_UNKNOWN',
              'ccy': 'EUR',
              'amount': 50.0,
              'secret_ssn': '999-00-3333',
          },
          # Enriched user_tier='FREE' -> filtered out by --post-where
          {
              'event_id': 'e4',
              'raw_uid': 'u2',
              'ccy': 'USD',
              'amount': 80.0,
              'secret_ssn': '999-00-4444',
          },
          # Enriched user_tier='VIP' & fx_rate=1.2 -> passes --post-where & --expect!
          {
              'event_id': 'e5',
              'raw_uid': 'u3',
              'ccy': 'GBP',
              'amount': 200.0,
              'secret_ssn': '999-00-5555',
          },
      ]
      with open(events_path, 'w', encoding='utf-8') as f:
        for ev in events:
          f.write(json.dumps(ev) + '\n')

      with open(users_csv, 'w', encoding='utf-8') as f:
        f.write('user_id,tier,zip_code\n')
        f.write('u1,VIP,00501\n')
        f.write('u2,FREE,94043\n')
        f.write('u3,VIP,10001\n')

      pq.write_table(
          pa.table({
              'currency': ['EUR', 'USD', 'GBP'],
              'rate': [1.1, 1.0, 1.25],
          }),
          fx_parquet)

      rc, stdout, stderr = _run_cli([
          'pipe',
          '--from',
          events_path,
          '--dedupe-by',
          'event_id',
          '--where',
          'amount > 0',
          '--pre-select',
          '*, lower(strip(raw_uid)) AS user_id',
          '--enrich',
          f'file:path={users_csv},key=user_id,columns=[tier,zip_code:str],prefix=u_,on_miss=dlq',
          '--enrich',
          f'file:path={fx_parquet},key=ccy,lookup_key=currency,columns=[rate:float],prefix=fx_,on_miss=null',
          '--post-where',
          "u_tier = 'VIP'",
          '--select',
          'event_id, user_id, u_tier AS tier, u_zip_code AS zip_code, round(amount * fx_rate, 2) AS usd_amount',
          '--expect',
          'usd_amount > 0',
          '--dlq',
          dlq_jsonl,
          '--dlq-redact-fields',
          'secret_ssn',
          '--to',
          out_parquet,
          '--assert-counts',
          '--json',
      ])
      self.assertEqual(rc, 0, msg=f'stdout={stdout}\nstderr={stderr}')
      env = json.loads(stdout)
      self.assertTrue(env['ok'])
      metrics = env['metrics']
      # 6 read = 2 written (e1, e5) + 3 filtered (1 dedup + 1 where + 1 post-where) + 1 dlq (e3)
      self.assertEqual(metrics['rows_read'], 6)
      self.assertEqual(metrics['rows_written'], 2)
      self.assertEqual(metrics['rows_filtered_out'], 3)
      self.assertEqual(metrics['dlq_rows'], 1)
      self.assertEqual(metrics['duplicates_dropped'], 1)
      self.assertGreaterEqual(metrics['enrich_hits'], 5)
      self.assertEqual(metrics['enrich_misses'], 1)

      out_rows = sorted(
          pq.read_table(out_parquet).to_pylist(), key=lambda r: r['event_id'])
      self.assertEqual(
          out_rows,
          [
              {
                  'event_id': 'e1',
                  'user_id': 'u1',
                  'tier': 'VIP',
                  'zip_code': '00501',
                  'usd_amount': 110.0,
              },
              {
                  'event_id': 'e5',
                  'user_id': 'u3',
                  'tier': 'VIP',
                  'zip_code': '10001',
                  'usd_amount': 250.0,
              },
          ])

      with open(dlq_jsonl, encoding='utf-8') as df:
        dlq_rows = [json.loads(line) for line in df if line.strip()]
      self.assertEqual(len(dlq_rows), 1)
      self.assertEqual(dlq_rows[0]['step'], 'CliEnrich')
      self.assertEqual(dlq_rows[0]['error_type'], 'EnrichmentMissError')
      # Raw PII key 'u_unknown' and SSN '999-00-3333' must NOT leak!
      self.assertNotIn('u_unknown', dlq_rows[0]['error'])
      self.assertNotIn('999-00-3333', dlq_rows[0]['raw_payload'])
      dlq_payload = json.loads(dlq_rows[0]['raw_payload'])
      self.assertEqual(dlq_payload['secret_ssn'], '<REDACTED>')

  def test_on_miss_drop_conserves_assert_counts(self):
    with tempfile.TemporaryDirectory() as tmp:
      src_path = os.path.join(tmp, 'in.jsonl')
      dims_path = os.path.join(tmp, 'dims.jsonl')
      out_path = os.path.join(tmp, 'out.jsonl')
      with open(src_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps({'id': '1', 'v': 10}) + '\n')
        f.write(json.dumps({'id': '2', 'v': 20}) + '\n')
        f.write(json.dumps({'id': '3', 'v': 30}) + '\n')
      with open(dims_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps({'id': '1', 'label': 'ok'}) + '\n')

      rc, stdout, stderr = _run_cli([
          'pipe',
          '--from',
          src_path,
          '--enrich',
          f'file:path={dims_path},key=id,columns=[label],on_miss=drop',
          '--to',
          out_path,
          '--assert-counts',
          '--json',
      ])
      self.assertEqual(rc, 0, msg=f'stdout={stdout}\nstderr={stderr}')
      env = json.loads(stdout)
      self.assertEqual(env['metrics']['rows_read'], 3)
      self.assertEqual(env['metrics']['rows_written'], 1)
      self.assertEqual(env['metrics']['enrich_misses_dropped'], 2)
      self.assertEqual(env['metrics']['rows_filtered_out'], 2)
      self.assertTrue(any('on_miss=drop' in w for w in env.get('warnings', [])))

  def test_dry_run_check_plan_package_auto_sdk_and_opt_file_redaction(self):
    with tempfile.TemporaryDirectory() as tmp:
      src_path = os.path.join(tmp, 'in.jsonl')
      dims_path = os.path.join(tmp, 'dims.jsonl')
      opt_file = os.path.join(tmp, 'enrich_opts.yaml')
      with open(src_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps({'user_id': 'u1'}) + '\n')
      with open(dims_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps({'user_id': 'u1', 'tier': 'VIP'}) + '\n')
      with open(opt_file, 'w', encoding='utf-8') as f:
        f.write(
            'redis: redis://127.0.0.1:6379\nredis_password: top_secret_pw\n')

      # Permissive 0644 secret opt-file must be rejected!
      os.chmod(opt_file, 0o644)
      rc_bad, _, _ = _run_cli([
          'pipe',
          '--from',
          src_path,
          '--enrich',
          f'file:path={dims_path},key=user_id,columns=[tier]',
          '--enrich-opt-file',
          opt_file,
          '--to',
          os.path.join(tmp, 'out.jsonl'),
          '--dry-run',
      ])
      self.assertEqual(rc_bad, 2)

      # Restrict to 0600 -> succeeds and scrubs password in emitted YAML!
      os.chmod(opt_file, 0o600)
      rc_ok, yaml_out, _ = _run_cli([
          'pipe',
          '--from',
          src_path,
          '--enrich',
          f'file:path={dims_path},key=user_id,columns=[tier]',
          '--enrich-opt-file',
          opt_file,
          '--dlq',
          os.path.join(tmp, 'dlq.jsonl'),
          '--dlq-redact-fields',
          'tier',
          '--to',
          os.path.join(tmp, 'out.jsonl'),
          '--dry-run',
      ])
      self.assertEqual(rc_ok, 0)
      self.assertNotIn('top_secret_pw', yaml_out)
      self.assertIn('<REDACTED_SECRET>', yaml_out)

      # `beam check --plan` without `--trust-providers` must pass on built-in CliEnrich + redacted DLQ!
      yaml_file = os.path.join(tmp, 'pipeline.yaml')
      with open(yaml_file, 'w', encoding='utf-8') as yf:
        yf.write(yaml_out)
      chk = check_cmd.run_check(yaml_file, expand_plan=True)
      self.assertTrue(chk['valid'])
      self.assertEqual(chk['mode'], 'expanded_plan')

      # `beam package --sdk=auto --dry-run` upgrades to PYTHON and detects `redis`.
      pkg = pkg_mod.run_package(yaml_file, sdk='auto', dry_run=True)
      self.assertEqual(pkg['sdk_language'], 'PYTHON')
      self.assertTrue(any('redis' in w for w in pkg.get('warnings', [])))

  def test_streaming_bigquery_finops_warning_and_guard_errors(self):
    with tempfile.TemporaryDirectory() as tmp:
      from_opt_file = os.path.join(tmp, 'pubsub_opts.yaml')
      with open(from_opt_file, 'w', encoding='utf-8') as f:
        f.write(
            'format: JSON\n'
            'schema:\n'
            '  type: object\n'
            '  properties:\n'
            '    user_id: {type: string}\n')
      os.chmod(from_opt_file, 0o600)

      rc, stdout, stderr = _run_cli([
          'pipe',
          '--from',
          'pubsub://projects/my-proj/subscriptions/events-sub',
          '--from-opt-file',
          from_opt_file,
          '--enrich',
          'bigquery:project=my-proj,table=my-proj.d.users,key=user_id,columns=[tier]',
          '--window',
          'fixed:60s',
          '--to',
          os.path.join(tmp, 'out.parquet'),
          '--dry-run',
          '--json',
      ])
      self.assertEqual(rc, 0, msg=f'stdout={stdout}\nstderr={stderr}')
      env = json.loads(stdout)
      self.assertTrue(
          any(
              'streaming BigQuery --enrich without redis=' in w
              for w in env.get('warnings', [])))

      # `--key` collision guard with `--enrich`
      rc_key, _, _ = _run_cli([
          'pipe',
          '--from',
          from_opt_file,
          '--enrich',
          f'file:path={from_opt_file},key=id',
          '--key',
          'id',
          '--to',
          os.path.join(tmp, 'o.jsonl'),
          '--dry-run',
      ])
      self.assertEqual(rc_key, 2)

      # `--enrich on_miss=dlq` without `--dlq` fails fast
      with self.assertRaisesRegex(pipe_spec.PipeSpecError, 'requires --dlq'):
        pipe_spec.build_pipe_spec(
            from_opt_file,
            os.path.join(tmp, 'o.jsonl'),
            from_format='json',
            enrich='file:path=dims.jsonl,key=id,on_miss=dlq')


if __name__ == '__main__':
  unittest.main()
