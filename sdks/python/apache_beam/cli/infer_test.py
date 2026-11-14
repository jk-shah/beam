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

"""Hermetic unit, security, and pipeline integration tests for `CliInfer` (`B1.4`)."""

from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from unittest import mock

import pyarrow.parquet as pq

import apache_beam as beam
from apache_beam.cli import check_cmd
from apache_beam.cli import infer
from apache_beam.cli import jobs
from apache_beam.cli import main as cli_main
from apache_beam.cli import package as package_mod
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import window
from apache_beam.transforms.window import TimestampedValue


def _run_cli(argv: list[str]) -> tuple[int, str, str]:
  out_buf = io.StringIO()
  err_buf = io.StringIO()
  with mock.patch('sys.stdout', out_buf), mock.patch('sys.stderr', err_buf):
    rc = cli_main.main(argv)
  return rc, out_buf.getvalue(), err_buf.getvalue()


class _FakeLoadedModel:
  pass


class _RecordingFakeModelHandler(ModelHandler):
  """Picklable fake `ModelHandler` logging load/batch calls to a temp JSON file."""
  def __init__(
      self,
      log_path: str,
      *,
      poison_marker: str = 'POISON',
      catastrophic_error: bool = False,
  ):
    super().__init__()
    self._log_path = log_path
    self._poison_marker = poison_marker
    self._catastrophic_error = catastrophic_error

  def _append_event(self, event: dict) -> None:
    events: list[dict] = []
    if os.path.exists(self._log_path):
      with open(self._log_path, encoding='utf-8') as f:
        raw = f.read().strip()
        if raw:
          events = json.loads(raw)
    events.append(event)
    with open(self._log_path, 'w', encoding='utf-8') as f:
      json.dump(events, f)

  def load_model(self):
    self._append_event({'op': 'load_model'})
    return _FakeLoadedModel()

  def run_inference(self, batch, model, inference_args=None):
    del model, inference_args
    self._append_event({'op': 'run_inference', 'batch_len': len(batch)})
    if self._catastrophic_error:
      # KeyboardInterrupt / BaseException bypasses Exception singleton catch,
      # exercising outer RunInference.with_exception_handling() if configured,
      # or we can test _ExplodeFailedBatchDoFn directly.
      raise RuntimeError('catastrophic_model_failure')
    for item in batch:
      text = json.dumps(item, sort_keys=True, default=str)
      if self._poison_marker and self._poison_marker in text:
        raise ValueError(
            "Invalid feature payload 'SECRET_SSN_999-00-1234_DO_NOT_LOG' "
            f'for record {text}')
    return [
        PredictionResult(
            item,
            {
                'score': 0.88, 'label': 'ALLOW', 'risk_band': 'LOW'
            },
            'fake-model-v2',
        ) for item in batch
    ]


class CliInferConfigAndSecurityTest(unittest.TestCase):
  def test_parse_infer_fields_sql_aliases_and_dotted_paths(self):
    parsed = infer.parse_infer_fields(
        'amount AS feature_a, user.tier AS feature_b, country')
    self.assertEqual(
        parsed,
        (
            infer.InferFieldMapping(('amount', ), 'feature_a'),
            infer.InferFieldMapping(('user', 'tier'), 'feature_b'),
            infer.InferFieldMapping(('country', ), 'country'),
        ),
    )
    self.assertEqual(
        infer.format_infer_fields(parsed),
        ['amount AS feature_a', 'user.tier AS feature_b', 'country'],
    )

    for bad_expr in (
        '_secret AS x',
        'user.__class__ AS x',
        'amount AS _bad',
        'amount AS dup, country AS dup',
        'lower(amount) AS x',
    ):
      with self.subTest(bad_expr=bad_expr):
        with self.assertRaises(ValueError):
          infer.parse_infer_fields(bad_expr)

  def test_hf_supply_chain_and_callable_lockdowns(self):
    sha = 'a' * 40
    cfg = infer.parse_infer_config({
        'provider': 'hf',
        'task': 'text-classification',
        'model': f'distilbert/sst2@{sha}',
        'fields': 'body AS text',
    })
    self.assertEqual(cfg.provider, 'hf')
    self.assertEqual(cfg.model, 'distilbert/sst2')
    self.assertEqual(cfg.revision, sha)
    self.assertTrue(cfg.is_pinned_revision)
    self.assertTrue(cfg.large_model)
    self.assertEqual(cfg.model_copies, 1)

    unpinned = infer.parse_infer_config({
        'provider': 'hf',
        'task': 'text-classification',
        'model': 'distilbert/sst2@main',
        'fields': 'body',
    })
    self.assertFalse(unpinned.is_pinned_revision)

    blocked_cases = [
        {
            'provider': 'hf',
            'task': 'text-classification',
            'model': 'org/m',
            'fields': 'body',
            'trust_remote_code': True,
        },
        {
            'provider': 'hf',
            'task': 'text-classification',
            'model': 'org/m',
            'fields': 'body',
            'load_pipeline_args': {
                'trust_remote_code': 'true'
            },
        },
        {
            'provider': 'hf',
            'task': 'text-classification',
            'model': 'org/m',
            'fields': 'body',
            'use_safetensors': False,
        },
        {
            'provider': 'hf',
            'task': 'text-classification',
            'model': 'org/m',
            'fields': 'body',
            'model_kwargs': {
                'use_safetensors': 'false'
            },
        },
        {
            'provider': 'hf',
            'task': 'text-classification',
            'model': 'org/m',
            'fields': 'body',
            'env_vars': {
                'LD_PRELOAD': '/tmp/evil.so'
            },
        },
        {
            'provider': 'hf',
            'task': 'text-classification',
            'model': 'https://evil.example.com/model',
            'fields': 'body',
        },
        {
            'provider': 'hf',
            'task': 'text-classification',
            'model': '../escape/model',
            'fields': 'body',
        },
        {
            'provider': 'hf',
            'task': 'text-classification',
            'model': 'org/m',
            'fields': 'body',
            'preprocess': {
                'callable': 'lambda x: x'
            },
        },
        {
            'provider': 'hf',
            'task': 'text-classification',
            'model': 'org/m',  # Missing --infer-fields
        },
    ]
    for bad_cfg in blocked_cases:
      with self.subTest(bad_cfg=bad_cfg):
        with self.assertRaises(ValueError):
          infer.parse_infer_config(bad_cfg)

    with self.assertRaises(ValueError):
      infer.parse_infer_spec_str(
          'hf:task=text-classification,model=org/m,hf_token=hf_secret12345678901234567890',
          infer_fields='body',
      )

  def test_vertexai_validation_and_defaults(self):
    vcfg = infer.parse_infer_config(
        {
            'provider': 'vertexai',
            'endpoint_id': '1234567890',
            'location': 'us-central1',
            'rate_limit': 50,
        },
        default_project='demo-proj',
    )
    self.assertEqual(vcfg.project, 'demo-proj')
    self.assertEqual(vcfg.max_batch_size, 64)
    self.assertEqual(vcfg.max_batch_duration_secs, 0.2)
    self.assertEqual(vcfg.max_payload_bytes, 1_400_000)
    self.assertEqual(vcfg.rate_limit, 50.0)

    # private=True requires VPC network
    with self.assertRaises(ValueError):
      infer.parse_infer_config({
          'provider': 'vertexai',
          'endpoint_id': '123',
          'project': 'demo-proj',
          'location': 'us-central1',
          'private': True,
      })

    # Path traversal in invoke_route blocked
    with self.assertRaises(ValueError):
      infer.parse_infer_config({
          'provider': 'vertexai',
          'endpoint_id': '123',
          'project': 'demo-proj',
          'location': 'us-central1',
          'invoke_route': '/v1/../../admin',
      })

  def test_hf_inner_handler_passes_strict_security_kwargs(self):
    sha = 'b' * 40
    cfg = infer.parse_infer_config({
        'provider': 'hf',
        'task': 'text-classification',
        'model': f'org/safe-model@{sha}',
        'device': 'CPU',
        'fields': 'text',
    })
    lazy = infer._LazyCliModelHandler(cfg)
    fake_cls = mock.MagicMock()
    import sys
    import types
    fake_hf_mod = types.ModuleType(
        'apache_beam.ml.inference.huggingface_inference')
    fake_hf_mod.HuggingFacePipelineModelHandler = fake_cls
    with mock.patch.dict(os.environ,
                         {'HF_TOKEN': 'hf_env_token_value_1234567890'},
                         clear=False):
      with mock.patch.dict(
          sys.modules,
          {'apache_beam.ml.inference.huggingface_inference': fake_hf_mod}):
        lazy._ensure_inner_handler()

    fake_cls.assert_called_once()
    kwargs = fake_cls.call_args.kwargs
    self.assertEqual(kwargs['task'], 'text-classification')
    self.assertEqual(kwargs['model'], 'org/safe-model')
    self.assertEqual(kwargs['device'], 'CPU')
    self.assertTrue(kwargs['large_model'])
    self.assertEqual(kwargs['model_copies'], 1)
    lpa = kwargs['load_pipeline_args']
    self.assertIs(lpa['trust_remote_code'], False)
    self.assertEqual(lpa['revision'], sha)
    self.assertEqual(lpa['token'], 'hf_env_token_value_1234567890')
    self.assertIs(lpa['model_kwargs']['use_safetensors'], True)
    self.assertIs(lpa['model_kwargs']['trust_remote_code'], False)


class CliInferExecutionAndResilienceTest(unittest.TestCase):
  def test_lazy_expand_and_dry_run_make_zero_sdk_or_endpoint_calls(self):
    with tempfile.TemporaryDirectory() as tmp:
      src = os.path.join(tmp, 'in.jsonl')
      with open(src, 'w', encoding='utf-8') as f:
        f.write('{"id": 1, "text": "hello", "amount": 10.0}\n')
      dst = os.path.join(tmp, 'out.parquet')

      with mock.patch(
          'apache_beam.ml.inference.vertex_ai_inference.VertexAIModelHandlerJSON',
          side_effect=AssertionError('VertexAI should not init in --dry-run')):
        rc_v, out_v, _ = _run_cli([
            'pipe',
            '--from',
            src,
            '--to',
            dst,
            '--infer',
            'vertexai:endpoint_id=999,project=my-proj,location=us-central1',
            '--infer-fields',
            'amount AS amt',
            '--infer-flatten',
            '--dry-run',
            '--json',
        ])
      self.assertEqual(rc_v, 0, out_v)
      env_v = json.loads(out_v)
      self.assertIn('CliInfer', env_v['outputs']['yaml'])

      # Unpinned HF Hub model emits warning; pinned 40-char SHA does not.
      rc_u, out_u, _ = _run_cli([
          'pipe',
          '--from',
          src,
          '--to',
          dst,
          '--infer',
          'hf:task=text-classification,model=org/unpinned-model',
          '--infer-fields',
          'text',
          '--dry-run',
          '--json',
      ])
      self.assertEqual(rc_u, 0, out_u)
      env_u = json.loads(out_u)
      self.assertTrue(
          any('40-char commit SHA' in w for w in env_u.get('warnings', [])),
          env_u.get('warnings'),
      )

      sha = 'c' * 40
      rc_p, out_p, _ = _run_cli([
          'pipe',
          '--from',
          src,
          '--to',
          dst,
          '--infer',
          f'hf:task=text-classification,model=org/pinned-model@{sha}',
          '--infer-fields',
          'text',
          '--dry-run',
          '--json',
      ])
      self.assertEqual(rc_p, 0, out_p)
      env_p = json.loads(out_p)
      self.assertFalse(
          any('40-char commit SHA' in w for w in env_p.get('warnings', [])))

  def test_local_missing_ml_packages_exit_with_code_4(self):
    with tempfile.TemporaryDirectory() as tmp:
      src = os.path.join(tmp, 'in.jsonl')
      with open(src, 'w', encoding='utf-8') as f:
        f.write('{"id": 1, "text": "hello"}\n')
      dst = os.path.join(tmp, 'out.jsonl')

      orig_find_spec = infer.os  # unused anchor; patch importlib.util.find_spec
      del orig_find_spec
      real_find_spec = __import__('importlib.util').util.find_spec

      def fake_find_spec(name, *args, **kwargs):
        if name in ('transformers', 'google.cloud.aiplatform'):
          return None
        return real_find_spec(name, *args, **kwargs)

      with mock.patch('importlib.util.find_spec', side_effect=fake_find_spec):
        rc_hf, out_hf, _ = _run_cli([
            'pipe',
            '--from',
            src,
            '--to',
            dst,
            '--infer',
            'hf:task=text-classification,model=org/m',
            '--infer-fields',
            'text',
            '--json',
        ])
        self.assertEqual(rc_hf, 4, out_hf)

        rc_vx, out_vx, _ = _run_cli([
            'pipe',
            '--from',
            src,
            '--to',
            dst,
            '--infer',
            'vertexai:endpoint_id=123,project=p,location=us-central1',
            '--json',
        ])
        self.assertEqual(rc_vx, 4, out_vx)

  def test_vertexai_payload_guard_and_cumulative_batch_splitting(self):
    with tempfile.TemporaryDirectory() as tmp:
      log_path = os.path.join(tmp, 'vertex_calls.json')
      # Use max_payload_bytes=1000 so tests run instantaneously in memory!
      cfg = infer.parse_infer_config({
          'provider': 'vertexai',
          'endpoint_id': '123',
          'project': 'p',
          'location': 'us-central1',
          'max_payload_bytes': 1000,
          'max_batch_size': 10,
          'flatten': True,
      })

      rows = [
          beam.Row(id=1, blob='a' * 600),  # ~620B (< 1000B)
          beam.Row(id=2,
                   blob='b' * 600),  # ~620B -> forces flush before adding!
          beam.Row(id=3, blob='x' * 1200),  # > 1000B -> routed pre-RPC to DLQ!
          beam.Row(id=4, blob='c' * 100),  # ~120B -> batches with id=2
      ]

      with beam.Pipeline(
          options=PipelineOptions(['--runner=FnApiRunner'])) as p:
        res = (
            p
            | beam.Create(rows)
            | infer.CliInfer(
                cfg,
                error_handling={'output': 'errors'},
                handler_factory=lambda _c: _RecordingFakeModelHandler(log_path),
            ))
        assert_that(
            res['good']
            | 'Ids' >> beam.Map(lambda r: (r.id, r.prediction_label)),
            equal_to([(1, 'ALLOW'), (2, 'ALLOW'), (4, 'ALLOW')]),
            label='CheckGood',
        )
        assert_that(
            res['errors'] | 'ErrTypes' >> beam.Map(lambda r: r.error_type),
            equal_to(['PayloadTooLargeError']),
            label='CheckDlq',
        )

      with open(log_path, encoding='utf-8') as f:
        events = json.load(f)
      batch_lens = [
          e['batch_len'] for e in events if e['op'] == 'run_inference'
      ]
      # 3 valid rows total; row 1 (620B) + row 2 (620B) > 1000B so they split!
      self.assertEqual(sum(batch_lens), 3)
      self.assertTrue(all(bl <= 2 for bl in batch_lens), batch_lens)

  def test_singleton_bisect_isolates_poison_row_without_model_reload(self):
    with tempfile.TemporaryDirectory() as tmp:
      log_path = os.path.join(tmp, 'bisect_calls.json')
      cfg = infer.parse_infer_config({
          'provider': 'vertexai',
          'endpoint_id': '123',
          'project': 'p',
          'location': 'us-central1',
          'max_batch_size': 10,
          'flatten': True,
          'output_columns': ['risk_band:str'],
          'dlq_redact_fields': ['ssn'],
      })

      rows = [
          beam.Row(id=1, note='clean_1', ssn='111-11-1111'),
          beam.Row(id=2, note='POISON_ROW', ssn='999-00-1234'),
          beam.Row(id=3, note='clean_3', ssn='333-33-3333'),
      ]

      p = beam.Pipeline(options=PipelineOptions(['--runner=FnApiRunner']))
      res = (
          p
          | beam.Create(rows)
          | infer.CliInfer(
              cfg,
              error_handling={'output': 'errors'},
              handler_factory=lambda _c: _RecordingFakeModelHandler(log_path),
          ))
      assert_that(
          res['good']
          | beam.Map(lambda r: (r.id, r.prediction_score, r.risk_band)),
          equal_to([(1, 0.88, 'LOW'), (3, 0.88, 'LOW')]),
          label='CheckHealthy',
      )
      assert_that(
          res['errors'] | beam.Map(lambda r: (r.error, r.raw_payload)),
          beam.Map(lambda x: x)  # no-op check via custom matcher below
          if False else equal_to(res['errors'])  # replaced below
      ) if False else None

      # Capture DLQ contents deterministically via file or Map check:
      assert_that(
          res['errors']
          | 'CheckScrubbed' >> beam.Map(
              lambda r: (
                  'SECRET_SSN_999-00-1234_DO_NOT_LOG' not in r.error,
                  '999-00-1234' not in r.raw_payload,
                  '<REDACTED>' in r.raw_payload, )),
          equal_to([(True, True, True)]),
          label='CheckPiiScrubbed',
      )
      pres = p.run()
      pres.wait_until_finish()

      metrics = jobs.extract_beam_cli_counters(pres)
      self.assertEqual(metrics.get('inference_rows'), 2)
      self.assertEqual(metrics.get('inference_errors'), 1)
      self.assertGreaterEqual(metrics.get('inference_batches', 0), 1)
      self.assertIn('inference_latency_ms', metrics)

      with open(log_path, encoding='utf-8') as f:
        events = json.load(f)
      load_calls = [e for e in events if e['op'] == 'load_model']
      self.assertEqual(len(load_calls), 1, events)

  def test_explode_failed_batch_dofn_preserves_per_row_timestamps(self):
    cfg = infer.parse_infer_config({'provider': 'mock'})
    dofn = infer._ExplodeFailedBatchDoFn(cfg)
    gw = window.GlobalWindow()
    failed_batch = [
        (({
            'id': 10, 'val': 'a'
        }, 100, gw), {
            'val': 'a'
        }),
        (({
            'id': 20, 'val': 'b'
        }, 200, gw), {
            'val': 'b'
        }),
    ]
    exploded = list(
        dofn.process(
            (failed_batch, "RuntimeError: 'SUPER_SECRET_PAYLOAD_1234567890'")))
    self.assertEqual(len(exploded), 2)
    self.assertEqual(exploded[0].timestamp, 100)
    self.assertEqual(exploded[1].timestamp, 200)
    self.assertNotIn('SUPER_SECRET_PAYLOAD_1234567890', exploded[0].value.error)
    self.assertIn('"id": 10', exploded[0].value.raw_payload)
    self.assertIn('"id": 20', exploded[1].value.raw_payload)

  def test_event_time_timestamps_preserved_into_fixed_windows(self):
    cfg = infer.parse_infer_config({
        'provider': 'mock',
        'score': 0.95,
        'max_batch_size': 16,
        'flatten': True,
    })
    items = [
        TimestampedValue(beam.Row(id=1, txt='a'), 10),
        TimestampedValue(beam.Row(id=2, txt='b'), 25),
        TimestampedValue(beam.Row(id=3, txt='c'), 75),
    ]
    with beam.Pipeline(options=PipelineOptions(['--runner=FnApiRunner'])) as p:
      window_counts = (
          p
          | beam.Create(items)
          | infer.CliInfer(cfg)
          | beam.WindowInto(window.FixedWindows(60))
          | beam.Map(lambda r: ('w', 1))
          | beam.CombinePerKey(sum)
          | beam.Map(
              lambda kv, w=beam.DoFn.WindowParam:
              (int(w.start), int(w.end), kv[1])))
      assert_that(window_counts, equal_to([(0, 60, 2), (60, 120, 1)]))


class CliInferEndToEndPipeTest(unittest.TestCase):
  def test_full_tier1_pipe_dedup_enrich_infer_nested_and_flatten_to_parquet(
      self):
    with tempfile.TemporaryDirectory() as tmp:
      events_path = os.path.join(tmp, 'events.jsonl')
      dims_path = os.path.join(tmp, 'dims.jsonl')
      out_nested = os.path.join(tmp, 'out_nested.parquet')
      dlq_nested = os.path.join(tmp, 'dlq_nested.jsonl')
      out_flat = os.path.join(tmp, 'out_flat.parquet')
      dlq_flat = os.path.join(tmp, 'dlq_flat.jsonl')

      with open(dims_path, 'w', encoding='utf-8') as f:
        f.write('{"user_id": "u1", "mult": 2.0}\n')
        f.write('{"user_id": "u2", "mult": 1.0}\n')
        f.write('{"user_id": "u3", "mult": 1.0}\n')

      # 6 rows:
      # - e1 (u1, amount=40) -> mult=2.0 -> score=0.80 >= 0.5 -> WRITTEN
      # - e1 duplicate -> DROPPED by --dedupe-by
      # - e2 (amount=-5) -> FILTERED by --where
      # - e3 (u2, amount=20) -> mult=1.0 -> score=0.20 < 0.5 -> FILTERED by --post-where
      # - e4 (u3, note=POISON) -> DLQ by --infer (after singleton bisect!)
      # - e5 (u1, amount=30) -> mult=2.0 -> score=0.60 >= 0.5 -> WRITTEN
      rows = [
          {
              'event_id': 'e1',
              'raw_uid': ' U1 ',
              'amount': 40.0,
              'note': 'ok',
              'ssn': '111'
          },
          {
              'event_id': 'e1',
              'raw_uid': ' U1 ',
              'amount': 40.0,
              'note': 'dup',
              'ssn': '111'
          },
          {
              'event_id': 'e2',
              'raw_uid': 'u2',
              'amount': -5.0,
              'note': 'neg',
              'ssn': '222'
          },
          {
              'event_id': 'e3',
              'raw_uid': 'u2',
              'amount': 20.0,
              'note': 'low',
              'ssn': '333'
          },
          {
              'event_id': 'e4',
              'raw_uid': 'u3',
              'amount': 90.0,
              'note': 'POISON',
              'ssn': '999-SECRET'
          },
          {
              'event_id': 'e5',
              'raw_uid': 'u1',
              'amount': 30.0,
              'note': 'ok2',
              'ssn': '555'
          },
      ]
      with open(events_path, 'w', encoding='utf-8') as f:
        for r in rows:
          f.write(json.dumps(r) + '\n')

      # 1) Nested `prediction` struct mode (`prediction.score >= 0.5` in --post-where)
      rc1, out1, err1 = _run_cli([
          'pipe',
          '--from',
          events_path,
          '--to',
          out_nested,
          '--dedupe-by',
          'event_id',
          '--where',
          'amount > 0',
          '--pre-select',
          '*, lower(strip(raw_uid)) AS user_id',
          '--enrich',
          f'file:path={dims_path},key=user_id,columns=[mult:float]',
          '--infer',
          'mock:mode=linear,score_col=amt,scale=0.02,threshold=0.5,pos_label=HIGH,neg_label=LOW,fail_batch_once=true,fail_on=POISON',
          '--infer-fields',
          'amount AS amt, note AS note_txt',
          '--post-where',
          'prediction.score >= 0.5',
          '--select',
          'event_id, user_id, mult, prediction.score AS risk_score, prediction.label AS risk_label',
          '--dlq',
          dlq_nested,
          '--dlq-redact-fields',
          'ssn',
          '--assert-counts',
          '--json',
      ])
      self.assertEqual(rc1, 0, f'STDOUT: {out1}\nSTDERR: {err1}')
      env1 = json.loads(out1)
      m1 = env1['metrics']
      self.assertEqual(m1['rows_read'], 6)
      self.assertEqual(m1['duplicates_dropped'], 1)
      self.assertEqual(m1['dlq_rows'], 1)
      self.assertEqual(
          m1['rows_filtered_out'], 3)  # 1 dup + 1 where + 1 post-where
      self.assertEqual(m1['rows_written'], 2)
      self.assertEqual(
          m1['inference_rows'], 3)  # e1, e3, e5 succeeded inference
      self.assertEqual(m1['inference_errors'], 1)  # e4 POISON

      tbl1 = pq.read_table(out_nested).to_pylist()
      by_id1 = {r['event_id']: r for r in tbl1}
      self.assertEqual(set(by_id1), {'e1', 'e5'})
      self.assertAlmostEqual(by_id1['e1']['risk_score'], 0.8)
      self.assertEqual(by_id1['e1']['risk_label'], 'HIGH')

      with open(dlq_nested, encoding='utf-8') as f:
        dlq_recs = [json.loads(ln) for ln in f if ln.strip()]
      self.assertEqual(len(dlq_recs), 1)
      self.assertNotIn('999-SECRET', dlq_recs[0]['raw_payload'])
      self.assertIn('<REDACTED>', dlq_recs[0]['raw_payload'])

      # 2) `--infer-flatten` mode writing directly to Parquet (including nested prediction cols)
      rc2, out2, err2 = _run_cli([
          'pipe',
          '--from',
          events_path,
          '--to',
          out_flat,
          '--dedupe-by',
          'event_id',
          '--where',
          'amount > 0',
          '--pre-select',
          '*, lower(strip(raw_uid)) AS user_id',
          '--enrich',
          f'file:path={dims_path},key=user_id,columns=[mult:float]',
          '--infer',
          'mock:mode=linear,score_col=amt,scale=0.02,threshold=0.5,fail_on=POISON',
          '--infer-fields',
          'amount AS amt, note',
          '--infer-tag',
          'fraud',
          '--infer-flatten',
          '--post-where',
          'fraud_score >= 0.5',
          '--dlq',
          dlq_flat,
          '--assert-counts',
          '--json',
      ])
      self.assertEqual(rc2, 0, f'STDOUT: {out2}\nSTDERR: {err2}')
      tbl2 = pq.read_table(out_flat).to_pylist()
      self.assertEqual(len(tbl2), 2)
      self.assertIn('fraud_score', tbl2[0])
      self.assertIn('fraud_label', tbl2[0])
      self.assertIn('fraud_model_id', tbl2[0])

  def test_opt_file_0600_redaction_check_plan_and_package_auto(self):
    with tempfile.TemporaryDirectory() as tmp:
      src = os.path.join(tmp, 'in.jsonl')
      with open(src, 'w', encoding='utf-8') as f:
        f.write('{"id": 1, "text": "great product"}\n')
      dst = os.path.join(tmp, 'out.parquet')
      opt_file = os.path.join(tmp, 'infer_secrets.yaml')
      with open(opt_file, 'w', encoding='utf-8') as f:
        f.write('hf_token: hf_SuperSecretTokenValue1234567890\n')

      if os.name == 'posix':
        os.chmod(opt_file, 0o644)
        rc_bad, out_bad, _ = _run_cli([
            'pipe',
            '--from',
            src,
            '--to',
            dst,
            '--infer',
            'hf:task=text-classification,model=org/m@' + ('d' * 40),
            '--infer-fields',
            'text',
            '--infer-opt-file',
            opt_file,
            '--dry-run',
            '--json',
        ])
        self.assertEqual(rc_bad, 2, out_bad)
        os.chmod(opt_file, 0o600)

      rc_ok, out_ok, _ = _run_cli([
          'pipe',
          '--from',
          src,
          '--to',
          dst,
          '--infer',
          'hf:task=text-classification,model=org/m@' + ('d' * 40),
          '--infer-fields',
          'text',
          '--infer-opt-file',
          opt_file,
          '--dry-run',
      ])
      self.assertEqual(rc_ok, 0, out_ok)
      self.assertNotIn('hf_SuperSecretTokenValue1234567890', out_ok)
      self.assertTrue(
          '<REDACTED_SECRET>' in out_ok or '<redacted>' in out_ok, out_ok)

      yaml_path = os.path.join(tmp, 'infer_pipe.yaml')
      with open(yaml_path, 'w', encoding='utf-8') as f:
        f.write(out_ok)

      # Offline + --plan check succeeds WITHOUT --trust-providers!
      chk = check_cmd.run_check(
          yaml_path, expand_plan=True, trust_providers=False)
      self.assertTrue(chk['valid'])
      self.assertEqual(chk['mode'], 'expanded_plan')

      # beam package --sdk=auto selects PYTHON and detects transformers + torch!
      import yaml as pyyaml
      doc = pyyaml.safe_load(out_ok)
      pkgs = package_mod._detect_yaml_extra_packages(doc)
      self.assertIn('transformers', pkgs)
      self.assertIn('torch', pkgs)
      pkg_res = package_mod.run_package(
          yaml_path,
          to_location=os.path.join(tmp, 'spec.json'),
          sdk='auto',
          output_dir=os.path.join(tmp, 'pkg_bundle'),
          dry_run=True,
      )
      self.assertEqual(pkg_res['sdk_language'], 'PYTHON')
      self.assertTrue(
          any('transformers' in w for w in pkg_res['warnings']),
          pkg_res['warnings'])


if __name__ == '__main__':
  unittest.main()
