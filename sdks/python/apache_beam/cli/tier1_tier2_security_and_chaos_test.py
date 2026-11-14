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

"""Adversarial, security, chaos, and composition tests across Tier 1 & Tier 2."""

from __future__ import annotations

import contextlib
import io
import json
import os
import shutil
import tempfile
import unittest
from unittest import mock

import pyarrow.parquet as pq

import apache_beam as beam
from apache_beam.cli import asof_join
from apache_beam.cli import cep
from apache_beam.cli import check_cmd
from apache_beam.cli import dlq
from apache_beam.cli import expressions
from apache_beam.cli import interval_join
from apache_beam.cli import main as cli_main
from apache_beam.cli import pipe_spec
from apache_beam.cli import uris
from apache_beam.runners.portability.fn_api_runner import fn_runner
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


def _run_cli(argv: list[str]) -> tuple[int, str, str]:
  out_buf = io.StringIO()
  err_buf = io.StringIO()
  with mock.patch.dict(os.environ, {'BEAM_NO_CONFIG': '1'}, clear=False):
    with contextlib.redirect_stdout(out_buf), contextlib.redirect_stderr(
        err_buf):
      rc = cli_main.main(argv)
  return rc, out_buf.getvalue(), err_buf.getvalue()


class Tier1Tier2SecurityAndAdversarialTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam-t12-sec-')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

  def test_check_plan_blocks_getattr_string_concat_rce_and_bare_getattr_alias(
      self):
    # Adversarial attempts to bypass `node.attr.startswith('__')` via dynamic
    # `getattr` string concatenation, `type()`, or first-class `getattr` aliasing!
    rce_payloads = [
        "lambda row: getattr(getattr((), '_' + '_class_' + '_'), '_' + '_bases_' + '_')[0]",
        "lambda row: getattr(row, '__class__')",
        "lambda row: getattr(row, 'gi_frame')",
        "lambda row: (f := getattr) and f(row, '__class__')",
        "lambda row: type(row).__mro__",
        "def fn(row):\n  g = getattr\n  return g(row, 'x')\n",
        "lambda row: hasattr(row, '_' + '_subclasses__')",
    ]
    for payload in rce_payloads:
      with self.subTest(payload=payload):
        self.assertFalse(check_cmd._is_safe_inline_callable(payload))
        yaml_p = os.path.join(self.tmp, 'evil_callable.yaml')
        with open(yaml_p, 'w', encoding='utf-8') as f:
          f.write(
              'pipeline:\n'
              '  transforms:\n'
              '    - type: Create\n'
              '      config:\n'
              '        elements: [{x: 1}]\n'
              '    - type: MapToFields\n'
              '      input: Create\n'
              '      config:\n'
              '        language: python\n'
              '        fields:\n'
              '          y:\n'
              f'            callable: {json.dumps(payload)}\n')
        with self.assertRaises(check_cmd.CheckError):
          check_cmd.run_check(yaml_p, expand_plan=True, trust_providers=False)

    # Legitimate CLI-compiled `--where` and redacted DLQ normalizers STILL pass!
    w_cfg, _ = expressions.compile_where("lower(status) == 'ok' AND amount > 10")
    self.assertTrue(
        check_cmd._is_safe_inline_callable(w_cfg['keep']['callable']))
    norm_t = dlq.build_dlq_normalizer_transform(
        'Norm', 'In.errors', 'Filter', redact_fields=('ssn', 'secret'))
    for f_entry in norm_t['config']['fields'].values():
      self.assertTrue(check_cmd._is_safe_inline_callable(f_entry['callable']))

  def test_nested_chain_and_composite_cannot_bypass_cli_security_validators(
      self):
    # Wrapping malicious `CliEnrich` (SSRF endpoint), `CliInfer` (`env_vars`),
    # `CliAsofJoin` (`right` join), or `CliPatternMatch` (`A+` ReDoS) inside a
    # nested `type: chain` or `type: composite` MUST still fail `beam check`!
    nested_attacks = [
        (
            'CliEnrich',
            'handler: vertexai\n'
            '            fields: [uid]\n'
            '            project: p\n'
            '            location: us-central1\n'
            '            feature_store: fs\n'
            '            feature_view: fv\n'
            '            api_endpoint: https://169.254.169.254/latest/meta-data',
        ),
        (
            'CliInfer',
            'provider: hf\n'
            '            task: text-classification\n'
            '            model: org/m@0123456789abcdef0123456789abcdef01234567\n'
            '            env_vars: {LD_PRELOAD: /tmp/evil.so}',
        ),
        (
            'CliAsofJoin',
            'key: k\n            type: right',
        ),
        (
            'CliPatternMatch',
            'key: k\n            seq: "A+ -> B within 5m"',
        ),
    ]
    for idx, (t_type, cfg_yaml) in enumerate(nested_attacks):
      with self.subTest(t_type=t_type):
        yp = os.path.join(self.tmp, f'nested_{idx}.yaml')
        with open(yp, 'w', encoding='utf-8') as f:
          f.write(
              'pipeline:\n'
              '  transforms:\n'
              '    - type: chain\n'
              '      name: NestedAttack\n'
              '      transforms:\n'
              '        - type: Create\n'
              '          config:\n'
              '            elements: [{uid: "u1", k: "k1"}]\n'
              f'        - type: {t_type}\n'
              '          config:\n'
              f'            {cfg_yaml}\n')
        with self.assertRaises(check_cmd.CheckError):
          check_cmd.run_check(yp, expand_plan=False, trust_providers=False)

    # Raw Beam YAML `RunInference` and `Enrichment` require `--trust-providers` on `--plan`!
    for raw_t in ('RunInference', 'Enrichment'):
      yp = os.path.join(self.tmp, f'raw_{raw_t}.yaml')
      with open(yp, 'w', encoding='utf-8') as f:
        f.write(
            'pipeline:\n'
            '  transforms:\n'
            '    - type: Create\n'
            '      config:\n'
            '        elements: [{x: 1}]\n'
            f'    - type: {raw_t}\n'
            '      input: Create\n'
            '      config:\n'
            '        foo: bar\n')
      with self.assertRaises(check_cmd.CheckError):
        check_cmd.run_check(yp, expand_plan=True, trust_providers=False)

  @unittest.skipIf(os.name != 'posix', 'POSIX symlink and FIFO checks')
  def test_symlink_secret_opt_file_and_fifo_enrichment_blocked(self):
    # 1. Symlink masking a secret-bearing opt-file is rejected!
    real_secret = os.path.join(self.tmp, 'real_secret.yaml')
    with open(real_secret, 'w', encoding='utf-8') as f:
      f.write('redis_password: SuperSecretPassword123\n')
    os.chmod(real_secret, 0o600)
    sym_secret = os.path.join(self.tmp, 'sym_secret.yaml')
    os.symlink(real_secret, sym_secret)
    with self.assertRaises(uris.UriError) as ctx:
      uris.parse_kv_options(None, sym_secret, '--enrich-opt')
    self.assertIn('symbolic link', str(ctx.exception))

    # 2. POSIX FIFO passed to `file:` enrichment is rejected immediately without hanging!
    fifo_path = os.path.join(self.tmp, 'hang.fifo')
    os.mkfifo(fifo_path)
    from apache_beam.cli import enrich
    ecfg = enrich.parse_enrich_config({
        'handler': 'file',
        'path': fifo_path,
        'fields': ['id'],
        'columns': ['tier'],
    })
    caller = enrich._FileEnrichmentCaller(ecfg)
    with self.assertRaises((FileNotFoundError, ValueError)):
      caller.__enter__()

  def test_poison_pill_nan_inf_bool_and_extreme_timestamps_route_to_dlq(self):
    # Microseconds (`1.7e15`) and nanoseconds (`1.7e18`) scale cleanly to epoch seconds:
    self.assertAlmostEqual(
        interval_join.extract_record_timestamp({'ts': 1_700_000_000_000_000},
                                               'ts'),
        1_700_000_000.0,
    )
    self.assertAlmostEqual(
        interval_join.extract_record_timestamp(
            {'ts': 1_700_000_000_000_000_000}, 'ts'),
        1_700_000_000.0,
    )

    # Boolean, NaN, Inf, and 1e50 / year-55840 overflow raise ValueError!
    for poison in (
        True,
        False,
        float('nan'),
        float('inf'),
        float('-inf'),
        1e50,
        -1e50,
        '1e40',
        '99999-01-01T00:00:00Z',
    ):
      with self.subTest(poison=poison):
        with self.assertRaises(ValueError):
          interval_join.extract_record_timestamp({'ts': poison}, 'ts')

    # End-to-end CEP pipeline: poison-pill `1e50` and `None` timestamps route to `--dlq`
    # with `<REDACTED>` secret instead of crashing protobuf coders or `fromtimestamp`!
    in_p = os.path.join(self.tmp, 'poison_cep.jsonl')
    out_p = os.path.join(self.tmp, 'good_cep.parquet')
    dlq_p = os.path.join(self.tmp, 'dlq_cep.jsonl')
    with open(in_p, 'w', encoding='utf-8') as f:
      f.write(
          '{"uid": "u1", "ts": "1e50", "event_type": "login", "ssn": "SSN-POISON-1"}\n'
      )
      f.write(
          '{"uid": null, "ts": "100", "event_type": "login", "ssn": "SSN-NULL-KEY"}\n'
      )
      f.write(
          '{"uid": "u1", "ts": "100", "event_type": "login", "ssn": "SSN-OK-1"}\n'
      )
      f.write(
          '{"uid": "u1", "ts": "110", "event_type": "purchase", "ssn": "SSN-OK-2"}\n'
      )

    rc, out, err = _run_cli([
        'pipe',
        '--from',
        in_p,
        '--to',
        out_p,
        '--pattern',
        'key=uid,timestamp_col=ts,seq=login -> purchase within 30s',
        '--dlq',
        dlq_p,
        '--dlq-redact-fields',
        'ssn',
        '--json',
    ])
    self.assertEqual(rc, 0, f'STDOUT: {out}\nSTDERR: {err}')
    env = json.loads(out)
    self.assertEqual(env['metrics']['rows_written'], 1)
    self.assertEqual(env['metrics']['dlq_rows'], 2)
    with open(dlq_p, encoding='utf-8') as f:
      dlq_txt = f.read()
    self.assertNotIn('SSN-POISON-1', dlq_txt)
    self.assertNotIn('SSN-NULL-KEY', dlq_txt)
    self.assertIn('<REDACTED>', dlq_txt)

  def test_sql_null_key_non_matching_in_asof_join_and_null_key_isolation_in_cep(
      self):
    # 1. `NULL != NULL` across bounded `CliAsofJoin`, stateful `CliAsofJoin`, and SQL `ASOF LEFT JOIN`!
    trades = [
        beam.Row(sym=None, t_ts=100, qty=10),
        beam.Row(sym='AAPL', t_ts=100, qty=5),
    ]
    quotes = [
        beam.Row(sym=None, q_ts=95, price=999.0),
        beam.Row(sym='AAPL', q_ts=95, price=185.0),
    ]
    for force_st in (False, True):
      with self.subTest(force_stateful=force_st):
        p = beam.Pipeline(runner=fn_runner.FnApiRunner())
        l_pc = p | 'L' >> beam.Create(trades)
        r_pc = p | 'R' >> beam.Create(quotes)
        joined = {
            'left': l_pc, 'right': r_pc
        } | asof_join.CliAsofJoin(
            key='sym',
            left_time='t_ts',
            right_time='q_ts',
            type='left',
            force_stateful=force_st,
        )
        assert_that(
            joined | beam.Map(lambda r: (r.sym, r.price)),
            equal_to([
                (None, None),  # NULL != NULL -> MUST NOT match 999.0!
                ('AAPL', 185.0),
            ]),
        )
        p.run().wait_until_finish()

    # 2. Anonymous `user_id=None` events in CEP never cross-correlate into false matches!
    anon_events = [
        {
            'user_id': None, 'ts': 10, 'event_type': 'login'
        },
        {
            'user_id': None, 'ts': 15, 'event_type': 'purchase'
        },
        {
            'user_id': 'u_real', 'ts': 20, 'event_type': 'login'
        },
        {
            'user_id': 'u_real', 'ts': 25, 'event_type': 'purchase'
        },
    ]
    matches, cnt, _, _ = cep.match_bounded_patterns(
        anon_events,
        cep.parse_pattern_config({
            'key': 'user_id',
            'timestamp_col': 'ts',
            'seq': 'login -> purchase within 30s',
        }),
    )
    self.assertEqual(cnt, 1)
    self.assertEqual(matches[0]['user_id'], 'u_real')

  def test_nested_struct_and_error_message_pii_redaction_in_dlq(self):
    # Nested PII `{"profile": {"ssn": "999-88-7777"}}` AND error strings containing
    # `ssn='999-88-7777'` are BOTH scrubbed when `--dlq-redact-fields ssn` is set!
    in_p = os.path.join(self.tmp, 'nested_pii.jsonl')
    out_p = os.path.join(self.tmp, 'nested_out.jsonl')
    dlq_p = os.path.join(self.tmp, 'nested_dlq.jsonl')
    with open(in_p, 'w', encoding='utf-8') as f:
      f.write(
          '{"id": 1, "profile": {"ssn": "999-88-7777"}, "denom": 0}\n'
          '{"id": 2, "profile": {"ssn": "111-22-3333"}, "denom": 2}\n')

    rc, out, err = _run_cli([
        'pipe',
        '--from',
        in_p,
        '--to',
        out_p,
        '--select',
        'id, 100 / denom AS ratio',
        '--dlq',
        dlq_p,
        '--dlq-redact-fields',
        'ssn',
        '--json',
    ])
    self.assertEqual(rc, 0, f'STDOUT: {out}\nSTDERR: {err}')
    with open(dlq_p, encoding='utf-8') as f:
      dlq_content = f.read()
    self.assertNotIn('999-88-7777', dlq_content)
    self.assertIn('<REDACTED>', dlq_content)

  def test_adversarial_asof_sql_and_oversized_spec_dos_rejected(self):
    t_p = os.path.join(self.tmp, 't.jsonl')
    q_p = os.path.join(self.tmp, 'q.jsonl')
    out_p = os.path.join(self.tmp, 'out.jsonl')
    for p in (t_p, q_p):
      with open(p, 'w', encoding='utf-8') as f:
        f.write('{"k": "a", "ts": 10, "v": 1}\n')

    bad_sqls = [
        "SELECT * FROM t ASOF JOIN q ON t.k = q.k AND t.ts >= q.ts; DROP TABLE t",
        "ATTACH DATABASE ':memory:' AS evil",
        "SELECT * FROM t ASOF JOIN q ON t.k = q.k OR t.ts >= q.ts",
        "SELECT * FROM t FULL ASOF JOIN q ON t.k = q.k AND t.ts >= q.ts",
    ]
    for bsql in bad_sqls:
      with self.subTest(bsql=bsql):
        rc, _, _ = _run_cli([
            'pipe',
            '--from',
            f't={t_p}',
            '--join',
            f'q={q_p}',
            '--to',
            out_p,
            '--sql',
            bsql,
            '--dry-run',
        ])
        self.assertEqual(rc, 2)

    with self.assertRaises(ValueError):
      cep.parse_pattern_spec_str(
          'key=u,seq=' + ('A -> ' * 5000) + 'B within 5m')
    with self.assertRaises(ValueError):
      asof_join.parse_asof_join_spec_str('key=' + ('a' * 20000))

  def test_full_tier1_plus_tier2_composition_and_yaml_secret_redaction(self):
    # Full composition: `--dedupe-by` -> `--where` -> `--enrich file:` ->
    # `--infer mock:` -> `--post-where` -> `--pattern` -> `--window` + `--emit-pane-info` -> Parquet!
    events_p = os.path.join(self.tmp, 'stream_events.jsonl')
    users_p = os.path.join(self.tmp, 'users_dim.jsonl')
    out_p = os.path.join(self.tmp, 'composed_alerts.parquet')
    dlq_p = os.path.join(self.tmp, 'composed_dlq.jsonl')

    with open(users_p, 'w', encoding='utf-8') as f:
      f.write('{"uid": "u_vip", "tier": "vip"}\n')
      f.write('{"uid": "u_free", "tier": "free"}\n')

    stream_rows = [
        # Duplicate `e1` retry -> deduplicated by `--dedupe-by eid`!
        {
            'eid': 'e1', 'uid': 'u_vip', 'ts': 10, 'ev': 'login', 'amt': 10.0
        },
        {
            'eid': 'e1', 'uid': 'u_vip', 'ts': 10, 'ev': 'login', 'amt': 10.0
        },
        {
            'eid': 'e2',
            'uid': 'u_vip',
            'ts': 25,
            'ev': 'purchase',
            'amt': 500.0
        },
        # `u_free` filtered out by `--post-where "tier == 'vip'"` after enrichment!
        {
            'eid': 'e3', 'uid': 'u_free', 'ts': 12, 'ev': 'login', 'amt': 10.0
        },
        {
            'eid': 'e4',
            'uid': 'u_free',
            'ts': 28,
            'ev': 'purchase',
            'amt': 500.0
        },
    ]
    with open(events_p, 'w', encoding='utf-8') as f:
      for r in stream_rows:
        f.write(json.dumps(r) + '\n')

    rc, out, err = _run_cli([
        'pipe',
        '--from',
        events_p,
        '--to',
        out_p,
        '--timestamp-col',
        'ts',
        '--dedupe-by',
        'eid',
        '--where',
        'amt > 0',
        '--enrich',
        f'file:path={users_p},key=uid,columns=[tier:str]',
        '--infer',
        'mock:score=0.95,label=risky',
        '--infer-flatten',
        '--post-where',
        "tier == 'vip' AND prediction_score > 0.8",
        '--pattern',
        'key=uid,event_col=ev,seq=login -> purchase within 30s,select=[uid=purchase.uid,amt=purchase.amt,tier=purchase.tier]',
        '--window',
        'fixed:60s',
        '--allowed-lateness',
        '10s',
        '--emit-pane-info',
        '--dlq',
        dlq_p,
        '--json',
    ])
    self.assertEqual(rc, 0, f'STDOUT: {out}\nSTDERR: {err}')
    rows = pq.read_table(out_p).to_pylist()
    self.assertEqual(len(rows), 1)
    self.assertEqual(rows[0]['uid'], 'u_vip')
    self.assertEqual(rows[0]['amt'], 500.0)
    self.assertEqual(rows[0]['tier'], 'vip')
    self.assertIn('pane_timing', rows[0])

    # Secret redaction in `redact_yaml_text` / `to_yaml` across inline credentials:
    dirty_yaml = (
        'pipeline:\n'
        '  transforms:\n'
        '    - type: CliEnrich\n'
        '      config:\n'
        '        redis: rediss://:MyTopSecretRedisPass@10.0.0.5:6379\n'
        '        redis_password: MyTopSecretRedisPass\n'
        '        hf_token: hf_abcdefghijklmnopqrstuvwxyz123456\n'
        '        auth_hdr: Bearer ya29.a0AfH6SMBxSecretTokenValue12345\n')
    redacted = pipe_spec.redact_yaml_text(dirty_yaml)
    self.assertNotIn('MyTopSecretRedisPass', redacted)
    self.assertNotIn('hf_abcdefghijklmnopqrstuvwxyz123456', redacted)
    self.assertNotIn('ya29.a0AfH6SMBxSecretTokenValue12345', redacted)
    self.assertIn('<REDACTED_SECRET>', redacted)


if __name__ == '__main__':
  unittest.main()
