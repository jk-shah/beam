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

"""Hermetic tests for Beam CLI human & AI agent UX/DX ergonomics."""

import contextlib
import io
import json
import os
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from apache_beam.cli import main as cli_main


def _run_cli(argv: list[str]) -> tuple[int, str, str]:
  out = io.StringIO()
  err = io.StringIO()
  with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
    rc = cli_main.main(argv)
  return rc, out.getvalue(), err.getvalue()


def _run_cli_json(argv: list[str]) -> tuple[int, dict, str]:
  rc, out, err = _run_cli(argv + ['--json'])
  payload = json.loads(out)
  return rc, payload, err


class ErgonomicsTest(unittest.TestCase):
  def setUp(self):
    self._tmp = tempfile.TemporaryDirectory()
    self.tmp = self._tmp.name
    self.csv_path = os.path.join(self.tmp, 'orders.csv')
    with open(self.csv_path, 'w', encoding='utf-8') as fh:
      fh.write('id,uid,amount,status,tier,email\n')
      fh.write('1,u1,150,paid,gold,alice@example.com\n')
      fh.write('2,u2,40,paid,silver,bob@example.com\n')
      fh.write('3,u1,200,cancelled,gold,alice@example.com\n')
      fh.write('4,u3,100,paid,plat,\n')

  def tearDown(self):
    self._tmp.cleanup()

  def test_positional_and_flag_uri_symmetry_across_subcommands(self):
    # 1. `beam pipe SOURCE SINK` positional 2-arg form
    out_jsonl = os.path.join(self.tmp, 'pos_out.jsonl')
    rc, env, _ = _run_cli_json(['pipe', self.csv_path, out_jsonl])
    self.assertEqual(rc, 0)
    self.assertTrue(env['ok'])
    self.assertEqual(env['metrics']['rows_written'], 4)
    self.assertTrue(os.path.isfile(out_jsonl))

    # 2. Mixing `--from` + stray positional still fails fast with `--to` hint
    rc_bad, env_bad, _ = _run_cli_json(
        ['pipe', '--from', self.csv_path, 'accidental.jsonl'])
    self.assertEqual(rc_bad, 2)
    self.assertIn('--to', env_bad['error']['message'])

    # 3. `beam inspect --from URI`
    rc_i, env_i, _ = _run_cli_json(['inspect', '--from', self.csv_path])
    self.assertEqual(rc_i, 0)
    self.assertEqual(env_i['outputs']['format'], 'csv')

    # 4. `beam tail --from URI`
    rc_t, env_t, _ = _run_cli_json(['tail', '--from', out_jsonl, '-n', '2'])
    self.assertEqual(rc_t, 0)
    self.assertEqual(len(env_t['untrusted_data']['rows']), 2)

    # 5. `beam diff --left A --right B` AND `beam diff --from A --to B`
    #    (must NOT clobber B as an output report!)
    copy_csv = os.path.join(self.tmp, 'orders_copy.csv')
    with open(self.csv_path, encoding='utf-8') as src, open(
        copy_csv, 'w', encoding='utf-8') as dst:
      orig_bytes = src.read()
      dst.write(orig_bytes)

    rc_d1, env_d1, _ = _run_cli_json(
        ['diff', '--left', self.csv_path, '--right', copy_csv, '--key', 'id'])
    self.assertEqual(rc_d1, 0)
    self.assertTrue(env_d1['outputs']['match'])

    rc_d2, env_d2, _ = _run_cli_json(
        ['diff', '--from', self.csv_path, '--to', copy_csv, '--key', 'id'])
    self.assertEqual(rc_d2, 0)
    self.assertTrue(env_d2['outputs']['match'])
    with open(copy_csv, encoding='utf-8') as fh:
      self.assertEqual(fh.read(), orig_bytes)

    # 6. `beam catalog sync SRC TGT` positional form
    pq_src = os.path.join(self.tmp, 'cat_src.parquet')
    pq.write_table(pa.table({'id': [1, 2], 'v': ['a', 'b']}), pq_src)
    ice_tgt = os.path.join(self.tmp, 'ice_wh')
    rc_c, env_c, _ = _run_cli_json(
        ['catalog', 'sync', pq_src, ice_tgt, '--table', 'ns.tbl', '--dry-run'])
    self.assertEqual(rc_c, 0)
    self.assertEqual(env_c['outputs']['status'], 'dry_run')

    # 7. `beam schema A B --mode backward` (omitting `diff`, lowercase mode)
    pq_narrow = os.path.join(self.tmp, 'narrow.parquet')
    pq.write_table(
        pa.table({'id': pa.array([1, 2], type=pa.int32())}), pq_narrow)
    pq_wide = os.path.join(self.tmp, 'wide.parquet')
    pq.write_table(
        pa.table({
            'id': pa.array([1.5, 2.5], type=pa.float64()),
            'v': ['a', 'b'],
        }),
        pq_wide)
    rc_s, env_s, _ = _run_cli_json(
        ['schema', pq_wide, pq_narrow, '--mode', 'backward'])
    self.assertEqual(rc_s, 2)
    self.assertFalse(env_s['ok'])
    # `error` must be populated whenever `ok` is False, and `next_actions` must
    # live exclusively on the top-level envelope (not duplicated in `outputs`).
    self.assertIsNotNone(env_s['error'])
    self.assertEqual(env_s['error']['type'], 'SchemaContractViolationError')
    self.assertNotIn('next_actions', env_s['outputs'])
    self.assertTrue(len(env_s['next_actions']) >= 1)

    rc_s2, env_s2, _ = _run_cli_json(
        ['schema', 'diff', '--from', pq_src, '--to', pq_src, '--mode', 'full'])
    self.assertEqual(rc_s2, 0)
    self.assertTrue(env_s2['ok'])
    self.assertIsNone(env_s2['error'])

  def test_sinkless_preview_and_preview_flag_on_fast_and_runner_paths(self):
    # 1. Bare `beam pipe in.csv` -> composable NDJSON on stdout, zero temp leak
    rc, stdout_txt, stderr_txt = _run_cli(['pipe', self.csv_path, '-n', '2'])
    self.assertEqual(rc, 0, stderr_txt)
    lines = [json.loads(l) for l in stdout_txt.strip().splitlines()]
    self.assertEqual(len(lines), 2)
    self.assertEqual([r['id'] for r in lines], [1, 2])

    # 2. Sinkless `--json` preview with `--where`
    rc_j, env_j, _ = _run_cli_json(
        ['pipe', self.csv_path, '--where', 'amount >= 150'])
    self.assertEqual(rc_j, 0)
    self.assertEqual(env_j['outputs']['files'], [])
    self.assertEqual(env_j['outputs']['file_count'], 0)
    self.assertEqual([r['id'] for r in env_j['untrusted_data']['rows']], [1, 3])

    # 3. Sinkless preview through Beam runner (`--enrich` + `--infer`)
    dims_path = os.path.join(self.tmp, 'dims.jsonl')
    with open(dims_path, 'w', encoding='utf-8') as fh:
      fh.write(json.dumps({'uid': 'u1', 'vip': 10}) + '\n')
      fh.write(json.dumps({'uid': 'u2', 'vip': 1}) + '\n')
      fh.write(json.dumps({'uid': 'u3', 'vip': 5}) + '\n')
    rc_r, env_r, _ = _run_cli_json([
        'pipe',
        self.csv_path,
        '--where',
        'amount >= 150',
        '--enrich',
        f'file:path={dims_path},key=uid',
        '--infer',
        'mock:tag=risk,score_col=vip,scale=2.0',
        '--infer-fields',
        'amount,vip',
    ])
    self.assertEqual(rc_r, 0)
    self.assertEqual(env_r['outputs']['files'], [])
    rows_r = env_r['untrusted_data']['rows']
    self.assertEqual(len(rows_r), 2)
    self.assertEqual(rows_r[0]['risk']['score'], 20.0)

    # 4. `--preview` alongside `--to out.parquet` attaches `untrusted_data.rows`
    out_pq = os.path.join(self.tmp, 'with_preview.parquet')
    rc_p, env_p, _ = _run_cli_json(
        ['pipe', self.csv_path, out_pq, '--preview', '-n', '2'])
    self.assertEqual(rc_p, 0)
    self.assertEqual(env_p['outputs']['file_count'], 1)
    self.assertEqual(len(env_p['untrusted_data']['rows']), 2)

  def test_sql_predicate_auto_translation_and_column_typo_suggestions(self):
    # SQL muscle-memory operators (`=`, `<>`, `AND`, `IN`, `IS NOT NULL`) +
    # `--filter` alias work seamlessly without corrupting string literals.
    rc, env, _ = _run_cli_json([
        'pipe',
        self.csv_path,
        '--filter',
        "status <> 'cancelled' AND tier IN ('gold', 'plat') "
        "AND amount = 150 AND email IS NOT NULL",
    ])
    self.assertEqual(rc, 0)
    rows = env['untrusted_data']['rows']
    self.assertEqual([r['id'] for r in rows], [1])

    # Native SQL `LIKE` works in `--where`, while regex `RLIKE` hints `--sql`
    rc_like, env_like, _ = _run_cli_json(
        ['pipe', self.csv_path, '--where', "email LIKE '%@example.com'"])
    self.assertEqual(rc_like, 0)
    self.assertGreaterEqual(len(env_like['untrusted_data']['rows']), 1)

    rc_rlike, env_rlike, _ = _run_cli_json(
        ['pipe', self.csv_path, '--where', "email RLIKE '.*@example\\.com'"])
    self.assertEqual(rc_rlike, 2)
    self.assertIn('--sql', env_rlike['error']['message'])

    # Misspelled column on fixed-schema CSV fails in <50ms with did-you-mean!
    rc_col, env_col, _ = _run_cli_json(
        ['pipe', self.csv_path, '--where', 'amnt > 100'])
    self.assertEqual(rc_col, 2)
    self.assertIn("unknown column 'amnt'", env_col['error']['message'])
    self.assertIn("did you mean 'amount'?", env_col['error']['message'])

  def test_spec_key_aliases_and_unknown_key_did_you_mean_hints(self):
    # 1. `tag=` inside `--infer` works without repeating `--infer-tag`
    rc, env, _ = _run_cli_json([
        'pipe',
        self.csv_path,
        '--infer',
        'mock:tag=custom_score,score=0.75,output_type=float',
        '--infer-fields',
        'amount',
        '-n',
        '1',
    ])
    self.assertEqual(rc, 0)
    self.assertAlmostEqual(
        env['untrusted_data']['rows'][0]['custom_score'], 0.75)

    # 2. Unknown key in `--infer` / `--enrich` / `--pattern` / `--interval-join`
    #    raises crisp `did you mean ...?` instead of silently ignoring!
    rc_inf, env_inf, _ = _run_cli_json(
        ['pipe', self.csv_path, '--infer', 'mock:tag=s,batc_size=16'])
    self.assertEqual(rc_inf, 2)
    self.assertIn("did you mean 'batch_size'?", env_inf['error']['message'])

    dims = os.path.join(self.tmp, 'd.jsonl')
    with open(dims, 'w', encoding='utf-8') as fh:
      fh.write('{"uid": "u1", "x": 1}\n')
    rc_enr, env_enr, _ = _run_cli_json(
        ['pipe', self.csv_path, '--enrich', f'file:path={dims},key=uid,on_mis=dlq'])
    self.assertEqual(rc_enr, 2)
    self.assertIn("did you mean 'on_miss'?", env_enr['error']['message'])

    # 3. Aliases `key=`, `pattern=`, `window=` in `--pattern` and
    #    `key=`, `left_ts=`, `right_ts=`, `within=` in `--interval-join`
    out_yaml = os.path.join(self.tmp, 'ij.jsonl')
    rc_ij, env_ij, _ = _run_cli_json([
        'pipe',
        '--from',
        f'l={self.csv_path}',
        '--join',
        f'r={self.csv_path}',
        '--to',
        out_yaml,
        '--interval-join',
        'key=id,left_ts=amount,right_ts=amount,within=5s',
        '--dry-run',
    ])
    self.assertEqual(rc_ij, 0)
    self.assertIn('CliIntervalJoin', env_ij['outputs']['yaml'])

    rc_cep, env_cep, _ = _run_cli_json([
        'pipe',
        self.csv_path,
        out_yaml,
        '--timestamp-col',
        'amount',
        '--pattern',
        'key=uid,pattern=paid -> cancelled,event_col=status,window=60s',
        '--dry-run',
    ])
    self.assertEqual(rc_cep, 0)
    self.assertIn('CliPatternMatch', env_cep['outputs']['yaml'])

  def test_verify_with_key_sql_file_and_envelope_hygiene(self):
    # 1. `--verify --key id` on ordinary non-CDC data routes `id` to
    #    `verify_keys` instead of turning on CDC mode!
    out_v = os.path.join(self.tmp, 'verified.jsonl')
    rc_v, env_v, _ = _run_cli_json(
        ['pipe', self.csv_path, out_v, '--verify', '--key', 'id'])
    self.assertEqual(rc_v, 0)
    self.assertTrue(env_v['outputs']['verify']['match'])

    # 2. `--sql-file` and `--sql @file.sql`
    sql_path = os.path.join(self.tmp, 'agg.sql')
    with open(sql_path, 'w', encoding='utf-8') as fh:
      fh.write(
          "SELECT tier, COUNT(*) AS n FROM o WHERE status = 'paid' "
          "GROUP BY tier ORDER BY tier\n")
    rc_sf, env_sf, _ = _run_cli_json(
        ['pipe', '--from', f'o={self.csv_path}', '--sql-file', sql_path])
    self.assertEqual(rc_sf, 0)
    self.assertEqual(
        env_sf['untrusted_data']['rows'],
        [{
            'tier': 'gold', 'n': 1
        }, {
            'tier': 'plat', 'n': 1
        }, {
            'tier': 'silver', 'n': 1
        }],
    )
    rc_at, env_at, _ = _run_cli_json(
        ['pipe', '--from', f'o={self.csv_path}', '--sql', f'@{sql_path}'])
    self.assertEqual(rc_at, 0)
    self.assertEqual(
        env_at['untrusted_data']['rows'], env_sf['untrusted_data']['rows'])

    # 3. Existing output collision attaches `next_actions` with `--if-exists=replace`
    rc_dup, env_dup, _ = _run_cli_json(['pipe', self.csv_path, out_v])
    self.assertEqual(rc_dup, 2)
    self.assertTrue(
        any(
            '--if-exists=replace' in a.get('command', '')
            for a in env_dup['next_actions']))

    # 4. Unbounded local stream without `--wait-timeout` attaches `beam tail`
    rc_ub, env_ub, _ = _run_cli_json(
        ['pipe', 'pubsub://projects/demo/topics/events'])
    self.assertEqual(rc_ub, 2)
    self.assertTrue(
        any(
            'beam tail pubsub://projects/demo/topics/events' in a.get(
                'command', '') for a in env_ub['next_actions']))

    # 5. `beam check --json` deduplicates `warnings` (only top-level `warnings`)
    yaml_file = os.path.join(self.tmp, 'p.yaml')
    with open(yaml_file, 'w', encoding='utf-8') as fh:
      fh.write(
          'pipeline:\n'
          '  type: chain\n'
          '  transforms:\n'
          f'    - type: ReadFromCsv\n      config:\n        path: {self.csv_path}\n'
          '    - type: MapToFields\n'
          '      config:\n'
          '        language: python\n'
          '        fields:\n'
          '          dbl:\n'
          '            callable: "lambda row: row.amount * 2"\n'
          f'    - type: WriteToParquet\n      config:\n        path: {self.tmp}/out.parquet\n'
      )
    rc_chk, env_chk, _ = _run_cli_json(['check', yaml_file])
    self.assertEqual(rc_chk, 0)
    self.assertNotIn('warnings', env_chk['outputs'])
    self.assertTrue(len(env_chk['warnings']) >= 1)

  def test_unknown_flag_typo_suggestions_and_allow_unknown_options(self):
    rc_typo, env_typo, _ = _run_cli_json(
        ['pipe', self.csv_path, '--dedup-byy', 'uid'])
    self.assertEqual(rc_typo, 2)
    self.assertIn('--dedup-by', env_typo['error']['message'])

    rc_insp, env_insp, _ = _run_cli_json(
        ['inspect', self.csv_path, '--limt', '2'])
    self.assertEqual(rc_insp, 2)
    self.assertIn('--limit', env_insp['error']['message'])

    out_ok = os.path.join(self.tmp, 'allow_unk.jsonl')
    rc_allow, env_allow, _ = _run_cli_json([
        'pipe',
        self.csv_path,
        out_ok,
        '--allow-unknown-options',
        '--my-custom-user-opt=42',
    ])
    self.assertEqual(rc_allow, 0)
    self.assertEqual(env_allow['metrics']['rows_written'], 4)

  def test_limit_assert_counts_dedupe_dlq_uri_and_jinja_params(self):
    # `-n` / `--limit` conserves `--assert-counts` across both fast & runner paths
    out_lim = os.path.join(self.tmp, 'lim.jsonl')
    rc_l, env_l, _ = _run_cli_json(
        ['pipe', self.csv_path, out_lim, '-n', '2', '--assert-counts'])
    self.assertEqual(rc_l, 0)
    self.assertEqual(env_l['metrics']['rows_written'], 2)

    # `--dedupe-dlq URI` routes duplicate records to the target URI directly
    dedup_dlq = os.path.join(self.tmp, 'dups_dlq.jsonl')
    out_dedup = os.path.join(self.tmp, 'dedup_out.jsonl')
    rc_dd, env_dd, _ = _run_cli_json([
        'pipe',
        self.csv_path,
        out_dedup,
        '--dedupe-by',
        'uid',
        '--dedupe-dlq',
        dedup_dlq,
        '--assert-counts',
    ])
    self.assertEqual(rc_dd, 0)
    self.assertEqual(env_dd['metrics']['rows_written'], 3)
    self.assertEqual(env_dd['metrics']['dlq_rows'], 1)

    # `-p` / `--param KEY=VAL` works across `beam check` and `beam export-schedule`
    j_yaml = os.path.join(self.tmp, 'j.yaml')
    with open(j_yaml, 'w', encoding='utf-8') as fh:
      fh.write(
          'pipeline:\n'
          '  type: chain\n'
          '  transforms:\n'
          '    - type: ReadFromCsv\n'
          '      config:\n'
          '        path: "{{ src_path }}"\n'
          '    - type: WriteToJson\n'
          '      config:\n'
          f'        path: {self.tmp}/j_out.jsonl\n')
    rc_jc, env_jc, _ = _run_cli_json(
        ['check', j_yaml, '-p', f'src_path={self.csv_path}'])
    self.assertEqual(rc_jc, 0)
    self.assertTrue(env_jc['ok'])


if __name__ == '__main__':
  unittest.main()
