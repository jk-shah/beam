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

"""Round-2 adversarial security and multi-writer concurrency regression tests.

Covers all Critical/High/Medium security findings (`C-1`..`C-5`, `H-1`..`H-11`,
`M-1`..`M-5`) and P0/P1/P2 concurrency/race-isolation fixes (`P0-1`..`P0-4`,
`P1-1`..`P1-6`, `P2-1`) across `apache_beam.cli` and `apache_beam_cli_preview`.
"""

from __future__ import annotations

import contextlib
import datetime
import io
import json
import logging
import os
import shutil
import stat
import struct
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from pathlib import Path
from unittest import mock

import yaml

from apache_beam.cli import cep
from apache_beam.cli import check_cmd
from apache_beam.cli import config
from apache_beam.cli import dlq
from apache_beam.cli import enrich
from apache_beam.cli import expect
from apache_beam.cli import expressions
from apache_beam.cli import fast_local
from apache_beam.cli import infer
from apache_beam.cli import inspect_cmd
from apache_beam.cli import jobs
from apache_beam.cli import lakehouse
from apache_beam.cli import main as cli_main
from apache_beam.cli import output
from apache_beam.cli import package
from apache_beam.cli import pipe_spec
from apache_beam.cli import pipe_sql
from apache_beam.cli import scheduler
from apache_beam.cli import tail
from apache_beam.cli import uris


def _invoke_cli(
    argv: list[str],
    *,
    beam_home: str,
    stdin_text: str | None = None,
) -> tuple[int, str, str]:
  out_buf = io.StringIO()
  err_buf = io.StringIO()
  env_patch = {
      'BEAM_HOME': beam_home,
      'BEAM_CONFIG': os.path.join(beam_home, 'config.yaml'),
  }
  with contextlib.ExitStack() as stack:
    if os.environ.get('BEAM_HOME') != beam_home:
      stack.enter_context(mock.patch.dict(os.environ, env_patch))
    stack.enter_context(contextlib.redirect_stdout(out_buf))
    stack.enter_context(contextlib.redirect_stderr(err_buf))
    if stdin_text is not None:
      stack.enter_context(mock.patch('sys.stdin', io.StringIO(stdin_text)))
    rc = cli_main.main(argv)
  return rc, out_buf.getvalue(), err_buf.getvalue()


class JinjaRunAndSandboxSecurityTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam_r2_jinja_')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.beam_home = os.path.join(self.tmp, 'beam_home')
    os.makedirs(self.beam_home, mode=0o700, exist_ok=True)

  def test_jinja_blocks_filter_attribute_dunder_and_mul_dos(self):
    evil_templates = [
        "{{ items | selectattr('__class__') | list }}",
        "{{ items | map(attribute='__init__.__globals__') | list }}",
        "{{ items | groupby('__subclasses__') }}",
        "{{ 'x' | attr('__class__') }}",
        "{{ '%s' | format(secret) }}",
        "{{ 'A' * 100000 }}",
        "{{ 2 ** 64 }}",
        "{{ cycler(1, 2) }}",
        "{{ namespace(a=1) }}",
    ]
    for tpl in evil_templates:
      with self.subTest(tpl=tpl):
        with self.assertRaises(ValueError):
          output.validate_safe_jinja_template(tpl)

  def test_beam_run_blocks_yaml_bomb_with_jinja_and_shadow_flags(self):
    # YAML billion-laughs bomb combined with a `{{ v }}` Jinja marker (`C-1.3`).
    bomb_lines = ['a0: &a0 ["x", "x", "x", "x", "x", "x", "x", "x"]']
    for i in range(1, 9):
      prev = f'*a{i - 1}'
      bomb_lines.append(
          f'a{i}: &a{i} [{prev}, {prev}, {prev}, {prev}, {prev}, {prev}]')
    bomb_lines.append('pipeline:\n  transforms: [] # {{ env }}')
    bomb_path = os.path.join(self.tmp, 'bomb.yaml')
    Path(bomb_path).write_text('\n'.join(bomb_lines), encoding='utf-8')

    rc, _, err = _invoke_cli(
        ['--no-config', 'run', bomb_path, '-p', 'env=dev', '--dry-run'],
        beam_home=self.beam_home)
    self.assertEqual(rc, cli_main.EXIT_USAGE, msg=err)

    # Shadow `--yaml_pipeline` flag alongside FILE is rejected (`C-1.2`).
    clean_path = os.path.join(self.tmp, 'clean.yaml')
    Path(clean_path).write_text(
        'pipeline:\n  type: chain\n  transforms:\n'
        '    - type: Create\n      config: {elements: [{id: 1}]}\n',
        encoding='utf-8')
    rc2, _, err2 = _invoke_cli(
        [
            '--no-config',
            'run',
            clean_path,
            '--yaml_pipeline=pipeline: {transforms: []}',
        ],
        beam_home=self.beam_home)
    self.assertEqual(rc2, cli_main.EXIT_USAGE, msg=err2)
    self.assertIn('cannot pass --yaml_pipeline', err2)

    # `file://` URI input receives identical Jinja SSTI rejection (`C-1.2`).
    ssti_path = os.path.join(self.tmp, 'ssti.yaml')
    Path(ssti_path).write_text(
        "pipeline:\n  transforms:\n    - type: Create\n"
        "      config: {elements: [{x: \"{{ ''.__class__ }}\"}]}\n",
        encoding='utf-8')
    rc3, _, _ = _invoke_cli(
        ['--no-config', 'run', Path(ssti_path).as_uri(), '--dry-run'],
        beam_home=self.beam_home)
    self.assertEqual(rc3, cli_main.EXIT_USAGE)

  def test_beam_run_dry_run_redacts_secrets(self):
    secret_yaml = os.path.join(self.tmp, 'with_secret.yaml')
    Path(secret_yaml).write_text(
        'pipeline:\n  type: chain\n  transforms:\n'
        '    - type: Create\n'
        '      config:\n'
        '        elements: [{id: 1}]\n'
        'options:\n'
        '  redis_password: "SuperSecretRedisPassword123!"\n',
        encoding='utf-8')
    rc, out, err = _invoke_cli(
        ['--json', '--no-config', 'run', secret_yaml, '--dry-run'],
        beam_home=self.beam_home)
    # Whether dry-run validates or rejects unknown option, secret never leaks!
    combined = out + err
    self.assertNotIn('SuperSecretRedisPassword123!', combined)

  def test_allow_jinja_callable_blocks_quote_breakout_injection(self):
    for expr, params in (
        (
            'amount > {{ min_amt }}',
            {'min_amt': "0) or __import__('os').system('echo PWN') or (1"},
        ),
        (
            "country = '{{ region }}'",
            {'region': "US' or __import__('os').system('echo PWN') or '"},
        ),
    ):
      cfg, _ = expressions.compile_where(
          expr, known_columns=None, allow_jinja=True)
      rendered = output.safe_expand_jinja(json.dumps(cfg), params)
      loaded_cfg = json.loads(rendered)
      keep_spec = loaded_cfg['keep']
      if isinstance(keep_spec, dict) and 'callable' in keep_spec:
        # Must remain AST-safe and never inject `__import__` as executable code!
        self.assertTrue(
            check_cmd._is_safe_inline_callable(keep_spec['callable']),
            msg=keep_spec['callable'])


class CheckPlanAndFastLocalSandboxTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam_r2_sandbox_')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

  def test_check_plan_blocks_generic_language_cr_and_stdlib_attr_escapes(self):
    payloads = [
        # 1) Omitted `language:` (`generic` default) with external `path` hook
        {
            'pipeline': {
                'type': 'chain',
                'transforms': [
                    {
                        'type': 'Create', 'config': {
                            'elements': [{
                                'x': 1
                            }]
                        }
                    },
                    {
                        'type': 'MapToFields',
                        'config': {
                            'fields': {
                                'y': {
                                    'path': '/tmp/evil.py', 'name': 'fn'
                                }
                            }
                        },
                    },
                ],
            }
        },
        # 2) `AnomalyDetection` transform without `--trust-providers`
        {
            'pipeline': {
                'type': 'chain',
                'transforms': [
                    {
                        'type': 'Create', 'config': {
                            'elements': [{
                                'x': 1.0
                            }]
                        }
                    },
                    {
                        'type': 'AnomalyDetection', 'config': {
                            'detector': 'x'
                        }
                    },
                ],
            }
        },
        # 3) `\r` comment-stripping parser differential
        {
            'pipeline': {
                'type': 'chain',
                'transforms': [
                    {
                        'type': 'Create', 'config': {
                            'elements': [{
                                'x': 1
                            }]
                        }
                    },
                    {
                        'type': 'Filter',
                        'config': {
                            'language': 'python',
                            'keep': {
                                'callable': (
                                    "def f(r):\n"
                                    "  a = '''\n"
                                    "# \r ''' + str(__import__('os')) + '''\n"
                                    "  '''\n"
                                    "  return True\n")
                            },
                        },
                    },
                ],
            }
        },
        # 4) Transitive stdlib attribute escape (`re.sys` / `json.codecs`)
        {
            'pipeline': {
                'type': 'chain',
                'transforms': [
                    {
                        'type': 'Create', 'config': {
                            'elements': [{
                                'x': 1
                            }]
                        }
                    },
                    {
                        'type': 'MapToFields',
                        'config': {
                            'language': 'python',
                            'fields': {
                                'y': {
                                    'callable': (
                                        'def f(r):\n'
                                        '  import re\n'
                                        '  return str(re.sys.modules)\n')
                                }
                            },
                        },
                    },
                ],
            }
        },
    ]
    for idx, doc in enumerate(payloads):
      p = os.path.join(self.tmp, f'evil_{idx}.yaml')
      Path(p).write_text(yaml.safe_dump(doc), encoding='utf-8')
      with self.assertRaises(check_cmd.CheckError,
                             msg=f'case {idx} unexpectedly passed'):
        check_cmd.run_check(p, expand_plan=True, trust_providers=False)

  def test_fast_local_module_proxies_hide_sys_codecs_and_builtins(self):
    fn_ok = fast_local._compile_callable(
        'def f(r):\n'
        '  import json, hashlib, re\n'
        '  d = json.dumps({"k": re.sub(r"\\d+", "N", r.val)})\n'
        '  return hashlib.sha256(d.encode("utf-8")).hexdigest()[:8]\n')
    row = fast_local._rowify({'val': 'abc123'})
    self.assertEqual(len(fn_ok(row)), 8)

    for bad_src in (
        'def f(r):\n  import re\n  return str(re.sys)\n',
        'def f(r):\n  import json\n  return str(json.codecs)\n',
        'def f(r):\n  import datetime\n  return str(datetime.sys)\n',
    ):
      with self.subTest(bad_src=bad_src):
        with self.assertRaises((fast_local.FastPipeError, AttributeError)):
          bad_fn = fast_local._compile_callable(bad_src)
          bad_fn(row)

  def test_window_json_aggregations_and_backtick_columns_prevent_exec(self):
    with self.assertRaises((ValueError, pipe_spec.PipeSpecError)):
      pipe_spec._build_combine_config({
          'group_by': ['k'],
          'combine': {
              'evil': {
                  'fn': "__import__('os').system('id')",
                  'value': 'x',
              }
          },
      },
                                      force_python=True)

    # Backticked non-identifier column name emits `getattr(__row__, ...)`
    # callable instead of raw `expression:` interpolation (`C-4`).
    sel_cfg, _ = expressions.compile_select(
        "`1) + len(str(__row__)) + (0` AS val", known_columns=None)
    field_spec = sel_cfg['fields']['val']
    self.assertIsInstance(field_spec, dict)
    self.assertIn('callable', field_spec)
    self.assertIn('getattr(__row__', field_spec['callable'])

    # `--sql-engine=python` backticked column also lowers safely without exec.
    lowered = pipe_sql.try_lower_sql_to_python_yaml(
        'SELECT `weird col; 1+1` AS clean_col FROM s', ['s'])
    self.assertIsNotNone(lowered)
    dumped = json.dumps(lowered.spec)
    self.assertIn('getattr(__row__', dumped)

  def test_chained_replace_amplification_bounded_across_compilers(self):
    seed = 'a' * 512
    # 512 -> 51200 -> 5,120,000 (> 1 MiB safety limit)
    expr = "s.replace('a', '" + ('a' * 100) + "').replace('a', '" + (
        'a' * 100) + "')"

    sel_cfg, _ = expressions.compile_select(f'{expr} AS big', known_columns=None)
    fn = fast_local._compile_field(sel_cfg['fields']['big'])
    with self.assertRaises(Exception):
      fn(fast_local._rowify({'s': seed}))

    exp_code = expect.compile_expectation_expression(f"len({expr}) > 0")
    with self.assertRaises(Exception):
      expect.evaluate_record(exp_code, {'s': seed})

    cep_pred = cep._CompiledPredicate(f"len({expr}) > 0")
    with self.assertRaises(Exception):
      cep_pred({'s': seed})

  def test_sinkless_select_blocks_union_sql_injection(self):
    csv_in = os.path.join(self.tmp, 'orders.csv')
    Path(csv_in).write_text('id,amount\n1,10\n', encoding='utf-8')
    beam_home = os.path.join(self.tmp, 'bh')
    os.makedirs(beam_home, mode=0o700, exist_ok=True)
    rc, _, err = _invoke_cli(
        [
            '--no-config',
            'pipe',
            csv_in,
            '--select',
            'id FROM orders UNION ALL SELECT 999',
        ],
        beam_home=beam_home)
    self.assertEqual(rc, cli_main.EXIT_USAGE, msg=err)


class ReDoSAndDlqScrubbingSecurityTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam_r2_dlq_')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

  def test_no_exponential_redos_on_unclosed_backslash_quotes_or_large_blobs(
      self):
    adversarial_quote = 'password="' + ('\\' * 120)
    t0 = time.monotonic()
    _ = output.scrub_error_message(adversarial_quote)
    _ = output.redact_inline_secrets(adversarial_quote)
    _ = dlq.scrub_unparseable_dlq_text(adversarial_quote, ('password', ))
    large_no_at = 'A' * 150_000
    _ = dlq.scrub_unparseable_dlq_text(large_no_at, ('email', ))
    elapsed_ms = (time.monotonic() - t0) * 1000.0
    self.assertLess(elapsed_ms, 500.0, msg=f'scrubber took {elapsed_ms:.1f}ms')

  def test_truncated_json_and_malformed_csv_dlq_redaction_and_0600_mode(self):
    # 1) Unclosed truncated JSON quote with spaces and commas (`H-8.2`).
    trunc = '{"id": 1, "secret_note": "first_word, second_word TOP_SECRET_TAIL'
    scrubbed = dlq.scrub_unparseable_dlq_text(trunc, ('secret_note', ))
    self.assertNotIn('TOP_SECRET_TAIL', scrubbed)
    self.assertNotIn('second_word', scrubbed)

    # 2) End-to-end dirty CSV + DLQ redaction + `0600` file mode (`H-8.1`, `M-3`).
    dirty_csv = os.path.join(self.tmp, 'dirty.csv')
    dlq_out = os.path.join(self.tmp, 'dlq.jsonl')
    clean_out = os.path.join(self.tmp, 'clean.jsonl')
    Path(dirty_csv).write_text(
        'id,ssn_pin,amount\n'
        '1,PIN-OK-1,10.5\n'
        '2,SECRET-COL-MISMATCH,20.0,EXTRA_COL\n'
        '3,SECRET-TYPE-FAIL,NOT_A_FLOAT\n',
        encoding='utf-8')
    beam_home = os.path.join(self.tmp, 'bh')
    os.makedirs(beam_home, mode=0o700, exist_ok=True)
    rc, _, err = _invoke_cli(
        [
            '--json',
            '--no-config',
            'pipe',
            dirty_csv,
            clean_out,
            '--dlq',
            dlq_out,
            '--dlq-redact-fields',
            'ssn_pin',
        ],
        beam_home=beam_home)
    self.assertEqual(rc, 0, msg=err)
    dlq_text = Path(dlq_out).read_text(encoding='utf-8')
    self.assertNotIn('SECRET-COL-MISMATCH', dlq_text)
    self.assertNotIn('SECRET-TYPE-FAIL', dlq_text)
    if os.name == 'posix':
      mode = stat.S_IMODE(os.stat(dlq_out).st_mode)
      self.assertEqual(mode & 0o077, 0, msg=f'DLQ mode {oct(mode)} not 0600')

  def test_underscore_row_columns_and_expect_builtin_shadowing_safe(self):
    dlq.install_keyword_safe_row_hooks()
    from apache_beam.portability.api import schema_pb2
    from apache_beam.typehints import schemas
    sc = schema_pb2.Schema(
        id='test-underscore-cols',
        fields=[
            schema_pb2.Field(
                name='_asdict',
                type=schema_pb2.FieldType(atomic_type=schema_pb2.STRING)),
            schema_pb2.Field(
                name='_fields',
                type=schema_pb2.FieldType(atomic_type=schema_pb2.INT64)),
            schema_pb2.Field(
                name='normal',
                type=schema_pb2.FieldType(atomic_type=schema_pb2.STRING)),
        ])
    cls = schemas.named_tuple_from_schema(sc)
    inst = cls('v1', 42, 'ok')
    self.assertTrue(callable(inst._asdict))
    self.assertEqual(inst._asdict()['_asdict'], 'v1')
    self.assertEqual(inst._fields, ('_asdict', '_fields', 'normal'))

    # `expect.evaluate_record` with row columns named `len` and `_cli_exp_cmp` (`M-5.2`)
    # plus SQL 3-valued logic (`NOT`, `AND`, `OR`, `IN`, `*`, temporal `==`).
    code = expect.compile_expectation_expression("len(name) > 2 AND len > 0")
    self.assertTrue(
        expect.evaluate_record(
            code, {
                'name': 'Ada', 'len': 5, '_cli_exp_cmp': 'shadow_attempt'
            }))
    tvl_code = expect.compile_expectation_expression(
        "NOT (flag) OR (qty * 2 > 4 AND tag IN ('ok', NULL) "
        "AND ts == '2026-11-16T00:00:00Z')")
    self.assertTrue(
        expect.evaluate_record(
            tvl_code,
            {
                'flag': True,
                'qty': 3,
                'tag': 'ok',
                'ts': datetime.datetime(
                    2026, 11, 16, tzinfo=datetime.timezone.utc),
            },
        ))
    self.assertFalse(
        expect.evaluate_record(
            tvl_code,
            {
                'flag': True, 'qty': None, 'tag': 'miss', 'ts': None
            },
        ))
    with self.assertRaises(package.PackageError):
      package.extract_jinja_parameters(
          "options:\n  db_password: \"{{ db_password | default('hunter2') }}\"\n"
      )


class EndpointKafkaJdbcBqAndTailSecurityTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam_r2_ep_')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

  def test_kafka_and_jdbc_dangerous_properties_blocked(self):
    bad_kafka_props = [
        {
            'plugin.library.paths': '/tmp/evil.so'
        },
        {
            'sasl.kerberos.kinit.cmd': 'sh -c id'
        },
        {
            'sasl.oauthbearer.token.endpoint.url': 'http://169.254.169.254/tok'
        },
        {
            'config.providers': 'file'
        },
        {
            'config.providers.file.class': 'org.apache.kafka.common.config.provider.FileConfigProvider'
        },
        {
            'value.deserializer': 'com.evil.ExploitDeserializer'
        },
        {
            'sasl.jaas.config': 'com.sun.security.auth.module.JndiLoginModule required;'
        },
    ]
    for kp in bad_kafka_props:
      with self.subTest(kp=kp):
        with self.assertRaises(uris.UriError):
          uris.parse_endpoint(
              'kafka://broker:9092/events',
              is_read=True,
              options={'consumer_config': kp})

    with self.assertRaises(uris.UriError):
      uris.parse_endpoint(
          'kafka://broker:9092/events',
          is_read=True,
          options={'consumer_config': 'not_a_dict'})

    # JDBC `url` override and `str` / `;`-delimited `connection_properties` (`C-5.2`).
    with self.assertRaises(uris.UriError):
      uris.parse_endpoint(
          'postgres://dbhost:5432/app?table=users',
          is_read=True,
          options={'url': 'jdbc:postgresql://evil/db?socketFactory=Evil'})
    with self.assertRaises(uris.UriError):
      uris.parse_endpoint(
          'postgres://dbhost:5432/app?table=users',
          is_read=True,
          options={'connection_properties': 'ssl=true;socketFactory=org.Evil'})
    with self.assertRaises(uris.UriError):
      uris.parse_endpoint(
          'mysql://dbhost:3306/app?table=users',
          is_read=True,
          options={'connection_properties': 'allowLoadLocalInfile=true'})

  def test_bigquery_row_restriction_blocks_hash_raw_and_triple_quotes(self):
    injections = [
        'id = 1 # "\n OR 1=1 UNION ALL SELECT * FROM secret -- "',
        r"name = r'\' UNION ALL SELECT 1 --'",
        "name = '''a'b''' UNION ALL SELECT 1",
        "id = 1\nUNION ALL SELECT 1",
    ]
    for inj in injections:
      with self.subTest(inj=inj):
        with self.assertRaises(uris.UriError):
          uris.validate_bq_row_restriction(inj)

  def test_schema_registry_ssrf_basic_auth_and_jsonschema_guards(self):
    cache: dict = {}
    bad_urls = [
        'http://169.254.169.254/sr',
        'http://[::ffff:169.254.169.254]:8081',
        'http://metadata.google.internal.:8081',
        'https://sr.example.com:8081/path?inject=1',
        'https://sr.example.com:8081/path#frag',
        'https://user:pass@sr.example.com:8081',
    ]
    for u in bad_urls:
      with self.subTest(url=u):
        with self.assertRaises(tail.TailError):
          tail._fetch_confluent_schema_doc(u, 1, cache)

    # Cleartext HTTP Basic Auth to non-loopback host is refused (`H-4.2`).
    with self.assertRaises(tail.TailError):
      tail._fetch_confluent_schema_doc(
          'http://93.184.216.34:8081', 1, cache, basic_auth='u:secret')

    # JSON Schema with external `$ref` or nested-quantifier ReDoS `pattern` (`H-4.3`).
    raw_msg = b'\x00' + struct.pack('>I', 7) + b'{"name": "aaaaaaaaaaaaaaaa!"}'
    evil_schema_cache = {
        7: {
            '_beam_schema_type': 'JSON',
            'parsed': {
                'type': 'object',
                'properties': {
                    'name': {
                        'type': 'string', 'pattern': '^(a+)+$'
                    }
                },
            },
        }
    }
    t0 = time.monotonic()
    with self.assertRaises(tail.TailError):
      tail._decode_kafka_wire_message(
          raw_msg, 'https://localhost:8081', evil_schema_cache)
    self.assertLess((time.monotonic() - t0) * 1000.0, 500.0)

  def test_lakehouse_partition_traversal_biglake_auth_and_seq_file_guards(self):
    # Partition `p_name` traversal from untrusted `metadata.json` (`H-3.1`).
    bad_state = {
        'metadata': {
            'default-spec-id': 0,
            'partition-specs': [{
                'spec-id': 0,
                'fields': [{
                    'source-id': 1,
                    'field-id': 1000,
                    'name': '../../escaped_dir',
                    'transform': 'identity',
                }],
            }],
        }
    }
    with self.assertRaises(lakehouse.LakehouseError):
      lakehouse._resolve_iceberg_partition_plan(
          None, [{
              'id': 1, 'name': 'dt', 'type': 'string'
          }], bad_state)

    # Google ADC token allowed ONLY on `biglake.googleapis.com` (`H-3.2`).
    self.assertTrue(
        lakehouse._is_googleapis_https_uri(
            'https://biglake.googleapis.com/iceberg/v1/restcatalog'))
    self.assertTrue(
        lakehouse._is_googleapis_https_uri(
            'https://us-central1-biglake.googleapis.com/iceberg/v1/restcatalog')
    )
    self.assertFalse(
        lakehouse._is_googleapis_https_uri(
            'https://storage.googleapis.com/my-public-bucket'))
    self.assertFalse(
        lakehouse._is_googleapis_https_uri(
            'https://biglake.googleapis.com/iceberg#frag'))

    # `%2e%2e` URL-encoded traversal rejected (`H-3.3`).
    with self.assertRaises(lakehouse.LakehouseError):
      lakehouse._normalize_fs_root('file:///tmp/wh/%2e%2e/etc/passwd')

    # `beam.cli.cdc.seq-file` confined under `metadata/` (`M-3.3`).
    tdir = os.path.join(self.tmp, 'ice_seq')
    os.makedirs(os.path.join(tdir, 'metadata'), exist_ok=True)
    outside = os.path.join(self.tmp, 'outside_secret.json')
    Path(outside).write_text('{"leak": [1, "x"]}', encoding='utf-8')
    with self.assertRaises(lakehouse.LakehouseError):
      lakehouse._load_prior_cdc_seq_map(
          tdir,
          {
              'metadata': {
                  'properties': {
                      'beam.cli.cdc.seq-file': '../outside_secret.json'
                  }
              }
          }, ['id'],
          None)

  def test_feast_hf_vertex_config_opt_file_and_inspect_secret_guards(self):
    # Feast `offline_store` / dotted class import rejected (`H-6`).
    with self.assertRaises(ValueError):
      enrich.validate_feast_yaml_content(
          'project: p\nprovider: local\n'
          'offline_store:\n  type: os.system\n',
          trust_providers=False)

    # HF `proxies` & Vertex `endpoint_id` `/` traversal rejected (`H-7`).
    with self.assertRaises(ValueError):
      infer.parse_infer_config({
          'provider': 'hf',
          'model': 'org/model',
          'task': 'text-classification',
          'fields': 'text',
          'load_pipeline_args': {
              'proxies': {
                  'https': 'http://evil.example'
              }
          },
      })
    with self.assertRaises(ValueError):
      infer.parse_infer_config({
          'provider': 'vertexai',
          'endpoint_id': '123/../../other',
          'project': 'my-proj',
          'region': 'us-central1',
          'fields': 'x',
      })

    # `config.yaml` blocks dangerous execution options & secret options (`M-2.1`).
    cfg_file = os.path.join(self.tmp, 'bad_config.yaml')
    Path(cfg_file).write_text(
        'active_profile: default\n'
        'profiles:\n'
        '  default:\n'
        '    options:\n'
        '      prism_location: /tmp/evil_binary\n',
        encoding='utf-8')
    if os.name == 'posix':
      os.chmod(cfg_file, 0o600)
    with self.assertRaises(config.ConfigError):
      config.resolve_pipeline_args([], config_path=cfg_file)

    # World-writable `0666` opt-file rejected even without secret keys (`M-2.3`),
    # while `0600` `--from-opt-file` with secrets preserves `@file` (`M-2.2`)!
    opt_p = os.path.join(self.tmp, 'db.opts')
    Path(opt_p).write_text(
        'password=SuperSecretDbPass!\nfetchsize=500\n', encoding='utf-8')
    if os.name == 'posix':
      os.chmod(opt_p, 0o666)
      with self.assertRaises(uris.UriError):
        uris.parse_kv_options([], opt_p, '--from-opt')
      os.chmod(opt_p, 0o600)
    merged_list = cli_main._merge_opt_file_into_kv_list(['schema=public'],
                                                        opt_p,
                                                        '--from-opt')
    parsed_opts = uris.parse_kv_options(merged_list, None, '--from-opt')
    ep = uris.parse_endpoint(
        'postgres://dbhost:5432/app?table=users',
        is_read=True,
        options=parsed_opts)
    self.assertEqual(ep.config['password'], 'SuperSecretDbPass!')

    # Nested secret redaction in `inspect_cmd` & underscored keys in `output` (`H-10`).
    red = output.redact_inline_secrets('redis_password: "MyRedisSecretValue99"')
    self.assertNotIn('MyRedisSecretValue99', red)
    kafka_ep = uris.ParsedEndpoint(
        kind='kafka',
        scheme='kafka',
        raw='kafka://b:9092/t',
        format='kafka',
        yaml_type='ReadFromKafka',
        is_unbounded=True,
        config={
            'bootstrap_servers': 'b:9092',
            'topic': 't',
            'consumer_config': {
                'sasl.password': 'KafkaNestedSecret99!'
            },
        })
    # Build config_preview through inspect helper / recursive scrubber:
    with mock.patch.object(uris, 'parse_endpoint', return_value=kafka_ep):
      rep = inspect_cmd.run_inspect('kafka://b:9092/t')
    self.assertNotIn('KafkaNestedSecret99!', json.dumps(rep))

    # `scheduler` rejects multiline cron & YAML `options:` secrets (`H-10.3`).
    with self.assertRaises(scheduler.SchedulerError):
      scheduler.validate_cron_expression('0 * * * *\nmalicious')


class AdvancedConcurrencyAndRaceIsolationTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam_r2_conc_')
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.beam_home = os.path.join(self.tmp, 'beam_home')
    os.makedirs(self.beam_home, mode=0o700, exist_ok=True)
    self.enterContext(
        mock.patch.dict(
            os.environ,
            {
                'BEAM_HOME': self.beam_home,
                'BEAM_CONFIG': os.path.join(self.beam_home, 'config.yaml'),
            },
        ))

  def test_concurrent_iceberg_overwrite_partitions_disjoint_and_stale_hint(
      self):
    table_dir = os.path.join(self.tmp, 'ice_part_ov')
    fields = [
        {
            'id': 1, 'name': 'id', 'type': 'long', 'nullable': False
        },
        {
            'id': 2, 'name': 'dt', 'type': 'string', 'nullable': False
        },
        {
            'id': 3, 'name': 'val', 'type': 'long', 'nullable': False
        },
    ]
    part_spec, part_plan = lakehouse._resolve_iceberg_partition_plan(
        'dt', fields, None)

    # Seed initial `dt=2026-11-01`..`dt=2026-11-06` with old value `val=-1`.
    seed_files = lakehouse._write_iceberg_data_batches(
        os.path.join(table_dir, 'data'),
        'seed0', [{
            'id': i, 'dt': f'2026-11-0{i}', 'val': -1
        } for i in range(1, 7)],
        fields,
        part_plan)
    lakehouse.commit_iceberg_snapshot(
        table_dir,
        iceberg_fields=fields,
        new_data_files=seed_files,
        partition_spec=part_spec)

    # 6 concurrent threads overwrite disjoint partitions `dt=2026-11-01..06`!
    def _overwrite_one(day_idx: int) -> None:
      dt_str = f'2026-11-0{day_idx}'
      with lakehouse._iceberg_local_commit_lock(table_dir):
        df = lakehouse._write_iceberg_data_batches(
            os.path.join(table_dir, 'data'),
            f'b_{day_idx}', [{
                'id': day_idx, 'dt': dt_str, 'val': day_idx * 100
            }],
            fields,
            part_plan)
        lakehouse.commit_iceberg_snapshot(
            table_dir,
            iceberg_fields=fields,
            new_data_files=df,
            partition_spec=part_spec,
            overwrite_partitions=True)

    with ThreadPoolExecutor(max_workers=6) as pool:
      futs = [pool.submit(_overwrite_one, i) for i in range(1, 7)]
      for f in as_completed(futs):
        f.result()

    # Simulate stale `version-hint.text` lagging at `1` (`P0-1.2`):
    Path(os.path.join(table_dir, 'metadata', 'version-hint.text')).write_text(
        '1\n', encoding='utf-8')
    ver, _ = lakehouse.find_latest_iceberg_metadata_path(
        Path(table_dir).as_uri())
    self.assertEqual(ver, 7)

    _, rows = lakehouse.read_iceberg_rows_local(
        lakehouse.load_iceberg_state(table_dir))
    by_dt = {r['dt']: r['val'] for r in rows}
    self.assertEqual(by_dt, {f'2026-11-0{i}': i * 100 for i in range(1, 7)})
    self.assertEqual(len(rows), 6)

  def test_concurrent_iceberg_if_exists_replace_never_merges_rows(self):
    table_dir = os.path.join(self.tmp, 'ice_replace_race')

    def _run_replace(worker_id: int) -> int:
      in_csv = os.path.join(self.tmp, f'rep_{worker_id}.csv')
      Path(in_csv).write_text(
          'id,tag\n' + '\n'.join(f'{k},w{worker_id}' for k in range(10)) + '\n',
          encoding='utf-8')
      rc, _, err = _invoke_cli(
          [
              '--json',
              '--no-config',
              'pipe',
              in_csv,
              f'iceberg://{table_dir}',
              '--if-exists',
              'replace',
          ],
          beam_home=self.beam_home)
      self.assertEqual(rc, 0, msg=err)
      return worker_id

    with ThreadPoolExecutor(max_workers=4) as pool:
      futs = [pool.submit(_run_replace, w) for w in range(4)]
      for f in as_completed(futs):
        f.result()

    _, final_rows = lakehouse.read_iceberg_rows_local(
        lakehouse.load_iceberg_state(table_dir))
    # Must contain EXACTLY 10 rows from the final replacing writer, NEVER 40!
    self.assertEqual(len(final_rows), 10)
    self.assertEqual(len({r['tag'] for r in final_rows}), 1)

  def test_concurrent_idem_submission_lock_deduplicates_in_flight_run(self):
    idem_key = jobs.compute_idempotency_key(
        ['pipe', 'gs://b/in.csv', 'gs://b/out.parquet'], 'sha256:abc123')
    submit_count = 0
    submit_guard = threading.Lock()
    barrier = threading.Barrier(4)

    def _attempt_submit(idx: int) -> tuple[bool, str]:
      nonlocal submit_count
      barrier.wait(timeout=10)
      with jobs.idem_submission_lock(idem_key):
        existing = jobs.lookup_recent_submission(idem_key)
        if existing is not None:
          return True, str(existing['job_id'])
        with submit_guard:
          submit_count += 1
        time.sleep(0.05)  # Simulate `pipeline.run()` staging + jobs.create
        rec = jobs.record_job_submission(
            f'job-remote-{idx}',
            runner='DataflowRunner',
            state='RUNNING',
            command='pipe',
            project='proj-a',
            region='us-central1',
            idem_key=idem_key)
        return False, str(rec['job_id'])

    with ThreadPoolExecutor(max_workers=4) as pool:
      outcomes = [
          f.result()
          for f in [pool.submit(_attempt_submit, i) for i in range(4)]
      ]

    self.assertEqual(submit_count, 1)
    dedup_flags = [d for d, _ in outcomes]
    self.assertEqual(dedup_flags.count(False), 1)
    self.assertEqual(dedup_flags.count(True), 3)
    self.assertEqual(len({jid for _, jid in outcomes}), 1)

  def test_concurrent_sql_append_shared_dlq_assert_counts_and_umask_stability(
      self):
    shared_dlq = os.path.join(self.tmp, 'shared_sql_dlq.jsonl')
    extless_sink = os.path.join(self.tmp, 'extless_out')
    initial_umask = os.umask(0o022)
    os.umask(initial_umask)

    def _sql_worker(idx: int) -> None:
      src = os.path.join(self.tmp, f'sql_in_{idx}.csv')
      Path(src).write_text(
          f'id,val\n{idx * 10 + 1},10\n{idx * 10 + 2},BAD,EXTRA_COL\n',
          encoding='utf-8')
      rc, _, err = _invoke_cli(
          [
              '--json',
              '--no-config',
              'pipe',
              '--from',
              f's={src}',
              '--to',
              extless_sink,
              '--to-format',
              'json',
              '--sql',
              'SELECT id, val FROM s WHERE val > 0',
              '--dlq',
              shared_dlq,
              '--if-exists',
              'append',
          ],
          beam_home=self.beam_home)
      self.assertEqual(rc, 0, msg=err)

    with ThreadPoolExecutor(max_workers=4) as pool:
      futs = [pool.submit(_sql_worker, i) for i in range(4)]
      for f in as_completed(futs):
        f.result()

    # Verify zero lost rows on both extensionless primary sink AND shared DLQ!
    sink_shard = f'{extless_sink}-00000-of-00001'
    self.assertTrue(os.path.isfile(sink_shard))
    self.assertEqual(
        len(Path(sink_shard).read_text(encoding='utf-8').strip().splitlines()),
        4)
    self.assertEqual(
        len(Path(shared_dlq).read_text(encoding='utf-8').strip().splitlines()),
        4)

    # Verify process umask was never corrupted across threads (`P1-2.4`).
    after_umask = os.umask(initial_umask)
    os.umask(initial_umask)
    self.assertEqual(after_umask, initial_umask)

  def test_concurrent_diff_to_if_exists_fail_single_winner(self):
    left_p = os.path.join(self.tmp, 'left.csv')
    right_p = os.path.join(self.tmp, 'right.csv')
    Path(left_p).write_text('id,v\n1,a\n2,b\n', encoding='utf-8')
    Path(right_p).write_text('id,v\n1,a\n2,b\n', encoding='utf-8')
    report_out = os.path.join(self.tmp, 'diff_report.json')

    def _contend() -> int:
      rc, _, _ = _invoke_cli(
          [
              '--json',
              '--no-config',
              'diff',
              left_p,
              right_p,
              '--key',
              'id',
              '--to',
              report_out,
          ],
          beam_home=self.beam_home)
      return rc

    with ThreadPoolExecutor(max_workers=5) as pool:
      rcs = [f.result() for f in [pool.submit(_contend) for _ in range(5)]]

    self.assertEqual(rcs.count(0), 1, msg=f'rcs={rcs}')
    self.assertEqual(rcs.count(cli_main.EXIT_USAGE), 4, msg=f'rcs={rcs}')
    parsed = json.loads(Path(report_out).read_text(encoding='utf-8'))
    self.assertTrue(parsed['match'])

  def test_symlink_lock_rejected_tail_sleep_lease_and_quiet_logging_threadsafe(
      self):
    # 1) Symlink `.lock` file rejected via `O_NOFOLLOW` (`M-3.2`).
    if os.name == 'posix':
      victim = os.path.join(self.tmp, 'victim.txt')
      Path(victim).write_text('intact', encoding='utf-8')
      sym_lock = Path(os.path.join(self.tmp, 'evil.lock'))
      os.symlink(victim, sym_lock)
      with self.assertRaises((OSError, jobs.JobError)):
        with jobs._file_lock(sym_lock):
          pass
      self.assertEqual(Path(victim).read_text(encoding='utf-8'), 'intact')

    # 2) Live same-UID PID survives expired `lease_until` (laptop sleep `P1-6.1`),
    #    and path-traversal `sub_name` is rejected (`M-3.4`).
    with mock.patch.dict(os.environ, {'BEAM_HOME': self.beam_home}):
      valid_sub = 'beam-cli-follow-67390000-a1b2c3d4'
      tail._write_local_tail_lease(valid_sub, int(time.time()) - 3600)
      self.assertTrue(tail._is_local_tail_pid_alive(valid_sub, time.time()))
      self.assertFalse(
          tail._is_local_tail_pid_alive('../../outside', time.time()))

    # 3) Concurrent `_quiet_expansion` across 10 threads never leaves root
    #    logging disabled at `logging.CRITICAL` (`P2-1.2`)!
    logging.disable(logging.NOTSET)

    def _run_quiet() -> None:
      with check_cmd._quiet_expansion(True):
        time.sleep(0.01)

    with ThreadPoolExecutor(max_workers=10) as pool:
      for fut in [pool.submit(_run_quiet) for _ in range(10)]:
        fut.result()

    self.assertEqual(logging.root.manager.disable, logging.NOTSET)


if __name__ == '__main__':
  unittest.main()
