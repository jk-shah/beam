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

"""End-to-end tests for the 8 core data engineering use cases and skill/doc parity."""

from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
import stat
import struct
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml

from apache_beam.cli import infer
from apache_beam.cli import lakehouse
from apache_beam.cli import llms_txt
from apache_beam.cli import main as cli_main
from apache_beam.cli import tail
from apache_beam.cli import uris


def _run_cli(
    argv: list[str], env_overrides: dict[str, str] | None = None
) -> tuple[int, str, str]:
  out_buf = io.StringIO()
  err_buf = io.StringIO()
  merged_env = {'BEAM_NO_CONFIG': '1'}
  if env_overrides:
    merged_env.update(env_overrides)
  with mock.patch.dict(os.environ, merged_env, clear=False):
    with mock.patch.object(sys, 'stdout', out_buf), mock.patch.object(
        sys, 'stderr', err_buf):
      try:
        ret = cli_main.main(argv)
        code = int(ret) if isinstance(ret, int) else 0
      except SystemExit as exc:
        code = int(exc.code) if isinstance(exc.code, int) else 1
  return code, out_buf.getvalue(), err_buf.getvalue()


class EightUseCasesAndParityTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp(prefix='beam_cli_uc_')

  def tearDown(self):
    shutil.rmtree(self.tmp, ignore_errors=True)

  def test_uc1_nightly_json_to_partitioned_iceberg_and_append_inheritance(self):
    wh_dir = os.path.join(self.tmp, 'wh')
    day1_path = os.path.join(self.tmp, 'events_2026_11_15.jsonl')
    day2_path = os.path.join(self.tmp, 'events_2026_11_16.jsonl')
    with open(day1_path, 'w', encoding='utf-8') as f:
      f.write(json.dumps({'id': 1, 'amount': 42.5, 'dt': '2026-11-15'}) + '\n')
      f.write(json.dumps({'id': 2, 'amount': 19.0, 'dt': '2026-11-15'}) + '\n')
    with open(day2_path, 'w', encoding='utf-8') as f:
      f.write(json.dumps({'id': 3, 'amount': 88.0, 'dt': '2026-11-16'}) + '\n')

    ice_uri = f'iceberg://lake.events?warehouse={wh_dir}'
    # Night 1: initial partitioned write via --to-opt partition_fields=dt
    rc1, out1, err1 = _run_cli([
        'pipe',
        '--from',
        day1_path,
        '--to',
        ice_uri,
        '--to-opt',
        'partition_fields=dt',
        '--json',
    ])
    self.assertEqual(rc1, 0, msg=err1 or out1)

    # Night 2: append WITHOUT repeating --to-opt partition_fields -> must inherit!
    rc2, out2, err2 = _run_cli([
        'pipe',
        '--from',
        day2_path,
        '--to',
        ice_uri,
        '--if-exists',
        'append',
        '--json',
    ])
    self.assertEqual(rc2, 0, msg=err2 or out2)

    tdir = lakehouse.resolve_iceberg_table_dir(
        uris.parse_endpoint(ice_uri, is_read=True).config)
    st = lakehouse.load_iceberg_state(tdir)
    self.assertEqual(st['raw_record_count'], 3)
    data_paths = [df['path'] for df in st['data_files']]
    self.assertTrue(any('dt=2026-11-15' in p for p in data_paths), data_paths)
    self.assertTrue(any('dt=2026-11-16' in p for p in data_paths), data_paths)

    # Emit parameterized nightly template and export Composer DAG
    tmpl_yaml = os.path.join(self.tmp, 'nightly.yaml')
    rc_y, out_y, err_y = _run_cli([
        'pipe',
        '--from',
        'gs://bkt/raw/{{ ds }}/*.jsonl',
        '--to',
        'iceberg://lake.events?warehouse=gs://bkt/wh',
        '--select',
        "*, '{{ ds }}' AS dt",
        '--to-opt',
        'partition_by=dt',
        '--if-exists',
        'append',
        '--emit-yaml',
    ])
    self.assertEqual(rc_y, 0, msg=err_y)
    spec_y = yaml.safe_load(out_y)
    sink_step = [
        t for t in spec_y['pipeline']['transforms']
        if t.get('type') == 'WriteToIceberg'
    ][0]
    self.assertEqual(sink_step['config'].get('partition_fields'), ['dt'])
    Path(tmpl_yaml).write_text(out_y, encoding='utf-8')
    dag_out = os.path.join(self.tmp, 'nightly_dag.py')
    rc_s, out_s, err_s = _run_cli([
        'export-schedule',
        tmpl_yaml,
        '--template-spec',
        'gs://bkt/templates/nightly.json',
        '--orchestrator',
        'composer',
        '--schedule',
        '0 2 * * *',
        '--to',
        dag_out,
        '--json',
    ])
    self.assertEqual(rc_s, 0, msg=err_s or out_s)
    self.assertTrue(os.path.isfile(dag_out))

  def test_uc2_pii_masking_parity_across_fast_local_beam_and_sql(self):
    src = os.path.join(self.tmp, 'payments.jsonl')
    rows = [
        {
            'id': 1,
            'uid': 'u-42',
            'email': 'alice@example.com',
            'card': '4111222233334444',
            'secret_note': 'vip',
            'amount': 125.0,
        },
        {
            'id': 2,
            'uid': None,
            'email': None,
            'card': '99',
            'secret_note': None,
            'amount': 50.0,
        },
    ]
    with open(src, 'w', encoding='utf-8') as f:
      for r in rows:
        f.write(json.dumps(r) + '\n')

    sel_expr = (
        'id, sha256(uid) AS uid_hash, mask_email(email) AS email_m, '
        'mask_last4(card) AS card_m, redact(secret_note) AS note_m, '
        "substr(card, 1, 4) AS bin4, concat('ID-', str(id)) AS label")

    out_fast = os.path.join(self.tmp, 'masked_fast.parquet')
    rc_f, o_f, e_f = _run_cli(
        ['pipe', src, out_fast, '--select', sel_expr, '--json'])
    self.assertEqual(rc_f, 0, msg=e_f or o_f)

    out_beam = os.path.join(self.tmp, 'masked_beam.parquet')
    rc_b, o_b, e_b = _run_cli(
        ['pipe', src, out_beam, '--select', sel_expr, '--json'],
        env_overrides={'BEAM_CLI_NO_FAST_PATH': '1'})
    self.assertEqual(rc_b, 0, msg=e_b or o_b)

    rc_diff, o_d, e_d = _run_cli(['diff', out_fast, out_beam, '--key', 'id', '--json'])
    self.assertEqual(rc_diff, 0, msg=e_d or o_d)

    out_sql = os.path.join(self.tmp, 'masked_sql.jsonl')
    rc_q, o_q, e_q = _run_cli([
        'pipe',
        '--from',
        f'p={src}',
        '--to',
        out_sql,
        '--sql',
        (
            'SELECT id, SHA256(uid) AS uid_hash, MASK_EMAIL(email) AS email_m, '
            'MASK_LAST4(card) AS card_m, REDACT(secret_note) AS note_m FROM p '
            'ORDER BY id'),
        '--json',
    ])
    self.assertEqual(rc_q, 0, msg=e_q or o_q)
    sql_rows = [
        json.loads(line)
        for line in Path(out_sql).read_text(encoding='utf-8').splitlines()
        if line.strip()
    ]
    exp_hash = hashlib.sha256(b'u-42').hexdigest()
    self.assertEqual(sql_rows[0]['uid_hash'], exp_hash)
    self.assertEqual(sql_rows[0]['email_m'], 'a***@example.com')
    self.assertEqual(sql_rows[0]['card_m'], '************4444')
    self.assertEqual(sql_rows[0]['note_m'], '<REDACTED>')
    self.assertIsNone(sql_rows[1]['uid_hash'])
    self.assertIsNone(sql_rows[1]['email_m'])
    self.assertEqual(sql_rows[1]['card_m'], '**')
    self.assertIsNone(sql_rows[1]['note_m'])

    # Kafka -> Iceberg YAML compilation with query params + masking
    rc_ky, o_ky, e_ky = _run_cli([
        'pipe',
        '--from',
        'kafka://broker:9092/payments?auto_offset_reset=latest',
        '--to',
        'iceberg://lake.payments?warehouse=gs://bkt/wh',
        '--where',
        "status = 'CAPTURED' AND amount > 0",
        '--select',
        'payment_id, sha256(user_id) AS uid_hash, mask_email(email) AS email',
        '--to-opt',
        'partition_fields=day(event_ts)',
        '--emit-yaml',
    ])
    self.assertEqual(rc_ky, 0, msg=e_ky)
    ky_spec = yaml.safe_load(o_ky)
    t_types = [t.get('type') for t in ky_spec['pipeline']['transforms']]
    self.assertIn('ReadFromKafka', t_types)
    self.assertIn('WriteToIceberg', t_types)

  def test_uc3_join_two_kafka_streams_interval_and_asof_yaml(self):
    rc_ij, out_ij, err_ij = _run_cli([
        'pipe',
        '--from',
        'c=kafka://broker:9092/clicks?timestamp_policy=CreateTime&auto_offset_reset=latest',
        '--join',
        'o=kafka://broker:9092/orders?schema_registry=https://sr.example.com:8081',
        '--to',
        'iceberg://lake.attributed_orders?warehouse=gs://bkt/wh',
        '--sql',
        (
            'SELECT o.order_id, o.user_id, c.campaign_id '
            'FROM c JOIN o ON c.user_id = o.user_id '
            "AND o.order_ts BETWEEN c.click_ts AND c.click_ts + INTERVAL '15' MINUTE"
        ),
        '--max-watermark-skew',
        '30m',
        '--skew-action',
        'dlq',
        '--dlq',
        'gs://bkt/dlq/skew.jsonl',
        '--emit-yaml',
    ])
    self.assertEqual(rc_ij, 0, msg=err_ij)
    ij_spec = yaml.safe_load(out_ij)
    transforms = ij_spec['pipeline']['transforms']
    read_o = next(t for t in transforms if t.get('name') == 'Read_o')
    self.assertEqual(read_o['config']['format'], 'AVRO')
    self.assertIn('CliIntervalJoin', [t.get('type') for t in transforms])

  def test_uc4_postgres_to_iceberg_cdc_replication_yaml(self):
    rc, out, err = _run_cli(
        [
            'pipe',
            '--from',
            'postgres://pg.internal:5432/prod?table=public.orders',
            '--to',
            'iceberg://lake.orders?warehouse=gs://bkt/wh',
            '--cdc',
            '--primary-key',
            'order_id',
            '--emit-yaml',
        ],
        env_overrides={
            'BEAM_POSTGRES_USER': 'replicator',
            'BEAM_POSTGRES_PASSWORD': 'secret-wal-password',
        })
    self.assertEqual(rc, 0, msg=err)
    # Emitted YAML must redact passwords
    self.assertNotIn('secret-wal-password', out)
    spec = yaml.safe_load(out)
    transforms = spec['pipeline']['transforms']
    deb = next(t for t in transforms if t.get('type') == 'ReadFromDebezium')
    self.assertEqual(deb['config']['connector'], 'POSTGRES')
    self.assertIn('plugin.name=pgoutput', deb['config']['connection_properties'])
    ice = next(t for t in transforms if t.get('type') == 'WriteToIceberg')
    self.assertEqual(ice['config']['mode'], 'merge-on-read')
    self.assertEqual(ice['config']['equality_columns'], ['order_id'])

  def test_uc5_partitioned_catalog_sync_dry_run_and_iceberg_zero_copy_yaml(self):
    import pyarrow as pa
    import pyarrow.parquet as pq

    part_dir = os.path.join(self.tmp, 'hive_events', 'dt=2026-11-15')
    os.makedirs(part_dir, exist_ok=True)
    pq.write_table(
        pa.table({'id': [1, 2], 'v': ['a', 'b']}),
        os.path.join(part_dir, 'part-0.parquet'))

    wh_dir = os.path.join(self.tmp, 'wh_sync')
    ice_dst = f'iceberg://lake.hive_events?warehouse={wh_dir}'
    rc_dr, out_dr, err_dr = _run_cli([
        'catalog',
        'sync',
        '--dry-run',
        os.path.join(self.tmp, 'hive_events', '*', '*.parquet'),
        ice_dst,
        '--json',
    ])
    self.assertEqual(rc_dr, 0, msg=err_dr or out_dr)
    env_dr = json.loads(out_dr)
    self.assertEqual(env_dr['outputs']['partition_columns'], ['dt'])

    rc_sync, out_sync, err_sync = _run_cli([
        'catalog',
        'sync',
        os.path.join(self.tmp, 'hive_events', '*', '*.parquet'),
        ice_dst,
        '--json',
    ])
    self.assertEqual(rc_sync, 0, msg=err_sync or out_sync)

    # Iceberg-to-Iceberg --zero-copy --emit-yaml must succeed cleanly
    ice_alias = f'iceberg://lake.hive_events_alias?warehouse={wh_dir}'
    rc_zc, out_zc, err_zc = _run_cli([
        'pipe',
        '--from',
        ice_dst,
        '--to',
        ice_alias,
        '--zero-copy',
        '--emit-yaml',
    ])
    self.assertEqual(rc_zc, 0, msg=err_zc)
    zc_spec = yaml.safe_load(out_zc)
    self.assertIn(
        'IcebergAddFiles',
        [t.get('type') for t in zc_spec['pipeline']['transforms']])

  def test_uc6_multi_table_sql_with_expect_quality_rules(self):
    orders_p = os.path.join(self.tmp, 'orders.jsonl')
    cust_p = os.path.join(self.tmp, 'customers.jsonl')
    with open(orders_p, 'w', encoding='utf-8') as f:
      f.write(json.dumps({'order_id': 1, 'cid': 10, 'amount': 150.0, 'env': 'prod'}) + '\n')
      f.write(json.dumps({'order_id': 2, 'cid': 99, 'amount': 80.0, 'env': 'prod'}) + '\n')
      f.write(json.dumps({'order_id': 3, 'cid': 10, 'amount': -5.0, 'env': 'prod'}) + '\n')
      f.write(json.dumps({'order_id': 4, 'cid': 10, 'amount': 20.0, 'env': 'dev'}) + '\n')
    with open(cust_p, 'w', encoding='utf-8') as f:
      f.write(json.dumps({'cid': 10, 'tier': 'GOLD'}) + '\n')

    out_clean = os.path.join(self.tmp, 'curated.jsonl')
    out_dlq = os.path.join(self.tmp, 'qual_dlq.jsonl')
    sql_q = (
        'SELECT o.order_id, c.tier, o.amount, o.env '
        'FROM o LEFT JOIN c ON o.cid = c.cid ORDER BY o.order_id')

    rc, out, err = _run_cli([
        'pipe',
        '--from',
        f'o={orders_p}',
        '--from',
        f'c={cust_p}',
        '--to',
        out_clean,
        '--dlq',
        out_dlq,
        '--sql',
        sql_q,
        '--expect',
        'pk_ok: order_id IS NOT NULL -> fail',
        '--expect',
        "prod_only: env = 'prod' -> drop",
        '--expect',
        'valid_tier_amt: tier IS NOT NULL AND amount > 0 -> dlq',
        '--json',
    ])
    self.assertEqual(rc, 0, msg=err or out)
    env = json.loads(out)
    self.assertEqual(env['metrics']['rows_written'], 1)
    self.assertEqual(env['metrics']['dlq_rows'], 2)
    self.assertEqual(env['metrics']['expectation_dropped_rows'], 1)
    self.assertEqual(env['metrics']['expectation_quarantined_rows'], 2)

    clean_rows = [
        json.loads(l)
        for l in Path(out_clean).read_text(encoding='utf-8').splitlines()
        if l.strip()
    ]
    self.assertEqual(len(clean_rows), 1)
    self.assertEqual(clean_rows[0]['order_id'], 1)

    dlq_rows = [
        json.loads(l)
        for l in Path(out_dlq).read_text(encoding='utf-8').splitlines()
        if l.strip()
    ]
    self.assertEqual(len(dlq_rows), 2)
    self.assertTrue(all(r['step'] == 'CliExpect' for r in dlq_rows))

    # --emit-yaml with --sql + --expect must wire ValidateExpect + NormalizeExpectDlq + provider
    rc_y, out_y, err_y = _run_cli([
        'pipe',
        '--from',
        f'o={orders_p}',
        '--from',
        f'c={cust_p}',
        '--to',
        out_clean,
        '--dlq',
        out_dlq,
        '--sql',
        sql_q,
        '--expect',
        'valid_tier_amt: tier IS NOT NULL AND amount > 0 -> dlq',
        '--emit-yaml',
    ])
    self.assertEqual(rc_y, 0, msg=err_y)
    spec_y = yaml.safe_load(out_y)
    t_names = [t.get('name') for t in spec_y['pipeline']['transforms']]
    self.assertIn('ValidateExpect', t_names)
    self.assertIn('NormalizeExpectDlq', t_names)
    self.assertTrue(spec_y.get('providers'))

    # Failing `-> fail` expectation must return EXIT_PIPELINE_FAILED (1)
    rc_fail, out_fail, _ = _run_cli([
        'pipe',
        '--from',
        f'o={orders_p}',
        '--from',
        f'c={cust_p}',
        '--to',
        os.path.join(self.tmp, 'fail_out.jsonl'),
        '--sql',
        sql_q,
        '--expect',
        'strict_amt: amount > 0 -> fail',
        '--json',
    ])
    self.assertEqual(rc_fail, 1, msg=out_fail)

  def test_uc7_kafka_tail_readonly_group_sasl_avro_and_interactive_json(self):
    import fastavro

    avro_schema = {
        'type': 'record',
        'name': 'Payment',
        'fields': [
            {'name': 'payment_id', 'type': 'string'},
            {'name': 'amount', 'type': 'double'},
        ],
    }
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, avro_schema, {'payment_id': 'pay-9', 'amount': 77.5})
    confluent_payload = b'\x00' + struct.pack('>I', 42) + buf.getvalue()

    captured_conf: dict[str, object] = {}

    class _FakeMsg:
      def error(self):
        return None

      def value(self):
        return confluent_payload

      def key(self):
        return b'k-9'

      def partition(self):
        return 2

      def offset(self):
        return 105

      def timestamp(self):
        return (1, 1700000000123)

    class _FakeConsumer:
      def __init__(self, conf):
        captured_conf.update(conf)
        self._polled = False

      def subscribe(self, topics):
        captured_conf['_topics'] = list(topics)

      def poll(self, timeout=0.25):
        if not self._polled:
          self._polled = True
          return _FakeMsg()
        return None

      def close(self):
        captured_conf['_closed'] = True

    fake_ck = mock.MagicMock()
    fake_ck.Consumer = _FakeConsumer

    opt_file = os.path.join(self.tmp, 'kafka.opts')
    Path(opt_file).write_text(
        'security_protocol=SASL_SSL\n'
        'sasl_mechanism=SCRAM-SHA-256\n'
        'sasl_username=svc-reader\n',
        encoding='utf-8')
    os.chmod(opt_file, stat.S_IRUSR | stat.S_IWUSR)

    with mock.patch.dict(sys.modules, {'confluent_kafka': fake_ck}):
      with mock.patch.object(
          tail, '_fetch_confluent_avro_schema', return_value=avro_schema):
        rc, out, err = _run_cli(
            [
                'tail',
                'kafka:///payments?schema_registry=https://sr.internal:8081&auto_offset_reset=latest',
                '-n',
                '1',
                '--duration',
                '2s',
                '-i',
                '--from-opt-file',
                opt_file,
                '--json',
            ],
            env_overrides={
                'BEAM_KAFKA_BOOTSTRAP_SERVERS': 'b1.internal:9092,b2.internal:9092',
                'BEAM_KAFKA_SASL_PASSWORD': 'super-secret-kafka-pwd',
            })

    self.assertEqual(rc, 0, msg=err or out)
    # Secrets must never leak to stdout or stderr
    self.assertNotIn('super-secret-kafka-pwd', out)
    self.assertNotIn('super-secret-kafka-pwd', err)
    # Interactive TUI rendered to stderr while stdout stayed pure RFC-8259 JSON!
    self.assertIn('msg/s', err)
    env = json.loads(out)
    self.assertTrue(env['ok'])
    rows = env['untrusted_data']['rows']
    self.assertEqual(len(rows), 1)
    self.assertEqual(rows[0]['partition'], 2)
    self.assertEqual(rows[0]['offset'], 105)
    self.assertEqual(rows[0]['key'], 'k-9')
    self.assertEqual(rows[0]['schema_id'], 42)
    self.assertIs(rows[0]['schema_valid'], True)
    self.assertEqual(rows[0]['payload']['payment_id'], 'pay-9')
    self.assertEqual(rows[0]['payload']['amount'], 77.5)
    self.assertEqual(rows[0]['timestamp_ms'], 1700000000123)
    self.assertTrue(str(captured_conf['group.id']).startswith('beam-tail-'))
    self.assertIs(captured_conf['enable.auto.commit'], False)
    self.assertEqual(
        captured_conf['bootstrap.servers'], 'b1.internal:9092,b2.internal:9092')
    self.assertEqual(captured_conf['security.protocol'], 'SASL_SSL')
    self.assertEqual(captured_conf['sasl.mechanism'], 'SCRAM-SHA-256')
    self.assertEqual(captured_conf['sasl.username'], 'svc-reader')
    self.assertEqual(captured_conf['sasl.password'], 'super-secret-kafka-pwd')
    self.assertEqual(captured_conf['auto.offset.reset'], 'latest')

  def test_kafka_tail_no_group_assign_json_schema_validation_and_raw_hex(self):
    valid_wire = (
        b'\x00' + struct.pack('>I', 77) +
        json.dumps({'order_id': 'ord-1', 'amount': 15.0}).encode('utf-8'))
    invalid_wire = (
        b'\x00' + struct.pack('>I', 77) +
        json.dumps({'order_id': 'ord-2', 'amount': 'bad-num'}).encode('utf-8'))

    assigned_parts: list[tuple[str, int, int]] = []
    subscribe_called = False

    class _FakeMsg:
      def __init__(self, payload: bytes, offset: int):
        self._payload = payload
        self._offset = offset

      def error(self):
        return None

      def value(self):
        return self._payload

      def key(self):
        return b'k'

      def partition(self):
        return 0

      def offset(self):
        return self._offset

    class _FakeConsumer:
      def __init__(self, _conf):
        self._msgs = [_FakeMsg(valid_wire, 1), _FakeMsg(invalid_wire, 2)]

      def list_topics(self, topic=None, timeout=5.0):
        t_meta = mock.MagicMock()
        t_meta.partitions = {0: object(), 1: object()}
        meta = mock.MagicMock()
        meta.topics = {topic: t_meta}
        return meta

      def assign(self, tps):
        assigned_parts.extend((tp.topic, tp.partition, tp.offset) for tp in tps)

      def subscribe(self, _topics):
        nonlocal subscribe_called
        subscribe_called = True

      def poll(self, timeout=0.25):
        return self._msgs.pop(0) if self._msgs else None

      def close(self):
        pass

    class _FakeTP:
      def __init__(self, topic: str, partition: int, offset: int):
        self.topic = topic
        self.partition = partition
        self.offset = offset

    fake_ck = mock.MagicMock()
    fake_ck.Consumer = _FakeConsumer
    fake_ck.TopicPartition = _FakeTP
    fake_ck.OFFSET_END = -1

    captured_headers: dict[str, str] = {}
    sr_doc = json.dumps({
        'schemaType': 'JSON',
        'schema': json.dumps({
            'type': 'object',
            'properties': {
                'order_id': {'type': 'string'},
                'amount': {'type': 'number'},
            },
            'required': ['order_id', 'amount'],
        }),
    }).encode('utf-8')

    def _fake_urlopen(req, timeout=5.0):
      captured_headers.update(dict(req.header_items()))
      cm = mock.MagicMock()
      cm.__enter__.return_value.read.return_value = sr_doc
      return cm

    with mock.patch.dict(sys.modules, {'confluent_kafka': fake_ck}):
      with mock.patch('urllib.request.urlopen', side_effect=_fake_urlopen):
        rc, out, err = _run_cli(
            [
                'tail',
                (
                    'kafka://broker:9092/orders?schema_registry=https://sr.internal:8081'
                    '&no_group=true&raw_bytes=true&auto_offset_reset=latest'),
                '-n',
                '2',
                '--duration',
                '2s',
                '--json',
            ],
            env_overrides={'BEAM_SCHEMA_REGISTRY_USER_INFO': 'sr-key:sr-sec'})

    self.assertEqual(rc, 0, msg=err or out)
    self.assertFalse(subscribe_called)
    self.assertEqual(assigned_parts, [('orders', 0, -1), ('orders', 1, -1)])
    self.assertIn('Authorization', captured_headers)
    self.assertNotIn('sr-sec', out)
    rows = json.loads(out)['untrusted_data']['rows']
    self.assertEqual(len(rows), 2)
    self.assertEqual(rows[0]['schema_id'], 77)
    self.assertIs(rows[0]['schema_valid'], True)
    self.assertTrue(rows[0]['raw_hex'].startswith('000000004d'))
    self.assertGreater(rows[0]['size_bytes'], 10)
    self.assertEqual(rows[1]['schema_id'], 77)
    self.assertIs(rows[1]['schema_valid'], False)
    self.assertTrue(rows[1].get('schema_error'))

  def test_local_fast_256mib_and_cloud_sql_sink_upload(self):
    from apache_beam.cli import fast_local
    from apache_beam.io.filesystems import FileSystems

    self.assertEqual(fast_local.MAX_INPUT_BYTES, 256 * 1024 * 1024)
    src_a = os.path.join(self.tmp, 'a.jsonl')
    src_b = os.path.join(self.tmp, 'b.jsonl')
    Path(src_a).write_text(json.dumps({'id': 1, 'v': 10}) + '\n', encoding='utf-8')
    Path(src_b).write_text(json.dumps({'id': 1, 'name': 'Ada'}) + '\n', encoding='utf-8')

    uploaded = io.BytesIO()

    class _NonClosingBytesIO(io.BytesIO):
      def close(self):
        uploaded.write(self.getvalue())
        super().close()

    with mock.patch.object(FileSystems, 'match', return_value=[]), mock.patch.object(
        FileSystems, 'create', side_effect=lambda _p: _NonClosingBytesIO()):
      rc, out, err = _run_cli([
          'pipe',
          '--from',
          f'a={src_a}',
          '--from',
          f'b={src_b}',
          '--to',
          'gs://test-bkt/out/joined.jsonl',
          '--sql',
          'SELECT a.id, b.name, a.v FROM a JOIN b ON a.id = b.id',
          '--json',
      ])
    self.assertEqual(rc, 0, msg=err or out)
    env = json.loads(out)
    self.assertEqual(env['outputs']['files'], ['gs://test-bkt/out/joined.jsonl'])
    written_row = json.loads(uploaded.getvalue().decode('utf-8').strip())
    self.assertEqual(written_row, {'id': 1, 'name': 'Ada', 'v': 10})

  def test_uc8_low_delay_scoring_max_batch_duration_ms(self):
    cfg = infer.parse_infer_config({
        'provider': 'mock',
        'min_batch_size': 1,
        'max_batch_size': 16,
        'max_batch_duration_ms': 10,
    })
    self.assertAlmostEqual(cfg.max_batch_duration_secs, 0.01)

    with self.assertRaises(ValueError):
      infer.parse_infer_config({
          'provider': 'mock',
          'max_batch_duration_ms': 10,
          'max_batch_duration_secs': 0.2,
      })

    txns = os.path.join(self.tmp, 'txns.jsonl')
    scored = os.path.join(self.tmp, 'scored.jsonl')
    with open(txns, 'w', encoding='utf-8') as f:
      f.write(json.dumps({'txn_id': 't1', 'amount': 250.0}) + '\n')

    rc, out, err = _run_cli([
        'pipe',
        txns,
        scored,
        '--infer',
        'mock:score=0.92,min_batch_size=1,max_batch_size=16,max_batch_duration_ms=10',
        '--infer-fields',
        'amount',
        '--infer-flatten',
        '--json',
    ])
    self.assertEqual(rc, 0, msg=err or out)
    res_rows = [
        json.loads(l)
        for l in Path(scored).read_text(encoding='utf-8').splitlines()
        if l.strip()
    ]
    self.assertEqual(len(res_rows), 1)
    self.assertAlmostEqual(res_rows[0]['prediction_score'], 0.92)

  def test_hc3_dlq_redact_fields_fast_local_and_corrupt_raw_lines(self):
    dirty_csv = os.path.join(self.tmp, 'dirty_pii.csv')
    Path(dirty_csv).write_text(
        'order_id,customer_email,card_number,amount\n'
        'ord-1,ada@acme.com,4111222233334444,49.5\n'
        'ord-2,"shifted,col,bob@acme.com,4532-1111-2222-3333,123-45-6789,oops\n'
        ',carol@acme.com,5500111122223333,10.0\n',
        encoding='utf-8')

    for label, env_ov in (
        ('fast', None),
        ('beam', {'BEAM_CLI_NO_FAST_PATH': '1'}),
    ):
      out_pq = os.path.join(self.tmp, f'clean_{label}.parquet')
      dlq_jl = os.path.join(self.tmp, f'dlq_{label}.jsonl')
      rc, out, err = _run_cli(
          [
              'pipe',
              dirty_csv,
              out_pq,
              '--where',
              'amount > 0',
              '--expect',
              'pk_ok: order_id IS NOT NULL -> dlq',
              '--dlq',
              dlq_jl,
              '--dlq-redact-fields',
              'customer_email,card_number',
              '--assert-counts',
              '--json',
          ],
          env_overrides=env_ov)
      self.assertEqual(rc, 0, msg=f'{label}: {err or out}')
      env = json.loads(out)
      if label == 'fast':
        self.assertEqual(env['runner'], 'local_fast')
      dlq_txt = Path(dlq_jl).read_text(encoding='utf-8')
      self.assertNotIn('bob@acme.com', dlq_txt)
      self.assertNotIn('carol@acme.com', dlq_txt)
      self.assertNotIn('4532-1111-2222-3333', dlq_txt)
      self.assertNotIn('5500111122223333', dlq_txt)
      self.assertNotIn('123-45-6789', dlq_txt)
      self.assertIn('<REDACTED>', dlq_txt)

    # Also verify truncated/corrupt JSONL raw-line redaction
    dirty_jl = os.path.join(self.tmp, 'dirty_pii.jsonl')
    Path(dirty_jl).write_text(
        '{"id": "1", "email": "ada@acme.com", "amount": 20.0}\n'
        '{"id": "2", "email": "eve@acme.com", "card": "4111-2222-3333-9999", BROKEN\n',
        encoding='utf-8')
    out_jl = os.path.join(self.tmp, 'clean_jl.parquet')
    dlq_broken_jl = os.path.join(self.tmp, 'dlq_broken_jl.jsonl')
    rc_j, out_j, err_j = _run_cli([
        'pipe',
        dirty_jl,
        out_jl,
        '--dlq',
        dlq_broken_jl,
        '--dlq-redact-fields',
        'email,card',
        '--assert-counts',
        '--json',
    ])
    self.assertEqual(rc_j, 0, msg=err_j or out_j)
    self.assertEqual(json.loads(out_j)['runner'], 'local_fast')
    broken_dlq_txt = Path(dlq_broken_jl).read_text(encoding='utf-8')
    self.assertNotIn('eve@acme.com', broken_dlq_txt)
    self.assertNotIn('4111-2222-3333-9999', broken_dlq_txt)
    self.assertIn('<REDACTED>', broken_dlq_txt)

  def test_iceberg_dynamic_partition_overwrite_idempotent_retry(self):
    wh = os.path.join(self.tmp, 'wh_ov')
    day1_jl = os.path.join(self.tmp, 'events_2026_11_14.jsonl')
    day2_v1_jl = os.path.join(self.tmp, 'events_2026_11_15_v1.jsonl')
    day2_v2_jl = os.path.join(self.tmp, 'events_2026_11_15_v2.jsonl')
    Path(day1_jl).write_text(
        '{"event_id": "e1", "amount": 10.0}\n'
        '{"event_id": "e2", "amount": 20.0}\n',
        encoding='utf-8')
    Path(day2_v1_jl).write_text(
        '{"event_id": "e3", "amount": 30.0}\n'
        '{"event_id": "e4", "amount": 40.0}\n',
        encoding='utf-8')
    Path(day2_v2_jl).write_text(
        '{"event_id": "e3", "amount": 300.0}\n'
        '{"event_id": "e5", "amount": 500.0}\n',
        encoding='utf-8')
    ice_uri = f'iceberg://lake.events?warehouse={wh}'

    # 1. Load 2026-11-14
    rc1, out1, err1 = _run_cli([
        'pipe',
        '--from',
        day1_jl,
        '--to',
        ice_uri,
        '--select',
        "*, '2026-11-14' AS dt",
        '--to-opt',
        'partition_fields=dt',
        '--to-opt',
        'overwrite_partitions=true',
        '--if-exists',
        'append',
        '--json',
    ])
    self.assertEqual(rc1, 0, msg=err1 or out1)

    # 2. Load 2026-11-15 (initial attempt)
    rc2, out2, err2 = _run_cli([
        'pipe',
        '--from',
        day2_v1_jl,
        '--to',
        ice_uri,
        '--select',
        "*, '2026-11-15' AS dt",
        '--to-opt',
        'partition_fields=dt',
        '--to-opt',
        'overwrite_partitions=true',
        '--if-exists',
        'append',
        '--json',
    ])
    self.assertEqual(rc2, 0, msg=err2 or out2)

    # 3. Retry 2026-11-15 with overwrite_partitions=true -> must atomically
    #    replace ONLY dt=2026-11-15 while preserving dt=2026-11-14!
    rc3, out3, err3 = _run_cli([
        'pipe',
        '--from',
        day2_v2_jl,
        '--to',
        ice_uri,
        '--select',
        "*, '2026-11-15' AS dt",
        '--to-opt',
        'partition_fields=dt',
        '--to-opt',
        'overwrite_partitions=true',
        '--if-exists',
        'append',
        '--json',
    ])
    self.assertEqual(rc3, 0, msg=err3 or out3)

    tdir = os.path.join(wh, 'lake', 'events')
    st = lakehouse.load_iceberg_state(tdir)
    snaps = st['metadata']['snapshots']
    self.assertEqual(snaps[-1]['summary']['operation'], 'overwrite')

    out_read = os.path.join(self.tmp, 'events_readback.jsonl')
    rc_r, out_r, err_r = _run_cli(
        ['pipe', '--from', ice_uri, '--to', out_read, '--json'])
    self.assertEqual(rc_r, 0, msg=err_r or out_r)
    rows = sorted(
        (
            json.loads(line)
            for line in Path(out_read).read_text(encoding='utf-8').splitlines()
            if line.strip()
        ),
        key=lambda r: (r['dt'], r['event_id']))
    self.assertEqual(
        rows,
        [
            {'event_id': 'e1', 'amount': 10.0, 'dt': '2026-11-14'},
            {'event_id': 'e2', 'amount': 20.0, 'dt': '2026-11-14'},
            {'event_id': 'e3', 'amount': 300.0, 'dt': '2026-11-15'},
            {'event_id': 'e5', 'amount': 500.0, 'dt': '2026-11-15'},
        ])

  def test_uniform_opt_file_flags_across_inspect_diff_schema_and_catalog_sync(
      self):
    left_jl = os.path.join(self.tmp, 'left_u.jsonl')
    right_jl = os.path.join(self.tmp, 'right_u.jsonl')
    Path(left_jl).write_text('{"id": 1, "v": "a"}\n', encoding='utf-8')
    Path(right_jl).write_text('{"id": 1, "v": "a"}\n', encoding='utf-8')
    pq_dir = os.path.join(self.tmp, 'pq_cat_src')
    os.makedirs(pq_dir, exist_ok=True)
    _rc_p, _o_p, _e_p = _run_cli(
        ['pipe', left_jl, os.path.join(pq_dir, 'part0.parquet'), '--json'])
    self.assertEqual(_rc_p, 0, msg=_e_p or _o_p)

    good_opt = os.path.join(self.tmp, 'good.opts')
    Path(good_opt).write_text('sample_rows=50\n', encoding='utf-8')
    os.chmod(good_opt, stat.S_IRUSR | stat.S_IWUSR)

    cat_opt = os.path.join(self.tmp, 'cat.opts')
    Path(cat_opt).write_text('s3.region=us-east-1\n', encoding='utf-8')
    os.chmod(cat_opt, stat.S_IRUSR | stat.S_IWUSR)

    # 1. beam inspect accepts --to-opt-file and --opt-file
    for flag in ('--to-opt-file', '--opt-file', '--from-opt-file'):
      rc_i, out_i, err_i = _run_cli(
          ['inspect', left_jl, flag, good_opt, '--sample', '0', '--json'])
      self.assertEqual(rc_i, 0, msg=f'{flag}: {err_i or out_i}')

    # 2. beam diff accepts --from-opt-file and --to-opt-file
    rc_d, out_d, err_d = _run_cli([
        'diff',
        left_jl,
        right_jl,
        '--key',
        'id',
        '--from-opt-file',
        good_opt,
        '--to-opt-file',
        good_opt,
        '--json',
    ])
    self.assertEqual(rc_d, 0, msg=err_d or out_d)

    # 3. beam schema diff accepts --from-opt-file and --to-opt-file
    rc_s, out_s, err_s = _run_cli([
        'schema',
        'diff',
        left_jl,
        right_jl,
        '--from-opt-file',
        good_opt,
        '--to-opt-file',
        good_opt,
        '--json',
    ])
    self.assertEqual(rc_s, 0, msg=err_s or out_s)

    # 4. beam catalog sync accepts --from-opt-file and --to-opt-file (and rejects 0644!)
    wh_cat = os.path.join(self.tmp, 'wh_cat')
    rc_c, out_c, err_c = _run_cli([
        'catalog',
        'sync',
        '--dry-run',
        pq_dir,
        f'iceberg://lake.synced?warehouse={wh_cat}',
        '--from-opt-file',
        good_opt,
        '--to-opt-file',
        cat_opt,
        '--json',
    ])
    self.assertEqual(rc_c, 0, msg=err_c or out_c)

    bad_opt = os.path.join(self.tmp, 'world_readable.opts')
    Path(bad_opt).write_text('token=secret\n', encoding='utf-8')
    os.chmod(bad_opt, 0o644)
    rc_bad, out_bad, _ = _run_cli([
        'catalog',
        'sync',
        '--dry-run',
        pq_dir,
        f'iceberg://lake.synced?warehouse={wh_cat}',
        '--to-opt-file',
        bad_opt,
        '--json',
    ])
    self.assertEqual(rc_bad, 2, msg=out_bad)

  def test_dirty_csv_explicit_float_typing_and_dlq_redaction(self):
    dirty_csv = os.path.join(self.tmp, 'partner_orders_raw.csv')
    Path(dirty_csv).write_text(
        'order_id,customer_email,amount\n'
        'o1,alice@acme.com,42.5\n'
        'o2,bob@acme.com,not_a_float\n'
        'o3,carol@acme.com,-5.0\n'
        ',dave@acme.com,10.0\n'
        'o5,"eve@acme.com,broken_quote,99.0\n',
        encoding='utf-8')
    clean_pq = os.path.join(self.tmp, 'orders_clean.parquet')
    rej_jl = os.path.join(self.tmp, 'orders_rejected.jsonl')
    rc, out, err = _run_cli([
        'pipe',
        dirty_csv,
        clean_pq,
        '--from-opt',
        'fields=amount:float',
        '--expect',
        'valid_order: order_id IS NOT NULL AND amount > 0 -> dlq',
        '--dlq',
        rej_jl,
        '--dlq-redact-fields',
        'customer_email',
        '--assert-counts',
        '--json',
    ])
    self.assertEqual(rc, 0, msg=err or out)
    env = json.loads(out)
    self.assertEqual(env['runner'], 'local_fast')
    self.assertEqual(env['metrics']['rows_written'], 1)
    self.assertEqual(env['metrics']['dlq_rows'], 4)
    import pyarrow as pa
    import pyarrow.parquet as pq
    tbl = pq.read_table(clean_pq)
    self.assertTrue(pa.types.is_floating(tbl.schema.field('amount').type))
    rej_txt = Path(rej_jl).read_text(encoding='utf-8')
    for leaked in ('bob@acme.com', 'carol@acme.com', 'dave@acme.com', 'eve@acme.com'):
      self.assertNotIn(leaked, rej_txt)
    self.assertIn('<REDACTED>', rej_txt)

  def test_two_kafka_streams_sql_join_opt_file_and_allowed_lateness(self):
    kafka_c_opt = os.path.join(self.tmp, 'kafka_c.opts')
    kafka_o_opt = os.path.join(self.tmp, 'kafka_o.opts')
    cat_opt = os.path.join(self.tmp, 'cat_j.opts')
    Path(kafka_c_opt).write_text(
        'security_protocol=SASL_SSL\nsasl_mechanism=SCRAM-SHA-256\nsasl_username=clicks-svc\n',
        encoding='utf-8')
    Path(kafka_o_opt).write_text(
        'security_protocol=SASL_SSL\nsasl_mechanism=SCRAM-SHA-256\nsasl_username=orders-svc\n',
        encoding='utf-8')
    Path(cat_opt).write_text('s3.region=us-east-1\n', encoding='utf-8')
    for p in (kafka_c_opt, kafka_o_opt, cat_opt):
      os.chmod(p, stat.S_IRUSR | stat.S_IWUSR)

    rc, out, err = _run_cli(
        [
            'pipe',
            '--from',
            'c=kafka://broker:9092/clicks?timestamp_policy=CreateTime&format=json&consumer_group=attr-clicks&commit_offset_in_finalize=true',
            '--join',
            'o=kafka://broker:9092/orders?timestamp_policy=CreateTime&format=json&consumer_group=attr-orders&commit_offset_in_finalize=true',
            '--to',
            'iceberg://lake.attributed_orders?warehouse=s3://bkt/wh',
            '--to-opt',
            'triggering_frequency_seconds=30',
            '--allowed-lateness',
            '5m',
            '--sql',
            "SELECT o.order_id, o.user_id, c.campaign_id, o.amount, o.order_ts FROM c JOIN o ON c.user_id = o.user_id AND o.order_ts BETWEEN c.click_ts AND c.click_ts + INTERVAL '15' MINUTE",
            '--max-watermark-skew',
            '30m',
            '--skew-action',
            'dlq',
            '--dlq',
            's3://bkt/dlq/skew/',
            '--from-opt-file',
            kafka_c_opt,
            '--join-opt-file',
            kafka_o_opt,
            '--to-opt-file',
            cat_opt,
            '--emit-yaml',
            '--json',
        ],
        env_overrides={'BEAM_KAFKA_SASL_PASSWORD': 'secret-pwd'})
    self.assertEqual(rc, 0, msg=err or out)
    self.assertNotIn('secret-pwd', out)
    yaml_txt = json.loads(out)['outputs']['yaml']
    self.assertIn('timestamp_col: click_ts', yaml_txt)
    self.assertIn('timestamp_col: order_ts', yaml_txt)
    self.assertIn('allowed_lateness: 5m', yaml_txt)
    self.assertIn('clicks-svc', yaml_txt)
    self.assertIn('orders-svc', yaml_txt)

  def test_skill_and_llms_txt_agreement_guardrails(self):
    manual = llms_txt.render_llms_txt()
    self.assertIn('beam catalog sync', manual)
    self.assertIn('--zero-copy', manual)
    self.assertIn('overwrite_partitions=true', manual)
    self.assertIn('--join-opt-file kafka.opts', manual)
    self.assertIn('percentile(x, 95)', manual)
    self.assertIn('quantile(x, 0.95)', manual)
    self.assertIn('256 MiB', manual)
    self.assertNotIn('percentile(x, 0.95)', manual)

    repo_root = Path(__file__).resolve().parents[4]
    skill_dir = repo_root / '.agents' / 'skills' / 'apache-beam-cli'
    if not skill_dir.is_dir():
      skill_dir = Path.cwd() / '.agents' / 'skills' / 'apache-beam-cli'
    if skill_dir.is_dir():
      skill_md = (skill_dir / 'SKILL.md').read_text(encoding='utf-8')
      sql_cdc_md = (
          skill_dir / 'references' / 'sql-and-cdc.md').read_text(encoding='utf-8')
      cloud_md = (
          skill_dir / 'references' / 'cloud-and-flex.md').read_text(
              encoding='utf-8')
      total_bytes = sum(
          len(c.encode('utf-8')) for c in (skill_md, sql_cdc_md, cloud_md))
      self.assertLess(total_bytes, 49000)
      frontmatter = skill_md.split('---', 2)[1]
      desc_line = next(
          line.split(':', 1)[1].strip()
          for line in frontmatter.splitlines()
          if line.startswith('description:'))
      self.assertLessEqual(len(desc_line), 1024)
      self.assertIn('apache-beam-cli-preview v0.2.0', frontmatter)
      self.assertIn('Use when', frontmatter)
      for content in (skill_md, sql_cdc_md, cloud_md):
        low = content.lower()
        self.assertNotIn('do not use `beam`', low)
        self.assertNotIn('percentile(x, 0.95)', content)
        self.assertNotIn('requires unpartitioned delta tables', low)
        for hype in (
            'enterprise',
            'production-ready',
            'production-hardened',
            'production-grade',
            'turnkey',
            'mathematical',
        ):
          self.assertNotIn(hype, low)
      self.assertIn('beam catalog sync', skill_md)
      self.assertIn('--zero-copy', skill_md)
      self.assertIn('Skills & Tools to Pair With', skill_md)
      self.assertIn('Operating Streaming, CDC, & Scheduled Jobs', skill_md)
      self.assertIn('--checkpointing_interval=30000', skill_md)
      self.assertIn('--join-opt-file kafka.opts', skill_md)
      self.assertIn('overwrite_partitions=true', skill_md)
      self.assertIn('fields=amount:float', skill_md)
      self.assertIn('snapshot.mode: when_needed', skill_md)
      self.assertIn('pg_drop_replication_slot', sql_cdc_md)
      self.assertIn('REPLICA IDENTITY FULL', sql_cdc_md)
      self.assertIn('mysql://', sql_cdc_md)
      self.assertIn('kafka.opts.example', cloud_md)
      self.assertIn('pg.opts.example', cloud_md)
      self.assertIn('rewrite_data_files', cloud_md)


if __name__ == '__main__':
  unittest.main()
