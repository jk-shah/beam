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

"""Tests for distributed Data Quality Expectations (Proposal 1)."""

import csv
import json
import os
import shutil
import tempfile
import unittest

from apache_beam.cli import expect
from apache_beam.cli import fast_local
from apache_beam.cli import pipe_spec


class ExpectRuleParsingTest(unittest.TestCase):

  def test_parse_simple_string(self):
    rule = expect.parse_expectation_rule('amount > 0.0')
    self.assertEqual(rule.name, 'amount > 0.0')
    self.assertEqual(rule.expression, 'amount > 0.0')
    self.assertEqual(rule.on_violation, 'dlq')

  def test_parse_named_rule(self):
    rule = expect.parse_expectation_rule('positive_amt: amount > 0.0')
    self.assertEqual(rule.name, 'positive_amt')
    self.assertEqual(rule.expression, 'amount > 0.0')
    self.assertEqual(rule.on_violation, 'dlq')

  def test_parse_action_suffix(self):
    rule1 = expect.parse_expectation_rule('amount > 0.0 -> fail')
    self.assertEqual(rule1.name, 'amount > 0.0')
    self.assertEqual(rule1.expression, 'amount > 0.0')
    self.assertEqual(rule1.on_violation, 'fail')

    rule2 = expect.parse_expectation_rule('valid_curr: currency in ["USD", "EUR"] -> drop')
    self.assertEqual(rule2.name, 'valid_curr')
    self.assertEqual(rule2.expression, 'currency in ["USD", "EUR"]')
    self.assertEqual(rule2.on_violation, 'drop')

  def test_parse_dict_spec(self):
    rule = expect.parse_expectation_rule({
        'name': 'non_null_id',
        'expression': 'id is not None',
        'on_violation': 'fail',
    })
    self.assertEqual(rule.name, 'non_null_id')
    self.assertEqual(rule.expression, 'id is not None')
    self.assertEqual(rule.on_violation, 'fail')

  def test_sql_expression_normalization(self):
    expr = 'customer_id IS NOT NULL AND status <> "CANCELLED" AND amount > 0'
    normalized = expect.normalize_expectation_expression(expr)
    self.assertIn('is not None', normalized)
    self.assertIn('!=', normalized)
    self.assertIn('and', normalized)

  def test_invalid_rule_raises(self):
    with self.assertRaises(ValueError):
      expect.parse_expectation_rule('')
    with self.assertRaises(ValueError):
      expect.parse_expectation_rule('amount > 0 -> invalid_action')
    with self.assertRaises(ValueError):
      expect.parse_expectation_rule({'name': 'missing_expr'})


class ExpectEvaluationTest(unittest.TestCase):

  def test_evaluate_dict_and_row(self):
    code = compile('amount > 10.0 and status == "ACTIVE"', '<test>', 'eval')
    self.assertTrue(expect.evaluate_record(code, {'amount': 15.0, 'status': 'ACTIVE'}))
    self.assertFalse(expect.evaluate_record(code, {'amount': 5.0, 'status': 'ACTIVE'}))
    self.assertFalse(expect.evaluate_record(code, {'amount': 15.0, 'status': 'PENDING'}))

  def test_evaluate_handles_null_safely(self):
    code = compile('amount > 0', '<test>', 'eval')
    # When amount is None or missing, evaluate_record catches TypeError/KeyError and returns False
    self.assertFalse(expect.evaluate_record(code, {'amount': None}))
    self.assertFalse(expect.evaluate_record(code, {}))


class ExpectPipeSpecTest(unittest.TestCase):

  def test_chain_steps_when_no_dlq_sink(self):
    pipe = pipe_spec.build_pipe_spec(
        source='sample.csv',
        sink='out.csv',
        expect=['amount > 0 -> fail'],
    )
    transforms = pipe.spec['pipeline']['transforms']
    types = [t['type'] for t in transforms]
    self.assertIn('CliExpect', types)
    expect_step = next(t for t in transforms if t['type'] == 'CliExpect')
    self.assertEqual(len(expect_step['config']['rules']), 1)
    self.assertEqual(expect_step['config']['rules'][0]['on_violation'], 'fail')

  def test_dag_transforms_with_dlq_sink(self):
    pipe = pipe_spec.build_pipe_spec(
        source='sample.csv',
        sink='out.csv',
        dlq_sink='err.jsonl',
        expect=['positive_amount: amount > 0.0'],
    )
    transforms = pipe.spec['pipeline']['transforms']
    names = [t.get('name') for t in transforms]
    self.assertIn('ValidateExpect', names)
    self.assertIn('NormalizeExpectDlq', names)
    self.assertIn('MergeDlq', names)

  def test_dlq_action_without_dlq_sink_fails(self):
    with self.assertRaises(pipe_spec.PipeSpecError) as ctx:
      pipe_spec.build_pipe_spec(
          source='sample.csv',
          sink='out.csv',
          expect=['amount > 0'],
          on_violation='dlq',
      )
    self.assertIn('cannot route expectation violations to DLQ', str(ctx.exception))

  def test_zero_copy_disallows_expect(self):
    with self.assertRaises(pipe_spec.PipeSpecError) as ctx:
      pipe_spec.build_pipe_spec(
          source='delta://table',
          sink='iceberg://dest',
          zero_copy=True,
          expect=['amount > 0 -> fail'],
      )
    self.assertIn('--zero-copy', str(ctx.exception))


class ExpectFastLocalExecutionTest(unittest.TestCase):

  def setUp(self):
    self.tmp_dir = tempfile.mkdtemp()
    self.input_file = os.path.join(self.tmp_dir, 'input.csv')
    self.output_file = os.path.join(self.tmp_dir, 'output.csv')
    self.dlq_file = os.path.join(self.tmp_dir, 'dlq.jsonl')

    # 4 rows: row 1 and 2 are good, row 3 has negative amount, row 4 has 0 amount
    with open(self.input_file, 'w', newline='', encoding='utf-8') as f:
      writer = csv.writer(f)
      writer.writerow(['id', 'amount', 'currency'])
      writer.writerow(['1', '100.5', 'USD'])
      writer.writerow(['2', '250.0', 'EUR'])
      writer.writerow(['3', '-50.0', 'USD'])
      writer.writerow(['4', '0.0', 'GBP'])

  def tearDown(self):
    shutil.rmtree(self.tmp_dir)

  def test_fast_local_dlq_routing(self):
    pipe = pipe_spec.build_pipe_spec(
        source=self.input_file,
        sink=self.output_file,
        dlq_sink=self.dlq_file,
        expect=['positive_amount: amount > 0.0'],
        on_violation='dlq',
    )
    result = fast_local.run(pipe, [self.input_file], assert_counts=True)
    self.assertEqual(result.metrics['rows_read'], 4)
    self.assertEqual(result.metrics['rows_written'], 2)  # IDs 1 and 2
    self.assertEqual(result.metrics['dlq_rows'], 2)     # IDs 3 and 4
    self.assertEqual(result.metrics['expectation_passed_positive_amount'], 2)
    self.assertEqual(result.metrics['expectation_quarantined_positive_amount'], 2)

    # Check output file rows
    with open(self.output_file, 'r', encoding='utf-8') as f:
      lines = [line.strip() for line in f if line.strip()]
      self.assertEqual(len(lines), 3)  # header + 2 rows

    # Check DLQ file rows
    with open(self.dlq_file, 'r', encoding='utf-8') as f:
      dlq_lines = [json.loads(line) for line in f if line.strip()]
      self.assertEqual(len(dlq_lines), 2)
      self.assertEqual(dlq_lines[0]['step'], 'CliExpect')
      self.assertEqual(dlq_lines[0]['error_type'], 'ExpectationViolation')

  def test_fast_local_drop_action(self):
    pipe = pipe_spec.build_pipe_spec(
        source=self.input_file,
        sink=self.output_file,
        expect=['positive_amount: amount > 0.0 -> drop'],
    )
    result = fast_local.run(pipe, [self.input_file], assert_counts=True)
    self.assertEqual(result.metrics['rows_read'], 4)
    self.assertEqual(result.metrics['rows_written'], 2)
    self.assertEqual(result.metrics['rows_filtered_out'], 2)
    self.assertEqual(result.metrics['expectation_passed_positive_amount'], 2)
    self.assertEqual(result.metrics['expectation_dropped_positive_amount'], 2)

  def test_fast_local_fail_action(self):
    pipe = pipe_spec.build_pipe_spec(
        source=self.input_file,
        sink=self.output_file,
        expect=['positive_amount: amount > 0.0 -> fail'],
    )
    with self.assertRaises(fast_local.FastPipeError) as ctx:
      fast_local.run(pipe, [self.input_file], assert_counts=True)
    self.assertIn("Expectation rule 'positive_amount'", str(ctx.exception))


class ExpectBeamRunnerExecutionTest(unittest.TestCase):
  """Tests CliExpect directly on real Apache Beam runner."""

  def test_beam_direct_runner_expect_transform(self):
    import apache_beam as beam
    from apache_beam.runners.direct.direct_runner import BundleBasedDirectRunner
    from apache_beam.testing.util import assert_that, equal_to

    records = [
        beam.Row(id=1, amount=50.0),
        beam.Row(id=2, amount=-10.0),
        beam.Row(id=3, amount=75.0),
    ]

    with beam.Pipeline(runner=BundleBasedDirectRunner()) as p:
      inp = p | beam.Create(records)
      transform = expect.cli_expect(
          rules=[{'name': 'pos_amt', 'expression': 'amount > 0.0', 'on_violation': 'dlq'}],
          error_handling={'output': 'errors'},
      )
      outputs = inp | transform
      good = outputs['good']
      errors = outputs['errors']

      assert_that(
          good | 'ExtractGoodIds' >> beam.Map(lambda r: r.id),
          equal_to([1, 3]),
          label='CheckGoodRows',
      )
      assert_that(
          errors | 'ExtractErrorSteps' >> beam.Map(lambda r: r.step),
          equal_to(['CliExpect']),
          label='CheckErrorRows',
      )


if __name__ == '__main__':
  unittest.main()
