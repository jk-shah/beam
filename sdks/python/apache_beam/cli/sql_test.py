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
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET
from unittest import mock

from apache_beam.cli import sql
from apache_beam.cli.sql import SqlError


def _proc(returncode=0, stdout='', stderr=''):
  return subprocess.CompletedProcess(
      args=[], returncode=returncode, stdout=stdout, stderr=stderr)


class SqlShellTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp()
    self.addCleanup(shutil.rmtree, self.tmp)
    self.beam_home = os.path.join(self.tmp, '.beam')

  def test_cache_hit_skips_maven_and_matches_beam_sql_sh_filename(self):
    cache_dir = os.path.join(self.beam_home, 'cache')
    os.makedirs(cache_dir)
    cases = [
        ('direct', [], 'beam-2.75.0_runner-direct_ios-.jar'),
        (
            'DataflowRunner', ['parquet', 'CSV', 'csv'],
            'beam-2.75.0_runner-dataflow_ios-csv-parquet.jar'),
    ]
    for runner, ios, expected_name in cases:
      with self.subTest(runner=runner, ios=ios):
        jar_path = os.path.join(cache_dir, expected_name)
        with open(jar_path, 'wb') as f:
          f.write(b'cached-jar')
        calls = []

        def fake_run(cmd, **kwargs):
          calls.append(cmd)
          if cmd[-1] == '-version':
            return _proc(0, stderr='openjdk version "17.0.9" 2023-10-17')
          return _proc(0)

        with (
            mock.patch.dict(os.environ, {}, clear=True),
            mock.patch('shutil.which', return_value='/usr/bin/java'),
            mock.patch('subprocess.run', side_effect=fake_run),
            contextlib.redirect_stderr(io.StringIO()),
        ):
          rc = sql.run(
              version='2.75.0',
              runner=runner,
              ios=ios,
              sqlline_args=['-e', 'SELECT 1'],
              beam_home=self.beam_home)
        self.assertEqual(rc, 0)
        self.assertEqual(
            calls[-1],
            [
                '/usr/bin/java',
                '-cp',
                jar_path,
                'org.apache.beam.sdk.extensions.sql.jdbc.BeamSqlLine',
                '-e',
                'SELECT 1',
            ])

  def test_cache_miss_builds_shaded_jar_and_caches_atomically(self):
    pom_captured = []

    def fake_download(url, dest):
      with open(dest, 'wb') as f:
        f.write(b'stub')

    def fake_run(cmd, **kwargs):
      if cmd[-1] == '-version':
        return _proc(0, stderr='openjdk version "21.0.2"')
      if cmd[0].endswith('mvnw'):
        pom_file = cmd[cmd.index('-f') + 1]
        with open(pom_file, encoding='utf-8') as f:
          pom_captured.append(f.read())
        target = os.path.join(os.path.dirname(pom_file), 'target')
        os.makedirs(target)
        with open(os.path.join(target, 'beam-sql-shell-runner-1.0.jar'),
                  'wb') as f:
          f.write(b'built-uber-jar')
        return _proc(0)
      return _proc(0)

    with (
        mock.patch.dict(os.environ, {}, clear=True),
        mock.patch('shutil.which', return_value='/opt/jdk21/bin/java'),
        mock.patch.object(sql, '_download_https', side_effect=fake_download),
        mock.patch('subprocess.run', side_effect=fake_run),
        contextlib.redirect_stderr(io.StringIO()),
    ):
      rc = sql.run(
          version='2.75.0',
          runner='dataflow',
          ios=['csv'],
          beam_home=self.beam_home)

    self.assertEqual(rc, 0)
    cached = os.path.join(
        self.beam_home, 'cache', 'beam-2.75.0_runner-dataflow_ios-csv.jar')
    self.assertTrue(os.path.isfile(cached))
    # Generated POM must parse as valid XML and shade ServiceLoader entries.
    ET.fromstring(pom_captured[0])
    self.assertIn('beam-sdks-java-io-csv', pom_captured[0])
    self.assertIn('beam-runners-google-cloud-dataflow-java', pom_captured[0])
    self.assertIn('ServicesResourceTransformer', pom_captured[0])

  def test_java_prerequisite_checks_exit_with_4(self):
    cases = [
        ('missing java', None, _proc(0), 'not found'),
        (
            'macOS /usr/bin/java stub',
            '/usr/bin/java',
            _proc(1, stderr='Unable to locate a Java Runtime.'),
            'no working Java runtime'),
        (
            'Java 8 too old',
            '/usr/bin/java',
            _proc(0, stderr='java version "1.8.0_392"'),
            'found Java 8'),
    ]
    for label, which_java, version_proc, msg in cases:
      with self.subTest(label=label):
        with (
            mock.patch.dict(os.environ, {}, clear=True),
            mock.patch('shutil.which', return_value=which_java),
            mock.patch('subprocess.run', return_value=version_proc),
        ):
          with self.assertRaisesRegex(SqlError, msg) as ctx:
            sql.run(version='2.75.0', beam_home=self.beam_home)
          self.assertEqual(ctx.exception.exit_code, 4)

  def test_warns_on_bleeding_edge_java(self):
    cache_dir = os.path.join(self.beam_home, 'cache')
    os.makedirs(cache_dir)
    with open(os.path.join(cache_dir, 'beam-2.75.0_runner-direct_ios-.jar'),
              'wb') as f:
      f.write(b'jar')
    err = io.StringIO()
    with (
        mock.patch.dict(os.environ, {}, clear=True),
        mock.patch('shutil.which', return_value='/usr/bin/java'),
        mock.patch('subprocess.run',
                   side_effect=[
                       _proc(0, stderr='openjdk version "26.0.2.1"'),
                       _proc(0),
                   ]),
        contextlib.redirect_stderr(err),
    ):
      sql.run(version='2.75.0', beam_home=self.beam_home)
    self.assertIn('warning: Java 26', err.getvalue())

  def test_usage_and_maven_artifact_errors_exit_with_2(self):
    for kwargs, pattern in [
        ({'version': '2.75.0', 'runner': 'spark'}, 'unsupported runner'),
        ({'version': '2.75.0', 'ios': ['../bad']}, 'invalid --io'),
        ({'version': '2.75;rm'}, 'invalid Beam version'),
    ]:
      with self.subTest(kwargs=kwargs):
        with self.assertRaisesRegex(SqlError, pattern) as ctx:
          sql.run(beam_home=self.beam_home, **kwargs)
        self.assertEqual(ctx.exception.exit_code, 2)

  def test_dev_sdk_resolves_latest_release_from_maven_metadata(self):
    metadata = (
        '<metadata><versioning><versions>'
        '<version>2.9.0</version>'
        '<version>2.75.0</version>'
        '<version>2.76.0-SNAPSHOT</version>'
        '</versions></versioning></metadata>')
    with (
        mock.patch.object(sql.beam_version, '__version__', '2.76.0.dev'),
        mock.patch.object(sql, '_fetch_text', return_value=metadata),
        contextlib.redirect_stderr(io.StringIO()),
    ):
      self.assertEqual(sql._resolve_version(None), '2.75.0')

  def test_list_flags_online_and_offline_fallback(self):
    solr_ios = json.dumps({
        'response': {
            'docs': [
                {
                    'a': 'beam-sdks-java-io-parquet'
                },
                {
                    'a': 'beam-sdks-java-io-csv'
                },
                {
                    'a': 'beam-sdks-java-io-kafka-upgrade'
                },
            ]
        }
    })
    out = io.StringIO()
    with (
        mock.patch.object(sql, '_fetch_text', return_value=solr_ios),
        contextlib.redirect_stdout(out),
    ):
      self.assertEqual(sql.run(list_ios=True), 0)
    self.assertIn('csv parquet', out.getvalue())
    self.assertNotIn('upgrade', out.getvalue())

    out_offline, err_offline = io.StringIO(), io.StringIO()
    with (
        mock.patch.object(sql, '_fetch_text', return_value=None),
        contextlib.redirect_stdout(out_offline),
        contextlib.redirect_stderr(err_offline),
    ):
      self.assertEqual(sql.run(list_ios=True), 1)
    self.assertIn('parquet', out_offline.getvalue())


if __name__ == '__main__':
  unittest.main()
