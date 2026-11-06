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
import urllib.error
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
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
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

        err = io.StringIO()
        with (
            mock.patch.dict(os.environ, {}, clear=True),
            mock.patch('shutil.which', return_value='/usr/bin/java'),
            mock.patch('subprocess.run', side_effect=fake_run),
            contextlib.redirect_stderr(err),
        ):
          rc = sql.run(
              version='2.75.0',
              runner=runner,
              ios=ios,
              debug=True,
              sqlline_args=['-e', 'SELECT 1'],
              beam_home=self.beam_home)
        self.assertEqual(rc, 0)
        self.assertIn('+ /usr/bin/java -cp', err.getvalue())
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
      # Second build with cached Maven Wrapper and debug=True.
      os.remove(
          os.path.join(
              self.beam_home,
              'cache',
              'beam-2.75.0_runner-dataflow_ios-csv.jar'))
      rc2 = sql.run(
          version='2.75.0',
          runner='dataflow',
          ios=['csv'],
          debug=True,
          beam_home=self.beam_home)

    self.assertEqual(rc, 0)
    self.assertEqual(rc2, 0)
    cached = os.path.join(
        self.beam_home, 'cache', 'beam-2.75.0_runner-dataflow_ios-csv.jar')
    self.assertTrue(os.path.isfile(cached))
    # Generated POM must parse as valid XML and shade ServiceLoader entries.
    ET.fromstring(pom_captured[0])
    self.assertIn('beam-sdks-java-io-csv', pom_captured[0])
    self.assertIn('beam-runners-google-cloud-dataflow-java', pom_captured[0])
    self.assertIn('ServicesResourceTransformer', pom_captured[0])

    # Partial `.part` JAR is cleaned up if atomic replace fails.
    with (
        mock.patch.dict(os.environ, {}, clear=True),
        mock.patch('shutil.which', return_value='/opt/jdk21/bin/java'),
        mock.patch('subprocess.run', side_effect=fake_run),
        mock.patch('os.replace', side_effect=OSError('disk full')),
        contextlib.redirect_stderr(io.StringIO()),
    ):
      os.remove(cached)
      with self.assertRaises(OSError):
        sql.run(
            version='2.75.0',
            runner='dataflow',
            ios=['csv'],
            beam_home=self.beam_home)
    self.assertEqual(os.listdir(os.path.join(self.beam_home, 'cache')), [])

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

    # JAVA_HOME missing bin/java, valid JAVA_HOME, and OSError on execution.
    bad_home = os.path.join(self.tmp, 'bad_jdk')
    with mock.patch.dict(os.environ, {'JAVA_HOME': bad_home}, clear=True):
      with self.assertRaisesRegex(SqlError, 'does not contain bin/java') as ctx:
        sql._find_java()
      self.assertEqual(ctx.exception.exit_code, 4)

    good_home = os.path.join(self.tmp, 'good_jdk')
    os.makedirs(os.path.join(good_home, 'bin'))
    java_bin = os.path.join(good_home, 'bin', 'java')
    with open(java_bin, 'w', encoding='utf-8') as f:
      f.write('stub')
    with (
        mock.patch.dict(os.environ, {'JAVA_HOME': good_home}, clear=True),
        mock.patch('shutil.which', return_value=None),
        mock.patch('subprocess.run', side_effect=OSError('exec format error')),
    ):
      with self.assertRaisesRegex(SqlError, 'could not execute') as ctx:
        sql._find_java()
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

  def test_usage_and_maven_artifact_errors(self):
    for kwargs, pattern in [
        ({'version': '2.75.0', 'runner': 'spark'}, 'unsupported runner'),
        ({'version': '2.75.0', 'ios': ['../bad']}, 'invalid --io'),
        ({'version': '2.75;rm'}, 'invalid Beam version'),
    ]:
      with self.subTest(kwargs=kwargs):
        with self.assertRaisesRegex(SqlError, pattern) as ctx:
          sql.run(beam_home=self.beam_home, **kwargs)
        self.assertEqual(ctx.exception.exit_code, 2)

    # Maven missing artifact (exit 2) vs generic Maven build failure (exit 1).
    with (
        mock.patch(
            'subprocess.run',
            return_value=_proc(
                1,
                stderr='[ERROR] Could not find artifact org.apache.beam:bad:1.0'
            )),
        contextlib.redirect_stderr(io.StringIO()),
    ):
      with self.assertRaisesRegex(SqlError, 'could not resolve') as ctx:
        sql._build_shaded_jar(
            'mvnw',
            '/jdk',
            '2.75.0',
            'direct', [],
            os.path.join(self.tmp, 'out.jar'),
            False)
      self.assertEqual(ctx.exception.exit_code, 2)

    with (
        mock.patch('subprocess.run',
                   return_value=_proc(1, stderr='compilation error')),
        contextlib.redirect_stderr(io.StringIO()),
    ):
      with self.assertRaisesRegex(SqlError, 'Maven build.*failed') as ctx:
        sql._build_shaded_jar(
            'mvnw',
            '/jdk',
            '2.75.0',
            'direct', [],
            os.path.join(self.tmp, 'out.jar'),
            False)
      self.assertEqual(ctx.exception.exit_code, 1)

  def test_resolve_version_release_and_dev(self):
    with mock.patch.object(sql.beam_version, '__version__', '2.75.0'):
      self.assertEqual(sql._resolve_version(None), '2.75.0')

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

    with (
        mock.patch.object(sql.beam_version, '__version__', '2.76.0.dev'),
        mock.patch.object(sql, '_fetch_text', return_value=None),
    ):
      with self.assertRaisesRegex(SqlError, 'unreleased') as ctx:
        sql._resolve_version(None)
      self.assertEqual(ctx.exception.exit_code, 1)

  def test_download_https_and_fetch_text(self):
    with self.assertRaises(ValueError):
      sql._download_https('http://insecure.example/x', 'dest')
    with self.assertRaises(ValueError):
      sql._fetch_text('http://insecure.example/x')

    dest = os.path.join(self.tmp, 'downloaded.bin')
    resp = io.BytesIO(b'hello-bytes')
    with mock.patch('urllib.request.urlopen',
                    return_value=contextlib.nullcontext(resp)):
      sql._download_https('https://example.com/file', dest)
    with open(dest, 'rb') as f:
      self.assertEqual(f.read(), b'hello-bytes')

    resp_txt = io.BytesIO(b'hello-text')
    with mock.patch('urllib.request.urlopen',
                    return_value=contextlib.nullcontext(resp_txt)):
      self.assertEqual(sql._fetch_text('https://example.com/txt'), 'hello-text')

    with mock.patch('urllib.request.urlopen',
                    side_effect=urllib.error.URLError('offline')):
      self.assertIsNone(sql._fetch_text('https://example.com/txt'))
      with self.assertRaisesRegex(SqlError, 'failed to download') as ctx:
        sql._download_https('https://example.com/file2', dest + '2')
      self.assertEqual(ctx.exception.exit_code, 1)

    # If a partial `.part` file exists when writing fails, it is cleaned up.
    part_dest = os.path.join(self.tmp, 'part_cleanup.bin')
    with (
        mock.patch('urllib.request.urlopen',
                   return_value=contextlib.nullcontext(io.BytesIO(b'data'))),
        mock.patch('os.replace', side_effect=OSError('disk full')),
    ):
      with self.assertRaises(SqlError):
        sql._download_https('https://example.com/file3', part_dest)
    self.assertFalse(os.path.exists(part_dest + '.part'))

  def test_list_flags_online_and_offline_fallback(self):
    # 1. --list-versions online, malformed XML, and offline.
    metadata = (
        '<metadata><versioning><versions>'
        '<version>2.74.0</version><version>2.75.0</version>'
        '</versions></versioning></metadata>')
    out = io.StringIO()
    with (
        mock.patch.object(sql, '_fetch_text', return_value=metadata),
        contextlib.redirect_stdout(out),
    ):
      self.assertEqual(sql.run(list_versions=True), 0)
    self.assertIn('2.75.0', out.getvalue())

    with mock.patch.object(sql, '_fetch_text', return_value='<not-xml'):
      with self.assertRaises(SqlError) as ctx:
        sql.run(list_versions=True)
      self.assertEqual(ctx.exception.exit_code, 1)

    # 2. --list-ios online, invalid JSON, and offline fallback.
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
        mock.patch.object(sql, '_fetch_text', return_value='{bad-json'),
        contextlib.redirect_stdout(out_offline),
        contextlib.redirect_stderr(err_offline),
    ):
      self.assertEqual(sql.run(list_ios=True), 1)
    self.assertIn('parquet', out_offline.getvalue())

    # 3. --list-runners online and offline fallback.
    solr_runners = json.dumps({
        'response': {
            'docs': [
                {
                    'a': 'beam-runners-direct-java'
                },
                {
                    'a': 'beam-runners-google-cloud-dataflow-java'
                },
                {
                    'a': 'beam-runners-flink-java'
                },
            ]
        }
    })
    out_runners = io.StringIO()
    with (
        mock.patch.object(sql, '_fetch_text', return_value=solr_runners),
        contextlib.redirect_stdout(out_runners),
    ):
      self.assertEqual(sql.run(version='2.75.0', list_runners=True), 0)
    self.assertIn('DirectRunner', out_runners.getvalue())
    self.assertIn('DataflowRunner', out_runners.getvalue())
    self.assertIn('flink', out_runners.getvalue())

    out_r_off, err_r_off = io.StringIO(), io.StringIO()
    with (
        mock.patch.object(sql, '_fetch_text', return_value=None),
        contextlib.redirect_stdout(out_r_off),
        contextlib.redirect_stderr(err_r_off),
    ):
      self.assertEqual(sql.run(version='2.75.0', list_runners=True), 1)
    self.assertIn('DirectRunner', out_r_off.getvalue())


if __name__ == '__main__':
  unittest.main()
