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
import zipfile
from unittest import mock

from apache_beam.cli import sql
from apache_beam.cli.sql import SqlError


def _proc(returncode=0, stdout='', stderr=''):
  return subprocess.CompletedProcess(
      args=[], returncode=returncode, stdout=stdout, stderr=stderr)


def _write_dummy_jar(path: str) -> None:
  os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
  with zipfile.ZipFile(path, 'w') as zf:
    zf.writestr('META-INF/MANIFEST.MF', 'Manifest-Version: 1.0\n')


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
        _write_dummy_jar(jar_path)
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

  def test_cache_miss_builds_shaded_jar_via_maven_wrapper_main(self):
    pom_captured = []
    wrapper_cmds = []

    def fake_download(url, dest, **kwargs):
      _write_dummy_jar(dest)

    def fake_run(cmd, **kwargs):
      if cmd[-1] == '-version':
        return _proc(0, stderr='openjdk version "21.0.2"')
      if 'org.apache.maven.wrapper.MavenWrapperMain' in cmd:
        wrapper_cmds.append(cmd)
        pom_file = cmd[cmd.index('-f') + 1]
        with open(pom_file, encoding='utf-8') as f:
          pom_captured.append(f.read())
        target = os.path.join(os.path.dirname(pom_file), 'target')
        os.makedirs(target)
        _write_dummy_jar(os.path.join(target, 'beam-sql-shell-runner-1.0.jar'))
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
    self.assertEqual(len(wrapper_cmds), 1)
    self.assertEqual(wrapper_cmds[0][0], '/opt/jdk21/bin/java')
    self.assertTrue(
        wrapper_cmds[0][1].startswith('-Dmaven.multiModuleProjectDirectory='))
    self.assertFalse(
        os.path.exists(os.path.join(self.beam_home, 'maven-wrapper', 'mvnw')))
    cached = os.path.join(
        self.beam_home, 'cache', 'beam-2.75.0_runner-dataflow_ios-csv.jar')
    self.assertTrue(zipfile.is_zipfile(cached))
    # Generated POM must parse as valid XML and shade ServiceLoader entries.
    ET.fromstring(pom_captured[0])
    self.assertIn('beam-sdks-java-io-csv', pom_captured[0])
    self.assertIn('beam-runners-google-cloud-dataflow-java', pom_captured[0])
    self.assertIn('ServicesResourceTransformer', pom_captured[0])

  def test_corrupt_cached_jar_is_rebuilt_not_trusted(self):
    cache_dir = os.path.join(self.beam_home, 'cache')
    os.makedirs(cache_dir)
    corrupt_jar = os.path.join(
        cache_dir, 'beam-2.75.0_runner-direct_ios-.jar')
    with open(corrupt_jar, 'wb') as f:
      f.write(b'corrupt-truncated-not-a-zip')

    rebuilt = []

    def fake_download(url, dest, **kwargs):
      _write_dummy_jar(dest)

    def fake_run(cmd, **kwargs):
      if cmd[-1] == '-version':
        return _proc(0, stderr='openjdk version "21.0.2"')
      if 'org.apache.maven.wrapper.MavenWrapperMain' in cmd:
        rebuilt.append(True)
        pom_file = cmd[cmd.index('-f') + 1]
        _write_dummy_jar(
            os.path.join(
                os.path.dirname(pom_file),
                'target',
                'beam-sql-shell-runner-1.0.jar'))
        return _proc(0)
      return _proc(0)

    with (
        mock.patch.dict(os.environ, {}, clear=True),
        mock.patch('shutil.which', return_value='/opt/jdk21/bin/java'),
        mock.patch.object(sql, '_download_https', side_effect=fake_download),
        mock.patch('subprocess.run', side_effect=fake_run),
        contextlib.redirect_stderr(io.StringIO()),
    ):
      rc = sql.run(version='2.75.0', beam_home=self.beam_home)
    self.assertEqual(rc, 0)
    self.assertEqual(rebuilt, [True])
    self.assertTrue(zipfile.is_zipfile(corrupt_jar))

  def test_windows_java_home_resolves_java_exe(self):
    fake_jdk = os.path.join(self.tmp, 'jdk21')
    os.makedirs(os.path.join(fake_jdk, 'bin'))
    java_exe = os.path.join(fake_jdk, 'bin', 'java.exe')
    with open(java_exe, 'wb') as f:
      f.write(b'MZ')

    with (
        mock.patch.dict(os.environ, {'JAVA_HOME': fake_jdk}, clear=True),
        mock.patch.object(sql.os, 'name', 'nt'),
        mock.patch(
            'subprocess.run',
            return_value=_proc(0, stderr='openjdk version "21.0.2"')),
    ):
      found_bin, found_home = sql._find_java()
    self.assertTrue(found_bin.endswith('java.exe'))
    self.assertEqual(found_home, fake_jdk)

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
    _write_dummy_jar(
        os.path.join(cache_dir, 'beam-2.75.0_runner-direct_ios-.jar'))
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
        mock.patch.object(sql,
                          '_installed_beam_version',
                          return_value='2.76.0.dev'),
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
    self.assertIn('delta', out_offline.getvalue())

  def test_delta_and_iceberg_pom_matches_upstream_beam_sql_sh(self):
    pom_xml = sql._render_pom('2.79.0', 'direct', ['delta', 'iceberg'])
    ET.fromstring(pom_xml)
    self.assertIn('beam-sdks-java-extensions-sql-delta', pom_xml)
    self.assertIn('beam-sdks-java-extensions-sql-iceberg', pom_xml)
    self.assertIn('<artifactId>gcs-connector</artifactId><version>4.0.4</version>', pom_xml)
    self.assertIn('<artifactId>iceberg-gcp</artifactId><version>1.12.0</version>', pom_xml)
    self.assertIn('<Multi-Release>true</Multi-Release>', pom_xml)
    self.assertIn('<id>apache.snapshots</id>', pom_xml)
    self.assertLess(
        pom_xml.index('beam-sdks-java-extensions-sql-delta'),
        pom_xml.index('beam-sdks-java-extensions-sql-jdbc'))

    with mock.patch.dict(os.environ, {'ICEBERG_VERSION': '../bad'}, clear=False):
      with self.assertRaisesRegex(SqlError, 'invalid ICEBERG_VERSION') as ctx:
        sql._render_pom('2.79.0', 'direct', ['iceberg'])
      self.assertEqual(ctx.exception.exit_code, 2)

  def test_list_versions_and_list_runners_online_and_offline(self):
    out_v = io.StringIO()
    with (
        mock.patch.object(
            sql, '_fetch_maven_versions', return_value=['2.79.0', '2.78.0']
        ),
        contextlib.redirect_stdout(out_v),
    ):
      self.assertEqual(sql.run(list_versions=True), 0)
    self.assertIn('2.79.0', out_v.getvalue())

    with mock.patch.object(sql, '_fetch_maven_versions', return_value=[]):
      with self.assertRaisesRegex(SqlError, 'could not retrieve Beam versions') as c_v:
        sql.run(list_versions=True)
      self.assertEqual(c_v.exception.exit_code, 1)

    solr_runners = json.dumps({
        'response': {
            'docs': [
                {'a': 'beam-runners-direct-java'},
                {'a': 'beam-runners-google-cloud-dataflow-java'},
                {'a': 'beam-runners-flink-1.18'},
                {'a': 'beam-runners-core-java'},
            ]
        }
    })
    out_r = io.StringIO()
    with (
        mock.patch.object(sql, '_fetch_text', return_value=solr_runners),
        contextlib.redirect_stdout(out_r),
    ):
      self.assertEqual(sql.run(version='2.79.0', list_runners=True), 0)
    self.assertIn('DirectRunner', out_r.getvalue())
    self.assertIn('DataflowRunner', out_r.getvalue())
    self.assertIn('flink-1.18', out_r.getvalue())

    out_ro, err_ro = io.StringIO(), io.StringIO()
    with (
        mock.patch.object(sql, '_fetch_text', return_value=None),
        contextlib.redirect_stdout(out_ro),
        contextlib.redirect_stderr(err_ro),
    ):
      self.assertEqual(sql.run(version='2.79.0', list_runners=True), 1)
    self.assertIn('DirectRunner', out_ro.getvalue())

  def test_download_https_symlink_and_maven_failure_guards(self):
    # Non-https rejected
    with self.assertRaises(ValueError):
      sql._download_https('http://example.com/a.jar', os.path.join(self.tmp, 'a.jar'))
    with self.assertRaises(ValueError):
      sql._fetch_text('http://example.com/meta.xml')

    # Redirect downgrade to http:// rejected
    handler = sql._HttpsOnlyRedirectHandler()
    with self.assertRaises(sql.urllib.error.URLError):
      handler.redirect_request(None, None, 302, 'Found', {}, 'http://evil.test/x')

    # Atomic download success & max_bytes overflow
    dest = os.path.join(self.tmp, 'dl', 'ok.jar')
    with mock.patch.object(
        sql,
        '_https_urlopen',
        side_effect=lambda u, timeout: contextlib.nullcontext(io.BytesIO(b'hello')),
    ):
      sql._download_https('https://repo.maven.apache.org/ok.jar', dest, max_bytes=16)
    with open(dest, 'rb') as f:
      self.assertEqual(f.read(), b'hello')

    with mock.patch.object(
        sql,
        '_https_urlopen',
        side_effect=lambda u, timeout: contextlib.nullcontext(io.BytesIO(b'x' * 32)),
    ):
      with self.assertRaisesRegex(SqlError, 'exceeded maximum size'):
        sql._download_https('https://repo.maven.apache.org/big.jar', dest, max_bytes=8)

    # _fetch_text bounded read + XXE guard in _fetch_maven_versions
    with mock.patch.object(
        sql,
        '_https_urlopen',
        side_effect=lambda u, timeout: contextlib.nullcontext(io.BytesIO(b'<!DOCTYPE foo>')),
    ):
      self.assertEqual(sql._fetch_maven_versions(), [])

    # Missing JAVA_HOME/bin/java & OSError on execution
    empty_jdk = os.path.join(self.tmp, 'empty_jdk')
    os.makedirs(empty_jdk)
    with mock.patch.dict(os.environ, {'JAVA_HOME': empty_jdk}, clear=True):
      with self.assertRaisesRegex(SqlError, 'does not contain') as c_jh:
        sql._find_java()
      self.assertEqual(c_jh.exception.exit_code, 4)

    with (
        mock.patch.dict(os.environ, {}, clear=True),
        mock.patch('shutil.which', return_value='/usr/bin/java'),
        mock.patch('subprocess.run', side_effect=OSError('exec format error')),
    ):
      with self.assertRaisesRegex(SqlError, 'could not execute') as c_os:
        sql._find_java()
      self.assertEqual(c_os.exception.exit_code, 4)

    # Symlinked wrapper dir rejected on POSIX
    if hasattr(os, 'symlink'):
      real_d = os.path.join(self.tmp, 'real_w')
      os.makedirs(real_d)
      os.makedirs(self.beam_home, exist_ok=True)
      os.symlink(real_d, os.path.join(self.beam_home, 'maven-wrapper'))
      with self.assertRaisesRegex(SqlError, 'refusing symlinked'):
        sql._ensure_maven_wrapper(self.beam_home)
      os.unlink(os.path.join(self.beam_home, 'maven-wrapper'))

    # Maven missing-artifact (exit 2) and generic build failure (exit 1)
    wrapper_jar = os.path.join(
        self.beam_home, 'maven-wrapper', '.mvn', 'wrapper', 'maven-wrapper.jar'
    )
    _write_dummy_jar(wrapper_jar)
    with mock.patch(
        'subprocess.run',
        return_value=_proc(
            1, stderr='[ERROR] Could not find artifact org.apache.beam:beam-sdks-java-io-nope:jar:2.75.0'
        ),
    ), contextlib.redirect_stderr(io.StringIO()):
      with self.assertRaisesRegex(SqlError, 'Maven could not resolve') as c_mv:
        sql._build_shaded_jar(
            wrapper_jar,
            java_home='/opt/jdk',
            version='2.75.0',
            runner_key='direct',
            ios=['nope'],
            jar_path=os.path.join(self.tmp, 'out.jar'),
            debug=False,
            java_bin='/opt/jdk/bin/java',
        )
      self.assertEqual(c_mv.exception.exit_code, 2)


if __name__ == '__main__':
  unittest.main()
