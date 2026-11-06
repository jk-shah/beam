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

"""Launches the Apache Beam SQL shell (`BeamSqlLine`) for `beam sql`.

`scripts/beam-sql.sh` builds a self-contained shaded uber-JAR with Maven
Wrapper, caches it in `~/.beam/cache/`, and starts `BeamSqlLine`. This module
does the same in Python with identical cache paths and filenames, so cached
JARs and `~/.beam/maven-wrapper/` are shared between `beam-sql.sh` and
`beam sql`, while checking for a real Java 11+ runtime (catching macOS's
`/usr/bin/java` stub and older JDKs) without needing `curl`, `sed`, or GNU
`sort -V`.
"""

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape as xml_escape

from apache_beam import version as beam_version

# Exit codes matching `apache_beam.cli.main`.
_EXIT_OK = 0
_EXIT_FAILED = 1
_EXIT_USAGE = 2
_EXIT_MISSING_DEPENDENCY = 4


class SqlError(Exception):
  """An error in `beam sql`, reported to the user with `exit_code`."""
  def __init__(self, message, exit_code):
    super().__init__(message)
    self.exit_code = exit_code


_MAIN_CLASS = 'org.apache.beam.sdk.extensions.sql.jdbc.BeamSqlLine'

# Supported runners and the extra Maven dependency each needs, matching
# `scripts/beam-sql.sh`. DirectRunner is already a transitive dependency of
# `beam-sdks-java-extensions-sql-jdbc`.
_RUNNER_ARTIFACTS = {
    'direct': None,
    'dataflow': 'beam-runners-google-cloud-dataflow-java',
}

_VALID_ARTIFACT_PART = re.compile(r'^[a-z0-9][a-z0-9._-]*$')


def run(
    version=None,
    runner='direct',
    ios=(),
    list_versions=False,
    list_ios=False,
    list_runners=False,
    debug=False,
    sqlline_args=(),
    beam_home=None):
  """Runs `beam sql` and returns the process exit code."""
  if list_versions:
    return _list_versions()
  if list_ios:
    return _list_ios()

  resolved_version = _resolve_version(version)
  if list_runners:
    return _list_runners(resolved_version)

  runner_key = _normalize_runner(runner)
  clean_ios = _normalize_ios(ios)
  java_bin, java_home = _find_java()

  home = beam_home or os.path.expanduser(os.environ.get('BEAM_HOME', '~/.beam'))
  cache_dir = os.path.join(home, 'cache')
  os.makedirs(cache_dir, exist_ok=True)
  jar_path = os.path.join(
      cache_dir, _cache_filename(resolved_version, runner_key, clean_ios))

  if os.path.isfile(jar_path):
    print(f'Using cached Beam SQL JAR: {jar_path}', file=sys.stderr)
  else:
    mvnw = _ensure_maven_wrapper(home)
    _build_shaded_jar(
        mvnw=mvnw,
        java_home=java_home,
        version=resolved_version,
        runner_key=runner_key,
        ios=clean_ios,
        jar_path=jar_path,
        debug=debug)

  cmd = [java_bin, '-cp', jar_path, _MAIN_CLASS, *sqlline_args]
  if debug:
    print('+ ' + ' '.join(cmd), file=sys.stderr)
  proc = subprocess.run(cmd, check=False)
  return proc.returncode


def _resolve_version(version):
  """Returns the Maven release version to use for Beam SQL artifacts."""
  if version:
    if not _VALID_ARTIFACT_PART.match(version.lower()):
      raise SqlError(f'invalid Beam version {version!r}', _EXIT_USAGE)
    return version
  installed = beam_version.__version__
  if '.dev' not in installed and 'rc' not in installed:
    return installed
  # A development checkout (`2.XX.0.dev`) has no published Maven JARs; fall
  # back to the latest release on Maven Central, just as `beam-sql.sh` uses a
  # released default.
  versions = _fetch_maven_versions()
  if not versions:
    raise SqlError(
        f'installed Apache Beam version is {installed!r} (unreleased) and '
        'Maven Central could not be reached; pass --version <release>',
        _EXIT_FAILED)
  latest = versions[0]
  print(
      f'Installed Beam is {installed}; using latest released Beam SQL '
      f'{latest} (override with --version).',
      file=sys.stderr)
  return latest


def _normalize_runner(runner):
  """Maps 'direct' / 'DirectRunner' -> 'direct', 'dataflow' -> 'dataflow'."""
  name = (runner or 'direct').strip().lower()
  if name.endswith('runner') and name != 'runner':
    name = name[:-len('runner')]
  if name not in _RUNNER_ARTIFACTS:
    supported = ', '.join(_RUNNER_ARTIFACTS)
    raise SqlError(
        f'unsupported runner {runner!r} for beam sql; '
        f'supported runners: {supported}',
        _EXIT_USAGE)
  return name


def _normalize_ios(ios):
  """Validates, deduplicates, and sorts `--io` connector names."""
  seen = set()
  for raw in ios:
    name = raw.strip().lower()
    if not name or not _VALID_ARTIFACT_PART.match(name):
      raise SqlError(
          f'invalid --io connector {raw!r}; run `beam sql --list-ios` to see '
          'available connectors',
          _EXIT_USAGE)
    seen.add(name)
  return sorted(seen)


def _cache_filename(version, runner_key, sorted_ios):
  """Exact cache filename used by `scripts/beam-sql.sh`."""
  ios_str = '-'.join(sorted_ios)
  return f'beam-{version}_runner-{runner_key}_ios-{ios_str}.jar'


# Matches `java -version` output across OpenJDK / Oracle / Temurin:
# `openjdk version "17.0.9"` -> ('17', '0'), `java version "1.8.0_392"` -> ('1', '8').
_JAVA_VERSION_RE = re.compile(r'version "(\d+)(?:\.(\d+))?')
_MIN_JAVA_MAJOR = 11
# Beam 2.75's shaded ByteBuddy supports classfile versions up to Java 25.
_WARN_UNSUPPORTED_JAVA_MAJOR = 26


def _find_java():
  """Locates `java`, verifies Java 11+, and returns `(java_bin, java_home)`.

  `shutil.which('java')` alone is not enough: on macOS `/usr/bin/java` is an OS
  stub that exits non-zero when no JDK is installed.
  """
  env_home = os.environ.get('JAVA_HOME', '').strip()
  if env_home:
    bin_dir = os.path.join(env_home, 'bin')
    java_bin = shutil.which(
        'java', path=bin_dir) or os.path.join(bin_dir, 'java')
    if not os.path.isfile(java_bin):
      raise SqlError(
          f'JAVA_HOME={env_home!r} does not contain bin/java; '
          'Beam SQL requires Java 11 or newer',
          _EXIT_MISSING_DEPENDENCY)
  else:
    java_bin = shutil.which('java')
    if not java_bin:
      raise SqlError(
          "'java' command not found; Beam SQL requires Java 11 or newer. "
          'Install OpenJDK 11+ or set JAVA_HOME.',
          _EXIT_MISSING_DEPENDENCY)

  try:
    proc = subprocess.run([java_bin, '-version'],
                          capture_output=True,
                          text=True,
                          check=False)
  except OSError as e:
    raise SqlError(
        f'could not execute {java_bin!r}: {e}\n'
        '  Beam SQL requires Java 11 or newer; install OpenJDK 11+ or set '
        'JAVA_HOME',
        _EXIT_MISSING_DEPENDENCY) from None

  output = f'{proc.stderr}\n{proc.stdout}'
  match = _JAVA_VERSION_RE.search(output)
  if proc.returncode != 0 or not match:
    raise SqlError(
        'no working Java runtime found; Beam SQL requires Java 11 or newer. '
        'Install OpenJDK 11+ or set JAVA_HOME.',
        _EXIT_MISSING_DEPENDENCY)

  first, second = int(match.group(1)), int(match.group(2) or 0)
  major = second if first == 1 else first
  if major < _MIN_JAVA_MAJOR:
    raise SqlError(
        f'Beam SQL requires Java {_MIN_JAVA_MAJOR} or newer '
        f'(found Java {major} at {java_bin}). '
        'Install OpenJDK 11+ or set JAVA_HOME.',
        _EXIT_MISSING_DEPENDENCY)
  if major >= _WARN_UNSUPPORTED_JAVA_MAJOR:
    print(
        f'warning: Java {major} ({java_bin}) may be newer than Beam SQL '
        'supports; if queries fail with a ByteBuddy error, set JAVA_HOME to '
        'Java 11, 17, or 21',
        file=sys.stderr)

  java_home = env_home or os.path.dirname(
      os.path.dirname(os.path.realpath(java_bin)))
  return java_bin, java_home


# Same Maven Wrapper versions and URLs as `scripts/beam-sql.sh`.
_MAVEN_WRAPPER_VERSION = '3.2.0'
_MAVEN_VERSION = '3.9.6'
_MAVEN_SHADE_PLUGIN_VERSION = '3.5.1'
_MAVEN_WRAPPER_SCRIPT_URL = (
    'https://raw.githubusercontent.com/apache/maven-wrapper/refs/tags/'
    f'maven-wrapper-{_MAVEN_WRAPPER_VERSION}/maven-wrapper-distribution/'
    'src/resources/mvnw')
_MAVEN_WRAPPER_JAR_URL = (
    'https://repo.maven.apache.org/maven2/org/apache/maven/wrapper/'
    f'maven-wrapper/{_MAVEN_WRAPPER_VERSION}/'
    f'maven-wrapper-{_MAVEN_WRAPPER_VERSION}.jar')
_MAVEN_DISTRIBUTION_URL = (
    'https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/'
    f'{_MAVEN_VERSION}/apache-maven-{_MAVEN_VERSION}-bin.zip')


def _ensure_maven_wrapper(beam_home):
  """Returns cached `~/.beam/maven-wrapper/mvnw`, downloading it once."""
  wrapper_dir = os.path.join(beam_home, 'maven-wrapper')
  mvnw = os.path.join(wrapper_dir, 'mvnw')
  wrapper_jar = os.path.join(
      wrapper_dir, '.mvn', 'wrapper', 'maven-wrapper.jar')
  wrapper_props = os.path.join(
      wrapper_dir, '.mvn', 'wrapper', 'maven-wrapper.properties')

  if (os.path.isfile(mvnw) and os.path.isfile(wrapper_jar) and
      os.path.isfile(wrapper_props)):
    return mvnw

  print(f'Downloading Maven Wrapper into {wrapper_dir}...', file=sys.stderr)
  os.makedirs(os.path.dirname(wrapper_jar), exist_ok=True)
  with open(wrapper_props, 'w', encoding='utf-8') as f:
    f.write(f'distributionUrl={_MAVEN_DISTRIBUTION_URL}\n')
  _download_https(_MAVEN_WRAPPER_SCRIPT_URL, mvnw)
  _download_https(_MAVEN_WRAPPER_JAR_URL, wrapper_jar)
  os.chmod(mvnw, 0o755)
  return mvnw


def _download_https(url, dest_path):
  """Downloads an `https://` URL atomically to `dest_path`."""
  if not url.startswith('https://'):
    raise ValueError(f'expected https:// URL, got {url!r}')
  tmp_path = dest_path + '.part'
  try:
    with urllib.request.urlopen(url, timeout=30) as resp:
      data = resp.read()
    with open(tmp_path, 'wb') as f:
      f.write(data)
    os.replace(tmp_path, dest_path)
  except (urllib.error.URLError, OSError) as e:
    if os.path.exists(tmp_path):
      os.remove(tmp_path)
    raise SqlError(f'failed to download {url}: {e}', _EXIT_FAILED) from None


def _render_pom(version, runner_key, ios):
  """Generates the `pom.xml` used to shade `BeamSqlLine` and selected IOs."""
  deps = []
  for io_name in ios:
    deps.append(
        '        <dependency>'
        '<groupId>org.apache.beam</groupId>'
        f'<artifactId>beam-sdks-java-io-{xml_escape(io_name)}</artifactId>'
        '<version>${beam.version}</version>'
        '</dependency>')
  runner_artifact = _RUNNER_ARTIFACTS[runner_key]
  if runner_artifact:
    deps.append(
        '        <dependency>'
        '<groupId>org.apache.beam</groupId>'
        f'<artifactId>{xml_escape(runner_artifact)}</artifactId>'
        '<version>${beam.version}</version>'
        '</dependency>')
  extra_deps = ('\n' + '\n'.join(deps)) if deps else ''
  return f"""\
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-sql-shell-runner</artifactId>
    <version>1.0</version>
    <dependencies>
        <dependency>
            <groupId>org.apache.beam</groupId>
            <artifactId>beam-sdks-java-extensions-sql-jdbc</artifactId>
            <version>${{beam.version}}</version>
        </dependency>{extra_deps}
    </dependencies>
    <properties>
        <beam.version>{xml_escape(version)}</beam.version>
    </properties>
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>{_MAVEN_SHADE_PLUGIN_VERSION}</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <transformers>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                            </transformers>
                            <filters>
                                <filter>
                                    <artifact>*:*</artifact>
                                    <excludes>
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
"""


def _build_shaded_jar(
    mvnw, java_home, version, runner_key, ios, jar_path, debug):
  """Builds the Beam SQL shaded uber-JAR and caches it atomically."""
  io_desc = f', ios: {", ".join(ios)}' if ios else ''
  print(
      f'Building Beam SQL v{version} JAR (runner: {runner_key}{io_desc}); '
      'this takes a moment on first run...',
      file=sys.stderr)
  env = os.environ.copy()
  env['JAVA_HOME'] = java_home

  with tempfile.TemporaryDirectory() as work_dir:
    pom_path = os.path.join(work_dir, 'pom.xml')
    with open(pom_path, 'w', encoding='utf-8') as f:
      f.write(_render_pom(version, runner_key, ios))

    cmd = [mvnw, '-f', pom_path]
    if not debug:
      cmd.append('-q')
    cmd.extend(['--batch-mode', 'package'])
    if debug:
      print('+ ' + ' '.join(cmd), file=sys.stderr)
      proc = subprocess.run(cmd, env=env, check=False)
      details = ''
    else:
      proc = subprocess.run(
          cmd, env=env, capture_output=True, text=True, check=False)
      details = (proc.stderr or proc.stdout or '').strip()

    built_jar = os.path.join(
        work_dir, 'target', 'beam-sql-shell-runner-1.0.jar')
    if proc.returncode != 0 or not os.path.isfile(built_jar):
      if 'Could not find artifact org.apache.beam:' in details:
        raise SqlError(
            f'Maven could not resolve a Beam {version} artifact:\n'
            f'  {details.splitlines()[-1]}\n'
            '  check --version (`beam sql --list-versions`) and --io '
            '(`beam sql --list-ios`)',
            _EXIT_USAGE)
      suffix = f':\n{details}' if details else ''
      raise SqlError(
          f'Maven build for Beam SQL JAR failed{suffix}', _EXIT_FAILED)

    # Atomic replace inside the cache directory so Ctrl-C never leaves a
    # truncated JAR that a later run would mistake for a valid cache hit.
    tmp_jar = f'{jar_path}.{os.getpid()}.part'
    try:
      shutil.copyfile(built_jar, tmp_jar)
      os.replace(tmp_jar, jar_path)
    finally:
      if os.path.exists(tmp_jar):
        os.remove(tmp_jar)

  print(f'Cached Beam SQL JAR at {jar_path}', file=sys.stderr)


_METADATA_URL = (
    'https://repo1.maven.org/maven2/org/apache/beam/'
    'beam-sdks-java-core/maven-metadata.xml')

_KNOWN_IOS = (
    'amazon-web-services2 amqp azure azure-cosmos cassandra cdap clickhouse '
    'csv debezium elasticsearch google-ads google-cloud-platform '
    'hadoop-format hbase hcatalog iceberg influxdb jdbc jms json kafka '
    'kinesis kudu mongodb mqtt neo4j parquet pulsar rabbitmq redis '
    'singlestore snowflake solace solr sparkreceiver splunk synthetic thrift '
    'tika xml').split()

_EXCLUDED_IO_RE = re.compile(r'(tests?|expansion-service|parent|upgrade)')
_EXCLUDED_RUNNER_RE = re.compile(
    r'(tests?|parent|core-construction|core-java|extensions|job-server|'
    r'legacy-worker|windmill|examples|experimental|orchestrator|'
    r'java-fn-execution|java-job-service|gcp-gcemd|gcp-gcsproxy|'
    r'local-java-core|portability-java|prism-java|reference-java)')


def _fetch_text(url):
  if not url.startswith('https://'):
    raise ValueError(f'expected https:// URL, got {url!r}')
  try:
    with urllib.request.urlopen(url, timeout=15) as resp:
      return resp.read().decode('utf-8')
  except (urllib.error.URLError, OSError, UnicodeDecodeError):
    return None


def _fetch_maven_versions():
  xml_text = _fetch_text(_METADATA_URL)
  if not xml_text or '<!DOCTYPE' in xml_text or '<!ENTITY' in xml_text:
    return []
  try:
    root = ET.fromstring(xml_text)
  except ET.ParseError:
    return []
  versions = [
      el.text.strip() for el in root.findall('.//versions/version')
      if el.text and 'SNAPSHOT' not in el.text
  ]
  return sorted(
      versions,
      key=lambda v: [(0, int(p)) if p.isdigit() else (1, p)
                     for p in re.split(r'[.-]', v)],
      reverse=True)


def _query_maven_artifacts(prefix, excluded_re):
  """Returns sorted suffix names for `org.apache.beam:<prefix>*` on Maven."""
  url = (
      'https://search.maven.org/solrsearch/select?'
      f'q=g:org.apache.beam+AND+a:{prefix}*&rows=100&wt=json')
  body = _fetch_text(url)
  if not body:
    return []
  try:
    docs = json.loads(body).get('response', {}).get('docs', [])
    return sorted({
        doc['a'][len(prefix):]
        for doc in docs if doc.get('a', '').startswith(prefix) and
        not excluded_re.search(doc['a'][len(prefix):])
    })
  except (ValueError, KeyError, TypeError):
    return []


def _list_versions():
  versions = _fetch_maven_versions()
  if not versions:
    raise SqlError(
        'could not retrieve Beam versions from Maven Central; '
        'check your network connection',
        _EXIT_FAILED)
  print('10 latest Apache Beam releases:')
  for ver in versions[:10]:
    print(f'  {ver}')
  return _EXIT_OK


def _list_ios():
  ios = _query_maven_artifacts('beam-sdks-java-io-', _EXCLUDED_IO_RE)
  if not ios:
    print(
        'warning: could not reach Maven Central; showing known IO connectors:',
        file=sys.stderr)
    print(
        textwrap.fill(
            ', '.join(_KNOWN_IOS),
            width=78,
            initial_indent='  ',
            subsequent_indent='  '))
    return _EXIT_FAILED

  print('Available Apache Beam SQL IO connectors:')
  print(
      textwrap.fill(
          ' '.join(ios), width=78, initial_indent='  ', subsequent_indent='  '))
  return _EXIT_OK


def _list_runners(version):
  # Maven Central's artifact index only matches `v:` against `latestVersion`,
  # so query `beam-runners-*` directly rather than failing whenever `--version`
  # is not the newest release.
  runners = _query_maven_artifacts('beam-runners-', _EXCLUDED_RUNNER_RE)
  if not runners:
    print(
        f'warning: could not fetch runners for Beam {version} from Maven '
        'Central; supported by `beam sql`:',
        file=sys.stderr)
    print('  direct    - DirectRunner (runs locally, default)')
    print('  dataflow  - DataflowRunner (runs on Google Cloud Dataflow)')
    return _EXIT_FAILED

  print(f'Available runners in Maven Central (Beam {version}):')
  for raw in runners:
    if raw == 'direct-java':
      print('  direct    - DirectRunner (supported by beam sql, default)')
    elif raw == 'google-cloud-dataflow-java':
      print('  dataflow  - DataflowRunner (supported by beam sql)')
    else:
      clean = re.sub(r'(-java$|^gcp-|^local-)', '', raw)
      print(f'  {clean:<9} - {raw}')
  print(
      "\nNote: `beam sql --runner` currently supports 'direct' and 'dataflow'.")
  return _EXIT_OK
