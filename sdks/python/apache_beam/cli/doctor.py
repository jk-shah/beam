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

"""Environment, extras, Java 11–25, cache, and ADC diagnostics (`beam doctor`)."""

import importlib.util
import os
import platform
import sqlite3
import sys
from typing import Any

from . import config
from . import output
from . import sql


def _has_module(name: str) -> bool:
  try:
    return importlib.util.find_spec(name) is not None
  except (ModuleNotFoundError, ValueError):
    return False


def _package_version(dist_name: str) -> str | None:
  try:
    from importlib import metadata
    return metadata.version(dist_name)
  except Exception:  # pylint: disable=broad-except
    return None


def _detect_binary_resolution() -> tuple[dict[str, Any], list[str]]:
  """Detects `beam` PATH shadowing (e.g. `beam-client` or Erlang `beam.smp`)."""
  warnings: list[str] = []
  path_dirs = [p for p in os.environ.get('PATH', '').split(os.pathsep) if p]
  beam_bins: list[str] = []
  apache_beam_bins: list[str] = []
  beam_smp_bins: list[str] = []
  seen_beam: set[str] = set()
  seen_ab: set[str] = set()
  seen_smp: set[str] = set()

  for d in path_dirs:
    for name, bucket, seen in (
        ('beam', beam_bins, seen_beam),
        ('apache-beam', apache_beam_bins, seen_ab),
        ('beam.smp', beam_smp_bins, seen_smp),
    ):
      cand = os.path.join(d, name)
      if os.path.isfile(cand) and os.access(cand, os.X_OK):
        norm = os.path.abspath(cand)
        if norm not in seen:
          seen.add(norm)
          bucket.append(norm)

  beam_client_ver = _package_version('beam-client')
  py_bin_dir = os.path.dirname(os.path.abspath(sys.executable))
  expected_beam = os.path.join(py_bin_dir, 'beam')

  if beam_client_ver is not None:
    warnings.append(
        f"beam-client {beam_client_ver} (Beam Cloud) is installed in this "
        "environment and also defines a 'beam' console script; invoke "
        "'apache-beam' in scripts/CI for unambiguous CLI resolution.")

  if beam_bins and os.path.isfile(expected_beam):
    if os.path.realpath(beam_bins[0]) != os.path.realpath(expected_beam):
      warnings.append(
          f"PATH resolves 'beam' to {beam_bins[0]!r} before this Python "
          f"environment's {expected_beam!r}; invoke 'apache-beam' or prepend "
          f"{py_bin_dir!r} to PATH.")
    elif len(beam_bins) > 1:
      warnings.append(
          f"Multiple 'beam' executables exist on PATH ({', '.join(beam_bins[:3])}); "
          "prefer 'apache-beam' in multi-tool automation.")

  info = {
      'active_beam': beam_bins[0] if beam_bins else None,
      'active_apache_beam': apache_beam_bins[0] if apache_beam_bins else None,
      'beam_on_path': beam_bins,
      'beam_client_installed': beam_client_ver is not None,
      'erlang_beam_smp_on_path': bool(beam_smp_bins),
  }
  return info, warnings


def run_doctor() -> dict[str, Any]:
  """Runs local environment checks without executing any remote pipeline."""
  beam_ver = output.installed_beam_version()
  py_ver = platform.python_version()

  extras = {
      'yaml': _has_module('yaml') and _has_module('jsonschema'),
      'pyarrow': _has_module('pyarrow'),
      'gcp_bigquery': _has_module('google.cloud.bigquery'),
      'gcp_bigquery_storage': _has_module('google.cloud.bigquery_storage_v1'),
      'gcp_pubsub': _has_module('google.cloud.pubsub_v1'),
      'gcp_storage': _has_module('google.cloud.storage'),
      'fastavro': _has_module('fastavro'),
      'duckdb': _has_module('duckdb'),
      'deltalake': _has_module('deltalake'),
      'pyiceberg': _has_module('pyiceberg'),
  }

  # Local `beam pipe --sql` engines (`--sql-engine`). Versions come from
  # package metadata so doctor stays fast and never imports DuckDB.
  from . import pipe_sql
  duck_ver = pipe_sql.duckdb_version() if extras['duckdb'] else None
  sql_engines = {
      'default': 'sqlite',
      'sqlite': {
          'available': True, 'version': sqlite3.sqlite_version
      },
      'duckdb': {
          'available': duck_ver is not None,
          'version': duck_ver,
          **({} if duck_ver else {
              'install': pipe_sql.DUCKDB_INSTALL_HINT
          }),
      },
  }

  # Java 11–25 check (reusing sql._find_java!).
  java_info: dict[str, Any]
  try:
    java_bin, java_home = sql._find_java()
    java_info = {'ok': True, 'java_bin': java_bin, 'java_home': java_home}
  except sql.SqlError as e:
    java_info = {'ok': False, 'message': str(e)}

  # Google Application Default Credentials check (never exposes tokens!).
  adc_info: dict[str, Any]
  if _has_module('google.auth'):
    try:
      import google.auth
      creds, default_proj = google.auth.default()
      adc_info = {
          'ok': creds is not None,
          'default_project': default_proj,
          'credential_type': type(creds).__name__ if creds else None,
      }
    except Exception as e:  # pylint: disable=broad-except
      adc_info = {'ok': False, 'message': output.scrub_error_message(str(e))}
  else:
    adc_info = {'ok': False, 'message': 'google-auth package not installed'}

  beam_home = os.path.expanduser(os.environ.get('BEAM_HOME', '~/.beam'))
  cfg_path = config._default_config_path(None)
  expansion_cache = os.path.expanduser('~/.apache_beam/cache/jars')
  try:
    cached_jars = (
        len(os.listdir(expansion_cache))
        if os.path.isdir(expansion_cache) else 0)
  except OSError:
    cached_jars = 0

  binary_info, warnings = _detect_binary_resolution()
  checks_ok = bool(extras['yaml'] and extras['pyarrow'])
  return {
      'healthy': checks_ok,
      'beam_version': beam_ver,
      'is_dev_version': '.dev' in beam_ver,
      'python_version': py_ver,
      'python_executable': sys.executable,
      'extras': extras,
      'sql_engines': sql_engines,
      'java': java_info,
      'gcp_adc': adc_info,
      'binary_resolution': binary_info,
      'warnings': warnings,
      'paths': {
          'beam_home': beam_home,
          'config_file': cfg_path,
          'config_exists': os.path.isfile(cfg_path),
          'expansion_jar_cache': expansion_cache,
          'cached_expansion_jars': cached_jars,
      },
  }
