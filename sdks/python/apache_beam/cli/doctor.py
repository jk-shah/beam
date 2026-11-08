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
import sys
from typing import Any

from . import config
from . import output
from . import sql


def run_doctor() -> dict[str, Any]:
  """Runs local environment checks without executing any remote pipeline."""
  beam_ver = output.installed_beam_version()
  py_ver = platform.python_version()

  extras = {
      'yaml': importlib.util.find_spec('yaml') is not None and
      importlib.util.find_spec('jsonschema') is not None,
      'pyarrow': importlib.util.find_spec('pyarrow') is not None,
      'gcp_bigquery': importlib.util.find_spec('google.cloud.bigquery') is not None,
      'gcp_pubsub': importlib.util.find_spec('google.cloud.pubsub_v1') is not None,
      'gcp_storage': importlib.util.find_spec('google.cloud.storage') is not None,
      'fastavro': importlib.util.find_spec('fastavro') is not None,
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
  if importlib.util.find_spec('google.auth') is not None:
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

  checks_ok = bool(extras['yaml'] and extras['pyarrow'])
  return {
      'healthy': checks_ok,
      'beam_version': beam_ver,
      'is_dev_version': '.dev' in beam_ver,
      'python_version': py_ver,
      'python_executable': sys.executable,
      'extras': extras,
      'java': java_info,
      'gcp_adc': adc_info,
      'paths': {
          'beam_home': beam_home,
          'config_file': cfg_path,
          'config_exists': os.path.isfile(cfg_path),
          'expansion_jar_cache': expansion_cache,
          'cached_expansion_jars': cached_jars,
      },
  }
