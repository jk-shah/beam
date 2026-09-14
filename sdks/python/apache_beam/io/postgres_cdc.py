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

"""Cross-language PostgreSQL CDC and Bulk Write transforms backed by the Go postgresio connector.

This module provides Python-idiomatic PTransforms for continuous Change Data Capture (CDC)
streaming and high-throughput bulk writing to PostgreSQL databases:

  * :class:`ReadFromPostgresCDC`: Streams row mutations using logical replication and pgoutput.
  * :class:`WriteToPostgres`: High-performance bulk sink using staged COPY and UNNEST upserts.

Both transforms leverage the Go SchemaTransform expansion service, automatically discovering
schemas and adapting configuration.
"""

import logging
import os
import shutil
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

from apache_beam.transforms import external
from apache_beam.transforms import ptransform

_LOGGER = logging.getLogger(__name__)

POSTGRES_WRITE_URN = 'beam:schematransform:org.apache.beam:postgres_write:v1'
POSTGRES_CDC_READ_URN = 'beam:schematransform:org.apache.beam:postgres_read_cdc:v1'

# Backward-compatibility alias
POSTGRES_BULK_WRITE_URN = POSTGRES_WRITE_URN


class ExpansionServiceNotFoundError(RuntimeError):
  """Raised when the Go expansion service cannot be located across any resolution tier."""
  pass


def _find_beam_repo_root() -> Optional[str]:
  """Searches directory ancestors for an Apache Beam source tree."""
  curr = os.path.abspath(os.path.dirname(__file__))
  while curr and curr != os.path.dirname(curr):
    if os.path.exists(os.path.join(curr, 'sdks', 'go', 'cmd', 'beam-go-expansion-service')):
      return curr
    if os.path.exists(os.path.join(curr, '.git')):
      candidate = os.path.join(curr, 'sdks', 'go', 'cmd', 'beam-go-expansion-service')
      if os.path.exists(candidate):
        return curr
    curr = os.path.dirname(curr)
  return None


def default_expansion_service(
    expansion_service: Optional[Any] = None,
    allow_build: bool = True) -> Any:
  """Resolves an expansion service for Go SchemaTransforms through a multi-tier fallback chain.

  Tiers evaluated in priority order:
    1. Explicitly provided expansion_service parameter (endpoint string or service object).
    2. BEAM_GO_EXPANSION_SERVICE environment variable (host:port or binary path).
    3. Sibling binary in PATH or current virtual environment (beam-go-expansion-service).
    4. Pre-built binary cached at ~/.apache_beam/cache/bin/beam-go-expansion-service.
    5. Local compilation via 'go build -mod=readonly' if a source checkout and Go compiler exist.

  Raises:
    ExpansionServiceNotFoundError: If all resolution tiers fail.
  """
  # Tier 1: Explicit parameter
  if expansion_service is not None:
    return expansion_service

  # Tier 2: Environment variable
  env_svc = os.environ.get('BEAM_GO_EXPANSION_SERVICE')
  if env_svc:
    if ':' in env_svc and not os.path.exists(env_svc):
      _LOGGER.info('Resolved Go expansion service from BEAM_GO_EXPANSION_SERVICE endpoint: %s', env_svc)
      return env_svc
    if os.path.exists(env_svc):
      _LOGGER.info('Resolved Go expansion service from BEAM_GO_EXPANSION_SERVICE binary: %s', env_svc)
      return external.GoBinaryExpansionService(env_svc)

  # Tier 3: Sibling binary in PATH or sys.prefix
  for name in ('beam-go-expansion-service', 'beam_go_expansion_service'):
    which_path = shutil.which(name)
    if which_path and os.path.exists(which_path):
      _LOGGER.info('Resolved Go expansion service from PATH: %s', which_path)
      return external.GoBinaryExpansionService(which_path)

    prefix_path = os.path.join(sys.prefix, 'bin', name)
    if os.path.exists(prefix_path):
      _LOGGER.info('Resolved Go expansion service from sys.prefix: %s', prefix_path)
      return external.GoBinaryExpansionService(prefix_path)

  # Tier 4: Cached pre-built binary
  cache_dir = os.path.expanduser('~/.apache_beam/cache/bin')
  cached_bin = os.path.join(cache_dir, 'beam-go-expansion-service')
  if os.path.exists(cached_bin) and os.access(cached_bin, os.X_OK):
    _LOGGER.info('Resolved Go expansion service from cache: %s', cached_bin)
    return external.GoBinaryExpansionService(cached_bin)

  # Tier 5: Local source compilation
  if allow_build:
    go_path = shutil.which('go')
    if go_path:
      repo_root = _find_beam_repo_root()
      if repo_root:
        cmd_dir = os.path.join(repo_root, 'sdks', 'go', 'cmd', 'beam-go-expansion-service')
        if os.path.exists(cmd_dir):
          os.makedirs(cache_dir, exist_ok=True)
          target_bin = os.path.join(cache_dir, 'beam-go-expansion-service')
          _LOGGER.info('Compiling Go expansion service from source: %s -> %s', cmd_dir, target_bin)
          t0 = time.time()
          try:
            subprocess.run(
                [go_path, 'build', '-mod=readonly', '-o', target_bin, '.'],
                cwd=cmd_dir,
                check=True,
                capture_output=True,
                text=True,
            )
            _LOGGER.info('Successfully compiled Go expansion service in %.2fs', time.time() - t0)
            return external.GoBinaryExpansionService(target_bin)
          except Exception as ex:
            _LOGGER.warning('Failed to compile Go expansion service from source: %s', ex)

  raise ExpansionServiceNotFoundError(
      "Unable to locate or build the Apache Beam Go expansion service.\n"
      "Attempted the following resolution tiers in order:\n"
      "  1. Explicit expansion_service parameter (none provided)\n"
      "  2. BEAM_GO_EXPANSION_SERVICE environment variable (unset)\n"
      "  3. Sibling binary in PATH or sys.prefix (not found)\n"
      "  4. Pre-built cached binary at ~/.apache_beam/cache/bin/ (not found)\n"
      "  5. Local source compilation via 'go build -mod=readonly' (compiler or checkout missing)\n\n"
      "Remediation:\n"
      "  - Pass expansion_service='host:port' directly to the transform.\n"
      "  - Or set BEAM_GO_EXPANSION_SERVICE=/path/to/beam-go-expansion-service\n"
      "  - Or install Go and build: cd sdks/go && go build -o ~/.apache_beam/cache/bin/beam-go-expansion-service ./cmd/beam-go-expansion-service"
  )


class PostgreSqlWriteResult:
  """Exposes the output collections of WriteToPostgres for Dead Letter Queue routing."""
  def __init__(self, transform_output: Any):
    self._output = transform_output

  @property
  def successful_rows(self) -> Any:
    """PCollection of rows successfully written to the database."""
    if hasattr(self._output, '__getitem__'):
      try:
        return self._output['output']
      except (KeyError, TypeError):
        pass
    return self._output

  @property
  def failed_rows(self) -> Any:
    """PCollection of failed mutation rows routed to the Dead Letter Queue."""
    if hasattr(self._output, '__getitem__'):
      try:
        return self._output['errors']
      except (KeyError, TypeError):
        pass
    return None


class ReadFromPostgresCDC(ptransform.PTransform):
  """Reads a continuous stream of change data capture events from PostgreSQL.

  Example usage::

    pipeline | ReadFromPostgresCDC(
        host='localhost',
        database='mydb',
        slot_name='beam_slot',
        publication='beam_pub',
        username='beam_cdc',
        password_env_var='PGPASSWORD',
    )
  """
  def __init__(
      self,
      host: str,
      database: str,
      slot_name: str,
      publication: str,
      username: str,
      port: int = 5432,
      password: Optional[str] = None,
      password_env_var: Optional[str] = None,
      sslmode: str = 'verify-full',
      tables: Optional[List[str]] = None,
      origin_filter: Optional[str] = None,
      output_format: str = 'row',
      arrow_batch_rows: Optional[int] = None,
      proto_version: Optional[int] = None,
      binary_mode: Optional[bool] = None,
      streaming_mode: Optional[str] = None,
      expansion_service: Optional[Any] = None):
    super().__init__()
    self._config = {
        'host': host,
        'port': port,
        'database': database,
        'slot_name': slot_name,
        'publication': publication,
        'username': username,
        'sslmode': sslmode,
        'output_format': output_format,
    }
    if password is not None:
      self._config['password'] = password
    if password_env_var is not None:
      self._config['password_env_var'] = password_env_var
    if tables is not None:
      self._config['tables'] = tables
    if origin_filter is not None:
      self._config['origin_filter'] = origin_filter
    if arrow_batch_rows is not None:
      self._config['arrow_batch_rows'] = arrow_batch_rows
    if proto_version is not None:
      self._config['proto_version'] = proto_version
    if binary_mode is not None:
      self._config['binary_mode'] = binary_mode
    if streaming_mode is not None:
      self._config['streaming_mode'] = streaming_mode

    self._expansion_service = default_expansion_service(expansion_service)

  def expand(self, pcoll):
    return pcoll | external.SchemaAwareExternalTransform(
        identifier=POSTGRES_CDC_READ_URN,
        expansion_service=self._expansion_service,
        rearrange_based_on_discovery=True,
        **self._config)


# Convenience aliases for CDC reading
ReadFromPostgresCdc = ReadFromPostgresCDC
ReadFromPostgres = ReadFromPostgresCDC


class WriteToPostgres(ptransform.PTransform):
  """Writes rows to PostgreSQL with high throughput using staged COPY and UNNEST upserts.

  Example usage::

    rows | WriteToPostgres(
        host='localhost',
        database='mydb',
        table='public.orders',
        username='beam_writer',
        password_env_var='PGPASSWORD',
        conflict_keys=['order_id'],
    )
  """
  def __init__(
      self,
      host: str,
      database: str,
      table: str,
      username: str,
      port: int = 5432,
      password: Optional[str] = None,
      password_env_var: Optional[str] = None,
      sslmode: str = 'verify-full',
      conflict_keys: Optional[List[str]] = None,
      update_fields: Optional[List[str]] = None,
      max_batch_rows: Optional[int] = None,
      max_batch_bytes: Optional[int] = None,
      use_pgbouncer: bool = False,
      replication_origin: Optional[str] = None,
      expansion_service: Optional[Any] = None):
    super().__init__()
    self._config = {
        'host': host,
        'port': port,
        'database': database,
        'table': table,
        'username': username,
        'sslmode': sslmode,
        'use_pgbouncer': use_pgbouncer,
    }
    if password is not None:
      self._config['password'] = password
    if password_env_var is not None:
      self._config['password_env_var'] = password_env_var
    if conflict_keys is not None:
      self._config['conflict_keys'] = conflict_keys
    if update_fields is not None:
      self._config['update_fields'] = update_fields
    if max_batch_rows is not None:
      self._config['max_batch_rows'] = max_batch_rows
    if max_batch_bytes is not None:
      self._config['max_batch_bytes'] = max_batch_bytes
    if replication_origin is not None:
      self._config['replication_origin'] = replication_origin

    self._expansion_service = default_expansion_service(expansion_service)

  def expand(self, pcoll):
    res = pcoll | external.SchemaAwareExternalTransform(
        identifier=POSTGRES_WRITE_URN,
        expansion_service=self._expansion_service,
        rearrange_based_on_discovery=True,
        **self._config)
    return PostgreSqlWriteResult(res)


# Alias for explicit bulk write naming
WriteToPostgresBulk = WriteToPostgres
