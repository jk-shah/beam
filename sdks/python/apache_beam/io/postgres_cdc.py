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
from typing import Any
from typing import Optional

from apache_beam.transforms import external
from apache_beam.transforms import ptransform

if not hasattr(external, 'GoBinaryExpansionService'):

  class _GoBinaryExpansionService(object):
    def __init__(self, path_to_binary, extra_args=None, append_args=None):
      self.path_to_binary = path_to_binary
      self._extra_args = extra_args
      self._append_args = append_args or []

  external.GoBinaryExpansionService = _GoBinaryExpansionService

_LOGGER = logging.getLogger(__name__)

POSTGRES_WRITE_URN = 'beam:schematransform:org.apache.beam:postgres_write:v1'
POSTGRES_CDC_READ_URN = 'beam:schematransform:org.apache.beam:postgres_read_cdc:v1'
POSTGRES_READ_URN = 'beam:schematransform:org.apache.beam:postgres_read:v1'

# Backward-compatibility alias
POSTGRES_BULK_WRITE_URN = POSTGRES_WRITE_URN

# Relative path identifying an Apache Beam source checkout.
_GO_EXPANSION_SERVICE_SOURCE = os.path.join(
    'sdks', 'go', 'cmd', 'beam-go-expansion-service')

# Opts tier 5 in. Compiling a binary is a side effect well outside what applying
# a PTransform is expected to do, so it happens only when asked for.
_ALLOW_BUILD_ENV_VAR = 'BEAM_GO_EXPANSION_SERVICE_ALLOW_BUILD'

_TRUTHY = frozenset(('1', 'true', 'yes', 'on'))


def _build_opted_in() -> bool:
  """Reports whether local compilation has been enabled in the environment."""
  return os.environ.get(_ALLOW_BUILD_ENV_VAR, '').strip().lower() in _TRUTHY


class ExpansionServiceNotFoundError(RuntimeError):
  """Raised when the Go expansion service cannot be located across any resolution tier."""
  pass


def _find_beam_repo_root() -> Optional[str]:
  """Searches directory ancestors for an Apache Beam source tree."""
  curr = os.path.abspath(os.path.dirname(__file__))
  while curr and curr != os.path.dirname(curr):
    if os.path.exists(os.path.join(curr, _GO_EXPANSION_SERVICE_SOURCE)):
      return curr
    curr = os.path.dirname(curr)
  return None


def default_expansion_service(
    expansion_service: Optional[Any] = None,
    allow_build: Optional[bool] = None) -> Any:
  """Resolves an expansion service for Go SchemaTransforms through a multi-tier fallback chain.

  Tiers evaluated in priority order:
    1. Explicitly provided expansion_service parameter (endpoint string or service object).
    2. BEAM_GO_EXPANSION_SERVICE environment variable (host:port or binary path).
    3. Sibling binary in PATH or current virtual environment (beam-go-expansion-service).
    4. Pre-built binary cached at ~/.apache_beam/cache/bin/beam-go-expansion-service.
    5. Local compilation via 'go build -mod=readonly'. Opt-in only; see allow_build.

  Args:
    expansion_service: An endpoint string or expansion service object to use directly.
    allow_build: Whether tier 5 may compile the service from a source checkout.
      Defaults to the BEAM_GO_EXPANSION_SERVICE_ALLOW_BUILD environment variable,
      and to False when that is unset. Tier 5 is not automatic because invoking a
      compiler is a side effect well outside the expected cost of applying a
      PTransform: it needs a Go toolchain and a source checkout, it can take tens
      of seconds, and it happens during pipeline graph construction rather than at
      execution. Tiers 1-4 cover the supported ways to supply the service.

  Raises:
    ExpansionServiceNotFoundError: If all enabled resolution tiers fail.
  """
  if allow_build is None:
    allow_build = _build_opted_in()

  # Tier 1: Explicit parameter
  if expansion_service is not None:
    return expansion_service

  # Tier 2: Environment variable
  env_svc = os.environ.get('BEAM_GO_EXPANSION_SERVICE')
  if env_svc:
    if ':' in env_svc and not os.path.exists(env_svc):
      _LOGGER.info(
          'Resolved Go expansion service from BEAM_GO_EXPANSION_SERVICE endpoint: %s',
          env_svc)
      return env_svc
    if os.path.exists(env_svc):
      _LOGGER.info(
          'Resolved Go expansion service from BEAM_GO_EXPANSION_SERVICE binary: %s',
          env_svc)
      return external.GoBinaryExpansionService(env_svc)

  # Tier 3: Sibling binary in PATH or sys.prefix
  for name in ('beam-go-expansion-service', 'beam_go_expansion_service'):
    which_path = shutil.which(name)
    if which_path and os.path.exists(which_path):
      _LOGGER.info('Resolved Go expansion service from PATH: %s', which_path)
      return external.GoBinaryExpansionService(which_path)

    prefix_path = os.path.join(sys.prefix, 'bin', name)
    if os.path.exists(prefix_path):
      _LOGGER.info(
          'Resolved Go expansion service from sys.prefix: %s', prefix_path)
      return external.GoBinaryExpansionService(prefix_path)

  # Tier 4: Cached pre-built binary
  cache_dir = os.path.expanduser('~/.apache_beam/cache/bin')
  cached_bin = os.path.join(cache_dir, 'beam-go-expansion-service')
  if os.path.exists(cached_bin) and os.access(cached_bin, os.X_OK):
    _LOGGER.info('Resolved Go expansion service from cache: %s', cached_bin)
    return external.GoBinaryExpansionService(cached_bin)

  # Tier 5: Local source compilation, opt-in only.
  if allow_build:
    go_path = shutil.which('go')
    if go_path:
      repo_root = _find_beam_repo_root()
      if repo_root:
        cmd_dir = os.path.join(
            repo_root, 'sdks', 'go', 'cmd', 'beam-go-expansion-service')
        if os.path.exists(cmd_dir):
          os.makedirs(cache_dir, exist_ok=True)
          target_bin = os.path.join(cache_dir, 'beam-go-expansion-service')
          _LOGGER.info(
              'Compiling Go expansion service from source: %s -> %s',
              cmd_dir,
              target_bin)
          t0 = time.time()
          try:
            subprocess.run(
                [go_path, 'build', '-mod=readonly', '-o', target_bin, '.'],
                cwd=cmd_dir,
                check=True,
                capture_output=True,
                text=True,
            )
            _LOGGER.info(
                'Successfully compiled Go expansion service in %.2fs',
                time.time() - t0)
            return external.GoBinaryExpansionService(target_bin)
          except Exception as ex:
            _LOGGER.warning(
                'Failed to compile Go expansion service from source: %s', ex)

  if allow_build:
    tier5 = (
        "  5. Local source compilation via 'go build -mod=readonly' "
        "(compiler or checkout missing, or the build failed)\n")
    tier5_remedy = ""
  else:
    tier5 = (
        "  5. Local source compilation (not attempted; opt-in, currently "
        "disabled)\n")
    tier5_remedy = (
        f"  - Or opt in to building from a source checkout by setting "
        f"{_ALLOW_BUILD_ENV_VAR}=1, or by passing allow_build=True.\n")

  raise ExpansionServiceNotFoundError(
      "Unable to locate or build the Apache Beam Go expansion service.\n"
      "Attempted the following resolution tiers in order:\n"
      "  1. Explicit expansion_service parameter (none provided)\n"
      "  2. BEAM_GO_EXPANSION_SERVICE environment variable (unset)\n"
      "  3. Sibling binary in PATH or sys.prefix (not found)\n"
      "  4. Pre-built cached binary at ~/.apache_beam/cache/bin/ (not found)\n"
      f"{tier5}"
      "\nRemediation:\n"
      "  - Pass expansion_service='host:port' directly to the transform.\n"
      "  - Or set BEAM_GO_EXPANSION_SERVICE=/path/to/beam-go-expansion-service\n"
      "  - Or install Go and build: cd sdks/go && go build -o ~/.apache_beam/cache/bin/beam-go-expansion-service ./cmd/beam-go-expansion-service\n"
      f"{tier5_remedy}")


class PostgreSqlWriteResult(dict):
  """Exposes the output collections of WriteToPostgres for Dead Letter Queue routing.

  Inherits from dict to seamlessly integrate with Beam YAML multi-output routing,
  while providing convenient named properties for Python pipeline authors.
  """
  def __init__(self, transform_output: Any):
    self._output = transform_output
    if isinstance(transform_output, dict):
      super().__init__(transform_output)
    elif hasattr(transform_output, '__getitem__') and hasattr(transform_output,
                                                              'keys'):
      super().__init__(
          {k: transform_output[k]
           for k in transform_output.keys()})
    elif hasattr(transform_output, '__getitem__'):
      d = {}
      try:
        d['out'] = transform_output['output']
      except (KeyError, TypeError):
        pass
      try:
        d['errors'] = transform_output['errors']
      except (KeyError, TypeError):
        pass
      super().__init__(d if d else {'out': transform_output})
    else:
      super().__init__({'out': transform_output})

  @property
  def successful_rows(self) -> Any:
    """PCollection of rows successfully written to the database."""
    if hasattr(self._output, '__getitem__'):
      try:
        return self._output['output']
      except (KeyError, TypeError):
        pass
    return self.get('out', self._output)

  @property
  def failed_rows(self) -> Any:
    """PCollection of failed mutation rows routed to the Dead Letter Queue."""
    if hasattr(self._output, '__getitem__'):
      try:
        return self._output['errors']
      except (KeyError, TypeError):
        pass
    return self.get('errors', None)


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
      tables: Optional[list[str]] = None,
      origin_filter: Optional[str] = None,
      output_format: str = 'row',
      arrow_batch_rows: Optional[int] = None,
      proto_version: Optional[int] = None,
      binary_mode: Optional[bool] = None,
      streaming_mode: Optional[str] = None,
      failover_slot: Optional[bool] = None,
      publication_tables: Optional[list[dict[str, Any]]] = None,
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
    if failover_slot is not None:
      self._config['failover_slot'] = failover_slot
    if publication_tables is not None:
      self._config['publication_tables'] = publication_tables

    self._expansion_service = default_expansion_service(expansion_service)

  def expand(self, pcoll):
    return pcoll | external.SchemaAwareExternalTransform(
        identifier=POSTGRES_CDC_READ_URN,
        expansion_service=self._expansion_service,
        rearrange_based_on_discovery=True,
        **self._config)


# Convenience aliases for CDC reading
ReadFromPostgresCdc = ReadFromPostgresCDC


class ReadFromPostgres(ptransform.PTransform):
  """Reads rows from PostgreSQL using partitioned or bounded table/query execution.

  Example usage::

    pipeline | ReadFromPostgres(
        host='localhost',
        database='mydb',
        table='public.orders',
        username='beam_reader',
        password_env_var='PGPASSWORD',
        partition_column='order_id',
        num_partitions=4,
    )
  """
  def __init__(
      self,
      host: Optional[str] = None,
      database: Optional[str] = None,
      table: Optional[str] = None,
      username: Optional[str] = None,
      port: int = 5432,
      password: Optional[str] = None,
      password_env_var: Optional[str] = None,
      sslmode: str = 'require',
      query: Optional[str] = None,
      location: Optional[str] = None,
      read_query: Optional[str] = None,
      jdbc_url: Optional[str] = None,
      fetch_size: int = 5000,
      partition_column: Optional[str] = None,
      num_partitions: Optional[int] = None,
      lower_bound: Optional[int] = None,
      upper_bound: Optional[int] = None,
      expansion_service: Optional[Any] = None):
    super().__init__()
    self._config = {
        'port': port,
        'sslmode': sslmode,
        'fetch_size': fetch_size,
    }
    if host is not None:
      self._config['host'] = host
    if database is not None:
      self._config['database'] = database
    if table is not None:
      self._config['table'] = table
    if username is not None:
      self._config['username'] = username
    if password is not None:
      self._config['password'] = password
    if password_env_var is not None:
      self._config['password_env_var'] = password_env_var
    if query is not None:
      self._config['query'] = query
    if location is not None:
      self._config['location'] = location
    if read_query is not None:
      self._config['read_query'] = read_query
    if jdbc_url is not None:
      self._config['jdbc_url'] = jdbc_url
    if partition_column is not None:
      self._config['partition_column'] = partition_column
    if num_partitions is not None:
      self._config['num_partitions'] = num_partitions
    if lower_bound is not None:
      self._config['lower_bound'] = lower_bound
    if upper_bound is not None:
      self._config['upper_bound'] = upper_bound

    self._expansion_service = default_expansion_service(expansion_service)

  def expand(self, pcoll):
    return pcoll | external.SchemaAwareExternalTransform(
        identifier=POSTGRES_READ_URN,
        expansion_service=self._expansion_service,
        rearrange_based_on_discovery=True,
        **self._config)


# Convenience alias for batch read
ReadFromPostgresBatch = ReadFromPostgres


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
      conflict_keys: Optional[list[str]] = None,
      update_fields: Optional[list[str]] = None,
      max_batch_rows: Optional[int] = None,
      max_batch_bytes: Optional[int] = None,
      use_pgbouncer: bool = False,
      replication_origin: Optional[str] = None,
      write_mode: Optional[str] = None,
      op_column: Optional[str] = None,
      delete_op_value: Optional[str] = None,
      explain_analyze: Optional[bool] = None,
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
    if write_mode is not None:
      self._config['write_mode'] = write_mode
    if op_column is not None:
      self._config['op_column'] = op_column
    if delete_op_value is not None:
      self._config['delete_op_value'] = delete_op_value
    if explain_analyze is not None:
      self._config['explain_analyze'] = explain_analyze

    self._expansion_service = default_expansion_service(expansion_service)

  def expand(self, pcoll):
    res = pcoll | external.SchemaAwareExternalTransform(
        identifier=POSTGRES_WRITE_URN,
        expansion_service=self._expansion_service,
        rearrange_based_on_discovery=True,
        **self._config)
    return PostgreSqlWriteResult(res)


# Aliases for explicit bulk write and SchemaTransform parity
WriteToPostgresBulk = WriteToPostgres
WriteToPostgresIO = WriteToPostgres
ReadFromPostgresIO = ReadFromPostgres
