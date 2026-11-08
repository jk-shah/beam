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

"""Universal URI parser, secret-rejection guard, and IO config mapper.

Supported schemes across `beam pipe`, `beam inspect`, `beam diff`, `beam tail`:
- Local paths & globs (`users.csv`, `./data/*.parquet`)
- Cloud/distributed filesystems (`gs://`, `s3://`, `azfs://`, `hdfs://`, `file://`)
- `bigquery://PROJECT.DATASET.TABLE` (or `PROJECT:DATASET.TABLE` / `DATASET.TABLE`)
- `postgres://HOST[:PORT]/DB?table=SCHEMA.TABLE` (credentials via env / opt-file)
- `pubsub://projects/PROJECT/topics/TOPIC` or `.../subscriptions/SUB`
- `kafka://HOST:PORT/TOPIC`
- `iceberg://CATALOG.NAMESPACE.TABLE`

Security rule: inline credentials in URIs (`user:pass@host`, `?password=...`) or
CLI `--from-opt` / `--to-opt` flags are strictly rejected with `UriError`
(`EXIT_USAGE = 2`) so secrets never appear in `ps aux`, shell history, or agent
transcripts.
"""

import json
import os
import re
import urllib.parse
from typing import Any
from typing import Iterable
from typing import NamedTuple


class UriError(ValueError):
  """Raised when a URI, format, or `--*-opt` flag is invalid or unsafe."""


class ParsedEndpoint(NamedTuple):
  kind: str  # 'file', 'bigquery', 'postgres', 'pubsub', 'kafka', 'iceberg'
  scheme: str
  raw: str
  format: str | None
  yaml_type: str
  config: dict[str, Any]
  output_prefix: str | None = None
  is_java_xlang: bool = False
  is_unbounded: bool = False


FILE_SCHEMES = frozenset(['gs', 's3', 'azfs', 'hdfs', 'file'])
NON_FILE_SCHEMES = frozenset(
    ['bigquery', 'postgres', 'postgresql', 'pubsub', 'kafka', 'iceberg'])
ALL_SCHEMES = FILE_SCHEMES | NON_FILE_SCHEMES

FILE_FORMATS = {
    'csv': 'Csv',
    'json': 'Json',
    'jsonl': 'Json',
    'parquet': 'Parquet',
    'avro': 'Avro',
    'text': 'Text',
}
EXTENSIONS = {
    '.csv': 'csv',
    '.json': 'json',
    '.jsonl': 'json',
    '.parquet': 'parquet',
    '.avro': 'avro',
    '.txt': 'text',
}

_SHARD_OR_GLOB_SUFFIX = re.compile(r'(-\d+-of-\d+|[-*?])+$')
_SECRET_KEY_RE = re.compile(
    r'(password|passwd|secret|token|private_key|client_secret|'
    r'access_key|api_key|jaas|credential|connection_string)',
    re.IGNORECASE)
_BQ_TABLE_ID_RE = re.compile(
    r'^(?:(?P<project>[A-Za-z0-9_.:-]+)[:.])?'
    r'(?P<dataset>[A-Za-z0-9_]+)\.(?P<table>[A-Za-z0-9_$-]+)$')


def extension_of(path: str) -> str:
  name = _SHARD_OR_GLOB_SUFFIX.sub('', os.path.basename(path.replace('\\', '/')))
  return os.path.splitext(name)[1].lower()


def sniff_file_format(path: str) -> str | None:
  """Sniffs `PAR1` / `Obj\\x01` / JSON magic bytes on extensionless shards."""
  try:
    if '://' not in path:
      import glob as _glob
      candidates = sorted(
          p for p in _glob.glob(path) if os.path.isfile(p) and
          os.path.getsize(p) >= 4)
      if not candidates and not any(c in path for c in '*?['):
        candidates = [
            p for p in sorted(_glob.glob(_glob.escape(path) + '*'))
            if os.path.isfile(p) and os.path.getsize(p) >= 4
        ]
      if not candidates:
        return None
      with open(candidates[0], 'rb') as f:
        head = f.read(64)
        # Parquet also ends with PAR1, and starts with PAR1.
    else:
      from apache_beam.io.filesystems import FileSystems
      pattern = path if any(c in path for c in '*?[') else path + '*'
      matches = [
          m for m in FileSystems.match([pattern])[0].metadata_list
          if m.size_in_bytes >= 4
      ]
      if not matches:
        return None
      with FileSystems.open(matches[0].path) as f:
        head = f.read(64)
  except Exception:  # pylint: disable=broad-except
    return None

  if head.startswith(b'PAR1'):
    return 'parquet'
  if head.startswith(b'Obj\x01'):
    return 'avro'
  stripped = head.lstrip()
  if stripped.startswith(b'{'):
    return 'json'
  return None


def reject_inline_credentials(uri: str) -> None:
  """Rejects inline `user:pass@` userinfo or secret query parameters in URIs."""
  if '://' not in uri:
    return
  parsed = urllib.parse.urlsplit(uri)
  if parsed.username is not None or parsed.password is not None or '@' in (
      parsed.netloc or ''):
    raise UriError(
        f'inline credentials in URI {parsed.scheme}://... are not allowed for '
        'security (process argv and logs are visible); pass credentials via '
        'environment variables (e.g. BEAM_POSTGRES_USER / '
        'BEAM_POSTGRES_PASSWORD), Application Default Credentials, or '
        '--from-opt-file / --to-opt-file')
  for key, _ in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True):
    if _SECRET_KEY_RE.search(key):
      raise UriError(
          f'secret query parameter {key!r} in URI is not allowed; use '
          'environment variables or --from-opt-file / --to-opt-file')


def parse_kv_options(
    opt_items: Iterable[str] | None,
    opt_file: str | None = None,
    flag_name: str = '--from-opt',
) -> dict[str, Any]:
  """Parses repeatable `KEY=VALUE` flags + optional YAML/JSON `--*-opt-file`."""
  merged: dict[str, Any] = {}
  if opt_file:
    if not os.path.isfile(opt_file):
      raise UriError(f'options file not found: {opt_file!r}')
    import yaml
    loader = getattr(yaml, 'CSafeLoader', yaml.SafeLoader)
    try:
      with open(opt_file, encoding='utf-8') as f:
        loaded = yaml.load(f, Loader=loader) or {}
    except (OSError, yaml.YAMLError) as e:
      raise UriError(f'could not parse {opt_file!r}: {e}') from None
    if not isinstance(loaded, dict):
      raise UriError(f'expected key/value mapping in {opt_file!r}')
    merged.update(loaded)

  for raw in opt_items or ():
    if '=' not in raw:
      raise UriError(
          f'expected KEY=VALUE for {flag_name}, got {raw!r}')
    key, val = raw.split('=', 1)
    key = key.strip()
    if not key:
      raise UriError(f'empty option key in {flag_name} {raw!r}')
    if _SECRET_KEY_RE.search(key):
      raise UriError(
          f'refusing secret option {key!r} on the command line ({flag_name}); '
          f'place secrets in {flag_name}-file (chmod 0600) or environment '
          'variables')
    merged[key] = _coerce_opt_value(val)
  return merged


def _coerce_opt_value(raw: str) -> Any:
  low = raw.strip().lower()
  if low == 'true':
    return True
  if low == 'false':
    return False
  if raw.isdigit():
    return int(raw)
  if raw.startswith(('[', '{')):
    try:
      return json.loads(raw)
    except ValueError:
      pass
  return raw


def normalize_bq_table_spec(
    raw_uri_or_spec: str, default_project: str | None = None
) -> tuple[str, str | None, str, str]:
  """Normalizes `bigquery://P.D.T` or `P:D.T` -> `('P:D.T', P, D, T)`."""
  body = raw_uri_or_spec
  if body.lower().startswith('bigquery://'):
    body = body[len('bigquery://'):]
  body = body.strip('/')
  m = _BQ_TABLE_ID_RE.match(body)
  if not m:
    raise UriError(
        f'invalid BigQuery table URI {raw_uri_or_spec!r}; expected '
        'bigquery://PROJECT.DATASET.TABLE or bigquery://PROJECT:DATASET.TABLE')
  proj = m.group('project') or default_project
  dataset = m.group('dataset')
  table = m.group('table')
  spec = f'{proj}:{dataset}.{table}' if proj else f'{dataset}.{table}'
  return spec, proj, dataset, table


def parse_pubsub_uri(uri: str) -> tuple[str, str]:
  """Returns `('topic'|'subscription', 'projects/<p>/topics/<t>')`."""
  reject_inline_credentials(uri)
  body = uri[len('pubsub://'):].strip('/')
  parts = body.split('/')
  if (len(parts) == 4 and parts[0] == 'projects' and
      parts[2] in ('topics', 'subscriptions') and parts[1] and parts[3]):
    kind = 'topic' if parts[2] == 'topics' else 'subscription'
    return kind, body
  raise UriError(
      f'invalid Pub/Sub URI {uri!r}; expected '
      'pubsub://projects/<project>/topics/<topic> or '
      'pubsub://projects/<project>/subscriptions/<subscription>')


def parse_endpoint(
    location: str,
    *,
    is_read: bool,
    explicit_format: str | None = None,
    options: dict[str, Any] | None = None,
    if_exists: str = 'fail',
    default_project: str | None = None,
    allow_sniff: bool = False,
) -> ParsedEndpoint:
  """Parses a `--from` or `--to` location into a `ParsedEndpoint`."""
  if not location or not location.strip():
    raise UriError('endpoint path/URI cannot be empty')
  reject_inline_credentials(location)
  opts = dict(options or {})
  verb = 'ReadFrom' if is_read else 'WriteTo'
  flag_hint = '--from-format' if is_read else '--to-format'

  scheme = location.split('://', 1)[0].lower() if '://' in location else ''
  if scheme and scheme not in ALL_SCHEMES:
    supported = ', '.join(sorted(s + '://' for s in ALL_SCHEMES))
    raise UriError(
        f'unsupported location {location!r}: use a local path or one of '
        f'{supported}')

  # 1. Local or distributed file (`gs://`, `s3://`, `azfs://`, `hdfs://`, `file://`)
  if not scheme or scheme in FILE_SCHEMES:
    fmt = (explicit_format or EXTENSIONS.get(extension_of(location), '')).lower()
    if not fmt and is_read and allow_sniff:
      fmt = sniff_file_format(location) or ''
    if not fmt:
      raise UriError(
          f'cannot tell the format of {location!r} (no known file extension)\n'
          f'  add {flag_hint}, for example: {flag_hint} json')
    if fmt not in FILE_FORMATS:
      valid = ', '.join(k for k in ('csv', 'json', 'parquet', 'avro'))
      raise UriError(
          f'unsupported format {fmt!r} for {location!r}; use one of: {valid}')
    norm_fmt = 'json' if fmt == 'jsonl' else fmt
    cfg = {'path': location, **opts}
    return ParsedEndpoint(
        kind='file',
        scheme=scheme or 'local',
        raw=location,
        format=norm_fmt,
        yaml_type=verb + FILE_FORMATS[norm_fmt],
        config=cfg,
        output_prefix=None if is_read else location,
        is_java_xlang=False,
        is_unbounded=False)

  # 2. BigQuery (`bigquery://PROJECT.DATASET.TABLE`)
  if scheme == 'bigquery':
    if is_read and 'query' in opts:
      cfg = dict(opts)
    else:
      table_spec, _, _, _ = normalize_bq_table_spec(
          location, default_project=default_project)
      cfg = {'table': table_spec, **opts}
    if not is_read:
      disp_map = {
          'fail': 'WRITE_EMPTY',
          'append': 'WRITE_APPEND',
          'replace': 'WRITE_TRUNCATE',
      }
      cfg.setdefault('create_disposition', 'CREATE_IF_NEEDED')
      cfg.setdefault('write_disposition', disp_map.get(if_exists, 'WRITE_EMPTY'))
    return ParsedEndpoint(
        kind='bigquery',
        scheme='bigquery',
        raw=location,
        format='bigquery',
        yaml_type=verb + 'BigQuery',
        config=cfg,
        output_prefix=None,
        is_java_xlang=not is_read,
        is_unbounded=False)

  # 3. Postgres (`postgres://host:port/db?table=public.users`)
  if scheme in ('postgres', 'postgresql'):
    parsed = urllib.parse.urlsplit(location)
    if not parsed.hostname or not parsed.path.strip('/'):
      raise UriError(
          f'invalid Postgres URI {location!r}; expected '
          'postgres://HOST[:PORT]/DATABASE?table=TABLE')
    qparams = dict(
        urllib.parse.parse_qsl(parsed.query, keep_blank_values=False))
    table_name = opts.pop('table', None) or qparams.pop('table', None)
    query_sql = opts.pop('query', None)
    if not table_name and not (is_read and query_sql):
      raise UriError(
          f'Postgres URI {location!r} requires ?table=TABLE (or --from-opt '
          'query="SELECT ...")')
    port_str = f':{parsed.port}' if parsed.port else ':5432'
    db_name = parsed.path.lstrip('/')
    jdbc_query = urllib.parse.urlencode(qparams)
    jdbc_url = f'jdbc:postgresql://{parsed.hostname}{port_str}/{db_name}'
    if jdbc_query:
      jdbc_url = f'{jdbc_url}?{jdbc_query}'

    cfg: dict[str, Any] = {'url': jdbc_url}
    if table_name:
      cfg['table'] = table_name
    if query_sql:
      cfg['query'] = query_sql
    user = opts.pop('username', None) or os.environ.get('BEAM_POSTGRES_USER')
    pwd = opts.pop('password', None) or os.environ.get('BEAM_POSTGRES_PASSWORD')
    if user:
      cfg['username'] = user
    if pwd:
      cfg['password'] = pwd
    cfg.update(opts)
    return ParsedEndpoint(
        kind='postgres',
        scheme='postgres',
        raw=location,
        format='jdbc',
        yaml_type=verb + 'Postgres',
        config=cfg,
        output_prefix=None,
        is_java_xlang=True,
        is_unbounded=False)

  # 4. Pub/Sub (`pubsub://projects/P/topics/T` or `.../subscriptions/S`)
  if scheme == 'pubsub':
    ps_kind, ps_resource = parse_pubsub_uri(location)
    if not is_read and ps_kind != 'topic':
      raise UriError(
          f'WriteToPubSub requires a topic URI, got subscription {location!r}')
    fmt = (explicit_format or opts.pop('format', 'json')).upper()
    cfg = {ps_kind: ps_resource, 'format': fmt, **opts}
    return ParsedEndpoint(
        kind='pubsub',
        scheme='pubsub',
        raw=location,
        format=fmt.lower(),
        yaml_type=verb + 'PubSub',
        config=cfg,
        output_prefix=None,
        is_java_xlang=False,
        is_unbounded=is_read)

  # 5. Kafka (`kafka://host:port/topic`)
  if scheme == 'kafka':
    parsed = urllib.parse.urlsplit(location)
    topic = parsed.path.strip('/')
    if not parsed.netloc or not topic:
      raise UriError(
          f'invalid Kafka URI {location!r}; expected '
          'kafka://BOOTSTRAP_SERVERS/TOPIC')
    fmt = (explicit_format or opts.pop('format', 'JSON')).upper()
    cfg = {
        'bootstrap_servers': parsed.netloc,
        'topic': topic,
        'format': fmt,
        **opts,
    }
    return ParsedEndpoint(
        kind='kafka',
        scheme='kafka',
        raw=location,
        format=fmt.lower(),
        yaml_type=verb + 'Kafka',
        config=cfg,
        output_prefix=None,
        is_java_xlang=True,
        is_unbounded=is_read)

  # 6. Iceberg (`iceberg://catalog.namespace.table` or `--*-opt catalog_name=...`)
  parsed = urllib.parse.urlsplit(location)
  raw_ident = (parsed.netloc + parsed.path).strip('/')
  cfg = {'table': raw_ident, **opts}
  return ParsedEndpoint(
      kind='iceberg',
      scheme='iceberg',
      raw=location,
      format='iceberg',
      yaml_type=verb + 'Iceberg',
      config=cfg,
      output_prefix=None,
      is_java_xlang=True,
      is_unbounded=False)
