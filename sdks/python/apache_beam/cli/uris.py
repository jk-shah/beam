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
  kind: str  # 'file', 'bigquery', 'postgres', 'mysql', 'spanner', 'pubsub', 'kafka', 'iceberg', 'delta'
  scheme: str
  raw: str
  format: str | None
  yaml_type: str
  config: dict[str, Any]
  output_prefix: str | None = None
  is_java_xlang: bool = False
  is_unbounded: bool = False


FILE_SCHEMES = frozenset(['gs', 's3', 'azfs', 'hdfs', 'file'])
NON_FILE_SCHEMES = frozenset([
    'bigquery',
    'postgres',
    'postgresql',
    'mysql',
    'spanner',
    'pubsub',
    'kafka',
    'iceberg',
    'delta',
])
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
    '.log': 'text',
    '.parquet': 'parquet',
    '.avro': 'avro',
    '.txt': 'text',
}


def _looks_like_utf8_text(sample: bytes) -> bool:
  """Returns True when `sample` looks like UTF-8 plain text/log content."""
  if not sample or b'\x00' in sample:
    return False
  trimmed = sample
  if len(trimmed) == 512:
    # Avoid rejecting valid UTF-8 whose final multi-byte code point is split.
    for _ in range(3):
      if trimmed and (trimmed[-1] & 0xC0) == 0x80:
        trimmed = trimmed[:-1]
      else:
        break
    if trimmed and trimmed[-1] >= 0xC0:
      trimmed = trimmed[:-1]
  try:
    text = trimmed.decode('utf-8')
  except UnicodeDecodeError:
    return False
  if not text:
    return False
  return all(
      ch in '\t\n\r\x0b\x0c\x1b' or (ord(ch) >= 32 and ord(ch) != 127)
      for ch in text)


_SHARD_OR_GLOB_SUFFIX = re.compile(r'(-\d+-of-\d+|[-*?])+$')
_SECRET_KEY_RE = re.compile(
    r'(password|passwd|passphrase|secret|token|private_key|signing_key|'
    r'encryption_key|client_secret|access_key|api[-_]?key|authorization|'
    r'bearer|jaas|credential|connection_string|'
    r'(?:^|[_.-])(?:auth|pwd|cookie)(?:$|[_.-]))',
    re.IGNORECASE)
_BQ_TABLE_ID_RE = re.compile(
    r'^(?:(?P<project>[A-Za-z0-9_.:-]+)[:.])?'
    r'(?P<dataset>[A-Za-z0-9_]+)\.(?P<table>[A-Za-z0-9_$-]+)$')


def extension_of(path: str) -> str:
  name = _SHARD_OR_GLOB_SUFFIX.sub('', os.path.basename(path.replace('\\', '/')))
  return os.path.splitext(name)[1].lower()


def local_format_groups(path: str) -> dict[str, list[str]]:
  """`{format: files}` when a local glob matches files in 2+ formats, else {}.

  Only files with a known extension count; a glob that also matches files
  without one (for example Beam shards) keeps the single-format behavior.
  """
  if '://' in path or not any(c in path for c in '*?['):
    return {}
  import glob as _glob
  groups: dict[str, list[str]] = {}
  for p in sorted(_glob.glob(path)):
    if not os.path.isfile(p):
      continue
    fmt = EXTENSIONS.get(extension_of(p))
    if fmt not in ('csv', 'json', 'parquet', 'avro'):
      return {}
    groups.setdefault(fmt, []).append(p)
  return groups if len(groups) > 1 else {}


def sniff_file_format(path: str) -> str | None:
  """Sniffs `PAR1` / `Obj\\x01` / JSON / plain UTF-8 text on shards."""
  try:
    if '://' not in path:
      import glob as _glob
      candidates = sorted(
          p for p in _glob.glob(path) if os.path.isfile(p) and
          os.path.getsize(p) > 0)
      if not candidates and not any(c in path for c in '*?['):
        candidates = [
            p for p in sorted(_glob.glob(_glob.escape(path) + '*'))
            if os.path.isfile(p) and os.path.getsize(p) > 0
        ]
      if not candidates:
        return None
      with open(candidates[0], 'rb') as f:
        head = f.read(512)
        # Parquet also ends with PAR1, and starts with PAR1.
    else:
      from apache_beam.io.filesystems import FileSystems
      pattern = path if any(c in path for c in '*?[') else path + '*'
      matches = [
          m for m in FileSystems.match([pattern])[0].metadata_list
          if m.size_in_bytes > 0
      ]
      if not matches:
        return None
      with FileSystems.open(matches[0].path) as f:
        head = f.read(512)
  except Exception:  # pylint: disable=broad-except
    return None

  if head.startswith(b'PAR1'):
    return 'parquet'
  if head.startswith(b'Obj\x01'):
    return 'avro'
  stripped = head.lstrip()
  if stripped.startswith(b'{'):
    return 'json'

  cand_paths = candidates if '://' not in path else [m.path for m in matches]
  for c_path in cand_paths:
    ext = extension_of(c_path)
    if ext in EXTENSIONS:
      ext_fmt = EXTENSIONS[ext]
      return 'json' if ext_fmt == 'jsonl' else ext_fmt

  if _looks_like_utf8_text(head):
    return 'text'

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


def _mapping_has_secret_key(obj: Any) -> str | None:
  """Returns the first key name matching `_SECRET_KEY_RE` in `obj`, or None."""
  if isinstance(obj, dict):
    for k, v in obj.items():
      if _SECRET_KEY_RE.search(str(k)):
        return str(k)
      nested = _mapping_has_secret_key(v)
      if nested:
        return nested
  elif isinstance(obj, list):
    for item in obj:
      nested = _mapping_has_secret_key(item)
      if nested:
        return nested
  return None


def parse_kv_options(
    opt_items: Iterable[str] | None,
    opt_file: str | None = None,
    flag_name: str = '--from-opt',
) -> dict[str, Any]:
  """Parses repeatable `KEY=VALUE` flags + optional YAML/JSON `--*-opt-file`."""
  from . import output as out_mod
  merged: dict[str, Any] = {}
  if opt_file:
    if not os.path.isfile(opt_file):
      raise UriError(f'options file not found: {opt_file!r}')
    import yaml
    try:
      raw_text = out_mod.read_bounded_text(opt_file, label=opt_file)
      loaded = out_mod.safe_yaml_load(raw_text) or {}
    except (OSError, ValueError, yaml.YAMLError) as e:
      raise UriError(f'could not parse {opt_file!r}: {e}') from None
    if not isinstance(loaded, dict):
      raise UriError(f'expected key/value mapping in {opt_file!r}')
    secret_key = _mapping_has_secret_key(loaded)
    if secret_key and os.name == 'posix':
      if os.path.islink(opt_file):
        raise UriError(
            f'options file {opt_file!r} contains secret key {secret_key!r} '
            'and cannot be a symbolic link; pass a regular 0600 file')
      try:
        mode = os.stat(opt_file).st_mode & 0o777
      except OSError:
        mode = 0
      if mode & 0o077:
        raise UriError(
            f'options file {opt_file!r} contains secret key {secret_key!r} '
            f'with overly permissive mode {oct(mode)}; restrict permissions '
            f'first with: chmod 0600 {opt_file}')
    merged.update(loaded)

  if isinstance(opt_items, dict):
    merged.update(opt_items)
    return merged

  for raw in opt_items or ():
    if '=' not in raw:
      shown = raw if re.fullmatch(r'[A-Za-z0-9_.-]{1,32}', raw) else '<redacted>'
      raise UriError(f'expected KEY=VALUE for {flag_name}, got {shown!r}')
    key, val = raw.split('=', 1)
    key = key.strip()
    if not key:
      raise UriError(f'empty option key in {flag_name}')
    if _SECRET_KEY_RE.search(key):
      raise UriError(
          f'refusing secret option {key!r} on the command line ({flag_name}); '
          f'place secrets in {flag_name}-file (chmod 0600) or environment '
          'variables')
    coerced = _coerce_opt_value(val)
    nested_secret = _mapping_has_secret_key(coerced)
    if nested_secret:
      raise UriError(
          f'refusing nested secret key {nested_secret!r} in {flag_name} '
          f'{key}=...; place secrets in {flag_name}-file (chmod 0600) or '
          'environment variables')
    merged[key] = coerced
  return merged


_BQ_DISALLOWED_ROW_RESTRICTION_KW_RE = re.compile(
    r'\b(SELECT|UNION|INTERSECT|EXCEPT|FROM|JOIN|INTO|INSERT|UPDATE|DELETE|'
    r'DROP|ALTER|CREATE|MERGE|CALL|EXECUTE|GRANT|REVOKE|TRUNCATE)\b',
    re.IGNORECASE)


def validate_bq_row_restriction(raw: str) -> str:
  """Validates that a BigQuery `row_restriction` is a single filter predicate."""
  expr = str(raw or '').strip()
  if not expr:
    return ''
  if len(expr) > 4096:
    raise UriError('BigQuery row_restriction exceeds maximum length (4096)')
  unquoted_chars: list[str] = []
  quote: str | None = None
  escaped = False
  paren_depth = 0
  i = 0
  n = len(expr)
  while i < n:
    ch = expr[i]
    if quote is not None:
      if escaped:
        escaped = False
      elif ch == '\\':
        escaped = True
      elif ch == quote:
        quote = None
      i += 1
      continue
    if ch in ("'", '"', '`'):
      quote = ch
      unquoted_chars.append(' ')
      i += 1
      continue
    if expr[i:i + 2] in ('--', '/*', '*/'):
      raise UriError(
          'SQL comments are not allowed in BigQuery row_restriction')
    if ch == ';':
      raise UriError(
          'semicolons are not allowed in BigQuery row_restriction')
    if ch == '(':
      paren_depth += 1
    elif ch == ')':
      paren_depth -= 1
      if paren_depth < 0:
        raise UriError(
            'unbalanced parentheses in BigQuery row_restriction')
    unquoted_chars.append(ch)
    i += 1
  if quote is not None or paren_depth != 0:
    raise UriError(
        'unterminated quote or unbalanced parentheses in BigQuery row_restriction'
    )
  unquoted = ''.join(unquoted_chars)
  m_kw = _BQ_DISALLOWED_ROW_RESTRICTION_KW_RE.search(unquoted)
  if m_kw:
    raise UriError(
        f'SQL keyword {m_kw.group(1).upper()!r} is not allowed in BigQuery '
        'row_restriction; pass a full query via --from-opt query="..." instead')
  return expr


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


_JINJA_BLOCK_RE = re.compile(r'\{\{(.*?)\}\}', re.DOTALL)
_JINJA_LEAD_VAR_RE = re.compile(r'^\s*([A-Za-z_][A-Za-z0-9_]*)')
_JINJA_STR_LIT_RE = re.compile(r"""['"]([^'"]+)['"]""")
_JINJA_FORMAT_TOKENS: dict[str, str] = {
    'csv': 'csv',
    'tsv': 'csv',
    'json': 'json',
    'jsonl': 'json',
    'ndjson': 'json',
    'parquet': 'parquet',
    'pq': 'parquet',
    'avro': 'avro',
}


def _has_jinja_syntax(text: str) -> bool:
  return ('{{' in text or '}}' in text or '{%' in text or '%}' in text or
          '{#' in text or '#}' in text)


def _validate_jinja_uri(uri: str) -> None:
  """Validates Jinja2 `{{ ... }}` placeholders via AST without rendering."""
  if '{%' in uri or '%}' in uri or '{#' in uri or '#}' in uri:
    raise UriError(
        f'Jinja control/comment blocks in {uri!r} are not allowed; '
        'use {{ variable }} value placeholders')
  if uri.count('{{') != uri.count('}}') or '{{' not in uri:
    raise UriError(f'unclosed Jinja2 {{ ... }} placeholder in {uri!r}')
  for m in _JINJA_BLOCK_RE.finditer(uri):
    if not m.group(1).strip():
      raise UriError(f'empty Jinja2 {{ }} placeholder in {uri!r}')
  try:
    import jinja2
    from jinja2 import nodes as jnodes
  except ImportError:
    return
  try:
    parsed_j = jinja2.Environment().parse(uri)
  except jinja2.TemplateSyntaxError as e:
    raise UriError(
        f'invalid Jinja2 syntax in URI {uri!r}: {e.message}') from None
  for jnode in parsed_j.find_all((jnodes.Getattr, jnodes.Getitem, jnodes.Call)):
    raise UriError(
        f'disallowed Jinja2 expression {type(jnode).__name__} in URI {uri!r}; '
        'only simple {{ param }} or {{ param | default(...) }} placeholders '
        'are permitted')
  for nnode in parsed_j.find_all(jnodes.Name):
    if str(nnode.name).startswith('_'):
      raise UriError(
          f'underscore-prefixed Jinja2 variable {nnode.name!r} is not allowed')


def _infer_jinja_file_format(location: str) -> str:
  """Infers file format from Jinja URI templates (`{{ input_csv }}`, etc.)."""
  stripped = _JINJA_BLOCK_RE.sub('', location)
  ext_fmt = EXTENSIONS.get(extension_of(stripped), '').lower()
  if ext_fmt:
    return ext_fmt

  blocks = _JINJA_BLOCK_RE.findall(location)
  if not blocks:
    return ''

  # Check default('...ext') string literals inside the last Jinja block first.
  last_expr = blocks[-1]
  for lit in _JINJA_STR_LIT_RE.findall(last_expr):
    lit_fmt = EXTENSIONS.get(extension_of(lit), '').lower()
    if lit_fmt:
      return lit_fmt

  m_var = _JINJA_LEAD_VAR_RE.match(last_expr)
  if not m_var:
    return ''
  tokens = [t for t in m_var.group(1).lower().split('_') if t]
  matched = {_JINJA_FORMAT_TOKENS[t] for t in tokens if t in _JINJA_FORMAT_TOKENS}
  if len(matched) == 1:
    return next(iter(matched))
  return ''


_SAFE_BQ_PROJECT_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$')


def validate_bq_project_id(proj: str | None) -> str | None:
  """Validates a GCP / BigQuery project identifier before SQL/API interpolation."""
  if proj is None or proj == '':
    return None
  p = str(proj)
  if '..' in p or not _SAFE_BQ_PROJECT_RE.match(p):
    raise UriError(f'invalid BigQuery project identifier {proj!r}')
  return p


def normalize_bq_table_spec(
    raw_uri_or_spec: str,
    default_project: str | None = None,
    *,
    allow_jinja: bool = False,
) -> tuple[str, str | None, str, str]:
  """Normalizes `bigquery://P.D.T` or `P:D.T` -> `('P:D.T', P, D, T)`."""
  if default_project and not (allow_jinja and _has_jinja_syntax(default_project)):
    validate_bq_project_id(default_project)
  body = raw_uri_or_spec
  if body.lower().startswith('bigquery://'):
    body = body[len('bigquery://'):]
  body = body.strip('/')
  if allow_jinja and _has_jinja_syntax(body):
    _validate_jinja_uri(raw_uri_or_spec)
    masked = _JINJA_BLOCK_RE.sub('__j__', body)
    if ':' in masked:
      spec = body
    elif masked.count('.') == 2:
      # Convert first top-level `.` outside `{{ ... }}` into `:`.
      depth = 0
      dot_pos = -1
      for idx, ch in enumerate(body):
        if body[idx:idx + 2] == '{{':
          depth += 1
        elif body[idx:idx + 2] == '}}' and depth > 0:
          depth -= 1
        elif ch == '.' and depth == 0:
          dot_pos = idx
          break
      spec = f'{body[:dot_pos]}:{body[dot_pos + 1:]}' if dot_pos != -1 else body
    elif masked.count('.') == 1 and default_project:
      spec = f'{default_project}:{body}'
    else:
      spec = body
    return spec, default_project, '', ''
  m = _BQ_TABLE_ID_RE.match(body)
  if not m or '..' in (m.group('project') or ''):
    raise UriError(
        f'invalid BigQuery table URI {raw_uri_or_spec!r}; expected '
        'bigquery://PROJECT.DATASET.TABLE or bigquery://PROJECT:DATASET.TABLE')
  proj = validate_bq_project_id(m.group('project') or default_project)
  dataset = m.group('dataset')
  table = m.group('table')
  spec = f'{proj}:{dataset}.{table}' if proj else f'{dataset}.{table}'
  return spec, proj, dataset, table


_SAFE_GCP_RESOURCE_SEG_RE = re.compile(r'^[A-Za-z0-9_.:~+%=-]{1,256}$')
_BLOCKED_JDBC_PROPS = frozenset([
    'allowloadlocalinfile',
    'allowloadlocalinfileinpath',
    'allowurlinlocalinfile',
    'authenticationpluginclassname',
    'autodeserialize',
    'clientcertificatekeystoreurl',
    'connectionlifecycleinterceptors',
    'customsocketfactory',
    'detectcustomcollations',
    'exceptioninterceptors',
    'loggerfile',
    'loggerlevel',
    'propertiestransform',
    'queryinterceptors',
    'socketfactory',
    'socketfactoryarg',
    'socksproxyhost',
    'sslfactory',
    'sslfactoryarg',
    'sslpasswordcallback',
    'statementinterceptors',
    'trustcertificatekeystoreurl',
])
_BLOCKED_KAFKA_CLIENT_PROPS = frozenset([
    'interceptor.classes',
    'metric.reporters',
    'sasl.client.callback.handler.class',
    'sasl.login.callback.handler.class',
    'sasl.login.class',
    'security.providers',
    'ssl.engine.factory.class',
])


def _validate_resource_seg(seg: str, label: str, allow_jinja: bool = False) -> str:
  if allow_jinja and _has_jinja_syntax(seg):
    _validate_jinja_uri(seg)
    return seg
  if not seg or seg in ('.', '..') or '..' in seg or not _SAFE_GCP_RESOURCE_SEG_RE.match(seg):
    raise UriError(f'invalid {label} identifier {seg!r}')
  return seg


def _validate_jdbc_properties(
    qparams: dict[str, Any],
    conn_props: Iterable[Any],
    extra_opts: dict[str, Any],
) -> None:
  for k in qparams:
    norm = str(k).strip().lower()
    if norm in _BLOCKED_JDBC_PROPS or any(ord(c) < 32 for c in str(k)):
      raise UriError(f'disallowed JDBC driver property {k!r}')
  for prop in conn_props:
    prop_s = str(prop)
    if any(ord(c) < 32 for c in prop_s):
      raise UriError('control character in connection_properties is not allowed')
    pk = prop_s.split('=', 1)[0].strip().lower()
    if pk in _BLOCKED_JDBC_PROPS:
      raise UriError(f'disallowed JDBC connection property {pk!r}')
  for k, v in extra_opts.items():
    norm = str(k).strip().lower()
    if norm in _BLOCKED_JDBC_PROPS:
      raise UriError(f'disallowed JDBC option {k!r}')
    if isinstance(v, dict):
      for sub_k in v:
        if str(sub_k).strip().lower() in _BLOCKED_JDBC_PROPS:
          raise UriError(f'disallowed JDBC property {sub_k!r}')


def _validate_kafka_options(opts: dict[str, Any]) -> None:
  for cfg_key in ('consumer_config', 'producer_config'):
    sub = opts.get(cfg_key)
    if isinstance(sub, dict):
      for k, v in sub.items():
        nk = str(k).strip().lower()
        if nk in _BLOCKED_KAFKA_CLIENT_PROPS:
          raise UriError(f'disallowed Kafka client class option {k!r}')
        if nk == 'sasl.jaas.config' and 'ldaploginmodule' in str(v).lower():
          raise UriError('disallowed JNDI LdapLoginModule in sasl.jaas.config')


def parse_pubsub_uri(uri: str, *, allow_jinja: bool = False) -> tuple[str, str]:
  """Returns `('topic'|'subscription', 'projects/<p>/topics/<t>')`."""
  reject_inline_credentials(uri)
  parsed = urllib.parse.urlsplit(uri)
  if parsed.query or parsed.fragment:
    raise UriError(
        f'unexpected query/fragment in Pub/Sub URI {uri!r}; pass options via '
        '--from-opt / --to-opt')
  body = uri[len('pubsub://'):].strip('/')
  parts = body.split('/')
  if (len(parts) == 4 and parts[0] == 'projects' and
      parts[2] in ('topics', 'subscriptions') and parts[1] and parts[3]):
    _validate_resource_seg(parts[1], 'Pub/Sub project', allow_jinja=allow_jinja)
    _validate_resource_seg(parts[3], f'Pub/Sub {parts[2][:-1]}', allow_jinja=allow_jinja)
    kind = 'topic' if parts[2] == 'topics' else 'subscription'
    return kind, body
  raise UriError(
      f'invalid Pub/Sub URI {uri!r}; expected '
      'pubsub://projects/<project>/topics/<topic> or '
      'pubsub://projects/<project>/subscriptions/<subscription>')


def parse_spanner_uri(
    uri: str, *, allow_jinja: bool = False
) -> tuple[str, str, str, dict[str, str]]:
  """Parses `spanner://projects/P/instances/I/databases/D?...` or `spanner://P/I/D?...`."""
  reject_inline_credentials(uri)
  parsed = urllib.parse.urlsplit(uri)
  raw_path = (parsed.netloc + parsed.path).strip('/')
  parts = [p for p in raw_path.split('/') if p]
  qparams = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=False))
  if (len(parts) == 6 and parts[0] == 'projects' and
      parts[2] == 'instances' and parts[4] == 'databases'):
    proj = _validate_resource_seg(parts[1], 'Spanner project', allow_jinja=allow_jinja)
    inst = _validate_resource_seg(parts[3], 'Spanner instance', allow_jinja=allow_jinja)
    db = _validate_resource_seg(parts[5], 'Spanner database', allow_jinja=allow_jinja)
    return proj, inst, db, qparams
  if len(parts) == 3 and parts[0] != 'projects':
    proj = _validate_resource_seg(parts[0], 'Spanner project', allow_jinja=allow_jinja)
    inst = _validate_resource_seg(parts[1], 'Spanner instance', allow_jinja=allow_jinja)
    db = _validate_resource_seg(parts[2], 'Spanner database', allow_jinja=allow_jinja)
    return proj, inst, db, qparams
  raise UriError(
      f'invalid Spanner URI {uri!r}; expected '
      'spanner://projects/<project>/instances/<instance>/databases/<database>'
      '?table=<table> (or spanner://<project>/<instance>/<database>?...)')


def parse_endpoint(
    location: str,
    *,
    is_read: bool,
    explicit_format: str | None = None,
    options: dict[str, Any] | None = None,
    if_exists: str = 'fail',
    default_project: str | None = None,
    allow_sniff: bool = False,
    cdc: bool = False,
    zero_copy: bool = False,
    allow_jinja: bool = False,
) -> ParsedEndpoint:
  """Parses a `--from` or `--to` location into a `ParsedEndpoint`."""
  if not location or not location.strip():
    raise UriError('endpoint path/URI cannot be empty')
  reject_inline_credentials(location)
  has_jinja = _has_jinja_syntax(location)
  if allow_jinja and has_jinja:
    _validate_jinja_uri(location)

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
    if not fmt and allow_jinja and has_jinja:
      fmt = _infer_jinja_file_format(location)
    if not fmt and is_read and (allow_sniff or not has_jinja):
      fmt = sniff_file_format(location) or ''
    if not fmt:
      jinja_hint = (
          '\n  (pass --emit-yaml to emit a parameterized Jinja2 YAML template)'
          if has_jinja and not allow_jinja else '')
      raise UriError(
          f'cannot tell the format of {location!r} (no known file extension)\n'
          f'  add {flag_hint}, for example: {flag_hint} json{jinja_hint}')
    if fmt not in FILE_FORMATS:
      valid = ', '.join(k for k in ('csv', 'json', 'parquet', 'avro', 'text'))
      raise UriError(
          f'unsupported format {fmt!r} for {location!r}; use one of: {valid}')
    norm_fmt = 'json' if fmt == 'jsonl' else fmt
    read_path = location
    if is_read and not (allow_jinja and has_jinja) and not any(
        c in location for c in '*?['):
      if not scheme and not os.path.exists(location):
        import glob as _glob
        shard_pat = _glob.escape(location) + '-*-of-*'
        if _glob.glob(shard_pat):
          read_path = shard_pat
      elif scheme in FILE_SCHEMES and allow_sniff:
        try:
          from apache_beam.io.filesystems import FileSystems
          if not FileSystems.match([location])[0].metadata_list:
            remote_shard_pat = location + '-*-of-*'
            if FileSystems.match([remote_shard_pat])[0].metadata_list:
              read_path = remote_shard_pat
        except Exception:  # pylint: disable=broad-except
          pass
    cfg = {'path': read_path, **opts}
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
          location, default_project=default_project, allow_jinja=allow_jinja)
      cfg = {'table': table_spec, **opts}
    if cfg.get('row_restriction') and not (
        allow_jinja and _has_jinja_syntax(str(cfg['row_restriction']))):
      cfg['row_restriction'] = validate_bq_row_restriction(
          str(cfg['row_restriction']))
    if not is_read:
      disp_map = {
          'fail': 'WRITE_EMPTY',
          'append': 'WRITE_APPEND',
          'replace': 'WRITE_TRUNCATE',
      }
      cfg.setdefault('create_disposition', 'CREATE_IF_NEEDED')
      cfg.setdefault(
          'write_disposition',
          'WRITE_APPEND' if (cdc and if_exists == 'fail') else
          disp_map.get(if_exists, 'WRITE_EMPTY'))
    bq_yaml_type = (
        'WriteToBigQueryCDC' if (not is_read and cdc) else verb + 'BigQuery')
    return ParsedEndpoint(
        kind='bigquery',
        scheme='bigquery',
        raw=location,
        format='bigquery',
        yaml_type=bq_yaml_type,
        config=cfg,
        output_prefix=None,
        is_java_xlang=not is_read,
        is_unbounded=False)

  # 3. Postgres & MySQL (`postgres://host:port/db?table=...`, `mysql://...`)
  if scheme in ('postgres', 'postgresql', 'mysql'):
    is_pg = scheme in ('postgres', 'postgresql')
    norm_kind = 'postgres' if is_pg else 'mysql'
    default_port = 5432 if is_pg else 3306
    label_db = 'Postgres' if is_pg else 'MySql'
    parsed = urllib.parse.urlsplit(location)
    if not parsed.hostname or not parsed.path.strip('/'):
      raise UriError(
          f'invalid {label_db} URI {location!r}; expected '
          f'{norm_kind}://HOST[:PORT]/DATABASE?table=TABLE')
    qparams = dict(
        urllib.parse.parse_qsl(parsed.query, keep_blank_values=False))
    table_name = opts.pop('table', None) or qparams.pop('table', None)
    query_sql = opts.pop('query', None)
    if not table_name and not (is_read and query_sql and not cdc):
      raise UriError(
          f'{label_db} URI {location!r} requires ?table=TABLE'
          + ('' if cdc else ' (or --from-opt query="SELECT ...")'))
    port_num = parsed.port or default_port
    db_name = parsed.path.lstrip('/')
    if not (allow_jinja and _has_jinja_syntax(db_name)):
      if any(ch in db_name for ch in (';', '?', '&', '#', '/', '\\')) or any(
          ord(c) <= 32 for c in db_name) or '..' in db_name:
        raise UriError(f'invalid {label_db} database name {db_name!r}')
    conn_props_in = list(opts.get('connection_properties') or [])
    _validate_jdbc_properties(qparams, conn_props_in, opts)
    env_prefix = 'BEAM_POSTGRES' if is_pg else 'BEAM_MYSQL'
    user = opts.pop('username', None) or os.environ.get(f'{env_prefix}_USER')
    pwd = opts.pop('password', None) or os.environ.get(f'{env_prefix}_PASSWORD')

    if is_read and cdc:
      conn_props = list(opts.pop('connection_properties', None) or [])
      if not any(str(p).startswith('database.dbname=') for p in conn_props):
        conn_props.append(f'database.dbname={db_name}')
      if is_pg and not any(
          str(p).startswith('plugin.name=') for p in conn_props):
        conn_props.append('plugin.name=pgoutput')
      for qk, qv in qparams.items():
        conn_props.append(f'{qk}={qv}')
      deb_cfg: dict[str, Any] = {
          'connector': 'POSTGRES' if is_pg else 'MYSQL',
          'host': parsed.hostname,
          'port': port_num,
          'table': table_name,
          'connection_properties': conn_props,
      }
      if user:
        deb_cfg['username'] = user
      if pwd:
        deb_cfg['password'] = pwd
      deb_cfg.update(opts)
      unbounded_cdc = not (
          'max_number_of_records' in deb_cfg or 'max_time_to_run' in deb_cfg)
      return ParsedEndpoint(
          kind=norm_kind,
          scheme=norm_kind,
          raw=location,
          format='debezium',
          yaml_type='ReadFromDebezium',
          config=deb_cfg,
          output_prefix=None,
          is_java_xlang=True,
          is_unbounded=unbounded_cdc)

    jdbc_sub = 'postgresql' if is_pg else 'mysql'
    jdbc_query = urllib.parse.urlencode(qparams)
    jdbc_url = f'jdbc:{jdbc_sub}://{parsed.hostname}:{port_num}/{db_name}'
    if jdbc_query:
      jdbc_url = f'{jdbc_url}?{jdbc_query}'

    cfg = {'url': jdbc_url}
    if table_name:
      cfg['table'] = table_name
    if query_sql:
      cfg['query'] = query_sql
    if user:
      cfg['username'] = user
    if pwd:
      cfg['password'] = pwd
    cfg.update(opts)
    return ParsedEndpoint(
        kind=norm_kind,
        scheme=norm_kind,
        raw=location,
        format='jdbc',
        yaml_type=verb + label_db,
        config=cfg,
        output_prefix=None,
        is_java_xlang=True,
        is_unbounded=False)

  # 4. Cloud Spanner (`spanner://projects/P/instances/I/databases/D?table=T`)
  if scheme == 'spanner':
    sp_proj, sp_inst, sp_db, qparams = parse_spanner_uri(
        location, allow_jinja=allow_jinja)
    sp_cfg: dict[str, Any] = {
        'project': sp_proj,
        'instance': sp_inst,
        'database': sp_db,
    }
    for qk, qv in qparams.items():
      sp_cfg[qk] = _coerce_opt_value(qv)
    sp_cfg.update(opts)
    is_sp_cdc = bool(is_read and (cdc or 'change_stream' in sp_cfg))
    if not is_sp_cdc and not sp_cfg.get('table') and not (
        is_read and sp_cfg.get('query')):
      raise UriError(
          f'Spanner URI {location!r} requires ?table=TABLE '
          '(or ?change_stream=STREAM for CDC)')
    return ParsedEndpoint(
        kind='spanner',
        scheme='spanner',
        raw=location,
        format='spanner_cdc' if is_sp_cdc else 'spanner',
        yaml_type='ReadFromSpannerCDC' if is_sp_cdc else verb + 'Spanner',
        config=sp_cfg,
        output_prefix=None,
        is_java_xlang=True,
        is_unbounded=bool(is_sp_cdc and 'end_at' not in sp_cfg))

  # 5. Pub/Sub (`pubsub://projects/P/topics/T` or `.../subscriptions/S`)
  if scheme == 'pubsub':
    ps_kind, ps_resource = parse_pubsub_uri(location, allow_jinja=allow_jinja)
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

  # 6. Kafka (`kafka://host:port/topic`)
  if scheme == 'kafka':
    parsed = urllib.parse.urlsplit(location)
    topic = parsed.path.strip('/')
    if not parsed.netloc or not topic or parsed.query or parsed.fragment:
      raise UriError(
          f'invalid Kafka URI {location!r}; expected '
          'kafka://BOOTSTRAP_SERVERS/TOPIC')
    if not (allow_jinja and _has_jinja_syntax(parsed.netloc)) and not re.fullmatch(
        r'[A-Za-z0-9._:,\[\]-]+', parsed.netloc):
      raise UriError(f'invalid Kafka bootstrap_servers {parsed.netloc!r}')
    if not (allow_jinja and _has_jinja_syntax(topic)) and (
        topic in ('.', '..') or '..' in topic or
        not re.fullmatch(r'[A-Za-z0-9._-]+', topic)):
      raise UriError(f'invalid Kafka topic name {topic!r}')
    # Confluent Schema Registry configuration
    schema_reg = (
        opts.pop('schema_registry', None) or
        opts.pop('schema_registry_url', None) or
        opts.pop('confluent_schema_registry_url', None))
    if schema_reg:
      schema_reg_s = str(schema_reg).strip()
      if not (schema_reg_s.startswith('http://') or schema_reg_s.startswith('https://')):
        raise UriError(
            f'invalid schema_registry URL {schema_reg!r}; expected http:// or https://')
      opts['confluent_schema_registry_url'] = schema_reg_s

    schema_subj = (
        opts.pop('schema_registry_subject', None) or
        opts.pop('confluent_schema_registry_subject', None))
    if schema_subj:
      opts['confluent_schema_registry_subject'] = str(schema_subj).strip()

    # Consumer group / group.id configuration
    consumer_grp = opts.pop('consumer_group', None) or opts.pop('group_id', None)
    if consumer_grp:
      c_cfg = opts.setdefault('consumer_config', {})
      if isinstance(c_cfg, dict):
        c_cfg['group.id'] = str(consumer_grp).strip()

    # Offset reset configuration
    offset_reset = (
        opts.pop('auto_offset_reset', None) or
        opts.pop('offset_reset', None) or
        opts.pop('auto_offset_reset_config', None))
    if offset_reset:
      opts['auto_offset_reset_config'] = str(offset_reset).strip().lower()

    # Timestamp policy / attribute configuration
    ts_policy = (
        opts.pop('timestamp_policy', None) or
        opts.pop('timestamp_attribute', None))
    if ts_policy:
      opts['timestamp_policy'] = str(ts_policy).strip()

    _validate_kafka_options(opts)
    default_kafka_fmt = 'AVRO' if schema_reg else 'JSON'
    fmt = (explicit_format or opts.pop('format', default_kafka_fmt)).upper()
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

  # 7. Delta Lake (`delta:///path/to/table` or `delta://gs://bucket/table`)
  if scheme == 'delta':
    if not is_read:
      raise UriError(
          f'writing directly to {location!r} is not supported; write to '
          'iceberg:// (e.g. iceberg://namespace.table?warehouse=...) or '
          'parquet instead')
    raw_rest = location[len('delta://'):]
    path_part, _, query_str = raw_rest.partition('?')
    path_part = path_part.strip()
    if not path_part:
      raise UriError(
          f'invalid Delta Lake URI {location!r}; expected '
          'delta://<local_path_or_gs_uri>')
    if '://' in path_part:
      reject_inline_credentials(path_part)
    qparams = dict(urllib.parse.parse_qsl(query_str, keep_blank_values=False))
    delta_cfg: dict[str, Any] = {'table': path_part}
    for qk, qv in qparams.items():
      delta_cfg[qk] = _coerce_opt_value(qv)
    delta_cfg.update(opts)
    return ParsedEndpoint(
        kind='delta',
        scheme='delta',
        raw=location,
        format='delta_cdc' if cdc else 'delta',
        yaml_type='ReadFromDeltaCDC' if cdc else 'ReadFromDelta',
        config=delta_cfg,
        output_prefix=None,
        is_java_xlang=True,
        is_unbounded=False)

  # 8. Iceberg (`iceberg://namespace.table?warehouse=...` or `?catalog=biglake&...`)
  parsed = urllib.parse.urlsplit(location)
  if not parsed.netloc and parsed.path.startswith('/'):
    raw_ident = parsed.path.rstrip('/')
  else:
    raw_ident = (parsed.netloc + parsed.path).strip('/')
  if not raw_ident:
    raise UriError(
        f'invalid Iceberg URI {location!r}; expected '
        'iceberg://NAMESPACE.TABLE?warehouse=gs://bucket/warehouse')
  qparams = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=False))
  cat_shortcut = str(
      opts.pop('catalog', None) or qparams.pop('catalog', '') or
      '').strip().lower()
  if not cat_shortcut:
    low_ident = raw_ident.lower()
    if low_ident.startswith('biglake.'):
      cat_shortcut = 'biglake'
    elif low_ident.startswith('bigquery.'):
      cat_shortcut = 'bigquery'
  warehouse_val = opts.pop('warehouse', None) or qparams.pop('warehouse', None)
  rest_uri = opts.pop('uri', None) or qparams.pop('uri', None)
  cat_props = dict(opts.pop('catalog_properties', None) or {})

  if cat_shortcut in ('bigquery', 'bigquery-managed', 'bigquery_managed'):
    opts.setdefault('catalog_name', 'bigquery')
    cat_props.setdefault('type', 'bigquery')
    cat_props.setdefault('api', 'https://bigquery.googleapis.com')
    cat_props.setdefault('rest.auth.type', 'google')
    if warehouse_val:
      cat_props.setdefault('warehouse', str(warehouse_val))
    if default_project:
      cat_props.setdefault('header.x-goog-user-project', default_project)
  elif cat_shortcut == 'biglake':
    opts.setdefault('catalog_name', 'biglake')
    cat_props.setdefault('type', 'rest')
    cat_props.setdefault(
        'uri',
        str(rest_uri or 'https://biglake.googleapis.com/iceberg/v1/restcatalog'))
    cat_props.setdefault('rest.auth.type', 'google')
    if warehouse_val:
      cat_props.setdefault('warehouse', str(warehouse_val))
    if default_project:
      cat_props.setdefault('header.x-goog-user-project', default_project)
  elif cat_shortcut == 'rest' or rest_uri:
    opts.setdefault('catalog_name', cat_shortcut or 'rest')
    cat_props.setdefault('type', 'rest')
    if rest_uri:
      cat_props.setdefault('uri', str(rest_uri))
    if warehouse_val:
      cat_props.setdefault('warehouse', str(warehouse_val))
  elif warehouse_val:
    opts.setdefault('catalog_name', cat_shortcut or 'hadoop')
    cat_props.setdefault('type', 'hadoop')
    cat_props.setdefault('warehouse', str(warehouse_val))
  elif not cat_shortcut and not warehouse_val and not rest_uri and '/' not in raw_ident and '.' in raw_ident:
    # Auto-resolve using the strict catalog hierarchy:
    # 1. BigQuery managed Iceberg API first
    # 2. Google Cloud Lakehouse / BigLake REST Catalog second
    # 3. Generic Iceberg REST Catalog third
    opts.setdefault('catalog_name', 'bigquery')
    cat_props.setdefault('type', 'bigquery')
    cat_props.setdefault('api', 'https://bigquery.googleapis.com')
    cat_props.setdefault('rest.auth.type', 'google')
    if default_project:
      cat_props.setdefault('header.x-goog-user-project', default_project)

  for qk, qv in qparams.items():
    opts.setdefault(qk, _coerce_opt_value(qv))
  if cat_props:
    opts['catalog_properties'] = cat_props

  cfg = {'table': raw_ident, **opts}
  if is_read and cdc:
    ice_yaml_type = 'ReadFromIcebergCDC'
  elif not is_read and zero_copy:
    ice_yaml_type = 'IcebergAddFiles'
  else:
    ice_yaml_type = verb + 'Iceberg'
  return ParsedEndpoint(
      kind='iceberg',
      scheme='iceberg',
      raw=location,
      format='iceberg_cdc' if (is_read and cdc) else 'iceberg',
      yaml_type=ice_yaml_type,
      config=cfg,
      output_prefix=None,
      is_java_xlang=True,
      is_unbounded=False)
