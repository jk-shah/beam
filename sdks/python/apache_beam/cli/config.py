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

"""User-level default `PipelineOptions` for the experimental `beam` command.

`beam config` stores named profiles in `~/.beam/config.yaml` using the same
`options:` YAML mapping that Beam YAML pipeline files use. Options are checked
against `PipelineOptions` subclasses so typos are flagged when set and boolean
or list flags serialize into the exact argv flags `PipelineOptions` expects.

Safety rules:
- Configuration is read only from `~/.beam/config.yaml` (or `BEAM_CONFIG`),
  never from the current working directory.
- Explicit CLI flags and a pipeline file's `options:` block always win.
- Applied defaults are announced in one line on `stderr`, and `--no-config`
  (`BEAM_NO_CONFIG=1`) disables configuration completely for scripts and CI.
- Secret-shaped option names are rejected.
"""

import argparse
import difflib
import functools
import json
import os
import re
import sys
from pathlib import Path
from typing import Any
from typing import NamedTuple

_EXIT_NOT_FOUND = 1
_EXIT_USAGE = 2


class ConfigError(Exception):
  """An error in `beam config`, reported to the user with `exit_code`."""
  def __init__(self, message, exit_code):
    super().__init__(message)
    self.exit_code = exit_code


_VALID_KEY_RE = re.compile(r'^[a-zA-Z][a-zA-Z0-9_-]*$')
_VALID_PROFILE_RE = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9._-]*$')

# Credentials belong in ADC, environment variables, or `--*-opt-file`, never in
# a plain-text default options file.
_SECRET_KEY_RE = re.compile(
    r'(password|passwd|secret|token|private_key|client_secret|'
    r'access_key|api_key|jaas|credential|connection_string)',
    re.IGNORECASE)


class _OptionMeta(NamedTuple):
  dest: str
  primary_flag: str | None = None
  is_bool: bool = False
  true_flag: str | None = None
  false_flag: str | None = None
  is_comma_list: bool = False
  is_append_list: bool = False


def _all_subclasses(cls):
  result = []
  for sub in cls.__subclasses__():
    result.append(sub)
    result.extend(_all_subclasses(sub))
  return result


def _installed_beam_version() -> str:
  version_file = Path(__file__).resolve().parents[1] / 'version.py'
  if version_file.is_file():
    match = re.search(
        r"__version__\s*=\s*['\"]([^'\"]+)['\"]",
        version_file.read_text(encoding='utf-8'))
    if match:
      return match.group(1)
  import importlib.metadata
  try:
    return importlib.metadata.version('apache-beam')
  except importlib.metadata.PackageNotFoundError:
    return 'unknown'


def _option_cache_path() -> str:
  beam_home = os.path.expanduser(os.environ.get('BEAM_HOME', '~/.beam'))
  return os.path.join(
      beam_home, 'cache', f'pipeline_options-{_installed_beam_version()}.json')


def _introspect_pipeline_options(
) -> tuple[dict[str, _OptionMeta], dict[str, tuple[str, bool | None]]]:
  from apache_beam.options.pipeline_options import PipelineOptions

  parser = argparse.ArgumentParser(add_help=False, conflict_handler='resolve')
  for sub in _all_subclasses(PipelineOptions):
    sub._add_argparse_args(parser)

  by_dest: dict[str, _OptionMeta] = {}
  by_flag: dict[str, tuple[str, bool | None]] = {}
  for action in parser._actions:
    if not action.option_strings:
      continue
    dest = action.dest
    prev = by_dest.get(dest, _OptionMeta(dest=dest))
    cls_name = type(action).__name__
    long_flags = [s for s in action.option_strings if s.startswith('--')]
    dest_flag = f'--{dest}'
    primary = (
        dest_flag if dest_flag in action.option_strings else
        (long_flags[0] if long_flags else action.option_strings[0]))

    if cls_name in ('_StoreTrueAction', '_StoreFalseAction'):
      const_val = bool(getattr(action, 'const', cls_name == '_StoreTrueAction'))
      meta = prev._replace(
          is_bool=True,
          true_flag=primary if const_val else prev.true_flag,
          false_flag=primary if not const_val else prev.false_flag)
      for opt in action.option_strings:
        by_flag[opt.lstrip('-')] = (dest, const_val)
    else:
      meta = prev._replace(
          primary_flag=primary,
          is_comma_list=(cls_name == '_CommaSeparatedListAction'),
          is_append_list=(cls_name == '_AppendAction'))
      for opt in action.option_strings:
        by_flag[opt.lstrip('-')] = (dest, None)
    by_dest[dest] = meta
    by_flag.setdefault(dest, (dest, None))
  return by_dest, by_flag


@functools.lru_cache(maxsize=4)
def _cached_option_catalog(
    cache_key: str,
) -> tuple[dict[str, _OptionMeta], dict[str, tuple[str, bool | None]]]:
  if cache_key == ':live:':
    return _introspect_pipeline_options()

  try:
    with open(cache_key, encoding='utf-8') as f:
      raw = json.load(f)
    by_dest = {k: _OptionMeta(**v) for k, v in raw['by_dest'].items()}
    by_flag = {k: (v[0], v[1]) for k, v in raw['by_flag'].items()}
    return by_dest, by_flag
  except (OSError, ValueError, KeyError, TypeError):
    pass

  by_dest, by_flag = _introspect_pipeline_options()
  tmp_path = f'{cache_key}.{os.getpid()}.part'
  try:
    os.makedirs(os.path.dirname(cache_key), mode=0o700, exist_ok=True)
    fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    payload = {
        'by_dest': {
            k: v._asdict()
            for k, v in by_dest.items()
        },
        'by_flag': {
            k: [v[0], v[1]]
            for k, v in by_flag.items()
        },
    }
    with os.fdopen(fd, 'w', encoding='utf-8') as f:
      json.dump(payload, f)
    os.replace(tmp_path, cache_key)
  except OSError:
    if os.path.exists(tmp_path):
      try:
        os.remove(tmp_path)
      except OSError:
        pass
  return by_dest, by_flag


def _option_catalog(
) -> tuple[dict[str, _OptionMeta], dict[str, tuple[str, bool | None]]]:
  """Returns `(by_dest, by_flag)` for `PipelineOptions`, using disk cache.

  When `apache_beam` is not yet imported, reads
  `~/.beam/cache/pipeline_options-<version>.json` so `beam config set` runs in
  ~30ms without importing the full SDK.
  """
  key = (
      ':live:' if 'apache_beam.options.pipeline_options' in sys.modules else
      _option_cache_path())
  return _cached_option_catalog(key)


def run_command(args, config_path=None):
  """Executes `beam config <action>` and returns an exit code."""
  path = _default_config_path(config_path)
  data = _load_config(path)
  action = args.config_action

  if action == 'use':
    profile = _validate_profile_name(args.profile_name)
    data['active_profile'] = profile
    data.setdefault('profiles', {}).setdefault(profile, {'options': {}})
    _save_config(path, data)
    print(f'Switched active profile to {profile!r} ({path}).')
    return 0

  profile = _select_profile(data, getattr(args, 'profile', None))
  options = (
      data.setdefault('profiles', {}).setdefault(profile,
                                                 {}).setdefault('options', {}))

  if action == 'set':
    dest, value = _parse_key_value(args.option, args.value)
    options[dest] = value
    _save_config(path, data)
    return 0

  if action == 'get':
    stripped = _validate_option_key(args.option)
    if not options:
      return _EXIT_NOT_FOUND
    dest = (
        stripped if stripped in options else _canonical_dest(
            args.option, warn_unknown=False))
    if dest not in options:
      return _EXIT_NOT_FOUND
    val = options[dest]
    if isinstance(val, bool):
      print('true' if val else 'false')
    elif isinstance(val, list):
      print(','.join(str(x) for x in val))
    else:
      print(val)
    return 0

  if action == 'unset':
    stripped = _validate_option_key(args.option)
    if options:
      dest = (
          stripped if stripped in options else _canonical_dest(
              args.option, warn_unknown=False))
      options.pop(dest, None)
    _save_config(path, data)
    return 0

  # action == 'list'
  import yaml
  dumper = getattr(yaml, 'CSafeDumper', yaml.SafeDumper)
  print(f'# Profile: {profile} ({path})')
  print(yaml.dump({'options': options}, Dumper=dumper, sort_keys=False), end='')
  return 0


def resolve_pipeline_args(
    cli_args, profile=None, no_config=False, config_path=None) -> list[str]:
  """Prepends non-overridden profile options to `cli_args` and logs to stderr."""
  if _is_disabled(no_config):
    return list(cli_args)
  path = _default_config_path(config_path)
  if not os.path.isfile(path):
    return list(cli_args)

  data = _load_config(path)
  active = _select_profile(data, profile)
  options = data.get('profiles', {}).get(active, {}).get('options', {})
  if not options:
    return list(cli_args)

  overridden = _overridden_dests(cli_args)
  by_dest, _ = _option_catalog()
  injected: list[str] = []
  for key, value in options.items():
    norm_k = str(key).strip().lstrip('-').replace('-', '_').lower()
    if norm_k in _DANGEROUS_CONFIG_OPTIONS:
      raise ConfigError(
          f'refusing code/package option {key!r} from config file {path}; '
          'pass code-loading options explicitly on the command line',
          _EXIT_USAGE)
    if key in overridden or value is None:
      continue
    injected.extend(_option_to_flags(key, value, by_dest.get(key)))

  if injected:
    print(
        f'Using config profile {active!r} ({path}): {" ".join(injected)}',
        file=sys.stderr)
  return injected + list(cli_args)


def configured_sql_runner(
    explicit_runner, profile=None, no_config=False, config_path=None) -> str:
  """Returns the runner for `beam sql`, consulting `config.yaml` if unset."""
  if explicit_runner:
    return explicit_runner
  if _is_disabled(no_config):
    return 'direct'
  path = _default_config_path(config_path)
  if not os.path.isfile(path):
    return 'direct'

  data = _load_config(path)
  active = _select_profile(data, profile)
  raw = data.get('profiles', {}).get(active, {}).get('options',
                                                     {}).get('runner')
  if not raw:
    return 'direct'
  norm = str(raw).strip().lower()
  if norm.endswith('runner') and norm != 'runner':
    norm = norm[:-len('runner')]
  if norm in ('direct', 'dataflow'):
    print(
        f'Using config profile {active!r} ({path}): --runner={norm}',
        file=sys.stderr)
    return norm
  return 'direct'


def _is_disabled(no_config):
  if no_config:
    return True
  return os.environ.get('BEAM_NO_CONFIG',
                        '').strip().lower() in ('1', 'true', 'yes')


def _default_config_path(config_path):
  if config_path:
    return config_path
  env_cfg = os.environ.get('BEAM_CONFIG', '').strip()
  if env_cfg:
    return os.path.expanduser(env_cfg)
  beam_home = os.path.expanduser(os.environ.get('BEAM_HOME', '~/.beam'))
  return os.path.join(beam_home, 'config.yaml')


def _validate_profile_name(name):
  clean = name.strip()
  if not _VALID_PROFILE_RE.match(clean):
    raise ConfigError(f'invalid profile name {name!r}', _EXIT_USAGE)
  return clean


def _select_profile(data, explicit_profile):
  chosen = (
      explicit_profile or os.environ.get('BEAM_PROFILE', '').strip() or
      data.get('active_profile') or 'default')
  return _validate_profile_name(str(chosen))


_DANGEROUS_CONFIG_OPTIONS = frozenset([
    'beam_plugin',
    'beam_plugins',
    'setup_file',
    'extra_package',
    'extra_packages',
    'requirements_file',
    'sdk_location',
    'pickle_library',
])


def _load_config(path) -> dict[str, Any]:
  if not os.path.isfile(path):
    return {
        'active_profile': 'default', 'profiles': {
            'default': {
                'options': {}
            }
        }
    }
  if os.name == 'posix':
    try:
      mode = os.stat(path).st_mode & 0o777
    except OSError:
      mode = 0
    if mode & 0o002:
      raise ConfigError(
          f'refusing world-writable config file {path} (mode {oct(mode)}); '
          f'fix permissions with: chmod 0600 {path}',
          _EXIT_USAGE)
  import yaml
  from . import output as out_mod
  try:
    raw_text = out_mod.read_bounded_text(path, label=path)
    loaded = out_mod.safe_yaml_load(raw_text) or {}
  except (OSError, ValueError, yaml.YAMLError) as e:
    raise ConfigError(f'could not read {path}: {e}', _EXIT_USAGE) from None
  if not isinstance(loaded, dict):
    raise ConfigError(
        f'expected a YAML mapping in {path}, got {type(loaded).__name__}',
        _EXIT_USAGE)
  return loaded


def _save_config(path, data):
  import yaml
  dumper = getattr(yaml, 'CSafeDumper', yaml.SafeDumper)
  parent = os.path.dirname(path)
  if parent:
    os.makedirs(parent, mode=0o700, exist_ok=True)
  tmp_path = f'{path}.{os.getpid()}.part'
  try:
    fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w', encoding='utf-8') as f:
      yaml.dump(data, f, Dumper=dumper, sort_keys=False)
    os.replace(tmp_path, path)
  except OSError as e:
    if os.path.exists(tmp_path):
      os.remove(tmp_path)
    raise ConfigError(f'could not write {path}: {e}', _EXIT_USAGE) from None


def _parse_bool(raw, option_name):
  lowered = raw.strip().lower()
  if lowered in ('true', '1', 'yes'):
    return True
  if lowered in ('false', '0', 'no'):
    return False
  raise ConfigError(
      f'option {option_name!r} expects true or false, got {raw!r}', _EXIT_USAGE)


def _validate_option_key(raw_option: str) -> str:
  stripped = raw_option.strip().lstrip('-')
  if not _VALID_KEY_RE.match(stripped):
    raise ConfigError(f'invalid option name {raw_option!r}', _EXIT_USAGE)
  if _SECRET_KEY_RE.search(stripped):
    raise ConfigError(
        f'refusing to store secret option {stripped!r} in plain-text config; '
        'use credentials providers or environment variables instead',
        _EXIT_USAGE)
  if stripped.replace('-', '_').lower() in _DANGEROUS_CONFIG_OPTIONS:
    raise ConfigError(
        f'refusing to store code/package-loading option {stripped!r} in '
        'default config; pass it explicitly on the command line',
        _EXIT_USAGE)
  return stripped


def _canonical_dest(raw_option, warn_unknown=True):
  stripped = _validate_option_key(raw_option)
  by_dest, by_flag = _option_catalog()
  if stripped in by_flag:
    return by_flag[stripped][0]
  if warn_unknown:
    normalized = stripped.replace('-', '_')
    matches = difflib.get_close_matches(
        normalized, by_dest.keys(), n=1, cutoff=0.75)
    hint = f' (did you mean {matches[0]!r}?)' if matches else ''
    print(
        f'warning: {stripped!r} is not a built-in Beam pipeline option{hint}',
        file=sys.stderr)
  return stripped


def _parse_key_value(raw_option, raw_value):
  stripped = raw_option.strip().lstrip('-')
  dest = _canonical_dest(raw_option, warn_unknown=True)
  by_dest, by_flag = _option_catalog()
  meta = by_dest.get(dest)
  _, flag_bool = by_flag.get(stripped, (dest, None))

  if meta and meta.is_bool:
    parsed = _parse_bool(raw_value, stripped)
    # `beam config set no_use_public_ips true` means `use_public_ips: false`.
    if flag_bool is False:
      parsed = not parsed
    return dest, parsed

  if meta and (meta.is_comma_list or meta.is_append_list):
    items = [item.strip() for item in raw_value.split(',') if item.strip()]
    return dest, items

  return dest, raw_value


def _overridden_dests(cli_args) -> set[str]:
  _, by_flag = _option_catalog()
  overridden: set[str] = set()
  for token in cli_args:
    if not token.startswith('--') or token == '--':
      continue
    flag_name = token[2:].split('=', 1)[0]
    if flag_name in by_flag:
      overridden.add(by_flag[flag_name][0])
    else:
      overridden.add(flag_name)
      overridden.add(flag_name.replace('-', '_'))
  return overridden


def _option_to_flags(key, value, meta: _OptionMeta | None) -> list[str]:
  if isinstance(value, bool) or (meta and meta.is_bool):
    bval = bool(value)
    if bval:
      flag = (meta.true_flag if meta and meta.true_flag else f'--{key}')
      return [flag]
    if meta and meta.false_flag:
      return [meta.false_flag]
    return []

  flag = meta.primary_flag if meta and meta.primary_flag else f'--{key}'
  if isinstance(value, list):
    if meta and meta.is_comma_list:
      return [f'{flag}={",".join(str(v) for v in value)}']
    return [f'{flag}={v}' for v in value]

  return [f'{flag}={value}']
