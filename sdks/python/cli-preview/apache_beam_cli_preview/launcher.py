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

"""Fast `<90ms` launcher, Beam 2.75 shims & remote worker wheel stager."""

import base64
import hashlib
import importlib
import importlib.abc
import importlib.machinery
import importlib.util
import os
import re
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from . import __version__

_FAST_PKG = '_apache_beam_cli_pkg'
_CANON_PKG = 'apache_beam.cli'
_PTH_CONTENT = (
    'import apache_beam_cli_preview.launcher as _bcp; '
    '_bcp.install_runtime_hooks(strict=False)\n')
_REMOTE_RUNNERS = frozenset({
    'dataflow',
    'testdataflow',
    'flink',
    'spark',
    'portable',
})
_EXTERNAL_ENV_TYPES = frozenset({'DOCKER', 'PROCESS', 'EXTERNAL'})


def _locate_cli_dir() -> Path:
  env_dir = os.environ.get('BEAM_CLI_DIR')
  if env_dir:
    resolved_env = Path(env_dir).expanduser().resolve()
    if (resolved_env / 'main.py').is_file():
      return resolved_env
  bundled = (Path(__file__).resolve().parent / '_bundled_cli').resolve()
  if (bundled / 'main.py').is_file():
    return bundled
  repo_cli = (
      Path(__file__).resolve().parents[2] / 'apache_beam' / 'cli').resolve()
  if (repo_cli / 'main.py').is_file():
    return repo_cli
  raise RuntimeError(f'could not locate apache_beam/cli sources from {__file__}')


def _is_truthy_env(var_name: str) -> bool:
  return os.environ.get(var_name, '').strip().lower() in ('1', 'true', 'yes')


def _core_beam_cli_dir() -> Path | None:
  """Returns core `apache_beam/cli` path if installed core Beam ships it."""
  ab_mod = sys.modules.get('apache_beam')
  if ab_mod is not None:
    search_locs = list(getattr(ab_mod, '__path__', ()) or ())
  else:
    search_paths = [p for p in sys.path if p]
    try:
      ab_spec = importlib.machinery.PathFinder.find_spec(
          'apache_beam', search_paths)
    except Exception:  # pylint: disable=broad-except
      ab_spec = None
    search_locs = (
        list(ab_spec.submodule_search_locations or ())
        if ab_spec is not None else [])
  for loc in search_locs:
    cand = Path(loc) / 'cli'
    if (cand / '__init__.py').is_file():
      return cand.resolve()
  return None


def _patch_arrow_module(module) -> None:
  """Shims apache/beam#41042 (merged in #41045) on Beam <= 2.78 wheels."""
  conv = getattr(module, 'PyarrowBatchConverter', None)
  if conv is not None and not getattr(conv, '_cli_preview_patched', False):
    orig_produce = conv.produce_batch

    def _safe_produce_batch(self_conv, elements):
      if not isinstance(elements, (list, tuple)):
        elements = list(elements)
      return orig_produce(self_conv, elements)

    conv.produce_batch = _safe_produce_batch
    conv._cli_preview_patched = True


def _make_arbitrary_field_row_tuple(type_name: str, subfields) -> type:
  import operator
  field_names = tuple(name for name, _ in subfields)
  field_index = {name: idx for idx, name in enumerate(field_names)}
  num_fields = len(field_names)

  def _new(cls, *args, **kwargs):
    if args and kwargs:
      if len(args) + len(kwargs) != num_fields:
        raise TypeError(
            f'{type_name}() expected {num_fields} arguments, '
            f'got {len(args) + len(kwargs)}')
      vals = list(args) + [None] * (num_fields - len(args))
      for k, v in kwargs.items():
        idx = field_index.get(k)
        if idx is None or idx < len(args):
          raise TypeError(f'{type_name}() got unexpected keyword {k!r}')
        vals[idx] = v
      return tuple.__new__(cls, vals)
    if kwargs:
      if len(kwargs) != num_fields or set(kwargs) != set(field_names):
        raise TypeError(
            f'{type_name}() keyword mismatch: expected {field_names}')
      return tuple.__new__(cls, (kwargs[name] for name in field_names))
    if len(args) != num_fields:
      raise TypeError(
          f'{type_name}() expected {num_fields} arguments, got {len(args)}')
    return tuple.__new__(cls, args)

  ns = {
      '__slots__': (),
      '_fields': field_names,
      '__annotations__': dict(subfields),
      '__new__': _new,
      '_asdict': lambda self: dict(zip(field_names, self)),
      '_replace': lambda self, **kw: type(self)(
          *(kw.get(f, v) for f, v in zip(field_names, self))),
      '__repr__': lambda self: (
          f'{type_name}(' +
          ', '.join(f'{f}={v!r}' for f, v in zip(field_names, self)) + ')'),
  }
  cls = type(type_name, (tuple, ), ns)
  for idx, name in enumerate(field_names):
    if name not in ('count', 'index') and not name.startswith('__'):
      setattr(cls, name, property(operator.itemgetter(idx)))
  return cls


def _patch_schemas_module(module) -> None:
  """Shims apache/beam#40698 (keyword / non-identifier Python Row columns)."""
  st_cls = getattr(module, 'SchemaTranslation', None)
  if st_cls is None or getattr(st_cls, '_cli_preview_patched', False):
    return
  orig_nt = st_cls.named_tuple_from_schema

  def _safe_named_tuple_from_schema(self_st, schema):
    try:
      return orig_nt(self_st, schema)
    except (ValueError, TypeError, SyntaxError):
      from apache_beam import coders
      from apache_beam.typehints import row_type
      type_name = 'BeamSchema_{}'.format(schema.id.replace('-', '_'))
      if schema.id in self_st.schema_registry.by_id:
        return self_st.schema_registry.by_id[schema.id][0]
      subfields = []
      descriptions = {}
      for field in schema.fields:
        field_py_type = self_st.typing_from_runner_api(field.type)
        if isinstance(field_py_type, row_type.RowTypeConstraint):
          field_py_type = field_py_type.user_type
        descriptions[field.name] = field.description
        subfields.append((field.name, field_py_type))
      user_type = _make_arbitrary_field_row_tuple(type_name, subfields)
      setattr(
          user_type,
          '__reduce__',
          module._named_tuple_reduce_method(schema.SerializeToString()))
      setattr(user_type, '_field_descriptions', descriptions)
      setattr(user_type, row_type._BEAM_SCHEMA_ID, schema.id)
      self_st.schema_registry.add(user_type, schema)
      coders.registry.register_coder(user_type, coders.RowCoder)
      return user_type

  st_cls.named_tuple_from_schema = _safe_named_tuple_from_schema
  st_cls._cli_preview_patched = True


def _patch_yaml_mapping_module(module) -> None:
  """Shims apache/beam#41043 (merged in #41044) and apache/beam#41077."""
  import keyword
  orig_validate = getattr(module, 'validate_generic_expression', None)
  is_literal = getattr(module, 'is_literal', None)
  if (orig_validate is not None and is_literal is not None and
      not getattr(orig_validate, '_cli_preview_patched', False)):

    def _patched_validate(expr_dict, input_fields, allow_cmp, error_field):
      try:
        return orig_validate(expr_dict, input_fields, allow_cmp, error_field)
      except ValueError:
        if (allow_cmp and isinstance(expr_dict, dict) and
            len(expr_dict) == 1 and 'expression' in expr_dict):
          expr = str(expr_dict['expression'])
          m = re.fullmatch(r'(.*?)([<>=!]+)(.*)', expr)
          if m:
            left, cmp_op, right = m.groups()
            is_atom = lambda s: is_literal(s) or s in input_fields
            if (is_atom(left.strip()) and is_atom(right.strip()) and
                cmp_op in {'==', '<=', '>=', '<', '>', '!='}):
              return None
        raise

    _patched_validate._cli_preview_patched = True
    module.validate_generic_expression = _patched_validate

  orig_expand = getattr(module, '_expand_python_mapping_func', None)
  py_callable_mod = getattr(module, 'python_callable', None)
  if (orig_expand is not None and py_callable_mod is not None and
      not getattr(orig_expand, '_cli_preview_patched', False)):

    def _patched_expand_py(
        original_fields, expression=None, callable=None, path=None, name=None):
      if expression and not (path and name):
        if expression in original_fields:
          src = f'def fn(__row__):\n  return getattr(__row__, {expression!r})'
          return py_callable_mod.PythonCallableWithSource(src)
        safe_fields = [
            f for f in original_fields
            if f.isidentifier() and not keyword.iskeyword(f)
        ]
        return orig_expand(
            safe_fields,
            expression=expression,
            callable=callable,
            path=path,
            name=name)
      return orig_expand(
          original_fields,
          expression=expression,
          callable=callable,
          path=path,
          name=name)

    _patched_expand_py._cli_preview_patched = True
    module._expand_python_mapping_func = _patched_expand_py


def _patch_opcodes_module(module) -> None:
  """Resolves nested NamedTuple and Optional[Row] attrs (apache/beam#41099)."""
  orig = getattr(module, '_getattr', None)
  if orig is None or getattr(orig, '_cli_preview_patched', False):
    return

  def _getattr(o, name):
    import inspect
    from apache_beam.typehints import native_type_compatibility as ntc
    from apache_beam.typehints import row_type
    from apache_beam.typehints import typehints

    result = orig(o, name)
    if result is not typehints.Any:
      return result

    def _row_type_of(t):
      if isinstance(t, row_type.RowTypeConstraint):
        return t
      if inspect.isclass(t) and ntc.match_is_named_tuple(t):
        return row_type.RowTypeConstraint.from_user_type(t)
      return None

    if isinstance(o, row_type.RowTypeConstraint) or (
        inspect.isclass(o) and ntc.match_is_named_tuple(o)):
      row = _row_type_of(o)
      nullable = False
    else:
      beam_t = ntc.convert_to_beam_type(o)
      row = None
      if isinstance(beam_t, typehints.UnionConstraint):
        non_none = [u for u in beam_t.union_types if u is not type(None)]
        if len(non_none) == 1:
          row = _row_type_of(non_none[0])
      nullable = True
    if row is None or name not in dict(row._fields):
      return result
    field_type = row.get_type_for(name)
    if not nullable:
      return field_type
    return typehints.Optional[ntc.convert_to_beam_type(field_type)]

  _getattr._cli_preview_patched = True
  module._getattr = _getattr


def _rewrite_module_member_origins(module: Any, canon_name: str) -> None:
  mod_dict = getattr(module, '__dict__', None)
  if not isinstance(mod_dict, dict):
    return
  for obj in list(mod_dict.values()):
    orig_mod = getattr(obj, '__module__', None)
    if isinstance(orig_mod, str) and (
        orig_mod == _FAST_PKG or orig_mod.startswith(f'{_FAST_PKG}.')):
      target_mod = (
          _CANON_PKG + orig_mod[len(_FAST_PKG):]
          if orig_mod != _FAST_PKG else canon_name)
      try:
        obj.__module__ = target_mod
      except (AttributeError, TypeError):
        pass


def canonicalize_cli_module_aliases() -> None:
  """Syncs `_apache_beam_cli_pkg.*` <-> `apache_beam.cli.*` module objects."""
  beam_loaded = 'apache_beam' in sys.modules
  for mod_name, mod in list(sys.modules.items()):
    if mod is None:
      continue
    if mod_name == _FAST_PKG or mod_name.startswith(f'{_FAST_PKG}.'):
      suffix = mod_name[len(_FAST_PKG):]
      canon_name = _CANON_PKG + suffix
      existing_canon = sys.modules.get(canon_name)
      if existing_canon is not None and existing_canon is not mod:
        # Keep the active fast-loaded module authoritative so state matches.
        sys.modules[canon_name] = mod
      else:
        sys.modules[canon_name] = mod
      try:
        mod.__name__ = canon_name
      except (AttributeError, TypeError):
        pass
      if beam_loaded:
        is_pkg = hasattr(mod, '__path__')
        try:
          mod.__package__ = (
              canon_name if is_pkg else canon_name.rpartition('.')[0])
        except (AttributeError, TypeError):
          pass
      _rewrite_module_member_origins(mod, canon_name)
    elif mod_name == _CANON_PKG or mod_name.startswith(f'{_CANON_PKG}.'):
      suffix = mod_name[len(_CANON_PKG):]
      fast_name = _FAST_PKG + suffix
      sys.modules.setdefault(fast_name, mod)
      _rewrite_module_member_origins(mod, mod_name)

  if beam_loaded:
    ab_mod = sys.modules.get('apache_beam')
    cli_pkg = sys.modules.get(_CANON_PKG)
    if ab_mod is not None and cli_pkg is not None:
      setattr(ab_mod, 'cli', cli_pkg)
      for mod_name, mod in list(sys.modules.items()):
        if mod is not None and mod_name.startswith(f'{_CANON_PKG}.'):
          rel = mod_name[len(_CANON_PKG) + 1:]
          if '.' not in rel and rel.isidentifier():
            setattr(cli_pkg, rel, mod)


class _PreviewCliFinder(importlib.abc.MetaPathFinder):
  """Redirects `apache_beam.cli.*` and patches Beam 2.75 runtime modules."""
  def __init__(self, cli_dir: Path):
    self._cli_dir = cli_dir
    self._patching_arrow = False
    self._patching_yaml_mapping = False
    self._patching_schemas = False
    self._patching_opcodes = False

  def find_spec(self, fullname, path, target=None):
    del target
    if fullname in (_CANON_PKG, _FAST_PKG):
      counterpart = _FAST_PKG if fullname == _CANON_PKG else _CANON_PKG
      existing = sys.modules.get(counterpart)
      init_py = self._cli_dir / '__init__.py'
      if existing is not None:
        return self._alias_spec(
            fullname, _CANON_PKG, _FAST_PKG, existing, init_py, is_pkg=True)
      return self._cli_file_spec(
          fullname, _CANON_PKG, _FAST_PKG, init_py, is_pkg=True)

    for pkg_prefix in (f'{_CANON_PKG}.', f'{_FAST_PKG}.'):
      if fullname.startswith(pkg_prefix):
        sub = fullname[len(pkg_prefix):]
        parts = sub.split('.')
        if not parts or not all(seg.isidentifier() for seg in parts):
          return None
        canon_name = f'{_CANON_PKG}.{sub}'
        fast_name = f'{_FAST_PKG}.{sub}'
        counterpart = fast_name if fullname == canon_name else canon_name
        existing = sys.modules.get(counterpart)

        sub_pkg_dir = self._cli_dir.joinpath(*parts)
        sub_pkg_init = sub_pkg_dir / '__init__.py'
        if sub_pkg_init.is_file():
          if existing is not None:
            return self._alias_spec(
                fullname,
                canon_name,
                fast_name,
                existing,
                sub_pkg_init,
                is_pkg=True)
          return self._cli_file_spec(
              fullname, canon_name, fast_name, sub_pkg_init, is_pkg=True)

        mod_py = self._cli_dir.joinpath(*parts[:-1], f'{parts[-1]}.py')
        if mod_py.is_file():
          if existing is not None:
            return self._alias_spec(
                fullname,
                canon_name,
                fast_name,
                existing,
                mod_py,
                is_pkg=False)
          return self._cli_file_spec(
              fullname, canon_name, fast_name, mod_py, is_pkg=False)

    if (fullname == 'apache_beam.typehints.arrow_type_compatibility' and
        not self._patching_arrow):
      return self._wrap_spec(
          fullname, path, _patch_arrow_module, '_patching_arrow')
    if (fullname == 'apache_beam.typehints.schemas' and
        not self._patching_schemas):
      return self._wrap_spec(
          fullname, path, _patch_schemas_module, '_patching_schemas')
    if (fullname == 'apache_beam.yaml.yaml_mapping' and
        not self._patching_yaml_mapping):
      return self._wrap_spec(
          fullname, path, _patch_yaml_mapping_module, '_patching_yaml_mapping')
    if (fullname == 'apache_beam.typehints.opcodes' and
        not self._patching_opcodes):
      return self._wrap_spec(
          fullname, path, _patch_opcodes_module, '_patching_opcodes')
    return None

  def _alias_spec(
      self,
      fullname: str,
      canon_name: str,
      fast_name: str,
      existing_mod: Any,
      origin_path: Path,
      *,
      is_pkg: bool,
  ):
    orig_loader = getattr(existing_mod, '__loader__', None)

    class _AliasLoader(importlib.abc.Loader):
      def __init__(self, wrapped):
        self._orig_loader = wrapped

      def create_module(self, spec):
        del spec
        return existing_mod

      def exec_module(self, module):
        sys.modules[canon_name] = module
        sys.modules[fast_name] = module
        try:
          module.__name__ = canon_name
        except (AttributeError, TypeError):
          pass
        if 'apache_beam' in sys.modules:
          try:
            module.__package__ = (
                canon_name if is_pkg else canon_name.rpartition('.')[0])
          except (AttributeError, TypeError):
            pass
        _rewrite_module_member_origins(module, canon_name)

      def __getattr__(self, attr):
        if self._orig_loader is not None:
          return getattr(self._orig_loader, attr)
        raise AttributeError(attr)

    spec = importlib.machinery.ModuleSpec(
        fullname,
        _AliasLoader(orig_loader),
        origin=str(origin_path),
        is_package=is_pkg)
    if is_pkg:
      spec.submodule_search_locations = [str(origin_path.parent)]
    return spec

  def _cli_file_spec(
      self,
      fullname: str,
      canon_name: str,
      fast_name: str,
      file_path: Path,
      *,
      is_pkg: bool,
  ):
    search_locs = [str(file_path.parent)] if is_pkg else None
    spec = importlib.util.spec_from_file_location(
        fullname, file_path, submodule_search_locations=search_locs)
    if spec is None or spec.loader is None:
      return None
    orig_loader = spec.loader

    class _CliSourceLoader(importlib.abc.Loader):
      def __init__(self, wrapped):
        self._orig_loader = wrapped

      def create_module(self, mod_spec):
        if hasattr(self._orig_loader, 'create_module'):
          return self._orig_loader.create_module(mod_spec)
        return None

      def exec_module(self, module):
        self._orig_loader.exec_module(module)
        sys.modules[fast_name] = module
        sys.modules[canon_name] = module
        try:
          module.__name__ = canon_name
        except (AttributeError, TypeError):
          pass
        if 'apache_beam' in sys.modules:
          try:
            module.__package__ = (
                canon_name if is_pkg else canon_name.rpartition('.')[0])
          except (AttributeError, TypeError):
            pass
        _rewrite_module_member_origins(module, canon_name)

      def __getattr__(self, attr):
        return getattr(self._orig_loader, attr)

    spec.loader = _CliSourceLoader(orig_loader)
    return spec

  def _wrap_spec(self, fullname: str, path, patch_fn, guard_attr: str):
    setattr(self, guard_attr, True)
    try:
      orig_spec = importlib.machinery.PathFinder.find_spec(fullname, path)
    finally:
      setattr(self, guard_attr, False)
    if orig_spec is None or orig_spec.loader is None:
      return None
    orig_loader = orig_spec.loader

    class _PatchingLoader(importlib.abc.Loader):
      def __init__(self, wrapped):
        self._orig_loader = wrapped

      def create_module(self, spec):
        if hasattr(self._orig_loader, 'create_module'):
          return self._orig_loader.create_module(spec)
        return None

      def exec_module(self, module):
        self._orig_loader.exec_module(module)
        patch_fn(module)

      def __getattr__(self, attr):
        return getattr(self._orig_loader, attr)

    orig_spec.loader = _PatchingLoader(orig_loader)
    return orig_spec


def install_runtime_hooks(*, strict: bool = True) -> Path | None:
  force_preview = (
      bool(os.environ.get('BEAM_CLI_DIR')) or
      _is_truthy_env('BEAM_CLI_FORCE_PREVIEW'))
  if not force_preview:
    core_cli = _core_beam_cli_dir()
    if core_cli is not None:
      sys.meta_path[:] = [
          f for f in sys.meta_path
          if type(f).__name__ != '_PreviewCliFinder'
      ]
      canonicalize_cli_module_aliases()
      return core_cli

  try:
    cli_dir = _locate_cli_dir()
  except (RuntimeError, OSError):
    if strict:
      raise
    return None

  sys.meta_path[:] = [
      f for f in sys.meta_path
      if type(f).__name__ != '_PreviewCliFinder'
  ]
  sys.meta_path.insert(0, _PreviewCliFinder(cli_dir))
  # Also patch if any target module was already imported before hooks ran.
  arrow_mod = sys.modules.get('apache_beam.typehints.arrow_type_compatibility')
  if arrow_mod is not None:
    _patch_arrow_module(arrow_mod)
  schemas_mod = sys.modules.get('apache_beam.typehints.schemas')
  if schemas_mod is not None:
    _patch_schemas_module(schemas_mod)
  yaml_mod = sys.modules.get('apache_beam.yaml.yaml_mapping')
  if yaml_mod is not None:
    _patch_yaml_mapping_module(yaml_mod)
  opcodes_mod = sys.modules.get('apache_beam.typehints.opcodes')
  if opcodes_mod is not None:
    _patch_opcodes_module(opcodes_mod)
  canonicalize_cli_module_aliases()
  return cli_dir


def ensure_preview_wheel() -> str:
  """Synthesizes a PEP 427 `py3-none-any.whl` in `<5ms` for remote workers."""
  cli_dir = _locate_cli_dir()
  pkg_dir = Path(__file__).resolve().parent

  entries: list[tuple[str, bytes]] = []
  for name in ('__init__.py', 'launcher.py', '_worker_bootstrap.py', 'demo.py'):
    p = pkg_dir / name
    if p.is_file():
      entries.append((f'apache_beam_cli_preview/{name}', p.read_bytes()))

  for py in sorted(cli_dir.rglob('*.py')):
    if not py.name.endswith('_test.py') and '__pycache__' not in py.parts:
      rel = py.relative_to(cli_dir).as_posix()
      entries.append((
          f'apache_beam_cli_preview/_bundled_cli/{rel}',
          py.read_bytes(),
      ))

  entries.append(('apache_beam_cli_preview.pth', _PTH_CONTENT.encode('utf-8')))

  tree_hasher = hashlib.sha256()
  for arcname, data in entries:
    tree_hasher.update(arcname.encode('utf-8'))
    tree_hasher.update(data)
  digest = tree_hasher.hexdigest()[:12]

  explicit_cache = os.environ.get('BEAM_CACHE_DIR')
  beam_home_env = os.environ.get('BEAM_HOME')
  if explicit_cache:
    cache_root = Path(explicit_cache)
  elif beam_home_env:
    cache_root = Path(os.path.expanduser(beam_home_env)) / 'cache'
  else:
    cache_root = Path.home() / '.beam' / 'cache'
  if os.path.islink(cache_root):
    raise RuntimeError(
        f'refusing symlinked cache directory {str(cache_root)!r}')
  wheel_dir = cache_root / 'wheels' / digest
  wheel_name = f'apache_beam_cli_preview-{__version__}-py3-none-any.whl'
  wheel_path = wheel_dir / wheel_name
  if wheel_path.is_file() and not os.path.islink(wheel_path):
    return str(wheel_path)
  try:
    wheel_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
  except OSError:
    if explicit_cache:
      raise
    uid_suffix = f'-{os.getuid()}' if hasattr(os, 'getuid') else ''
    user_tmp_root = Path(tempfile.gettempdir()) / f'beam-cli-cache{uid_suffix}'
    user_tmp_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if os.path.islink(user_tmp_root):
      raise RuntimeError(
          f'refusing symlinked cache directory {str(user_tmp_root)!r}')
    if hasattr(os, 'getuid'):
      st = os.lstat(user_tmp_root)
      if st.st_uid != os.getuid():
        raise RuntimeError(
            f'refusing foreign-owned cache directory {str(user_tmp_root)!r}')
      try:
        os.chmod(user_tmp_root, 0o700)
      except OSError:
        pass
    wheel_dir = user_tmp_root / 'wheels' / digest
    wheel_path = wheel_dir / wheel_name
    if wheel_path.is_file() and not os.path.islink(wheel_path):
      return str(wheel_path)
    wheel_dir.mkdir(mode=0o700, parents=True, exist_ok=True)

  dist_info = f'apache_beam_cli_preview-{__version__}.dist-info'
  metadata_bytes = (
      'Metadata-Version: 2.1\n'
      'Name: apache-beam-cli-preview\n'
      f'Version: {__version__}\n'
      'Summary: Pure-Python preview overlay for Apache Beam CLI\n'
  ).encode('utf-8')
  wheel_meta_bytes = (
      'Wheel-Version: 1.0\n'
      'Generator: apache-beam-cli-preview\n'
      'Root-Is-Purelib: true\n'
      'Tag: py3-none-any\n'
  ).encode('utf-8')
  top_level_bytes = b'apache_beam_cli_preview\n'
  entry_points_bytes = (
      b'[console_scripts]\n'
      b'beam = apache_beam_cli_preview.launcher:main\n'
      b'apache-beam = apache_beam_cli_preview.launcher:main\n'
      b'beam-demo = apache_beam_cli_preview.demo:main\n'
  )

  all_files = entries + [
      (f'{dist_info}/METADATA', metadata_bytes),
      (f'{dist_info}/WHEEL', wheel_meta_bytes),
      (f'{dist_info}/top_level.txt', top_level_bytes),
      (f'{dist_info}/entry_points.txt', entry_points_bytes),
  ]

  record_lines: list[str] = []
  for arcname, data in all_files:
    b64 = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(
        b'=').decode('ascii')
    record_lines.append(f'{arcname},sha256={b64},{len(data)}')
  record_path = f'{dist_info}/RECORD'
  record_lines.append(f'{record_path},,')
  record_bytes = ('\n'.join(record_lines) + '\n').encode('utf-8')

  fd, tmp_whl = tempfile.mkstemp(
      prefix='.tmp-', suffix='.whl', dir=str(wheel_dir))
  os.close(fd)
  try:
    with zipfile.ZipFile(tmp_whl, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
      for arcname, data in all_files:
        zf.writestr(arcname, data)
      zf.writestr(record_path, record_bytes)
    os.replace(tmp_whl, wheel_path)
  finally:
    if os.path.exists(tmp_whl):
      try:
        os.unlink(tmp_whl)
      except OSError:
        pass
  return str(wheel_path)


def _normalize_runner_name(raw_runner: str | None) -> str:
  name = str(raw_runner or 'DirectRunner').strip().rsplit('.', 1)[-1].lower()
  if name.endswith('runner') and name != 'runner':
    name = name[:-len('runner')]
  return name


def _extract_flag_from_argv(
    argv: list[str], flag_names: tuple[str, ...]) -> str | None:
  found: str | None = None
  i = 0
  while i < len(argv):
    tok = argv[i]
    if tok == '--':
      break
    matched_eq = False
    for flg in flag_names:
      if tok.startswith(flg + '='):
        found = tok.split('=', 1)[1]
        matched_eq = True
        break
    if matched_eq:
      i += 1
      continue
    if tok in flag_names and i + 1 < len(argv):
      found = argv[i + 1]
      i += 2
      continue
    i += 1
  return found


def _is_non_in_process_runner(
    runner: str | None, env_type: str | None) -> bool:
  norm_runner = _normalize_runner_name(runner)
  if norm_runner in _REMOTE_RUNNERS:
    return True
  norm_env = str(env_type or '').strip().upper()
  if norm_runner in ('prism', 'direct', 'switchingdirect', '') and (
      norm_env in _EXTERNAL_ENV_TYPES):
    return True
  return False


def maybe_inject_dataflow_worker_args(pipeline_args: list[str]) -> list[str]:
  """Stages the preview `.whl` + worker bootstrap plugin onto remote workers."""
  skip_flags = {'--no-stage-cli-wheel', '--no_stage_cli_wheel'}
  explicit_skip = any(a in skip_flags for a in pipeline_args)
  out = [a for a in pipeline_args if a not in skip_flags]
  env_skip = _is_truthy_env('BEAM_NO_STAGE_CLI_WHEEL')
  if explicit_skip or env_skip:
    return out
  has_preview_pkg = any('apache_beam_cli_preview' in a for a in out)
  if not has_preview_pkg:
    whl = ensure_preview_wheel()
    out.append(f'--extra_package={whl}')
  plugin_flag = '--beam_plugin=apache_beam_cli_preview._worker_bootstrap'
  if plugin_flag not in out:
    out.append(plugin_flag)
  return out


def maybe_inject_remote_worker_args(
    argv: list[str], runner: str | None = None) -> list[str]:
  """Stages the preview `.whl` + plugin for any non-in-process runner."""
  skip_flags = {'--no-stage-cli-wheel', '--no_stage_cli_wheel'}
  explicit_skip = any(a in skip_flags for a in argv)
  out = [a for a in argv if a not in skip_flags]
  if explicit_skip or _is_truthy_env('BEAM_NO_STAGE_CLI_WHEEL'):
    return out
  eff_runner = runner or _extract_flag_from_argv(out, ('--runner', '-r'))
  env_type = _extract_flag_from_argv(
      out, ('--environment_type', '--environment-type'))
  if not _is_non_in_process_runner(eff_runner, env_type):
    return out
  return maybe_inject_dataflow_worker_args(out)


def stage_preview_wheel_into_options(options: Any) -> None:
  """Stages `--extra_package` + `--beam_plugin` into a `PipelineOptions` object."""
  if options is None or _is_truthy_env('BEAM_NO_STAGE_CLI_WHEEL'):
    return
  plugin_mod = 'apache_beam_cli_preview._worker_bootstrap'

  if isinstance(options, dict):
    if options.get('no_stage_cli_wheel'):
      return
    runner_val = options.get('runner')
    env_val = options.get('environment_type')
    if not _is_non_in_process_runner(runner_val, env_val):
      return
    whl = ensure_preview_wheel()
    pkgs = list(options.get('extra_packages') or options.get('extra_package') or [])
    if isinstance(pkgs, str):
      pkgs = [pkgs]
    if not any('apache_beam_cli_preview' in str(p) for p in pkgs):
      pkgs.append(whl)
      options['extra_packages'] = pkgs
    plugins = list(options.get('beam_plugins') or options.get('beam_plugin') or [])
    if isinstance(plugins, str):
      plugins = [plugins]
    if plugin_mod not in plugins:
      plugins.append(plugin_mod)
      options['beam_plugins'] = plugins
    return

  raw_flags = getattr(options, '_flags', None)
  if isinstance(raw_flags, list):
    skip_flags = {'--no-stage-cli-wheel', '--no_stage_cli_wheel'}
    if any(f in skip_flags for f in raw_flags):
      raw_flags[:] = [f for f in raw_flags if f not in skip_flags]
      return

  all_opts = (
      options.get_all_options() if hasattr(options, 'get_all_options') else {})
  if all_opts.get('no_stage_cli_wheel'):
    return

  runner_val = all_opts.get('runner')
  env_val = all_opts.get('environment_type')
  if hasattr(options, 'view_as'):
    from apache_beam.options.pipeline_options import PortableOptions
    from apache_beam.options.pipeline_options import SetupOptions
    from apache_beam.options.pipeline_options import StandardOptions
    runner_val = runner_val or options.view_as(StandardOptions).runner
    env_val = env_val or options.view_as(PortableOptions).environment_type
    if not _is_non_in_process_runner(runner_val, env_val):
      return
    whl = ensure_preview_wheel()
    setup_opts = options.view_as(SetupOptions)
    cur_pkgs = list(setup_opts.extra_packages or [])
    if not any('apache_beam_cli_preview' in str(p) for p in cur_pkgs):
      cur_pkgs.append(whl)
      setup_opts.extra_packages = cur_pkgs
    cur_plugins = list(setup_opts.beam_plugins or [])
    if plugin_mod not in cur_plugins:
      cur_plugins.append(plugin_mod)
      setup_opts.beam_plugins = cur_plugins


def main(argv=None) -> int:
  os.environ.setdefault('GRPC_VERBOSITY', 'ERROR')
  os.environ.setdefault('GRPC_ENABLE_FORK_SUPPORT', '0')
  cli_dir = install_runtime_hooks()
  if (cli_dir is not None and 'apache_beam' not in sys.modules and
      _FAST_PKG not in sys.modules):
    spec = importlib.util.spec_from_file_location(
        _FAST_PKG,
        cli_dir / '__init__.py',
        submodule_search_locations=[str(cli_dir)])
    if spec is not None and spec.loader is not None:
      pkg = importlib.util.module_from_spec(spec)
      sys.modules[_FAST_PKG] = pkg
      spec.loader.exec_module(pkg)
      sys.modules.setdefault(_CANON_PKG, pkg)
  target = (
      f'{_FAST_PKG}.main'
      if _FAST_PKG in sys.modules else f'{_CANON_PKG}.main')
  return importlib.import_module(target).main(argv)


if __name__ == '__main__':
  sys.exit(main())
