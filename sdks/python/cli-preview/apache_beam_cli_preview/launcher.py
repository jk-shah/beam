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

"""Fast `<90ms` launcher, Beam 2.75 shims & remote Dataflow wheel stager."""

import base64
import hashlib
import importlib
import importlib.abc
import importlib.machinery
import importlib.util
import os
from pathlib import Path
import re
import sys
import tempfile
import zipfile

from . import __version__

_FAST_PKG = '_apache_beam_cli_pkg'
_PTH_CONTENT = (
    'import apache_beam_cli_preview.launcher as _bcp; '
    '_bcp.install_runtime_hooks()\n')


def _locate_cli_dir() -> Path:
  bundled = Path(__file__).resolve().parent / '_bundled_cli'
  if (bundled / 'main.py').is_file():
    return bundled
  repo_cli = Path(__file__).resolve().parents[2] / 'apache_beam' / 'cli'
  if (repo_cli / 'main.py').is_file():
    return repo_cli
  raise RuntimeError(f'could not locate apache_beam/cli sources from {__file__}')


def _patch_arrow_module(module) -> None:
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


class _PreviewCliFinder(importlib.abc.MetaPathFinder):
  """Redirects `apache_beam.cli.*` and patches Beam 2.75 Arrow/YAML/Schema modules."""
  def __init__(self, cli_dir: Path):
    self._cli_dir = cli_dir
    self._patching_arrow = False
    self._patching_yaml_mapping = False
    self._patching_schemas = False

  def find_spec(self, fullname, path, target=None):
    del target
    if fullname in ('apache_beam.cli', _FAST_PKG):
      return importlib.util.spec_from_file_location(
          fullname,
          self._cli_dir / '__init__.py',
          submodule_search_locations=[str(self._cli_dir)])
    for pkg_prefix in ('apache_beam.cli.', f'{_FAST_PKG}.'):
      if fullname.startswith(pkg_prefix):
        sub = fullname[len(pkg_prefix):]
        parts = sub.split('.')
        if not parts or not all(seg.isidentifier() for seg in parts):
          return None
        mod_py = self._cli_dir.joinpath(*parts[:-1], f'{parts[-1]}.py')
        if mod_py.is_file() and not os.path.islink(mod_py):
          return importlib.util.spec_from_file_location(fullname, mod_py)
    if (fullname == 'apache_beam.typehints.arrow_type_compatibility' and
        not self._patching_arrow):
      return self._wrap_spec(fullname, path, _patch_arrow_module, '_patching_arrow')
    if (fullname == 'apache_beam.typehints.schemas' and
        not self._patching_schemas):
      return self._wrap_spec(
          fullname, path, _patch_schemas_module, '_patching_schemas')
    if (fullname == 'apache_beam.yaml.yaml_mapping' and
        not self._patching_yaml_mapping):
      return self._wrap_spec(
          fullname, path, _patch_yaml_mapping_module, '_patching_yaml_mapping')
    return None

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
      def create_module(self, spec):
        if hasattr(orig_loader, 'create_module'):
          return orig_loader.create_module(spec)
        return None

      def exec_module(self, module):
        orig_loader.exec_module(module)
        patch_fn(module)

    orig_spec.loader = _PatchingLoader()
    return orig_spec


def install_runtime_hooks() -> Path:
  cli_dir = _locate_cli_dir()
  sys.meta_path[:] = [
      f for f in sys.meta_path
      if type(f).__name__ != '_PreviewCliFinder'
  ]
  sys.meta_path.insert(0, _PreviewCliFinder(cli_dir))
  # Also patch if any target module was already imported before hooks were installed.
  arrow_mod = sys.modules.get('apache_beam.typehints.arrow_type_compatibility')
  if arrow_mod is not None:
    _patch_arrow_module(arrow_mod)
  schemas_mod = sys.modules.get('apache_beam.typehints.schemas')
  if schemas_mod is not None:
    _patch_schemas_module(schemas_mod)
  yaml_mod = sys.modules.get('apache_beam.yaml.yaml_mapping')
  if yaml_mod is not None:
    _patch_yaml_mapping_module(yaml_mod)
  return cli_dir


def ensure_preview_wheel() -> str:
  """Synthesizes a PEP 427 `py3-none-any.whl` in `<5ms` for Dataflow workers."""
  cli_dir = _locate_cli_dir()
  pkg_dir = Path(__file__).resolve().parent

  entries: list[tuple[str, bytes]] = []
  for name in ('__init__.py', 'launcher.py', '_worker_bootstrap.py', 'demo.py'):
    p = pkg_dir / name
    if p.is_file():
      entries.append((f'apache_beam_cli_preview/{name}', p.read_bytes()))

  for py in sorted(cli_dir.glob('*.py')):
    if not py.name.endswith('_test.py'):
      entries.append((
          f'apache_beam_cli_preview/_bundled_cli/{py.name}',
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
  wheel_dir = cache_root / 'wheels' / digest
  wheel_name = f'apache_beam_cli_preview-{__version__}-py3-none-any.whl'
  wheel_path = wheel_dir / wheel_name
  if wheel_path.is_file():
    return str(wheel_path)
  try:
    wheel_dir.mkdir(parents=True, exist_ok=True)
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


def maybe_inject_dataflow_worker_args(pipeline_args: list[str]) -> list[str]:
  """Stages the preview `.whl` + worker bootstrap plugin onto Dataflow VMs."""
  out = list(pipeline_args)
  has_preview_pkg = any(
      'apache_beam_cli_preview' in a for a in out)
  if not has_preview_pkg:
    whl = ensure_preview_wheel()
    out.append(f'--extra_package={whl}')
  plugin_flag = '--beam_plugin=apache_beam_cli_preview._worker_bootstrap'
  if plugin_flag not in out:
    out.append(plugin_flag)
  return out


def main(argv=None) -> int:
  cli_dir = install_runtime_hooks()
  if 'apache_beam' not in sys.modules and _FAST_PKG not in sys.modules:
    spec = importlib.util.spec_from_file_location(
        _FAST_PKG,
        cli_dir / '__init__.py',
        submodule_search_locations=[str(cli_dir)])
    if spec is not None and spec.loader is not None:
      pkg = importlib.util.module_from_spec(spec)
      sys.modules[_FAST_PKG] = pkg
      spec.loader.exec_module(pkg)
  target = (
      f'{_FAST_PKG}.main'
      if _FAST_PKG in sys.modules else 'apache_beam.cli.main')
  return importlib.import_module(target).main(argv)


if __name__ == '__main__':
  sys.exit(main())
