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


def _patch_yaml_mapping_module(module) -> None:
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


class _PreviewCliFinder(importlib.abc.MetaPathFinder):
  """Redirects `apache_beam.cli.*` and patches Beam 2.75 Arrow/YAML modules."""
  def __init__(self, cli_dir: Path):
    self._cli_dir = cli_dir
    self._patching_arrow = False
    self._patching_yaml_mapping = False

  def find_spec(self, fullname, path, target=None):
    del target
    if fullname in ('apache_beam.cli', _FAST_PKG):
      return importlib.util.spec_from_file_location(
          fullname,
          self._cli_dir / '__init__.py',
          submodule_search_locations=[str(self._cli_dir)])
    for pkg_prefix in ('apache_beam.cli.', f'{_FAST_PKG}.'):
      if fullname.startswith(pkg_prefix):
        rel = fullname[len(pkg_prefix):].replace('.', '/')
        mod_py = self._cli_dir / f'{rel}.py'
        if mod_py.is_file():
          return importlib.util.spec_from_file_location(fullname, mod_py)
    if (fullname == 'apache_beam.typehints.arrow_type_compatibility' and
        not self._patching_arrow):
      return self._wrap_spec(fullname, path, _patch_arrow_module, '_patching_arrow')
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
  if not any(isinstance(f, _PreviewCliFinder) for f in sys.meta_path):
    sys.meta_path.insert(0, _PreviewCliFinder(cli_dir))
  # Also patch if either module was already imported before hooks were installed.
  arrow_mod = sys.modules.get('apache_beam.typehints.arrow_type_compatibility')
  if arrow_mod is not None:
    _patch_arrow_module(arrow_mod)
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
    wheel_dir = Path(tempfile.gettempdir()) / 'beam-cli-cache' / 'wheels' / digest
    wheel_path = wheel_dir / wheel_name
    if wheel_path.is_file():
      return str(wheel_path)
    wheel_dir.mkdir(parents=True, exist_ok=True)

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
