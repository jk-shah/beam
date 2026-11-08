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

"""Fast `<90ms` launcher + runtime wheel shim for `apache-beam-cli-preview`.

1. Maps `apache_beam.cli` onto the preview `cli` directory via a lightweight
   `MetaPathFinder` so fast commands (`--help`, `--version`, `--llms-txt`,
   `io`, `doctor`, `check`, `config`, local `inspect`) never import
   `apache_beam` (`<50–90ms`), while full pipeline runs (`pipe`, `run`, `diff`)
   seamlessly resolve `apache_beam.cli.dlq.*` providers.
2. When `apache_beam` (`<=2.75.x`) finally loads for a pipeline run, applies
   the one-line Fix 0a shim on `PyarrowBatchConverter.produce_batch`
   (Apache Beam PR #41044) so multi-column `WriteToParquet` works out of the
   box on released wheels without recompiling Cython!
"""

import importlib
import importlib.abc
import importlib.util
from pathlib import Path
import sys

_FAST_PKG = '_apache_beam_cli_pkg'


def _locate_cli_dir() -> Path:
  bundled = Path(__file__).resolve().parent / '_bundled_cli'
  if (bundled / 'main.py').is_file():
    return bundled
  # Editable checkout layout: `sdks/python/cli-preview/apache_beam_cli_preview`
  repo_cli = Path(__file__).resolve().parents[2] / 'apache_beam' / 'cli'
  if (repo_cli / 'main.py').is_file():
    return repo_cli
  raise RuntimeError(f'could not locate apache_beam/cli sources from {__file__}')


class _PreviewCliFinder(importlib.abc.MetaPathFinder):
  """Redirects `apache_beam.cli.*` and patches `arrow_type_compatibility`."""
  def __init__(self, cli_dir: Path):
    self._cli_dir = cli_dir
    self._patching_arrow = False

  def find_spec(self, fullname, path, target=None):
    del path, target
    if fullname == 'apache_beam.cli':
      return importlib.util.spec_from_file_location(
          fullname,
          self._cli_dir / '__init__.py',
          submodule_search_locations=[str(self._cli_dir)])
    if fullname.startswith('apache_beam.cli.'):
      rel = fullname[len('apache_beam.cli.'):].replace('.', '/')
      mod_py = self._cli_dir / f'{rel}.py'
      if mod_py.is_file():
        return importlib.util.spec_from_file_location(fullname, mod_py)
    if (fullname == 'apache_beam.typehints.arrow_type_compatibility' and
        not self._patching_arrow):
      return self._wrap_arrow_spec(fullname)
    return None

  def _wrap_arrow_spec(self, fullname: str):
    self._patching_arrow = True
    try:
      orig_spec = importlib.machinery.PathFinder.find_spec(fullname, sys.path)
    finally:
      self._patching_arrow = False
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
        conv = getattr(module, 'PyarrowBatchConverter', None)
        if conv is not None and not getattr(conv, '_cli_preview_patched', False):
          orig_produce = conv.produce_batch

          def _safe_produce_batch(self_conv, elements):
            if not isinstance(elements, (list, tuple)):
              elements = list(elements)
            return orig_produce(self_conv, elements)

          conv.produce_batch = _safe_produce_batch
          conv._cli_preview_patched = True

    orig_spec.loader = _PatchingLoader()
    return orig_spec


def install_runtime_hooks() -> Path:
  cli_dir = _locate_cli_dir()
  if not any(isinstance(f, _PreviewCliFinder) for f in sys.meta_path):
    sys.meta_path.insert(0, _PreviewCliFinder(cli_dir))
  return cli_dir


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
