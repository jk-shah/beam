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

"""Setup hook installing `apache_beam_cli_preview.pth` & bundling `cli/`."""

import base64
import hashlib
import os
import shutil
import tempfile
import zipfile
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py
from setuptools.command.sdist import sdist as _sdist

try:
  from setuptools.command.editable_wheel import editable_wheel as _editable_wheel
except ImportError:  # pragma: no cover
  _editable_wheel = None

_PTH_FILENAME = 'apache_beam_cli_preview.pth'
_PTH_LINE = (
    'import apache_beam_cli_preview.launcher as _bcp; '
    '_bcp.install_runtime_hooks(strict=False)\n')


def _list_cli_py_files(src_cli: Path) -> list[Path]:
  if not src_cli.is_dir():
    return []
  return [
      py for py in sorted(src_cli.rglob('*.py'))
      if not py.name.endswith('_test.py') and '__pycache__' not in py.parts
  ]


def _resolve_cli_source_dir(here: Path) -> Path:
  repo_cli = here.parent / 'apache_beam' / 'cli'
  bundled_cli = here / 'apache_beam_cli_preview' / '_bundled_cli'
  for candidate in (repo_cli, bundled_cli):
    if _list_cli_py_files(candidate):
      return candidate
  raise RuntimeError(
      'Cannot build apache-beam-cli-preview: neither '
      f'{repo_cli} nor {bundled_cli} contains apache_beam/cli .py sources.')


def _copy_file_safe(src: Path, dst: Path) -> None:
  dst.parent.mkdir(parents=True, exist_ok=True)
  if dst.exists() or dst.is_symlink():
    try:
      if os.path.samefile(src, dst):
        return
    except OSError:
      pass
    dst.unlink()
  shutil.copy2(src, dst)


def _copy_cli_sources(src_cli: Path, dst_cli: Path) -> None:
  py_files = _list_cli_py_files(src_cli)
  if not py_files:
    raise RuntimeError(
        f'Cannot build apache-beam-cli-preview: 0 .py files found in {src_cli}')
  dst_cli.mkdir(parents=True, exist_ok=True)
  for py in py_files:
    rel = py.relative_to(src_cli)
    _copy_file_safe(py, dst_cli / rel)


def _inject_pth_into_wheel_archive(wheel_path: Path) -> None:
  with zipfile.ZipFile(wheel_path, 'r') as zf_in:
    names = zf_in.namelist()
    if _PTH_FILENAME in names:
      return
    items = [(info, zf_in.read(info.filename)) for info in zf_in.infolist()]

  pth_bytes = _PTH_LINE.encode('utf-8')
  digest_b64 = base64.urlsafe_b64encode(
      hashlib.sha256(pth_bytes).digest()).rstrip(b'=').decode('ascii')
  pth_record_line = f'{_PTH_FILENAME},sha256={digest_b64},{len(pth_bytes)}'

  fd, tmp_whl = tempfile.mkstemp(
      prefix='.tmp-editable-', suffix='.whl', dir=str(wheel_path.parent))
  os.close(fd)
  try:
    with zipfile.ZipFile(
        tmp_whl, 'w', compression=zipfile.ZIP_DEFLATED) as zf_out:
      wrote_pth = False
      for info, data in items:
        if info.filename.endswith('.dist-info/RECORD'):
          if not wrote_pth:
            zf_out.writestr(_PTH_FILENAME, pth_bytes)
            wrote_pth = True
          rec_lines = data.decode('utf-8').splitlines()
          out_lines: list[str] = []
          inserted = False
          for ln in rec_lines:
            if ln.startswith(f'{info.filename},') and not inserted:
              out_lines.append(pth_record_line)
              inserted = True
            out_lines.append(ln)
          if not inserted:
            out_lines.append(pth_record_line)
          zf_out.writestr(info, ('\n'.join(out_lines) + '\n').encode('utf-8'))
        else:
          zf_out.writestr(info, data)
      if not wrote_pth:
        zf_out.writestr(_PTH_FILENAME, pth_bytes)
    os.replace(tmp_whl, wheel_path)
  finally:
    if os.path.exists(tmp_whl):
      try:
        os.unlink(tmp_whl)
      except OSError:
        pass


class _SdistWithCliBundle(_sdist):
  """Ensures `uv pip install git+...#subdirectory=...` (sdist -> wheel) bundles `cli/`."""
  def make_release_tree(self, base_dir, files):
    super().make_release_tree(base_dir, files)
    here = Path(__file__).resolve().parent
    src_cli = _resolve_cli_source_dir(here)
    _copy_cli_sources(
        src_cli, Path(base_dir) / 'apache_beam_cli_preview' / '_bundled_cli')
    for extra_file in ('demo_showcase.sh', 'PREVIEW_DEMO.md'):
      src_extra = here / extra_file
      if src_extra.is_file():
        _copy_file_safe(src_extra, Path(base_dir) / extra_file)


class _BuildPyWithCliBundle(_build_py):
  def run(self):
    super().run()
    here = Path(__file__).resolve().parent
    src_cli = _resolve_cli_source_dir(here)
    _copy_cli_sources(
        src_cli,
        Path(self.build_lib) / 'apache_beam_cli_preview' / '_bundled_cli')
    demo_sh = here / 'demo_showcase.sh'
    if demo_sh.is_file():
      dst_pkg = Path(self.build_lib) / 'apache_beam_cli_preview'
      dst_pkg.mkdir(parents=True, exist_ok=True)
      _copy_file_safe(demo_sh, dst_pkg / '_bundled_demo.sh')
    pth_file = Path(self.build_lib) / _PTH_FILENAME
    pth_file.write_text(_PTH_LINE, encoding='utf-8')


_CMDCLASS: dict[str, type] = {
    'build_py': _BuildPyWithCliBundle,
    'sdist': _SdistWithCliBundle,
}

if _editable_wheel is not None:

  class _EditableWheelWithPth(_editable_wheel):
    """Injects `apache_beam_cli_preview.pth` into PEP 660 editable wheels."""
    def run(self):
      here = Path(__file__).resolve().parent
      _resolve_cli_source_dir(here)
      super().run()
      dist_dir = Path(self.dist_dir)
      for whl in sorted(dist_dir.glob('*.whl')):
        _inject_pth_into_wheel_archive(whl)

  _CMDCLASS['editable_wheel'] = _EditableWheelWithPth


setup(cmdclass=_CMDCLASS)
