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

from pathlib import Path
import shutil
from setuptools import setup
from setuptools.command.build_py import build_py as _build_py
from setuptools.command.editable_wheel import editable_wheel as _editable_wheel

_PTH_LINE = (
    'import apache_beam_cli_preview.launcher as _bcp; '
    '_bcp.install_runtime_hooks()\n')


class _BuildPyWithCliBundle(_build_py):
  def run(self):
    super().run()
    here = Path(__file__).resolve().parent
    src_cli = here.parent / 'apache_beam' / 'cli'
    if src_cli.is_dir():
      dst_cli = Path(self.build_lib) / 'apache_beam_cli_preview' / '_bundled_cli'
      dst_cli.mkdir(parents=True, exist_ok=True)
      for py in src_cli.glob('*.py'):
        if not py.name.endswith('_test.py'):
          shutil.copy2(py, dst_cli / py.name)
    pth_file = Path(self.build_lib) / 'apache_beam_cli_preview.pth'
    pth_file.write_text(_PTH_LINE, encoding='utf-8')


class _EditableWheelWithPth(_editable_wheel):
  def run(self):
    super().run()


setup(cmdclass={'build_py': _BuildPyWithCliBundle})
