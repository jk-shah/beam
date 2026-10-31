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

"""Fast console-script entry point for the experimental `beam` command.

Importing `apache_beam.cli.main` directly first runs `apache_beam/__init__.py`,
which loads the full SDK (~1s) before `--help`, `beam config`, `beam sql`, or
argument validation can run. Loading `apache_beam/cli` under a private package
alias defers importing `apache_beam` until a subcommand actually builds or
executes a Beam pipeline.
"""

import importlib
import importlib.util
import sys
from pathlib import Path

_FAST_PACKAGE = '_apache_beam_cli_pkg'


def main(argv=None):
  """Runs `apache_beam/cli/main.py` without eagerly importing `apache_beam`."""
  if 'apache_beam' not in sys.modules and _FAST_PACKAGE not in sys.modules:
    cli_dir = Path(__file__).resolve().with_name('apache_beam') / 'cli'
    init_py = cli_dir / '__init__.py'
    if init_py.is_file():
      spec = importlib.util.spec_from_file_location(
          _FAST_PACKAGE, init_py, submodule_search_locations=[str(cli_dir)])
      if spec is not None and spec.loader is not None:
        pkg = importlib.util.module_from_spec(spec)
        sys.modules[_FAST_PACKAGE] = pkg
        spec.loader.exec_module(pkg)
  target = (
      f'{_FAST_PACKAGE}.main'
      if _FAST_PACKAGE in sys.modules else 'apache_beam.cli.main')
  return importlib.import_module(target).main(argv)


if __name__ == '__main__':
  sys.exit(main())
