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

"""Runs the interactive 8-step `beam` CLI showcase (`python -m apache_beam_cli_preview.demo`)."""

import os
from pathlib import Path
import shutil
import subprocess
import sys


def main() -> int:
  here = Path(__file__).resolve().parent
  demo_sh = here / '_bundled_demo.sh'
  if not demo_sh.is_file():
    demo_sh = here.parent / 'demo_showcase.sh'
  if not demo_sh.is_file():
    sys.stderr.write('error: could not locate demo_showcase.sh\n')
    return 1

  env = dict(os.environ)
  if 'BEAM_BIN' not in env:
    sibling_beam = Path(sys.executable).parent / 'beam'
    if sibling_beam.is_file():
      env['BEAM_BIN'] = str(sibling_beam)
    else:
      found = shutil.which('beam')
      if found:
        env['BEAM_BIN'] = found

  return subprocess.call(['bash', str(demo_sh)], env=env)


if __name__ == '__main__':
  sys.exit(main())
