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

"""Runs the interactive 12-step `beam` CLI showcase (`beam-demo` / `python -m apache_beam_cli_preview.demo`)."""

import os
import shutil
import subprocess
import sys


def main() -> int:
  here = os.path.dirname(os.path.abspath(__file__))
  demo_sh = os.path.join(here, '_bundled_demo.sh')
  if not os.path.isfile(demo_sh):
    demo_sh = os.path.join(os.path.dirname(here), 'demo_showcase.sh')
  if not os.path.isfile(demo_sh):
    sys.stderr.write('error: could not locate demo_showcase.sh\n')
    return 1

  env = dict(os.environ)
  env.setdefault('PYTHON_BIN', sys.executable)
  if 'BEAM_BIN' not in env:
    py_dir = os.path.dirname(os.path.abspath(sys.executable))
    search_dirs = [
        py_dir,
        os.path.join(py_dir, 'Scripts'),
        os.path.join(os.path.dirname(py_dir), 'Scripts'),
    ]
    bin_names = (
        ('beam.exe', 'beam.cmd', 'beam.bat', 'beam')
        if os.name == 'nt' else ('beam', ))
    resolved_bin: str | None = None
    for cand_dir in search_dirs:
      for bname in bin_names:
        cand = os.path.join(cand_dir, bname)
        if os.path.isfile(cand):
          resolved_bin = cand
          break
      if resolved_bin:
        break
    if resolved_bin:
      env['BEAM_BIN'] = resolved_bin
    else:
      found = shutil.which('beam')
      if found:
        env['BEAM_BIN'] = found

  try:
    return subprocess.call(['bash', demo_sh], env=env)
  except FileNotFoundError:
    sys.stderr.write(
        "error: 'bash' was not found on PATH; install Git Bash or WSL to run "
        'beam-demo on Windows.\n')
    return 1


if __name__ == '__main__':
  sys.exit(main())
