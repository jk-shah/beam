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

"""Entry point of the `beam` (`apache-beam`) command-line tool.

Supports human-friendly text output and deterministic `--json` (`--output=json`)
envelopes for autonomous AI agents across:
- `beam run`     — execute Beam YAML files or `-` stdin locally or on clusters
- `beam pipe`    — universal URI ingestion, `--where`, `--select`, `--dlq`,
                   `--max-bytes`, `--assert-counts`, and `--verify`
- `beam diff`    — distributed O(1)-memory key/multiset dataset reconciliation
- `beam inspect` — `<200ms` zero-runner schema, stats, `$0` BQ metadata & search
- `beam check`   — offline SafeLoader YAML validation & deterministic `plan_hash`
- `beam tail`    — bounded non-destructive stream peek (`pubsub://`, `kafka://`)
- `beam wait` / `beam cancel` — async remote job polling, drain, & cancellation
- `beam io` / `beam doctor` / `beam config` / `beam sql` / `beam --llms-txt`
"""

import argparse
import glob
import hashlib
import io
import json
import logging
import os
import sys
import time
import traceback
from typing import Any

from . import _option_arity
from . import output

# Documented exit codes.
EXIT_OK = 0
EXIT_PIPELINE_FAILED = 1
EXIT_USAGE = 2
EXIT_MISSING_PACKAGE = 4
EXIT_DIFF_MISMATCH = 6
EXIT_TIMEOUT = 7
EXIT_COST_LIMIT = 8
EXIT_INTERRUPTED = 130  # 128 + SIGINT

_EXIT_CODES = """
exit codes:
  0    success (pipeline finished, datasets matched, or dry-run validated)
  1    pipeline execution or --assert-counts conservation failure
  2    usage, validation, schema, or security error
  4    a required optional package or Java runtime is missing
  6    beam diff / --verify detected dataset mismatch
  7    --wait-timeout / beam wait --timeout expired
  8    --max-bytes pre-flight scan guardrail exceeded
  130  interrupted (Ctrl-C)
"""

_RUN_EPILOG = """\
examples:
  beam run pipeline.yaml
  beam run - < pipeline.yaml --json
  beam run pipeline.yaml --runner=dataflow --project=my-project \\
      --region=us-central1 --temp_location=gs://my-bucket/tmp

Every other --flag is passed to the pipeline as a Beam pipeline option.
""" + _EXIT_CODES

_PIPE_EPILOG = """\
examples:
  beam pipe --from users.csv --to out/users.parquet
  beam pipe --from gs://raw/orders.csv --to bigquery://proj.ds.orders \\
      --where "amount > 0" --dlq gs://raw/dlq/orders.json --assert-counts --json
  beam pipe --from users.csv --to out/users.parquet --dry-run > pipeline.yaml

Formats: csv, json, parquet, avro, bigquery://, postgres://, pubsub://,
kafka://, iceberg://. Every other --flag is passed as a Beam pipeline option.
""" + _EXIT_CODES

_SQL_EPILOG = """\
examples:
  beam sql
  beam sql --io csv --io parquet
  beam sql -e "SELECT 1 + 1 AS two;"
  beam sql --runner dataflow --io google-cloud-platform

Builds a shaded Beam SQL JAR once with Maven Wrapper and caches it in
~/.beam/cache/ (sharing the cache with scripts/beam-sql.sh). Requires Java 11+.
Every other argument is passed through to BeamSqlLine.
""" + _EXIT_CODES

_CONFIG_EPILOG = """\
examples:
  beam config set runner DataflowRunner
  beam config set project my-gcp-project
  beam config list --json
  beam config set runner PrismRunner --profile dev
  beam config use dev
  beam config unset temp_location

Stores named profiles in ~/.beam/config.yaml using Beam YAML's options:
mapping. Explicit CLI flags and a pipeline file's options: block always
override saved defaults. Pass --no-config (or set BEAM_NO_CONFIG=1) on
beam run, beam pipe, or beam sql to ignore ~/.beam/config.yaml.
"""

_SUBCOMMANDS = frozenset([
    'run',
    'pipe',
    'sql',
    'config',
    'diff',
    'inspect',
    'check',
    'io',
    'doctor',
    'tail',
    'wait',
    'cancel',
])
_PASSTHROUGH_SUBCOMMANDS = frozenset(['run', 'pipe', 'diff', 'check'])
_CLI_ZERO_FLAGS = {
    None: frozenset(['--help', '--json', '--version', '--llms-txt']),
    'run': frozenset([
        '--verbose',
        '--no-config',
        '--no-jinja',
        '--allow-stdin-jinja',
        '--dry-run',
        '--dry_run',
        '--help',
    ]),
    'pipe': frozenset([
        '--dry-run',
        '--dry_run',
        '--emit-yaml',
        '--assert-counts',
        '--verify',
        '--verbose',
        '--no-config',
        '--help',
    ]),
    'diff': frozenset(['--verbose', '--no-config', '--help']),
    'check': frozenset([
        '--plan',
        '--trust-providers',
        '--no-config',
        '--help',
    ]),
}
_CLI_ONE_FLAGS = {
    None: frozenset(['--output']),
    'run': frozenset(['--profile', '--wait-timeout']),
    'pipe': frozenset([
        '--from',
        '--to',
        '--format',
        '--from-format',
        '--to-format',
        '--where',
        '--select',
        '--dlq',
        '--dlq-format',
        '--if-exists',
        '--from-opt',
        '--to-opt',
        '--from-opt-file',
        '--to-opt-file',
        '--max-bytes',
        '--verify-key',
        '--wait-timeout',
        '--profile',
    ]),
    'diff': frozenset([
        '--key',
        '--left-format',
        '--right-format',
        '--rtol',
        '--atol',
        '--max-bytes',
        '--profile',
    ]),
    'check': frozenset(['--jinja-variables', '--profile']),
}


class _CliError(Exception):
  """An error reported to the user cleanly with a deterministic exit code."""
  def __init__(
      self,
      message: str,
      exit_code: int,
      *,
      extra: dict[str, Any] | None = None,
      next_actions: list[dict[str, Any]] | None = None,
  ):
    super().__init__(message)
    self.exit_code = exit_code
    self.extra = extra or {}
    self.next_actions = next_actions


def _extract_global_flags(
    raw_argv: list[str]) -> tuple[list[str], bool, bool, bool]:
  """Extracts `--json`/`--output`, top-level `--version`, and `--llms-txt`."""
  cleaned: list[str] = []
  json_mode = False
  top_version = False
  llms_txt = False
  seen_subcommand = False
  i = 0
  while i < len(raw_argv):
    tok = raw_argv[i]
    if tok == '--':
      cleaned.extend(raw_argv[i:])
      break
    if not seen_subcommand and tok in _SUBCOMMANDS:
      seen_subcommand = True
      cleaned.append(tok)
      i += 1
      continue
    if tok == '--json':
      json_mode = True
      i += 1
      continue
    if tok.startswith('--output='):
      mode = tok.split('=', 1)[1].strip().lower()
      if mode == 'json':
        json_mode = True
      elif mode == 'text':
        json_mode = False
      else:
        cleaned.append(tok)  # Let argparse report invalid --output value.
      i += 1
      continue
    if tok == '--output' and i + 1 < len(raw_argv):
      mode = raw_argv[i + 1].strip().lower()
      if mode in ('json', 'text'):
        json_mode = (mode == 'json')
        i += 2
        continue
    if tok == '--llms-txt':
      llms_txt = True
      i += 1
      continue
    if not seen_subcommand and tok == '--version':
      top_version = True
      i += 1
      continue
    cleaned.append(tok)
    i += 1
  return cleaned, json_mode, top_version, llms_txt


def _normalize_cli_argv(argv: list[str]) -> list[str]:
  return _option_arity.normalize_argv(
      argv,
      subcommands=_SUBCOMMANDS,
      passthrough_subcommands=_PASSTHROUGH_SUBCOMMANDS,
      cli_zero_flags_by_cmd=_CLI_ZERO_FLAGS,
      cli_one_flags_by_cmd=_CLI_ONE_FLAGS)


def main(argv=None) -> int:
  """Runs the `beam` command and returns its exit code."""
  t0 = time.monotonic()
  raw_argv = list(sys.argv[1:] if argv is None else argv)
  stripped_argv, json_mode, top_version, llms_txt = _extract_global_flags(
      raw_argv)

  with output.JsonOutputSession(enabled=json_mode) as json_session:
    if llms_txt:
      from . import llms_txt as llms_mod
      manual = llms_mod.render_llms_txt()
      if json_mode:
        json_session.emit(
            output.build_envelope(
                ok=True,
                command='llms-txt',
                exit_code=EXIT_OK,
                duration_ms=int((time.monotonic() - t0) * 1000),
                outputs={'llms_txt': manual}))
      else:
        print(manual, end='')
      return EXIT_OK

    if top_version and not stripped_argv:
      ver = output.installed_beam_version()
      if json_mode:
        json_session.emit(
            output.build_envelope(
                ok=True,
                command='version',
                exit_code=EXIT_OK,
                duration_ms=int((time.monotonic() - t0) * 1000),
                outputs={'beam_version': ver}))
      else:
        print(f'beam {ver}')
      return EXIT_OK

    norm_argv = _normalize_cli_argv(stripped_argv)
    parser = _build_parser()
    active_cmd: str | None = None
    verbose_flag = '--verbose' in stripped_argv

    try:
      if json_mode:
        # Capture argparse stderr so `--json` usage errors emit a clean JSON
        # envelope on stdout instead of exiting with empty stdout.
        err_buf = io.StringIO()
        try:
          import contextlib
          with contextlib.redirect_stderr(err_buf):
            args, extra_args = parser.parse_known_args(norm_argv)
        except SystemExit as sys_exc:
          exit_c = int(sys_exc.code) if isinstance(sys_exc.code, int) else 2
          if exit_c == 0:
            return EXIT_OK
          msg = err_buf.getvalue().strip() or 'invalid command-line arguments'
          raise _CliError(msg, exit_c) from None
      else:
        args, extra_args = parser.parse_known_args(norm_argv)

      active_cmd = getattr(args, 'command', None)
      verbose_flag = bool(getattr(args, 'verbose', False))
      if json_mode and not verbose_flag:
        import warnings
        logging.getLogger().setLevel(logging.ERROR)
        warnings.filterwarnings(
            'ignore', category=UserWarning, module=r'google\.auth.*')
      else:
        logging.getLogger().setLevel(logging.INFO)

      if active_cmd == 'run':
        return _run(args, extra_args, json_session=json_session, t0=t0)
      if active_cmd == 'pipe':
        pipe_tail = (
            stripped_argv[stripped_argv.index('pipe') + 1:]
            if 'pipe' in stripped_argv else norm_argv[1:])
        return _pipe(
            args,
            extra_args,
            pipe_tail,
            raw_argv=stripped_argv,
            json_session=json_session,
            t0=t0)
      if active_cmd == 'diff':
        return _diff(args, extra_args, json_session=json_session, t0=t0)
      if active_cmd == 'inspect':
        return _inspect(args, extra_args, json_session=json_session, t0=t0)
      if active_cmd == 'check':
        return _check(args, extra_args, json_session=json_session, t0=t0)
      if active_cmd == 'io':
        return _io(args, extra_args, json_session=json_session, t0=t0)
      if active_cmd == 'doctor':
        return _doctor(args, extra_args, json_session=json_session, t0=t0)
      if active_cmd == 'tail':
        return _tail(args, extra_args, json_session=json_session, t0=t0)
      if active_cmd == 'wait':
        return _wait(args, extra_args, json_session=json_session, t0=t0)
      if active_cmd == 'cancel':
        return _cancel_cmd(args, extra_args, json_session=json_session, t0=t0)
      if active_cmd == 'sql':
        return _sql(args, extra_args, json_session=json_session, t0=t0)
      if active_cmd == 'config':
        return _config(args, extra_args, json_session=json_session, t0=t0)

      # External plugin dispatch (`apache_beam.cli.plugins`).
      from . import llms_txt as llms_mod
      plugins = llms_mod.discover_cli_plugins(_SUBCOMMANDS)
      if active_cmd in plugins:
        plugin_fn = plugins[active_cmd].load()
        return int(plugin_fn(args, extra_args) or 0)
      raise _CliError(f'unknown command: {active_cmd!r}', EXIT_USAGE)

    except _CliError as e:
      if not json_mode or verbose_flag:
        print(f'error: {e}', file=sys.stderr)
      if json_mode:
        err_obj, auto_actions = output.classify_error(
            str(e), e.exit_code, command=active_cmd, verbose=verbose_flag)
        json_session.emit(
            output.build_envelope(
                ok=False,
                command=active_cmd,
                exit_code=e.exit_code,
                duration_ms=int((time.monotonic() - t0) * 1000),
                error=err_obj,
                next_actions=e.next_actions
                if e.next_actions is not None else auto_actions,
                extra=e.extra))
      return e.exit_code
    except KeyboardInterrupt:
      if not json_mode or verbose_flag:
        print('error: interrupted', file=sys.stderr)
      if json_mode:
        err_obj, _ = output.classify_error(
            'interrupted', EXIT_INTERRUPTED, command=active_cmd)
        json_session.emit(
            output.build_envelope(
                ok=False,
                command=active_cmd,
                exit_code=EXIT_INTERRUPTED,
                duration_ms=int((time.monotonic() - t0) * 1000),
                error=err_obj))
      return EXIT_INTERRUPTED


def _add_config_selection_args(parser):
  parser.add_argument(
      '--profile',
      metavar='NAME',
      help='configuration profile from ~/.beam/config.yaml')
  parser.add_argument(
      '--no-config',
      action='store_true',
      help='ignore saved options in ~/.beam/config.yaml')


def _reject_extra_args(extra_args: list[str]) -> None:
  if extra_args:
    raise _CliError(
        f'unrecognized arguments: {" ".join(extra_args)}', EXIT_USAGE)


def _build_parser():
  parser = argparse.ArgumentParser(
      prog='beam',
      description='Apache Beam command-line tool for humans and AI agents.',
      allow_abbrev=False)
  parser.add_argument(
      '--json',
      action='store_true',
      help='emit a single structured JSON response envelope on stdout')
  parser.add_argument(
      '--output',
      choices=['text', 'json'],
      default='text',
      help='output format: text (default) or json')
  parser.add_argument(
      '--version', action='store_true', help='print installed Beam version')
  parser.add_argument(
      '--llms-txt',
      action='store_true',
      help='print compact machine-readable agent manual and exit')

  commands = parser.add_subparsers(
      dest='command', required=True, metavar='COMMAND')

  # 1. `beam run`
  run = commands.add_parser(
      'run',
      help='run a Beam YAML pipeline file (or - for stdin)',
      description=(
          'Run a Beam YAML pipeline file or stdin (-), passing any extra '
          '--flags through as Beam PipelineOptions.'),
      epilog=_RUN_EPILOG,
      formatter_class=argparse.RawDescriptionHelpFormatter,
      allow_abbrev=False)
  run.add_argument(
      'file',
      metavar='FILE',
      help='pipeline YAML file (local path, gs://..., or - for stdin)')
  run.add_argument(
      '--no-jinja',
      action='store_true',
      help='disable Jinja2 template expansion (default when FILE is -)')
  run.add_argument(
      '--allow-stdin-jinja',
      action='store_true',
      help='explicitly allow Jinja2 template expansion when reading from -')
  run.add_argument(
      '--dry-run',
      '--dry_run',
      action='store_true',
      help='build and validate the pipeline without executing it')
  run.add_argument(
      '--wait-timeout',
      metavar='DURATION',
      help='max wait time (e.g. 30s, 5m); detaches remote jobs with exit 7')
  run.add_argument(
      '--verbose', action='store_true', help='show tracebacks for errors')
  _add_config_selection_args(run)

  # 2. `beam pipe`
  pipe = commands.add_parser(
      'pipe',
      help='move, filter, project, clean, and verify data across URIs',
      description=(
          'Move, convert, filter, project, dead-letter, and verify data across '
          'local files, cloud object stores, BigQuery, Postgres, Pub/Sub, '
          'Kafka, and Iceberg.'),
      epilog=_PIPE_EPILOG,
      formatter_class=argparse.RawDescriptionHelpFormatter,
      allow_abbrev=False)
  pipe.add_argument(
      '--from',
      dest='source',
      required=True,
      metavar='SOURCE',
      help='source path, glob, or URI (gs://, bigquery://, pubsub://, ...)')
  pipe.add_argument(
      '--to',
      dest='sink',
      required=True,
      metavar='SINK',
      help='destination path prefix or URI')
  pipe.add_argument(
      '--format',
      metavar='FMT',
      help='format of both sides; overridden by --from-format / --to-format')
  pipe.add_argument(
      '--from-format',
      metavar='FMT',
      help='source format (csv, json, parquet, avro)')
  pipe.add_argument(
      '--to-format',
      metavar='FMT',
      help='sink format (csv, json, parquet, avro)')
  pipe.add_argument(
      '--where',
      metavar='EXPR',
      help='safe SQL-like row filter (e.g. "amount > 0 AND status == \'OK\'")')
  pipe.add_argument(
      '--select',
      metavar='EXPR',
      help='projection / scalar expressions (e.g. "id, lower(email) AS email")')
  pipe.add_argument(
      '--dlq',
      metavar='SINK',
      help='dead-letter destination URI/path for corrupt or failing records')
  pipe.add_argument(
      '--dlq-format',
      metavar='FMT',
      help='dead-letter file format (default: json or inferred from extension)')
  pipe.add_argument(
      '--if-exists',
      choices=['fail', 'append', 'replace'],
      default='fail',
      help='behavior if destination exists: fail (default), append, or replace')
  pipe.add_argument(
      '--from-opt',
      dest='from_opts',
      action='append',
      default=[],
      metavar='KEY=VAL',
      help='additional source connector option (repeatable)')
  pipe.add_argument(
      '--to-opt',
      dest='to_opts',
      action='append',
      default=[],
      metavar='KEY=VAL',
      help='additional sink connector option (repeatable)')
  pipe.add_argument(
      '--from-opt-file',
      metavar='FILE',
      help='YAML/JSON file with source connector options (for credentials)')
  pipe.add_argument(
      '--to-opt-file',
      metavar='FILE',
      help='YAML/JSON file with sink connector options (for credentials)')
  pipe.add_argument(
      '--max-bytes',
      metavar='SIZE',
      help='abort with exit code 8 if estimated source scan exceeds SIZE')
  pipe.add_argument(
      '--assert-counts',
      action='store_true',
      help='fail if rows_read != rows_written + rows_filtered_out + dlq_rows')
  pipe.add_argument(
      '--verify',
      action='store_true',
      help='run distributed beam diff reconciliation after writing')
  pipe.add_argument(
      '--verify-key',
      dest='verify_keys',
      action='append',
      default=[],
      metavar='COL',
      help='primary key column for --verify reconciliation (repeatable)')
  pipe.add_argument(
      '--dry-run',
      '--dry_run',
      '--emit-yaml',
      dest='dry_run',
      action='store_true',
      help='check the pipeline and print it as YAML instead of running it')
  pipe.add_argument(
      '--wait-timeout',
      metavar='DURATION',
      help='max wait time (e.g. 30s, 5m); detaches remote jobs with exit 7')
  pipe.add_argument(
      '--verbose', action='store_true', help='show tracebacks for errors')
  _add_config_selection_args(pipe)

  # 3. `beam diff`
  diff_p = commands.add_parser(
      'diff',
      help='reconcile two datasets by primary key or row multiset hash',
      allow_abbrev=False)
  diff_p.add_argument('left', metavar='LEFT_URI', help='left dataset URI/path')
  diff_p.add_argument(
      'right', metavar='RIGHT_URI', help='right dataset URI/path')
  diff_p.add_argument(
      '--key',
      dest='keys',
      action='append',
      default=[],
      metavar='COL',
      help='key column(s) to join on (repeatable; omit for multiset hash diff)')
  diff_p.add_argument('--left-format', metavar='FMT', help='left file format')
  diff_p.add_argument('--right-format', metavar='FMT', help='right file format')
  diff_p.add_argument(
      '--rtol',
      type=float,
      default=0.0,
      help='relative float tolerance (default: 0.0)')
  diff_p.add_argument(
      '--atol',
      type=float,
      default=0.0,
      help='absolute float tolerance (default: 0.0)')
  diff_p.add_argument(
      '--max-bytes',
      metavar='SIZE',
      help='abort with exit code 8 if combined scan exceeds SIZE')
  diff_p.add_argument(
      '--verbose', action='store_true', help='show tracebacks for errors')
  _add_config_selection_args(diff_p)

  # 4. `beam inspect`
  insp = commands.add_parser(
      'inspect',
      help='inspect schema, row count, byte size, and sample rows (<200ms)',
      allow_abbrev=False)
  insp.add_argument('uri', metavar='URI', help='file path, glob, or URI')
  insp.add_argument('--format', metavar='FMT', help='explicit file format')
  insp.add_argument(
      '--sample',
      type=int,
      default=5,
      metavar='N',
      help='number of sample rows to include (default: 5; 0 for schema only)')
  insp.add_argument(
      '--stats',
      action='store_true',
      help='compute null counts across sampled rows')
  insp.add_argument(
      '--search',
      metavar='SUBSTRING',
      help='case-insensitive substring filter across sampled head scan')
  insp.add_argument(
      '--max-scanned-bytes',
      default='64MB',
      metavar='SIZE',
      help='maximum bytes read during local/GCS head scan (default: 64MB)')
  insp.add_argument(
      '--from-opt',
      dest='from_opts',
      action='append',
      default=[],
      metavar='KEY=VAL',
      help='source connector option (repeatable)')
  insp.add_argument('--from-opt-file', metavar='FILE', help='options YAML/JSON')
  insp.add_argument('--project', metavar='PROJECT', help='default GCP project')

  # 5. `beam check`
  chk = commands.add_parser(
      'check',
      help='validate Beam YAML syntax/structure offline & compute plan_hash',
      allow_abbrev=False)
  chk.add_argument(
      'file', metavar='FILE', help='Beam YAML file path or - for stdin')
  chk.add_argument(
      '--plan',
      action='store_true',
      help='expand transforms into a full Beam Runner API graph')
  chk.add_argument(
      '--trust-providers',
      action='store_true',
      help='allow custom providers: code execution during --plan expansion')
  chk.add_argument(
      '--jinja-variables',
      metavar='JSON',
      help='JSON mapping of Jinja variables when explicitly expanding templates')
  _add_config_selection_args(chk)

  # 6. `beam io`
  io_p = commands.add_parser(
      'io',
      help='list or describe supported Beam URI & YAML connectors (<50ms)',
      allow_abbrev=False)
  io_sub = io_p.add_subparsers(dest='io_action', required=True, metavar='ACTION')
  io_sub.add_parser('list', help='list supported connectors', allow_abbrev=False)
  io_desc = io_sub.add_parser(
      'describe', help='describe one connector in detail', allow_abbrev=False)
  io_desc.add_argument('connector', metavar='CONNECTOR', help='connector name')

  # 7. `beam doctor`
  commands.add_parser(
      'doctor',
      help='verify Beam extras, Java runtime, caches, and GCP ADC (<90ms)',
      allow_abbrev=False)

  # 8. `beam tail`
  tail_p = commands.add_parser(
      'tail',
      help='non-destructively peek at a Pub/Sub, Kafka, or dataset stream',
      allow_abbrev=False)
  tail_p.add_argument('uri', metavar='URI', help='stream or dataset URI')
  tail_p.add_argument(
      '--limit',
      type=int,
      default=5,
      metavar='N',
      help='max messages/rows to sample (default: 5)')
  tail_p.add_argument(
      '--duration',
      default='5s',
      metavar='DURATION',
      help='bounded peek window (default: 5s)')
  tail_p.add_argument(
      '--max-scanned-bytes',
      default='64MB',
      metavar='SIZE',
      help='byte cap for sampled payloads (default: 64MB)')
  tail_p.add_argument('--project', metavar='PROJECT', help='default GCP project')

  # 9. `beam wait`
  wait_p = commands.add_parser(
      'wait',
      help='poll a detached remote job until completion or --timeout',
      allow_abbrev=False)
  wait_p.add_argument('job_id', metavar='JOB_ID', help='job identifier')
  wait_p.add_argument(
      '--timeout',
      metavar='DURATION',
      help='maximum wait duration (exits with 7 if still running)')
  wait_p.add_argument(
      '--poll-interval',
      type=float,
      default=3.0,
      metavar='SEC',
      help='seconds between status polls (default: 3.0)')
  wait_p.add_argument('--project', metavar='PROJECT', help='GCP project')
  wait_p.add_argument('--region', metavar='REGION', help='GCP region')

  # 10. `beam cancel`
  cancel_p = commands.add_parser(
      'cancel',
      help='cancel or drain a running remote job',
      allow_abbrev=False)
  cancel_p.add_argument('job_id', metavar='JOB_ID', help='job identifier')
  cancel_p.add_argument(
      '--drain',
      action='store_true',
      help='drain in-flight windows cleanly instead of immediate hard cancel')
  cancel_p.add_argument('--project', metavar='PROJECT', help='GCP project')
  cancel_p.add_argument('--region', metavar='REGION', help='GCP region')

  # 11. `beam sql`
  sql_parser = commands.add_parser(
      'sql',
      help='launch the interactive Beam SQL shell',
      description=(
          'Launch the Apache Beam SQL shell (BeamSqlLine), caching shaded '
          'JARs in ~/.beam/cache/ compatibly with scripts/beam-sql.sh.'),
      epilog=_SQL_EPILOG,
      formatter_class=argparse.RawDescriptionHelpFormatter,
      allow_abbrev=False)
  sql_parser.add_argument(
      '--version',
      metavar='VER',
      help='Beam release version for SQL artifacts (default: installed SDK)')
  sql_parser.add_argument(
      '--runner',
      default=None,
      metavar='RUNNER',
      help='runner to include: direct (default) or dataflow')
  sql_parser.add_argument(
      '--io',
      dest='ios',
      action='append',
      default=[],
      metavar='CONNECTOR',
      help='IO connector to include; may be given multiple times')
  sql_parser.add_argument(
      '--list-versions',
      action='store_true',
      help='list recent Beam versions on Maven Central and exit')
  sql_parser.add_argument(
      '--list-ios',
      action='store_true',
      help='list available IO connectors on Maven Central and exit')
  sql_parser.add_argument(
      '--list-runners',
      action='store_true',
      help='list available runners on Maven Central and exit')
  sql_parser.add_argument(
      '--debug',
      action='store_true',
      help='print Maven and Java commands as they run')
  _add_config_selection_args(sql_parser)

  # 12. `beam config`
  config_parser = commands.add_parser(
      'config',
      help='manage default pipeline options in ~/.beam/config.yaml',
      description=(
          'Manage named profiles of default PipelineOptions in '
          '~/.beam/config.yaml.'),
      epilog=_CONFIG_EPILOG,
      formatter_class=argparse.RawDescriptionHelpFormatter,
      allow_abbrev=False)
  actions = config_parser.add_subparsers(
      dest='config_action', required=True, metavar='ACTION')

  set_p = actions.add_parser(
      'set', help='set a default pipeline option', allow_abbrev=False)
  set_p.add_argument('option', metavar='OPTION', help='PipelineOptions name')
  set_p.add_argument('value', metavar='VALUE', help='option value')
  set_p.add_argument('--profile', metavar='NAME', help='profile to modify')

  get_p = actions.add_parser(
      'get', help='print a configured option value', allow_abbrev=False)
  get_p.add_argument('option', metavar='OPTION', help='PipelineOptions name')
  get_p.add_argument('--profile', metavar='NAME', help='profile to read')

  unset_p = actions.add_parser(
      'unset', help='remove a configured option', allow_abbrev=False)
  unset_p.add_argument('option', metavar='OPTION', help='PipelineOptions name')
  unset_p.add_argument('--profile', metavar='NAME', help='profile to modify')

  list_p = actions.add_parser(
      'list', help='show configured options for a profile', allow_abbrev=False)
  list_p.add_argument('--profile', metavar='NAME', help='profile to list')

  use_p = actions.add_parser(
      'use', help='switch the active profile', allow_abbrev=False)
  use_p.add_argument(
      'profile_name', metavar='PROFILE', help='profile name to make active')
  return parser


def _resolve_config_args(args, pipeline_args: list[str]) -> list[str]:
  from . import config
  try:
    return config.resolve_pipeline_args(
        pipeline_args,
        profile=getattr(args, 'profile', None),
        no_config=bool(getattr(args, 'no_config', False)))
  except config.ConfigError as e:
    raise _CliError(str(e), e.exit_code) from None


def _extract_project_from_args(resolved_args: list[str]) -> str | None:
  for i, tok in enumerate(resolved_args):
    if tok.startswith('--project='):
      return tok.split('=', 1)[1]
    if tok == '--project' and i + 1 < len(resolved_args):
      return resolved_args[i + 1]
  return None


def _extract_runner_from_args(resolved_args: list[str]) -> str | None:
  for i, tok in enumerate(resolved_args):
    if tok.startswith('--runner='):
      return tok.split('=', 1)[1]
    if tok == '--runner' and i + 1 < len(resolved_args):
      return resolved_args[i + 1]
  return None


def _config(args, extra_args, *, json_session, t0: float) -> int:
  from . import config
  _reject_extra_args(extra_args)
  buf = io.StringIO()
  try:
    if json_session.enabled:
      import contextlib
      with contextlib.redirect_stdout(buf):
        rc = config.run_command(args)
    else:
      rc = config.run_command(args)
  except config.ConfigError as e:
    raise _CliError(str(e), e.exit_code) from None

  if json_session.enabled:
    text_out = buf.getvalue().strip()
    json_session.emit(
        output.build_envelope(
            ok=(rc == 0),
            command='config',
            exit_code=rc,
            duration_ms=int((time.monotonic() - t0) * 1000),
            outputs={
                'action': args.config_action,
                'result': text_out,
            },
            error=output.classify_error('option not set', rc, command='config')[0]
            if rc != 0 else None))
  return rc


def _sql(args, sqlline_args, *, json_session, t0: float) -> int:
  from . import config
  from . import sql
  if sqlline_args and sqlline_args[0] == '--':
    sqlline_args = sqlline_args[1:]
  try:
    if args.list_versions or args.list_ios or args.list_runners:
      runner = args.runner or 'direct'
    else:
      runner = config.configured_sql_runner(
          args.runner, profile=args.profile, no_config=args.no_config)
    rc = sql.run(
        version=args.version,
        runner=runner,
        ios=args.ios,
        list_versions=args.list_versions,
        list_ios=args.list_ios,
        list_runners=args.list_runners,
        debug=args.debug,
        sqlline_args=sqlline_args)
    if json_session.enabled:
      json_session.emit(
          output.build_envelope(
              ok=(rc == 0),
              command='sql',
              exit_code=rc,
              runner=runner,
              duration_ms=int((time.monotonic() - t0) * 1000)))
    return rc
  except (sql.SqlError, config.ConfigError) as e:
    raise _CliError(str(e), e.exit_code) from None


def _io(args, extra_args, *, json_session, t0: float) -> int:
  from . import io_cmd
  _reject_extra_args(extra_args)
  try:
    if args.io_action == 'list':
      items = io_cmd.list_connectors()
      if json_session.enabled:
        json_session.emit(
            output.build_envelope(
                ok=True,
                command='io',
                exit_code=EXIT_OK,
                duration_ms=int((time.monotonic() - t0) * 1000),
                outputs={'connectors': items}))
      else:
        for c in items:
          dlq_tag = ' [dlq]' if c.get('supports_dlq') else ''
          print(
              f"{c['name']:10s}  {c['read_transform']} / "
              f"{c['write_transform']}{dlq_tag}\n"
              f"            example: {c['uri_example']}")
      return EXIT_OK

    info = io_cmd.describe_connector(args.connector)
    if json_session.enabled:
      json_session.emit(
          output.build_envelope(
              ok=True,
              command='io',
              exit_code=EXIT_OK,
              duration_ms=int((time.monotonic() - t0) * 1000),
              outputs={'connector': info}))
    else:
      for k, v in info.items():
        print(f'{k}: {v}')
    return EXIT_OK
  except ValueError as e:
    raise _CliError(str(e), EXIT_USAGE) from None


def _doctor(args, extra_args, *, json_session, t0: float) -> int:
  del args
  from . import doctor
  _reject_extra_args(extra_args)
  report = doctor.run_doctor()
  rc = EXIT_OK if report['healthy'] else EXIT_MISSING_PACKAGE
  if json_session.enabled:
    json_session.emit(
        output.build_envelope(
            ok=report['healthy'],
            command='doctor',
            exit_code=rc,
            duration_ms=int((time.monotonic() - t0) * 1000),
            outputs=report))
  else:
    status = 'HEALTHY' if report['healthy'] else 'DEGRADED'
    print(f"Beam CLI Doctor: {status} (beam {report['beam_version']}, "
          f"python {report['python_version']})")
    for k, ok_val in report['extras'].items():
      print(f"  extra.{k:14s}: {'ok' if ok_val else 'missing'}")
    print(f"  java                : {'ok (' + report['java']['java_bin'] + ')' if report['java']['ok'] else report['java']['message']}")
    print(f"  gcp_adc             : {'ok (project=' + str(report['gcp_adc'].get('default_project')) + ')' if report['gcp_adc']['ok'] else report['gcp_adc'].get('message')}")
  return rc


def _inspect(args, extra_args, *, json_session, t0: float) -> int:
  from . import cost_guard
  from . import inspect_cmd
  from . import uris
  _reject_extra_args(extra_args)
  try:
    info = inspect_cmd.run_inspect(
        args.uri,
        explicit_format=args.format,
        sample_count=args.sample,
        include_stats=args.stats,
        search_term=args.search,
        max_scanned_bytes=args.max_scanned_bytes,
        from_opts=args.from_opts,
        from_opt_file=args.from_opt_file,
        default_project=args.project)
  except (uris.UriError, ValueError, FileNotFoundError) as e:
    raise _CliError(str(e), EXIT_USAGE) from None
  except ImportError as e:
    raise _CliError(
        f'a required package is missing: {e}', EXIT_MISSING_PACKAGE) from None
  except Exception as e:  # pylint: disable=broad-except
    raise _CliError(f'inspect failed: {e}', EXIT_PIPELINE_FAILED) from None

  untrusted = info.pop('untrusted_data', None)
  insp_warnings = info.pop('warnings', None) or []
  if json_session.enabled:
    json_session.emit(
        output.build_envelope(
            ok=True,
            command='inspect',
            exit_code=EXIT_OK,
            duration_ms=int((time.monotonic() - t0) * 1000),
            warnings=insp_warnings,
            outputs=info,
            untrusted_data=untrusted))
  else:
    sz = (
        cost_guard.format_bytes(info['estimated_bytes'])
        if info.get('estimated_bytes') is not None else 'unknown')
    print(f"URI      : {info['uri']} ({info['kind']} / {info['format']})")
    print(f"Size     : {sz}   Rows: {info.get('row_count', 'unknown')}")
    if info.get('schema'):
      print('Schema   :')
      for col in info['schema']:
        null_s = 'NULLABLE' if col.get('nullable', True) else 'REQUIRED'
        print(f"  - {col['name']}: {col['type']} ({null_s})")
    for w in insp_warnings:
      print(f'Warning  : {w}', file=sys.stderr)
    if untrusted and untrusted.get('rows'):
      print(f"Sample ({len(untrusted['rows'])} rows, sanitized):")
      for r in untrusted['rows']:
        print('  ' + json.dumps(r, sort_keys=True))
  return EXIT_OK


def _tail(args, extra_args, *, json_session, t0: float) -> int:
  from . import tail
  _reject_extra_args(extra_args)
  try:
    info = tail.run_tail(
        args.uri,
        limit=args.limit,
        duration=args.duration,
        max_scanned_bytes=args.max_scanned_bytes,
        default_project=args.project)
  except tail.TailError as e:
    raise _CliError(str(e), e.exit_code) from None
  except ValueError as e:
    raise _CliError(str(e), EXIT_USAGE) from None
  except Exception as e:  # pylint: disable=broad-except
    raise _CliError(f'tail failed: {e}', EXIT_PIPELINE_FAILED) from None

  untrusted = info.pop('untrusted_data', None)
  if json_session.enabled:
    json_session.emit(
        output.build_envelope(
            ok=True,
            command='tail',
            exit_code=EXIT_OK,
            duration_ms=int((time.monotonic() - t0) * 1000),
            outputs=info,
            untrusted_data=untrusted))
  else:
    print(
        f"Sampled {info['sampled_count']} record(s) from {info['uri']} "
        f"(mode={info['mode']}, non_destructive={info['non_destructive']}):")
    for r in (untrusted or {}).get('rows', ()):
      print('  ' + json.dumps(r, sort_keys=True))
  return EXIT_OK


def _check(args, extra_args, *, json_session, t0: float) -> int:
  from . import check_cmd
  resolved = _resolve_config_args(args, extra_args) if args.plan else extra_args
  if not args.plan:
    _reject_extra_args(extra_args)
  try:
    report = check_cmd.run_check(
        args.file,
        expand_plan=args.plan,
        trust_providers=args.trust_providers,
        jinja_variables_json=args.jinja_variables,
        pipeline_args=resolved)
  except check_cmd.CheckError as e:
    acts = None
    if '--trust-providers' in str(e):
      acts = [{
          'command': f'beam check {args.file} --plan --trust-providers --json',
          'description': 'Expand pipeline plan with explicit provider execution trust.',
          'requires_human': True,
      }]
    raise _CliError(str(e), EXIT_USAGE, next_actions=acts) from None
  except ImportError as e:
    raise _CliError(
        f'a required package is missing: {e}', EXIT_MISSING_PACKAGE) from None
  except Exception as e:  # pylint: disable=broad-except
    raise _CliError(f'could not validate pipeline: {e}', EXIT_USAGE) from None

  if json_session.enabled:
    json_session.emit(
        output.build_envelope(
            ok=True,
            command='check',
            exit_code=EXIT_OK,
            duration_ms=int((time.monotonic() - t0) * 1000),
            outputs=report))
  else:
    print(
        f"Valid Beam YAML ({report['mode']}): {report['transform_count']} "
        f"transform(s), plan_hash={report['plan_hash']}")
  return EXIT_OK


def _diff(args, extra_args, *, json_session, t0: float) -> int:
  from . import cost_guard
  from . import diff
  from . import uris
  resolved_args = _resolve_config_args(args, extra_args)
  proj = _extract_project_from_args(resolved_args)
  diff_runner = _extract_runner_from_args(resolved_args) or 'FnApiRunner'
  try:
    rc, report = diff.run_diff(
        args.left,
        args.right,
        keys=args.keys,
        left_format=args.left_format,
        right_format=args.right_format,
        rtol=args.rtol,
        atol=args.atol,
        max_bytes=args.max_bytes,
        pipeline_args=resolved_args,
        default_project=proj)
  except cost_guard.CostLimitError as e:
    raise _CliError(
        str(e),
        EXIT_COST_LIMIT,
        extra={
            'estimated_bytes': e.estimated_bytes,
            'limit_bytes': e.limit_bytes,
            'outputs': {
                'estimated_bytes': e.estimated_bytes,
                'limit_bytes': e.limit_bytes,
            },
        }) from None
  except (uris.UriError, ValueError) as e:
    raise _CliError(str(e), EXIT_USAGE) from None
  except ImportError as e:
    raise _CliError(
        f'a required package is missing: {e}', EXIT_MISSING_PACKAGE) from None
  except Exception as e:  # pylint: disable=broad-except
    _maybe_print_traceback(args.verbose)
    raise _CliError(f'diff failed: {_summarize(e)}', EXIT_PIPELINE_FAILED) from None

  untrusted = report.pop('untrusted_data', None)
  summary = report['summary']
  if json_session.enabled:
    json_session.emit(
        output.build_envelope(
            ok=(rc == EXIT_OK),
            command='diff',
            exit_code=rc,
            runner=diff_runner,
            duration_ms=int((time.monotonic() - t0) * 1000),
            metrics={
                'left_rows': summary['left_rows'],
                'right_rows': summary['right_rows'],
                'matched_rows': summary['matched_rows'],
                'only_in_left': summary['only_in_left'],
                'only_in_right': summary['only_in_right'],
                'value_mismatches': summary['value_mismatches'],
                'duplicate_key_mismatches': summary['duplicate_key_mismatches'],
            },
            outputs=report,
            error=output.classify_error(
                'datasets differ', rc, command='diff')[0]
            if rc != EXIT_OK else None,
            untrusted_data=untrusted))
  else:
    if rc == EXIT_OK:
      print(
          f"MATCH: {summary['matched_rows']} row(s) identical between "
          f"{args.left} and {args.right}.")
    else:
      print(
          f"MISMATCH (exit {rc}): left={summary['left_rows']}, "
          f"right={summary['right_rows']}, matched={summary['matched_rows']}, "
          f"only_in_left={summary['only_in_left']}, "
          f"only_in_right={summary['only_in_right']}, "
          f"value_mismatches={summary['value_mismatches']}")
  return rc


def _wait(args, extra_args, *, json_session, t0: float) -> int:
  from . import jobs
  _reject_extra_args(extra_args)
  try:
    timeout_sec = (
        jobs.parse_duration_seconds(args.timeout) if args.timeout else None)
    rc, record = jobs.wait_for_job(
        args.job_id,
        timeout_seconds=timeout_sec,
        poll_interval=args.poll_interval,
        project=args.project,
        region=args.region)
  except (jobs.JobError, ValueError) as e:
    raise _CliError(str(e), getattr(e, 'exit_code', EXIT_USAGE)) from None
  except Exception as e:  # pylint: disable=broad-except
    raise _CliError(f'wait failed: {e}', EXIT_PIPELINE_FAILED) from None

  if rc == EXIT_PIPELINE_FAILED and record.get('post_check_error'):
    raise _CliError(record['post_check_error'], EXIT_PIPELINE_FAILED)

  next_acts = None
  if rc == EXIT_TIMEOUT:
    next_acts = [{
        'command': f"beam wait {record['job_id']} --json",
        'description': 'Continue polling the running remote job.',
        'requires_human': False,
    }]
  if json_session.enabled:
    json_session.emit(
        output.build_envelope(
            ok=(rc == EXIT_OK),
            command='wait',
            exit_code=rc,
            runner=record.get('runner'),
            job_id=record.get('job_id'),
            state=record.get('state'),
            duration_ms=int((time.monotonic() - t0) * 1000),
            metrics=record.get('metrics') or {},
            outputs=record.get('outputs') or {},
            error=output.classify_error(
                f"job finished or timed out in state {record.get('state')}",
                rc,
                command='wait')[0] if rc != EXIT_OK else None,
            next_actions=next_acts))
  else:
    print(f"Job {record['job_id']}: state={record.get('state')}")
  return rc


def _cancel_cmd(args, extra_args, *, json_session, t0: float) -> int:
  from . import jobs
  _reject_extra_args(extra_args)
  try:
    res = jobs.cancel_job(
        args.job_id,
        drain=args.drain,
        project=args.project,
        region=args.region)
  except jobs.JobError as e:
    raise _CliError(str(e), e.exit_code) from None
  except Exception as e:  # pylint: disable=broad-except
    raise _CliError(f'cancel failed: {e}', EXIT_PIPELINE_FAILED) from None

  if json_session.enabled:
    json_session.emit(
        output.build_envelope(
            ok=True,
            command='cancel',
            exit_code=EXIT_OK,
            job_id=res['job_id'],
            state=res['requested_state'],
            duration_ms=int((time.monotonic() - t0) * 1000),
            outputs=res))
  else:
    print(f"Requested {res['requested_state']} for job {res['job_id']}.")
  return EXIT_OK


def _run(args, pipeline_args, *, json_session, t0: float) -> int:
  from . import jobs
  file = args.file
  wait_timeout_sec = None
  if args.wait_timeout:
    try:
      wait_timeout_sec = jobs.parse_duration_seconds(args.wait_timeout)
    except ValueError as e:
      raise _CliError(str(e), EXIT_USAGE) from None

  if file == '-':
    stdin_yaml = sys.stdin.read()
    if not stdin_yaml.strip():
      raise _CliError('stdin (-) is empty; expected Beam YAML pipeline', EXIT_USAGE)
    resolved_args = _resolve_config_args(args, pipeline_args)
    if args.allow_stdin_jinja and not args.no_jinja:
      return _run_yaml(
          ['--yaml_pipeline=' + stdin_yaml] + resolved_args,
          args.verbose,
          command_name='run',
          dry_run_yaml=stdin_yaml if args.dry_run else None,
          wait_timeout_sec=wait_timeout_sec,
          json_session=json_session,
          t0=t0)
    return _run_yaml(
        resolved_args,
        args.verbose,
        direct_yaml_text=stdin_yaml,
        command_name='run',
        dry_run_yaml=stdin_yaml if args.dry_run else None,
        wait_timeout_sec=wait_timeout_sec,
        json_session=json_session,
        t0=t0)

  if '://' not in file and not os.path.isfile(file):
    hint = ''
    if pipeline_args:
      hint = '\n  custom pipeline options go after FILE, written as --flag=value'
    raise _CliError(f'no such file: {file!r}{hint}', EXIT_USAGE)

  resolved_args = _resolve_config_args(args, pipeline_args)
  if args.no_jinja and '://' not in file:
    with open(file, encoding='utf-8') as f:
      raw_yaml = f.read()
    return _run_yaml(
        resolved_args,
        args.verbose,
        direct_yaml_text=raw_yaml,
        command_name='run',
        dry_run_yaml=raw_yaml if args.dry_run else None,
        wait_timeout_sec=wait_timeout_sec,
        json_session=json_session,
        t0=t0)

  dry_text = None
  if args.dry_run and '://' not in file:
    with open(file, encoding='utf-8') as f:
      dry_text = f.read()

  return _run_yaml(
      ['--yaml_pipeline_file=' + file] + resolved_args,
      args.verbose,
      command_name='run',
      dry_run_yaml=dry_text,
      wait_timeout_sec=wait_timeout_sec,
      json_session=json_session,
      t0=t0)


def _pipe(
    args,
    pipeline_args,
    pipe_argv,
    *,
    raw_argv: list[str],
    json_session,
    t0: float,
) -> int:
  """Runs `beam pipe`; `pipe_argv` is its command line, for `--dry-run`."""
  from . import cost_guard
  from . import jobs
  from . import pipe_spec

  wait_timeout_sec = None
  if args.wait_timeout:
    try:
      wait_timeout_sec = jobs.parse_duration_seconds(args.wait_timeout)
    except ValueError as e:
      raise _CliError(str(e), EXIT_USAGE) from None

  resolved_args = _resolve_config_args(args, pipeline_args)
  default_proj = _extract_project_from_args(resolved_args)
  explicit_runner = _extract_runner_from_args(resolved_args)
  is_local_runner = _runner_name(explicit_runner) in _LOCAL_RUNNERS

  # Track row counters whenever `--json` or `--assert-counts` is active, except
  # when `--dry-run` is emitting clean minimal YAML without `--assert-counts`.
  want_counters = bool(
      args.assert_counts or (json_session.enabled and not args.dry_run))
  prefer_py_bq = bool(is_local_runner and not args.dry_run)

  try:
    pipe = pipe_spec.build_pipe_spec(
        args.source,
        args.sink,
        args.from_format or args.format,
        args.to_format or args.format,
        where=args.where,
        select=args.select,
        dlq_sink=args.dlq,
        dlq_format=args.dlq_format,
        if_exists=args.if_exists,
        from_opts=args.from_opts,
        to_opts=args.to_opts,
        from_opt_file=args.from_opt_file,
        to_opt_file=args.to_opt_file,
        default_project=default_proj,
        include_counters=want_counters,
        prefer_python_bigquery_sink=prefer_py_bq)
  except pipe_spec.PipeSpecError as e:
    raise _CliError(str(e), EXIT_USAGE) from None

  if pipe.is_unbounded and is_local_runner and not args.dry_run and not wait_timeout_sec:
    raise _CliError(
        f'source {args.source!r} is an unbounded stream; pass '
        '--wait-timeout <DURATION> for a bounded local run, use `beam tail` '
        'for a non-destructive stream peek, or specify a remote runner '
        '(--runner=DataflowRunner)',
        EXIT_USAGE)

  # Pre-flight `--max-bytes` guardrail (`EXIT_COST_LIMIT = 8`) before running!
  if args.max_bytes and pipe.source_endpoint is not None:
    try:
      cost_guard.check_max_bytes(
          pipe.source_endpoint, args.max_bytes, default_project=default_proj)
    except cost_guard.CostLimitError as e:
      raise _CliError(
          str(e),
          EXIT_COST_LIMIT,
          extra={
              'estimated_bytes': e.estimated_bytes,
              'limit_bytes': e.limit_bytes,
              'outputs': {
                  'estimated_bytes': e.estimated_bytes,
                  'limit_bytes': e.limit_bytes,
              },
          }) from None
    except ValueError as e:
      raise _CliError(str(e), EXIT_USAGE) from None

  # Local existing-output check runs before importing `apache_beam` (`<50 ms`),
  # skipped on `--dry-run` / `--emit-yaml` which never writes output!
  if (pipe.output_prefix is not None and '://' not in pipe.output_prefix and
      args.if_exists == 'fail' and not args.dry_run):
    _refuse_existing_local_output(pipe.output_prefix)

  import yaml
  dumper = getattr(yaml, 'CSafeDumper', yaml.SafeDumper)
  yaml_str = yaml.dump(pipe.spec, Dumper=dumper, sort_keys=False)
  canonical_spec = json.dumps(pipe.spec, sort_keys=True, separators=(',', ':'))
  plan_hash = 'sha256:' + hashlib.sha256(canonical_spec.encode('utf-8')).hexdigest()

  local_check_paths = [args.source, args.sink]
  if args.dlq:
    local_check_paths.append(args.dlq)

  post_checks = {
      'assert_counts': bool(args.assert_counts),
      'verify_diff': {
          'left': args.source,
          'right': args.sink,
          'keys': list(args.verify_keys),
      } if args.verify else None,
  }

  return _run_yaml(
      resolved_args,
      args.verbose,
      direct_yaml_text=yaml_str,
      command_name='pipe',
      local_paths=local_check_paths,
      dry_run_yaml=pipe_spec.to_yaml(pipe.spec, pipe_argv)
      if args.dry_run else None,
      output_prefix=pipe.output_prefix,
      if_exists=args.if_exists,
      dlq_prefix=(
          pipe.dlq_endpoint.output_prefix if pipe.dlq_endpoint else None),
      requires_java_xlang=pipe.requires_java_xlang,
      wait_timeout_sec=wait_timeout_sec,
      raw_argv_for_idem=raw_argv,
      plan_hash=plan_hash,
      post_checks=post_checks,
      json_session=json_session,
      t0=t0)


def _run_yaml(
    yaml_or_pipeline_argv: list[str],
    verbose: bool,
    *,
    direct_yaml_text: str | None = None,
    command_name: str = 'run',
    local_paths=(),
    dry_run_yaml: str | None = None,
    output_prefix: str | None = None,
    if_exists: str = 'fail',
    dlq_prefix: str | None = None,
    requires_java_xlang: bool = False,
    wait_timeout_sec: float | None = None,
    raw_argv_for_idem: list[str] | None = None,
    plan_hash: str | None = None,
    post_checks: dict[str, Any] | None = None,
    json_session=None,
    t0: float = 0.0,
) -> int:
  import contextlib
  import threading
  from . import jobs

  try:
    import apache_beam as beam
    from apache_beam.options.pipeline_options import CrossLanguageOptions
    from apache_beam.options.pipeline_options import GoogleCloudOptions
    from apache_beam.options.pipeline_options import StandardOptions
    from apache_beam.runners.runner import PipelineState
    from apache_beam.utils import subprocess_server
    from apache_beam.yaml import main as yaml_main
  except ImportError as e:
    _maybe_print_traceback(verbose)
    raise _CliError(
        f'a required package is missing: {e}\n'
        "  Beam YAML needs the [yaml] extra: pip install 'apache-beam[yaml]'",
        EXIT_MISSING_PACKAGE) from None

  # Bound local Java XLang expansion heap to 256MB when user hasn't set JVM opts.
  prev_java_opts = os.environ.get('JAVA_TOOL_OPTIONS')
  set_temp_java_opts = bool(requires_java_xlang and not prev_java_opts)
  if set_temp_java_opts:
    os.environ['JAVA_TOOL_OPTIONS'] = '-Xmx256m'

  pipeline = None
  result = None
  warnings_list: list[str] = []
  try:
    with contextlib.ExitStack() as stack:
      try:
        if direct_yaml_text is not None:
          # Safe direct YAML compile: bypasses `expand_jinja()` completely so
          # untrusted `--where`, file paths, or stdin YAML cannot trigger SSTI!
          options, constructor = (
              yaml_main.build_pipeline_components_from_yaml(
                  direct_yaml_text,
                  yaml_or_pipeline_argv,
                  validate_schema='generic'))
          display_data = {'yaml': direct_yaml_text}
        else:
          options, constructor, display_data = (
              yaml_main.build_pipeline_components_from_argv(
                  yaml_or_pipeline_argv))

        stack.enter_context(
            subprocess_server.JavaJarServer.beam_services(
                options.view_as(CrossLanguageOptions).beam_services))
        stack.enter_context(
            subprocess_server.SubprocessServer.cache_subprocesses())
        pipeline = beam.Pipeline(options=options, display_data=display_data)
        if output_prefix is not None and if_exists == 'fail' and dry_run_yaml is None:
          _refuse_existing_output(output_prefix, verbose)
        elif (output_prefix is not None and if_exists == 'replace' and
              dry_run_yaml is None):
          _remove_existing_output_for_replace(output_prefix)
        constructor(pipeline)
      except _CliError:
        raise
      except ImportError as e:
        _maybe_print_traceback(verbose)
        raise _CliError(
            f'a required package is missing: {e}\n'
            '  install the extra the pipeline needs, for example: '
            "pip install 'apache-beam[yaml,gcp]'",
            EXIT_MISSING_PACKAGE) from None
      except Exception as e:  # pylint: disable=broad-except
        _maybe_print_traceback(verbose)
        raise _CliError(
            f'could not build the pipeline: {e}', EXIT_USAGE) from None

      if local_paths:
        warnings_list.extend(_warn_if_local_paths(local_paths, options))

      if dry_run_yaml is not None:
        if json_session is not None and json_session.enabled:
          json_session.emit(
              output.build_envelope(
                  ok=True,
                  command=command_name,
                  exit_code=EXIT_OK,
                  state='DRY_RUN_VALIDATED',
                  duration_ms=int((time.monotonic() - t0) * 1000),
                  warnings=warnings_list,
                  outputs={
                      'yaml': dry_run_yaml,
                      'plan_hash': plan_hash,
                  }))
        else:
          print(dry_run_yaml, end='')
          print(
              'Dry run: the pipeline was built and checked but not run. To run '
              'it, save the YAML (for example with > pipeline.yaml) and use: '
              'beam run pipeline.yaml',
              file=sys.stderr)
        return EXIT_OK

      runner_raw = options.view_as(StandardOptions).runner
      is_local = _runner_name(runner_raw) in _LOCAL_RUNNERS
      no_wait = options.view_as(StandardOptions).no_wait_until_finish

      # Remote submission 30-minute idempotency check!
      idem_key: str | None = None
      if not is_local and raw_argv_for_idem and plan_hash:
        idem_key = jobs.compute_idempotency_key(raw_argv_for_idem, plan_hash)
        existing_rec = jobs.lookup_recent_submission(idem_key)
        if existing_rec is not None:
          if json_session is not None and json_session.enabled:
            json_session.emit(
                output.build_envelope(
                    ok=True,
                    command=command_name,
                    exit_code=EXIT_OK,
                    runner=existing_rec.get('runner'),
                    job_id=existing_rec.get('job_id'),
                    state=existing_rec.get('state'),
                    duration_ms=int((time.monotonic() - t0) * 1000),
                    warnings=warnings_list + [
                        'Re-attached to recent identical remote submission '
                        '(idempotency guard).'
                    ],
                    outputs={
                        **existing_rec.get('outputs', {}),
                        'deduplicated': True,
                    }))
          else:
            print(
                f"Re-attached to existing job {existing_rec['job_id']} "
                f"(state: {existing_rec.get('state')}).")
          return EXIT_OK

      state = None
      timed_out = False
      try:
        try:
          result = _execute_with_optional_timeout(
              pipeline,
              options=options,
              no_wait=no_wait,
              wait_timeout_sec=wait_timeout_sec,
              is_local=is_local)
        except NotImplementedError:
          # When Prism cannot bind a port in a restricted sandbox, DirectRunner
          # falls back to BundleBasedDirectRunner which rejects batched PyArrow
          # DoFns (`WriteToParquet`). Retry seamlessly on in-process FnApiRunner!
          if is_local and _runner_name(runner_raw) == 'direct':
            options.view_as(StandardOptions).runner = 'FnApiRunner'
            pipeline = beam.Pipeline(options=options, display_data=display_data)
            constructor(pipeline)
            result = _execute_with_optional_timeout(
                pipeline,
                options=options,
                no_wait=no_wait,
                wait_timeout_sec=wait_timeout_sec,
                is_local=True)
            options.view_as(StandardOptions).runner = runner_raw
          else:
            raise
        result, state, timed_out = result
      except KeyboardInterrupt:
        raise
      except Exception as e:  # pylint: disable=broad-except
        _maybe_print_traceback(verbose)
        more = '' if verbose else '\n  rerun with --verbose for the traceback'
        raise _CliError(
            f'pipeline failed: {_summarize(e)}{more}',
            EXIT_PIPELINE_FAILED) from None

    # Persist remote job record when a remote job ID exists!
    job_id = _extract_job_id(result)
    gcp_opts = options.view_as(GoogleCloudOptions)
    if not is_local and job_id:
      try:
        jobs.record_job_submission(
            job_id,
            runner=str(runner_raw or 'DataflowRunner'),
            state=str(state or getattr(result, 'state', 'RUNNING')),
            command=command_name,
            project=gcp_opts.project,
            region=gcp_opts.region,
            idem_key=idem_key,
            post_checks=post_checks,
            outputs={'output_prefix': output_prefix, 'dlq_prefix': dlq_prefix})
      except jobs.JobError:
        pass

    if timed_out:
      if is_local:
        raise _CliError(
            f'local pipeline exceeded --wait-timeout ({wait_timeout_sec}s) and '
            'was cancelled',
            EXIT_TIMEOUT)
      next_acts = [{
          'command': f'beam wait {job_id} --json',
          'description': 'Poll the detached remote job until completion.',
          'requires_human': False,
      }] if job_id else []
      raise _CliError(
          f'pipeline still running after --wait-timeout ({wait_timeout_sec}s); '
          f'detached remote job {job_id!r}',
          EXIT_TIMEOUT,
          extra={
              'detached': True,
              'job_id': job_id,
              'runner': str(runner_raw),
              'state': str( getattr(result, 'state', 'RUNNING')),
          },
          next_actions=next_acts)

    if no_wait:
      state_s = str(getattr(result, 'state', 'RUNNING'))
      if json_session is not None and json_session.enabled:
        json_session.emit(
            output.build_envelope(
                ok=True,
                command=command_name,
                exit_code=EXIT_OK,
                runner=str(runner_raw or 'DirectRunner'),
                job_id=job_id,
                state=state_s,
                duration_ms=int((time.monotonic() - t0) * 1000),
                warnings=warnings_list,
                outputs={'submitted': True}))
      else:
        print(
            f'Submitted {_describe_runner(options, result)} '
            f'(state: {state_s}; not waiting for completion).')
      return EXIT_OK

    if state != PipelineState.DONE:
      raise _CliError(
          f'pipeline finished in state {state}', EXIT_PIPELINE_FAILED)

    row_metrics = jobs.extract_beam_cli_counters(
        result.metrics() if hasattr(result, 'metrics') else None)

    if post_checks and post_checks.get('assert_counts'):
      err_msg = jobs.check_assert_counts(row_metrics)
      if err_msg:
        raise _CliError(err_msg, EXIT_PIPELINE_FAILED)

    outputs_dict: dict[str, Any] = {}
    written: list[str] = []
    if output_prefix is not None:
      written = _matching_files(
          output_prefix, verbose, error_exit_code=EXIT_PIPELINE_FAILED)
      outputs_dict['files'] = written
      outputs_dict['file_count'] = len(written)
    if dlq_prefix is not None:
      dlq_files = _matching_files(
          dlq_prefix, verbose, error_exit_code=EXIT_PIPELINE_FAILED)
      outputs_dict['dlq_files'] = dlq_files

    if post_checks and post_checks.get('verify_diff'):
      from . import diff
      v_cfg = post_checks['verify_diff']
      v_right = (
          output_prefix + '*'
          if output_prefix is not None else v_cfg['right'])
      diff_rc, diff_rep = diff.run_diff(
          v_cfg['left'],
          v_right,
          keys=v_cfg.get('keys'),
          default_project=gcp_opts.project)
      diff_rep.pop('untrusted_data', None)
      outputs_dict['verify'] = diff_rep
      if diff_rc != EXIT_OK:
        raise _CliError(
            f'--verify detected mismatch between {v_cfg["left"]} and {v_cfg["right"]}',
            EXIT_DIFF_MISMATCH,
            extra={'outputs': outputs_dict, 'metrics': row_metrics})

    if json_session is not None and json_session.enabled:
      json_session.emit(
          output.build_envelope(
              ok=True,
              command=command_name,
              exit_code=EXIT_OK,
              runner=_describe_runner(options, result),
              job_id=job_id,
              state=str(state),
              duration_ms=int((time.monotonic() - t0) * 1000),
              metrics=row_metrics,
              outputs=outputs_dict,
              warnings=warnings_list))
    else:
      print(f'Done. Ran {_describe_runner(options, result)}.')
      if output_prefix is not None:
        print(f'Wrote {len(written)} file{"" if len(written) == 1 else "s"}:')
        for path in written:
          print('  ' + path)
    return EXIT_OK
  finally:
    # Break frame reference cycles for PortableRunner `atexit` handlers.
    pipeline = None
    result = None
    if set_temp_java_opts:
      os.environ.pop('JAVA_TOOL_OPTIONS', None)
    del threading


def _execute_with_optional_timeout(
    pipeline,
    *,
    options,
    no_wait: bool,
    wait_timeout_sec: float | None,
    is_local: bool,
):
  del options
  if no_wait:
    res = pipeline.run()
    return res, None, False
  if wait_timeout_sec is None:
    res = pipeline.run()
    try:
      st = res.wait_until_finish()
      return res, st, False
    except KeyboardInterrupt:
      _cancel(res)
      raise

  import threading
  if not is_local:
    # Remote runners (e.g. DataflowRunner) stage artifacts and create the remote
    # job inside `pipeline.run()`; complete submission first so `job_id` is
    # always captured and persisted before bounding `wait_until_finish()`.
    res = pipeline.run()
    box_remote: dict[str, Any] = {'state': None, 'exc': None}

    def _wait_remote():
      try:
        box_remote['state'] = res.wait_until_finish()
      except BaseException as exc:  # pylint: disable=broad-except
        box_remote['exc'] = exc

    t_rem = threading.Thread(target=_wait_remote, daemon=True)
    t_rem.start()
    t_rem.join(timeout=wait_timeout_sec)
    if t_rem.is_alive():
      return res, getattr(res, 'state', 'RUNNING'), True
    if box_remote['exc'] is not None:
      raise box_remote['exc']
    return res, box_remote['state'], False

  box: dict[str, Any] = {'res': None, 'state': None, 'exc': None}

  def _waiter():
    try:
      r = pipeline.run()
      box['res'] = r
      box['state'] = r.wait_until_finish()
    except BaseException as exc:  # pylint: disable=broad-except
      box['exc'] = exc

  t = threading.Thread(target=_waiter, daemon=True)
  t.start()
  t.join(timeout=wait_timeout_sec)
  if t.is_alive():
    if box['res'] is not None:
      _cancel(box['res'])
    return box['res'], None, True
  if box['exc'] is not None:
    raise box['exc']
  return box['res'], box['state'], False


def _extract_job_id(result: Any) -> str | None:
  if result is None:
    return None
  for attr in ('job_id', '_job_id'):
    val = getattr(result, attr, None)
    if callable(val):
      try:
        val = val()
      except Exception:  # pylint: disable=broad-except
        val = None
    if isinstance(val, str) and val.strip():
      return val.strip()
  return None


def _remove_existing_output_for_replace(prefix: str) -> None:
  if '://' not in prefix:
    for p in glob.glob(glob.escape(prefix) + '*'):
      if os.path.isfile(p):
        os.unlink(p)
    return
  from apache_beam.io.filesystems import FileSystems
  matches = FileSystems.match([prefix + '*'])[0].metadata_list
  if matches:
    FileSystems.delete([m.path for m in matches])


def _format_existing_output_error(prefix, existing):
  shown = '\n'.join('  ' + path for path in existing[:5])
  more = f'\n  ... and {len(existing) - 5} more' if len(existing) > 5 else ''
  return _CliError(
      f'output already exists for {prefix!r}; beam pipe never overwrites '
      f'data:\n{shown}{more}\n'
      '  remove these files, choose another --to path, or pass '
      '--if-exists=replace',
      EXIT_USAGE)


def _refuse_existing_local_output(prefix):
  existing = sorted(
      p for p in glob.glob(glob.escape(prefix) + '*') if os.path.isfile(p))
  if existing:
    raise _format_existing_output_error(prefix, existing)


def _refuse_existing_output(prefix, verbose):
  """Fails if any file starts with the output prefix: pipe never overwrites."""
  existing = _matching_files(prefix, verbose, error_exit_code=EXIT_USAGE)
  if existing:
    raise _format_existing_output_error(prefix, existing)


def _matching_files(prefix, verbose, error_exit_code):
  from apache_beam.io.filesystem import BeamIOError
  from apache_beam.io.filesystems import FileSystems

  dot = (
      '.' +
      os.sep if '://' not in prefix and not os.path.dirname(prefix) else '')
  try:
    match = FileSystems.match([dot + prefix + '*'])[0]
  except (BeamIOError, ValueError) as e:
    _maybe_print_traceback(verbose)
    cause = (
        next(iter(e.exception_details.values()))
        if isinstance(e, BeamIOError) and e.exception_details else e)
    if (isinstance(cause, ImportError) or
        'dependencies are not installed' in str(cause) or
        'Unable to get filesystem from specified path' in str(cause)):
      raise _CliError(
          f'a required package is missing: {cause}',
          EXIT_MISSING_PACKAGE) from None
    raise _CliError(
        f'could not access {prefix!r}: {cause}', error_exit_code) from None
  return sorted(
      m.path[len(dot):] if dot and m.path.startswith(dot) else m.path
      for m in match.metadata_list)


def _runner_name(runner):
  """'DirectRunner', 'direct', None -> 'direct'; 'DataflowRunner' -> ..."""
  name = str(runner or 'DirectRunner').rsplit('.', 1)[-1].lower()
  return name[:-len('runner')] if name.endswith('runner') else name


_LOCAL_RUNNERS = frozenset(
    ['direct', 'switchingdirect', 'bundlebaseddirect', 'prism', 'fnapi'])


def _warn_if_local_paths(paths, options) -> list[str]:
  """Warns when a runner that isn't local would open local files."""
  from apache_beam.options.pipeline_options import StandardOptions
  runner = options.view_as(StandardOptions).runner
  local = [
      path for path in paths
      if '://' not in path or path.lower().startswith('file://')
  ]
  if local and _runner_name(runner) not in _LOCAL_RUNNERS:
    msg = (
        f'warning: {", ".join(local)}: local paths are opened by the '
        f"{runner} runner's workers, not by this machine; use storage every "
        'worker can reach (for example gs://, s3:// or hdfs://, or a shared '
        'mount at the same absolute path)')
    print(msg, file=sys.stderr)
    return [msg]
  return []


def _maybe_print_traceback(verbose):
  if verbose:
    traceback.print_exc()


_PYTHON_TRACEBACK = 'Traceback (most recent call last):'


def _summarize(error, max_lines=10):
  """Returns the part of a pipeline error that says what went wrong."""
  message = str(error)
  if type(error) is not RuntimeError and not message.startswith(
      type(error).__name__):
    message = f'{type(error).__name__}: {message}'
  lines = [
      line.rstrip() for line in message.splitlines()
      if line.strip() and not line[0].isspace()
  ]
  if any(_PYTHON_TRACEBACK in line for line in lines):
    context = lines[0].split(_PYTHON_TRACEBACK)[0].rstrip()
    causes = [
        line for line in lines if _PYTHON_TRACEBACK not in line and
        not line.startswith('During handling of the above exception')
    ]
    lines = [line for line in (context, causes[-1] if causes else '') if line]
  if not lines:
    return repr(error)
  shown = '\n  '.join(lines[:max_lines])
  return shown + ('\n  ...' if len(lines) > max_lines else '')


def _cancel(result):
  """Cancels the pipeline after Ctrl-C, saying whether that worked."""
  try:
    result.cancel()
    print('Cancelled the pipeline.', file=sys.stderr)
  except Exception as e:  # pylint: disable=broad-except
    print(
        f'Could not cancel the pipeline ({e}). If it is still running, '
        "stop it with the runner's tools.",
        file=sys.stderr)


def _describe_runner(options, result):
  """Says which runner ran the pipeline, for the last line of output."""
  from apache_beam.options.pipeline_options import StandardOptions
  runner = options.view_as(StandardOptions).runner
  if _runner_name(runner) != 'direct':
    return f'on {runner}'
  if 'portable_runner' in type(result).__module__:
    return 'locally on Prism'
  return 'locally on the Python direct runner'


if __name__ == '__main__':
  sys.exit(main())
