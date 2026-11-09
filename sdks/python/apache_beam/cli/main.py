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
from typing import Iterable

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
  beam pipe --from orders=out/orders.parquet --join tiers=data/vip_tiers.csv \\
      --sql "SELECT COALESCE(tiers.tier, 'STANDARD') AS tier, COUNT(*) AS order_count, ROUND(SUM(orders.amount), 2) AS total_amount FROM orders LEFT JOIN tiers USING (email) GROUP BY 1 ORDER BY 1" \\
      --to out/tier_spend.json --json
  beam pipe --from users.csv --to out/users.parquet --dry-run > pipeline.yaml

Formats: csv, json, parquet, avro, delta://, iceberg://, bigquery://,
postgres://, mysql://, spanner://, pubsub://, kafka://.
Every other --flag is passed as a Beam pipeline option.
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
    'package',
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
        '--cdc',
        '--zero-copy',
        '--assert-counts',
        '--verify',
        '--verbose',
        '--no-config',
        '--help',
    ]),
    'diff': frozenset(['--exit-zero', '--verbose', '--no-config', '--help']),
    'check': frozenset([
        '--plan',
        '--trust-providers',
        '--no-config',
        '--help',
    ]),
    'tail': frozenset(['-f', '--follow', '--help']),
    'package': frozenset([
        '--no-use-public-ips',
        '--no_use_public_ips',
        '--enable-streaming-engine',
        '--build-image',
        '--dry-run',
        '--dry_run',
        '--no-config',
        '--help',
    ]),
}
_CLI_ONE_FLAGS = {
    None: frozenset(['--output']),
    'run': frozenset(['--profile', '--wait-timeout']),
    'pipe': frozenset([
        '--from',
        '--join',
        '--to',
        '--sql',
        '--format',
        '--from-format',
        '--to-format',
        '--where',
        '--select',
        '--dlq',
        '--dlq-format',
        '--null-values',
        '--sql-engine',
        '--if-exists',
        '--from-opt',
        '--to-opt',
        '--from-opt-file',
        '--to-opt-file',
        '--max-bytes',
        '--verify-key',
        '--primary-key',
        '--cdc-format',
        '--cdc-op-col',
        '--cdc-seq-col',
        '--wait-timeout',
        '--profile',
    ]),
    'diff': frozenset([
        '--key',
        '--left-format',
        '--right-format',
        '--left-opt',
        '--right-opt',
        '--rtol',
        '--atol',
        '--columns',
        '--to',
        '--max-bytes',
        '--profile',
    ]),
    'check': frozenset(['--jinja-variables', '--profile']),
    'tail': frozenset([
        '-n',
        '--limit',
        '--duration',
        '--poll-interval',
        '--max-scanned-bytes',
        '--from-opt',
        '--project',
    ]),
    'package': frozenset([
        '--to',
        '--name',
        '--description',
        '--sdk',
        '--image',
        '--base-image',
        '--region',
        '--project',
        '--subnetwork',
        '--network',
        '--staging-location',
        '--temp-location',
        '--service-account-email',
        '--worker-machine-type',
        '--max-workers',
        '--num-workers',
        '--requirements',
        '--output-dir',
        '--profile',
    ]),
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
  output.DLQ_REQUESTED = any(
      a == '--dlq' or a.startswith('--dlq=') for a in raw_argv)
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
      if active_cmd == 'package':
        return _package(args, extra_args, json_session=json_session, t0=t0)

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


_TOP_LEVEL_EPILOG = """\
what you can do (beam --llms-txt prints the full agent manual):
  convert/filter files    beam pipe --from in.csv --to out.parquet \\
                            --where "amount > 100" --select "id, amount"
  small local jobs        run in-process in well under a second; larger or
                          remote jobs use a Beam runner (or pass --runner)
  bad records             --dlq bad.jsonl routes unparseable or failing rows
                          instead of failing the run; --assert-counts checks
                          rows_read = written + filtered + dlq
  nested fields           --where "user.tier == 'gold'" on JSON/Parquet/Avro
  NULLs                   SQL rules: --where "state != 'CA'" drops rows with
                          no state; empty CSV cells are NULL (--null-values
                          NA,NULL adds markers); test with IS [NOT] NULL
  SQL                     beam pipe --from t=in.csv --sql "SELECT ... FROM t"
                          (local SQLite with median/percentile/stddev; or
                          --sql-engine duckdb if installed, see beam doctor)
  CDC                     --cdc --primary-key id applies change events
                          (files keep the latest row per key)
  compare datasets        beam diff left.parquet right.parquet --key id
                          (schema drift, differing keys, value mismatches)
  look before moving      beam inspect FILE_OR_TABLE (schema, rows, sample)
  export for production   beam pipe ... --emit-yaml > pipeline.yaml, then
                          beam check pipeline.yaml and beam package
  machine-readable        add --json for one structured envelope on stdout
"""


def _build_parser():
  parser = argparse.ArgumentParser(
      prog='beam',
      description='Apache Beam command-line tool for humans and AI agents.',
      epilog=_TOP_LEVEL_EPILOG,
      formatter_class=argparse.RawDescriptionHelpFormatter,
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
      dest='sources',
      action='append',
      required=True,
      metavar='[ALIAS=]SOURCE',
      help=(
          'source path, glob, or URI (gs://, bigquery://, delta://, '
          'iceberg://, ...); repeatable with --sql'))
  pipe.add_argument(
      '--join',
      dest='joins',
      action='append',
      default=[],
      metavar='[ALIAS=]SOURCE',
      help='additional source URI to join in --sql (repeatable)')
  pipe.add_argument(
      '--to',
      dest='sink',
      required=False,
      default=None,
      metavar='SINK',
      help='destination path or URI (optional when running --sql queries)')
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
      help='safe SQL/Python row filter (e.g. "amount > 0 AND status = \'OK\'")')
  pipe.add_argument(
      '--select',
      metavar='EXPR',
      help=(
          'projection / scalar expressions (e.g. '
          '"CAST(id AS INT) AS id, lower(email) AS email")'))
  pipe.add_argument(
      '--sql',
      dest='sql',
      default=None,
      metavar='QUERY',
      help=(
          'read-only SQL SELECT/JOIN/GROUP BY query across --from and --join '
          'sources'))
  pipe.add_argument(
      '--dlq',
      metavar='SINK',
      help='dead-letter destination URI/path for corrupt or failing records')
  pipe.add_argument(
      '--dlq-format',
      metavar='FMT',
      help='dead-letter file format (default: json or inferred from extension)')
  pipe.add_argument(
      '--sql-engine',
      choices=['sqlite', 'duckdb'],
      default='sqlite',
      help='local engine for --sql: sqlite (default, built in) or duckdb '
      '(optional, pip install duckdb; adds PERCENTILE_CONT ... WITHIN GROUP, '
      'QUALIFY, struct fields). beam doctor shows which are installed')
  pipe.add_argument(
      '--null-values',
      metavar='LIST',
      help='comma-separated CSV cell values to read as NULL in addition to '
      'empty cells, for example NA,NULL,N/A')
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
      '--cdc',
      action='store_true',
      help='enable cross-engine Change Data Capture (UPSERT + DELETE propagation)')
  pipe.add_argument(
      '--primary-key',
      dest='primary_keys',
      action='append',
      default=[],
      metavar='COL',
      help='primary key column(s) for --cdc upserts/deletes (repeatable or comma-separated)')
  pipe.add_argument(
      '--cdc-format',
      choices=['auto', 'debezium', 'flat'],
      default='auto',
      help='CDC envelope format: auto (default), debezium, or flat')
  pipe.add_argument(
      '--cdc-op-col',
      metavar='COL',
      help='operation column name for flat CDC events (default: _change_type or op)')
  pipe.add_argument(
      '--cdc-seq-col',
      metavar='COL',
      help='sequence/version column name for flat CDC events')
  pipe.add_argument(
      '--zero-copy',
      action='store_true',
      help='register existing Parquet or delta:// files into iceberg:// without copying data')
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
      '--left-opt',
      dest='left_opts',
      action='append',
      default=[],
      metavar='KEY=VAL',
      help='left endpoint connector option (repeatable)')
  diff_p.add_argument(
      '--right-opt',
      dest='right_opts',
      action='append',
      default=[],
      metavar='KEY=VAL',
      help='right endpoint connector option (repeatable)')
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
      '--columns',
      choices=('shared', 'all'),
      default='shared',
      help=(
          'shared (default): compare columns present on both sides and report '
          'added/removed/retyped columns in schema_diff; all: compare the '
          'union, treating a missing column as null'))
  diff_p.add_argument(
      '--to',
      dest='sink',
      metavar='FILE',
      help='write the structured JSON reconciliation report to FILE')
  diff_p.add_argument(
      '--exit-zero',
      action='store_true',
      help='exit 0 even when datasets differ (drift is still in outputs.match)')
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
      help='non-destructively peek or follow (-f) a stream or dataset',
      allow_abbrev=False)
  tail_p.add_argument('uri', metavar='URI', help='stream or dataset URI')
  tail_p.add_argument(
      '-f',
      '--follow',
      action='store_true',
      help='follow new messages/rows continuously until Ctrl-C, -n, or --duration')
  tail_p.add_argument(
      '-n',
      '--limit',
      dest='limit',
      type=int,
      default=None,
      metavar='N',
      help='max messages/rows to sample (default: 5, or unlimited with -f)')
  tail_p.add_argument(
      '--duration',
      default=None,
      metavar='DURATION',
      help='peek/follow window (default: 5s, or unlimited with -f)')
  tail_p.add_argument(
      '--poll-interval',
      type=float,
      default=0.5,
      metavar='SEC',
      help='poll interval in seconds in -f follow mode (default: 0.5)')
  tail_p.add_argument(
      '--max-scanned-bytes',
      default='64MB',
      metavar='SIZE',
      help='byte cap for sampled payloads (default: 64MB)')
  tail_p.add_argument(
      '--from-opt',
      dest='from_opts',
      action='append',
      default=[],
      metavar='KEY=VAL',
      help='source connector option (repeatable)')
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

  # 13. `beam package`
  pkg_p = commands.add_parser(
      'package',
      help='package a .yaml or .py pipeline as a Dataflow Flex Template (<1s for YAML)',
      allow_abbrev=False)
  pkg_p.add_argument(
      'source',
      metavar='FILE_OR_STDIN',
      help='pipeline .yaml/.py path, gs:// URI, or - for stdin YAML')
  pkg_p.add_argument(
      '--to',
      dest='to_location',
      metavar='SPEC_URI',
      help='destination Flex Template spec JSON (gs://... or local path)')
  pkg_p.add_argument('--name', metavar='NAME', help='template name')
  pkg_p.add_argument(
      '--description', metavar='TEXT', help='template description')
  pkg_p.add_argument(
      '--sdk',
      default='auto',
      choices=['auto', 'yaml', 'python'],
      help='Flex Template SDK mode (default: auto)')
  pkg_p.add_argument(
      '--image',
      metavar='IMAGE_URI',
      help='container image URI (optional for YAML; Artifact Registry URI for Python)')
  pkg_p.add_argument(
      '--base-image',
      default=None,
      metavar='BASE_IMAGE',
      help='launcher base image for Python templates')
  pkg_p.add_argument(
      '--region',
      default='us-central1',
      metavar='REGION',
      help='default GCP region (default: us-central1)')
  pkg_p.add_argument('--project', metavar='PROJECT', help='GCP project')
  pkg_p.add_argument(
      '--subnetwork', metavar='SUBNET', help='default worker GCE subnetwork')
  pkg_p.add_argument(
      '--network', metavar='NETWORK', help='default worker GCE network')
  pkg_p.add_argument(
      '--no-use-public-ips',
      '--no_use_public_ips',
      dest='no_use_public_ips',
      action='store_true',
      help='set WORKER_IP_PRIVATE in template defaultEnvironment')
  pkg_p.add_argument(
      '--staging-location',
      metavar='GCS_URI',
      help='default staging location in template defaultEnvironment')
  pkg_p.add_argument(
      '--temp-location',
      metavar='GCS_URI',
      help='default temp location in template defaultEnvironment')
  pkg_p.add_argument(
      '--service-account-email',
      metavar='EMAIL',
      help='default worker service account email')
  pkg_p.add_argument(
      '--worker-machine-type',
      metavar='TYPE',
      help='default worker machine type')
  pkg_p.add_argument(
      '--max-workers', type=int, metavar='N', help='default max workers')
  pkg_p.add_argument(
      '--num-workers', type=int, metavar='N', help='default initial workers')
  pkg_p.add_argument(
      '--enable-streaming-engine',
      action='store_true',
      help='enable Streaming Engine in template defaultEnvironment')
  pkg_p.add_argument(
      '--requirements',
      dest='requirements_file',
      metavar='FILE',
      help='requirements.txt to bake into Python Flex Template image')
  pkg_p.add_argument(
      '--output-dir',
      metavar='DIR',
      help='local directory to write generated Dockerfile, metadata.json, and spec')
  pkg_p.add_argument(
      '--build-image',
      action='store_true',
      help='build and push Python Flex Template container via Cloud Build')
  pkg_p.add_argument(
      '--dry-run',
      '--dry_run',
      dest='dry_run',
      action='store_true',
      help='validate and preview ContainerSpec without writing files or building images')
  _add_config_selection_args(pkg_p)
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
  follow = bool(getattr(args, 'follow', False))
  eff_limit = args.limit if args.limit is not None else (None if follow else 5)
  eff_duration = (
      args.duration if args.duration is not None else (None if follow else '5s')
  )

  on_record = None
  if follow:
    if json_session.enabled:
      def _stream_ndjson(rec: dict[str, Any], idx: int) -> None:
        json_session.emit_stream_line({
            'event': 'record',
            'index': idx,
            'untrusted_data': True,
            'record': rec,
        })

      on_record = _stream_ndjson
    else:
      print(
          f'Following {args.uri} (non_destructive=True; Ctrl-C to stop)...',
          file=sys.stderr,
          flush=True)

      def _stream_text(rec: dict[str, Any], idx: int) -> None:
        del idx
        print(json.dumps(rec, sort_keys=True), flush=True)

      on_record = _stream_text

  try:
    info = tail.run_tail(
        args.uri,
        limit=eff_limit,
        duration=eff_duration,
        follow=follow,
        poll_interval=args.poll_interval,
        max_scanned_bytes=args.max_scanned_bytes,
        from_opts=args.from_opts,
        default_project=args.project,
        on_record=on_record)
  except tail.TailError as e:
    raise _CliError(str(e), e.exit_code) from None
  except ValueError as e:
    raise _CliError(str(e), EXIT_USAGE) from None
  except Exception as e:  # pylint: disable=broad-except
    raise _CliError(f'tail failed: {e}', EXIT_PIPELINE_FAILED) from None

  untrusted = info.pop('untrusted_data', None)
  interrupted = bool(info.get('interrupted', False))
  rc = EXIT_INTERRUPTED if interrupted else EXIT_OK
  if json_session.enabled:
    json_session.emit(
        output.build_envelope(
            ok=not interrupted,
            command='tail',
            exit_code=rc,
            duration_ms=int((time.monotonic() - t0) * 1000),
            outputs=info,
            untrusted_data=untrusted))
  elif not follow:
    print(
        f"Sampled {info['sampled_count']} record(s) from {info['uri']} "
        f"(mode={info['mode']}, non_destructive={info['non_destructive']}):")
    for r in (untrusted or {}).get('rows', ()):
      print('  ' + json.dumps(r, sort_keys=True))
  else:
    print(
        f"Stopped following {info['uri']} after {info['sampled_count']} "
        'record(s).',
        file=sys.stderr)
  return rc


def _extract_opt_from_resolved(resolved: list[str], *keys: str) -> str | None:
  key_set = set(keys)
  i = 0
  val: str | None = None
  while i < len(resolved):
    tok = resolved[i]
    if '=' in tok:
      k, v = tok.split('=', 1)
      if k in key_set:
        val = v
    elif tok in key_set and i + 1 < len(resolved):
      val = resolved[i + 1]
      i += 1
    i += 1
  return val


def _package(args, extra_args, *, json_session, t0: float) -> int:
  from . import package as pkg_mod
  _reject_extra_args(extra_args)
  cfg_args = _resolve_config_args(args, [])
  eff_project = args.project or _extract_opt_from_resolved(
      cfg_args, '--project')
  eff_region = args.region or _extract_opt_from_resolved(
      cfg_args, '--region') or pkg_mod.DEFAULT_REGION
  eff_subnet = args.subnetwork or _extract_opt_from_resolved(
      cfg_args, '--subnetwork')
  eff_staging = args.staging_location or _extract_opt_from_resolved(
      cfg_args, '--staging_location', '--staging-location')
  eff_temp = args.temp_location or _extract_opt_from_resolved(
      cfg_args, '--temp_location', '--temp-location')
  eff_no_pub = bool(
      args.no_use_public_ips or '--no_use_public_ips' in cfg_args or
      '--no-use-public-ips' in cfg_args)

  kwargs: dict[str, Any] = {
      'to_location': args.to_location,
      'name': args.name,
      'description': args.description,
      'sdk': args.sdk,
      'image': args.image,
      'region': eff_region,
      'project': eff_project,
      'subnetwork': eff_subnet,
      'network': args.network,
      'no_use_public_ips': eff_no_pub,
      'staging_location': eff_staging,
      'temp_location': eff_temp,
      'service_account_email': args.service_account_email,
      'worker_machine_type': args.worker_machine_type,
      'max_workers': args.max_workers,
      'num_workers': args.num_workers,
      'enable_streaming_engine': args.enable_streaming_engine,
      'requirements_file': args.requirements_file,
      'output_dir': args.output_dir,
      'build_image': args.build_image,
      'dry_run': args.dry_run,
  }
  if args.base_image:
    kwargs['base_image'] = args.base_image

  try:
    report = pkg_mod.run_package(args.source, **kwargs)
  except pkg_mod.PackageError as e:
    raise _CliError(str(e), e.exit_code) from None
  except Exception as e:  # pylint: disable=broad-except
    raise _CliError(f'package failed: {e}', EXIT_PIPELINE_FAILED) from None

  pkg_warnings = report.pop('warnings', None) or []
  if json_session.enabled:
    json_session.emit(
        output.build_envelope(
            ok=True,
            command='package',
            exit_code=EXIT_OK,
            duration_ms=int((time.monotonic() - t0) * 1000),
            warnings=pkg_warnings,
            outputs=report))
  else:
    docker_msg = (
        'yes (single-image Python container)'
        if report['docker_build_required'] else
        'no (zero-Docker managed YAML launcher)')
    print(f"Template      : {report['name']} (sdk={report['sdk_language']})")
    print(f'Docker Build  : {docker_msg}')
    print(f"Launcher Image: {report['image']}")
    if report.get('template_location'):
      print(f"Template Spec : {report['template_location']}")
    if report.get('written_files'):
      print('Written Files :')
      for wf in report['written_files']:
        print(f'  - {wf}')
    if report.get('parameters'):
      print(f"Parameters ({len(report['parameters'])}):")
      for p in report['parameters']:
        req_s = 'optional' if p.get('isOptional') else 'required'
        def_s = (
            f", default={p['defaultValue']!r}"
            if 'defaultValue' in p else '')
        print(f"  - {p['name']} ({req_s}{def_s})")
    for w in pkg_warnings:
      print(f'Warning       : {w}', file=sys.stderr)
    if args.dry_run:
      print('\nContainerSpec Preview:')
      print(json.dumps(report['container_spec'], indent=2, sort_keys=True))
    print('\nLaunch with gcloud:')
    print(f"  {report['launch']['gcloud']}")
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
        pipeline_args=resolved,
        verbose=bool(getattr(args, 'verbose', False)))
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

  check_warnings = (
      [report['plan_skipped_reason']] if report.get('plan_skipped_reason') else
      [])
  if json_session.enabled:
    json_session.emit(
        output.build_envelope(
            ok=True,
            command='check',
            exit_code=EXIT_OK,
            duration_ms=int((time.monotonic() - t0) * 1000),
            outputs=report,
            warnings=check_warnings))
  else:
    print(
        f"Valid Beam YAML ({report['mode']}): {report['transform_count']} "
        f"transform(s), plan_hash={report['plan_hash']}")
    for w in check_warnings:
      print(f'warning: {w}', file=sys.stderr)
  return EXIT_OK


def _diff(args, extra_args, *, json_session, t0: float) -> int:
  from . import cost_guard
  from . import diff
  from . import uris
  resolved_args = _maybe_stage_preview_worker_args(
      _resolve_config_args(args, extra_args))
  proj = _extract_project_from_args(resolved_args)
  diff_runner = _extract_runner_from_args(resolved_args) or 'FnApiRunner'
  try:
    rc, report = diff.run_diff(
        args.left,
        args.right,
        keys=args.keys,
        left_format=args.left_format,
        right_format=args.right_format,
        left_opts=args.left_opts,
        right_opts=args.right_opts,
        rtol=args.rtol,
        atol=args.atol,
        columns=args.columns,
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
  diff_job_id = report.pop('job_id', None)
  diff_state = report.pop('state', None)
  summary = report['summary']
  drift_detected = (rc != EXIT_OK)
  if getattr(args, 'sink', None):
    if '://' in args.sink or args.sink == '-':
      raise _CliError(
          'beam diff --to requires a local file path (for example out/diff.json)',
          EXIT_USAGE)
    from . import pipe_sql as _ps_diff
    _ps_diff.write_json_document(args.sink, report)
    report = {**report, 'file': args.sink}
  eff_rc = EXIT_OK if (drift_detected and getattr(args, 'exit_zero', False)) else rc
  if json_session.enabled:
    json_session.emit(
        output.build_envelope(
            ok=(eff_rc == EXIT_OK),
            command='diff',
            exit_code=eff_rc,
            runner=diff_runner,
            job_id=diff_job_id,
            state=diff_state,
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
            if eff_rc != EXIT_OK else None,
            untrusted_data=untrusted))
  else:
    if not drift_detected:
      print(
          f"MATCH: {summary['matched_rows']} row(s) identical between "
          f"{args.left} and {args.right}.")
    else:
      print(
          f"MISMATCH (exit {eff_rc}): left={summary['left_rows']}, "
          f"right={summary['right_rows']}, matched={summary['matched_rows']}, "
          f"only_in_left={summary['only_in_left']}, "
          f"only_in_right={summary['only_in_right']}, "
          f"value_mismatches={summary['value_mismatches']}")
  return eff_rc


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


def _maybe_stage_preview_worker_args(
    resolved_args: list[str], *, dry_run: bool = False) -> list[str]:
  """Auto-stages the preview `.whl` onto remote Dataflow workers when active."""
  if dry_run:
    return resolved_args
  if _runner_name(_extract_runner_from_args(resolved_args)) != 'dataflow':
    return resolved_args
  preview_launcher = sys.modules.get('apache_beam_cli_preview.launcher')
  if preview_launcher is not None and hasattr(
      preview_launcher, 'maybe_inject_dataflow_worker_args'):
    return preview_launcher.maybe_inject_dataflow_worker_args(resolved_args)
  return resolved_args


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
    resolved_args = _maybe_stage_preview_worker_args(
        _resolve_config_args(args, pipeline_args), dry_run=bool(args.dry_run))
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

  resolved_args = _maybe_stage_preview_worker_args(
      _resolve_config_args(args, pipeline_args), dry_run=bool(args.dry_run))
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


_PRISM_AVAILABLE_CACHE: bool | None = None


def _can_use_prism_locally() -> bool:
  """Probes in <0.1ms whether PrismRunner has an executable cached binary & localhost socket."""
  global _PRISM_AVAILABLE_CACHE
  if _PRISM_AVAILABLE_CACHE is not None:
    return _PRISM_AVAILABLE_CACHE
  import socket
  try:
    prism_bin_dir = os.path.expanduser('~/.apache_beam/cache/prism/bin')
    if not os.path.isdir(prism_bin_dir):
      _PRISM_AVAILABLE_CACHE = False
      return False
    bins = [
        os.path.join(prism_bin_dir, f) for f in os.listdir(prism_bin_dir)
        if not f.startswith('.')
    ]
    candidates = [b for b in bins if os.access(b, os.X_OK)]
    if not candidates:
      _PRISM_AVAILABLE_CACHE = False
      return False
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
      sock.bind(('127.0.0.1', 0))
    # The executable bit is not enough: sandboxes (and noexec home dirs) deny
    # exec of the cached binary with PermissionError, which used to surface as
    # a Prism startup traceback. Run it once, briefly, to be sure.
    _PRISM_AVAILABLE_CACHE = any(_prism_binary_runs(b) for b in candidates)
  except OSError:
    _PRISM_AVAILABLE_CACHE = False
  return _PRISM_AVAILABLE_CACHE


def _prism_binary_runs(path: str) -> bool:
  """True if the cached Prism binary at `path` can be executed."""
  import subprocess
  try:
    subprocess.run([path, '--help'],
                   stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL,
                   timeout=5,
                   check=False)
    return True
  except (OSError, subprocess.SubprocessError):
    return False


_EXACT_LOCAL_FILE_EXTENSIONS = frozenset([
    '.parquet',
    '.pq',
    '.json',
    '.jsonl',
    '.ndjson',
    '.csv',
    '.tsv',
    '.avro',
    '.txt',
])


def _materialize_single_local_file(
    prefix: str, written: list[str]) -> list[str]:
  """Materializes sharded local output onto exact `prefix` when explicit."""
  if not prefix or not written or '://' in prefix:
    return written
  if any(ch in prefix for ch in '*?['):
    return written
  ext = os.path.splitext(prefix)[1].lower()
  if ext not in _EXACT_LOCAL_FILE_EXTENSIONS:
    return written
  if os.path.exists(prefix):
    return written
  if not all(os.path.isfile(p) for p in written):
    return written

  if len(written) == 1:
    os.replace(written[0], prefix)
    return [prefix]

  total_bytes = sum(os.path.getsize(p) for p in written)
  if total_bytes > 256 * 1024 * 1024:
    return written

  tmp_path = f'{prefix}.tmp.{os.getpid()}'
  try:
    if ext in ('.parquet', '.pq'):
      import pyarrow as pa
      import pyarrow.parquet as pq
      tables = [pq.read_table(p) for p in written]
      merged = pa.concat_tables(tables, promote_options='default')
      pq.write_table(merged, tmp_path)
    elif ext in ('.csv', '.tsv'):
      with open(tmp_path, 'wb') as out_f:
        wrote_header = False
        for idx, p in enumerate(written):
          with open(p, 'rb') as in_f:
            if idx == 0 or not wrote_header:
              data = in_f.read()
              if data:
                out_f.write(data)
                if not data.endswith(b'\n'):
                  out_f.write(b'\n')
                wrote_header = True
            else:
              _ = in_f.readline()
              rest = in_f.read()
              if rest:
                out_f.write(rest)
                if not rest.endswith(b'\n'):
                  out_f.write(b'\n')
    elif ext == '.avro':
      import fastavro
      records = []
      schema = None
      for p in written:
        with open(p, 'rb') as in_f:
          rdr = fastavro.reader(in_f)
          if schema is None:
            schema = rdr.writer_schema
          records.extend(rdr)
      if schema is None:
        return written
      with open(tmp_path, 'wb') as out_f:
        fastavro.writer(out_f, schema, records)
    else:
      with open(tmp_path, 'wb') as out_f:
        for p in written:
          with open(p, 'rb') as in_f:
            data = in_f.read()
          if data:
            out_f.write(data)
            if not data.endswith(b'\n'):
              out_f.write(b'\n')
    os.replace(tmp_path, prefix)
    for p in written:
      if p != prefix and os.path.exists(p):
        try:
          os.unlink(p)
        except OSError:
          pass
    return [prefix]
  except Exception:  # pylint: disable=broad-except
    if os.path.exists(tmp_path):
      try:
        os.unlink(tmp_path)
      except OSError:
        pass
    return written


def _cleanup_empty_beam_temp_dirs(prefix: str | None) -> None:
  """Prunes empty `.temp*` sibling directories left by local FileBasedSink."""
  if not prefix or '://' in prefix:
    return
  clean_prefix = prefix.split('*', 1)[0].split('?', 1)[0]
  parent = os.path.dirname(clean_prefix) or '.'
  if not os.path.isdir(parent):
    return
  try:
    entries = os.listdir(parent)
  except OSError:
    return
  for name in entries:
    if not name.startswith('.temp'):
      continue
    cand = os.path.join(parent, name)
    if not os.path.isdir(cand) or os.path.islink(cand):
      continue
    for root, _dirs, _files in os.walk(cand, topdown=False):
      try:
        os.rmdir(root)
      except OSError:
        # Non-empty directory (e.g. concurrent job); never delete files!
        pass


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
  if args.sink == '-':
    raise _CliError(
        '--to - (standard output) is not supported: write to a file instead '
        '(for example --to out.jsonl) and preview it with beam inspect '
        'out.jsonl --sample 20; with --sql, omit --to to print the result '
        'rows', EXIT_USAGE)
  from . import cost_guard
  from . import jobs
  from . import pipe_spec

  wait_timeout_sec = None
  if args.wait_timeout:
    try:
      wait_timeout_sec = jobs.parse_duration_seconds(args.wait_timeout)
    except ValueError as e:
      raise _CliError(str(e), EXIT_USAGE) from None

  resolved_args = _maybe_stage_preview_worker_args(
      _resolve_config_args(args, pipeline_args), dry_run=bool(args.dry_run))
  default_proj = _extract_project_from_args(resolved_args)
  explicit_runner = _extract_runner_from_args(resolved_args)
  is_local_runner = _runner_name(explicit_runner) in _LOCAL_RUNNERS

  raw_sources = list(args.sources or [])
  raw_joins = list(args.joins or [])
  is_sql_mode = bool(args.sql is not None or raw_joins or len(raw_sources) > 1)
  allow_jinja = bool(args.dry_run)

  if is_sql_mode:
    from . import pipe_sql
    from . import uris
    if args.sql is None:
      raise _CliError(
          'multiple --from sources or --join requires --sql "SELECT ..."',
          EXIT_USAGE)
    if args.where or args.select or args.cdc or args.zero_copy:
      raise _CliError(
          '--sql cannot be combined with --where, --select, --cdc, or '
          '--zero-copy; express filters and projections inside --sql',
          EXIT_USAGE)
    try:
      parsed_from_opts = uris.parse_kv_options(
          args.from_opts, args.from_opt_file, '--from-opt')
      parsed_to_opts = uris.parse_kv_options(
          args.to_opts, args.to_opt_file, '--to-opt')
      named_sources = pipe_sql.parse_named_sources(
          raw_sources,
          raw_joins,
          from_format=args.from_format or args.format,
          from_opts=parsed_from_opts,
          default_project=default_proj,
          allow_sniff=not allow_jinja,
          allow_jinja=allow_jinja)
      sink_ep = (
          uris.parse_endpoint(
              args.sink,
              is_read=False,
              explicit_format=args.to_format or args.format,
              options=parsed_to_opts,
              if_exists=args.if_exists,
              default_project=default_proj,
              allow_jinja=allow_jinja) if args.sink else None)
      dlq_sql_default_fmt = None
      if args.dlq and not args.dlq_format:
        if not (uris.extension_of(args.dlq) or (
            '://' in args.dlq and
            args.dlq.split('://', 1)[0].lower() not in uris.FILE_SCHEMES)):
          dlq_sql_default_fmt = (
              (uris._infer_jinja_file_format(args.dlq) or 'json')
              if allow_jinja and uris._has_jinja_syntax(args.dlq) else 'json')
      dlq_ep = (
          uris.parse_endpoint(
              args.dlq,
              is_read=False,
              explicit_format=args.dlq_format or dlq_sql_default_fmt,
              if_exists='append' if args.if_exists == 'append' else 'fail',
              default_project=default_proj,
              allow_jinja=allow_jinja) if args.dlq else None)
      sql_spec = pipe_sql.build_sql_yaml_spec(
          named_sources, args.sql, sink_ep, allow_jinja=allow_jinja)
    except (uris.UriError, pipe_sql.SqlPipeError) as e:
      raise _CliError(str(e), EXIT_USAGE) from None
    sql_null_values = _split_null_values(args.null_values)
    if sql_null_values:
      for ns in named_sources:
        if ns.endpoint.kind == 'file' and ns.endpoint.format == 'csv':
          ns.endpoint.config['null_values'] = sql_null_values

    if args.max_bytes:
      for ns in named_sources:
        try:
          cost_guard.check_max_bytes(
              ns.endpoint, args.max_bytes, default_project=default_proj)
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

    import yaml
    dumper = getattr(yaml, 'CSafeDumper', yaml.SafeDumper)
    sql_yaml_str = yaml.dump(
        sql_spec, Dumper=dumper, sort_keys=False, width=100000)
    sql_canon = json.dumps(sql_spec, sort_keys=True, separators=(',', ':'))
    sql_plan_hash = (
        'sha256:' + hashlib.sha256(sql_canon.encode('utf-8')).hexdigest())

    local_sql_paths = [ns.uri for ns in named_sources]
    if args.sink:
      local_sql_paths.append(args.sink)
    if args.dlq:
      local_sql_paths.append(args.dlq)

    if args.dry_run or not is_local_runner or (
        sink_ep is not None and sink_ep.kind != 'file'):
      if sink_ep is None and not args.dry_run:
        raise _CliError(
            'remote --sql execution requires --to <SINK>', EXIT_USAGE)
      if sink_ep is not None and 'key' in (sink_ep.config or {}):
        raise _CliError(
            '--to-opt key= (one keyed JSON document) is only for local --sql '
            'runs, not --emit-yaml or remote runners', EXIT_USAGE)
      non_portable = (
          pipe_sql.local_only_sql_functions(args.sql)
          if args.sql_engine == 'sqlite' else [])
      if args.sql_engine != 'sqlite':
        print(
            f'note: --sql-engine {args.sql_engine} applies to local runs; the '
            "exported or remote pipeline uses Beam SQL's dialect (Calcite)",
            file=sys.stderr)
      if non_portable:
        print(
            'warning: --sql uses ' + ', '.join(non_portable) + ', which run '
            "locally but are not in Beam SQL's remote dialect (Calcite); the "
            'exported or remote pipeline may fail to build',
            file=sys.stderr)
      return _run_yaml(
          resolved_args,
          args.verbose,
          direct_yaml_text=sql_yaml_str,
          command_name='pipe',
          local_paths=local_sql_paths,
          dry_run_yaml=pipe_spec.to_yaml(sql_spec, pipe_argv)
          if args.dry_run else None,
          output_prefix=sink_ep.output_prefix if sink_ep else None,
          if_exists=args.if_exists,
          dlq_prefix=dlq_ep.output_prefix if dlq_ep else None,
          requires_java_xlang=True,
          wait_timeout_sec=wait_timeout_sec,
          raw_argv_for_idem=raw_argv,
          plan_hash=sql_plan_hash,
          json_session=json_session,
          t0=t0)

    # Local `<100ms` zero-JVM authorizer-sandboxed `:memory:` SQLite execution!
    if any('://' in p for p in local_sql_paths):
      from apache_beam.io.filesystems import FileSystems
      from apache_beam.options.pipeline_options import PipelineOptions
      FileSystems.set_options(PipelineOptions(resolved_args))

    sink_prefix = (
        sink_ep.output_prefix or sink_ep.config.get('path')
        if sink_ep is not None else None)
    if sink_prefix and '://' not in sink_prefix:
      if args.if_exists == 'fail':
        _refuse_existing_local_output(sink_prefix)
      # --if-exists replace: the inputs (possibly the output itself, for an
      # in-place append) are read first; stale files go after the write.

    try:
      out_cols, out_rows, dlq_recs, total_read = pipe_sql.run_in_memory_sql(
          named_sources,
          args.sql,
          default_project=default_proj,
          enable_dlq=bool(dlq_ep is not None),
          engine=args.sql_engine)
      written_sql: list[str] = []
      sql_warnings: list[str] = []
      if sink_ep is not None:
        if '://' in (sink_prefix or ''):
          raise _CliError(
              f'remote sink {args.sink!r} with --sql requires '
              '--runner=DataflowRunner or a local destination path',
              EXIT_USAGE)
        written_sql = pipe_sql.write_rows_to_local_file(
            out_cols,
            out_rows,
            sink_ep,
            if_exists=args.if_exists,
            warnings=sql_warnings)
        if args.if_exists == 'replace' and sink_prefix:
          _remove_existing_output_for_replace(sink_prefix, keep=written_sql)
      dlq_written: list[str] = []
      if dlq_ep is not None:
        dlq_written = pipe_sql.write_rows_to_local_file(
            ['step', 'error_type', 'error', 'raw_payload'],
            dlq_recs,
            dlq_ep,
            if_exists='append' if args.if_exists == 'append' else 'replace')
    except pipe_sql.SqlEngineMissingError as e:
      raise _CliError(str(e), EXIT_MISSING_PACKAGE) from None
    except (pipe_sql.SqlPipeError, FileNotFoundError) as e:
      raise _CliError(str(e), EXIT_USAGE) from None
    except ImportError as e:
      raise _CliError(
          f'a required package is missing: {e}', EXIT_MISSING_PACKAGE) from None
    except Exception as e:  # pylint: disable=broad-except
      _maybe_print_traceback(args.verbose)
      raise _CliError(
          f'SQL pipeline failed: {_summarize(e)}',
          EXIT_PIPELINE_FAILED) from None

    sql_metrics = {
        'rows_read': int(total_read),
        'rows_written': int(len(out_rows)),
        'rows_filtered_out': 0,
        'dlq_rows': int(len(dlq_recs)),
    }
    sql_outputs: dict[str, Any] = {
        'files': written_sql,
        'file_count': len(written_sql),
        'row_count': len(out_rows),
        'columns': out_cols,
        'plan_hash': sql_plan_hash,
        'sql_engine': args.sql_engine,
    }
    if dlq_written:
      sql_outputs['dlq_files'] = dlq_written

    if json_session.enabled:
      preview_limit = 500
      json_session.emit(
          output.build_envelope(
              ok=True,
              command='pipe',
              exit_code=EXIT_OK,
              runner='local_sql',
              state='DONE',
              duration_ms=int((time.monotonic() - t0) * 1000),
              metrics=sql_metrics,
              outputs=sql_outputs,
              warnings=sql_warnings,
              untrusted_data=output.wrap_untrusted_rows(
                  out_rows[:preview_limit],
                  truncated=len(out_rows) > preview_limit)))
    else:
      for warning in sql_warnings:
        print(f'warning: {warning}', file=sys.stderr)
      if written_sql:
        engine_name = 'DuckDB' if args.sql_engine == 'duckdb' else 'SQLite'
        print(f'Done. Ran locally on the {engine_name} SQL engine.')
        print(
            f'Wrote {len(written_sql)} '
            f'file{"" if len(written_sql) == 1 else "s"}:')
        for path in written_sql:
          print('  ' + path)
      else:
        for r in out_rows:
          print(json.dumps(r, default=str))
    return EXIT_OK

  if not args.sink:
    raise _CliError('the following arguments are required: --to', EXIT_USAGE)

  source_uri = raw_sources[0]
  if '=' in source_uri and not os.path.exists(source_uri):
    from . import pipe_sql as _ps_mod
    m_alias = _ps_mod._ALIAS_PREFIX_RE.match(source_uri)
    if m_alias and m_alias.group('uri').strip():
      source_uri = m_alias.group('uri').strip()

  # Track row counters whenever `--json` or `--assert-counts` is active, except
  # when `--dry-run` is emitting clean minimal YAML without `--assert-counts`.
  want_counters = bool(
      args.assert_counts or (json_session.enabled and not args.dry_run))
  prefer_py_bq = bool(not args.dry_run or args.dlq)

  # A local glob over several formats (`data/*` with .csv and .jsonl files)
  # is read file by file on the in-process path; elsewhere it's an error
  # instead of reading every file with the first file's format.
  from . import uris as uris_mod
  mixed_groups = ({} if (args.from_format or args.format or args.cdc) else
                  uris_mod.local_format_groups(source_uri))
  mixed_from_format = next(iter(mixed_groups), None)

  try:
    pipe = pipe_spec.build_pipe_spec(
        source_uri,
        args.sink,
        args.from_format or args.format or mixed_from_format,
        args.to_format or args.format,
        where=args.where,
        select=args.select,
        dlq_sink=args.dlq,
        dlq_format=args.dlq_format,
        null_values=_split_null_values(args.null_values),
        if_exists=args.if_exists,
        from_opts=args.from_opts,
        to_opts=args.to_opts,
        from_opt_file=args.from_opt_file,
        to_opt_file=args.to_opt_file,
        default_project=default_proj,
        include_counters=want_counters,
        prefer_python_bigquery_sink=prefer_py_bq,
        cdc=bool(args.cdc),
        primary_keys=args.primary_keys,
        cdc_format=args.cdc_format,
        cdc_op_col=args.cdc_op_col,
        cdc_seq_col=args.cdc_seq_col,
        zero_copy=bool(args.zero_copy),
        prefer_python_lakehouse_and_cdc=bool(not args.dry_run),
        allow_jinja=allow_jinja)
  except pipe_spec.PipeSpecError as e:
    raise _CliError(str(e), EXIT_USAGE) from None

  if pipe.is_unbounded and is_local_runner and not args.dry_run and not wait_timeout_sec:
    raise _CliError(
        f'source {source_uri!r} is an unbounded stream; pass '
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

  # Zero-Copy Parquet / Delta Lake -> Apache Iceberg v2 metadata registration
  # (`<100 ms` pure-Python manifest synthesis with `bytes_copied == 0`).
  if args.zero_copy and not args.dry_run:
    from . import lakehouse
    if ('://' in source_uri or '://' in args.sink or any(
        '://' in str(x) for x in (args.to_opts or ()))):
      from apache_beam.io.filesystems import FileSystems
      from apache_beam.options.pipeline_options import PipelineOptions
      FileSystems.set_options(PipelineOptions(resolved_args))
    try:
      zc_res = lakehouse.register_zero_copy_to_iceberg(
          pipe.source_endpoint,
          pipe.sink_endpoint,
          if_exists=args.if_exists,
          default_project=default_proj)
    except (lakehouse.LakehouseError, FileNotFoundError, ValueError) as e:
      raise _CliError(str(e), EXIT_USAGE) from None
    except ImportError as e:
      raise _CliError(
          f'a required package is missing: {e}', EXIT_MISSING_PACKAGE) from None
    except Exception as e:  # pylint: disable=broad-except
      _maybe_print_traceback(args.verbose)
      raise _CliError(
          f'zero-copy registration failed: {_summarize(e)}',
          EXIT_PIPELINE_FAILED) from None

    zc_metrics = {
        'rows_read': int(zc_res['rows_registered']),
        'rows_written': int(zc_res['rows_registered']),
        'rows_filtered_out': 0,
        'dlq_rows': 0,
        'bytes_copied': 0,
    }
    if args.verify:
      from . import diff
      diff_rc, diff_rep = diff.run_diff(
          source_uri,
          args.sink,
          keys=args.verify_keys,
          left_opts=args.from_opts,
          right_opts=args.to_opts,
          pipeline_args=resolved_args,
          default_project=default_proj)
      diff_rep.pop('untrusted_data', None)
      zc_res['verify'] = diff_rep
      if diff_rc != EXIT_OK:
        raise _CliError(
            f'--verify detected mismatch between {source_uri} and {args.sink}',
            EXIT_DIFF_MISMATCH,
            extra={'outputs': zc_res, 'metrics': zc_metrics})

    if json_session.enabled:
      json_session.emit(
          output.build_envelope(
              ok=True,
              command='pipe',
              exit_code=EXIT_OK,
              runner='zero_copy_metadata',
              state='DONE',
              duration_ms=int((time.monotonic() - t0) * 1000),
              metrics=zc_metrics,
              outputs=zc_res))
    else:
      print(
          f"Zero-copy registered {zc_res['rows_registered']} row(s) across "
          f"{len(zc_res['registered_files'])} Parquet file(s) into Iceberg v2 "
          f"at {zc_res['metadata_path']} (bytes_copied=0).")
    return EXIT_OK

  # Local existing-output check runs before importing `apache_beam` (`<50 ms`),
  # skipped on `--dry-run` / `--emit-yaml` which never writes output!
  if (pipe.output_prefix is not None and '://' not in pipe.output_prefix and
      args.if_exists == 'fail' and not args.dry_run):
    _refuse_existing_local_output(pipe.output_prefix)

  # Small local file-to-file jobs run in-process, skipping the ~2 s Beam
  # runner start-up (see fast_local.py for what qualifies).
  from . import fast_local
  fast_files = fast_local.plan(
      pipe,
      explicit_runner=explicit_runner,
      dry_run=bool(args.dry_run),
      cdc=bool(args.cdc),
      zero_copy=bool(args.zero_copy),
      verify=bool(args.verify),
      if_exists=args.if_exists,
      wait_timeout_sec=wait_timeout_sec)
  if (fast_files is None and pipe.sink_endpoint is not None and
      'key' in (pipe.sink_endpoint.config or {})):
    raise _CliError(
        '--to-opt key= (one keyed JSON document) works for small local runs '
        'and local --sql only; without it, --to writes JSON lines',
        EXIT_USAGE)
  if mixed_groups and fast_files is None:
    counts = ', '.join(
        f'{len(files)} {fmt}' for fmt, files in mixed_groups.items())
    raise _CliError(
        f'--from {source_uri} matches files in several formats ({counts}). '
        'Mixed formats are read automatically only for small local jobs (up '
        'to 64 MB, an exact local output file, no --runner, --emit-yaml or '
        '--cdc). Otherwise use one glob per format, for example --from '
        'a=DIR/*.csv --join b=DIR/*.jsonl with --sql SELECT ... FROM a '
        'UNION ALL SELECT ... FROM b, or pass --from-format to read every '
        'file as one format', EXIT_USAGE)
  if fast_files is not None:
    fast_rc = _run_fast_local_pipe(
        pipe,
        fast_files,
        args,
        json_session=json_session,
        t0=t0,
        mixed_groups=mixed_groups)
    if fast_rc is not None:
      return fast_rc

  if args.if_exists == 'replace' and not args.dry_run:
    src_path = (pipe.source_endpoint.config.get('path')
                if pipe.source_endpoint is not None else None)
    if isinstance(src_path, str) and '://' not in src_path:
      from . import dlq as dlq_mod
      _refuse_replace_of_input(pipe.output_prefix, dlq_mod._match_paths(src_path))

  import yaml
  dumper = getattr(yaml, 'CSafeDumper', yaml.SafeDumper)
  yaml_str = yaml.dump(pipe.spec, Dumper=dumper, sort_keys=False, width=100000)
  canonical_spec = json.dumps(pipe.spec, sort_keys=True, separators=(',', ':'))
  plan_hash = 'sha256:' + hashlib.sha256(canonical_spec.encode('utf-8')).hexdigest()

  local_check_paths = [source_uri, args.sink]
  if args.dlq:
    local_check_paths.append(args.dlq)

  post_checks = {
      'assert_counts': bool(args.assert_counts),
      'verify_diff': {
          'left': source_uri,
          'right': args.sink,
          'keys': list(args.verify_keys),
          'left_opts': list(args.from_opts),
          'right_opts': list(args.to_opts),
      } if args.verify else None,
  }
  explicit_sharding = any(
      str(opt).split('=', 1)[0].strip() in ('num_shards', 'shard_name_template')
      for opt in (args.to_opts or ()))

  extra_warnings: list[str] = []
  if args.dry_run and _bigquery_write_disposition(pipe.spec) == 'WRITE_EMPTY':
    note = (
        'the BigQuery sink uses write_disposition: WRITE_EMPTY (the default, '
        '--if-exists fail): the job fails if the table already has rows and '
        'never overwrites data; pass --if-exists append to add rows or '
        '--if-exists replace to overwrite')
    extra_warnings.append(note)
    if not json_session.enabled:
      print(f'note: {note}', file=sys.stderr)

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
      explicit_sharding=explicit_sharding,
      extra_warnings=extra_warnings,
      json_session=json_session,
      t0=t0)


def _bigquery_write_disposition(spec: dict[str, Any]) -> str | None:
  """The write_disposition of the first BigQuery sink in a pipe spec."""
  for t in spec.get('pipeline', {}).get('transforms', []):
    if 'BigQuery' in str(t.get('type', '')) and str(
        t.get('type', '')).startswith(('Write', 'CliWrite')):
      return (t.get('config') or {}).get('write_disposition')
  return None


def _split_null_values(raw: str | None) -> list[str] | None:
  if not raw:
    return None
  return [v.strip() for v in raw.split(',') if v.strip()]


def _run_fast_local_pipe(
    pipe, files, args, *, json_session, t0, mixed_groups=None) -> int | None:
  """Runs a qualifying `beam pipe` in-process; None falls back to Beam."""
  from . import fast_local
  from . import jobs
  canonical_spec = json.dumps(pipe.spec, sort_keys=True, separators=(',', ':'))
  plan_hash = 'sha256:' + hashlib.sha256(canonical_spec.encode('utf-8')).hexdigest()
  try:
    res = fast_local.run(pipe, files, mixed_groups=mixed_groups)
  except ImportError as e:
    if mixed_groups:
      raise _CliError(
          f'a required package is missing: {e}', EXIT_MISSING_PACKAGE) from None
    # pyarrow/fastavro missing: the Beam runner reports it properly.
    return None
  except fast_local.FastPipeError as e:
    raise _CliError(f'pipeline failed: {e}', EXIT_PIPELINE_FAILED) from None
  except (ValueError, FileNotFoundError) as e:
    raise _CliError(
        f'could not build the pipeline: {e}', EXIT_USAGE) from None
  except Exception as e:  # pylint: disable=broad-except
    _maybe_print_traceback(args.verbose)
    raise _CliError(
        f'pipeline failed: {_summarize(e)}', EXIT_PIPELINE_FAILED) from None

  if args.assert_counts:
    err_msg = jobs.check_assert_counts(res.metrics)
    if err_msg:
      raise _CliError(err_msg, EXIT_PIPELINE_FAILED)
  if args.if_exists == 'replace':
    # The inputs were read before the new output replaced the old one.
    _remove_existing_output_for_replace(pipe.output_prefix, keep=res.files)

  # Same housekeeping as the runner path: drop empty `.temp*` dirs left by
  # earlier Beam runs next to the outputs.
  for prefix in (pipe.output_prefix,
                 pipe.dlq_endpoint.output_prefix if pipe.dlq_endpoint else None):
    if prefix:
      _cleanup_empty_beam_temp_dirs(prefix)

  outputs: dict[str, Any] = {
      'files': res.files,
      'file_count': len(res.files),
      'plan_hash': plan_hash,
  }
  if pipe.dlq_endpoint is not None:
    outputs['dlq_files'] = res.dlq_files
  if json_session.enabled:
    json_session.emit(
        output.build_envelope(
            ok=True,
            command='pipe',
            exit_code=EXIT_OK,
            runner=fast_local.RUNNER_NAME,
            state='DONE',
            duration_ms=int((time.monotonic() - t0) * 1000),
            metrics=res.metrics,
            outputs=outputs,
            warnings=list(res.warnings)))
  else:
    for warning in res.warnings:
      print(f'warning: {warning}', file=sys.stderr)
    print(
        'Done. Ran locally in-process (small local job; set '
        f'{fast_local.DISABLE_ENV_VAR}=1 or pass --runner to use a Beam '
        'runner).')
    print(f'Wrote {len(res.files)} file{"" if len(res.files) == 1 else "s"}:')
    for path in res.files:
      print('  ' + path)
  return EXIT_OK


def _dry_run_offline_ok(exc: BaseException, yaml_text: str) -> bool:
  """True if a dry run failed only for lack of cloud access and the YAML
  passes the offline static check (`beam check` without --plan)."""
  from . import check_cmd
  if not check_cmd._is_remote_access_error(exc):
    return False
  import tempfile
  with tempfile.NamedTemporaryFile(
      'w', suffix='.yaml', delete=False, encoding='utf-8') as f:
    f.write(yaml_text)
    path = f.name
  try:
    check_cmd.run_check(path)
    return True
  except check_cmd.CheckError:
    return False
  finally:
    os.unlink(path)


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
    explicit_sharding: bool = False,
    extra_warnings: list[str] | None = None,
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
    from apache_beam.options.pipeline_options import PipelineOptions
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

  from . import dlq as _dlq_mod
  _dlq_mod.install_keyword_safe_row_hooks()

  # Bound local Java XLang expansion heap to 256MB when user hasn't set JVM opts.
  prev_java_opts = os.environ.get('JAVA_TOOL_OPTIONS')
  set_temp_java_opts = bool(requires_java_xlang and not prev_java_opts)
  if set_temp_java_opts:
    os.environ['JAVA_TOOL_OPTIONS'] = '-Xmx256m'

  pipeline = None
  result = None
  warnings_list: list[str] = list(extra_warnings or ())
  orig_runner_raw = None
  switched_to_fnapi = False
  try:
    with contextlib.ExitStack() as stack:
      skip_xlang_dry_expand = False
      if dry_run_yaml is not None and '{{' in dry_run_yaml:
        from . import package as pkg_mod
        try:
          pkg_mod._validate_yaml_with_jinja_placeholders(dry_run_yaml)
          pkg_mod.extract_jinja_parameters(dry_run_yaml)
        except pkg_mod.PackageError as e:
          raise _CliError(str(e), e.exit_code) from None
        skip_xlang_dry_expand = True
      elif dry_run_yaml is not None and requires_java_xlang:
        from . import sql as sql_mod
        try:
          sql_mod._find_java()
        except sql_mod.SqlError:
          skip_xlang_dry_expand = True

      if skip_xlang_dry_expand:
        options = PipelineOptions(yaml_or_pipeline_argv)
      else:
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

          orig_runner_raw = options.view_as(StandardOptions).runner
          if (dry_run_yaml is None and _runner_name(orig_runner_raw) == 'direct'
              and not requires_java_xlang and not _can_use_prism_locally()):
            options.view_as(StandardOptions).runner = 'FnApiRunner'
            switched_to_fnapi = True

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
          if dry_run_yaml is not None and requires_java_xlang and any(
              tok in str(e) for tok in (
                  'Unknown type or missing provider',
                  'Unknown transform type',
                  'Unknown type or urn',
                  'Java',
                  'Service failed to start')):
            options = PipelineOptions(yaml_or_pipeline_argv)
          elif (dry_run_yaml is not None and command_name == 'pipe' and (
              isinstance(e, FileNotFoundError) or any(
                  tok in str(e) for tok in (
                      'Unable to get schema',
                      'Found no files',
                      'No files found',
                      'no such file',
                      'does not exist',
                      'empty or has no header')))):
            options = PipelineOptions(yaml_or_pipeline_argv)
            warnings_list.append(
                'Source files do not exist yet; emitted portable YAML without '
                'live schema inspection.')
          elif dry_run_yaml is not None and _dry_run_offline_ok(
              e, dry_run_yaml):
            # Like `beam check --plan`: building needs cloud storage or
            # credentials that aren't available, but the YAML is valid.
            options = PipelineOptions(yaml_or_pipeline_argv)
            warnings_list.append(
                'dry run passed (offline static validation); building the full '
                'pipeline was skipped because it needs access to remote storage '
                'or credentials that are not available here')
          else:
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

      runner_raw = (
          orig_runner_raw if switched_to_fnapi else
          options.view_as(StandardOptions).runner)
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
      subproc_logger = logging.getLogger('apache_beam.utils.subprocess_server')
      suppress_prism_log = bool(
          not verbose and is_local and _runner_name(runner_raw) == 'direct')
      prev_subproc_level = subproc_logger.level
      if suppress_prism_log:
        subproc_logger.setLevel(logging.CRITICAL)
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
        finally:
          if switched_to_fnapi:
            options.view_as(StandardOptions).runner = orig_runner_raw
          if suppress_prism_log:
            subproc_logger.setLevel(prev_subproc_level)
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
              'state': str(getattr(result, 'state', 'RUNNING')),
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
    allow_materialize = bool(
        is_local and command_name == 'pipe' and if_exists != 'append' and
        not explicit_sharding)
    if output_prefix is not None:
      written = _matching_files(
          output_prefix, verbose, error_exit_code=EXIT_PIPELINE_FAILED)
      if allow_materialize:
        written = _materialize_single_local_file(output_prefix, written)
      if is_local:
        _cleanup_empty_beam_temp_dirs(output_prefix)
      outputs_dict['files'] = written
      outputs_dict['file_count'] = len(written)
    if dlq_prefix is not None:
      dlq_files = _matching_files(
          dlq_prefix, verbose, error_exit_code=EXIT_PIPELINE_FAILED)
      if allow_materialize:
        dlq_files = _materialize_single_local_file(dlq_prefix, dlq_files)
      if is_local:
        _cleanup_empty_beam_temp_dirs(dlq_prefix)
      outputs_dict['dlq_files'] = dlq_files
    if is_local and local_paths:
      for lp in local_paths:
        _cleanup_empty_beam_temp_dirs(lp)

    if post_checks and post_checks.get('verify_diff'):
      from . import diff
      v_cfg = post_checks['verify_diff']
      v_right = (
          written[0] if len(written) == 1 else
          (output_prefix + '*' if output_prefix is not None else v_cfg['right']))
      diff_rc, diff_rep = diff.run_diff(
          v_cfg['left'],
          v_right,
          keys=v_cfg.get('keys'),
          left_opts=v_cfg.get('left_opts'),
          right_opts=v_cfg.get('right_opts'),
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


def _output_files_for_replace(prefix: str) -> list[str]:
  """The files `--if-exists replace` may delete: the exact output file and
  its Beam shards (`PREFIX-00000-of-00004...`), never other sibling files
  such as `out.parquet.bak`."""
  if '://' not in prefix:
    found = [prefix] if os.path.isfile(prefix) else []
    found += [
        p for p in glob.glob(glob.escape(prefix) + '-*-of-*')
        if os.path.isfile(p)
    ]
    return found
  from apache_beam.io.filesystems import FileSystems
  found = [m.path for m in FileSystems.match([prefix])[0].metadata_list]
  found += [
      m.path for m in FileSystems.match([prefix + '-*-of-*'])[0].metadata_list
  ]
  return found


def _remove_existing_output_for_replace(
    prefix: str, keep: Iterable[str] = ()) -> None:
  keep_abs = {os.path.abspath(k) for k in keep if '://' not in k}
  targets = [
      p for p in _output_files_for_replace(prefix)
      if os.path.abspath(p) not in keep_abs and p not in keep
  ]
  if not targets:
    return
  if '://' not in prefix:
    for p in targets:
      os.unlink(p)
    return
  from apache_beam.io.filesystems import FileSystems
  FileSystems.delete(targets)


def _refuse_replace_of_input(prefix: str | None, input_files: Iterable[str]):
  """Runner paths delete the old output before reading: refuse in-place."""
  if not prefix:
    return
  outputs = {os.path.abspath(p) for p in _output_files_for_replace(prefix)}
  overlap = sorted(
      p for p in input_files if os.path.abspath(p) in outputs)
  if overlap:
    raise _CliError(
        f'--to {prefix} is also an input ({overlap[0]}); with --if-exists '
        'replace on this runner the old output is deleted before the inputs '
        'are read. Write to a new file and move it over the old one, or run '
        'it as a small local job (in-process or local --sql), which reads the '
        'inputs first', EXIT_USAGE)


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
  has_tb = _PYTHON_TRACEBACK in message
  lines = [
      line.strip() if not has_tb else line.rstrip()
      for line in message.splitlines()
      if line.strip() and (
          not line[0].isspace() if has_tb else not line.startswith('  '))
  ]
  if has_tb and any(_PYTHON_TRACEBACK in line for line in lines):
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
