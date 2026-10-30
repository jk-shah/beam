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

"""Entry point of the experimental `beam` command.

`beam run FILE` runs a Beam YAML pipeline file the same way as
`python -m apache_beam.yaml.main --yaml_pipeline_file=FILE`. It builds the
pipeline with that module's own functions, so Beam YAML features (Jinja
variables, providers, ...) and runners behave identically, and every flag the
command doesn't define is passed through unchanged as a Beam pipeline option.

`beam pipe --from SOURCE --to SINK` reads files in one format and writes them
in another, without writing a pipeline first. It builds a two-step Beam YAML
pipeline (see `pipe_spec`) and runs it like `beam run`. `--dry-run` prints
that pipeline instead, ready to save, edit and `beam run`.

`beam sql` launches the interactive Apache Beam SQL shell (`BeamSqlLine`),
caching shaded JARs in `~/.beam/cache/` compatibly with `scripts/beam-sql.sh`.

`beam config` manages named profiles of default `PipelineOptions` in
`~/.beam/config.yaml` (using Beam YAML's `options:` schema). Explicit CLI
flags and a pipeline file's `options:` block always override saved defaults,
and `--no-config` (`BEAM_NO_CONFIG=1`) ignores them.

On top of that, the CLI gives command-line users and scripts what they rely on:
errors without tracebacks unless `--verbose` is given, documented exit codes,
Ctrl-C cancelling the pipeline, and a last line naming the runner that ran it.
"""

import argparse
import logging
import os
import sys
import traceback

import yaml

import apache_beam as beam
from apache_beam.cli import config
from apache_beam.cli import pipe_spec
from apache_beam.cli import sql
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners.runner import PipelineState

# Exit codes, also listed in `--help`. Usage errors use 2, the code argparse
# itself exits with.
EXIT_OK = 0
EXIT_PIPELINE_FAILED = 1
EXIT_USAGE = 2
EXIT_MISSING_PACKAGE = 4
EXIT_INTERRUPTED = 130  # 128 + SIGINT, the shell convention for Ctrl-C.

_EXIT_CODES = """
exit codes:
  0    the pipeline finished (or --dry-run checked it)
  1    the pipeline failed
  2    usage error, or the pipeline could not be built
  4    a required package is missing
  130  interrupted (Ctrl-C)
"""

_RUN_EPILOG = """\
examples:
  beam run pipeline.yaml
  beam run pipeline.yaml --runner=dataflow --project=my-project \\
      --region=us-central1 --temp_location=gs://my-bucket/tmp

Every other --flag is passed to the pipeline as a Beam pipeline option.
Write them after FILE, as --flag=value.
""" + _EXIT_CODES

_PIPE_EPILOG = """\
examples:
  beam pipe --from users.csv --to out/users.json
  beam pipe --from 'data/*.parquet' --to out/data --to-format avro
  beam pipe --from users.csv --to out/users.json --dry-run > pipeline.yaml

Formats: csv, json and avro can be read and written; parquet can be read.
Paths can be local or on any Beam filesystem (gs://, s3://, ...). The
output path is a prefix: Beam writes sharded files that start with it, and
existing output is never overwritten.
Every other --flag is passed to the pipeline as a Beam pipeline option.
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
  beam config list
  beam config set runner PrismRunner --profile dev
  beam config use dev
  beam config unset temp_location

Stores named profiles in ~/.beam/config.yaml using Beam YAML's options:
mapping. Explicit CLI flags and a pipeline file's options: block always
override saved defaults. Pass --no-config (or set BEAM_NO_CONFIG=1) on
beam run, beam pipe, or beam sql to ignore ~/.beam/config.yaml.

exit codes:
  0    success
  1    option not set (beam config get)
  2    usage or validation error
"""


def main(argv=None):
  """Runs the `beam` command and returns its exit code."""
  argv = sys.argv[1:] if argv is None else argv
  args, extra_args = _build_parser().parse_known_args(argv)
  # The same logging setup as `python -m apache_beam.yaml.main`.
  logging.getLogger().setLevel(logging.INFO)
  try:
    if args.command == 'run':
      return _run(args, extra_args)
    if args.command == 'pipe':
      return _pipe(args, extra_args, argv[argv.index('pipe') + 1:])
    if args.command == 'sql':
      return _sql(args, extra_args)
    return _config(args, extra_args)
  except _CliError as e:
    print(f'error: {e}', file=sys.stderr)
    return e.exit_code
  except KeyboardInterrupt:
    print('error: interrupted', file=sys.stderr)
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


def _build_parser():
  parser = argparse.ArgumentParser(
      prog='beam',
      description='Apache Beam command-line tool (experimental).',
      allow_abbrev=False)
  commands = parser.add_subparsers(
      dest='command', required=True, metavar='COMMAND')
  # allow_abbrev=False on each parser, so that a pipeline option is never
  # taken for an abbreviation of one of this command's flags.
  run = commands.add_parser(
      'run',
      help='run a Beam YAML pipeline file',
      description=(
          'Run a Beam YAML pipeline file, like '
          'python -m apache_beam.yaml.main --yaml_pipeline_file=FILE.'),
      epilog=_RUN_EPILOG,
      formatter_class=argparse.RawDescriptionHelpFormatter,
      allow_abbrev=False)
  run.add_argument(
      'file',
      metavar='FILE',
      help='the pipeline file: a local path, or a path on any Beam '
      'filesystem such as gs://')
  run.add_argument(
      '--verbose', action='store_true', help='show tracebacks for errors')
  _add_config_selection_args(run)

  pipe = commands.add_parser(
      'pipe',
      help='read files in one format and write them in another',
      description=(
          'Read files in one format and write them in another, by building '
          'and running a two-step Beam YAML pipeline.'),
      epilog=_PIPE_EPILOG,
      formatter_class=argparse.RawDescriptionHelpFormatter,
      allow_abbrev=False)
  pipe.add_argument(
      '--from',
      dest='source',
      required=True,
      metavar='SOURCE',
      help='files to read: a path or glob')
  pipe.add_argument(
      '--to',
      dest='sink',
      required=True,
      metavar='SINK',
      help='where to write: a path prefix')
  pipe.add_argument(
      '--format',
      metavar='FMT',
      help='format of both sides; overridden by --from-format / --to-format')
  pipe.add_argument(
      '--from-format',
      metavar='FMT',
      help='format of the source (default: from the file extension)')
  pipe.add_argument(
      '--to-format',
      metavar='FMT',
      help='format of the sink (default: from the file extension)')
  # `--dry_run` is accepted and consumed too: Beam has a test-only pipeline
  # option of that name, which some runners honor and others ignore. Passed
  # through, it could run the pipeline the user asked not to run.
  pipe.add_argument(
      '--dry-run',
      '--dry_run',
      action='store_true',
      help='check the pipeline and print it as YAML instead of running it')
  pipe.add_argument(
      '--verbose', action='store_true', help='show tracebacks for errors')
  _add_config_selection_args(pipe)

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
  set_p.add_argument(
      'option',
      metavar='OPTION',
      help='PipelineOptions name (for example runner or temp_location)')
  set_p.add_argument('value', metavar='VALUE', help='option value')
  set_p.add_argument(
      '--profile',
      metavar='NAME',
      help='profile to modify (default: active profile)')

  get_p = actions.add_parser(
      'get', help='print a configured option value', allow_abbrev=False)
  get_p.add_argument('option', metavar='OPTION', help='PipelineOptions name')
  get_p.add_argument(
      '--profile',
      metavar='NAME',
      help='profile to read (default: active profile)')

  unset_p = actions.add_parser(
      'unset', help='remove a configured option', allow_abbrev=False)
  unset_p.add_argument('option', metavar='OPTION', help='PipelineOptions name')
  unset_p.add_argument(
      '--profile',
      metavar='NAME',
      help='profile to modify (default: active profile)')

  list_p = actions.add_parser(
      'list', help='show configured options for a profile', allow_abbrev=False)
  list_p.add_argument(
      '--profile',
      metavar='NAME',
      help='profile to list (default: active profile)')

  use_p = actions.add_parser(
      'use', help='switch the active profile', allow_abbrev=False)
  use_p.add_argument(
      'profile_name', metavar='PROFILE', help='profile name to make active')
  return parser


class _CliError(Exception):
  """An error reported to the user as one message, without a traceback."""
  def __init__(self, message, exit_code):
    super().__init__(message)
    self.exit_code = exit_code


def _config(args, extra_args):
  if extra_args:
    raise _CliError(
        f'unrecognized arguments: {" ".join(extra_args)}', EXIT_USAGE)
  try:
    return config.run_command(args)
  except config.ConfigError as e:
    raise _CliError(str(e), e.exit_code) from None


def _resolve_config_args(args, pipeline_args):
  try:
    return config.resolve_pipeline_args(
        pipeline_args, profile=args.profile, no_config=args.no_config)
  except config.ConfigError as e:
    raise _CliError(str(e), e.exit_code) from None


def _sql(args, sqlline_args):
  if sqlline_args and sqlline_args[0] == '--':
    sqlline_args = sqlline_args[1:]
  try:
    if args.list_versions or args.list_ios or args.list_runners:
      runner = args.runner or 'direct'
    else:
      runner = config.configured_sql_runner(
          args.runner, profile=args.profile, no_config=args.no_config)
    return sql.run(
        version=args.version,
        runner=runner,
        ios=args.ios,
        list_versions=args.list_versions,
        list_ios=args.list_ios,
        list_runners=args.list_runners,
        debug=args.debug,
        sqlline_args=sqlline_args)
  except (sql.SqlError, config.ConfigError) as e:
    raise _CliError(str(e), e.exit_code) from None


def _run(args, pipeline_args):
  file = args.file
  # Paths with a scheme (gs://, s3://, ...) are opened by Beam's filesystems
  # when the pipeline is built, which reports its own errors for them.
  if '://' not in file and not os.path.isfile(file):
    hint = ''
    if pipeline_args:
      # `beam run --runner dataflow p.yaml` makes argparse take "dataflow"
      # for FILE.
      hint = '\n  pipeline options go after FILE, written as --flag=value'
    raise _CliError(f'no such file: {file!r}{hint}', EXIT_USAGE)

  resolved_args = _resolve_config_args(args, pipeline_args)
  options, pipeline = _build(
      ['--yaml_pipeline_file=' + file] + resolved_args, args.verbose)
  result = _execute(pipeline, args.verbose)
  print(f'Done. Ran {_describe_runner(options, result)}.')
  return EXIT_OK


def _pipe(args, pipeline_args, pipe_argv):
  """Runs `beam pipe`; `pipe_argv` is its command line, for --dry-run."""
  try:
    pipe = pipe_spec.build_pipe_spec(
        args.source,
        args.sink,
        args.from_format or args.format,
        args.to_format or args.format)
  except pipe_spec.PipeSpecError as e:
    raise _CliError(str(e), EXIT_USAGE) from None

  # A dry run makes every check a real run makes, including building the
  # pipeline, so that it predicts whether `beam run` on its output will work.
  _refuse_existing_output(pipe.output_prefix)
  resolved_args = _resolve_config_args(args, pipeline_args)
  options, pipeline = _build(
      ['--yaml_pipeline=' + yaml.safe_dump(pipe.spec, sort_keys=False)] +
      resolved_args,
      args.verbose)
  _warn_if_local_paths([args.source, args.sink], options)
  if args.dry_run:
    # The YAML goes to stdout and every message to stderr, so that
    # `> pipeline.yaml` captures exactly the pipeline.
    print(pipe_spec.to_yaml(pipe.spec, pipe_argv), end='')
    print(
        'Dry run: the pipeline was built and checked but not run. To run '
        'it, save the YAML (for example with > pipeline.yaml) and use: '
        'beam run pipeline.yaml',
        file=sys.stderr)
    return EXIT_OK

  result = _execute(pipeline, args.verbose)
  print(f'Done. Ran {_describe_runner(options, result)}.')
  written = _matching_files(pipe.output_prefix)
  print(f'Wrote {len(written)} file{"" if len(written) == 1 else "s"}:')
  for path in written:
    print('  ' + path)
  return EXIT_OK


def _refuse_existing_output(prefix):
  """Fails if any file starts with the output prefix: pipe never overwrites."""
  existing = _matching_files(prefix)
  if existing:
    shown = '\n'.join('  ' + path for path in existing[:5])
    more = f'\n  ... and {len(existing) - 5} more' if len(existing) > 5 else ''
    raise _CliError(
        f'output already exists for {prefix!r}; beam pipe never overwrites '
        f'data:\n{shown}{more}\n'
        '  remove these files or choose another --to path',
        EXIT_USAGE)


def _matching_files(prefix):
  try:
    match = FileSystems.match([prefix + '*'])[0]
  except ValueError as e:
    # No filesystem handles the scheme, for example gs:// without the [gcp]
    # extra. Beam's message names the extra to install.
    raise _CliError(str(e), EXIT_MISSING_PACKAGE) from None
  return sorted(m.path for m in match.metadata_list)


def _runner_name(runner):
  """'DirectRunner', 'direct', None -> 'direct'; 'DataflowRunner' -> ...

  None means the SDK's default runner, DirectRunner.
  """
  name = str(runner or 'DirectRunner').rsplit('.', 1)[-1].lower()
  return name[:-len('runner')] if name.endswith('runner') else name


# Runners that execute on this machine, by _runner_name.
_LOCAL_RUNNERS = frozenset(
    ['direct', 'switchingdirect', 'bundlebaseddirect', 'prism', 'fnapi'])


def _warn_if_local_paths(paths, options):
  """Warns when a runner that isn't local would open local files.

  On a cluster the runner's workers open the files, so a local or relative
  path refers to each worker's own disk or container. The job can then
  succeed while reading nothing, or write where the user never looks.
  """
  runner = options.view_as(StandardOptions).runner
  local = [path for path in paths if '://' not in path]
  if local and _runner_name(runner) not in _LOCAL_RUNNERS:
    print(
        f'warning: {", ".join(local)}: local paths are opened by the '
        f"{runner} runner's workers, not by this machine; use storage every "
        'worker can reach (for example gs://, s3:// or hdfs://, or a shared '
        'mount at the same absolute path)',
        file=sys.stderr)


def _build(yaml_main_argv, verbose):
  """Builds a Beam YAML pipeline; returns (options, pipeline).

  `yaml_main_argv` is what `python -m apache_beam.yaml.main` would receive.
  Building also validates the YAML and expands cross-language transforms.
  """
  # Beam YAML is imported here, not at the top of the module, because it needs
  # the [yaml] extra; a missing extra is then reported with exit code 4 and a
  # hint instead of a traceback.
  try:
    from apache_beam.yaml import main as yaml_main
  except ImportError as e:
    _maybe_print_traceback(verbose)
    raise _CliError(
        f'a required package is missing: {e}\n'
        "  Beam YAML needs the [yaml] extra: pip install 'apache-beam[yaml]'",
        EXIT_MISSING_PACKAGE) from None

  # Build and run in two steps (yaml_main.run does both in a `with` block) so
  # that build errors and pipeline failures get different exit codes, and so
  # that `pipe --dry-run` can stop after building.
  try:
    options, constructor, display_data = (
        yaml_main.build_pipeline_components_from_argv(yaml_main_argv))
    pipeline = beam.Pipeline(options=options, display_data=display_data)
    constructor(pipeline)
  except ImportError as e:
    # A transform's extra is missing, for example [gcp] for BigQuery.
    _maybe_print_traceback(verbose)
    raise _CliError(
        f'a required package is missing: {e}\n'
        '  install the extra the pipeline needs, for example: '
        "pip install 'apache-beam[yaml,gcp]'",
        EXIT_MISSING_PACKAGE) from None
  except Exception as e:  # pylint: disable=broad-except
    _maybe_print_traceback(verbose)
    raise _CliError(f'could not build the pipeline: {e}', EXIT_USAGE) from None
  return options, pipeline


def _execute(pipeline, verbose):
  """Runs a built pipeline until it finishes; returns its result."""
  try:
    result = pipeline.run()
    try:
      state = result.wait_until_finish()
    except KeyboardInterrupt:
      # main() reports the interruption after the pipeline is cancelled.
      _cancel(result)
      raise
  except Exception as e:  # pylint: disable=broad-except
    _maybe_print_traceback(verbose)
    more = '' if verbose else '\n  rerun with --verbose for the traceback'
    raise _CliError(
        f'pipeline failed: {_summarize(e)}{more}',
        EXIT_PIPELINE_FAILED) from None
  if state != PipelineState.DONE:
    raise _CliError(f'pipeline finished in state {state}', EXIT_PIPELINE_FAILED)
  return result


def _maybe_print_traceback(verbose):
  if verbose:
    traceback.print_exc()


_PYTHON_TRACEBACK = 'Traceback (most recent call last):'


def _summarize(error, max_lines=10):
  """Returns the part of a pipeline error that says what went wrong.

  Runner errors often embed whole stack traces. Prism and the default runner
  put a line of context ("Pipeline job-001 failed in state FAILED: bundle ...
  failed:") before the worker's Python traceback, whose last line names the
  exception and the failing step; Java runners add `\\tat ...` frames. Stack
  frames are indented, so only unindented lines are kept, and for a Python
  traceback only the context and the last exception line. `--verbose` still
  prints everything.
  """
  message = str(error)
  if type(error) is not RuntimeError and not message.startswith(
      type(error).__name__):
    # Errors raised directly (for example a ZeroDivisionError from the
    # in-process runner) don't name their type in the message.
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
  """Cancels the pipeline after Ctrl-C, saying whether that worked.

  A local runner usually receives the same Ctrl-C and stops by itself, in
  which case cancelling fails harmlessly.
  """
  try:
    result.cancel()
    print('Cancelled the pipeline.', file=sys.stderr)
  except Exception as e:  # pylint: disable=broad-except
    print(
        f'Could not cancel the pipeline ({e}). If it is still running, '
        "stop it with the runner's tools.",
        file=sys.stderr)


def _describe_runner(options, result):
  """Says which runner ran the pipeline, for the last line of output.

  Without --runner (or with --runner=DirectRunner), the Python SDK runs the
  pipeline on Prism and falls back to the Python direct runner when Prism
  can't run it. The type of the result tells the two apart.
  """
  runner = options.view_as(StandardOptions).runner
  if _runner_name(runner) != 'direct':
    return f'on {runner}'
  if 'portable_runner' in type(result).__module__:
    return 'locally on Prism'
  return 'locally on the Python direct runner'


if __name__ == '__main__':
  sys.exit(main())
