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
import glob
import logging
import os
import sys
import traceback

from . import _option_arity

# Exit codes, also listed in `--help`. Usage errors use 2, the code argparse
# itself exits with.
EXIT_OK = 0
EXIT_PIPELINE_FAILED = 1
EXIT_USAGE = 2
EXIT_MISSING_PACKAGE = 4
EXIT_INTERRUPTED = 130  # 128 + SIGINT, the shell convention for Ctrl-C.

_EXIT_CODES = """
exit codes:
  0    the pipeline finished (or --dry-run checked it, or --no_wait_until_finish submitted it)
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
""" + _EXIT_CODES

_PIPE_EPILOG = """\
examples:
  beam pipe --from users.csv --to out/users.parquet
  beam pipe --from 'data/*.parquet' --to out/data --to-format json
  beam pipe --from users.csv --to out/users.parquet --dry-run > pipeline.yaml

Formats: csv, json, parquet, avro (from the file extension unless overridden).
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

_SUBCOMMANDS = frozenset(['run', 'pipe', 'sql', 'config'])
_PASSTHROUGH_SUBCOMMANDS = frozenset(['run', 'pipe'])
_CLI_ZERO_FLAGS = {
    None: frozenset(['--help']),
    'run': frozenset(['--verbose', '--no-config', '--help']),
    'pipe': frozenset(
        ['--dry-run', '--dry_run', '--verbose', '--no-config', '--help']),
}
_CLI_ONE_FLAGS = {
    None: frozenset(),
    'run': frozenset(['--profile']),
    'pipe': frozenset([
        '--from',
        '--to',
        '--format',
        '--from-format',
        '--to-format',
        '--profile',
    ]),
}


def _normalize_cli_argv(argv):
  return _option_arity.normalize_argv(
      argv,
      subcommands=_SUBCOMMANDS,
      passthrough_subcommands=_PASSTHROUGH_SUBCOMMANDS,
      cli_zero_flags_by_cmd=_CLI_ZERO_FLAGS,
      cli_one_flags_by_cmd=_CLI_ONE_FLAGS)


def main(argv=None):
  """Runs the `beam` command and returns its exit code."""
  raw_argv = list(sys.argv[1:] if argv is None else argv)
  norm_argv = _normalize_cli_argv(raw_argv)
  args, extra_args = _build_parser().parse_known_args(norm_argv)
  # The same logging setup as `python -m apache_beam.yaml.main`.
  logging.getLogger().setLevel(logging.INFO)
  try:
    if args.command == 'run':
      return _run(args, extra_args)
    if args.command == 'pipe':
      pipe_tail = (
          raw_argv[raw_argv.index('pipe') + 1:]
          if 'pipe' in raw_argv else norm_argv[1:])
      return _pipe(args, extra_args, pipe_tail)
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
  from . import config
  if extra_args:
    raise _CliError(
        f'unrecognized arguments: {" ".join(extra_args)}', EXIT_USAGE)
  try:
    return config.run_command(args)
  except config.ConfigError as e:
    raise _CliError(str(e), e.exit_code) from None


def _resolve_config_args(args, pipeline_args):
  from . import config
  try:
    return config.resolve_pipeline_args(
        pipeline_args, profile=args.profile, no_config=args.no_config)
  except config.ConfigError as e:
    raise _CliError(str(e), e.exit_code) from None


def _sql(args, sqlline_args):
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
      hint = '\n  custom pipeline options go after FILE, written as --flag=value'
    raise _CliError(f'no such file: {file!r}{hint}', EXIT_USAGE)

  resolved_args = _resolve_config_args(args, pipeline_args)
  return _run_yaml(['--yaml_pipeline_file=' + file] + resolved_args,
                   args.verbose)


def _pipe(args, pipeline_args, pipe_argv):
  """Runs `beam pipe`; `pipe_argv` is its command line, for --dry-run."""
  from . import pipe_spec
  try:
    pipe = pipe_spec.build_pipe_spec(
        args.source,
        args.sink,
        args.from_format or args.format,
        args.to_format or args.format)
  except pipe_spec.PipeSpecError as e:
    raise _CliError(str(e), EXIT_USAGE) from None

  # Local existing-output check runs before importing `apache_beam` so fast
  # local error paths stay `<50 ms`; remote filesystems (`gs://`, `s3://`, ...)
  # are checked inside `_run_yaml` after `Pipeline(options=...)` initializes
  # `FileSystems._pipeline_options`.
  if pipe.output_prefix is not None and '://' not in pipe.output_prefix:
    _refuse_existing_local_output(pipe.output_prefix)

  resolved_args = _resolve_config_args(args, pipeline_args)
  import yaml
  dumper = getattr(yaml, 'CSafeDumper', yaml.SafeDumper)
  return _run_yaml(
      ['--yaml_pipeline=' + yaml.dump(pipe.spec, Dumper=dumper, sort_keys=False)
       ] + resolved_args,
      args.verbose,
      local_paths=[args.source, args.sink],
      dry_run_yaml=pipe_spec.to_yaml(pipe.spec, pipe_argv)
      if args.dry_run else None,
      output_prefix=pipe.output_prefix)


def _run_yaml(
    yaml_main_argv,
    verbose,
    local_paths=(),
    dry_run_yaml=None,
    output_prefix=None):
  # Beam and Beam YAML are imported here, not at the top of the module, so
  # fast CLI paths remain instant and a missing [yaml] extra is reported with
  # exit code 4 and a hint instead of a traceback.
  import contextlib
  try:
    import apache_beam as beam
    from apache_beam.options.pipeline_options import CrossLanguageOptions
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

  # Build and run in two steps (yaml_main.run does both in a `with` block) so
  # that build errors and pipeline failures get different exit codes, and so
  # that `pipe --dry-run` can stop after building. Cache subprocess servers
  # across `constructor(pipeline)` and `pipeline.run()` via `ExitStack` rather
  # than `Pipeline.__exit__`, which auto-runs the pipeline when no exception is
  # passed.
  with contextlib.ExitStack() as stack:
    try:
      options, constructor, display_data = (
          yaml_main.build_pipeline_components_from_argv(yaml_main_argv))
      stack.enter_context(
          subprocess_server.JavaJarServer.beam_services(
              options.view_as(CrossLanguageOptions).beam_services))
      stack.enter_context(
          subprocess_server.SubprocessServer.cache_subprocesses())
      pipeline = beam.Pipeline(options=options, display_data=display_data)
      if output_prefix is not None:
        _refuse_existing_output(output_prefix, verbose)
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
      _warn_if_local_paths(local_paths, options)
    if dry_run_yaml is not None:
      print(dry_run_yaml, end='')
      print(
          'Dry run: the pipeline was built and checked but not run. To run '
          'it, save the YAML (for example with > pipeline.yaml) and use: '
          'beam run pipeline.yaml',
          file=sys.stderr)
      return EXIT_OK

    no_wait = options.view_as(StandardOptions).no_wait_until_finish
    state = None
    try:
      result = pipeline.run()
      if not no_wait:
        try:
          state = result.wait_until_finish()
        except KeyboardInterrupt:
          _cancel(result)
          raise
    except KeyboardInterrupt:
      raise
    except Exception as e:  # pylint: disable=broad-except
      _maybe_print_traceback(verbose)
      more = '' if verbose else '\n  rerun with --verbose for the traceback'
      raise _CliError(
          f'pipeline failed: {_summarize(e)}{more}',
          EXIT_PIPELINE_FAILED) from None

  if no_wait:
    print(
        f'Submitted {_describe_runner(options, result)} '
        f'(state: {result.state}; not waiting for completion).')
    return EXIT_OK
  if state != PipelineState.DONE:
    raise _CliError(f'pipeline finished in state {state}', EXIT_PIPELINE_FAILED)

  print(f'Done. Ran {_describe_runner(options, result)}.')
  if output_prefix is not None:
    written = _matching_files(
        output_prefix, verbose, error_exit_code=EXIT_PIPELINE_FAILED)
    print(f'Wrote {len(written)} file{"" if len(written) == 1 else "s"}:')
    for path in written:
      print('  ' + path)
  return EXIT_OK


def _format_existing_output_error(prefix, existing):
  shown = '\n'.join('  ' + path for path in existing[:5])
  more = f'\n  ... and {len(existing) - 5} more' if len(existing) > 5 else ''
  return _CliError(
      f'output already exists for {prefix!r}; beam pipe never overwrites '
      f'data:\n{shown}{more}\n'
      '  remove these files or choose another --to path',
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
  """'DirectRunner', 'direct', None -> 'direct'; 'DataflowRunner' -> ...

  None means the SDK's default runner, DirectRunner.
  """
  name = str(runner or 'DirectRunner').rsplit('.', 1)[-1].lower()
  return name[:-len('runner')] if name.endswith('runner') else name


# Runners that execute on this machine, by _runner_name.
_LOCAL_RUNNERS = frozenset(
    ['direct', 'switchingdirect', 'bundlebaseddirect', 'prism', 'fnapi'])


def _warn_if_local_paths(paths, options):
  """Warns when a runner that isn't local would open local files."""
  from apache_beam.options.pipeline_options import StandardOptions
  runner = options.view_as(StandardOptions).runner
  local = [
      path for path in paths
      if '://' not in path or path.lower().startswith('file://')
  ]
  if local and _runner_name(runner) not in _LOCAL_RUNNERS:
    print(
        f'warning: {", ".join(local)}: local paths are opened by the '
        f"{runner} runner's workers, not by this machine; use storage every "
        'worker can reach (for example gs://, s3:// or hdfs://, or a shared '
        'mount at the same absolute path)',
        file=sys.stderr)


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
