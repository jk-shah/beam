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

"""Static `PipelineOptions` flag arity tables for `<1 ms` argv normalization.

Importing `apache_beam.options.pipeline_options` loads `apache_beam` (~2s).
Keeping static frozensets of built-in zero-arg and single-arg `PipelineOptions`
flags (verified against `PipelineOptions.__subclasses__()` in
`options_arity_test.py`) lets `beam` accept space-separated flags such as
`--runner direct` or `--temp-location gs://bucket/tmp` anywhere on the command
line without importing the SDK.
"""

from typing import Iterable

# Exact 0-arg flags across `PipelineOptions.__subclasses__()` (verified by test).
PIPELINE_ZERO_ARG_FLAGS = frozenset([
    '--allow_non_deterministic_key_coders',
    '--allow_unsafe_triggers',
    '--auto_unique_labels',
    '--direct_embed_docker_python',
    '--disable_beartype',
    '--enable_artifact_caching',
    '--enable_bucket_read_metric_counter',
    '--enable_bucket_write_metric_counter',
    '--enable_gcsio_blob_generation',
    '--enable_hot_key_logging',
    '--enable_streaming_engine',
    '--exclude_infer_dataclass_field_type',
    '--flink_submit_uber_jar',
    '--force_cloudpickle_deterministic_coders',
    '--hdfs_full_urls',
    '--no_auth',
    '--no_direct_runner_use_stacked_bundle',
    '--no_gcsio_throttling_counter',
    '--no_pipeline_type_check',
    '--no_save_main_session',
    '--no_use_public_ips',
    '--no_wait_until_finish',
    '--performance_runtime_type_check',
    '--profile_cpu',
    '--profile_memory',
    '--profiler_stop_after_crash',
    '--requirements_cache_only_sources',
    '--runtime_type_check',
    '--s3_disable_ssl',
    '--save_main_session',
    '--spark_submit_uber_jar',
    '--streaming',
    '--update',
    '--use_public_ips',
    '--use_transform_service',
])

# Exact multi-arg (`nargs` in `('*', '+')` or `> 1`) flags in `PipelineOptions`.
PIPELINE_MULTI_ARG_FLAGS: frozenset[str] = frozenset()

# Exact 1-arg flags across `PipelineOptions.__subclasses__()` (verified by test).
PIPELINE_ONE_ARG_FLAGS = frozenset([
    '--artifact_endpoint',
    '--artifact_port',
    '--artifacts_dir',
    '--autoscaling_algorithm',
    '--azure_connection_string',
    '--azure_managed_identity_client_id',
    '--beam_plugin',
    '--beam_plugins',
    '--beam_services',
    '--blob_service_endpoint',
    '--cloud_build_machine_type',
    '--create_from_snapshot',
    '--dataflow_endpoint',
    '--dataflow_job_file',
    '--dataflow_kms_key',
    '--dataflow_service_option',
    '--dataflow_service_options',
    '--default_sdk_harness_log_level',
    '--direct_num_workers',
    '--direct_runner_bundle_repeat',
    '--direct_running_mode',
    '--direct_test_splits',
    '--disk_provisioned_iops',
    '--disk_provisioned_throughput_mibps',
    '--disk_size_gb',
    '--disk_type',
    '--docker_registry_push_url',
    '--dry_run',
    '--element_processing_timeout_minutes',
    '--environment_cache_millis',
    '--environment_config',
    '--environment_option',
    '--environment_options',
    '--environment_type',
    '--expansion_port',
    '--experiment',
    '--experiments',
    '--extra_package',
    '--extra_packages',
    '--files_to_stage',
    '--flexrs_goal',
    '--flink_job_server_jar',
    '--flink_master',
    '--flink_version',
    '--gbek',
    '--gcp_oauth_scope',
    '--gcp_oauth_scopes',
    '--gcs_custom_audit_entries',
    '--gcs_custom_audit_entry',
    '--hdfs_host',
    '--hdfs_port',
    '--hdfs_user',
    '--impersonate_service_account',
    '--jar_cache_dir',
    '--job-server-timeout',
    '--job_endpoint',
    '--job_name',
    '--job_port',
    '--job_server_java_launcher',
    '--job_server_jvm_properties',
    '--job_server_jvm_property',
    '--job_server_timeout',
    '--label',
    '--labels',
    '--machine_type',
    '--maven_repository_url',
    '--max_cache_memory_usage_mb',
    '--max_num_workers',
    '--max_parallelism',
    '--min_cpu_platform',
    '--network',
    '--num_workers',
    '--number_of_worker_harness_threads',
    '--on_success_matcher',
    '--output_executable_path',
    '--parallelism',
    '--pickle_library',
    '--prebuild_sdk_container_base_image',
    '--prebuild_sdk_container_engine',
    '--prism_beam_version_override',
    '--prism_location',
    '--prism_log_kind',
    '--prism_log_level',
    '--profile_location',
    '--profile_postprocess_interval_sec',
    '--profile_sample_rate',
    '--profile_temp_location',
    '--profile_upload_interval_sec',
    '--profiler_agent',
    '--profiler_extra_arg',
    '--profiler_extra_args',
    '--profiler_extra_env_var',
    '--profiler_extra_env_vars',
    '--profiler_stop_after_sec',
    '--project',
    '--pubsub_root_url',
    '--region',
    '--requirements_cache',
    '--requirements_file',
    '--resource_hint',
    '--resource_hints',
    '--runner',
    '--s3_access_key_id',
    '--s3_api_version',
    '--s3_endpoint_url',
    '--s3_region_name',
    '--s3_secret_access_key',
    '--s3_session_token',
    '--s3_verify',
    '--sdk_container_image',
    '--sdk_harness_container_image_overrides',
    '--sdk_harness_log_level_overrides',
    '--sdk_location',
    '--sdk_worker_parallelism',
    '--service_account_email',
    '--setup_file',
    '--spark_job_server_jar',
    '--spark_master_url',
    '--spark_rest_url',
    '--spark_version',
    '--staging_location',
    '--subnetwork',
    '--temp_location',
    '--template_location',
    '--transform_name_mapping',
    '--type_check_additional',
    '--type_check_strictness',
    '--update_compatibility_version',
    '--user_agent',
    '--wait_until_finish_duration',
    '--worker_disk_type',
    '--worker_harness_container_image',
    '--worker_machine_type',
    '--worker_region',
    '--worker_zone',
    '--yaml_experimental_features',
    '--zone',
])

# Extra `apache_beam.yaml.main` flags accepted on `beam run`.
YAML_ZERO_ARG_FLAGS = frozenset([
    '--tests',
    '--no-tests',
    '--test',
    '--no-test',
    '--fix_tests',
    '--no-fix_tests',
    '--create_test',
    '--no-create_test',
])

YAML_ONE_ARG_FLAGS = frozenset([
    '--json_schema_validation',
    '--jinja_variables',
    '--jinja_variable_flags',
    '--test_suite',
])

_ALL_ZERO_ARG = PIPELINE_ZERO_ARG_FLAGS | YAML_ZERO_ARG_FLAGS
_ALL_ONE_ARG = PIPELINE_ONE_ARG_FLAGS | YAML_ONE_ARG_FLAGS

# Map kebab-case / snake_case spellings to the exact canonical flag expected by
# `PipelineOptions` / `yaml.main` (e.g. `--temp-location` -> `--temp_location`).
_CANONICAL_ZERO: dict[str, str] = {}
for _f in _ALL_ZERO_ARG:
  _CANONICAL_ZERO[_f] = _f
  _CANONICAL_ZERO['--' + _f[2:].replace('_', '-')] = _f

_CANONICAL_ONE: dict[str, str] = {}
for _f in _ALL_ONE_ARG:
  _CANONICAL_ONE[_f] = _f
  _CANONICAL_ONE['--' + _f[2:].replace('_', '-')] = _f


def normalize_argv(
    argv: Iterable[str],
    subcommands: frozenset[str],
    passthrough_subcommands: frozenset[str],
    cli_zero_flags_by_cmd: dict[str | None, frozenset[str]],
    cli_one_flags_by_cmd: dict[str | None, frozenset[str]],
) -> list[str]:
  """Normalizes space-separated `PipelineOptions` flags in `<1 ms`.

  Rules:
  - Known CLI flags for the global parser or active subcommand ALWAYS win first.
  - Bare `-` (stdin) is treated as a positional argument, never a flag.
  - `--` stops option normalization and preserves all trailing tokens verbatim.
  - On subcommands in `passthrough_subcommands`, known `PipelineOptions` flags
    written as `--flag value` (or kebab-case `--temp-location value`) are joined
    into `--canonical_flag=value`, and any `PipelineOptions` flags given before
    the subcommand token are placed right after the subcommand token so
    `argparse.parse_known_args` preserves exact left-to-right option precedence.
  """
  tokens = list(argv)
  if not tokens:
    return tokens

  # First pass: locate `active_cmd` (if any) without misidentifying the value of
  # a 1-arg flag (such as `--profile run` or `--runner direct`) as a subcommand.
  global_zero = cli_zero_flags_by_cmd.get(None, frozenset())
  global_one = cli_one_flags_by_cmd.get(None, frozenset())
  active_cmd: str | None = None
  cmd_index = -1
  i = 0
  while i < len(tokens):
    tok = tokens[i]
    if tok == '--':
      break
    if tok in subcommands:
      active_cmd = tok
      cmd_index = i
      break
    if tok.startswith('--') and '=' not in tok:
      if tok in global_one or (tok not in global_zero and
                               tok in _CANONICAL_ONE):
        i += 2
        continue
    i += 1

  if active_cmd is None or active_cmd not in passthrough_subcommands:
    return tokens

  cmd_zero = global_zero | cli_zero_flags_by_cmd.get(active_cmd, frozenset())
  cmd_one = global_one | cli_one_flags_by_cmd.get(active_cmd, frozenset())

  pre_cli: list[str] = []
  pre_pipeline: list[str] = []
  post_tokens: list[str] = []

  def _scan_segment(seg: list[str], is_pre: bool) -> None:
    idx = 0
    while idx < len(seg):
      tok = seg[idx]
      if tok == '--':
        target = pre_cli if is_pre else post_tokens
        target.extend(seg[idx:])
        break
      if tok == '-' or not tok.startswith('-'):
        (pre_cli if is_pre else post_tokens).append(tok)
        idx += 1
        continue
      if not tok.startswith('--'):
        # Short flags like `-h` stay untouched in CLI tokens.
        (pre_cli if is_pre else post_tokens).append(tok)
        idx += 1
        continue

      flag_part, has_eq, eq_val = tok.partition('=')
      # 1. Subcommand / global CLI flags always take priority over PipelineOptions.
      if flag_part in cmd_zero:
        (pre_cli if is_pre else post_tokens).append(tok)
        idx += 1
        continue
      if flag_part in cmd_one:
        target = pre_cli if is_pre else post_tokens
        target.append(tok)
        if not has_eq and idx + 1 < len(seg):
          target.append(seg[idx + 1])
          idx += 2
        else:
          idx += 1
        continue

      # 2. Known PipelineOptions / Beam YAML flags.
      if flag_part in _CANONICAL_ZERO:
        canon = _CANONICAL_ZERO[flag_part]
        rewritten = f'{canon}={eq_val}' if has_eq else canon
        (pre_pipeline if is_pre else post_tokens).append(rewritten)
        idx += 1
        continue
      if flag_part in _CANONICAL_ONE:
        canon = _CANONICAL_ONE[flag_part]
        if has_eq:
          (pre_pipeline if is_pre else post_tokens).append(f'{canon}={eq_val}')
          idx += 1
        elif idx + 1 < len(seg) and seg[idx + 1] != '--':
          (pre_pipeline
           if is_pre else post_tokens).append(f'{canon}={seg[idx + 1]}')
          idx += 2
        else:
          # Missing value: pass bare flag so downstream reports usage error.
          (pre_pipeline if is_pre else post_tokens).append(canon)
          idx += 1
        continue

      # 3. Unknown `--custom` flag: leave untouched so either `argparse` flags a
      # pre-command typo or custom `PipelineOptions` subclasses parse it.
      (pre_cli if is_pre else post_tokens).append(tok)
      idx += 1

  _scan_segment(tokens[:cmd_index], is_pre=True)
  _scan_segment(tokens[cmd_index + 1:], is_pre=False)
  return pre_cli + [active_cmd] + pre_pipeline + post_tokens
