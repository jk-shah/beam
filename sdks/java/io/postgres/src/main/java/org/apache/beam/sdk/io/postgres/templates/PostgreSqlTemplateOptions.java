/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.postgres.templates;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;

/** Common pipeline options for PostgreSQL CDC streaming Dataflow templates. */
public interface PostgreSqlTemplateOptions extends PipelineOptions, StreamingOptions {

  @Description("PostgreSQL JDBC URL (e.g. jdbc:postgresql://10.0.0.5:5432/mydb)")
  @Required
  String getPostgresUrl();

  void setPostgresUrl(String value);

  @Description("PostgreSQL database username")
  @Required
  String getPostgresUsername();

  void setPostgresUsername(String value);

  @Description("PostgreSQL database password or Secret Manager secret resource ID")
  String getPostgresPassword();

  void setPostgresPassword(String value);

  @Description(
      "Cloud SQL Instance Connection Name (for IAM authentication, e.g. project:region:instance)")
  String getCloudSqlInstance();

  void setCloudSqlInstance(String value);

  @Description("AlloyDB Instance Connection Name (projects/p/locations/l/clusters/c/instances/i)")
  String getAlloyDbInstance();

  void setAlloyDbInstance(String value);

  @Description("Enable Google Cloud IAM database authentication")
  @Default.Boolean(false)
  Boolean getEnableIamAuth();

  void setEnableIamAuth(Boolean value);

  @Description("Google Cloud Secret Manager secret URI (sm://projects/p/secrets/s/versions/v)")
  String getSecretManagerUri();

  void setSecretManagerUri(String value);

  @Description("PostgreSQL replication origin filter: 'none', 'any', or custom origin name")
  String getOriginFilter();

  void setOriginFilter(String value);

  @Description("PostgreSQL CDC publication name (default: beam_publication)")
  @Default.String("beam_publication")
  String getPublicationName();

  void setPublicationName(String value);

  @Description("PostgreSQL CDC replication slot name (default: beam_slot)")
  @Default.String("beam_slot")
  String getReplicationSlotName();

  void setReplicationSlotName(String value);

  @Description(
      "Replication scope: 'DATABASE' (all schemas), 'SCHEMA' (single schema), or 'TABLE' (single table)")
  @Default.String("TABLE")
  String getReplicationScope();

  void setReplicationScope(String value);

  @Description("Target schema name when scope is SCHEMA (e.g. public)")
  String getTargetSchema();

  void setTargetSchema(String value);

  @Description("Target table name when scope is TABLE (e.g. public.orders)")
  String getTargetTable();

  void setTargetTable(String value);

  @Description("Comma-separated list of tables to include (overrides single table setting)")
  String getTableWhitelist();

  void setTableWhitelist(String value);

  @Description("Dead-Letter Queue (DLQ) output path (e.g. gs://my-bucket/dlq/ or BQ table)")
  String getDlqPath();

  void setDlqPath(String value);
}
