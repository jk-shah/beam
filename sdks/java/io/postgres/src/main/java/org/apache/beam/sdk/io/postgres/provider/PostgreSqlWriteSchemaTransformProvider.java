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
package org.apache.beam.sdk.io.postgres.provider;

import static org.apache.beam.sdk.util.construction.BeamUrns.getUrn;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.sdk.io.postgres.PostgreSQLIO;
import org.apache.beam.sdk.io.postgres.PostgreSqlDataSourceConfiguration;
import org.apache.beam.sdk.io.postgres.sink.PostgreSqlWrite;
import org.apache.beam.sdk.io.postgres.sink.PostgreSqlWrite.WriteMode;
import org.apache.beam.sdk.io.postgres.sink.PostgreSqlWriteResult;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;

/** SchemaTransformProvider for writing to PostgreSQL. */
@AutoService(SchemaTransformProvider.class)
public class PostgreSqlWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        PostgreSqlWriteSchemaTransformProvider.PostgreSqlWriteConfiguration> {

  public static final String INPUT_TAG = "input";
  public static final String OUTPUT_TAG = "output";

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class PostgreSqlWriteConfiguration implements Serializable {

    @SchemaFieldDescription("PostgreSQL JDBC connection URL (e.g. jdbc:postgresql://host:5432/db)")
    public abstract @Nullable String getUrl();

    @SchemaFieldDescription("Target table name (e.g. public.orders)")
    public abstract String getTable();

    @SchemaFieldDescription("Database username")
    public abstract @Nullable String getUsername();

    @SchemaFieldDescription("Database password")
    public abstract @Nullable String getPassword();

    @SchemaFieldDescription("Google Cloud SQL instance connection name (project:region:instance)")
    public abstract @Nullable String getCloudSqlInstanceConnectionName();

    @SchemaFieldDescription(
        "Google Cloud AlloyDB instance connection name (projects/p/locations/l/clusters/c/instances/i)")
    public abstract @Nullable String getAlloyDbInstanceConnectionName();

    @SchemaFieldDescription("Enable Google Cloud IAM database authentication")
    public abstract @Nullable Boolean getEnableIamAuth();

    @SchemaFieldDescription("Google Cloud Secret Manager secret resource URI")
    public abstract @Nullable String getSecretManagerUri();

    @SchemaFieldDescription("IP routing type: PUBLIC, PRIVATE, or PSC")
    public abstract @Nullable String getIpType();

    @SchemaFieldDescription("List of primary key column names for upserting")
    public abstract @Nullable List<String> getPrimaryKeyColumns();

    @SchemaFieldDescription(
        "Write mode: STREAMING_UPSERT_UNNEST, APPEND_ONLY_COPY, or STAGED_COPY_UPSERT")
    public abstract @Nullable String getWriteMode();

    @SchemaFieldDescription("PostgreSQL replication origin name for session tagging")
    public abstract @Nullable String getReplicationOriginName();

    @SchemaFieldDescription("Maximum rows per batch")
    public abstract @Nullable Integer getBatchSize();

    @SchemaFieldDescription("Maximum buffering duration in milliseconds before flushing")
    public abstract @Nullable Long getMaxBufferingDurationMs();

    public static Builder builder() {
      return new AutoValue_PostgreSqlWriteSchemaTransformProvider_PostgreSqlWriteConfiguration
          .Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setUrl(@Nullable String url);

      public abstract Builder setTable(String table);

      public abstract Builder setUsername(@Nullable String username);

      public abstract Builder setPassword(@Nullable String password);

      public abstract Builder setCloudSqlInstanceConnectionName(@Nullable String instanceName);

      public abstract Builder setAlloyDbInstanceConnectionName(@Nullable String instanceName);

      public abstract Builder setEnableIamAuth(@Nullable Boolean enableIamAuth);

      public abstract Builder setSecretManagerUri(@Nullable String secretManagerUri);

      public abstract Builder setIpType(@Nullable String ipType);

      public abstract Builder setPrimaryKeyColumns(@Nullable List<String> primaryKeyColumns);

      public abstract Builder setWriteMode(@Nullable String writeMode);

      public abstract Builder setReplicationOriginName(@Nullable String originName);

      public abstract Builder setBatchSize(@Nullable Integer batchSize);

      public abstract Builder setMaxBufferingDurationMs(@Nullable Long maxBufferingDurationMs);

      public abstract PostgreSqlWriteConfiguration build();
    }
  }

  @Override
  public String identifier() {
    return getUrn(ExternalTransforms.ManagedTransforms.Urns.POSTGRES_WRITE);
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  @Override
  protected SchemaTransform from(PostgreSqlWriteConfiguration configuration) {
    return new PostgreSqlWriteSchemaTransform(configuration);
  }

  private static class PostgreSqlWriteSchemaTransform extends SchemaTransform {
    private final PostgreSqlWriteConfiguration configuration;

    public PostgreSqlWriteSchemaTransform(PostgreSqlWriteConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> inputRows = input.get(INPUT_TAG);

      String url =
          configuration.getUrl() != null
              ? configuration.getUrl()
              : "jdbc:postgresql://localhost:5432/postgres";
      PostgreSqlDataSourceConfiguration dsConfig =
          PostgreSqlDataSourceConfiguration.create(url)
              .withUsername(configuration.getUsername())
              .withPassword(configuration.getPassword());

      if (configuration.getCloudSqlInstanceConnectionName() != null) {
        dsConfig =
            dsConfig.withCloudSqlInstanceConnectionName(
                configuration.getCloudSqlInstanceConnectionName());
      }
      if (configuration.getAlloyDbInstanceConnectionName() != null) {
        dsConfig =
            dsConfig.withAlloyDbInstanceConnectionName(
                configuration.getAlloyDbInstanceConnectionName());
      }
      if (Boolean.TRUE.equals(configuration.getEnableIamAuth())) {
        dsConfig = dsConfig.withEnableIamAuth(true);
      }
      if (configuration.getSecretManagerUri() != null) {
        dsConfig = dsConfig.withSecretManagerUri(configuration.getSecretManagerUri());
      }
      if (configuration.getIpType() != null) {
        dsConfig = dsConfig.withIpType(configuration.getIpType());
      }
      if (configuration.getReplicationOriginName() != null) {
        dsConfig = dsConfig.withReplicationOriginName(configuration.getReplicationOriginName());
      }

      PostgreSqlWrite.Builder writeBuilder =
          PostgreSQLIO.write().withDataSourceConfiguration(dsConfig).to(configuration.getTable());

      if (configuration.getPrimaryKeyColumns() != null) {
        writeBuilder = writeBuilder.withPrimaryKeyColumns(configuration.getPrimaryKeyColumns());
      }
      if (configuration.getWriteMode() != null) {
        writeBuilder = writeBuilder.withWriteMode(WriteMode.valueOf(configuration.getWriteMode()));
      }
      if (configuration.getBatchSize() != null) {
        writeBuilder = writeBuilder.withBatchSize(configuration.getBatchSize());
      }
      if (configuration.getMaxBufferingDurationMs() != null) {
        writeBuilder =
            writeBuilder.withMaxBufferingDuration(
                Duration.millis(configuration.getMaxBufferingDurationMs()));
      }

      PostgreSqlWriteResult result = inputRows.apply(writeBuilder.build());
      return PCollectionRowTuple.of(OUTPUT_TAG, result.getSuccessfulRows());
    }
  }
}
