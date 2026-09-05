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
import org.apache.beam.sdk.io.postgres.cdc.ChangeEvent;
import org.apache.beam.sdk.io.postgres.cdc.PostgreSqlReadCDC;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

/** SchemaTransformProvider for reading from PostgreSQL using CDC or bounded tables. */
@AutoService(SchemaTransformProvider.class)
public class PostgreSqlReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        PostgreSqlReadSchemaTransformProvider.PostgreSqlReadConfiguration> {

  public static final String OUTPUT_TAG = "output";

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class PostgreSqlReadConfiguration implements Serializable {

    @SchemaFieldDescription("PostgreSQL JDBC connection URL (e.g. jdbc:postgresql://host:5432/db)")
    public abstract @Nullable String getUrl();

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

    @SchemaFieldDescription(
        "PostgreSQL replication origin filter: 'none', 'any', or custom origin name")
    public abstract @Nullable String getOriginFilter();

    @SchemaFieldDescription("PostgreSQL publication name for CDC streaming")
    public abstract @Nullable String getPublicationName();

    @SchemaFieldDescription("PostgreSQL logical replication slot name for CDC streaming")
    public abstract @Nullable String getReplicationSlotName();

    @SchemaFieldDescription("List of tables to replicate or read")
    public abstract @Nullable List<String> getTables();

    public static Builder builder() {
      return new AutoValue_PostgreSqlReadSchemaTransformProvider_PostgreSqlReadConfiguration
          .Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setUrl(@Nullable String url);

      public abstract Builder setUsername(@Nullable String username);

      public abstract Builder setPassword(@Nullable String password);

      public abstract Builder setCloudSqlInstanceConnectionName(@Nullable String instanceName);

      public abstract Builder setAlloyDbInstanceConnectionName(@Nullable String instanceName);

      public abstract Builder setEnableIamAuth(@Nullable Boolean enableIamAuth);

      public abstract Builder setSecretManagerUri(@Nullable String secretManagerUri);

      public abstract Builder setIpType(@Nullable String ipType);

      public abstract Builder setOriginFilter(@Nullable String originFilter);

      public abstract Builder setPublicationName(@Nullable String publicationName);

      public abstract Builder setReplicationSlotName(@Nullable String slotName);

      public abstract Builder setTables(@Nullable List<String> tables);

      public abstract PostgreSqlReadConfiguration build();
    }
  }

  @Override
  public String identifier() {
    return getUrn(ExternalTransforms.ManagedTransforms.Urns.POSTGRES_READ);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  @Override
  protected SchemaTransform from(PostgreSqlReadConfiguration configuration) {
    return new PostgreSqlReadSchemaTransform(configuration);
  }

  private static class PostgreSqlReadSchemaTransform extends SchemaTransform {
    private final PostgreSqlReadConfiguration configuration;

    public PostgreSqlReadSchemaTransform(PostgreSqlReadConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
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

      PostgreSqlReadCDC.Builder cdcBuilder =
          PostgreSQLIO.readCDC().withDataSourceConfiguration(dsConfig);

      if (configuration.getPublicationName() != null) {
        cdcBuilder = cdcBuilder.withPublicationName(configuration.getPublicationName());
      }
      if (configuration.getReplicationSlotName() != null) {
        cdcBuilder = cdcBuilder.withReplicationSlotName(configuration.getReplicationSlotName());
      }
      if (configuration.getTables() != null) {
        cdcBuilder = cdcBuilder.withTableWhitelist(configuration.getTables());
      }
      if (configuration.getOriginFilter() != null) {
        cdcBuilder = cdcBuilder.withOriginFilter(configuration.getOriginFilter());
      }

      PCollection<ChangeEvent<Row>> changes = input.getPipeline().apply(cdcBuilder.build());

      PCollection<Row> outputRows =
          changes.apply(
              "ExtractAfterImage",
              ParDo.of(
                  new DoFn<ChangeEvent<Row>, Row>() {
                    @ProcessElement
                    public void processElement(
                        @Element ChangeEvent<Row> event, OutputReceiver<Row> receiver) {
                      if (event.getAfter() != null) {
                        receiver.output(event.getAfter());
                      } else if (event.getBefore() != null) {
                        receiver.output(event.getBefore());
                      }
                    }
                  }));

      return PCollectionRowTuple.of(OUTPUT_TAG, outputRows);
    }
  }
}
