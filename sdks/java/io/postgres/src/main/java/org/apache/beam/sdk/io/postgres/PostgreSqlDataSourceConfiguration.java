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
package org.apache.beam.sdk.io.postgres;

import com.google.auto.value.AutoValue;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.Serializable;
import java.util.Properties;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.postgres.auth.DynamicAuthPostgreSqlDataSource;
import org.apache.beam.sdk.io.postgres.auth.DynamicPasswordProvider;
import org.apache.beam.sdk.io.postgres.auth.SecretManagerDynamicPasswordProvider;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * Encapsulates PostgreSQL connection parameters, connection pooling limits, SSL settings, Google
 * Cloud SQL / AlloyDB socket factories, and dynamic Cloud IAM / Secret Manager authentication.
 */
@AutoValue
public abstract class PostgreSqlDataSourceConfiguration implements Serializable, HasDisplayData {

  public abstract String getUrl();

  public abstract @Nullable String getUsername();

  public abstract @Nullable String getPassword();

  public abstract @Nullable DynamicPasswordProvider getDynamicPasswordProvider();

  public abstract @Nullable String getCloudSqlInstanceConnectionName();

  public abstract @Nullable String getAlloyDbInstanceConnectionName();

  public abstract boolean isEnableIamAuth();

  public abstract @Nullable String getIpType();

  public abstract @Nullable String getSecretManagerUri();

  public abstract @Nullable Duration getSecretRefreshInterval();

  public abstract @Nullable String getReplicationOriginName();

  public abstract int getMaxConnections();

  public abstract int getMinIdleConnections();

  public abstract int getConnectionTimeoutMs();

  public abstract int getIdleTimeoutMs();

  public abstract int getMaxLifetimeMs();

  public abstract Properties getConnectionProperties();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_PostgreSqlDataSourceConfiguration.Builder()
        .setEnableIamAuth(false)
        .setMaxConnections(4)
        .setMinIdleConnections(1)
        .setConnectionTimeoutMs(30000)
        .setIdleTimeoutMs(600000)
        .setMaxLifetimeMs(1800000)
        .setConnectionProperties(new Properties());
  }

  public static PostgreSqlDataSourceConfiguration create(String url) {
    return builder().setUrl(url).build();
  }

  public static PostgreSqlDataSourceConfiguration createForCloudSql(
      String instanceConnectionName, String databaseName) {
    return builder()
        .setUrl("jdbc:postgresql:///" + databaseName)
        .setCloudSqlInstanceConnectionName(instanceConnectionName)
        .build();
  }

  public static PostgreSqlDataSourceConfiguration createForAlloyDb(
      String instanceConnectionName, String databaseName) {
    return builder()
        .setUrl("jdbc:postgresql:///" + databaseName)
        .setAlloyDbInstanceConnectionName(instanceConnectionName)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setUrl(String url);

    public abstract Builder setUsername(@Nullable String username);

    public abstract Builder setPassword(@Nullable String password);

    public abstract Builder setDynamicPasswordProvider(@Nullable DynamicPasswordProvider provider);

    public abstract Builder setCloudSqlInstanceConnectionName(
        @Nullable String instanceConnectionName);

    public abstract Builder setAlloyDbInstanceConnectionName(
        @Nullable String instanceConnectionName);

    public abstract Builder setEnableIamAuth(boolean enableIamAuth);

    public abstract Builder setIpType(@Nullable String ipType);

    public abstract Builder setSecretManagerUri(@Nullable String secretManagerUri);

    public abstract Builder setSecretRefreshInterval(@Nullable Duration interval);

    public abstract Builder setReplicationOriginName(@Nullable String originName);

    public abstract Builder setMaxConnections(int maxConnections);

    public abstract Builder setMinIdleConnections(int minIdleConnections);

    public abstract Builder setConnectionTimeoutMs(int timeoutMs);

    public abstract Builder setIdleTimeoutMs(int idleTimeoutMs);

    public abstract Builder setMaxLifetimeMs(int maxLifetimeMs);

    public abstract Builder setConnectionProperties(Properties properties);

    public abstract PostgreSqlDataSourceConfiguration build();
  }

  public PostgreSqlDataSourceConfiguration withUsername(@Nullable String username) {
    return toBuilder().setUsername(username).build();
  }

  public PostgreSqlDataSourceConfiguration withPassword(@Nullable String password) {
    return toBuilder().setPassword(password).build();
  }

  public PostgreSqlDataSourceConfiguration withDynamicPasswordProvider(
      @Nullable DynamicPasswordProvider provider) {
    return toBuilder().setDynamicPasswordProvider(provider).build();
  }

  public PostgreSqlDataSourceConfiguration withCloudSqlInstanceConnectionName(
      @Nullable String instanceConnectionName) {
    return toBuilder().setCloudSqlInstanceConnectionName(instanceConnectionName).build();
  }

  public PostgreSqlDataSourceConfiguration withAlloyDbInstanceConnectionName(
      @Nullable String instanceConnectionName) {
    return toBuilder().setAlloyDbInstanceConnectionName(instanceConnectionName).build();
  }

  public PostgreSqlDataSourceConfiguration withEnableIamAuth(boolean enableIamAuth) {
    return toBuilder().setEnableIamAuth(enableIamAuth).build();
  }

  public PostgreSqlDataSourceConfiguration withIpType(@Nullable String ipType) {
    return toBuilder().setIpType(ipType).build();
  }

  public PostgreSqlDataSourceConfiguration withSecretManagerUri(@Nullable String secretUri) {
    return toBuilder().setSecretManagerUri(secretUri).build();
  }

  public PostgreSqlDataSourceConfiguration withSecretRefreshInterval(@Nullable Duration interval) {
    return toBuilder().setSecretRefreshInterval(interval).build();
  }

  public PostgreSqlDataSourceConfiguration withReplicationOriginName(@Nullable String originName) {
    return toBuilder().setReplicationOriginName(originName).build();
  }

  public PostgreSqlDataSourceConfiguration withMaxConnections(int maxConnections) {
    return toBuilder().setMaxConnections(maxConnections).build();
  }

  public PostgreSqlDataSourceConfiguration withProperty(String key, String value) {
    Properties props = (Properties) getConnectionProperties().clone();
    props.setProperty(key, value);
    return toBuilder().setConnectionProperties(props).build();
  }

  /**
   * Builds an unpooled raw {@link DataSource} supporting dynamic authentication and Cloud
   * SQL/AlloyDB sockets.
   */
  public DataSource buildRawDataSource() {
    Properties props = (Properties) getConnectionProperties().clone();

    // 1. Configure Cloud SQL Socket Factory
    if (getCloudSqlInstanceConnectionName() != null) {
      props.setProperty("socketFactory", "com.google.cloud.sql.postgres.SocketFactory");
      props.setProperty("cloudSqlInstance", getCloudSqlInstanceConnectionName());
      if (isEnableIamAuth()) {
        props.setProperty("enableIamAuth", "true");
        props.setProperty(
            "sslmode", "disable"); // SSL is handled directly by the Cloud SQL SocketFactory
      }
      if (getIpType() != null) {
        props.setProperty("ipTypes", getIpType());
      }
    }

    // 2. Configure AlloyDB Socket Factory
    if (getAlloyDbInstanceConnectionName() != null) {
      props.setProperty("socketFactory", "com.google.cloud.alloydb.SocketFactory");
      props.setProperty("alloydbInstance", getAlloyDbInstanceConnectionName());
      if (isEnableIamAuth()) {
        props.setProperty("enableIamAuth", "true");
        props.setProperty("sslmode", "disable");
      }
      if (getIpType() != null) {
        props.setProperty("ipTypes", getIpType());
      }
    }

    // 3. Resolve Dynamic Password Provider (Secret Manager vs Explicit Provider vs Static Password)
    DynamicPasswordProvider passwordProvider = getDynamicPasswordProvider();
    if (passwordProvider == null && getSecretManagerUri() != null) {
      Duration refreshInterval =
          getSecretRefreshInterval() != null
              ? getSecretRefreshInterval()
              : Duration.standardMinutes(15);
      passwordProvider =
          new SecretManagerDynamicPasswordProvider(getSecretManagerUri(), refreshInterval);
    }

    if (getPassword() != null && passwordProvider == null) {
      props.setProperty("password", getPassword());
    }

    return new DynamicAuthPostgreSqlDataSource(
        getUrl(), getUsername() != null ? getUsername() : "postgres", passwordProvider, props);
  }

  /** Builds a configured, production-ready {@link HikariDataSource} connection pool. */
  public HikariDataSource buildPooledDataSource() {
    HikariConfig config = new HikariConfig();
    config.setDataSource(buildRawDataSource());
    config.setMaximumPoolSize(getMaxConnections());
    config.setMinimumIdle(getMinIdleConnections());
    config.setConnectionTimeout(getConnectionTimeoutMs());
    config.setIdleTimeout(getIdleTimeoutMs());
    config.setMaxLifetime(getMaxLifetimeMs());
    config.setPoolName("BeamPostgresPool-" + Integer.toHexString(hashCode()));

    // PgBouncer transaction-pooling compatibility settings
    config.addDataSourceProperty("prepareThreshold", "0");
    if (getReplicationOriginName() != null) {
      String escapedOrigin = getReplicationOriginName().replace("'", "''");
      config.setConnectionInitSql(
          "SELECT pg_replication_origin_session_setup('" + escapedOrigin + "');");
    } else {
      config.setConnectionInitSql("RESET ROLE;");
    }
    config.setInitializationFailTimeout(-1);

    return new HikariDataSource(config);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    builder.add(DisplayData.item("url", getUrl()));
    if (getUsername() != null) {
      builder.add(DisplayData.item("username", getUsername()));
    }
    if (getCloudSqlInstanceConnectionName() != null) {
      builder.add(DisplayData.item("cloudSqlInstance", getCloudSqlInstanceConnectionName()));
    }
    if (getAlloyDbInstanceConnectionName() != null) {
      builder.add(DisplayData.item("alloyDbInstance", getAlloyDbInstanceConnectionName()));
    }
    if (getSecretManagerUri() != null) {
      builder.add(DisplayData.item("secretManagerUri", getSecretManagerUri()));
    }
    builder.add(DisplayData.item("maxConnections", getMaxConnections()));
  }
}
