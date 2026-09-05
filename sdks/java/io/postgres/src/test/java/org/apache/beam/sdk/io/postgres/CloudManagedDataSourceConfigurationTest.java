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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.sql.DataSource;
import org.apache.beam.sdk.io.postgres.auth.DynamicAuthPostgreSqlDataSource;
import org.apache.beam.sdk.io.postgres.auth.SecretManagerDynamicPasswordProvider;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for Cloud SQL, AlloyDB, IAM Auth, and Secret Manager configuration in {@link
 * PostgreSqlDataSourceConfiguration}.
 */
@RunWith(JUnit4.class)
public class CloudManagedDataSourceConfigurationTest {

  @Test
  public void testCloudSqlConfigurationProperties() {
    PostgreSqlDataSourceConfiguration config =
        PostgreSqlDataSourceConfiguration.createForCloudSql(
                "my-project:us-central1:my-instance", "ecommerce")
            .withUsername("beam_cdc_user")
            .withEnableIamAuth(true)
            .withIpType("PRIVATE");

    assertEquals("jdbc:postgresql:///ecommerce", config.getUrl());
    assertEquals("my-project:us-central1:my-instance", config.getCloudSqlInstanceConnectionName());
    assertEquals("beam_cdc_user", config.getUsername());
    assertTrue(config.isEnableIamAuth());
    assertEquals("PRIVATE", config.getIpType());

    DataSource ds = config.buildRawDataSource();
    assertTrue(ds instanceof DynamicAuthPostgreSqlDataSource);
    DynamicAuthPostgreSqlDataSource dynamicDs = (DynamicAuthPostgreSqlDataSource) ds;
    assertEquals(
        "com.google.cloud.sql.postgres.SocketFactory",
        dynamicDs.getConnectionProperties().getProperty("socketFactory"));
    assertEquals(
        "my-project:us-central1:my-instance",
        dynamicDs.getConnectionProperties().getProperty("cloudSqlInstance"));
    assertEquals("true", dynamicDs.getConnectionProperties().getProperty("enableIamAuth"));
    assertEquals("disable", dynamicDs.getConnectionProperties().getProperty("sslmode"));
    assertEquals("PRIVATE", dynamicDs.getConnectionProperties().getProperty("ipTypes"));
  }

  @Test
  public void testAlloyDbConfigurationProperties() {
    PostgreSqlDataSourceConfiguration config =
        PostgreSqlDataSourceConfiguration.createForAlloyDb(
                "projects/my-p/locations/us-central1/clusters/c1/instances/i1", "orders")
            .withUsername("alloy_user")
            .withEnableIamAuth(true)
            .withIpType("PSC");

    assertEquals("jdbc:postgresql:///orders", config.getUrl());
    assertEquals(
        "projects/my-p/locations/us-central1/clusters/c1/instances/i1",
        config.getAlloyDbInstanceConnectionName());
    assertTrue(config.isEnableIamAuth());
    assertEquals("PSC", config.getIpType());

    DataSource ds = config.buildRawDataSource();
    assertTrue(ds instanceof DynamicAuthPostgreSqlDataSource);
    DynamicAuthPostgreSqlDataSource dynamicDs = (DynamicAuthPostgreSqlDataSource) ds;
    assertEquals(
        "com.google.cloud.alloydb.SocketFactory",
        dynamicDs.getConnectionProperties().getProperty("socketFactory"));
    assertEquals(
        "projects/my-p/locations/us-central1/clusters/c1/instances/i1",
        dynamicDs.getConnectionProperties().getProperty("alloydbInstance"));
    assertEquals("true", dynamicDs.getConnectionProperties().getProperty("enableIamAuth"));
    assertEquals("disable", dynamicDs.getConnectionProperties().getProperty("sslmode"));
    assertEquals("PSC", dynamicDs.getConnectionProperties().getProperty("ipTypes"));
  }

  @Test
  public void testSecretManagerIntegration() {
    PostgreSqlDataSourceConfiguration config =
        PostgreSqlDataSourceConfiguration.create("jdbc:postgresql://localhost:5432/mydb")
            .withUsername("beam_user")
            .withSecretManagerUri("sm://projects/my-proj/secrets/db_pass/versions/latest")
            .withSecretRefreshInterval(Duration.standardMinutes(30));

    assertEquals(
        "sm://projects/my-proj/secrets/db_pass/versions/latest", config.getSecretManagerUri());
    assertEquals(Duration.standardMinutes(30), config.getSecretRefreshInterval());

    DataSource ds = config.buildRawDataSource();
    assertTrue(ds instanceof DynamicAuthPostgreSqlDataSource);
    DynamicAuthPostgreSqlDataSource dynamicDs = (DynamicAuthPostgreSqlDataSource) ds;
    assertNotNull(dynamicDs.getPasswordProvider());
    assertTrue(dynamicDs.getPasswordProvider() instanceof SecretManagerDynamicPasswordProvider);
    SecretManagerDynamicPasswordProvider smProvider =
        (SecretManagerDynamicPasswordProvider) dynamicDs.getPasswordProvider();
    assertEquals(
        "sm://projects/my-proj/secrets/db_pass/versions/latest", smProvider.getSecretUri());
    assertEquals(Duration.standardMinutes(30), smProvider.getCacheTtl());
  }

  @Test
  public void testReplicationOriginConfiguration() {
    PostgreSqlDataSourceConfiguration config =
        PostgreSqlDataSourceConfiguration.create("jdbc:postgresql://localhost:5432/mydb")
            .withUsername("beam_user")
            .withPassword("secret")
            .withReplicationOriginName("beam_us_east");

    assertEquals("beam_us_east", config.getReplicationOriginName());
    com.zaxxer.hikari.HikariDataSource ds = config.buildPooledDataSource();
    assertEquals(
        "SELECT pg_replication_origin_session_setup('beam_us_east');", ds.getConnectionInitSql());
    ds.close();
  }
}
