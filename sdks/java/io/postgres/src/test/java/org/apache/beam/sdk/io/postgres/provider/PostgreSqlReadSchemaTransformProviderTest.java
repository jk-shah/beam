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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import org.apache.beam.sdk.io.postgres.provider.PostgreSqlReadSchemaTransformProvider.PostgreSqlReadConfiguration;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PostgreSqlReadSchemaTransformProvider}. */
@RunWith(JUnit4.class)
public class PostgreSqlReadSchemaTransformProviderTest {

  @Test
  public void testProviderUrnIdentifier() {
    PostgreSqlReadSchemaTransformProvider provider = new PostgreSqlReadSchemaTransformProvider();
    assertNotNull(provider.identifier());
    assertTrue(provider.identifier().contains("postgres_read"));
  }

  @Test
  public void testConfigurationPropertiesAndBuilding() {
    PostgreSqlReadConfiguration config =
        PostgreSqlReadConfiguration.builder()
            .setUrl("jdbc:postgresql://localhost:5432/testdb")
            .setUsername("postgres")
            .setPassword("secret")
            .setPublicationName("beam_pub")
            .setReplicationSlotName("beam_slot")
            .setTables(Collections.singletonList("orders"))
            .build();

    assertEquals("jdbc:postgresql://localhost:5432/testdb", config.getUrl());
    assertEquals("postgres", config.getUsername());
    assertEquals("secret", config.getPassword());
    assertEquals("beam_pub", config.getPublicationName());
    assertEquals("beam_slot", config.getReplicationSlotName());
    assertEquals(Collections.singletonList("orders"), config.getTables());

    PostgreSqlReadSchemaTransformProvider provider = new PostgreSqlReadSchemaTransformProvider();
    SchemaTransform transform = provider.from(config);
    assertNotNull(transform);
  }

  @Test
  public void testCloudManagedConfiguration() {
    PostgreSqlReadConfiguration config =
        PostgreSqlReadConfiguration.builder()
            .setCloudSqlInstanceConnectionName("my-project:us-central1:my-instance")
            .setEnableIamAuth(true)
            .setIpType("PRIVATE")
            .setSecretManagerUri("sm://projects/p/secrets/s/versions/1")
            .setOriginFilter("none")
            .setPublicationName("beam_pub")
            .setReplicationSlotName("beam_slot")
            .build();

    assertEquals("my-project:us-central1:my-instance", config.getCloudSqlInstanceConnectionName());
    assertEquals(Boolean.TRUE, config.getEnableIamAuth());
    assertEquals("PRIVATE", config.getIpType());
    assertEquals("sm://projects/p/secrets/s/versions/1", config.getSecretManagerUri());
    assertEquals("none", config.getOriginFilter());

    PostgreSqlReadSchemaTransformProvider provider = new PostgreSqlReadSchemaTransformProvider();
    SchemaTransform transform = provider.from(config);
    assertNotNull(transform);
  }
}
