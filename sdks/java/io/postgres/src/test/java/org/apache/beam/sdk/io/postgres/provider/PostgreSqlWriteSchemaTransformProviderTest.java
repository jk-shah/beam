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
import org.apache.beam.sdk.io.postgres.provider.PostgreSqlWriteSchemaTransformProvider.PostgreSqlWriteConfiguration;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PostgreSqlWriteSchemaTransformProvider}. */
@RunWith(JUnit4.class)
public class PostgreSqlWriteSchemaTransformProviderTest {

  @Test
  public void testProviderUrnIdentifier() {
    PostgreSqlWriteSchemaTransformProvider provider = new PostgreSqlWriteSchemaTransformProvider();
    assertNotNull(provider.identifier());
    assertTrue(provider.identifier().contains("postgres_write"));
  }

  @Test
  public void testConfigurationPropertiesAndBuilding() {
    PostgreSqlWriteConfiguration config =
        PostgreSqlWriteConfiguration.builder()
            .setUrl("jdbc:postgresql://localhost:5432/testdb")
            .setTable("public.orders")
            .setUsername("postgres")
            .setPassword("secret")
            .setPrimaryKeyColumns(Collections.singletonList("id"))
            .setWriteMode("STAGED_COPY_UPSERT")
            .setBatchSize(5000)
            .setMaxBufferingDurationMs(1000L)
            .build();

    assertEquals("jdbc:postgresql://localhost:5432/testdb", config.getUrl());
    assertEquals("public.orders", config.getTable());
    assertEquals("postgres", config.getUsername());
    assertEquals("secret", config.getPassword());
    assertEquals(Collections.singletonList("id"), config.getPrimaryKeyColumns());
    assertEquals("STAGED_COPY_UPSERT", config.getWriteMode());
    assertEquals(Integer.valueOf(5000), config.getBatchSize());
    assertEquals(Long.valueOf(1000L), config.getMaxBufferingDurationMs());

    PostgreSqlWriteSchemaTransformProvider provider = new PostgreSqlWriteSchemaTransformProvider();
    SchemaTransform transform = provider.from(config);
    assertNotNull(transform);
  }

  @Test
  public void testCloudManagedWriteConfiguration() {
    PostgreSqlWriteConfiguration config =
        PostgreSqlWriteConfiguration.builder()
            .setTable("public.orders")
            .setCloudSqlInstanceConnectionName("my-proj:us-central1:my-inst")
            .setEnableIamAuth(true)
            .setIpType("PSC")
            .setSecretManagerUri("sm://projects/p/secrets/s/versions/1")
            .setPrimaryKeyColumns(Collections.singletonList("id"))
            .setWriteMode("STREAMING_UPSERT_UNNEST")
            .build();

    assertEquals("my-proj:us-central1:my-inst", config.getCloudSqlInstanceConnectionName());
    assertEquals(Boolean.TRUE, config.getEnableIamAuth());
    assertEquals("PSC", config.getIpType());
    assertEquals("sm://projects/p/secrets/s/versions/1", config.getSecretManagerUri());

    PostgreSqlWriteSchemaTransformProvider provider = new PostgreSqlWriteSchemaTransformProvider();
    SchemaTransform transform = provider.from(config);
    assertNotNull(transform);
  }
}
