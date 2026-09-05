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
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for SQL identifier escaping and SQL injection prevention in {@link PostgreSqlUtils}.
 */
@RunWith(JUnit4.class)
public class PostgreSqlSecurityIdentifierTest {

  @Test
  public void testEscapeIdentifierSimple() {
    assertEquals("\"orders\"", PostgreSqlUtils.escapeIdentifier("orders"));
    assertEquals("\"public\"", PostgreSqlUtils.escapeIdentifier("public"));
  }

  @Test
  public void testEscapeIdentifierWithQuotes() {
    assertEquals("\"ord\"\"ers\"", PostgreSqlUtils.escapeIdentifier("ord\"ers"));
  }

  @Test
  public void testEscapeTableIdentifierQualified() {
    assertEquals("\"public\".\"orders\"", PostgreSqlUtils.escapeTableIdentifier("public.orders"));
  }

  @Test
  public void testEscapeTableIdentifierWithInjectionPayload() {
    String malicious = "orders\"; DROP TABLE users; --";
    String escaped = PostgreSqlUtils.escapeTableIdentifier(malicious);
    assertEquals("\"orders\"\"; DROP TABLE users; --\"", escaped);
  }

  @Test
  public void testBuildUnnestUpsertQuery() {
    Schema schema =
        Schema.builder()
            .addInt64Field("id")
            .addStringField("name")
            .addDecimalField("price")
            .build();

    String query =
        PostgreSqlUtils.buildUnnestUpsertQuery(
            "public.orders", schema, Collections.singletonList("id"));

    assertTrue(query.startsWith("INSERT INTO \"public\".\"orders\" (\"id\", \"name\", \"price\")"));
    assertTrue(query.contains("UNNEST(?::bigint[], ?::text[], ?::numeric[])"));
    assertTrue(
        query.contains(
            "ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\", \"price\" = EXCLUDED.\"price\""));
  }
}
