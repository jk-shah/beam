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
package org.apache.beam.sdk.io.postgres.sink;

import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.io.postgres.PostgreSqlUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for parameterized PostgreSQL UNNEST streaming upsert SQL generation. */
@RunWith(JUnit4.class)
public class UnnestArrayUpsertWriterTest implements Serializable {

  @Test
  public void testBuildUnnestUpsertQuerySinglePrimaryKey() {
    Schema schema =
        Schema.builder().addInt32Field("id").addStringField("name").addDoubleField("score").build();

    String sql =
        PostgreSqlUtils.buildUnnestUpsertQuery(
            "public.players", schema, Collections.singletonList("id"));

    assertTrue(
        sql.startsWith(
            "INSERT INTO \"public\".\"players\" (\"id\", \"name\", \"score\") SELECT \"id\", \"name\", \"score\" FROM UNNEST"));
    assertTrue(
        sql.contains(
            "ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\", \"score\" = EXCLUDED.\"score\""));
  }

  @Test
  public void testBuildUnnestUpsertQueryCompositePrimaryKey() {
    Schema schema =
        Schema.builder()
            .addInt32Field("tenant_id")
            .addInt64Field("order_id")
            .addStringField("status")
            .build();

    String sql =
        PostgreSqlUtils.buildUnnestUpsertQuery(
            "orders", schema, Arrays.asList("tenant_id", "order_id"));

    assertTrue(
        sql.startsWith("INSERT INTO \"orders\" (\"tenant_id\", \"order_id\", \"status\") SELECT"));
    assertTrue(
        sql.contains(
            "ON CONFLICT (\"tenant_id\", \"order_id\") DO UPDATE SET \"status\" = EXCLUDED.\"status\""));
  }

  @Test
  public void testBuildUnnestUpsertQueryAllPrimaryKeysDoNothing() {
    Schema schema = Schema.builder().addInt32Field("user_id").addInt32Field("group_id").build();

    String sql =
        PostgreSqlUtils.buildUnnestUpsertQuery(
            "user_groups", schema, Arrays.asList("user_id", "group_id"));

    assertTrue(sql.contains("ON CONFLICT (\"user_id\", \"group_id\") DO NOTHING"));
  }
}
