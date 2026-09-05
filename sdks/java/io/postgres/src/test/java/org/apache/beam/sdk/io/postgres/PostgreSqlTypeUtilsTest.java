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

import java.sql.Types;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PostgreSqlTypeUtils}. */
@RunWith(JUnit4.class)
public class PostgreSqlTypeUtilsTest {

  @Test
  public void testMapJdbcTypeToBeamFieldType() {
    assertEquals(
        FieldType.INT32, PostgreSqlTypeUtils.mapJdbcTypeToBeamFieldType(Types.INTEGER, "int4"));
    assertEquals(
        FieldType.INT64, PostgreSqlTypeUtils.mapJdbcTypeToBeamFieldType(Types.BIGINT, "int8"));
    assertEquals(
        FieldType.DECIMAL,
        PostgreSqlTypeUtils.mapJdbcTypeToBeamFieldType(Types.NUMERIC, "numeric"));
    assertEquals(
        FieldType.STRING, PostgreSqlTypeUtils.mapJdbcTypeToBeamFieldType(Types.VARCHAR, "varchar"));
    assertEquals(
        FieldType.BOOLEAN, PostgreSqlTypeUtils.mapJdbcTypeToBeamFieldType(Types.BOOLEAN, "bool"));
    assertEquals(
        FieldType.DATETIME,
        PostgreSqlTypeUtils.mapJdbcTypeToBeamFieldType(Types.TIMESTAMP, "timestamptz"));
    assertEquals(
        FieldType.STRING, PostgreSqlTypeUtils.mapJdbcTypeToBeamFieldType(Types.OTHER, "jsonb"));
    assertEquals(
        FieldType.STRING, PostgreSqlTypeUtils.mapJdbcTypeToBeamFieldType(Types.OTHER, "uuid"));
    assertEquals(
        FieldType.array(FieldType.INT64),
        PostgreSqlTypeUtils.mapJdbcTypeToBeamFieldType(Types.ARRAY, "_int8"));
  }

  @Test
  public void testGetPostgreSqlArrayCastType() {
    assertEquals("bigint[]", PostgreSqlTypeUtils.getPostgreSqlArrayCastType(FieldType.INT64));
    assertEquals("integer[]", PostgreSqlTypeUtils.getPostgreSqlArrayCastType(FieldType.INT32));
    assertEquals("numeric[]", PostgreSqlTypeUtils.getPostgreSqlArrayCastType(FieldType.DECIMAL));
    assertEquals("text[]", PostgreSqlTypeUtils.getPostgreSqlArrayCastType(FieldType.STRING));
    assertEquals(
        "timestamptz[]", PostgreSqlTypeUtils.getPostgreSqlArrayCastType(FieldType.DATETIME));
  }
}
