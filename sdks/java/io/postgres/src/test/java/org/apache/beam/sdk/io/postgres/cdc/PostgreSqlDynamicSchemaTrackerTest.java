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
package org.apache.beam.sdk.io.postgres.cdc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.beam.sdk.io.postgres.cdc.SchemaChangeEvent.SchemaChangeType;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PostgreSqlDynamicSchemaTracker}. */
@RunWith(JUnit4.class)
public class PostgreSqlDynamicSchemaTrackerTest {

  @Test
  public void testInitialTableRegistration() {
    PostgreSqlDynamicSchemaTracker tracker = new PostgreSqlDynamicSchemaTracker();

    Schema schemaV1 = Schema.builder().addInt32Field("id").addStringField("name").build();

    SchemaChangeEvent event = tracker.processRelation(16384, "public", "orders", schemaV1, 1000L);

    assertNotNull(event);
    assertEquals(SchemaChangeType.TABLE_CREATED, event.getChangeType());
    assertEquals("orders", event.getTableName());
    assertEquals("public", event.getSchemaName());
    assertEquals(16384, event.getRelationOid());
    assertEquals(1000L, event.getCommitLsn());
    assertNull(event.getOldSchema());
    assertEquals(schemaV1, event.getNewSchema());
    assertEquals(2, event.getAddedFields().size());
  }

  @Test
  public void testNoChangeReturnsNull() {
    PostgreSqlDynamicSchemaTracker tracker = new PostgreSqlDynamicSchemaTracker();

    Schema schemaV1 = Schema.builder().addInt32Field("id").addStringField("name").build();

    tracker.processRelation(16384, "public", "orders", schemaV1, 1000L);

    // Process identical schema
    SchemaChangeEvent event2 = tracker.processRelation(16384, "public", "orders", schemaV1, 1500L);

    assertNull(event2);
  }

  @Test
  public void testColumnsAddedDetection() {
    PostgreSqlDynamicSchemaTracker tracker = new PostgreSqlDynamicSchemaTracker();

    Schema schemaV1 = Schema.builder().addInt32Field("id").addStringField("name").build();

    tracker.processRelation(16384, "public", "orders", schemaV1, 1000L);

    // Evolve schema: ADD COLUMN loyalty_points INT, ADD COLUMN notes TEXT
    Schema schemaV2 =
        Schema.builder()
            .addInt32Field("id")
            .addStringField("name")
            .addInt32Field("loyalty_points")
            .addStringField("notes")
            .build();

    SchemaChangeEvent event = tracker.processRelation(16384, "public", "orders", schemaV2, 2000L);

    assertNotNull(event);
    assertEquals(SchemaChangeType.COLUMNS_ADDED, event.getChangeType());
    assertEquals(2, event.getAddedFields().size());
    assertEquals("loyalty_points", event.getAddedFields().get(0).getName());
    assertEquals("notes", event.getAddedFields().get(1).getName());
    assertEquals(schemaV1, event.getOldSchema());
    assertEquals(schemaV2, event.getNewSchema());
  }

  @Test
  public void testColumnsDroppedDetection() {
    PostgreSqlDynamicSchemaTracker tracker = new PostgreSqlDynamicSchemaTracker();

    Schema schemaV1 =
        Schema.builder()
            .addInt32Field("id")
            .addStringField("name")
            .addStringField("temp_code")
            .build();

    tracker.processRelation(16384, "public", "orders", schemaV1, 1000L);

    // Drop temp_code
    Schema schemaV2 = Schema.builder().addInt32Field("id").addStringField("name").build();

    SchemaChangeEvent event = tracker.processRelation(16384, "public", "orders", schemaV2, 2500L);

    assertNotNull(event);
    assertEquals(SchemaChangeType.COLUMNS_DROPPED, event.getChangeType());
    assertEquals(1, event.getDroppedFields().size());
    assertEquals("temp_code", event.getDroppedFields().get(0).getName());
  }

  @Test
  public void testRowProjectionAcrossSchemaEvolution() {
    Schema schemaV1 = Schema.builder().addInt32Field("id").addStringField("name").build();

    Schema schemaV2 =
        Schema.builder()
            .addInt32Field("id")
            .addStringField("name")
            .addNullableField("bonus", Schema.FieldType.INT32)
            .build();

    Row rowV1 = Row.withSchema(schemaV1).addValues(10, "Alice").build();

    Row projected = PostgreSqlDynamicSchemaTracker.projectRow(rowV1, schemaV2);

    assertEquals(schemaV2, projected.getSchema());
    assertEquals(10, (int) projected.getInt32("id"));
    assertEquals("Alice", projected.getString("name"));
    assertNull(projected.getValue("bonus"));
  }
}
