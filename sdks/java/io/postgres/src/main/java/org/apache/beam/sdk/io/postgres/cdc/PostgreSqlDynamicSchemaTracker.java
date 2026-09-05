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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.io.postgres.cdc.SchemaChangeEvent.SchemaChangeType;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Tracks in-flight DDL schema evolution across logical replication relations, detects schema diffs
 * (added, dropped, or widened columns), and produces {@link SchemaChangeEvent}s for downstream
 * sinks.
 */
public class PostgreSqlDynamicSchemaTracker implements Serializable {

  private final Map<Integer, Schema> relationSchemas = new HashMap<>();
  private final Map<String, Schema> tableSchemas = new HashMap<>();

  /**
   * Registers or updates the schema for a PostgreSQL relation.
   *
   * @param relationOid PostgreSQL relation OID
   * @param schemaName Namespace/schema name (e.g. "public")
   * @param tableName Relation table name
   * @param newSchema The new schema parsed from the RELATION message
   * @param commitLsn The LSN of the relation message
   * @return A {@link SchemaChangeEvent} if the schema changed or was newly registered, or null if
   *     identical.
   */
  public @Nullable SchemaChangeEvent processRelation(
      int relationOid, String schemaName, String tableName, Schema newSchema, long commitLsn) {
    String fullTableName = schemaName + "." + tableName;
    Schema oldSchema = relationSchemas.get(relationOid);
    if (oldSchema == null) {
      oldSchema = tableSchemas.get(fullTableName);
    }

    if (oldSchema == null) {
      // New table registered
      relationSchemas.put(relationOid, newSchema);
      tableSchemas.put(fullTableName, newSchema);
      return SchemaChangeEvent.builder()
          .setChangeType(SchemaChangeType.TABLE_CREATED)
          .setSchemaName(schemaName)
          .setTableName(tableName)
          .setRelationOid(relationOid)
          .setCommitLsn(commitLsn)
          .setTimestamp(Instant.now())
          .setOldSchema(null)
          .setNewSchema(newSchema)
          .setAddedFields(newSchema.getFields())
          .build();
    }

    if (oldSchema.equals(newSchema)) {
      // No schema change
      return null;
    }

    // Compute DDL diff
    List<Field> addedFields = new ArrayList<>();
    List<Field> droppedFields = new ArrayList<>();
    List<Field> modifiedFields = new ArrayList<>();

    Map<String, Field> oldFieldMap = new HashMap<>();
    for (Field f : oldSchema.getFields()) {
      oldFieldMap.put(f.getName(), f);
    }

    Map<String, Field> newFieldMap = new HashMap<>();
    for (Field f : newSchema.getFields()) {
      newFieldMap.put(f.getName(), f);
      Field oldF = oldFieldMap.get(f.getName());
      if (oldF == null) {
        addedFields.add(f);
      } else if (!Objects.equals(oldF.getType(), f.getType())) {
        modifiedFields.add(f);
      }
    }

    for (Field oldF : oldSchema.getFields()) {
      if (!newFieldMap.containsKey(oldF.getName())) {
        droppedFields.add(oldF);
      }
    }

    SchemaChangeType changeType;
    if (!addedFields.isEmpty() && droppedFields.isEmpty() && modifiedFields.isEmpty()) {
      changeType = SchemaChangeType.COLUMNS_ADDED;
    } else if (addedFields.isEmpty() && !droppedFields.isEmpty() && modifiedFields.isEmpty()) {
      changeType = SchemaChangeType.COLUMNS_DROPPED;
    } else {
      changeType = SchemaChangeType.COLUMNS_MODIFIED;
    }

    relationSchemas.put(relationOid, newSchema);
    tableSchemas.put(fullTableName, newSchema);

    return SchemaChangeEvent.builder()
        .setChangeType(changeType)
        .setSchemaName(schemaName)
        .setTableName(tableName)
        .setRelationOid(relationOid)
        .setCommitLsn(commitLsn)
        .setTimestamp(Instant.now())
        .setOldSchema(oldSchema)
        .setNewSchema(newSchema)
        .setAddedFields(Collections.unmodifiableList(addedFields))
        .setDroppedFields(Collections.unmodifiableList(droppedFields))
        .setModifiedFields(Collections.unmodifiableList(modifiedFields))
        .build();
  }

  public @Nullable Schema getSchemaForRelation(int relationOid) {
    return relationSchemas.get(relationOid);
  }

  public @Nullable Schema getSchemaForTable(String schemaName, String tableName) {
    return tableSchemas.get(schemaName + "." + tableName);
  }

  /**
   * Projects a source {@link Row} onto a target {@link Schema}, matching columns by name and
   * filling missing columns with nulls.
   */
  public static Row projectRow(Row sourceRow, Schema targetSchema) {
    if (sourceRow.getSchema().equals(targetSchema)) {
      return sourceRow;
    }

    List<Object> projectedValues = new ArrayList<>(targetSchema.getFieldCount());
    for (Field targetField : targetSchema.getFields()) {
      String fieldName = targetField.getName();
      if (sourceRow.getSchema().hasField(fieldName)) {
        projectedValues.add(sourceRow.getValue(fieldName));
      } else {
        projectedValues.add(null);
      }
    }

    return Row.withSchema(targetSchema).addValues(projectedValues).build();
  }
}
