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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Encapsulates a DDL schema alteration event captured from the PostgreSQL logical replication
 * stream (e.g. {@code RELATION} 0x52 messages) to support dynamic schema evolution in downstream
 * sinks (BigQuery Storage Write API, Apache Iceberg, Snowflake).
 */
@AutoValue
public abstract class SchemaChangeEvent implements Serializable {

  public enum SchemaChangeType {
    TABLE_CREATED,
    COLUMNS_ADDED,
    COLUMNS_DROPPED,
    COLUMNS_MODIFIED,
    TABLE_DROPPED
  }

  public abstract SchemaChangeType getChangeType();

  public abstract String getSchemaName();

  public abstract String getTableName();

  public abstract int getRelationOid();

  public abstract long getCommitLsn();

  public abstract Instant getTimestamp();

  public abstract @Nullable Schema getOldSchema();

  public abstract Schema getNewSchema();

  public abstract List<Field> getAddedFields();

  public abstract List<Field> getDroppedFields();

  public abstract List<Field> getModifiedFields();

  public static Builder builder() {
    return new AutoValue_SchemaChangeEvent.Builder()
        .setAddedFields(Collections.emptyList())
        .setDroppedFields(Collections.emptyList())
        .setModifiedFields(Collections.emptyList())
        .setTimestamp(Instant.now());
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setChangeType(SchemaChangeType changeType);

    public abstract Builder setSchemaName(String schemaName);

    public abstract Builder setTableName(String tableName);

    public abstract Builder setRelationOid(int relationOid);

    public abstract Builder setCommitLsn(long commitLsn);

    public abstract Builder setTimestamp(Instant timestamp);

    public abstract Builder setOldSchema(@Nullable Schema oldSchema);

    public abstract Builder setNewSchema(Schema newSchema);

    public abstract Builder setAddedFields(List<Field> addedFields);

    public abstract Builder setDroppedFields(List<Field> droppedFields);

    public abstract Builder setModifiedFields(List<Field> modifiedFields);

    public abstract SchemaChangeEvent build();
  }
}
