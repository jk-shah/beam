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

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

/** Security utilities for PostgreSQL SQL generation, parameter binding, and identifier escaping. */
public class PostgreSqlUtils {

  /**
   * Safely escapes a single PostgreSQL identifier (schema, table, or column name) to prevent SQL
   * injection. Delegates to standard double-quote escaping per PostgreSQL spec.
   */
  public static String escapeIdentifier(String identifier) {
    Preconditions.checkNotNull(identifier, "Identifier cannot be null");
    Preconditions.checkArgument(!identifier.trim().isEmpty(), "Identifier cannot be empty");
    try {
      StringBuilder buf = new StringBuilder();
      org.postgresql.core.Utils.escapeIdentifier(buf, identifier);
      return buf.toString();
    } catch (SQLException e) {
      // Fallback manual double-quote escaping
      return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
  }

  /**
   * Safely escapes a potentially schema-qualified table name (e.g., "public.orders" or "orders").
   */
  public static String escapeTableIdentifier(String fullTableName) {
    Preconditions.checkNotNull(fullTableName, "Table name cannot be null");
    List<String> parts =
        org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter.on('.')
            .splitToList(fullTableName);
    if (parts.size() == 1) {
      return escapeIdentifier(parts.get(0));
    } else if (parts.size() == 2) {
      return escapeIdentifier(parts.get(0)) + "." + escapeIdentifier(parts.get(1));
    } else {
      throw new IllegalArgumentException(
          "Invalid table identifier format (expected 'table' or 'schema.table'): " + fullTableName);
    }
  }

  /** Generates a parameterized UNNEST query for high-performance multi-row streaming upserts. */
  public static String buildUnnestUpsertQuery(
      String targetTable, Schema schema, List<String> primaryKeyColumns) {
    Preconditions.checkNotNull(targetTable, "targetTable cannot be null");
    Preconditions.checkNotNull(schema, "schema cannot be null");
    Preconditions.checkArgument(
        !primaryKeyColumns.isEmpty(), "primaryKeyColumns cannot be empty for upsert mode");

    String escapedTable = escapeTableIdentifier(targetTable);

    List<String> columnNames = schema.getFieldNames();
    String columnList =
        columnNames.stream()
            .map(PostgreSqlUtils::escapeIdentifier)
            .collect(Collectors.joining(", "));

    String unnestArgs =
        schema.getFields().stream()
            .map(f -> "?::" + PostgreSqlTypeUtils.getPostgreSqlArrayCastType(f.getType()))
            .collect(Collectors.joining(", "));

    String aliasColumns =
        columnNames.stream()
            .map(PostgreSqlUtils::escapeIdentifier)
            .collect(Collectors.joining(", "));

    String conflictTarget =
        primaryKeyColumns.stream()
            .map(PostgreSqlUtils::escapeIdentifier)
            .collect(Collectors.joining(", "));

    List<String> nonPkColumns =
        columnNames.stream()
            .filter(col -> !primaryKeyColumns.contains(col))
            .collect(Collectors.toList());

    String updateClause;
    if (nonPkColumns.isEmpty()) {
      updateClause = "DO NOTHING";
    } else {
      String setStatements =
          nonPkColumns.stream()
              .map(col -> escapeIdentifier(col) + " = EXCLUDED." + escapeIdentifier(col))
              .collect(Collectors.joining(", "));
      updateClause = "DO UPDATE SET " + setStatements;
    }

    return String.format(
        "INSERT INTO %s (%s) SELECT %s FROM UNNEST(%s) AS t(%s) ON CONFLICT (%s) %s",
        escapedTable,
        columnList,
        aliasColumns,
        unnestArgs,
        aliasColumns,
        conflictTarget,
        updateClause);
  }
}
