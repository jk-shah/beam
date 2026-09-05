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

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.postgresql.util.PGobject;

/**
 * Utility methods for mapping between PostgreSQL data types, JDBC types, and Beam {@link Schema}
 * and {@link Row} structures.
 */
public class PostgreSqlTypeUtils {

  /** Infers a Beam {@link Schema} from a JDBC {@link ResultSetMetaData}. */
  public static Schema inferBeamSchema(ResultSetMetaData md) throws SQLException {
    Schema.Builder schemaBuilder = Schema.builder();
    int count = md.getColumnCount();
    for (int i = 1; i <= count; i++) {
      String colName = md.getColumnLabel(i);
      int jdbcType = md.getColumnType(i);
      String typeName = md.getColumnTypeName(i).toLowerCase();
      boolean nullable = md.isNullable(i) != ResultSetMetaData.columnNoNulls;

      FieldType fieldType = mapJdbcTypeToBeamFieldType(jdbcType, typeName);
      if (nullable) {
        schemaBuilder.addNullableField(colName, fieldType);
      } else {
        schemaBuilder.addField(colName, fieldType);
      }
    }
    return schemaBuilder.build();
  }

  /** Maps standard JDBC types and PostgreSQL-specific type names to Beam {@link FieldType}. */
  public static FieldType mapJdbcTypeToBeamFieldType(int jdbcType, String typeName) {
    switch (jdbcType) {
      case Types.SMALLINT:
      case Types.TINYINT:
        return FieldType.INT16;
      case Types.INTEGER:
        return FieldType.INT32;
      case Types.BIGINT:
        return FieldType.INT64;
      case Types.FLOAT:
      case Types.REAL:
        return FieldType.FLOAT;
      case Types.DOUBLE:
      case Types.NUMERIC:
      case Types.DECIMAL:
        return FieldType.DECIMAL;
      case Types.BOOLEAN:
      case Types.BIT:
        return FieldType.BOOLEAN;
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
        return FieldType.STRING;
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return FieldType.BYTES;
      case Types.TIMESTAMP:
      case Types.TIMESTAMP_WITH_TIMEZONE:
      case Types.DATE:
      case Types.TIME:
        return FieldType.DATETIME;
      case Types.ARRAY:
        if (typeName.startsWith("_int2")) {
          return FieldType.array(FieldType.INT16);
        } else if (typeName.startsWith("_int4")) {
          return FieldType.array(FieldType.INT32);
        } else if (typeName.startsWith("_int8")) {
          return FieldType.array(FieldType.INT64);
        } else if (typeName.startsWith("_text") || typeName.startsWith("_varchar")) {
          return FieldType.array(FieldType.STRING);
        }
        return FieldType.array(FieldType.STRING);
      case Types.OTHER:
      default:
        if ("json".equals(typeName) || "jsonb".equals(typeName) || "uuid".equals(typeName)) {
          return FieldType.STRING;
        }
        return FieldType.STRING;
    }
  }

  /**
   * Resolves PostgreSQL array cast type for UNNEST (?::type[]) safely against an immutable
   * whitelist.
   */
  public static String getPostgreSqlArrayCastType(FieldType fieldType) {
    TypeName typeName = fieldType.getTypeName();
    switch (typeName) {
      case INT16:
        return "smallint[]";
      case INT32:
        return "integer[]";
      case INT64:
        return "bigint[]";
      case FLOAT:
        return "real[]";
      case DOUBLE:
        return "double precision[]";
      case DECIMAL:
        return "numeric[]";
      case BOOLEAN:
        return "boolean[]";
      case BYTES:
        return "bytea[]";
      case DATETIME:
        return "timestamptz[]";
      case STRING:
      default:
        return "text[]";
    }
  }

  /**
   * Reads a typed object from a JDBC {@link ResultSet} and converts it to Beam standard
   * representation.
   */
  public static Object extractFieldValue(ResultSet rs, int colIndex, FieldType fieldType)
      throws SQLException {
    Object val = rs.getObject(colIndex);
    if (val == null || rs.wasNull()) {
      return null;
    }

    TypeName typeName = fieldType.getTypeName();
    switch (typeName) {
      case INT16:
        return rs.getShort(colIndex);
      case INT32:
        return rs.getInt(colIndex);
      case INT64:
        return rs.getLong(colIndex);
      case FLOAT:
        return rs.getFloat(colIndex);
      case DOUBLE:
        return rs.getDouble(colIndex);
      case DECIMAL:
        return rs.getBigDecimal(colIndex);
      case BOOLEAN:
        return rs.getBoolean(colIndex);
      case BYTES:
        return rs.getBytes(colIndex);
      case DATETIME:
        Timestamp ts = rs.getTimestamp(colIndex);
        return ts != null ? new Instant(ts.getTime()) : null;
      case ARRAY:
        Array sqlArray = rs.getArray(colIndex);
        if (sqlArray != null) {
          Object arrayObj = sqlArray.getArray();
          if (arrayObj instanceof Object[]) {
            return Arrays.asList((Object[]) arrayObj);
          }
        }
        return Collections.emptyList();
      case STRING:
      default:
        if (val instanceof PGobject) {
          return ((PGobject) val).getValue();
        }
        return val.toString();
    }
  }
}
