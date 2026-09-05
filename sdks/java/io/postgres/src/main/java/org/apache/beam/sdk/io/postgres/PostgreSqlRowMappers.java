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

import java.io.Serializable;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.values.Row;

/** Pre-built row mapper implementations for PostgreSQL. */
public class PostgreSqlRowMappers {

  @FunctionalInterface
  public interface RowMapper<T> extends Serializable {
    T mapRow(ResultSet resultSet) throws Exception;
  }

  /**
   * Returns a {@link RowMapper} that maps each JDBC {@link ResultSet} row to a Beam {@link Row}.
   */
  public static RowMapper<Row> forBeamSchema(Schema schema) {
    return new SchemaRowMapper(schema);
  }

  private static class SchemaRowMapper implements RowMapper<Row> {
    private final Schema schema;

    public SchemaRowMapper(Schema schema) {
      this.schema = schema;
    }

    @Override
    public Row mapRow(ResultSet rs) throws Exception {
      List<Object> values = new ArrayList<>(schema.getFieldCount());
      for (int i = 0; i < schema.getFieldCount(); i++) {
        Field field = schema.getField(i);
        Object val = PostgreSqlTypeUtils.extractFieldValue(rs, i + 1, field.getType());
        values.add(val);
      }
      return Row.withSchema(schema).addValues(values).build();
    }
  }
}
