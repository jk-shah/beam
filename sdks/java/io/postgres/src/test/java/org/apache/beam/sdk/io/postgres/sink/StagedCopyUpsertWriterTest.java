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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.postgres.PostgreSqlDataSourceConfiguration;
import org.apache.beam.sdk.io.postgres.PostgreSqlUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link StagedCopyUpsertWriterDoFn} and SQL generation. */
@RunWith(JUnit4.class)
public class StagedCopyUpsertWriterTest {

  @Test
  public void testBatchCompactorPreservesLatestRow() {
    Schema schema =
        Schema.builder()
            .addField(Field.of("id", FieldType.INT32))
            .addField(Field.of("status", FieldType.STRING))
            .build();

    Row row1 = Row.withSchema(schema).addValues(100, "CREATED").build();
    Row row2 = Row.withSchema(schema).addValues(100, "PAID").build();
    Row row3 = Row.withSchema(schema).addValues(101, "ACTIVE").build();

    List<Row> compacted =
        BatchCompactor.compactAndSort(
            Arrays.asList(row1, row2, row3), Collections.singletonList("id"));

    assertEquals(2, compacted.size());
    // Key 100 should have latest value "PAID"
    assertEquals(100, (int) compacted.get(0).getInt32("id"));
    assertEquals("PAID", compacted.get(0).getString("status"));
    assertEquals(101, (int) compacted.get(1).getInt32("id"));
  }

  @Test
  public void testSqlIdentifierEscapingProtectsAgainstInjection() {
    String escapedTable =
        PostgreSqlUtils.escapeTableIdentifier("public.orders; DROP TABLE students; --");
    assertTrue(escapedTable.contains("\"public\""));
    assertTrue(escapedTable.contains("\"orders; DROP TABLE students; --\""));

    String escapedColumn = PostgreSqlUtils.escapeIdentifier("col\"name");
    assertEquals("\"col\"\"name\"", escapedColumn);
  }

  @Test
  public void testStagedCopyWriterDoFnInitialization() {
    PostgreSqlDataSourceConfiguration dsConfig =
        PostgreSqlDataSourceConfiguration.create("jdbc:postgresql://localhost:5432/testdb")
            .withUsername("postgres")
            .withPassword("password");

    TupleTag<Row> successTag = new TupleTag<>("success");
    TupleTag<PostgreSqlWriteError> dlqTag = new TupleTag<>("dlq");

    StagedCopyUpsertWriterDoFn doFn =
        new StagedCopyUpsertWriterDoFn(
            dsConfig, Collections.singletonList("id"), successTag, dlqTag);

    assertNotNull(doFn);
  }
}
