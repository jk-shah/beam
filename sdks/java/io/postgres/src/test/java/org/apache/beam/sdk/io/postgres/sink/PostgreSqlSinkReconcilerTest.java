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

import java.io.Serializable;
import org.apache.beam.sdk.io.postgres.cdc.ChangeEvent;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PostgreSqlSinkReconciler}. */
@RunWith(JUnit4.class)
public class PostgreSqlSinkReconcilerTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testReconcileTombstonesEmitsDeleteForOrphanedTargetRows() {
    Schema schema = Schema.builder().addInt32Field("id").addStringField("name").build();

    Row row1 = Row.withSchema(schema).addValues(1, "Alice").build();
    Row row2 = Row.withSchema(schema).addValues(2, "Bob").build();
    Row row3 = Row.withSchema(schema).addValues(3, "Charlie").build(); // Orphaned in target

    PCollection<KV<String, Row>> targetRows =
        pipeline
            .apply(
                "CreateTargetRows", Create.of(KV.of("1", row1), KV.of("2", row2), KV.of("3", row3)))
            .setCoder(
                org.apache.beam.sdk.coders.KvCoder.of(
                    org.apache.beam.sdk.coders.StringUtf8Coder.of(),
                    org.apache.beam.sdk.coders.RowCoder.of(schema)));

    PCollection<KV<String, Row>> sourceRows =
        pipeline
            .apply("CreateSourceRows", Create.of(KV.of("1", row1), KV.of("2", row2)))
            .setCoder(
                org.apache.beam.sdk.coders.KvCoder.of(
                    org.apache.beam.sdk.coders.StringUtf8Coder.of(),
                    org.apache.beam.sdk.coders.RowCoder.of(schema)));

    PCollection<ChangeEvent<Row>> tombstones =
        PostgreSqlSinkReconciler.reconcileTombstones(targetRows, sourceRows, "public", "users");

    PAssert.that(tombstones)
        .satisfies(
            (SerializableFunction<Iterable<ChangeEvent<Row>>, Void>)
                events -> {
                  int count = 0;
                  for (ChangeEvent<Row> event : events) {
                    count++;
                    org.junit.Assert.assertEquals(ChangeEvent.OpType.DELETE, event.getOpType());
                    org.junit.Assert.assertEquals("users", event.getTableName());
                    org.junit.Assert.assertEquals("public", event.getSchemaName());
                    org.junit.Assert.assertNotNull(event.getBefore());
                    org.junit.Assert.assertEquals(3, (int) event.getBefore().getInt32("id"));
                    org.junit.Assert.assertNull(event.getAfter());
                  }
                  org.junit.Assert.assertEquals(1, count);
                  return null;
                });

    pipeline.run().waitUntilFinish();
  }
}
