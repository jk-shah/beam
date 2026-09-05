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

import java.util.Collections;
import java.util.HashSet;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.postgres.cdc.ChangeEvent.OpType;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PostgreSqlToastReconstructionDoFn}. */
@RunWith(JUnit4.class)
public class PostgreSqlToastReconstructionTest {

  private static final TypeDescriptor<ChangeEvent<Row>> CHANGE_EVENT_TYPE =
      new TypeDescriptor<ChangeEvent<Row>>() {};

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testToastColumnReconstructionOnUpdate() {
    Schema schema =
        Schema.builder()
            .addField(Field.of("id", FieldType.INT32))
            .addField(Field.of("title", FieldType.STRING))
            .addField(Field.nullable("large_payload", FieldType.STRING))
            .build();

    // 1. Initial INSERT with full payload
    Row insertRow =
        Row.withSchema(schema)
            .addValues(1, "Original Title", "VERY_LARGE_TOAST_DOCUMENT_CONTENTS")
            .build();

    ChangeEvent<Row> insertEvent =
        new ChangeEvent<>(
            OpType.INSERT,
            "public",
            "articles",
            100L,
            1L,
            Instant.now(),
            null,
            insertRow,
            Collections.emptySet());

    // 2. Subsequent UPDATE modifying only title; large_payload is omitted (unchanged TOAST)
    Row partialUpdateRow = Row.withSchema(schema).addValues(1, "Updated Title", null).build();

    ChangeEvent<Row> updateEvent =
        new ChangeEvent<>(
            OpType.UPDATE,
            "public",
            "articles",
            101L,
            2L,
            Instant.now(),
            null,
            partialUpdateRow,
            new HashSet<>(Collections.singletonList("large_payload")));

    // Expected fully reconstructed Row after DoFn processing
    Row expectedReconstructedRow =
        Row.withSchema(schema)
            .addValues(1, "Updated Title", "VERY_LARGE_TOAST_DOCUMENT_CONTENTS")
            .build();

    ChangeEvent<Row> expectedReconstructedEvent =
        new ChangeEvent<>(
            OpType.UPDATE,
            "public",
            "articles",
            101L,
            2L,
            updateEvent.getCommitTimestamp(),
            null,
            expectedReconstructedRow,
            Collections.emptySet());

    KV<String, ChangeEvent<Row>> insertKv = KV.of("articles:1", insertEvent);
    KV<String, ChangeEvent<Row>> updateKv = KV.of("articles:1", updateEvent);

    org.apache.beam.sdk.testing.TestStream<KV<String, ChangeEvent<Row>>> stream =
        org.apache.beam.sdk.testing.TestStream.create(
                KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CHANGE_EVENT_TYPE)))
            .addElements(insertKv)
            .advanceProcessingTime(Duration.standardSeconds(1))
            .addElements(updateKv)
            .advanceWatermarkToInfinity();

    PCollection<ChangeEvent<Row>> output =
        pipeline
            .apply(stream)
            .apply(ParDo.of(new PostgreSqlToastReconstructionDoFn(null)))
            .setCoder(SerializableCoder.of(CHANGE_EVENT_TYPE));

    PAssert.that(output).containsInAnyOrder(insertEvent, expectedReconstructedEvent);
    pipeline.run();
  }
}
