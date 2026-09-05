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

import java.util.Collections;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.postgres.cdc.ChangeEvent.OpType;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
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

/** Unit tests for {@link WatermarkDeduplicationDoFn} and {@link WatermarkSnapshotChunk}. */
@RunWith(JUnit4.class)
public class WatermarkDeduplicationTest {

  private static final TypeDescriptor<ChangeEvent<Row>> CHANGE_EVENT_TYPE =
      new TypeDescriptor<ChangeEvent<Row>>() {};

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testChunkMetadataPropertiesAndEquality() {
    WatermarkSnapshotChunk chunk1 =
        new WatermarkSnapshotChunk("chunk_0_100", "orders", "id", 0L, 100L);
    WatermarkSnapshotChunk chunk2 =
        new WatermarkSnapshotChunk("chunk_0_100", "orders", "id", 0L, 100L);

    assertEquals(chunk1, chunk2);
    assertEquals(chunk1.hashCode(), chunk2.hashCode());
    assertEquals("chunk_0_100", chunk1.getChunkId());
    assertEquals("orders", chunk1.getTableName());
    assertEquals("id", chunk1.getPrimaryKeyColumn());
    assertEquals(0L, chunk1.getMinId());
    assertEquals(100L, chunk1.getMaxId());
  }

  @Test
  public void testSnapshotEmittedWhenNoConcurrentLiveMutation() {
    Schema schema = Schema.builder().addField(Field.of("id", FieldType.INT32)).build();
    Row row = Row.withSchema(schema).addValue(1).build();

    ChangeEvent<Row> snapshotEvent =
        new ChangeEvent<>(
            OpType.READ,
            "public",
            "orders",
            0L,
            0L,
            Instant.now(),
            null,
            row,
            Collections.emptySet());

    KV<String, ChangeEvent<Row>> input = KV.of("orders:1", snapshotEvent);

    PCollection<ChangeEvent<Row>> output =
        pipeline
            .apply(
                Create.of(Collections.singletonList(input))
                    .withCoder(
                        KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CHANGE_EVENT_TYPE))))
            .apply(ParDo.of(new WatermarkDeduplicationDoFn(null)))
            .setCoder(SerializableCoder.of(CHANGE_EVENT_TYPE));

    PAssert.that(output).containsInAnyOrder(snapshotEvent);
    pipeline.run();
  }

  @Test
  public void testLiveMutationSupersedesSnapshotRecord() {
    Schema schema =
        Schema.builder()
            .addField(Field.of("id", FieldType.INT32))
            .addField(Field.of("status", FieldType.STRING))
            .build();

    Row oldRow = Row.withSchema(schema).addValues(1, "PENDING").build();
    Row newRow = Row.withSchema(schema).addValues(1, "SHIPPED").build();

    ChangeEvent<Row> liveUpdateEvent =
        new ChangeEvent<>(
            OpType.UPDATE,
            "public",
            "orders",
            1050L,
            42L,
            Instant.now(),
            oldRow,
            newRow,
            Collections.emptySet());

    ChangeEvent<Row> staleSnapshotEvent =
        new ChangeEvent<>(
            OpType.READ,
            "public",
            "orders",
            0L,
            0L,
            Instant.now().minus(Duration.standardSeconds(5)),
            null,
            oldRow,
            Collections.emptySet());

    // Live update arrives first, then stale snapshot arrives second for key "orders:1"
    KV<String, ChangeEvent<Row>> liveKv = KV.of("orders:1", liveUpdateEvent);
    KV<String, ChangeEvent<Row>> snapshotKv = KV.of("orders:1", staleSnapshotEvent);

    org.apache.beam.sdk.testing.TestStream<KV<String, ChangeEvent<Row>>> stream =
        org.apache.beam.sdk.testing.TestStream.create(
                KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CHANGE_EVENT_TYPE)))
            .addElements(liveKv)
            .advanceProcessingTime(Duration.standardSeconds(1))
            .addElements(snapshotKv)
            .advanceWatermarkToInfinity();

    PCollection<ChangeEvent<Row>> output =
        pipeline
            .apply(stream)
            .apply(ParDo.of(new WatermarkDeduplicationDoFn(null)))
            .setCoder(SerializableCoder.of(CHANGE_EVENT_TYPE));

    // Stale snapshot event must be suppressed; only liveUpdateEvent is emitted
    PAssert.that(output).containsInAnyOrder(liveUpdateEvent);
    pipeline.run();
  }
}
