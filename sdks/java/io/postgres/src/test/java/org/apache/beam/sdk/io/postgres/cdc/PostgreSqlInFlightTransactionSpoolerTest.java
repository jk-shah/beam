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
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.postgres.cdc.ChangeEvent.OpType;
import org.apache.beam.sdk.io.postgres.cdc.PostgreSqlInFlightTransactionSpooler.TransactionMessage;
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

/** Unit tests for {@link PostgreSqlInFlightTransactionSpooler}. */
@RunWith(JUnit4.class)
public class PostgreSqlInFlightTransactionSpoolerTest {

  private static final TypeDescriptor<ChangeEvent<Row>> CHANGE_EVENT_TYPE =
      new TypeDescriptor<ChangeEvent<Row>>() {};

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testTransactionMessageConstructorsAndEquality() {
    TransactionMessage msg1 = TransactionMessage.ofCommit(500L);
    TransactionMessage msg2 = TransactionMessage.ofCommit(500L);

    assertEquals(msg1, msg2);
    assertEquals(msg1.hashCode(), msg2.hashCode());
    assertEquals(500L, msg1.getTransactionId());
    assertEquals(
        PostgreSqlInFlightTransactionSpooler.MessageType.STREAM_COMMIT, msg1.getMessageType());
  }

  @Test
  public void testSpooledMutationsEmittedOnCommit() {
    Schema schema = Schema.builder().addField(Field.of("id", FieldType.INT32)).build();
    Row row1 = Row.withSchema(schema).addValue(1).build();
    Row row2 = Row.withSchema(schema).addValue(2).build();

    ChangeEvent<Row> event1 =
        new ChangeEvent<>(
            OpType.INSERT,
            "public",
            "orders",
            301L,
            700L,
            Instant.now(),
            null,
            row1,
            Collections.emptySet());

    ChangeEvent<Row> event2 =
        new ChangeEvent<>(
            OpType.INSERT,
            "public",
            "orders",
            302L,
            700L,
            Instant.now(),
            null,
            row2,
            Collections.emptySet());

    // Spool 2 mutations for tx 700, then commit tx 700
    KV<Long, TransactionMessage> m1 = KV.of(700L, TransactionMessage.ofMutation(700L, event1));
    KV<Long, TransactionMessage> m2 = KV.of(700L, TransactionMessage.ofMutation(700L, event2));
    KV<Long, TransactionMessage> commit = KV.of(700L, TransactionMessage.ofCommit(700L));

    org.apache.beam.sdk.testing.TestStream<KV<Long, TransactionMessage>> stream =
        org.apache.beam.sdk.testing.TestStream.create(
                KvCoder.of(BigEndianLongCoder.of(), SerializableCoder.of(TransactionMessage.class)))
            .addElements(m1, m2)
            .advanceProcessingTime(Duration.standardSeconds(1))
            .addElements(commit)
            .advanceWatermarkToInfinity();

    PCollection<ChangeEvent<Row>> output =
        pipeline
            .apply(stream)
            .apply(ParDo.of(new PostgreSqlInFlightTransactionSpooler(null)))
            .setCoder(SerializableCoder.of(CHANGE_EVENT_TYPE));

    PAssert.that(output).containsInAnyOrder(event1, event2);
    pipeline.run();
  }

  @Test
  public void testSpooledMutationsDiscardedOnAbort() {
    Schema schema = Schema.builder().addField(Field.of("id", FieldType.INT32)).build();
    Row row = Row.withSchema(schema).addValue(99).build();

    ChangeEvent<Row> uncommittedEvent =
        new ChangeEvent<>(
            OpType.INSERT,
            "public",
            "orders",
            401L,
            800L,
            Instant.now(),
            null,
            row,
            Collections.emptySet());

    // Spool 1 mutation for tx 800, then abort tx 800
    KV<Long, TransactionMessage> m =
        KV.of(800L, TransactionMessage.ofMutation(800L, uncommittedEvent));
    KV<Long, TransactionMessage> abort = KV.of(800L, TransactionMessage.ofAbort(800L));

    org.apache.beam.sdk.testing.TestStream<KV<Long, TransactionMessage>> stream =
        org.apache.beam.sdk.testing.TestStream.create(
                KvCoder.of(BigEndianLongCoder.of(), SerializableCoder.of(TransactionMessage.class)))
            .addElements(m)
            .advanceProcessingTime(Duration.standardSeconds(1))
            .addElements(abort)
            .advanceWatermarkToInfinity();

    PCollection<ChangeEvent<Row>> output =
        pipeline
            .apply(stream)
            .apply(ParDo.of(new PostgreSqlInFlightTransactionSpooler(null)))
            .setCoder(SerializableCoder.of(CHANGE_EVENT_TYPE));

    // Aborted transaction produces zero output elements
    PAssert.that(output).empty();
    pipeline.run();
  }
}
