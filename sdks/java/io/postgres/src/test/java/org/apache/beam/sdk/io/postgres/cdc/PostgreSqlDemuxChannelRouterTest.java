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
import static org.junit.Assert.assertTrue;

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
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PostgreSqlDemuxChannelRouter}. */
@RunWith(JUnit4.class)
public class PostgreSqlDemuxChannelRouterTest {

  private static final TypeDescriptor<ChangeEvent<Row>> CHANGE_EVENT_TYPE =
      new TypeDescriptor<ChangeEvent<Row>>() {};

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testChannelCalculationBoundsAndDeterminism() {
    PostgreSqlDemuxChannelRouter router = new PostgreSqlDemuxChannelRouter(8);
    assertEquals(8, router.getNumChannels());

    int ch1 = router.calculateChannel("orders:1001");
    int ch2 = router.calculateChannel("orders:1001");
    int ch3 = router.calculateChannel("orders:1002");

    assertEquals(ch1, ch2);
    assertTrue(ch1 >= 0 && ch1 < 8);
    assertTrue(ch3 >= 0 && ch3 < 8);
  }

  @Test
  public void testBoundaryAndNegativeHashCodes() {
    PostgreSqlDemuxChannelRouter router = new PostgreSqlDemuxChannelRouter(16);

    // Test with null and empty key
    assertEquals(0, router.calculateChannel(null));
    assertEquals(0, router.calculateChannel(""));

    // Test with various synthetic keys
    for (int i = -1000; i <= 1000; i++) {
      int ch = router.calculateChannel("key:" + i);
      assertTrue("Channel must be >= 0, was: " + ch, ch >= 0);
      assertTrue("Channel must be < 16, was: " + ch, ch < 16);
    }
  }

  @Test
  public void testDemuxRoutingPipelineExecution() {
    PostgreSqlDemuxChannelRouter router = new PostgreSqlDemuxChannelRouter(4);
    Schema schema = Schema.builder().addField(Field.of("id", FieldType.INT32)).build();
    Row row = Row.withSchema(schema).addValue(42).build();

    ChangeEvent<Row> event =
        new ChangeEvent<>(
            OpType.INSERT,
            "public",
            "users",
            200L,
            10L,
            Instant.now(),
            null,
            row,
            Collections.emptySet());

    String key = "users:42";
    int expectedChannel = router.calculateChannel(key);
    KV<String, ChangeEvent<Row>> inputKv = KV.of(key, event);
    KV<Integer, ChangeEvent<Row>> expectedOutput = KV.of(expectedChannel, event);

    PCollection<KV<Integer, ChangeEvent<Row>>> output =
        pipeline
            .apply(
                Create.of(Collections.singletonList(inputKv))
                    .withCoder(
                        KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CHANGE_EVENT_TYPE))))
            .apply(ParDo.of(router))
            .setCoder(
                KvCoder.of(
                    org.apache.beam.sdk.coders.VarIntCoder.of(),
                    SerializableCoder.of(CHANGE_EVENT_TYPE)));

    PAssert.that(output).containsInAnyOrder(expectedOutput);
    pipeline.run();
  }
}
