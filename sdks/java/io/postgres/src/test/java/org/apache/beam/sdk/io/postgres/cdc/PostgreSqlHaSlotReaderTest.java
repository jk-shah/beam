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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.postgres.PostgreSqlDataSourceConfiguration;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link PostgreSqlHaSlotReaderDoFn} and {@link PostgreSqlStagedChunkReaderDoFn}.
 */
@RunWith(JUnit4.class)
public class PostgreSqlHaSlotReaderTest {

  private static final org.apache.beam.sdk.values.TypeDescriptor<ChangeEvent<Row>>
      CHANGE_EVENT_TYPE = new org.apache.beam.sdk.values.TypeDescriptor<ChangeEvent<Row>>() {};

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final transient TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testWalCircuitBreakerTripsOnExhaustion() {
    PostgreSqlDataSourceConfiguration dsConfig =
        PostgreSqlDataSourceConfiguration.create("jdbc:postgresql://localhost:5432/testdb")
            .withUsername("postgres")
            .withPassword("password");

    PostgreSqlHaSlotReaderDoFn reader =
        new PostgreSqlHaSlotReaderDoFn(
            dsConfig,
            "test_ha_slot",
            "test_pub",
            "gs://test-bucket/staging/",
            Duration.standardSeconds(5),
            10 * 1024 * 1024L,
            PostgreSqlStagingFormat.RAW_AVRO,
            100L * 1024 * 1024L, // 100 MB max
            true,
            null);

    // Below threshold -> No exception
    reader.checkWalCircuitBreaker(50L * 1024 * 1024L, 2000L, 1000L);

    // At or above threshold -> Throws PostgreSqlWalExhaustionException
    try {
      reader.checkWalCircuitBreaker(150L * 1024 * 1024L, 5000L, 1000L);
      fail("Expected PostgreSqlWalExhaustionException when WAL lag exceeds safety threshold");
    } catch (PostgreSqlWalExhaustionException e) {
      assertEquals("test_ha_slot", e.getSlotName());
      assertEquals(150L * 1024 * 1024L, e.getCurrentWalRetainedBytes());
      assertEquals(100L * 1024 * 1024L, e.getMaxWalRetainedBytes());
      assertTrue(e.getMessage().contains("pg_drop_replication_slot('test_ha_slot')"));
    }
  }

  @Test
  public void testChunkMetadataSerializationAndEquality() {
    Instant now = Instant.now();
    PostgreSqlChunkMetadata meta1 =
        new PostgreSqlChunkMetadata(
            "gs://bucket/chunk_1_2_abc.avro",
            "slot_1",
            100L,
            200L,
            500L,
            4096L,
            PostgreSqlStagingFormat.RAW_AVRO,
            now);

    PostgreSqlChunkMetadata meta2 =
        new PostgreSqlChunkMetadata(
            "gs://bucket/chunk_1_2_abc.avro",
            "slot_1",
            100L,
            200L,
            500L,
            4096L,
            PostgreSqlStagingFormat.RAW_AVRO,
            now);

    assertEquals(meta1, meta2);
    assertEquals(meta1.hashCode(), meta2.hashCode());
    assertEquals(100L, meta1.getStartLsn());
    assertEquals(200L, meta1.getEndLsn());
    assertEquals(500L, meta1.getRecordCount());
    assertEquals(PostgreSqlStagingFormat.RAW_AVRO, meta1.getFormat());
  }

  @Test
  public void testStagedChunkReaderParsesRawFrames() throws Exception {
    File chunkFile = tmpFolder.newFile("chunk_0_100.pgframe");

    // Write a mock pgoutput frame with 4-byte length prefix
    // Message type 'B' (Begin transaction) in pgoutput format:
    // 'B' (1 byte) + final LSN (8 bytes) + commit timestamp (8 bytes) + xid (4 bytes)
    ByteBuffer frame = ByteBuffer.allocate(1 + 8 + 8 + 4);
    frame.put((byte) 'B');
    frame.putLong(1000L); // final LSN
    frame.putLong(Instant.now().getMillis()); // timestamp
    frame.putInt(500); // xid
    frame.flip();

    try (FileOutputStream fos = new FileOutputStream(chunkFile)) {
      int len = frame.remaining();
      fos.write((len >>> 24) & 0xFF);
      fos.write((len >>> 16) & 0xFF);
      fos.write((len >>> 8) & 0xFF);
      fos.write(len & 0xFF);
      fos.write(frame.array());
    }

    PostgreSqlChunkMetadata metadata =
        new PostgreSqlChunkMetadata(
            chunkFile.getAbsolutePath(),
            "slot_1",
            0L,
            1000L,
            1L,
            chunkFile.length(),
            PostgreSqlStagingFormat.RAW_PG_FRAMES,
            Instant.now());

    Schema schema = Schema.builder().addField(Field.of("id", FieldType.INT32)).build();
    Map<String, Schema> schemas = new HashMap<>();
    schemas.put("public.orders", schema);

    Schema errorSchema =
        Schema.builder()
            .addStringField("error_message")
            .addStringField("chunk_uri")
            .addInt64Field("timestamp")
            .build();

    TupleTag<ChangeEvent<Row>> outputTag = new TupleTag<>("output");
    TupleTag<Row> dlqTag = new TupleTag<>("dlq");

    PCollectionTuple tuple =
        pipeline
            .apply(Create.of(metadata))
            .apply(
                ParDo.of(new PostgreSqlStagedChunkReaderDoFn(schemas, outputTag, dlqTag))
                    .withOutputTags(outputTag, TupleTagList.of(dlqTag)));

    PCollection<ChangeEvent<Row>> output =
        tuple
            .get(outputTag)
            .setCoder(org.apache.beam.sdk.coders.SerializableCoder.of(CHANGE_EVENT_TYPE));
    PCollection<Row> dlq = tuple.get(dlqTag).setRowSchema(errorSchema);

    // Begin message produces no output row directly (tracked in transaction state)
    // Both output and DLQ should be empty
    PAssert.that(output).empty();
    PAssert.that(dlq).empty();
    pipeline.run();
  }
}
