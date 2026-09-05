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

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distributed DoFn that reads staged CDC chunk files from Cloud Storage (GCS / S3), parses the
 * encapsulated {@code pgoutput} message frames, constructs canonical {@link ChangeEvent} rows, and
 * routes parse errors to the Dead-Letter Queue (DLQ).
 */
public class PostgreSqlStagedChunkReaderDoFn
    extends DoFn<PostgreSqlChunkMetadata, ChangeEvent<Row>> {

  private static final Logger LOG = LoggerFactory.getLogger(PostgreSqlStagedChunkReaderDoFn.class);

  private final Counter recordsDecodedCounter =
      Metrics.counter(PostgreSqlStagedChunkReaderDoFn.class, "PostgreSQL_HA_Records_Decoded_Count");
  private final Counter dlqErrorsCounter =
      Metrics.counter(PostgreSqlStagedChunkReaderDoFn.class, "PostgreSQL_HA_DLQ_Errors_Count");

  private final Map<String, Schema> tableSchemas;
  private final TupleTag<ChangeEvent<Row>> outputTag;
  private final TupleTag<Row> dlqTag;
  private transient PgOutputParser parser;

  public PostgreSqlStagedChunkReaderDoFn(
      Map<String, Schema> tableSchemas,
      TupleTag<ChangeEvent<Row>> outputTag,
      TupleTag<Row> dlqTag) {
    this.tableSchemas = tableSchemas;
    this.outputTag = outputTag;
    this.dlqTag = dlqTag;
  }

  @Setup
  public void setup() {
    this.parser = new PgOutputParser();
  }

  @ProcessElement
  public void processElement(
      @Element PostgreSqlChunkMetadata metadata, MultiOutputReceiver receiver) throws Exception {
    ResourceId resourceId = FileSystems.matchNewResource(metadata.getResourceUri(), false);

    try (ReadableByteChannel channel = FileSystems.open(resourceId);
        InputStream in = Channels.newInputStream(channel)) {

      byte[] chunkBytes = ByteStreams.toByteArray(in);
      ByteBuffer buffer = ByteBuffer.wrap(chunkBytes);

      while (buffer.hasRemaining()) {
        if (buffer.remaining() < 4) {
          break;
        }
        int frameLength = buffer.getInt();
        if (buffer.remaining() < frameLength) {
          LOG.warn(
              "Incomplete frame of length {} in chunk {}. Remaining bytes: {}",
              frameLength,
              metadata.getResourceUri(),
              buffer.remaining());
          break;
        }

        byte[] frameBytes = new byte[frameLength];
        buffer.get(frameBytes);
        ByteBuffer frameBuffer = ByteBuffer.wrap(frameBytes);

        try {
          ChangeEvent<Row> event =
              parser.parseMessage(frameBuffer, metadata.getStartLsn(), tableSchemas);
          if (event != null) {
            receiver.get(outputTag).output(event);
            recordsDecodedCounter.inc();
          }
        } catch (Exception e) {
          dlqErrorsCounter.inc();
          LOG.warn(
              "Failed to parse CDC frame in chunk {}. Routing to DLQ.",
              metadata.getResourceUri(),
              e);

          // Construct dead-letter error row
          Schema errorSchema =
              Schema.builder()
                  .addStringField("error_message")
                  .addStringField("chunk_uri")
                  .addInt64Field("timestamp")
                  .build();
          Row dlqRow =
              Row.withSchema(errorSchema)
                  .addValues(
                      e.getMessage() != null ? e.getMessage() : "Unknown parse error",
                      metadata.getResourceUri(),
                      System.currentTimeMillis())
                  .build();
          receiver.get(dlqTag).output(dlqRow);
        }
      }
    }
  }
}
