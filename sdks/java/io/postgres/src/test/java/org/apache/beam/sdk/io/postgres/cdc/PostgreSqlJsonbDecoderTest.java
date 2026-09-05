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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for JSON and JSONB decoding in {@link PostgreSqlVectorizedDecoder}. */
@RunWith(JUnit4.class)
public class PostgreSqlJsonbDecoderTest {

  @Test
  public void testTextJsonDecoding() {
    Schema schema =
        Schema.builder()
            .addInt32Field("id")
            .addField(Field.of("payload", FieldType.STRING))
            .build();

    PostgreSqlVectorizedDecoder decoder = new PostgreSqlVectorizedDecoder();

    String jsonStr = "{\"customer\":\"Alice\",\"amount\":99.99}";
    byte[] jsonBytes = jsonStr.getBytes(StandardCharsets.UTF_8);

    ByteBuffer buf = ByteBuffer.allocate(64 + jsonBytes.length);
    buf.putShort((short) 2); // 2 columns

    // Col 1: id = 1
    buf.put((byte) 't');
    buf.putInt(1);
    buf.put("1".getBytes(StandardCharsets.UTF_8));

    // Col 2: payload (text format)
    buf.put((byte) 't');
    buf.putInt(jsonBytes.length);
    buf.put(jsonBytes);

    buf.flip();
    Row row = decoder.decodeTuple(buf, schema);

    assertEquals(1, (int) row.getInt32("id"));
    assertEquals(jsonStr, row.getString("payload"));
  }

  @Test
  public void testBinaryJsonbDecoding() {
    Schema schema =
        Schema.builder()
            .addInt32Field("id")
            .addField(Field.of("attributes", FieldType.STRING))
            .build();

    PostgreSqlVectorizedDecoder decoder = new PostgreSqlVectorizedDecoder();

    String jsonContent = "{\"tier\":\"gold\",\"active\":true}";
    byte[] rawJsonBytes = jsonContent.getBytes(StandardCharsets.UTF_8);

    // In pgoutput binary format, JSONB has a 1-byte version header (0x01) followed by json text
    byte[] jsonbBytes = new byte[rawJsonBytes.length + 1];
    jsonbBytes[0] = 1; // JSONB version 1
    System.arraycopy(rawJsonBytes, 0, jsonbBytes, 1, rawJsonBytes.length);

    ByteBuffer buf = ByteBuffer.allocate(64 + jsonbBytes.length);
    buf.putShort((short) 2);

    // Col 1: id = 42
    buf.put((byte) 't');
    buf.putInt(2);
    buf.put("42".getBytes(StandardCharsets.UTF_8));

    // Col 2: attributes (binary JSONB format)
    buf.put((byte) 'b');
    buf.putInt(jsonbBytes.length);
    buf.put(jsonbBytes);

    buf.flip();
    Row row = decoder.decodeTuple(buf, schema);

    assertEquals(42, (int) row.getInt32("id"));
    assertEquals(jsonContent, row.getString("attributes"));
  }
}
