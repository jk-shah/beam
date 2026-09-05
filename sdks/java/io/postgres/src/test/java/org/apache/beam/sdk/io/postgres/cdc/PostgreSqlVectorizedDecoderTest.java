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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PostgreSqlVectorizedDecoder}. */
@RunWith(JUnit4.class)
public class PostgreSqlVectorizedDecoderTest {

  @Test
  public void testDecoderInitializationAndFlags() {
    PostgreSqlVectorizedDecoder decoder = new PostgreSqlVectorizedDecoder();
    assertFalse(decoder.isNativeVectorizationEnabled());

    PostgreSqlVectorizedDecoder nativeDecoder = new PostgreSqlVectorizedDecoder(true);
    assertTrue(nativeDecoder.isNativeVectorizationEnabled());
  }

  @Test
  public void testDecodeTupleWithPrimitivesAndNulls() {
    Schema schema =
        Schema.builder()
            .addField(Field.of("id", FieldType.INT32))
            .addField(Field.of("name", FieldType.STRING))
            .addField(Field.of("price", FieldType.DOUBLE))
            .addField(Field.of("active", FieldType.BOOLEAN))
            .addField(Field.nullable("notes", FieldType.STRING))
            .build();

    // Prepare pgoutput tuple binary buffer:
    // [short: 5 columns]
    // Col 0: 't', length 4, "1001"
    // Col 1: 't', length 6, "Laptop"
    // Col 2: 't', length 6, "999.99"
    // Col 3: 't', length 1, "t"
    // Col 4: 'n' (null)
    ByteBuffer buffer = ByteBuffer.allocate(128);
    buffer.putShort((short) 5);

    // Col 0: id = 1001
    byte[] idBytes = "1001".getBytes(StandardCharsets.UTF_8);
    buffer.put((byte) 't').putInt(idBytes.length).put(idBytes);

    // Col 1: name = Laptop
    byte[] nameBytes = "Laptop".getBytes(StandardCharsets.UTF_8);
    buffer.put((byte) 't').putInt(nameBytes.length).put(nameBytes);

    // Col 2: price = 999.99
    byte[] priceBytes = "999.99".getBytes(StandardCharsets.UTF_8);
    buffer.put((byte) 't').putInt(priceBytes.length).put(priceBytes);

    // Col 3: active = true
    byte[] activeBytes = "t".getBytes(StandardCharsets.UTF_8);
    buffer.put((byte) 't').putInt(activeBytes.length).put(activeBytes);

    // Col 4: notes = null
    buffer.put((byte) 'n');

    buffer.flip();

    PostgreSqlVectorizedDecoder decoder = new PostgreSqlVectorizedDecoder();
    Row decodedRow = decoder.decodeTuple(buffer, schema);

    assertEquals(1001, (int) decodedRow.getInt32("id"));
    assertEquals("Laptop", decodedRow.getString("name"));
    assertEquals(999.99, decodedRow.getDouble("price"), 0.001);
    assertEquals(true, decodedRow.getBoolean("active"));
    assertNull(decodedRow.getString("notes"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBufferUnderflowThrowsException() {
    Schema schema = Schema.builder().addField(Field.of("id", FieldType.INT32)).build();
    ByteBuffer emptyBuffer = ByteBuffer.allocate(1);

    PostgreSqlVectorizedDecoder decoder = new PostgreSqlVectorizedDecoder();
    decoder.decodeTuple(emptyBuffer, schema);
  }
}
