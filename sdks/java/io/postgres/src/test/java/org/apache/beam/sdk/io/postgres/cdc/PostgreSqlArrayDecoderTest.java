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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PostgreSqlArrayDecoder}. */
@RunWith(JUnit4.class)
public class PostgreSqlArrayDecoderTest {

  @Test
  public void testTextIntegerArrayDecoding() {
    String textPayload = "{10,20,30,NULL,50}";
    List<@Nullable Object> result =
        PostgreSqlArrayDecoder.decodeArray(
            textPayload.getBytes(StandardCharsets.UTF_8), FieldType.INT32);

    assertEquals(5, result.size());
    assertEquals(10, result.get(0));
    assertEquals(20, result.get(1));
    assertEquals(30, result.get(2));
    assertNull(result.get(3));
    assertEquals(50, result.get(4));
  }

  @Test
  public void testTextStringArrayWithQuotesAndCommas() {
    String textPayload = "{\"apple\",\"banana,with,comma\",\"quote\\\"escaped\",NULL,\"orange\"}";
    List<@Nullable Object> result =
        PostgreSqlArrayDecoder.decodeArray(
            textPayload.getBytes(StandardCharsets.UTF_8), FieldType.STRING);

    assertEquals(5, result.size());
    assertEquals("apple", result.get(0));
    assertEquals("banana,with,comma", result.get(1));
    assertEquals("quote\"escaped", result.get(2));
    assertNull(result.get(3));
    assertEquals("orange", result.get(4));
  }

  @Test
  public void testBinaryInt32ArrayDecoding() {
    // Construct binary format: ndim=1, flags=1, elemOid=23, dimLen=3, dimLbound=1
    // Elements: 100, NULL (-1), 300
    ByteBuffer buf = ByteBuffer.allocate(64);
    buf.putInt(1); // ndim
    buf.putInt(1); // flags
    buf.putInt(23); // elemOid (int4)
    buf.putInt(3); // dimLen
    buf.putInt(1); // dimLbound

    // Element 1: len=4, value=100
    buf.putInt(4);
    buf.putInt(100);

    // Element 2: len=-1 (NULL)
    buf.putInt(-1);

    // Element 3: len=4, value=300
    buf.putInt(4);
    buf.putInt(300);

    buf.flip();
    byte[] binaryBytes = new byte[buf.remaining()];
    buf.get(binaryBytes);

    List<@Nullable Object> result =
        PostgreSqlArrayDecoder.decodeArray(binaryBytes, FieldType.INT32);

    assertEquals(3, result.size());
    assertEquals(100, result.get(0));
    assertNull(result.get(1));
    assertEquals(300, result.get(2));
  }

  @Test
  public void testBinaryDoubleArrayDecoding() {
    ByteBuffer buf = ByteBuffer.allocate(64);
    buf.putInt(1); // ndim
    buf.putInt(0); // flags
    buf.putInt(701); // elemOid (float8)
    buf.putInt(2); // dimLen
    buf.putInt(1); // dimLbound

    buf.putInt(8);
    buf.putDouble(3.14159);

    buf.putInt(8);
    buf.putDouble(2.71828);

    buf.flip();
    byte[] binaryBytes = new byte[buf.remaining()];
    buf.get(binaryBytes);

    List<@Nullable Object> result =
        PostgreSqlArrayDecoder.decodeArray(binaryBytes, FieldType.DOUBLE);

    assertEquals(2, result.size());
    assertEquals(3.14159, (Double) result.get(0), 0.0001);
    assertEquals(2.71828, (Double) result.get(1), 0.0001);
  }

  @Test
  public void testEmptyArray() {
    List<@Nullable Object> result =
        PostgreSqlArrayDecoder.decodeArray("{}".getBytes(StandardCharsets.UTF_8), FieldType.INT32);
    assertTrue(result.isEmpty());

    List<@Nullable Object> emptyBytes =
        PostgreSqlArrayDecoder.decodeArray(new byte[0], FieldType.STRING);
    assertTrue(emptyBytes.isEmpty());
  }
}
