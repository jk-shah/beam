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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * High-performance binary and text array decoder for PostgreSQL {@code pgoutput} array columns.
 *
 * <p>Supports zero-allocation binary parsing of 1D and multi-dimensional PostgreSQL arrays ({@code
 * INT4[]}, {@code INT8[]}, {@code TEXT[]}, {@code FLOAT8[]}, {@code BOOLEAN[]}, etc.) directly from
 * {@link ByteBuffer}, and provides robust unquoting fallback for text wire formats.
 */
public class PostgreSqlArrayDecoder implements Serializable {

  /**
   * Decodes array payload (either binary wire layout or text representation) into a Java {@link
   * List}.
   *
   * @param bytes Raw byte array of the column payload.
   * @param elemType The target element {@link FieldType}.
   * @return A {@link List} of decoded element objects.
   */
  public static List<@Nullable Object> decodeArray(byte[] bytes, FieldType elemType) {
    if (bytes.length == 0) {
      return Collections.emptyList();
    }

    // Check if the payload is in PostgreSQL text format: starts with '{' and ends with '}'
    if (bytes[0] == '{' && bytes[bytes.length - 1] == '}') {
      return decodeTextArray(new String(bytes, StandardCharsets.UTF_8), elemType);
    }

    // Otherwise, parse as PostgreSQL binary array wire format
    try {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      return decodeBinaryArray(buffer, elemType);
    } catch (Exception e) {
      // Fallback to text parsing if binary layout cannot be parsed
      return decodeTextArray(new String(bytes, StandardCharsets.UTF_8), elemType);
    }
  }

  /**
   * Decodes PostgreSQL binary array wire format: [int32: ndim] [int32: flags] [int32:
   * elem_type_oid] [ndim pairs of: int32 dim_length, int32 dim_lbound] [elements: int32 elem_len
   * (-1 for NULL), followed by elem_bytes]
   */
  public static List<@Nullable Object> decodeBinaryArray(ByteBuffer buffer, FieldType elemType) {
    if (buffer.remaining() < 12) {
      return Collections.emptyList();
    }

    int ndim = buffer.getInt();
    buffer.getInt(); // flags (0 = no nulls, 1 = has nulls)
    buffer.getInt(); // elemOid

    if (ndim == 0) {
      return Collections.emptyList();
    }

    int totalElements = 1;
    for (int i = 0; i < ndim; i++) {
      if (buffer.remaining() < 8) {
        return Collections.emptyList();
      }
      int dimLen = buffer.getInt();
      buffer.getInt(); // dimLbound (dimension lower bound)
      totalElements *= dimLen;
    }

    if (totalElements < 0 || totalElements > 10_000_000) {
      throw new IllegalArgumentException("Invalid array total elements count: " + totalElements);
    }

    List<@Nullable Object> result = new ArrayList<>(totalElements);
    for (int i = 0; i < totalElements && buffer.hasRemaining(); i++) {
      int elemLen = buffer.getInt();
      if (elemLen == -1) {
        // Null element
        result.add(null);
      } else if (elemLen == 0) {
        result.add(parseBinaryElement(new byte[0], elemType));
      } else {
        if (buffer.remaining() < elemLen) {
          throw new IllegalArgumentException("Buffer underflow while reading array element");
        }
        byte[] elemBytes = new byte[elemLen];
        buffer.get(elemBytes);
        result.add(parseBinaryElement(elemBytes, elemType));
      }
    }

    return result;
  }

  private static @Nullable Object parseBinaryElement(byte[] elemBytes, FieldType elemType) {
    TypeName typeName = elemType.getTypeName();
    ByteBuffer buf = ByteBuffer.wrap(elemBytes);

    switch (typeName) {
      case INT16:
        return elemBytes.length >= 2
            ? buf.getShort()
            : Short.parseShort(new String(elemBytes, StandardCharsets.UTF_8));
      case INT32:
        return elemBytes.length >= 4
            ? buf.getInt()
            : Integer.parseInt(new String(elemBytes, StandardCharsets.UTF_8));
      case INT64:
        return elemBytes.length >= 8
            ? buf.getLong()
            : Long.parseLong(new String(elemBytes, StandardCharsets.UTF_8));
      case FLOAT:
        return elemBytes.length >= 4
            ? buf.getFloat()
            : Float.parseFloat(new String(elemBytes, StandardCharsets.UTF_8));
      case DOUBLE:
        return elemBytes.length >= 8
            ? buf.getDouble()
            : Double.parseDouble(new String(elemBytes, StandardCharsets.UTF_8));
      case BOOLEAN:
        return elemBytes.length >= 1
            ? elemBytes[0] != 0
            : Boolean.parseBoolean(new String(elemBytes, StandardCharsets.UTF_8));
      case BYTES:
        return elemBytes;
      case STRING:
      default:
        return new String(elemBytes, StandardCharsets.UTF_8);
    }
  }

  /**
   * Parses PostgreSQL text array format e.g. {@code {1,2,3}} or {@code
   * {"a","b,with,comma","escaped\"quote",NULL}}.
   */
  public static List<@Nullable Object> decodeTextArray(String text, FieldType elemType) {
    String trimmed = text.trim();
    if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
      return Collections.emptyList();
    }

    String content = trimmed.substring(1, trimmed.length() - 1);
    if (content.isEmpty()) {
      return Collections.emptyList();
    }

    List<String> tokens = parseArrayTokens(content);
    List<@Nullable Object> result = new ArrayList<>(tokens.size());

    for (String token : tokens) {
      if (token == null || "NULL".equalsIgnoreCase(token)) {
        result.add(null);
      } else {
        result.add(parseTextElement(token, elemType));
      }
    }

    return result;
  }

  private static List<@Nullable String> parseArrayTokens(String content) {
    List<@Nullable String> tokens = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean inQuotes = false;
    boolean escaped = false;

    for (int i = 0; i < content.length(); i++) {
      char c = content.charAt(i);

      if (escaped) {
        current.append(c);
        escaped = false;
      } else if (c == '\\') {
        escaped = true;
      } else if (c == '"') {
        inQuotes = !inQuotes;
      } else if (c == ',' && !inQuotes) {
        tokens.add(current.toString());
        current.setLength(0);
      } else {
        current.append(c);
      }
    }

    tokens.add(current.toString());
    return tokens;
  }

  private static @Nullable Object parseTextElement(String token, FieldType elemType) {
    TypeName typeName = elemType.getTypeName();
    switch (typeName) {
      case INT16:
        return Short.parseShort(token);
      case INT32:
        return Integer.parseInt(token);
      case INT64:
        return Long.parseLong(token);
      case FLOAT:
        return Float.parseFloat(token);
      case DOUBLE:
        return Double.parseDouble(token);
      case BOOLEAN:
        return "t".equalsIgnoreCase(token) || "true".equalsIgnoreCase(token) || "1".equals(token);
      case BYTES:
        return token.getBytes(StandardCharsets.UTF_8);
      case STRING:
      default:
        return token;
    }
  }
}
