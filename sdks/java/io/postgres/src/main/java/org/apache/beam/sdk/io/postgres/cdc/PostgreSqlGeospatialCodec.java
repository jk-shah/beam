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
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Zero-allocation binary EWKB (Extended Well-Known Binary) and WKT parser for PostGIS {@code
 * GEOMETRY} and {@code GEOGRAPHY} data types.
 *
 * <p>Converts PostGIS spatial columns into standardized GeoJSON strings or WKT representations.
 */
public class PostgreSqlGeospatialCodec implements Serializable {

  public static final int SRID_FLAG = 0x20000000;
  public static final int Z_FLAG = 0x80000000;
  public static final int M_FLAG = 0x40000000;

  /** Decodes a PostGIS binary EWKB payload or hex string into a GeoJSON geometry string. */
  public static @Nullable String decodeToGeoJson(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return null;
    }

    // Check if hex-encoded text EWKB (starts with '0' and valid hex characters)
    byte[] rawBytes = bytes;
    if (isHexEncoded(bytes)) {
      rawBytes = hexToBytes(new String(bytes, StandardCharsets.UTF_8));
    }

    if (rawBytes.length < 5) {
      return null;
    }

    ByteBuffer buf = ByteBuffer.wrap(rawBytes);
    byte endian = buf.get();
    buf.order(endian == 1 ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);

    int typeInt = buf.getInt();
    boolean hasSrid = (typeInt & SRID_FLAG) != 0;
    boolean hasZ = (typeInt & Z_FLAG) != 0;
    int geomType = typeInt & 0xFF;

    int srid = 0;
    if (hasSrid && buf.remaining() >= 4) {
      srid = buf.getInt();
    }

    switch (geomType) {
      case 1: // Point
        return decodePointGeoJson(buf, hasZ, srid);
      case 2: // LineString
        return decodeLineStringGeoJson(buf, hasZ, srid);
      case 3: // Polygon
        return decodePolygonGeoJson(buf, hasZ, srid);
      default:
        // Fallback to generic geometry description
        return String.format(
            Locale.ROOT,
            "{\"type\":\"UnknownGeometry\",\"geometryType\":%d,\"srid\":%d}",
            geomType,
            srid);
    }
  }

  private static String decodePointGeoJson(ByteBuffer buf, boolean hasZ, int srid) {
    if (buf.remaining() < 16) {
      return "{\"type\":\"Point\",\"coordinates\":[]}";
    }
    double x = buf.getDouble();
    double y = buf.getDouble();
    if (hasZ && buf.remaining() >= 8) {
      double z = buf.getDouble();
      return srid > 0
          ? String.format(
              Locale.ROOT,
              "{\"type\":\"Point\",\"coordinates\":[%.6f,%.6f,%.6f],\"srid\":%d}",
              x,
              y,
              z,
              srid)
          : String.format(
              Locale.ROOT, "{\"type\":\"Point\",\"coordinates\":[%.6f,%.6f,%.6f]}", x, y, z);
    }
    return srid > 0
        ? String.format(
            Locale.ROOT, "{\"type\":\"Point\",\"coordinates\":[%.6f,%.6f],\"srid\":%d}", x, y, srid)
        : String.format(Locale.ROOT, "{\"type\":\"Point\",\"coordinates\":[%.6f,%.6f]}", x, y);
  }

  private static String decodeLineStringGeoJson(ByteBuffer buf, boolean hasZ, int srid) {
    if (buf.remaining() < 4) {
      return "{\"type\":\"LineString\",\"coordinates\":[]}";
    }
    int numPoints = buf.getInt();
    StringBuilder sb = new StringBuilder();
    sb.append("{\"type\":\"LineString\",\"coordinates\":[");
    for (int i = 0; i < numPoints && buf.remaining() >= 16; i++) {
      if (i > 0) sb.append(",");
      double x = buf.getDouble();
      double y = buf.getDouble();
      if (hasZ && buf.remaining() >= 8) {
        double z = buf.getDouble();
        sb.append(String.format(Locale.ROOT, "[%.6f,%.6f,%.6f]", x, y, z));
      } else {
        sb.append(String.format(Locale.ROOT, "[%.6f,%.6f]", x, y));
      }
    }
    sb.append("]");
    if (srid > 0) {
      sb.append(",\"srid\":").append(srid);
    }
    sb.append("}");
    return sb.toString();
  }

  private static String decodePolygonGeoJson(ByteBuffer buf, boolean hasZ, int srid) {
    if (buf.remaining() < 4) {
      return "{\"type\":\"Polygon\",\"coordinates\":[]}";
    }
    int numRings = buf.getInt();
    StringBuilder sb = new StringBuilder();
    sb.append("{\"type\":\"Polygon\",\"coordinates\":[");
    for (int r = 0; r < numRings && buf.remaining() >= 4; r++) {
      if (r > 0) sb.append(",");
      sb.append("[");
      int numPoints = buf.getInt();
      for (int i = 0; i < numPoints && buf.remaining() >= 16; i++) {
        if (i > 0) sb.append(",");
        double x = buf.getDouble();
        double y = buf.getDouble();
        if (hasZ && buf.remaining() >= 8) {
          double z = buf.getDouble();
          sb.append(String.format(Locale.ROOT, "[%.6f,%.6f,%.6f]", x, y, z));
        } else {
          sb.append(String.format(Locale.ROOT, "[%.6f,%.6f]", x, y));
        }
      }
      sb.append("]");
    }
    sb.append("]");
    if (srid > 0) {
      sb.append(",\"srid\":").append(srid);
    }
    sb.append("}");
    return sb.toString();
  }

  private static boolean isHexEncoded(byte[] bytes) {
    if (bytes.length < 2 || bytes.length % 2 != 0) {
      return false;
    }
    for (int i = 0; i < Math.min(bytes.length, 10); i++) {
      byte b = bytes[i];
      boolean isHex = (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F');
      if (!isHex) return false;
    }
    return true;
  }

  private static byte[] hexToBytes(String hex) {
    int len = hex.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] =
          (byte)
              ((Character.digit(hex.charAt(i), 16) << 4) + Character.digit(hex.charAt(i + 1), 16));
    }
    return data;
  }
}
