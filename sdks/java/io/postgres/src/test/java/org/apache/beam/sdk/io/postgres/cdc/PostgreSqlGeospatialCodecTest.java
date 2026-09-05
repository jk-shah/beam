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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PostgreSqlGeospatialCodec}. */
@RunWith(JUnit4.class)
public class PostgreSqlGeospatialCodecTest {

  @Test
  public void test2DPointWithSridGeoJson() {
    // Construct binary EWKB for Point(x = -122.084, y = 37.422) with SRID 4326 (WGS84)
    // Little Endian (1) + Type (1 | 0x20000000) + SRID (4326) + x (double) + y (double)
    ByteBuffer buf = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);
    buf.put((byte) 1); // Little endian
    buf.putInt(1 | PostgreSqlGeospatialCodec.SRID_FLAG); // Point with SRID
    buf.putInt(4326); // SRID
    buf.putDouble(-122.084);
    buf.putDouble(37.422);

    byte[] bytes = buf.array();
    String geoJson = PostgreSqlGeospatialCodec.decodeToGeoJson(bytes);

    assertNotNull(geoJson);
    assertTrue(geoJson.contains("\"type\":\"Point\""));
    assertTrue(geoJson.contains("-122.084000"));
    assertTrue(geoJson.contains("37.422000"));
    assertTrue(geoJson.contains("\"srid\":4326"));
  }

  @Test
  public void test3DPointGeoJson() {
    // Point(x = 10.0, y = 20.0, z = 30.0) with Z flag (0x80000000)
    ByteBuffer buf = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);
    buf.put((byte) 1);
    buf.putInt(1 | PostgreSqlGeospatialCodec.Z_FLAG);
    buf.putDouble(10.0);
    buf.putDouble(20.0);
    buf.putDouble(30.0);

    String geoJson = PostgreSqlGeospatialCodec.decodeToGeoJson(buf.array());

    assertNotNull(geoJson);
    assertTrue(geoJson.contains("\"type\":\"Point\""));
    assertTrue(geoJson.contains("[10.000000,20.000000,30.000000]"));
  }

  @Test
  public void testLineStringGeoJson() {
    // LineString with 2 points: (0, 0) -> (1, 1)
    ByteBuffer buf = ByteBuffer.allocate(48).order(ByteOrder.LITTLE_ENDIAN);
    buf.put((byte) 1);
    buf.putInt(2); // LineString without SRID
    buf.putInt(2); // 2 points
    buf.putDouble(0.0);
    buf.putDouble(0.0);
    buf.putDouble(1.0);
    buf.putDouble(1.0);

    String geoJson = PostgreSqlGeospatialCodec.decodeToGeoJson(buf.array());

    assertNotNull(geoJson);
    assertTrue(geoJson.contains("\"type\":\"LineString\""));
    assertTrue(geoJson.contains("[0.000000,0.000000]"));
    assertTrue(geoJson.contains("[1.000000,1.000000]"));
  }

  @Test
  public void testNullOrEmptyInput() {
    assertNull(PostgreSqlGeospatialCodec.decodeToGeoJson(null));
    assertNull(PostgreSqlGeospatialCodec.decodeToGeoJson(new byte[0]));
    assertNull(PostgreSqlGeospatialCodec.decodeToGeoJson(new byte[] {1, 2}));
  }
}
