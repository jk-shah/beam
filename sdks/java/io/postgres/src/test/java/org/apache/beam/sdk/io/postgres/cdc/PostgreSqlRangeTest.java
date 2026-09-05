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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PostgreSqlRange}. */
@RunWith(JUnit4.class)
public class PostgreSqlRangeTest {

  @Test
  public void testInclusiveExclusiveBoundsParsing() {
    PostgreSqlRange<String> range = PostgreSqlRange.parse("[10, 50)");

    assertFalse(range.isEmpty());
    assertEquals("10", range.getLower());
    assertEquals("50", range.getUpper());
    assertTrue(range.isLowerInclusive());
    assertFalse(range.isUpperInclusive());
    assertFalse(range.isLowerUnbounded());
    assertFalse(range.isUpperUnbounded());
    assertEquals("[10,50)", range.toString());
  }

  @Test
  public void testUnboundedRangeParsing() {
    PostgreSqlRange<String> range = PostgreSqlRange.parse("(, 100]");

    assertFalse(range.isEmpty());
    assertNull(range.getLower());
    assertEquals("100", range.getUpper());
    assertFalse(range.isLowerInclusive());
    assertTrue(range.isUpperInclusive());
    assertTrue(range.isLowerUnbounded());
    assertFalse(range.isUpperUnbounded());
  }

  @Test
  public void testEmptyRange() {
    PostgreSqlRange<String> range = PostgreSqlRange.parse("empty");

    assertTrue(range.isEmpty());
    assertNull(range.getLower());
    assertNull(range.getUpper());
    assertEquals("empty", range.toString());
  }

  @Test
  public void testTimestampRangeWithQuotes() {
    PostgreSqlRange<String> range =
        PostgreSqlRange.parse("[\"2026-01-01 00:00:00\",\"2026-06-01 00:00:00\")");

    assertEquals("2026-01-01 00:00:00", range.getLower());
    assertEquals("2026-06-01 00:00:00", range.getUpper());
    assertTrue(range.isLowerInclusive());
    assertFalse(range.isUpperInclusive());
  }
}
