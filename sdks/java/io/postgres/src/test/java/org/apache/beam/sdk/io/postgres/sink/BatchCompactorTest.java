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
package org.apache.beam.sdk.io.postgres.sink;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link BatchCompactor}. */
@RunWith(JUnit4.class)
public class BatchCompactorTest {

  @Test
  public void testCompactAndSortDeduplicatesAndOrders() {
    Schema schema = Schema.builder().addInt64Field("id").addStringField("val").build();

    Row r1 = Row.withSchema(schema).addValues(30L, "v1").build();
    Row r2 = Row.withSchema(schema).addValues(10L, "v2").build();
    Row r3 = Row.withSchema(schema).addValues(30L, "v3_latest").build(); // Duplicate key 30
    Row r4 = Row.withSchema(schema).addValues(20L, "v4").build();

    List<Row> result =
        BatchCompactor.compactAndSort(
            Arrays.asList(r1, r2, r3, r4), Collections.singletonList("id"));

    assertEquals(3, result.size());
    // Assert sorted order: 10, 20, 30
    assertEquals(10L, (long) result.get(0).getInt64("id"));
    assertEquals(20L, (long) result.get(1).getInt64("id"));
    assertEquals(30L, (long) result.get(2).getInt64("id"));
    // Assert LWW update
    assertEquals("v3_latest", result.get(2).getString("val"));
  }
}
