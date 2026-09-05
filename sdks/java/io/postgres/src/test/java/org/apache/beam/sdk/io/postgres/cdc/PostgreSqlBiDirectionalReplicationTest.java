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
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.io.postgres.PostgreSqlDataSourceConfiguration;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for bi-directional multi-master replication and conflict resolution. */
@RunWith(JUnit4.class)
public class PostgreSqlBiDirectionalReplicationTest {

  @Test
  public void testOriginFilterOptionInReadCDC() {
    PostgreSqlDataSourceConfiguration config =
        PostgreSqlDataSourceConfiguration.create("jdbc:postgresql://localhost:5432/mydb");

    PostgreSqlReadCDC readCDC =
        PostgreSqlReadCDC.builder()
            .setDataSourceConfiguration(config)
            .setPublicationName("my_pub")
            .setReplicationSlotName("my_slot")
            .setOriginFilter("none")
            .build();

    assertEquals("none", readCDC.getOriginFilter());
  }

  @Test
  public void testLastWriteWinsConflictResolution() {
    Schema schema = Schema.builder().addInt32Field("id").addStringField("val").build();
    Row row1 = Row.withSchema(schema).addValues(1, "version_1").build();
    Row row2 = Row.withSchema(schema).addValues(1, "version_2").build();

    Instant t1 = Instant.parse("2026-08-27T10:00:00.000Z");
    Instant t2 = Instant.parse("2026-08-27T10:00:05.000Z");

    ChangeEvent<Row> event1 =
        new ChangeEvent<>(
            ChangeEvent.OpType.UPDATE, "public", "items", 1000L, 101L, t1, null, row1, null);

    ChangeEvent<Row> event2 =
        new ChangeEvent<>(
            ChangeEvent.OpType.UPDATE, "public", "items", 1500L, 102L, t2, null, row2, null);

    // Event2 is fresher than Event1 (t2 > t1)
    assertTrue(PostgreSqlConflictResolverDoFn.isIncomingFresher(event2, event1));

    // Event1 is older than Event2 (t1 < t2)
    assertFalse(PostgreSqlConflictResolverDoFn.isIncomingFresher(event1, event2));
  }

  @Test
  public void testConflictResolutionWithIdenticalTimestampsTieBreaksOnLsn() {
    Schema schema = Schema.builder().addInt32Field("id").addStringField("val").build();
    Row row1 = Row.withSchema(schema).addValues(1, "v1").build();
    Row row2 = Row.withSchema(schema).addValues(1, "v2").build();

    Instant sameTime = Instant.parse("2026-08-27T12:00:00.000Z");

    ChangeEvent<Row> eventLsn1000 =
        new ChangeEvent<>(
            ChangeEvent.OpType.UPDATE, "public", "items", 1000L, 201L, sameTime, null, row1, null);

    ChangeEvent<Row> eventLsn2000 =
        new ChangeEvent<>(
            ChangeEvent.OpType.UPDATE, "public", "items", 2000L, 202L, sameTime, null, row2, null);

    // When timestamps are identical, higher LSN (2000 > 1000) wins
    assertTrue(PostgreSqlConflictResolverDoFn.isIncomingFresher(eventLsn2000, eventLsn1000));
    assertFalse(PostgreSqlConflictResolverDoFn.isIncomingFresher(eventLsn1000, eventLsn2000));
  }
}
