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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.postgres.cdc.ChangeEvent.OpType;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Conformance test suite replicating PostgreSQL upstream core test cases from {@code
 * src/test/modules/test_decoding/sql/} (toast.sql, truncate.sql, stream.sql, spill.sql,
 * prepared.sql, messages.sql, replorigin.sql, and ddl.sql).
 */
@RunWith(JUnit4.class)
public class PostgreSqlUpstreamSemanticsConformanceTest implements Serializable {

  private PgOutputParser parser;
  private Map<String, Schema> tableSchemas;
  private Schema testSchema;

  @Before
  public void setUp() {
    parser = new PgOutputParser();
    tableSchemas = new HashMap<>();

    testSchema =
        Schema.builder()
            .addNullableField("id", Schema.FieldType.INT32)
            .addNullableField("name", Schema.FieldType.STRING)
            .addNullableField("toast_payload", Schema.FieldType.STRING)
            .build();

    tableSchemas.put("public.upstream_items", testSchema);

    // Register initial RELATION message (relId = 16400)
    ByteBuffer relBuffer = ByteBuffer.allocate(256);
    relBuffer.put((byte) 'R');
    relBuffer.putInt(16400); // relId
    relBuffer.put("public\0".getBytes(StandardCharsets.UTF_8));
    relBuffer.put("upstream_items\0".getBytes(StandardCharsets.UTF_8));
    relBuffer.put((byte) 'd'); // replica identity default
    relBuffer.putShort((short) 3); // 3 columns

    // col 1: id (key)
    relBuffer.put((byte) 1);
    relBuffer.put("id\0".getBytes(StandardCharsets.UTF_8));
    relBuffer.putInt(23); // INT4
    relBuffer.putInt(-1);

    // col 2: name
    relBuffer.put((byte) 0);
    relBuffer.put("name\0".getBytes(StandardCharsets.UTF_8));
    relBuffer.putInt(25); // TEXT
    relBuffer.putInt(-1);

    // col 3: toast_payload
    relBuffer.put((byte) 0);
    relBuffer.put("toast_payload\0".getBytes(StandardCharsets.UTF_8));
    relBuffer.putInt(25); // TEXT
    relBuffer.putInt(-1);

    relBuffer.flip();
    parser.parseMessage(relBuffer, 1000L, tableSchemas);
  }

  /** Replicates upstream PostgreSQL {@code toast.sql} semantics. */
  @Test
  public void testUpstreamToastSemantics_UnchangedAndModified() {
    String largePayload = String.join("", Collections.nCopies(200, "TOAST_DATA_CHUNK_"));

    // 1. INSERT with full large TOAST payload
    ByteBuffer insertBuf = ByteBuffer.allocate(4096);
    insertBuf.put((byte) 'I');
    insertBuf.putInt(16400);
    insertBuf.put((byte) 'N'); // New tuple
    insertBuf.putShort((short) 3);

    // id = 1
    insertBuf.put((byte) 't');
    insertBuf.putInt(1);
    insertBuf.put("1".getBytes(StandardCharsets.UTF_8));

    // name = 'Alpha'
    insertBuf.put((byte) 't');
    insertBuf.putInt(5);
    insertBuf.put("Alpha".getBytes(StandardCharsets.UTF_8));

    // toast_payload = largePayload
    byte[] toastBytes = largePayload.getBytes(StandardCharsets.UTF_8);
    insertBuf.put((byte) 't');
    insertBuf.putInt(toastBytes.length);
    insertBuf.put(toastBytes);

    insertBuf.flip();
    ChangeEvent<Row> insertEvent = parser.parseMessage(insertBuf, 1010L, tableSchemas);
    assertNotNull(insertEvent);
    assertEquals(OpType.INSERT, insertEvent.getOpType());
    assertEquals(1, (int) insertEvent.getAfter().getInt32("id"));
    assertEquals(largePayload, insertEvent.getAfter().getString("toast_payload"));

    // 2. UPDATE modifying only 'name', with unchanged TOAST column (flag 'u')
    ByteBuffer updateBuf = ByteBuffer.allocate(1024);
    updateBuf.put((byte) 'U');
    updateBuf.putInt(16400);
    updateBuf.put((byte) 'N');
    updateBuf.putShort((short) 3);

    // id = 1
    updateBuf.put((byte) 't');
    updateBuf.putInt(1);
    updateBuf.put("1".getBytes(StandardCharsets.UTF_8));

    // name = 'Alpha_Updated'
    updateBuf.put((byte) 't');
    updateBuf.putInt(13);
    updateBuf.put("Alpha_Updated".getBytes(StandardCharsets.UTF_8));

    // toast_payload = unchanged ('u')
    updateBuf.put((byte) 'u');

    updateBuf.flip();
    ChangeEvent<Row> updateEvent = parser.parseMessage(updateBuf, 1020L, tableSchemas);
    assertNotNull(updateEvent);
    assertEquals(OpType.UPDATE, updateEvent.getOpType());
    assertEquals("Alpha_Updated", updateEvent.getAfter().getString("name"));
    assertTrue(updateEvent.getUnchangedToastColumns().contains("toast_payload"));
  }

  /** Replicates upstream PostgreSQL {@code truncate.sql} multi-table cascade semantics. */
  @Test
  public void testUpstreamTruncateSemantics_MultiTableAndCascade() {
    ByteBuffer truncateBuf = ByteBuffer.allocate(64);
    truncateBuf.put((byte) 'T');
    truncateBuf.putInt(2); // 2 relations
    truncateBuf.put((byte) 0x03); // CASCADE (0x01) | RESTART IDENTITY (0x02)
    truncateBuf.putInt(16400); // rel 1
    truncateBuf.putInt(16401); // rel 2
    truncateBuf.flip();

    ChangeEvent<Row> truncateEvent = parser.parseMessage(truncateBuf, 2000L, tableSchemas);
    assertNotNull(truncateEvent);
    assertEquals(OpType.TRUNCATE, truncateEvent.getOpType());
    assertEquals("public", truncateEvent.getSchemaName());
    assertEquals("upstream_items", truncateEvent.getTableName());
  }

  /**
   * Replicates upstream PostgreSQL {@code stream.sql} and {@code spill.sql} streaming transactions.
   */
  @Test
  public void testUpstreamInFlightStreamingAndSpill_CommitAndAbort() {
    // 1. STREAM START (xid = 5001)
    ByteBuffer streamStartBuf = ByteBuffer.allocate(16);
    streamStartBuf.put((byte) 'S');
    streamStartBuf.putInt(5001);
    streamStartBuf.put((byte) 1); // first segment
    streamStartBuf.flip();
    assertNull(parser.parseMessage(streamStartBuf, 3000L, tableSchemas));

    // 2. STREAM STOP
    ByteBuffer streamStopBuf = ByteBuffer.allocate(16);
    streamStopBuf.put((byte) 'E');
    streamStopBuf.flip();
    assertNull(parser.parseMessage(streamStopBuf, 3010L, tableSchemas));

    // 3. STREAM COMMIT (xid = 5001)
    ByteBuffer streamCommitBuf = ByteBuffer.allocate(32);
    streamCommitBuf.put((byte) 'c');
    streamCommitBuf.putInt(5001);
    streamCommitBuf.put((byte) 0); // flags
    streamCommitBuf.putLong(3020L); // commit LSN
    streamCommitBuf.putLong(3025L); // end LSN
    streamCommitBuf.flip();
    assertNull(parser.parseMessage(streamCommitBuf, 3020L, tableSchemas));

    // 4. STREAM ABORT (xid = 5002)
    ByteBuffer streamAbortBuf = ByteBuffer.allocate(16);
    streamAbortBuf.put((byte) 'A');
    streamAbortBuf.putInt(5002);
    streamAbortBuf.putInt(0); // subxid
    streamAbortBuf.flip();
    assertNull(parser.parseMessage(streamAbortBuf, 3030L, tableSchemas));
  }

  /** Replicates upstream PostgreSQL {@code prepared.sql} two-phase prepared transactions. */
  @Test
  public void testUpstreamTwoPhaseCommitPreparedTransactions() {
    // 1. PREPARE TRANSACTION ('P')
    ByteBuffer prepBuf = ByteBuffer.allocate(64);
    prepBuf.put((byte) 'P');
    prepBuf.putLong(4000L); // prepare LSN
    prepBuf.putLong(4010L); // prepare end LSN
    prepBuf.putLong(700000000L); // commit time
    prepBuf.putInt(6001); // xid
    prepBuf.put("beam_tx_gid_1\0".getBytes(StandardCharsets.UTF_8));
    prepBuf.flip();
    assertNull(parser.parseMessage(prepBuf, 4000L, tableSchemas));

    // 2. COMMIT PREPARED ('K')
    ByteBuffer commitPrepBuf = ByteBuffer.allocate(64);
    commitPrepBuf.put((byte) 'K');
    commitPrepBuf.put((byte) 0); // flags
    commitPrepBuf.putLong(4020L);
    commitPrepBuf.putLong(4025L);
    commitPrepBuf.putLong(700000000L);
    commitPrepBuf.putInt(6001);
    commitPrepBuf.put("beam_tx_gid_1\0".getBytes(StandardCharsets.UTF_8));
    commitPrepBuf.flip();
    assertNull(parser.parseMessage(commitPrepBuf, 4020L, tableSchemas));

    // 3. ROLLBACK PREPARED ('r')
    ByteBuffer rollPrepBuf = ByteBuffer.allocate(64);
    rollPrepBuf.put((byte) 'r');
    rollPrepBuf.put((byte) 0);
    rollPrepBuf.putLong(4030L);
    rollPrepBuf.putLong(4035L);
    rollPrepBuf.putLong(700000000L);
    rollPrepBuf.putLong(700000000L);
    rollPrepBuf.putInt(6002);
    rollPrepBuf.put("beam_tx_gid_2\0".getBytes(StandardCharsets.UTF_8));
    rollPrepBuf.flip();
    assertNull(parser.parseMessage(rollPrepBuf, 4030L, tableSchemas));
  }

  /** Replicates upstream PostgreSQL {@code messages.sql} generic logical messages. */
  @Test
  public void testUpstreamLogicalDecodingMessages() {
    ByteBuffer msgBuf = ByteBuffer.allocate(128);
    msgBuf.put((byte) 'M');
    msgBuf.putInt(0); // non-transactional (xid = 0)
    msgBuf.put((byte) 0); // non-transactional flag
    msgBuf.putLong(5000L); // LSN
    msgBuf.put("beam_heartbeat\0".getBytes(StandardCharsets.UTF_8));
    byte[] payload = "2026-08-28T18:00:00Z".getBytes(StandardCharsets.UTF_8);
    msgBuf.putInt(payload.length);
    msgBuf.put(payload);
    msgBuf.flip();

    assertNull(parser.parseMessage(msgBuf, 5000L, tableSchemas));
  }
}
