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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.postgres.cdc.ChangeEvent.OpType;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** High-performance binary parser for PostgreSQL {@code pgoutput} logical replication protocol. */
public class PgOutputParser implements Serializable {

  public static class RelationMetadata implements Serializable {
    public final int relationId;
    public final String schemaName;
    public final String tableName;
    public final char replicaIdentity;
    public final List<String> columnNames;
    public final List<Integer> columnTypeOids;

    public RelationMetadata(
        int relationId,
        String schemaName,
        String tableName,
        char replicaIdentity,
        List<String> columnNames,
        List<Integer> columnTypeOids) {
      this.relationId = relationId;
      this.schemaName = schemaName;
      this.tableName = tableName;
      this.replicaIdentity = replicaIdentity;
      this.columnNames = columnNames;
      this.columnTypeOids = columnTypeOids;
    }
  }

  private final Map<Integer, RelationMetadata> relationMap = new HashMap<>();
  private long currentTransactionId = 0L;
  private Instant currentCommitTimestamp = Instant.now();

  /** Decodes a raw {@code pgoutput} replication buffer into a structured {@link ChangeEvent}. */
  public @Nullable ChangeEvent<Row> parseMessage(
      ByteBuffer buffer, long lsn, Map<String, Schema> tableSchemas) {
    if (!buffer.hasRemaining()) {
      return null;
    }

    char messageType = (char) buffer.get();
    switch (messageType) {
      case 'B': // BEGIN
        buffer.getLong(); // final LSN
        long commitTimeMicros = buffer.getLong(); // commit time in microseconds since 2000-01-01
        currentCommitTimestamp = pgTimeToInstant(commitTimeMicros);
        currentTransactionId = buffer.getInt() & 0xFFFFFFFFL;
        return null;

      case 'C': // COMMIT
        buffer.get(); // flags
        buffer.getLong(); // commit LSN
        buffer.getLong(); // end LSN
        buffer.getLong(); // commit time
        return null;

      case 'R': // RELATION
        parseRelation(buffer);
        return null;

      case 'I': // INSERT
        return parseInsert(buffer, lsn, tableSchemas);

      case 'U': // UPDATE
        return parseUpdate(buffer, lsn, tableSchemas);

      case 'D': // DELETE
        return parseDelete(buffer, lsn, tableSchemas);

      case 'T': // TRUNCATE
        return parseTruncate(buffer, lsn);

        // Protocol v2 Streaming Messages
      case 'S': // STREAM START
        currentTransactionId = buffer.getInt() & 0xFFFFFFFFL;
        buffer.get(); // first segment flag
        return null;

      case 'E': // STREAM STOP
        return null;

      case 'c': // STREAM COMMIT
        currentTransactionId = buffer.getInt() & 0xFFFFFFFFL;
        buffer.get(); // flags
        buffer.getLong(); // commit LSN
        buffer.getLong(); // end LSN
        return null;

      case 'A': // STREAM ABORT
        currentTransactionId = buffer.getInt() & 0xFFFFFFFFL;
        buffer.getInt(); // subxid
        return null;

      case 'M': // LOGICAL DECODING MESSAGE (pg_logical_emit_message)
        currentTransactionId = buffer.getInt() & 0xFFFFFFFFL;
        buffer.get(); // flags (0 = non-transactional, 1 = transactional)
        buffer.getLong(); // message LSN
        readNullTerminatedString(buffer); // prefix
        int contentLen = buffer.getInt();
        if (contentLen > 0 && buffer.remaining() >= contentLen) {
          byte[] contentBytes = new byte[contentLen];
          buffer.get(contentBytes);
        }
        return null;

      case 'P': // PREPARE TRANSACTION
        buffer.getLong(); // prepare LSN
        buffer.getLong(); // prepare end LSN
        buffer.getLong(); // prepare commit time
        currentTransactionId = buffer.getInt() & 0xFFFFFFFFL;
        readNullTerminatedString(buffer); // GID
        return null;

      case 'K': // COMMIT PREPARED
        buffer.get(); // flags
        buffer.getLong(); // commit LSN
        buffer.getLong(); // end LSN
        buffer.getLong(); // commit time
        currentTransactionId = buffer.getInt() & 0xFFFFFFFFL;
        readNullTerminatedString(buffer); // GID
        return null;

      case 'r': // ROLLBACK PREPARED
        buffer.get(); // flags
        buffer.getLong(); // prepare end LSN
        buffer.getLong(); // rollback end LSN
        buffer.getLong(); // prepare time
        buffer.getLong(); // rollback time
        currentTransactionId = buffer.getInt() & 0xFFFFFFFFL;
        readNullTerminatedString(buffer); // GID
        return null;

      default:
        // Ignore unrecognized extension tags
        return null;
    }
  }

  private void parseRelation(ByteBuffer buffer) {
    int relId = buffer.getInt();
    String schemaName = readNullTerminatedString(buffer);
    String tableName = readNullTerminatedString(buffer);
    char replicaIdentity = (char) buffer.get();
    short numColumns = buffer.getShort();

    List<String> colNames = new ArrayList<>(numColumns);
    List<Integer> colTypes = new ArrayList<>(numColumns);

    for (int i = 0; i < numColumns; i++) {
      buffer.get(); // flags (1 = key column)
      colNames.add(readNullTerminatedString(buffer));
      colTypes.add(buffer.getInt()); // type OID
      buffer.getInt(); // type modifier
    }

    relationMap.put(
        relId,
        new RelationMetadata(relId, schemaName, tableName, replicaIdentity, colNames, colTypes));
  }

  private @Nullable ChangeEvent<Row> parseInsert(
      ByteBuffer buffer, long lsn, Map<String, Schema> tableSchemas) {
    int relId = buffer.getInt();
    RelationMetadata rel = relationMap.get(relId);
    if (rel == null) {
      return null;
    }

    buffer.get(); // skip 'N' tuple type indicator
    String fullTableName = rel.schemaName + "." + rel.tableName;
    Schema schema = tableSchemas.get(fullTableName);

    Set<String> unchangedToast = new HashSet<>();
    Row afterRow = parseTuple(buffer, rel, schema, unchangedToast);

    return new ChangeEvent<>(
        OpType.INSERT,
        rel.schemaName,
        rel.tableName,
        lsn,
        currentTransactionId,
        currentCommitTimestamp,
        null,
        afterRow,
        unchangedToast);
  }

  private @Nullable ChangeEvent<Row> parseUpdate(
      ByteBuffer buffer, long lsn, Map<String, Schema> tableSchemas) {
    int relId = buffer.getInt();
    RelationMetadata rel = relationMap.get(relId);
    if (rel == null) {
      return null;
    }

    char nextByte = (char) buffer.get();
    Row beforeRow = null;
    String fullTableName = rel.schemaName + "." + rel.tableName;
    Schema schema = tableSchemas.get(fullTableName);

    if (nextByte == 'K' || nextByte == 'O') {
      Set<String> dummy = new HashSet<>();
      beforeRow = parseTuple(buffer, rel, schema, dummy);
      nextByte = (char) buffer.get(); // 'N'
    }

    Set<String> unchangedToast = new HashSet<>();
    Row afterRow = parseTuple(buffer, rel, schema, unchangedToast);

    return new ChangeEvent<>(
        OpType.UPDATE,
        rel.schemaName,
        rel.tableName,
        lsn,
        currentTransactionId,
        currentCommitTimestamp,
        beforeRow,
        afterRow,
        unchangedToast);
  }

  private @Nullable ChangeEvent<Row> parseDelete(
      ByteBuffer buffer, long lsn, Map<String, Schema> tableSchemas) {
    int relId = buffer.getInt();
    RelationMetadata rel = relationMap.get(relId);
    if (rel == null) {
      return null;
    }

    buffer.get(); // 'K' or 'O'
    String fullTableName = rel.schemaName + "." + rel.tableName;
    Schema schema = tableSchemas.get(fullTableName);

    Set<String> dummy = new HashSet<>();
    Row beforeRow = parseTuple(buffer, rel, schema, dummy);

    return new ChangeEvent<>(
        OpType.DELETE,
        rel.schemaName,
        rel.tableName,
        lsn,
        currentTransactionId,
        currentCommitTimestamp,
        beforeRow,
        null,
        null);
  }

  private ChangeEvent<Row> parseTruncate(ByteBuffer buffer, long lsn) {
    int numRels = buffer.getInt();
    buffer.get(); // options flags
    int firstRelId = numRels > 0 ? buffer.getInt() : 0;
    RelationMetadata rel = relationMap.get(firstRelId);
    String schemaName = rel != null ? rel.schemaName : "public";
    String tableName = rel != null ? rel.tableName : "*";

    return new ChangeEvent<>(
        OpType.TRUNCATE,
        schemaName,
        tableName,
        lsn,
        currentTransactionId,
        currentCommitTimestamp,
        null,
        null,
        null);
  }

  private @Nullable Row parseTuple(
      ByteBuffer buffer,
      RelationMetadata rel,
      @Nullable Schema schema,
      Set<String> unchangedToastOut) {
    short numCols = buffer.getShort();
    List<Object> values = new ArrayList<>(numCols);

    for (int i = 0; i < numCols; i++) {
      char colType = (char) buffer.get();
      String colName = i < rel.columnNames.size() ? rel.columnNames.get(i) : ("col_" + i);

      switch (colType) {
        case 'n': // NULL
          values.add(null);
          break;
        case 'u': // Unchanged TOAST
          unchangedToastOut.add(colName);
          values.add(null);
          break;
        case 't': // Text formatted value
          int len = buffer.getInt();
          byte[] textBytes = new byte[len];
          buffer.get(textBytes);
          String strVal = new String(textBytes, StandardCharsets.UTF_8);
          FieldType fieldType =
              schema != null && i < schema.getFieldCount()
                  ? schema.getField(i).getType()
                  : FieldType.STRING;
          values.add(convertStringToTypedValue(strVal, fieldType));
          break;
        case 'b': // Binary formatted value
          int bLen = buffer.getInt();
          byte[] binBytes = new byte[bLen];
          buffer.get(binBytes);
          values.add(binBytes);
          break;
        default:
          values.add(null);
      }
    }

    if (schema == null) {
      Schema.Builder dynamicSchema = Schema.builder();
      for (int i = 0; i < rel.columnNames.size(); i++) {
        dynamicSchema.addNullableField(rel.columnNames.get(i), FieldType.STRING);
      }
      schema = dynamicSchema.build();
    }

    return Row.withSchema(schema).addValues(values).build();
  }

  private Object convertStringToTypedValue(String str, FieldType type) {
    if (str == null) return null;
    switch (type.getTypeName()) {
      case INT16:
        return Short.parseShort(str);
      case INT32:
        return Integer.parseInt(str);
      case INT64:
        return Long.parseLong(str);
      case FLOAT:
        return Float.parseFloat(str);
      case DOUBLE:
        return Double.parseDouble(str);
      case DECIMAL:
        return new java.math.BigDecimal(str);
      case BOOLEAN:
        return "t".equalsIgnoreCase(str) || "true".equalsIgnoreCase(str);
      case DATETIME:
        return Instant.parse(str);
      case STRING:
      default:
        return str;
    }
  }

  private String readNullTerminatedString(ByteBuffer buffer) {
    StringBuilder sb = new StringBuilder();
    byte b;
    while (buffer.hasRemaining() && (b = buffer.get()) != 0) {
      sb.append((char) b);
    }
    return sb.toString();
  }

  private Instant pgTimeToInstant(long microsSince2000) {
    // 2000-01-01 00:00:00 UTC = 946684800000 ms
    long epochMillis = 946684800000L + (microsSince2000 / 1000L);
    return new Instant(epochMillis);
  }
}
