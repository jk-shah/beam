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
import java.util.Collections;
import java.util.Set;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Represents a Change Data Capture (CDC) event emitted from PostgreSQL logical decoding or initial
 * table snapshot.
 */
@DefaultCoder(SerializableCoder.class)
public class ChangeEvent<T> implements Serializable {

  public enum OpType {
    READ,
    INSERT,
    UPDATE,
    DELETE,
    TRUNCATE
  }

  private final OpType opType;
  private final String schemaName;
  private final String tableName;
  private final long lsn;
  private final long transactionId;
  private final Instant commitTimestamp;
  private final @Nullable T before;
  private final @Nullable T after;
  private final Set<String> unchangedToastColumns;

  public ChangeEvent(
      OpType opType,
      String schemaName,
      String tableName,
      long lsn,
      long transactionId,
      Instant commitTimestamp,
      @Nullable T before,
      @Nullable T after,
      @Nullable Set<String> unchangedToastColumns) {
    this.opType = opType;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.lsn = lsn;
    this.transactionId = transactionId;
    this.commitTimestamp = commitTimestamp;
    this.before = before;
    this.after = after;
    this.unchangedToastColumns =
        unchangedToastColumns != null ? unchangedToastColumns : Collections.emptySet();
  }

  public OpType getOpType() {
    return opType;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  public long getLsn() {
    return lsn;
  }

  public long getTransactionId() {
    return transactionId;
  }

  public Instant getCommitTimestamp() {
    return commitTimestamp;
  }

  public @Nullable T getBefore() {
    return before;
  }

  public @Nullable T getAfter() {
    return after;
  }

  public Set<String> getUnchangedToastColumns() {
    return unchangedToastColumns;
  }

  private static boolean rowEquals(@Nullable Object o1, @Nullable Object o2) {
    if (o1 == o2) {
      return true;
    }
    if (o1 == null || o2 == null) {
      return false;
    }
    if (o1 instanceof org.apache.beam.sdk.values.Row
        && o2 instanceof org.apache.beam.sdk.values.Row) {
      org.apache.beam.sdk.values.Row r1 = (org.apache.beam.sdk.values.Row) o1;
      org.apache.beam.sdk.values.Row r2 = (org.apache.beam.sdk.values.Row) o2;
      return java.util.Objects.equals(r1.getValues(), r2.getValues())
          && java.util.Objects.equals(r1.getSchema().getFields(), r2.getSchema().getFields());
    }
    return java.util.Objects.equals(o1, o2);
  }

  private static int rowHashCode(@Nullable Object o) {
    if (o == null) {
      return 0;
    }
    if (o instanceof org.apache.beam.sdk.values.Row) {
      return java.util.Objects.hash(((org.apache.beam.sdk.values.Row) o).getValues());
    }
    return o.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ChangeEvent)) return false;
    ChangeEvent<?> that = (ChangeEvent<?>) o;
    return lsn == that.lsn
        && transactionId == that.transactionId
        && opType == that.opType
        && java.util.Objects.equals(schemaName, that.schemaName)
        && java.util.Objects.equals(tableName, that.tableName)
        && java.util.Objects.equals(commitTimestamp, that.commitTimestamp)
        && rowEquals(before, that.before)
        && rowEquals(after, that.after)
        && java.util.Objects.equals(unchangedToastColumns, that.unchangedToastColumns);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(
        opType,
        schemaName,
        tableName,
        lsn,
        transactionId,
        commitTimestamp,
        rowHashCode(before),
        rowHashCode(after),
        unchangedToastColumns);
  }

  @Override
  public String toString() {
    return "ChangeEvent{"
        + "opType="
        + opType
        + ", table='"
        + schemaName
        + "."
        + tableName
        + '\''
        + ", lsn="
        + Long.toHexString(lsn)
        + ", txId="
        + transactionId
        + ", timestamp="
        + commitTimestamp
        + '}';
  }
}
