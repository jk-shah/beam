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

import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Encapsulates a failed record rejected during PostgreSQL write execution, routed to the
 * Dead-Letter Queue (DLQ) with sanitized diagnostic error information.
 */
public class PostgreSqlWriteError implements Serializable {

  private final Row failedRow;
  private final String sqlState;
  private final String errorMessage;
  private final String destinationTable;
  private final Instant timestamp;

  public PostgreSqlWriteError(
      Row failedRow,
      @Nullable String sqlState,
      String errorMessage,
      String destinationTable,
      Instant timestamp) {
    this.failedRow = failedRow;
    this.sqlState = sqlState != null ? sqlState : "UNKNOWN";
    this.errorMessage = errorMessage;
    this.destinationTable = destinationTable;
    this.timestamp = timestamp;
  }

  public Row getFailedRow() {
    return failedRow;
  }

  public String getSqlState() {
    return sqlState;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public String getDestinationTable() {
    return destinationTable;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PostgreSqlWriteError)) return false;
    PostgreSqlWriteError that = (PostgreSqlWriteError) o;
    return Objects.equals(failedRow, that.failedRow)
        && Objects.equals(sqlState, that.sqlState)
        && Objects.equals(destinationTable, that.destinationTable);
  }

  @Override
  public int hashCode() {
    return Objects.hash(failedRow, sqlState, destinationTable);
  }

  @Override
  public String toString() {
    return "PostgreSqlWriteError{"
        + "table='"
        + destinationTable
        + '\''
        + ", sqlState='"
        + sqlState
        + '\''
        + ", error='"
        + errorMessage
        + '\''
        + ", row="
        + failedRow
        + '}';
  }
}
