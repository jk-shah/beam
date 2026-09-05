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

/**
 * Exception thrown when PostgreSQL Write-Ahead Log (WAL) accumulation exceeds the configured safety
 * threshold (e.g. {@code maxWalRetainedBytes}), tripping the circuit breaker to prevent primary
 * database storage exhaustion outages.
 */
public class PostgreSqlWalExhaustionException extends RuntimeException {

  private final String slotName;
  private final long currentWalRetainedBytes;
  private final long maxWalRetainedBytes;
  private final long serverWalEndLsn;
  private final long confirmedFlushLsn;

  public PostgreSqlWalExhaustionException(
      String slotName,
      long currentWalRetainedBytes,
      long maxWalRetainedBytes,
      long serverWalEndLsn,
      long confirmedFlushLsn) {
    super(
        String.format(
            "PostgreSQL replication slot '%s' has accumulated %d bytes of unacknowledged WAL, "
                + "exceeding the safety threshold of %d bytes (Server LSN: %s, Confirmed Flush LSN: %s). "
                + "Tripping circuit breaker to prevent primary database disk exhaustion. "
                + "Remediation: To drop the stalled slot on the database, execute: "
                + "SELECT pg_drop_replication_slot('%s');",
            slotName,
            currentWalRetainedBytes,
            maxWalRetainedBytes,
            Long.toHexString(serverWalEndLsn),
            Long.toHexString(confirmedFlushLsn),
            slotName));
    this.slotName = slotName;
    this.currentWalRetainedBytes = currentWalRetainedBytes;
    this.maxWalRetainedBytes = maxWalRetainedBytes;
    this.serverWalEndLsn = serverWalEndLsn;
    this.confirmedFlushLsn = confirmedFlushLsn;
  }

  public String getSlotName() {
    return slotName;
  }

  public long getCurrentWalRetainedBytes() {
    return currentWalRetainedBytes;
  }

  public long getMaxWalRetainedBytes() {
    return maxWalRetainedBytes;
  }

  public long getServerWalEndLsn() {
    return serverWalEndLsn;
  }

  public long getConfirmedFlushLsn() {
    return confirmedFlushLsn;
  }
}
