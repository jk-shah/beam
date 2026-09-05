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
import java.util.Objects;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;

/** Restriction representing a contiguous range of PostgreSQL Log Sequence Numbers (LSNs). */
public class LsnRange implements Serializable, HasDefaultTracker<LsnRange, LsnRangeTracker> {

  public static final long UNBOUNDED_STOP_LSN = Long.MAX_VALUE;

  private final long fromLsn;
  private final long toLsn;

  public LsnRange(long fromLsn, long toLsn) {
    this.fromLsn = fromLsn;
    this.toLsn = toLsn;
  }

  public static LsnRange of(long fromLsn, long toLsn) {
    return new LsnRange(fromLsn, toLsn);
  }

  public static LsnRange startingFrom(long fromLsn) {
    return new LsnRange(fromLsn, UNBOUNDED_STOP_LSN);
  }

  public long getFromLsn() {
    return fromLsn;
  }

  public long getToLsn() {
    return toLsn;
  }

  @Override
  public LsnRangeTracker newTracker() {
    return new LsnRangeTracker(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof LsnRange)) return false;
    LsnRange lsnRange = (LsnRange) o;
    return fromLsn == lsnRange.fromLsn && toLsn == lsnRange.toLsn;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fromLsn, toLsn);
  }

  @Override
  public String toString() {
    return "LsnRange["
        + Long.toHexString(fromLsn)
        + " -> "
        + (toLsn == UNBOUNDED_STOP_LSN ? "UNBOUNDED" : Long.toHexString(toLsn))
        + "]";
  }
}
