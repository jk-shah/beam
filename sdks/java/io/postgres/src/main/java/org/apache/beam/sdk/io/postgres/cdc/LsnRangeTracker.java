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

import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Thread-safe {@link RestrictionTracker} for unbounded and bounded PostgreSQL LSN ranges. */
public class LsnRangeTracker extends RestrictionTracker<LsnRange, Long>
    implements RestrictionTracker.HasProgress {

  private LsnRange restriction;
  private @Nullable Long lastClaimedLsn = null;

  public LsnRangeTracker(LsnRange restriction) {
    this.restriction = Preconditions.checkNotNull(restriction, "restriction cannot be null");
  }

  @Override
  public synchronized boolean tryClaim(Long position) {
    Preconditions.checkNotNull(position, "Position cannot be null");
    if (lastClaimedLsn != null && position < lastClaimedLsn) {
      throw new IllegalArgumentException(
          "Positions must be monotonically non-decreasing. Claimed: "
              + Long.toHexString(position)
              + ", last claimed: "
              + Long.toHexString(lastClaimedLsn));
    }
    if (position < restriction.getFromLsn()) {
      throw new IllegalArgumentException(
          "Position "
              + Long.toHexString(position)
              + " is before restriction start "
              + Long.toHexString(restriction.getFromLsn()));
    }
    if (position >= restriction.getToLsn()) {
      return false;
    }
    this.lastClaimedLsn = position;
    return true;
  }

  @Override
  public synchronized LsnRange currentRestriction() {
    return restriction;
  }

  @Override
  public synchronized @Nullable SplitResult<LsnRange> trySplit(double fractionOfRemainder) {
    if (lastClaimedLsn == null) {
      return null;
    }
    long current = lastClaimedLsn;
    long stop = restriction.getToLsn();

    // Checkpoint split for unbounded streaming
    if (stop == LsnRange.UNBOUNDED_STOP_LSN) {
      LsnRange primary = LsnRange.of(restriction.getFromLsn(), current + 1);
      LsnRange residual = LsnRange.startingFrom(current + 1);
      this.restriction = primary;
      return SplitResult.of(primary, residual);
    }

    long remainder = stop - (current + 1);
    if (remainder <= 0) {
      return null;
    }

    long splitPoint = current + 1 + Math.max(1L, (long) (remainder * fractionOfRemainder));
    if (splitPoint >= stop) {
      return null;
    }

    LsnRange primary = LsnRange.of(restriction.getFromLsn(), splitPoint);
    LsnRange residual = LsnRange.of(splitPoint, stop);
    this.restriction = primary;
    return SplitResult.of(primary, residual);
  }

  @Override
  public synchronized void checkDone() throws IllegalStateException {
    // Unbounded streaming DoFn is never done
    if (restriction.getToLsn() == LsnRange.UNBOUNDED_STOP_LSN) {
      return;
    }
    if (lastClaimedLsn == null || lastClaimedLsn < restriction.getToLsn() - 1) {
      throw new IllegalStateException(
          "RestrictionTracker not done: restriction="
              + restriction
              + ", lastClaimed="
              + (lastClaimedLsn != null ? Long.toHexString(lastClaimedLsn) : "none"));
    }
  }

  @Override
  public IsBounded isBounded() {
    return restriction.getToLsn() == LsnRange.UNBOUNDED_STOP_LSN
        ? IsBounded.UNBOUNDED
        : IsBounded.BOUNDED;
  }

  @Override
  public synchronized Progress getProgress() {
    long from = restriction.getFromLsn();
    long current = lastClaimedLsn != null ? lastClaimedLsn : from;
    double workRemaining =
        restriction.getToLsn() == LsnRange.UNBOUNDED_STOP_LSN
            ? Double.POSITIVE_INFINITY
            : (double) (restriction.getToLsn() - current);
    return Progress.from((double) (current - from), workRemaining);
  }
}
