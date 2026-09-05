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
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link LsnRangeTracker}. */
@RunWith(JUnit4.class)
public class LsnRangeTrackerTest {

  @Test
  public void testClaimMonotonicPositions() {
    LsnRange range = LsnRange.startingFrom(100L);
    LsnRangeTracker tracker = new LsnRangeTracker(range);

    assertTrue(tracker.tryClaim(100L));
    assertTrue(tracker.tryClaim(105L));
    assertTrue(tracker.tryClaim(110L));
  }

  @Test
  public void testUnboundedSplitCheckpoint() {
    LsnRange range = LsnRange.startingFrom(100L);
    LsnRangeTracker tracker = new LsnRangeTracker(range);

    tracker.tryClaim(150L);
    SplitResult<LsnRange> split = tracker.trySplit(0.5);

    assertNotNull(split);
    assertEquals(100L, split.getPrimary().getFromLsn());
    assertEquals(151L, split.getPrimary().getToLsn());
    assertEquals(151L, split.getResidual().getFromLsn());
    assertEquals(LsnRange.UNBOUNDED_STOP_LSN, split.getResidual().getToLsn());
  }
}
