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
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.io.postgres.PostgreSqlDataSourceConfiguration;
import org.apache.beam.sdk.io.postgres.cdc.PostgreSqlSlotRepairManager.SlotHealthReport;
import org.apache.beam.sdk.io.postgres.cdc.PostgreSqlSlotRepairManager.SlotRepairResult;
import org.apache.beam.sdk.io.postgres.cdc.PostgreSqlSlotRepairManager.SlotStatus;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PostgreSqlSlotRepairManager} and {@link SlotRepairPolicy}. */
@RunWith(JUnit4.class)
public class PostgreSqlSlotRepairManagerTest {

  @Test
  public void testSlotHealthReportPropertiesAndEquality() {
    SlotHealthReport report1 =
        new SlotHealthReport(
            "beam_slot", SlotStatus.HEALTHY, "0/1A00000", "0/1A00100", "normal", true, 12345L);

    SlotHealthReport report2 =
        new SlotHealthReport(
            "beam_slot", SlotStatus.HEALTHY, "0/1A00000", "0/1A00100", "normal", true, 12345L);

    assertEquals("beam_slot", report1.getSlotName());
    assertEquals(SlotStatus.HEALTHY, report1.getStatus());
    assertEquals("0/1A00000", report1.getRestartLsn());
    assertEquals("0/1A00100", report1.getConfirmedFlushLsn());
    assertEquals("normal", report1.getWalStatus());
    assertTrue(report1.isActive());
    assertEquals(Long.valueOf(12345L), report1.getActivePid());

    assertEquals(report1, report2);
    assertEquals(report1.hashCode(), report2.hashCode());
  }

  @Test
  public void testSlotRepairResultProperties() {
    SlotRepairResult result =
        new SlotRepairResult(
            true,
            "beam_cdc_slot",
            "0/2B00000",
            SlotRepairPolicy.AUTOMATIC_RECREATE_AND_CATCHUP,
            "Slot recreated successfully");

    assertTrue(result.isRepaired());
    assertEquals("beam_cdc_slot", result.getSlotName());
    assertEquals("0/2B00000", result.getNewConsistentLsn());
    assertEquals(SlotRepairPolicy.AUTOMATIC_RECREATE_AND_CATCHUP, result.getPolicyApplied());
    assertEquals("Slot recreated successfully", result.getMessage());
  }

  @Test
  public void testSlotRepairPolicyValues() {
    assertEquals(3, SlotRepairPolicy.values().length);
    assertEquals(SlotRepairPolicy.FAIL_FAST, SlotRepairPolicy.valueOf("FAIL_FAST"));
    assertEquals(
        SlotRepairPolicy.AUTOMATIC_RECREATE_AND_CATCHUP,
        SlotRepairPolicy.valueOf("AUTOMATIC_RECREATE_AND_CATCHUP"));
    assertEquals(
        SlotRepairPolicy.RECREATE_SLOT_LIVE_ONLY,
        SlotRepairPolicy.valueOf("RECREATE_SLOT_LIVE_ONLY"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSlotNameRejectsInjection() {
    PostgreSqlDataSourceConfiguration config =
        PostgreSqlDataSourceConfiguration.create("jdbc:postgresql://localhost:5432/db")
            .withUsername("user")
            .withPassword("pass");
    new PostgreSqlSlotRepairManager(
        config,
        "slot_name; DROP TABLE users;",
        "pgoutput",
        SlotRepairPolicy.AUTOMATIC_RECREATE_AND_CATCHUP);
  }
}
