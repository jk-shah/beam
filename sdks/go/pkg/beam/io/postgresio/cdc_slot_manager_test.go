// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package postgresio

import (
	"testing"
)

func TestSlotHealthReportPropertiesAndEquality(t *testing.T) {
	report1 := SlotHealthReport{
		SlotName:          "beam_slot",
		Status:            SlotStatusHealthy,
		RestartLSN:        "0/1A00000",
		ConfirmedFlushLSN: "0/1A00100",
		WalStatus:         "normal",
		Active:            true,
		ActivePID:         12345,
	}

	report2 := SlotHealthReport{
		SlotName:          "beam_slot",
		Status:            SlotStatusHealthy,
		RestartLSN:        "0/1A00000",
		ConfirmedFlushLSN: "0/1A00100",
		WalStatus:         "normal",
		Active:            true,
		ActivePID:         12345,
	}

	if report1 != report2 {
		t.Errorf("expected report1 to equal report2: %+v vs %+v", report1, report2)
	}
	if report1.SlotName != "beam_slot" || report1.Status != SlotStatusHealthy || !report1.Active {
		t.Errorf("unexpected field values: %+v", report1)
	}
}

func TestSlotRepairResultProperties(t *testing.T) {
	result := SlotRepairResult{
		Repaired:         true,
		SlotName:         "beam_cdc_slot",
		NewConsistentLSN: "0/2B00000",
		PolicyApplied:    SlotRepairAutomaticRecreateAndCatchup,
		Message:          "Slot recreated successfully",
	}

	if !result.Repaired || result.SlotName != "beam_cdc_slot" || result.PolicyApplied != SlotRepairAutomaticRecreateAndCatchup {
		t.Errorf("unexpected repair result: %+v", result)
	}
}

func TestSlotRepairPolicyValues(t *testing.T) {
	policies := []SlotRepairPolicy{
		SlotRepairFailFast,
		SlotRepairAutomaticRecreateAndCatchup,
		SlotRepairRecreateSlotLiveOnly,
	}
	if len(policies) != 3 {
		t.Errorf("expected 3 repair policies, got %d", len(policies))
	}
}

func TestSlotRepairManagerValidation(t *testing.T) {
	mgr, err := NewSlotRepairManager("beam_slot_1", "pgoutput", SlotRepairAutomaticRecreateAndCatchup)
	if err != nil || mgr == nil {
		t.Fatalf("expected valid manager, got err: %v", err)
	}

	// SQL injection attempts in slot name must be rejected
	_, err = NewSlotRepairManager("slot_name; DROP TABLE users;", "pgoutput", SlotRepairAutomaticRecreateAndCatchup)
	if err == nil {
		t.Fatalf("expected error on invalid slot name with SQL injection, but got nil")
	}

	_, err = NewSlotRepairManager("", "pgoutput", SlotRepairAutomaticRecreateAndCatchup)
	if err == nil {
		t.Fatalf("expected error on empty slot name, but got nil")
	}
}
