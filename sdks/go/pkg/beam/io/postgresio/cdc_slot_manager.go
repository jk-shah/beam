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
	"fmt"
)

// SlotStatus represents the runtime health status of a PostgreSQL replication slot.
type SlotStatus string

const (
	// SlotStatusHealthy indicates normal replication progress.
	SlotStatusHealthy SlotStatus = "HEALTHY"
	// SlotStatusLagging indicates WAL retention lag exceeding threshold.
	SlotStatusLagging SlotStatus = "LAGGING"
	// SlotStatusInactive indicates slot is not actively consumed by any client.
	SlotStatusInactive SlotStatus = "INACTIVE"
	// SlotStatusMissing indicates the slot does not exist on the database.
	SlotStatusMissing SlotStatus = "MISSING"
)

// SlotRepairPolicy defines recovery behavior when an invalid or dropped slot is encountered.
type SlotRepairPolicy string

const (
	// SlotRepairFailFast halts execution immediately.
	SlotRepairFailFast SlotRepairPolicy = "FAIL_FAST"
	// SlotRepairAutomaticRecreateAndCatchup recreates slot and catches up from available WAL.
	SlotRepairAutomaticRecreateAndCatchup SlotRepairPolicy = "AUTOMATIC_RECREATE_AND_CATCHUP"
	// SlotRepairRecreateSlotLiveOnly recreates slot from current live position without historical replay.
	SlotRepairRecreateSlotLiveOnly SlotRepairPolicy = "RECREATE_SLOT_LIVE_ONLY"
)

// SlotHealthReport describes the telemetry and state of a replication slot.
type SlotHealthReport struct {
	SlotName          string     `json:"slot_name"`
	Status            SlotStatus `json:"status"`
	RestartLSN        string     `json:"restart_lsn"`
	ConfirmedFlushLSN string     `json:"confirmed_flush_lsn"`
	WalStatus         string     `json:"wal_status"`
	Active            bool       `json:"active"`
	ActivePID         int64      `json:"active_pid"`
}

// SlotRepairResult captures the outcome of a repair operation.
type SlotRepairResult struct {
	Repaired           bool             `json:"repaired"`
	SlotName           string           `json:"slot_name"`
	NewConsistentLSN   string           `json:"new_consistent_lsn"`
	PolicyApplied      SlotRepairPolicy `json:"policy_applied"`
	Message            string           `json:"message"`
}

// SlotRepairManager coordinates replication slot inspection and automated remediation.
type SlotRepairManager struct {
	SlotName     string
	Plugin       string
	RepairPolicy SlotRepairPolicy
}

// NewSlotRepairManager creates a validated SlotRepairManager.
func NewSlotRepairManager(slotName, plugin string, policy SlotRepairPolicy) (*SlotRepairManager, error) {
	if slotName == "" {
		return nil, fmt.Errorf("slot name must not be empty")
	}
	if !validSlotRegex.MatchString(slotName) {
		return nil, fmt.Errorf("invalid replication slot name %q: must match ^[a-z0-9_]{1,63}$", slotName)
	}
	if plugin == "" {
		plugin = "pgoutput"
	}
	return &SlotRepairManager{
		SlotName:     slotName,
		Plugin:       plugin,
		RepairPolicy: policy,
	}, nil
}
