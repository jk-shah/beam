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
	"context"
	"errors"
	"fmt"
)

// SlotStatus is the health classification of a replication slot.
//
// The values mirror the health column of the beam_cdc_health view that
// PostgresProvisioningScript generates, so an operator reading SQL and an
// operator reading Go arrive at the same conclusion.
type SlotStatus string

const (
	// SlotStatusHealthy indicates the slot is active and reserving WAL normally.
	SlotStatusHealthy SlotStatus = "HEALTHY"
	// SlotStatusInactive indicates the slot exists but no client is consuming
	// it. It continues to retain WAL.
	SlotStatusInactive SlotStatus = "INACTIVE"
	// SlotStatusUnreserved indicates the slot has reserved no WAL.
	SlotStatusUnreserved SlotStatus = "UNRESERVED"
	// SlotStatusLost indicates the server has discarded WAL the slot required.
	// This is terminal: the slot cannot be resumed.
	SlotStatusLost SlotStatus = "LOST"
	// SlotStatusMissing indicates the slot does not exist.
	SlotStatusMissing SlotStatus = "MISSING"
)

// SlotHealthReport describes what a replication slot is currently costing the
// server.
type SlotHealthReport struct {
	SlotName string     `json:"slot_name"`
	Status   SlotStatus `json:"status"`

	// WALStatus is the server's own wal_status: reserved, extended,
	// unreserved or lost.
	WALStatus string `json:"wal_status"`

	// Active reports whether a client is currently consuming the slot.
	Active bool `json:"active"`

	// RetainedBytes is WAL held on this slot's behalf, measured from
	// restart_lsn. Zero when the slot reserves nothing or has been lost.
	RetainedBytes int64 `json:"retained_bytes"`

	// XminHorizonAge is transactions elapsed since the oldest catalog
	// transaction the slot forces the server to retain. A growing value means
	// VACUUM cannot clean up catalog tuples, which is a failure mode
	// independent of WAL volume.
	XminHorizonAge int64 `json:"xmin_horizon_age"`
}

// SlotHealth reports the current state of the configured replication slot.
//
// This is a package-level function rather than a method on the source, and
// deliberately so: a driver program cannot call methods on a DoFn that has been
// serialized and is executing on a worker. An operator wants this from the
// machine that submits the pipeline, or from an ad-hoc tool, so it opens its
// own short-lived connection and closes it before returning.
//
// It is the Go equivalent of querying the beam_cdc_health view and classifies
// identically. Prefer the view where SQL is available; prefer this where a Go
// program needs to gate on slot state.
func SlotHealth(ctx context.Context, opts CDCOptions) (SlotHealthReport, error) {
	querier, err := newSQLSlotQuerier(opts)
	if err != nil {
		return SlotHealthReport{}, err
	}
	defer querier.Close()

	return slotHealthFrom(ctx, querier, opts.SlotName)
}

// slotHealthFrom is the classification, separated from connection management so
// it can be driven by a fake querier in tests.
func slotHealthFrom(ctx context.Context, q slotRetentionQuerier, slotName string) (SlotHealthReport, error) {
	retention, err := q.QuerySlotRetention(ctx, slotName)
	if err != nil {
		if errors.Is(err, errSlotNotFound) {
			return SlotHealthReport{SlotName: slotName, Status: SlotStatusMissing}, nil
		}
		return SlotHealthReport{}, fmt.Errorf("postgresio: failed to read health for slot %q: %w", slotName, err)
	}

	report := SlotHealthReport{
		SlotName:       slotName,
		WALStatus:      retention.WALStatus,
		Active:         retention.Active,
		RetainedBytes:  retention.RetainedBytes,
		XminHorizonAge: retention.XminHorizonAge,
	}

	// Ordered exactly as the view's health column, and for the same reason:
	// invalidation also clears restart_lsn, so a lost slot presents as
	// unreserved. Classifying unreserved first would report the terminal state
	// as the benign one.
	switch {
	case retention.WALStatus == "lost":
		report.Status = SlotStatusLost
	case retention.RestartLSNUnset:
		report.Status = SlotStatusUnreserved
	case !retention.Active:
		report.Status = SlotStatusInactive
	default:
		report.Status = SlotStatusHealthy
	}
	return report, nil
}
