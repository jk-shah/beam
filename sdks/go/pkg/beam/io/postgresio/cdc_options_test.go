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
	"strings"
	"testing"
	"time"
)

func TestCDCOptionsDefaults(t *testing.T) {
	opts := NewCDCOptions(
		WithCDCSlotName("beam_slot"),
		WithCDCPublication("beam_pub"),
	)
	if opts.Port != 5432 {
		t.Errorf("expected default port 5432, got %d", opts.Port)
	}
	if opts.HeartbeatInterval != 10*time.Second {
		t.Errorf("expected default heartbeat 10s, got %v", opts.HeartbeatInterval)
	}
	if err := opts.Validate(); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestCDCOptionsSlotValidation(t *testing.T) {
	validSlots := []string{
		"beam_cdc_slot",
		"slot123",
		"test_replication_slot_01",
		"a",
		strings.Repeat("a", 63),
	}
	for _, s := range validSlots {
		opts := NewCDCOptions(
			WithCDCSlotName(s),
			WithCDCPublication("pub"),
		)
		if err := opts.Validate(); err != nil {
			t.Errorf("expected valid slot %q to pass validation, got: %v", s, err)
		}
	}

	invalidSlots := []string{
		"",
		"slot-with-hyphens",
		"slot.with.dots",
		"slot with spaces",
		"slot; DROP TABLE users;--",
		strings.Repeat("a", 64), // exceeds 63 characters
		"SlotWithUppercase",     // Postgres requires lowercase for replication slots
		"slot/path",
		"slot$name",
	}
	for _, s := range invalidSlots {
		opts := NewCDCOptions(
			WithCDCSlotName(s),
			WithCDCPublication("pub"),
		)
		if err := opts.Validate(); err == nil {
			t.Errorf("expected invalid slot %q to fail validation, but it passed", s)
		}
	}
}

func TestCDCOptionsHeartbeatIntervalGuard(t *testing.T) {
	opts := NewCDCOptions(
		WithCDCSlotName("slot"),
		WithCDCPublication("pub"),
		WithCDCHeartbeatInterval(70*time.Second), // Exceeds default wal_sender_timeout 60s
	)
	if err := opts.Validate(); err == nil {
		t.Errorf("expected error when heartbeat interval exceeds 60s, got nil")
	}

	optsZero := NewCDCOptions(
		WithCDCSlotName("slot"),
		WithCDCPublication("pub"),
		WithCDCHeartbeatInterval(0),
	)
	if err := optsZero.Validate(); err == nil {
		t.Errorf("expected error when heartbeat interval is zero, got nil")
	}
}

func TestCDCOptionsPasswordRedaction(t *testing.T) {
	opts := NewCDCOptions(
		WithCDCHost("db.example.com"),
		WithCDCSlotName("beam_slot"),
		WithCDCPublication("pub"),
		WithCDCPassword("super_secret_db_password"),
	)
	str := opts.String()
	if strings.Contains(str, "super_secret_db_password") {
		t.Fatalf("options string leaked raw password: %s", str)
	}
	if !strings.Contains(str, "<redacted>") {
		t.Fatalf("options string missing <redacted> token: %s", str)
	}
}
