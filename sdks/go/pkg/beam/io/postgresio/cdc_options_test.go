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
	"encoding/json"
	"fmt"
	"net"
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

func TestCDCOptionsOriginFilter(t *testing.T) {
	opts := NewCDCOptions(
		WithCDCSlotName("slot"),
		WithCDCPublication("pub"),
		WithCDCOriginFilter("none"),
	)
	if opts.OriginFilter != "none" {
		t.Errorf("expected origin filter 'none', got %q", opts.OriginFilter)
	}
}

func TestCDCOptionsOversizedTxnAndTimeout(t *testing.T) {
	opts := NewCDCOptions(
		WithCDCSlotName("slot"),
		WithCDCPublication("pub"),
		WithCDCMaxSpooledBytes(1024*1024),
		WithCDCOversizedTxnPolicy(OversizedTxnSkip),
		WithCDCTransactionStallTimeout(30*time.Second),
	)
	if opts.MaxSpooledBytes != 1024*1024 {
		t.Errorf("expected MaxSpooledBytes 1MB, got %d", opts.MaxSpooledBytes)
	}
	if opts.OversizedTxnPolicy != OversizedTxnSkip {
		t.Errorf("expected OversizedTxnSkip, got %v", opts.OversizedTxnPolicy)
	}
	if opts.TransactionStallTimeout != 30*time.Second {
		t.Errorf("expected TransactionStallTimeout 30s, got %v", opts.TransactionStallTimeout)
	}
}

func TestOptionsJSONSerializationPreservesPasswordForDistributedWorkers(t *testing.T) {
	cdcOpts := NewCDCOptions(
		WithCDCHost("localhost"),
		WithCDCSlotName("beam_slot"),
		WithCDCPublication("pub"),
		WithCDCPassword("worker_secret_key_123"),
	)
	data, err := json.Marshal(cdcOpts)
	if err != nil {
		t.Fatalf("failed to marshal CDCOptions: %v", err)
	}
	var roundTrip CDCOptions
	if err := json.Unmarshal(data, &roundTrip); err != nil {
		t.Fatalf("failed to unmarshal CDCOptions: %v", err)
	}
	if roundTrip.Password != "worker_secret_key_123" {
		t.Errorf("expected worker JSON deserialization to preserve password, got %q", roundTrip.Password)
	}
}

func TestCDCOptions_DialFuncMarkerSurvivesSerialization(t *testing.T) {
	dummyDial := func(ctx context.Context, network, addr string) (net.Conn, error) {
		return nil, fmt.Errorf("unused")
	}
	opts := NewCDCOptions(
		WithCDCHost("localhost"),
		WithCDCSlotName("beam_slot"),
		WithCDCPublication("pub"),
		WithCDCDialFunc(dummyDial),
	)
	if !opts.RequiresDialFunc {
		t.Error("WithCDCDialFunc did not set RequiresDialFunc")
	}

	data, err := json.Marshal(opts)
	if err != nil {
		t.Fatalf("failed to marshal CDCOptions: %v", err)
	}
	var roundTrip CDCOptions
	if err := json.Unmarshal(data, &roundTrip); err != nil {
		t.Fatalf("failed to unmarshal CDCOptions: %v", err)
	}
	if roundTrip.DialFunc != nil {
		t.Error("DialFunc closure should not have survived JSON serialization")
	}
	if !roundTrip.RequiresDialFunc {
		t.Error("RequiresDialFunc marker should have survived JSON serialization")
	}
}

func TestCDCOptions_TokenProviderMarkerSurvivesSerialization(t *testing.T) {
	dummyToken := &mockTokenProvider{token: "dynamic_token_123"}
	opts := NewCDCOptions(
		WithCDCHost("localhost"),
		WithCDCSlotName("beam_slot"),
		WithCDCPublication("pub"),
		WithCDCTokenProvider(dummyToken),
	)
	if !opts.RequiresTokenProvider {
		t.Error("WithCDCTokenProvider did not set RequiresTokenProvider for non-static provider")
	}

	data, err := json.Marshal(opts)
	if err != nil {
		t.Fatalf("failed to marshal CDCOptions: %v", err)
	}
	var roundTrip CDCOptions
	if err := json.Unmarshal(data, &roundTrip); err != nil {
		t.Fatalf("failed to unmarshal CDCOptions: %v", err)
	}
	if roundTrip.TokenProvider != nil {
		t.Error("TokenProvider interface should not have survived JSON serialization")
	}
	if !roundTrip.RequiresTokenProvider {
		t.Error("RequiresTokenProvider marker should have survived JSON serialization")
	}

	// StaticTokenProvider should not set RequiresTokenProvider because its password serializes
	staticToken := NewStaticTokenProvider("static_pass")
	optsStatic := NewCDCOptions(
		WithCDCSlotName("beam_slot"),
		WithCDCPublication("pub"),
		WithCDCTokenProvider(staticToken),
	)
	if optsStatic.RequiresTokenProvider {
		t.Error("StaticTokenProvider should not set RequiresTokenProvider")
	}
}

type mockTokenProvider struct {
	token string
}

func (m *mockTokenProvider) GetPassword(ctx context.Context) (string, error) {
	return m.token, nil
}

func TestCDCOptions_LostDialerAndTokenProviderErrors(t *testing.T) {
	ctx := context.Background()

	// 1. Lost dialer
	optsLostDial := CDCOptions{
		Host:             "localhost",
		Port:             5432,
		Database:         "testdb",
		SlotName:         "beam_slot",
		Publication:      "pub",
		Username:         "user",
		RequiresDialFunc: true,
		DialFunc:         nil,
	}

	_, err := NewNativeReplicationStream(ctx, optsLostDial)
	if err == nil || !strings.Contains(err.Error(), "DialFunc") {
		t.Errorf("expected errDialFuncLost from NewNativeReplicationStream, got %v", err)
	}

	_, err = newSQLPreflightQuerier(optsLostDial)
	if err == nil || !strings.Contains(err.Error(), "DialFunc") {
		t.Errorf("expected errDialFuncLost from newSQLPreflightQuerier, got %v", err)
	}

	_, err = newSQLSlotQuerier(optsLostDial)
	if err == nil || !strings.Contains(err.Error(), "DialFunc") {
		t.Errorf("expected errDialFuncLost from newSQLSlotQuerier, got %v", err)
	}

	// 2. Lost token provider
	optsLostToken := CDCOptions{
		Host:                  "localhost",
		Port:                  5432,
		Database:              "testdb",
		SlotName:              "beam_slot",
		Publication:           "pub",
		Username:              "user",
		RequiresTokenProvider: true,
		TokenProvider:         nil,
	}

	_, err = NewNativeReplicationStream(ctx, optsLostToken)
	if err == nil || !strings.Contains(err.Error(), "TokenProvider") {
		t.Errorf("expected errTokenProviderLost from NewNativeReplicationStream, got %v", err)
	}

	_, err = newSQLPreflightQuerier(optsLostToken)
	if err == nil || !strings.Contains(err.Error(), "TokenProvider") {
		t.Errorf("expected errTokenProviderLost from newSQLPreflightQuerier, got %v", err)
	}

	_, err = newSQLSlotQuerier(optsLostToken)
	if err == nil || !strings.Contains(err.Error(), "TokenProvider") {
		t.Errorf("expected errTokenProviderLost from newSQLSlotQuerier, got %v", err)
	}
}
