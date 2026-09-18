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

package main

import (
	"context"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestMain(m *testing.M) {
	ptest.Main(m)
}

func TestMaskSSNZeroAlloc(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"standard hyphenated", "123-45-6789", "***-**-6789"},
		{"unhyphenated 9 digits", "123456789", "***-**-6789"},
		{"short string", "12345", "***-**-****"},
		{"empty string", "", "***-**-****"},
		{"non-digit characters", "123-4A-6789", "***-**-****"},
		{"invalid separators", "123.45.6789", "***-**-****"},
		{"trailing characters", "123-45-67890", "***-**-****"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := MaskSSNZeroAlloc(tc.input)
			if got != tc.expected {
				t.Errorf("MaskSSNZeroAlloc(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestMaskEmail(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"standard email", "john.doe@example.com", "j***e@example.com"},
		{"single character local", "a@example.com", "a***@example.com"},
		{"two character local", "ab@example.com", "a***@example.com"},
		{"three character local", "abc@example.com", "a***c@example.com"},
		{"multi-subdomain", "user@sub.example.net", "u***r@sub.example.net"},
		{"missing at symbol", "invalid-email-address", "***@redacted.local"},
		{"leading at symbol", "@example.com", "***@redacted.local"},
		{"trailing at symbol", "user@", "***@redacted.local"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := MaskEmail(tc.input)
			if got != tc.expected {
				t.Errorf("MaskEmail(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestGeoPartitionFn(t *testing.T) {
	tests := []struct {
		region   string
		expected int
	}{
		{"US", 0},
		{"us", 0},
		{"USA", 0},
		{"NA", 0},
		{"NORTH_AMERICA", 0},
		{"EU", 1},
		{"eu", 1},
		{"EMEA", 1},
		{"EUROPE", 1},
		{"UK", 1},
		{"APAC", 2},
		{"JP", 2},
		{"LATAM", 2},
		{"", 2},
		{"UNKNOWN", 2},
	}

	for _, tc := range tests {
		t.Run(tc.region, func(t *testing.T) {
			acc := CustomerAccount{GeoRegion: tc.region}
			got := GeoPartitionFn(acc)
			if got != tc.expected {
				t.Errorf("GeoPartitionFn(%q) = %d, want %d", tc.region, got, tc.expected)
			}
		})
	}
}

func TestMaskCustomerPIIFn(t *testing.T) {
	fn := NewMaskCustomerPIIFn()
	var emitted []CustomerAccount
	emit := func(a CustomerAccount) {
		emitted = append(emitted, a)
	}

	now := time.Now().UTC()
	acc := CustomerAccount{
		AccountID:    "acc_01",
		CustomerName: "Alice Smith",
		SSN:          "123-45-6789",
		Email:        "alice.smith@domain.com",
		GeoRegion:    "US",
		Balance:      1000.0,
		LSN:          500,
		OpType:       "c",
		UpdatedAt:    now,
	}

	fn.ProcessElement(context.Background(), acc, emit)
	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted record, got %d", len(emitted))
	}

	got := emitted[0]
	if got.SSN != "***-**-6789" {
		t.Errorf("expected masked SSN '***-**-6789', got %q", got.SSN)
	}
	if got.Email != "a***h@domain.com" {
		t.Errorf("expected masked email 'a***h@domain.com', got %q", got.Email)
	}
}

func TestBuildGeoFanoutPipeline(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()

	now := time.Now().UTC()
	inputAccounts := []CustomerAccount{
		{
			AccountID:    "acc_us_01",
			CustomerName: "Alice Smith",
			SSN:          "123-45-6789",
			Email:        "alice.smith@usdomain.com",
			GeoRegion:    "US",
			Balance:      15000.50,
			LSN:          1001,
			OpType:       "c",
			UpdatedAt:    now,
		},
		{
			AccountID:    "acc_eu_01",
			CustomerName: "Bernhard Klein",
			SSN:          "987-65-4321",
			Email:        "b.klein@eudomain.de",
			GeoRegion:    "EU",
			Balance:      8200.00,
			LSN:          1002,
			OpType:       "c",
			UpdatedAt:    now,
		},
		{
			AccountID:    "acc_apac_01",
			CustomerName: "Chao Wei",
			SSN:          "555667777",
			Email:        "c@apacdomain.jp",
			GeoRegion:    "APAC",
			Balance:      45000.00,
			LSN:          1003,
			OpType:       "c",
			UpdatedAt:    now,
		},
	}

	col := beam.CreateList(s, inputAccounts)
	usCol, euCol, apacCol := BuildGeoFanoutPipeline(s, col)

	passert.Count(s, usCol, "us_count", 1)
	passert.Count(s, euCol, "eu_count", 1)
	passert.Count(s, apacCol, "apac_count", 1)

	ptest.RunAndValidate(t, p)
}
