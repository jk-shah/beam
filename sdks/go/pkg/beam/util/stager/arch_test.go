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

package stager

import (
	"runtime"
	"testing"
)

func TestNormalizeArchitecture(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"arm64", "arm64"},
		{"aarch64", "arm64"},
		{"ARM64_V8", "arm64"},
		{"amd64", "amd64"},
		{"x86_64", "amd64"},
		{"x86-64", "amd64"},
		{"X64", "amd64"},
		{"unknown", "unknown"},
	}

	for _, tt := range tests {
		got := NormalizeArchitecture(tt.input)
		if got != tt.expected {
			t.Errorf("NormalizeArchitecture(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestIsARM64MachineType(t *testing.T) {
	tests := []struct {
		machineType string
		isARM       bool
	}{
		{"t2a-standard-1", true},
		{"t2a-standard-4", true},
		{"c4a-standard-8", true},
		{"c4a-highmem-16", true},
		{"n4a-standard-4", true},
		{"m4a-standard-8", true},
		{"c6g.xlarge", true},
		{"m7g.4xlarge", true},
		{"c7gn.16xlarge", true},
		{"t4g.nano", true},
		{"n1-standard-4", false},
		{"n2-standard-8", false},
		{"e2-medium", false},
		{"c3-standard-4", false},
		{"c5.xlarge", false},
		{"m5.2xlarge", false},
		{"", false},
	}

	for _, tt := range tests {
		got := IsARM64MachineType(tt.machineType)
		if got != tt.isARM {
			t.Errorf("IsARM64MachineType(%q) = %v, want %v", tt.machineType, got, tt.isARM)
		}
	}
}

func TestResolveTargetArchitecture(t *testing.T) {
	tests := []struct {
		name        string
		opts        TargetResolutionOptions
		expectedOS  string
		expectedArch string
		expectErr   bool
	}{
		{
			name: "GCP T2A ARM64 via WorkerMachineType",
			opts: TargetResolutionOptions{
				WorkerMachineType: "t2a-standard-4",
			},
			expectedOS:   "linux",
			expectedArch: "arm64",
		},
		{
			name: "GCP Axion C4A ARM64 via MachineType",
			opts: TargetResolutionOptions{
				MachineType: "c4a-standard-16",
			},
			expectedOS:   "linux",
			expectedArch: "arm64",
		},
		{
			name: "AWS Graviton ARM64 via MachineType",
			opts: TargetResolutionOptions{
				MachineType: "m7g.2xlarge",
			},
			expectedOS:   "linux",
			expectedArch: "arm64",
		},
		{
			name: "Default AMD64 for N2",
			opts: TargetResolutionOptions{
				MachineType: "n2-standard-4",
			},
			expectedOS:   "linux",
			expectedArch: "amd64",
		},
		{
			name: "Empty MachineType defaults to linux/amd64",
			opts: TargetResolutionOptions{},
			expectedOS:   "linux",
			expectedArch: "amd64",
		},
		{
			name: "Explicit architecture arm64 override on generic machine",
			opts: TargetResolutionOptions{
				MachineType:        "custom-machine",
				WorkerArchitecture: "arm64",
			},
			expectedOS:   "linux",
			expectedArch: "arm64",
		},
		{
			name: "Experiment use_arm64_workers",
			opts: TargetResolutionOptions{
				Experiments: []string{"use_arm64_workers"},
			},
			expectedOS:   "linux",
			expectedArch: "arm64",
		},
		{
			name: "Contradiction: C4A ARM64 with explicit amd64 override fails",
			opts: TargetResolutionOptions{
				MachineType:        "c4a-standard-8",
				WorkerArchitecture: "amd64",
			},
			expectErr: true,
		},
		{
			name: "Contradiction: N2 AMD64 with explicit arm64 override fails",
			opts: TargetResolutionOptions{
				MachineType:        "n2-standard-8",
				WorkerArchitecture: "arm64",
			},
			expectErr: true,
		},
		{
			name: "Local execution uses host GOOS and GOARCH",
			opts: TargetResolutionOptions{
				IsLocalExecution: true,
			},
			expectedOS:   runtime.GOOS,
			expectedArch: runtime.GOARCH,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plat, err := ResolveTargetArchitecture(tt.opts)
			if (err != nil) != tt.expectErr {
				t.Fatalf("ResolveTargetArchitecture() error = %v, expectErr %v", err, tt.expectErr)
			}
			if !tt.expectErr {
				if plat.OS != tt.expectedOS {
					t.Errorf("got OS %q, want %q", plat.OS, tt.expectedOS)
				}
				if plat.Arch != tt.expectedArch {
					t.Errorf("got Arch %q, want %q", plat.Arch, tt.expectedArch)
				}
			}
		})
	}
}
