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

// Package stager provides architecture resolution, static cross-compilation,
// and artifact verification for Apache Beam Go worker binaries.
package stager

import (
	"fmt"
	"regexp"
	"runtime"
	"strings"
)

var (
	// gcpARMRegex matches Google Cloud ARM64 instances (Tau T2A, Axion C4A, N4A, M4A).
	gcpARMRegex = regexp.MustCompile(`^(t2a|c4a|n4a|m4a)(-.*)?$`)

	// awsARMRegex matches AWS Graviton ARM64 instances (e.g., c6g, m7g, c7gn, t4g).
	awsARMRegex = regexp.MustCompile(`^[a-z][0-9]g[a-z0-9]*\..*$`)
)

// TargetPlatform specifies the target operating system and machine architecture.
type TargetPlatform struct {
	OS   string
	Arch string
}

// String returns the platform in "GOOS/GOARCH" format.
func (p TargetPlatform) String() string {
	return fmt.Sprintf("%s/%s", p.OS, p.Arch)
}

// TargetResolutionOptions specifies pipeline runner configuration options used to
// resolve the target worker architecture.
type TargetResolutionOptions struct {
	MachineType        string
	WorkerMachineType  string
	Experiments        []string
	WorkerArchitecture string
	IsLocalExecution   bool
}

// NormalizeArchitecture standardizes architecture aliases to canonical Go GOARCH values.
func NormalizeArchitecture(arch string) string {
	arch = strings.ToLower(strings.TrimSpace(arch))
	switch arch {
	case "arm64", "aarch64", "arm64_v8", "armv8":
		return "arm64"
	case "amd64", "x86_64", "x86-64", "x64":
		return "amd64"
	default:
		return arch
	}
}

// IsARM64MachineType evaluates if the provided machine type string represents an ARM64 instance.
func IsARM64MachineType(machineType string) bool {
	mt := strings.ToLower(strings.TrimSpace(machineType))
	if mt == "" {
		return false
	}
	return gcpARMRegex.MatchString(mt) || awsARMRegex.MatchString(mt)
}

// ResolveTargetArchitecture evaluates runner options to determine the target OS and architecture,
// performing strict validation to detect and reject contradictory flag combinations.
func ResolveTargetArchitecture(opts TargetResolutionOptions) (TargetPlatform, error) {
	// 1. Determine machine type (worker machine type takes precedence over general machine type)
	machineType := strings.TrimSpace(opts.WorkerMachineType)
	if machineType == "" {
		machineType = strings.TrimSpace(opts.MachineType)
	}
	machineIsARM := IsARM64MachineType(machineType)

	// 2. Normalize explicit architecture override
	explicitArch := NormalizeArchitecture(opts.WorkerArchitecture)

	// 3. Contradiction detection: Explicit architecture vs. machine type
	if explicitArch != "" && machineType != "" {
		if machineIsARM && explicitArch == "amd64" {
			return TargetPlatform{}, fmt.Errorf(
				"configuration conflict: machine type %q requires ARM64 architecture, but worker_architecture is set to %q",
				machineType, opts.WorkerArchitecture,
			)
		}
		if !machineIsARM && explicitArch == "arm64" {
			// Check if machine type is an explicit x86-64 family (n1, n2, e2, c2, c3, m1, m2, c5, m5, etc.)
			lower := strings.ToLower(machineType)
			if strings.HasPrefix(lower, "n1-") || strings.HasPrefix(lower, "n2-") ||
				strings.HasPrefix(lower, "e2-") || strings.HasPrefix(lower, "c2-") ||
				strings.HasPrefix(lower, "c3-") || strings.HasPrefix(lower, "m1-") ||
				strings.HasPrefix(lower, "m2-") || strings.HasPrefix(lower, "c5.") ||
				strings.HasPrefix(lower, "m5.") {
				return TargetPlatform{}, fmt.Errorf(
					"configuration conflict: machine type %q requires AMD64 architecture, but worker_architecture is set to %q",
					machineType, opts.WorkerArchitecture,
				)
			}
		}
	}

	// 4. Resolve architecture
	resolvedArch := "amd64"
	if explicitArch != "" {
		resolvedArch = explicitArch
	} else if machineIsARM {
		resolvedArch = "arm64"
	} else {
		// Check experiment flags
		for _, exp := range opts.Experiments {
			exp = strings.ToLower(strings.TrimSpace(exp))
			if exp == "use_arm64_workers" || exp == "arm64_workers" || exp == "worker_architecture=arm64" {
				resolvedArch = "arm64"
				break
			}
		}
	}

	// 5. Resolve OS
	resolvedOS := "linux"
	if opts.IsLocalExecution {
		resolvedOS = runtime.GOOS
		if explicitArch == "" && !machineIsARM {
			resolvedArch = runtime.GOARCH
		}
	}

	return TargetPlatform{
		OS:   resolvedOS,
		Arch: resolvedArch,
	}, nil
}
