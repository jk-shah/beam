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
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSanitizeFlagToken(t *testing.T) {
	validTokens := []string{
		"netgo",
		"osusergo",
		"static_build",
		"-s",
		"-w",
		"key=value",
		"tags/v1.0",
	}

	for _, token := range validTokens {
		if err := SanitizeFlagToken(token); err != nil {
			t.Errorf("SanitizeFlagToken(%q) unexpected error: %v", token, err)
		}
	}

	invalidTokens := []string{
		"-toolexec=cmd",
		"-exec=/bin/sh",
		"-compiler=gcc",
		"-extld=ld",
		"tag;rm -rf /",
		"tag$(whoami)",
		"tag`id`",
		"tag|pipe",
		"tag&bg",
	}

	for _, token := range invalidTokens {
		if err := SanitizeFlagToken(token); err == nil {
			t.Errorf("SanitizeFlagToken(%q) expected error, got nil", token)
		}
	}
}

func TestBuildCleanEnv(t *testing.T) {
	plat := TargetPlatform{OS: "linux", Arch: "arm64"}
	env := buildCleanEnv(plat)

	var hasCGO, hasGOOS, hasGOARCH, hasHostileVar bool
	for _, entry := range env {
		if entry == "CGO_ENABLED=0" {
			hasCGO = true
		}
		if entry == "GOOS=linux" {
			hasGOOS = true
		}
		if entry == "GOARCH=arm64" {
			hasGOARCH = true
		}
		if strings.HasPrefix(entry, "GOFLAGS=") || (entry != "CGO_ENABLED=0" && strings.HasPrefix(entry, "CGO_")) {
			hasHostileVar = true
		}
	}

	if !hasCGO {
		t.Errorf("missing CGO_ENABLED=0 in clean env")
	}
	if !hasGOOS {
		t.Errorf("missing GOOS=linux in clean env")
	}
	if !hasGOARCH {
		t.Errorf("missing GOARCH=arm64 in clean env")
	}
	if hasHostileVar {
		t.Errorf("clean env contains disallowed GOFLAGS or CGO_* variables")
	}
}

func TestStaticCrossCompilation_And_ELFVerification(t *testing.T) {
	// Create a minimal Go main package in temp directory
	tmpDir, err := os.MkdirTemp("", "beam-compile-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	goModFile := filepath.Join(tmpDir, "go.mod")
	if err := os.WriteFile(goModFile, []byte("module beamtestpkg\n\ngo 1.22\n"), 0644); err != nil {
		t.Fatalf("failed to write go.mod: %v", err)
	}

	srcFile := filepath.Join(tmpDir, "main.go")
	srcCode := []byte(`package main
import "fmt"
func main() {
	fmt.Println("Apache Beam Worker Process")
}
`)
	if err := os.WriteFile(srcFile, srcCode, 0644); err != nil {
		t.Fatalf("failed to write main.go: %v", err)
	}

	// 1. Cross-compile to linux/amd64
	amd64Out := filepath.Join(tmpDir, "worker_linux_amd64")
	optsAmd64 := CompileOptions{
		Platform:    TargetPlatform{OS: "linux", Arch: "amd64"},
		PackagePath: tmpDir,
		OutputFile:  amd64Out,
	}
	binAmd64, err := BuildStaticWorkerBinary(context.Background(), optsAmd64)
	if err != nil {
		t.Fatalf("BuildStaticWorkerBinary(linux/amd64) failed: %v", err)
	}

	if err := VerifyStaticELFBinary(binAmd64, "amd64"); err != nil {
		t.Errorf("VerifyStaticELFBinary(amd64) failed: %v", err)
	}

	// Mismatched arch verification should fail
	if err := VerifyStaticELFBinary(binAmd64, "arm64"); err == nil {
		t.Errorf("expected error verifying amd64 binary against arm64 expectation, got nil")
	}

	// 2. Cross-compile to linux/arm64
	arm64Out := filepath.Join(tmpDir, "worker_linux_arm64")
	optsArm64 := CompileOptions{
		Platform:    TargetPlatform{OS: "linux", Arch: "arm64"},
		PackagePath: tmpDir,
		OutputFile:  arm64Out,
	}
	binArm64, err := BuildStaticWorkerBinary(context.Background(), optsArm64)
	if err != nil {
		t.Fatalf("BuildStaticWorkerBinary(linux/arm64) failed: %v", err)
	}

	if err := VerifyStaticELFBinary(binArm64, "arm64"); err != nil {
		t.Errorf("VerifyStaticELFBinary(arm64) failed: %v", err)
	}

	// Mismatched arch verification should fail
	if err := VerifyStaticELFBinary(binArm64, "amd64"); err == nil {
		t.Errorf("expected error verifying arm64 binary against amd64 expectation, got nil")
	}
}
