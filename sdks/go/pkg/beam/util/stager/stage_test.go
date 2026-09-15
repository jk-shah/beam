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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestComputeFileSHA256(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")
	content := []byte("hello beam world")
	if err := os.WriteFile(testFile, content, 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	digest, size, err := ComputeFileSHA256(testFile)
	if err != nil {
		t.Fatalf("ComputeFileSHA256 failed: %v", err)
	}

	if size != int64(len(content)) {
		t.Errorf("got size %d, want %d", size, len(content))
	}

	if len(digest) != 64 {
		t.Errorf("expected 64-char hex digest, got %d chars: %s", len(digest), digest)
	}
}

func TestGenerateCASURI(t *testing.T) {
	base := "gs://my-bucket/staging"
	sha := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	osName := "linux"
	arch := "arm64"

	uri := GenerateCASURI(base, sha, osName, arch)
	expected := "gs://my-bucket/staging/binaries/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/worker_linux_arm64"
	if uri != expected {
		t.Errorf("GenerateCASURI got %q, want %q", uri, expected)
	}
}

func TestPrepareStagedArtifact(t *testing.T) {
	tmpDir := t.TempDir()
	binPath := filepath.Join(tmpDir, "dummy_worker")
	if err := os.WriteFile(binPath, []byte("binary_payload"), 0755); err != nil {
		t.Fatalf("failed to write binary: %v", err)
	}

	plat := TargetPlatform{OS: "linux", Arch: "arm64"}
	artifact, err := PrepareStagedArtifact(binPath, "gs://beam-bucket/temp", plat)
	if err != nil {
		t.Fatalf("PrepareStagedArtifact failed: %v", err)
	}

	if artifact.LocalPath != binPath {
		t.Errorf("unexpected local path: %s", artifact.LocalPath)
	}
	if !strings.HasPrefix(artifact.RemoteURI, "gs://beam-bucket/temp/binaries/") {
		t.Errorf("unexpected remote URI: %s", artifact.RemoteURI)
	}
	if !strings.HasSuffix(artifact.RemoteURI, "/worker_linux_arm64") {
		t.Errorf("remote URI missing platform suffix: %s", artifact.RemoteURI)
	}
	if artifact.SizeBytes != int64(len("binary_payload")) {
		t.Errorf("unexpected size: %d", artifact.SizeBytes)
	}
}
