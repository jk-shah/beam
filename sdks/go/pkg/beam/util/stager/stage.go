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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
)

// ComputeFileSHA256 calculates the SHA-256 hex digest and byte size of the specified file.
func ComputeFileSHA256(filePath string) (string, int64, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", 0, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer f.Close()

	hasher := sha256.New()
	size, err := io.Copy(hasher, f)
	if err != nil {
		return "", 0, fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	digest := hex.EncodeToString(hasher.Sum(nil))
	return digest, size, nil
}

// GenerateCASURI constructs a deterministic Content-Addressable Storage (CAS) URI
// partitioned by SHA-256 digest, operating system, and architecture.
func GenerateCASURI(baseURL, sha256Hex, osName, arch string) string {
	base := strings.TrimRight(baseURL, "/")
	return fmt.Sprintf("%s/binaries/%s/worker_%s_%s", base, sha256Hex, osName, arch)
}

// StagedArtifact encapsulates metadata about an artifact prepared for runner staging.
type StagedArtifact struct {
	LocalPath  string
	RemoteURI  string
	SHA256     string
	SizeBytes  int64
	Platform   TargetPlatform
}

// PrepareStagedArtifact computes the cryptographic digest of a compiled worker binary
// and returns a StagedArtifact descriptor containing the canonical CAS URI.
func PrepareStagedArtifact(localPath, stagingBaseURL string, platform TargetPlatform) (*StagedArtifact, error) {
	digest, size, err := ComputeFileSHA256(localPath)
	if err != nil {
		return nil, err
	}

	remoteURI := GenerateCASURI(stagingBaseURL, digest, platform.OS, platform.Arch)
	return &StagedArtifact{
		LocalPath: localPath,
		RemoteURI: remoteURI,
		SHA256:    digest,
		SizeBytes: size,
		Platform:  platform,
	}, nil
}
