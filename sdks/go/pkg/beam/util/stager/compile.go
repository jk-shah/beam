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
	"debug/elf"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
)

var (
	safeTokenRegex = regexp.MustCompile(`^[a-zA-Z0-9_\-\.\/=]+$`)
)

// CompileOptions configures the static compiler.
type CompileOptions struct {
	Platform    TargetPlatform
	PackagePath string
	OutputFile  string
	BuildTags   []string
	Ldflags     []string
	IsTest      bool
}

// SanitizeFlagToken checks a compiler or linker token against security rules,
// rejecting forbidden execution hooks like -toolexec or -exec.
func SanitizeFlagToken(token string) error {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil
	}
	if !safeTokenRegex.MatchString(token) {
		return fmt.Errorf("flag token %q contains disallowed characters", token)
	}
	lower := strings.ToLower(token)
	prohibitedPrefixes := []string{"-toolexec", "-exec", "-compiler", "-extld"}
	for _, p := range prohibitedPrefixes {
		if strings.HasPrefix(lower, p) {
			return fmt.Errorf("flag token %q contains prohibited compiler hook prefix %q", token, p)
		}
	}
	return nil
}

// BuildStaticWorkerBinary compiles a pure-Go static worker binary targeting the
// specified platform, validating that the produced binary conforms to static ELF
// invariants without dynamic library dependencies.
func BuildStaticWorkerBinary(ctx context.Context, opts CompileOptions) (string, error) {
	if opts.Platform.OS == "" {
		opts.Platform.OS = "linux"
	}
	if opts.Platform.Arch == "" {
		opts.Platform.Arch = "amd64"
	}

	// 1. Sanitize user-provided build tags and ldflags
	for _, tag := range opts.BuildTags {
		if err := SanitizeFlagToken(tag); err != nil {
			return "", fmt.Errorf("invalid build tag: %w", err)
		}
	}
	for _, flag := range opts.Ldflags {
		if err := SanitizeFlagToken(flag); err != nil {
			return "", fmt.Errorf("invalid ldflag: %w", err)
		}
	}

	// 2. Prepare output file in isolated directory if not specified
	outputFile := opts.OutputFile
	if outputFile == "" {
		tmpDir, err := os.MkdirTemp("", "beam-stager-*")
		if err != nil {
			return "", fmt.Errorf("failed to create sandboxed build directory: %w", err)
		}
		outputFile = filepath.Join(tmpDir, fmt.Sprintf("worker_%s_%s", opts.Platform.OS, opts.Platform.Arch))
	} else {
		clean := filepath.Clean(outputFile)
		if strings.Contains(clean, "..") {
			return "", fmt.Errorf("output file path %q contains prohibited directory traversal", outputFile)
		}
		outputFile = clean
	}

	// 3. Assemble tags and ldflags
	baseTags := []string{"netgo", "osusergo", "static_build"}
	allTags := append(baseTags, opts.BuildTags...)
	tagArg := strings.Join(allTags, " ")

	baseLdflags := []string{"-s", "-w", "-extldflags", "'-static'"}
	allLdflags := append(baseLdflags, opts.Ldflags...)
	ldflagsArg := strings.Join(allLdflags, " ")

	// 4. Construct command
	pkg := opts.PackagePath
	var workingDir string
	if pkg == "" {
		pkg = "."
	} else if info, err := os.Stat(pkg); err == nil && info.IsDir() {
		workingDir = pkg
		pkg = "."
	}

	var cmdArgs []string
	if opts.IsTest {
		cmdArgs = []string{
			"test", "-trimpath", "-c",
			"-tags", tagArg,
			"-ldflags", ldflagsArg,
			"-o", outputFile,
			pkg,
		}
	} else {
		cmdArgs = []string{
			"build", "-trimpath",
			"-tags", tagArg,
			"-ldflags", ldflagsArg,
			"-o", outputFile,
			pkg,
		}
	}

	// 5. Construct sanitized environment (CGO_ENABLED=0, preserved GOCACHE)
	cmd := exec.CommandContext(ctx, "go", cmdArgs...)
	if workingDir != "" {
		cmd.Dir = workingDir
	}
	cmd.Env = buildCleanEnv(opts.Platform)

	log.Infof(ctx, "stager: Compiling static worker binary for %s/%s -> %s", opts.Platform.OS, opts.Platform.Arch, outputFile)
	out, err := cmd.CombinedOutput()
	if err != nil {
		outStr := string(out)
		if strings.Contains(outStr, "cgo") || strings.Contains(outStr, "C source files not allowed when not using cgo") {
			return "", fmt.Errorf(
				"cross-compilation failed: package requires Cgo which is disabled for static portability; migrate to a pure-Go alternative or supply a pre-built worker container: %v\n%s",
				err, outStr,
			)
		}
		return "", fmt.Errorf("failed to compile worker binary for %s/%s: %v\n%s", opts.Platform.OS, opts.Platform.Arch, err, outStr)
	}

	// 6. Verify ELF binary invariants when targeting Linux
	if opts.Platform.OS == "linux" {
		if err := VerifyStaticELFBinary(outputFile, opts.Platform.Arch); err != nil {
			_ = os.Remove(outputFile)
			return "", fmt.Errorf("ELF verification failed: %w", err)
		}
	}

	return outputFile, nil
}

// VerifyStaticELFBinary verifies that the compiled binary is a 64-bit little-endian
// static ELF binary for the expected architecture, with zero dynamic linker dependencies.
func VerifyStaticELFBinary(binaryPath, expectedArch string) error {
	f, err := elf.Open(binaryPath)
	if err != nil {
		return fmt.Errorf("failed to open ELF binary %s: %w", binaryPath, err)
	}
	defer f.Close()

	// 1. Verify Class (64-bit)
	if f.Class != elf.ELFCLASS64 {
		return fmt.Errorf("expected 64-bit ELF (ELFCLASS64), got %v", f.Class)
	}

	// 2. Verify Data encoding (Little Endian)
	if f.Data != elf.ELFDATA2LSB {
		return fmt.Errorf("expected little-endian ELF (ELFDATA2LSB), got %v", f.Data)
	}

	// 3. Verify Machine Architecture
	normArch := NormalizeArchitecture(expectedArch)
	switch normArch {
	case "amd64":
		if f.Machine != elf.EM_X86_64 {
			return fmt.Errorf("expected architecture EM_X86_64 for amd64, got %v", f.Machine)
		}
	case "arm64":
		if f.Machine != elf.EM_AARCH64 {
			return fmt.Errorf("expected architecture EM_AARCH64 for arm64, got %v", f.Machine)
		}
	default:
		return fmt.Errorf("unsupported architecture verification for %q", expectedArch)
	}

	// 4. Verify Static Linking: No PT_INTERP program header
	for _, prog := range f.Progs {
		if prog.Type == elf.PT_INTERP {
			return fmt.Errorf("binary is dynamically linked: contains PT_INTERP program header")
		}
	}

	// 5. Verify Static Linking: No dynamic imported libraries
	libs, err := f.ImportedLibraries()
	if err == nil && len(libs) > 0 {
		return fmt.Errorf("binary contains dynamic library dependencies: %v", libs)
	}

	return nil
}

func buildCleanEnv(plat TargetPlatform) []string {
	safeVars := map[string]bool{
		"PATH":        true,
		"HOME":        true,
		"GOPATH":      true,
		"GOCACHE":     true,
		"GOMODCACHE":  true,
		"TMPDIR":      true,
		"SYSTEMROOT":  true,
		"USERPROFILE": true,
	}

	var env []string
	for _, kv := range os.Environ() {
		parts := strings.SplitN(kv, "=", 2)
		key := parts[0]
		if safeVars[key] {
			env = append(env, kv)
		}
	}

	// Force CGO_ENABLED=0, target GOOS, and target GOARCH
	env = append(env, "CGO_ENABLED=0", "GOOS="+plat.OS, "GOARCH="+plat.Arch)
	return env
}
