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

// Package runnerlib contains utilities for submitting Go pipelines
// to a Beam model runner.
package runnerlib

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/stager"
)

// IsWorkerCompatibleBinary returns the path to itself and true if running
// a linux-amd64 binary that can directly be used as a worker binary.
func IsWorkerCompatibleBinary() (string, bool) {
	return "", false
}

var unique int32

// CompileOpts are additional options for dynamic compiles of the local code
// for development purposes. Production runs should build the worker binary
// separately for the target environment.
// See https://beam.apache.org/documentation/sdks/go-cross-compilation/ for details.
type CompileOpts struct {
	OS, Arch string
}

// BuildTempWorkerBinary creates a local worker binary in the tmp directory
// for linux/amd64. Caller responsible for deleting the binary.
func BuildTempWorkerBinary(ctx context.Context, opts CompileOpts) (string, error) {
	id := atomic.AddInt32(&unique, 1)
	filename := filepath.Join(os.TempDir(), fmt.Sprintf("worker-%v-%v", id, time.Now().UnixNano()))
	if err := buildWorkerBinary(ctx, filename, opts); err != nil {
		return "", err
	}
	return filename, nil
}

// buildWorkerBinary creates a local worker binary for the target platform using stager.
func buildWorkerBinary(ctx context.Context, filename string, opts CompileOpts) error {
	program := ""
	var isTest bool
	for i := 3; ; i++ {
		_, file, _, ok := runtime.Caller(i)
		if !ok || !strings.HasSuffix(file, ".go") || strings.HasSuffix(file, "runtime/proc.go") {
			break
		} else if strings.HasSuffix(file, "testing/testing.go") {
			isTest = true
			break
		}
		program = file
	}
	if !strings.HasSuffix(program, ".go") {
		return errors.New("could not detect user main")
	}
	goos := "linux"
	goarch := "amd64"

	if opts.OS != "" {
		goos = opts.OS
	}
	if opts.Arch != "" {
		goarch = opts.Arch
	}

	pkgDir := program[:strings.LastIndex(program, "/")+1]
	_, err := stager.BuildStaticWorkerBinary(ctx, stager.CompileOptions{
		Platform: stager.TargetPlatform{
			OS:   goos,
			Arch: goarch,
		},
		PackagePath: pkgDir,
		OutputFile:  filename,
		IsTest:      isTest,
	})
	return err
}
