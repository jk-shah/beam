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
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestMain(m *testing.M) {
	ptest.Main(m)
}

func TestGeneratePooledEventsFn(t *testing.T) {
	fn := &generatePooledEventsFn{}
	var emitted []PooledEventRecord
	emit := func(r PooledEventRecord) {
		emitted = append(emitted, r)
	}

	fn.ProcessElement(101, emit)

	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted record, got %d", len(emitted))
	}
	r := emitted[0]
	if r.EventID != 101 {
		t.Errorf("expected EventID 101, got %d", r.EventID)
	}
	if r.Source != "worker-5" {
		t.Errorf("expected Source worker-5, got %s", r.Source)
	}
	if r.Payload == "" {
		t.Errorf("expected non-empty payload")
	}
}

func TestValidatePoolerRecordFn(t *testing.T) {
	fn := &validatePoolerRecordFn{}

	t.Run("valid record emitted", func(t *testing.T) {
		var emitted []PooledEventRecord
		emit := func(r PooledEventRecord) {
			emitted = append(emitted, r)
		}

		valid := PooledEventRecord{
			EventID:   501,
			Source:    "worker-1",
			Payload:   "data",
			CreatedAt: time.Now(),
		}
		fn.ProcessElement(valid, emit)

		if len(emitted) != 1 {
			t.Fatalf("expected 1 record emitted, got %d", len(emitted))
		}
	})

	t.Run("invalid record filtered", func(t *testing.T) {
		var emitted []PooledEventRecord
		emit := func(r PooledEventRecord) {
			emitted = append(emitted, r)
		}

		invalid := PooledEventRecord{
			EventID: 0,
			Payload: "",
		}
		fn.ProcessElement(invalid, emit)

		if len(emitted) != 0 {
			t.Fatalf("expected 0 records emitted for invalid record, got %d", len(emitted))
		}
	})
}

func TestPgBouncerWriteOptions(t *testing.T) {
	opts := postgresio.NewWriteOptions(
		postgresio.WithHost("pooler.example.com"),
		postgresio.WithPort(6432),
		postgresio.WithDatabase("analytics"),
		postgresio.WithPgBouncer(true),
		postgresio.WithWriteMode(postgresio.WriteModeUpsert),
	)

	if !opts.UsePgBouncer {
		t.Errorf("expected UsePgBouncer to be true")
	}
	if opts.Port != 6432 {
		t.Errorf("expected port 6432, got %d", opts.Port)
	}
}
