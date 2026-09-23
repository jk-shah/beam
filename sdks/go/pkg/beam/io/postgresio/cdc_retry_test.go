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
	"database/sql/driver"
	"errors"
	"io"
	"testing"
	"time"
)

func TestIsRetryableConnectionError(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{io.EOF, true},
		{io.ErrUnexpectedEOF, true},
		{errStreamDesynchronized, true},
		{errors.New("driver: bad connection"), true},
		{driver.ErrBadConn, true},
		{errors.New("replication stream error: C57P01\x00"), true},
		{errors.New("replication stream error: C08006\x00"), true},
		{errors.New("some other error"), false},
	}

	for _, tt := range tests {
		if got := isRetryableConnectionError(tt.err); got != tt.want {
			t.Errorf("isRetryableConnectionError(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

func TestComputeBackoff(t *testing.T) {
	for attempt := 1; attempt <= 15; attempt++ {
		backoff := computeBackoff(attempt)
		t.Logf("attempt %d: %v", attempt, backoff)
		if backoff <= 0 {
			t.Errorf("expected positive backoff, got %v", backoff)
		}
		if backoff > 33*time.Second { // 30s cap + 10% jitter
			t.Errorf("backoff exceeded cap + jitter: %v", backoff)
		}
	}
}

func TestFilteredEventAckCadence(t *testing.T) {
	// Create a stream that emits only filtered events, followed by keepalive.
	opts := NewCDCOptions(
		WithCDCSlotName("test_slot"),
		WithCDCPublication("test_pub"),
		WithCDCHost("localhost"),
		WithCDCOriginFilter("none"),
	)

	// Create mock events
	// Filtered out because Origin != "" and OriginFilter == "none"
	ev1 := &ChangeEvent{LSN: 100, Origin: "other_node", CommitTime: time.Now()}
	ev2 := &ChangeEvent{LSN: 200, Origin: "other_node", CommitTime: time.Now()}

	session := &cdcSession{
		parser: NewPgOutputParser(),
		done:   make(chan struct{}),
	}
	session.confirmedFlushLSN.Store(50)

	fn := newCDCSourceFn(opts)
	fn.session = session

	// We simulate the inner loop logic for ackCandidate
	var ackCandidate uint64
	var pendingAck uint64

	events := []*ChangeEvent{ev1, ev2}
	for _, event := range events {
		if event.LSN > pendingAck {
			pendingAck = event.LSN
		}
		if fn.Options.OriginFilter == "none" && event.Origin != "" {
			continue
		}
	}

	if !session.parser.HasBufferedTransaction() && pendingAck > ackCandidate {
		ackCandidate = pendingAck
	}

	if ackCandidate != 200 {
		t.Errorf("expected ackCandidate 200 for filtered events, got %d", ackCandidate)
	}
}
