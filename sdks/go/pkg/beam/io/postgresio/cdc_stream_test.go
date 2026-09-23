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
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

func TestMockReplicationStream(t *testing.T) {
	ctx := context.Background()
	msg1 := []byte("msg1")
	msg2 := []byte("msg2")
	stream := NewMockReplicationStream(msg1, msg2)

	// 1. Read first message
	res, err := stream.NextMessage(ctx)
	if err != nil || string(res) != "msg1" {
		t.Fatalf("expected msg1, got %s (err: %v)", string(res), err)
	}

	// 2. Read second message
	res, err = stream.NextMessage(ctx)
	if err != nil || string(res) != "msg2" {
		t.Fatalf("expected msg2, got %s (err: %v)", string(res), err)
	}

	// 3. Read at EOF
	res, err = stream.NextMessage(ctx)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v (res: %v)", err, res)
	}

	// 4. Send standby status update
	status := StandbyStatus{
		WriteLSN:   1000,
		FlushLSN:   900,
		ApplyLSN:   900,
		ClientTime: time.Now(),
	}
	if err := stream.SendStandbyStatus(ctx, status); err != nil {
		t.Fatalf("unexpected error sending status: %v", err)
	}

	logs := stream.GetStatusLog()
	if len(logs) != 1 || logs[0].WriteLSN != 1000 || logs[0].FlushLSN != 900 {
		t.Fatalf("unexpected status log: %+v", logs)
	}

	// 5. Close stream
	if err := stream.Close(); err != nil {
		t.Fatalf("failed to close stream: %v", err)
	}
	_, err = stream.NextMessage(ctx)
	if err != io.EOF {
		t.Fatalf("expected io.EOF after close, got %v", err)
	}
}

func TestMockReplicationStreamConcurrencyRace(t *testing.T) {
	ctx := context.Background()
	stream := NewMockReplicationStream(
		[]byte("frame1"),
		[]byte("frame2"),
		[]byte("frame3"),
		[]byte("frame4"),
	)

	var wg sync.WaitGroup

	// Reader goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			_, err := stream.NextMessage(ctx)
			if err != nil {
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Heartbeat simulator goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			_ = stream.SendStandbyStatus(ctx, StandbyStatus{
				WriteLSN:   uint64(i * 100),
				FlushLSN:   uint64(i * 50),
				ClientTime: time.Now(),
			})
			time.Sleep(1 * time.Millisecond)
		}
	}()

	wg.Wait()

	statusList := stream.GetStatusLog()
	if len(statusList) != 5 {
		t.Errorf("expected 5 status entries, got %d", len(statusList))
	}
}
