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
	"net"
	"testing"
)

// These tests drive the SCRAM client through NativeReplicationStream, the
// CDC replication connection that owns the SASL handshake. The mechanism
// algebra and PBKDF2 vectors are covered by scram_test.go; the cases here
// exercise the full wire exchange against a stub server. The stub server
// helpers (runSCRAMServer and friends) also live in scram_test.go.

// --- Full wire-protocol exchange ---

// TestAuthenticateSASLCompletesAgainstRealServerExchange drives the actual
// PostgreSQL SASL message framing against a stub server that implements the
// SCRAM server role, including verifying the client proof.
//
// This exercises sendSASLInitialResponse, readSASLMessage, the 'p'/'R' message
// framing and the full crypto path end to end -- not just the string algebra.
func TestAuthenticateSASLCompletesAgainstRealServerExchange(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	defer func() { _ = clientSide.Close() }()

	const password = "correct horse battery staple"

	serverResult := make(chan error, 1)
	go func() {
		serverResult <- runSCRAMServer(serverSide, password)
		_ = serverSide.Close()
	}()

	stream := &NativeReplicationStream{
		conn: clientSide,
		opts: NewCDCOptions(
			WithCDCUsername("beam_cdc"),
			WithCDCSlotName("beam_slot"),
			WithCDCPublication("beam_pub"),
		),
	}

	// Mechanism list exactly as PostgreSQL sends it: null-terminated names.
	mechList := append([]byte(mechanismSCRAMSHA256), 0, 0)

	if err := stream.authenticateSASL(mechList, password); err != nil {
		t.Fatalf("authenticateSASL() failed against a conforming server: %v", err)
	}

	if err := <-serverResult; err != nil {
		t.Fatalf("server side rejected the client: %v", err)
	}
}

// TestAuthenticateSASLFailsOnWrongPassword confirms the server's proof check
// is actually reached, i.e. the client is not blindly reporting success.
func TestAuthenticateSASLFailsOnWrongPassword(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	defer func() { _ = clientSide.Close() }()

	serverResult := make(chan error, 1)
	go func() {
		// The server knows the real password; the client will send the wrong one.
		serverResult <- runSCRAMServer(serverSide, "the-real-password")
		_ = serverSide.Close()
	}()

	stream := &NativeReplicationStream{
		conn: clientSide,
		opts: NewCDCOptions(
			WithCDCUsername("beam_cdc"),
			WithCDCSlotName("beam_slot"),
			WithCDCPublication("beam_pub"),
		),
	}
	mechList := append([]byte(mechanismSCRAMSHA256), 0, 0)

	err := stream.authenticateSASL(mechList, "the-wrong-password")
	if err == nil {
		t.Fatal("authenticateSASL() reported success with an incorrect password")
	}

	if serr := <-serverResult; serr == nil {
		t.Error("stub server accepted a proof derived from the wrong password")
	}
}
