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

package expansion

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// HandshakePrefix is the sentinel prefix output to stdout when the Go expansion server is ready.
const HandshakePrefix = "BEAM_EXPANSION_READY"

// GenerateAuthToken creates a cryptographically secure 32-byte hexadecimal bearer token.
func GenerateAuthToken() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("failed to generate random auth token: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}

// FormatHandshake formats the synchronous readiness string for stdout handshake with Python/YAML clients.
func FormatHandshake(port int, token string) string {
	if token != "" {
		return fmt.Sprintf("%s:port=%d:token=%s\n", HandshakePrefix, port, token)
	}
	return fmt.Sprintf("%s:port=%d\n", HandshakePrefix, port)
}

// ParseHandshake parses the handshake line emitted by the Go expansion service.
func ParseHandshake(line string) (int, string, error) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, HandshakePrefix) {
		return 0, "", fmt.Errorf("invalid handshake prefix in line: %s", line)
	}

	parts := strings.Split(line, ":")
	port := 0
	token := ""

	for _, p := range parts[1:] {
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "port":
			var err error
			port, err = strconv.Atoi(kv[1])
			if err != nil {
				return 0, "", fmt.Errorf("invalid port in handshake: %w", err)
			}
		case "token":
			token = kv[1]
		}
	}

	if port == 0 {
		return 0, "", fmt.Errorf("handshake missing valid port: %s", line)
	}
	return port, token, nil
}

// StartStdinMonitor monitors standard input. If the parent process crashes or terminates,
// EOF is read from stdin, triggering immediate graceful shutdown of the daemon.
func StartStdinMonitor(onShutdown func()) {
	go func() {
		buf := make([]byte, 1)
		for {
			_, err := os.Stdin.Read(buf)
			if err != nil {
				if err == io.EOF || strings.Contains(err.Error(), "closed") {
					if onShutdown != nil {
						onShutdown()
					}
					os.Exit(0)
				}
				return
			}
		}
	}()
}
