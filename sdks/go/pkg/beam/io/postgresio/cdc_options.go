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
	"fmt"
	"regexp"
	"strings"
	"time"
)

var validSlotRegex = regexp.MustCompile(`^[a-z0-9_]{1,63}$`)

// CDCOptions configures the PostgreSQL CDC replication source.
type CDCOptions struct {
	Host                 string
	Port                 int
	Database             string
	Username             string
	Password             string
	SlotName             string
	Publication          string
	StartLSN             uint64
	HeartbeatInterval    time.Duration
	StatusInterval       time.Duration
	CreateSlotIfMissing  bool
	ReplicaIdentityFull  bool
	TokenProvider        TokenProvider            `beam:"-" json:"-"`
	DialFunc             DialFunc                 `beam:"-" json:"-"`
	StreamFactory        ReplicationStreamFactory `beam:"-" json:"-"`
}

// CDCOption defines a functional option for configuring CDCOptions.
type CDCOption func(*CDCOptions)

// NewCDCOptions returns a CDCOptions struct initialized with production defaults.
func NewCDCOptions(opts ...CDCOption) CDCOptions {
	co := CDCOptions{
		Port:              5432,
		HeartbeatInterval: 10 * time.Second,
		StatusInterval:    10 * time.Second,
	}
	for _, opt := range opts {
		opt(&co)
	}
	if co.Password != "" && co.TokenProvider == nil {
		co.TokenProvider = NewStaticTokenProvider(co.Password)
	}
	return co
}

// Validate validates that all required parameters are present and safe from injection.
func (o *CDCOptions) Validate() error {
	if o.SlotName == "" {
		return fmt.Errorf("replication slot name must not be empty")
	}
	if !validSlotRegex.MatchString(o.SlotName) {
		return fmt.Errorf("invalid replication slot name %q: must match ^[a-z0-9_]{1,63}$", o.SlotName)
	}
	if o.Publication == "" {
		return fmt.Errorf("publication name must not be empty")
	}
	if _, err := SanitizeIdentifier(o.Publication); err != nil {
		return fmt.Errorf("invalid publication name %q: %w", o.Publication, err)
	}
	if o.HeartbeatInterval <= 0 {
		return fmt.Errorf("heartbeat interval must be positive")
	}
	if o.HeartbeatInterval >= 60*time.Second {
		return fmt.Errorf("heartbeat interval (%v) must be less than default wal_sender_timeout (60s)", o.HeartbeatInterval)
	}
	return nil
}

// String returns a redacted representation of the CDCOptions, safe for logging.
func (o CDCOptions) String() string {
	pwd := "<redacted>"
	if o.Password == "" {
		pwd = "<none>"
	}
	return fmt.Sprintf("CDCOptions{Host: %s, Port: %d, Database: %s, Username: %s, Password: %s, SlotName: %s, Publication: %s, StartLSN: %d, HeartbeatInterval: %v}",
		o.Host, o.Port, o.Database, o.Username, pwd, o.SlotName, o.Publication, o.StartLSN, o.HeartbeatInterval)
}

// WithCDCHost sets the PostgreSQL hostname.
func WithCDCHost(host string) CDCOption {
	return func(o *CDCOptions) {
		o.Host = host
	}
}

// WithCDCPort sets the PostgreSQL port.
func WithCDCPort(port int) CDCOption {
	return func(o *CDCOptions) {
		o.Port = port
	}
}

// WithCDCDatabase sets the database name.
func WithCDCDatabase(db string) CDCOption {
	return func(o *CDCOptions) {
		o.Database = db
	}
}

// WithCDCUsername sets the replication username.
func WithCDCUsername(user string) CDCOption {
	return func(o *CDCOptions) {
		o.Username = user
	}
}

// WithCDCPassword sets the replication password.
func WithCDCPassword(password string) CDCOption {
	return func(o *CDCOptions) {
		o.Password = password
		o.TokenProvider = NewStaticTokenProvider(password)
	}
}

// WithCDCSlotName sets the replication slot name. Must match ^[a-z0-9_]{1,63}$.
func WithCDCSlotName(slot string) CDCOption {
	return func(o *CDCOptions) {
		o.SlotName = strings.TrimSpace(slot)
	}
}

// WithCDCPublication sets the publication name.
func WithCDCPublication(pub string) CDCOption {
	return func(o *CDCOptions) {
		o.Publication = strings.TrimSpace(pub)
	}
}

// WithCDCStartLSN sets the starting Log Sequence Number.
func WithCDCStartLSN(lsn uint64) CDCOption {
	return func(o *CDCOptions) {
		o.StartLSN = lsn
	}
}

// WithCDCHeartbeatInterval sets the frequency at which keepalive status updates are sent.
// Must be strictly less than wal_sender_timeout (default 60s).
func WithCDCHeartbeatInterval(interval time.Duration) CDCOption {
	return func(o *CDCOptions) {
		o.HeartbeatInterval = interval
	}
}

// WithCDCCreateSlotIfMissing instructs the reader to create the replication slot if it does not exist.
func WithCDCCreateSlotIfMissing(create bool) CDCOption {
	return func(o *CDCOptions) {
		o.CreateSlotIfMissing = create
	}
}

// WithCDCReplicaIdentityFull flags that all replicated tables have REPLICA IDENTITY FULL,
// allowing the pipeline to bypass stateful TOAST reassembly.
func WithCDCReplicaIdentityFull(full bool) CDCOption {
	return func(o *CDCOptions) {
		o.ReplicaIdentityFull = full
	}
}

// WithCDCTokenProvider registers a dynamic token provider for IAM/OAuth2 credentials.
func WithCDCTokenProvider(provider TokenProvider) CDCOption {
	return func(o *CDCOptions) {
		o.TokenProvider = provider
	}
}

// WithCDCDialFunc registers a custom network dialer (e.g., Cloud SQL Go connector).
func WithCDCDialFunc(dial DialFunc) CDCOption {
	return func(o *CDCOptions) {
		o.DialFunc = dial
	}
}

// WithCDCStreamFactory registers a pluggable replication stream factory (used in testing).
func WithCDCStreamFactory(factory ReplicationStreamFactory) CDCOption {
	return func(o *CDCOptions) {
		o.StreamFactory = factory
	}
}
