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
	Host              string
	Port              int
	Database          string
	Username          string
	Password          string `json:"password,omitempty"`
	SSLMode           string
	SSLRootCert       string
	SSLCert           string
	SSLKey            string
	SlotName          string
	Publication       string
	StartLSN          uint64
	HeartbeatInterval time.Duration
	StatusInterval    time.Duration

	// CheckpointInterval bounds how long the source reads before returning
	// from ProcessElement.
	//
	// Returning is what allows a bundle to finalize, and only a finalized
	// bundle advances the replication slot, so this is the upper bound on
	// acknowledgment latency on an idle database. Lowering it shortens the
	// window in which the primary retains WAL; raising it reduces per-bundle
	// overhead. Defaults to DefaultCheckpointInterval.
	CheckpointInterval time.Duration

	CreateSlotIfMissing bool
	ReplicaIdentityFull bool
	TokenProvider       TokenProvider `beam:"-" json:"-"`
	OriginFilter        string
	DialFunc            DialFunc                 `beam:"-" json:"-"`
	StreamFactory       ReplicationStreamFactory `beam:"-" json:"-"`
	ProtoVersion        int
	BinaryMode          *bool
	StreamingMode       string
	TwoPhaseCommit      bool

	// FailoverSlot requests that the replication slot be synchronized to
	// standbys, so it survives a failover. Requires PostgreSQL 17 or newer;
	// slot creation fails rather than silently downgrading on an older
	// server.
	//
	// Off by default. On a primary that has synchronized_standby_slots
	// configured, a failover-enabled logical slot withholds changes until the
	// listed physical standbys have received the corresponding WAL, which
	// couples the pipeline's latency to standby replication. That is the
	// correct trade for a slot that must survive failover, but it is not a
	// trade to make on a user's behalf.
	FailoverSlot bool

	// MaxSlotLagBytes is the WAL retention budget for this slot. Zero, the
	// default, disables the circuit breaker.
	//
	// A replication slot retains WAL from its restart_lsn until the consumer
	// acknowledges, with no client-side bound. A pipeline that stops
	// acknowledging therefore accumulates WAL on the primary until its volume
	// fills. Setting a budget makes that condition fail loudly instead.
	//
	// The breaker detects and reports; it does not reclaim WAL. Pair it with
	// the server-side max_slot_wal_keep_size, which does not depend on this
	// client being alive.
	MaxSlotLagBytes uint64

	// SlotLagPolicy selects what happens on breach. Defaults to
	// SlotLagFailPipeline.
	SlotLagPolicy SlotLagPolicy

	// SlotLagCheckInterval is how often retention is measured. Defaults to
	// DefaultSlotLagCheckInterval and is clamped to at least one second.
	SlotLagCheckInterval time.Duration

	// DisableSlotMonitoring turns off the slot retention monitor entirely.
	//
	// The monitor publishes cdc_slot_retained_bytes and
	// cdc_slot_xmin_horizon_age, which are the only accurate views of what this
	// slot is costing the server. It runs by default, independently of the
	// circuit breaker: measuring is useful even when nothing enforces a budget.
	//
	// The field is negated so that the zero value leaves monitoring enabled.
	// Set it through WithCDCSlotMonitoring(false), which reads in the positive.
	//
	// Cost when enabled: one additional connection per pipeline. The monitor
	// starts on the single worker that reads the replication stream, not on
	// every worker, and limits itself to one open connection.
	DisableSlotMonitoring bool

	// AllowPublisherRowSecurity permits publisher row security policies to
	// execute inside the replication session.
	//
	// A replication role that is neither SUPERUSER nor BYPASSRLS -- which is
	// what least privilege produces -- will evaluate row security policies
	// during logical decoding. A table owner can therefore cause expressions to
	// run in the replication session. By default the connector sends
	// row_security=off, which makes PostgreSQL halt replication rather than
	// execute such a policy.
	//
	// Setting this to true restores the permissive behavior. Do so only where
	// every table owner in the publication is trusted, or where a published
	// table legitimately carries a policy and halting is unacceptable.
	//
	// The field is phrased so that the zero value is the safe setting.
	AllowPublisherRowSecurity bool

	// PreflightAtConstruction additionally validates the server configuration
	// on the machine that builds the pipeline, before submission.
	//
	// Worker-side preflight always runs, on the single worker that opens the
	// replication stream. This option moves a copy of the same checks earlier,
	// which gives the fastest possible feedback but requires the submitting
	// machine to reach the database.
	//
	// Off by default because that requirement breaks two supported shapes: a
	// Dataflow Flex Template builds the graph without credentials or network
	// reachability to the primary, and TokenProvider exists precisely so
	// credentials resolve on the worker rather than at construction.
	//
	// Set it through WithCDCPreflight(true).
	PreflightAtConstruction bool
}

// CDCOption defines a functional option for configuring CDCOptions.
type CDCOption func(*CDCOptions)

// NewCDCOptions returns a CDCOptions struct initialized with production defaults.
func NewCDCOptions(opts ...CDCOption) CDCOptions {
	co := CDCOptions{
		Port:               5432,
		SSLMode:            DefaultSSLMode,
		HeartbeatInterval:  10 * time.Second,
		StatusInterval:     10 * time.Second,
		CheckpointInterval: DefaultCheckpointInterval,
	}
	for _, opt := range opts {
		opt(&co)
	}
	// An option that explicitly clears the mode must not be read as "plaintext".
	if co.SSLMode == "" {
		co.SSLMode = DefaultSSLMode
	}
	if co.CheckpointInterval <= 0 {
		co.CheckpointInterval = DefaultCheckpointInterval
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
	if err := validateSSLMode(o.SSLMode); err != nil {
		return err
	}
	if (o.SSLCert == "") != (o.SSLKey == "") {
		return fmt.Errorf("sslcert and sslkey must be supplied together")
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

// WithCDCSSLMode sets the SSL/TLS mode (disable, require, verify-ca, verify-full).
//
// The default is verify-full. An unrecognized value is rejected by Validate
// rather than being treated as a weaker mode.
func WithCDCSSLMode(sslMode string) CDCOption {
	return func(o *CDCOptions) {
		o.SSLMode = sslMode
	}
}

// WithCDCSSLRootCert sets the path to a PEM CA bundle used to verify the
// server certificate.
//
// Managed PostgreSQL services present certificates signed by CAs that are not
// present in the system trust store, so this is normally required when using
// verify-ca or verify-full against Cloud SQL, RDS or Azure Database.
func WithCDCSSLRootCert(path string) CDCOption {
	return func(o *CDCOptions) {
		o.SSLRootCert = path
	}
}

// WithCDCSSLCert sets the client certificate path for certificate
// authentication. Must be supplied together with WithCDCSSLKey.
func WithCDCSSLCert(path string) CDCOption {
	return func(o *CDCOptions) {
		o.SSLCert = path
	}
}

// WithCDCSSLKey sets the client private key path for certificate
// authentication. Must be supplied together with WithCDCSSLCert.
func WithCDCSSLKey(path string) CDCOption {
	return func(o *CDCOptions) {
		o.SSLKey = path
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
//
// Both fields are set because the session prefers StatusInterval when
// selecting its ticker period, and StatusInterval is non-zero by default.
// Setting HeartbeatInterval alone would therefore have no effect.
func WithCDCHeartbeatInterval(interval time.Duration) CDCOption {
	return func(o *CDCOptions) {
		o.HeartbeatInterval = interval
		o.StatusInterval = interval
	}
}

// WithCDCStatusInterval sets the frequency at which standby status updates are sent.
// Must be strictly less than wal_sender_timeout (default 60s).
func WithCDCStatusInterval(interval time.Duration) CDCOption {
	return func(o *CDCOptions) {
		o.StatusInterval = interval
		o.HeartbeatInterval = interval
	}
}

// WithCDCCheckpointInterval sets how long the source reads before returning
// from ProcessElement so the bundle can finalize.
//
// This is the upper bound on how long the replication slot goes
// unacknowledged when the database is idle, and therefore on how long the
// primary retains WAL that the pipeline has already durably consumed. A
// shorter interval reduces WAL retention at the cost of more, smaller bundles.
func WithCDCCheckpointInterval(interval time.Duration) CDCOption {
	return func(o *CDCOptions) {
		o.CheckpointInterval = interval
	}
}

// WithCDCCreateSlotIfMissing instructs the reader to create the replication slot if it does not exist.
func WithCDCCreateSlotIfMissing(create bool) CDCOption {
	return func(o *CDCOptions) {
		o.CreateSlotIfMissing = create
	}
}

// WithCDCFailoverSlot requests a replication slot that is synchronized to
// standbys and therefore survives a failover. Requires PostgreSQL 17 or newer.
//
// Only meaningful together with WithCDCCreateSlotIfMissing: the flag is set
// when the slot is created and this connector does not alter an existing slot.
// A slot created without it keeps its position on the primary only, so a
// failover loses the slot and the pipeline restarts from whatever position the
// new primary's slot has, if any.
//
// See the FailoverSlot field for the latency trade this implies on a primary
// with synchronized_standby_slots configured.
func WithCDCFailoverSlot(failover bool) CDCOption {
	return func(o *CDCOptions) {
		o.FailoverSlot = failover
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

// WithCDCOriginFilter configures the replication origin filter ('all' or 'none').
// When set to 'none', logical replication streams only locally-originated mutations,
// eliminating cyclic feedback loops in bidirectional active-active synchronization.
func WithCDCOriginFilter(filter string) CDCOption {
	return func(o *CDCOptions) {
		o.OriginFilter = filter
	}
}

// WithCDCProtoVersion explicitly sets the pgoutput proto_version (e.g. 1, 2, 4).
// When 0, the connector negotiates proto_version '4' on PostgreSQL >= 19
// and reverts to '1' on older versions.
func WithCDCProtoVersion(version int) CDCOption {
	return func(o *CDCOptions) {
		o.ProtoVersion = version
	}
}

// WithCDCBinaryMode sets whether column values are streamed in binary format ('b').
// When nil (default), binary mode is automatically enabled on PostgreSQL >= 19 and disabled on older versions.
func WithCDCBinaryMode(binary bool) CDCOption {
	return func(o *CDCOptions) {
		o.BinaryMode = &binary
	}
}

// WithCDCStreamingMode sets the in-progress transaction streaming mode ("parallel", "on", "off").
// When empty (default), "parallel" is automatically negotiated on PostgreSQL >= 19 and omitted on older versions.
func WithCDCStreamingMode(mode string) CDCOption {
	return func(o *CDCOptions) {
		o.StreamingMode = mode
	}
}

// WithCDCTwoPhase sets whether two-phase commit prepared transactions are decoded.
func WithCDCTwoPhase(twoPhase bool) CDCOption {
	return func(o *CDCOptions) {
		o.TwoPhaseCommit = twoPhase
	}
}

// WithCDCMaxSlotLagBytes sets the WAL retention budget and enables the
// replication slot circuit breaker.
//
// The budget is measured against pg_replication_slots.restart_lsn, which is the
// oldest LSN the slot still requires and therefore what the primary is actually
// retaining. Enabling the breaker opens one additional, ordinary connection to
// poll that value; it cannot be derived from the replication connection,
// because the in-process figure stops advancing when the pipeline stalls.
//
// On breach the default policy fails the pipeline and severs the replication
// session, leaving the slot in place so the pipeline can resume.
//
// Use MiB and GiB for readability:
//
//	postgresio.WithCDCMaxSlotLagBytes(8 * postgresio.GiB)
func WithCDCMaxSlotLagBytes(n uint64) CDCOption {
	return func(o *CDCOptions) {
		o.MaxSlotLagBytes = n
	}
}

// WithCDCSlotLagPolicy selects the action taken when the retention budget is
// exceeded. Defaults to SlotLagFailPipeline.
func WithCDCSlotLagPolicy(p SlotLagPolicy) CDCOption {
	return func(o *CDCOptions) {
		o.SlotLagPolicy = p
	}
}

// WithCDCSlotLagCheckInterval sets how often WAL retention is measured.
//
// Defaults to DefaultSlotLagCheckInterval. Values below one second are clamped:
// the query is cheap but not free, and a primary that is accumulating WAL may
// already be under pressure.
// WithCDCSlotMonitoring enables or disables the slot retention monitor.
//
// Monitoring is on by default and is independent of the circuit breaker: with
// no budget configured the monitor measures and publishes
// cdc_slot_retained_bytes and cdc_slot_xmin_horizon_age but never intervenes.
// Those are the only accurate retention signals the connector emits, so
// disabling monitoring leaves WAL growth unobservable from the pipeline.
//
// Disabling it saves one connection per pipeline.
func WithCDCSlotMonitoring(enabled bool) CDCOption {
	return func(o *CDCOptions) {
		o.DisableSlotMonitoring = !enabled
	}
}

// WithCDCPreflight validates the server configuration at pipeline construction
// time, in addition to the worker-side validation that always runs.
//
// Enable it where the submitting machine can reach the database and you want a
// misconfigured wal_level or a missing publication reported before the job is
// submitted. See the PreflightAtConstruction field for why it is off by
// default.
func WithCDCPreflight(enabled bool) CDCOption {
	return func(o *CDCOptions) {
		o.PreflightAtConstruction = enabled
	}
}

// WithCDCAllowPublisherRowSecurity permits publisher row security policies to
// execute inside the replication session.
//
// The default is to send row_security=off, which makes PostgreSQL halt
// replication if a published table carries a row security policy, rather than
// evaluating the policy under the replication role. See the
// AllowPublisherRowSecurity field for why that default is the safe one.
func WithCDCAllowPublisherRowSecurity(allow bool) CDCOption {
	return func(o *CDCOptions) {
		o.AllowPublisherRowSecurity = allow
	}
}

func WithCDCSlotLagCheckInterval(d time.Duration) CDCOption {
	return func(o *CDCOptions) {
		o.SlotLagCheckInterval = d
	}
}
