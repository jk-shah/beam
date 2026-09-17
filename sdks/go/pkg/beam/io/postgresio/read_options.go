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
	"os"
	"strings"
	"time"
)

// ReadOptions specifies configuration for bounded PostgreSQL reads.
type ReadOptions struct {
	Host           string        `json:"host,omitempty"`
	Port           int           `json:"port,omitempty"`
	Database       string        `json:"database,omitempty"`
	Username       string        `json:"username,omitempty"`
	Password       string        `beam:"password,secret" json:"password,omitempty"`
	PasswordEnvVar string        `json:"password_env_var,omitempty"`
	SSLMode        string        `json:"sslmode,omitempty"`
	SSLRootCert    string        `json:"sslrootcert,omitempty"`
	FetchSize      int           `json:"fetch_size,omitempty"`
	MaxConnections int           `json:"max_connections,omitempty"`
	QueryTimeout   time.Duration `json:"query_timeout,omitempty"`

	// Partitioning configuration for distributed reads.
	PartitionColumn string `json:"partition_column,omitempty"`
	LowerBound      int64  `json:"lower_bound,omitempty"`
	UpperBound      int64  `json:"upper_bound,omitempty"`
	NumPartitions   int    `json:"num_partitions,omitempty"`

	// Custom network dialer (unexported from JSON to avoid serialization errors).
	DialFunc DialFunc `beam:"-" json:"-"`
}

// ReadOption is a functional configuration option for ReadOptions.
type ReadOption func(*ReadOptions)

// NewReadOptions initializes a ReadOptions struct with production-grade defaults.
func NewReadOptions(opts ...ReadOption) ReadOptions {
	ro := ReadOptions{
		Port:           5432,
		SSLMode:        "require",
		FetchSize:      5000,
		MaxConnections: 2, // Clamps worker pool connections to prevent DB exhaustion
		QueryTimeout:   30 * time.Minute,
	}
	for _, opt := range opts {
		opt(&ro)
	}
	return ro
}

// WithReadHost sets the PostgreSQL database host.
func WithReadHost(host string) ReadOption {
	return func(o *ReadOptions) {
		o.Host = host
	}
}

// WithReadPort sets the PostgreSQL database port.
func WithReadPort(port int) ReadOption {
	return func(o *ReadOptions) {
		o.Port = port
	}
}

// WithReadDatabase sets the PostgreSQL database name.
func WithReadDatabase(database string) ReadOption {
	return func(o *ReadOptions) {
		o.Database = database
	}
}

// WithReadUsername sets the authentication username.
func WithReadUsername(username string) ReadOption {
	return func(o *ReadOptions) {
		o.Username = username
	}
}

// WithReadPassword sets the authentication password.
func WithReadPassword(password string) ReadOption {
	return func(o *ReadOptions) {
		o.Password = password
	}
}

// WithReadPasswordEnvVar sets the environment variable name to resolve the authentication password.
func WithReadPasswordEnvVar(envVar string) ReadOption {
	return func(o *ReadOptions) {
		o.PasswordEnvVar = envVar
	}
}

// WithReadSSLMode sets the SSL connection mode.
func WithReadSSLMode(sslMode string) ReadOption {
	return func(o *ReadOptions) {
		o.SSLMode = sslMode
	}
}

// WithReadSSLRootCert sets the SSL root certificate path or PEM.
func WithReadSSLRootCert(cert string) ReadOption {
	return func(o *ReadOptions) {
		o.SSLRootCert = cert
	}
}

// WithReadFetchSize sets the cursor fetch chunk size.
func WithReadFetchSize(fetchSize int) ReadOption {
	return func(o *ReadOptions) {
		o.FetchSize = fetchSize
	}
}

// WithReadMaxConnections sets the maximum number of open connections per worker pool.
func WithReadMaxConnections(maxConns int) ReadOption {
	return func(o *ReadOptions) {
		o.MaxConnections = maxConns
	}
}

// WithReadQueryTimeout sets the query and cursor statement timeout.
func WithReadQueryTimeout(timeout time.Duration) ReadOption {
	return func(o *ReadOptions) {
		o.QueryTimeout = timeout
	}
}

// WithReadPartitions configures parallel range partitioning for distributed worker reads.
func WithReadPartitions(column string, lower, upper int64, numPartitions int) ReadOption {
	return func(o *ReadOptions) {
		o.PartitionColumn = column
		o.LowerBound = lower
		o.UpperBound = upper
		o.NumPartitions = numPartitions
	}
}

// WithReadDialFunc sets a custom network dialer for Cloud SQL or AlloyDB IAM connections.
func WithReadDialFunc(dialFunc DialFunc) ReadOption {
	return func(o *ReadOptions) {
		o.DialFunc = dialFunc
	}
}

// String returns a redacted representation of ReadOptions, safe for logging.
func (o ReadOptions) String() string {
	pwd := "<redacted>"
	if o.Password == "" {
		pwd = "<none>"
	}
	return fmt.Sprintf("ReadOptions{Host: %s, Port: %d, Database: %s, Username: %s, Password: %s, SSLMode: %s, FetchSize: %d, NumPartitions: %d}",
		o.Host, o.Port, o.Database, o.Username, pwd, o.SSLMode, o.FetchSize, o.NumPartitions)
}

// ResolvePassword returns the password to use, prioritizing PasswordEnvVar,
// then Password, then the PGPASSWORD environment variable.
func (o ReadOptions) ResolvePassword() string {
	if o.PasswordEnvVar != "" {
		if val := os.Getenv(o.PasswordEnvVar); val != "" {
			return val
		}
	}
	if o.Password != "" {
		return o.Password
	}
	return os.Getenv("PGPASSWORD")
}

// Validate checks configuration invariants.
func (o ReadOptions) Validate() error {
	if strings.TrimSpace(o.Host) == "" {
		return fmt.Errorf("postgresio: host cannot be empty")
	}
	if o.Port <= 0 || o.Port > 65535 {
		return fmt.Errorf("postgresio: invalid port %d", o.Port)
	}
	if strings.TrimSpace(o.Database) == "" {
		return fmt.Errorf("postgresio: database cannot be empty")
	}
	if strings.TrimSpace(o.Username) == "" {
		return fmt.Errorf("postgresio: username cannot be empty")
	}
	if o.FetchSize < 0 {
		return fmt.Errorf("postgresio: fetch_size cannot be negative")
	}
	if o.MaxConnections < 0 {
		return fmt.Errorf("postgresio: max_connections cannot be negative")
	}
	if o.NumPartitions > 0 {
		if strings.TrimSpace(o.PartitionColumn) == "" {
			return fmt.Errorf("postgresio: partition_column cannot be empty when num_partitions > 0")
		}
		if o.LowerBound >= o.UpperBound {
			return fmt.Errorf("postgresio: lower_bound (%d) must be less than upper_bound (%d)", o.LowerBound, o.UpperBound)
		}
	}
	return nil
}
