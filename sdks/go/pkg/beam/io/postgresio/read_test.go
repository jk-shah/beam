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
	"database/sql"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/schematransform"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestReadOptions_Validation(t *testing.T) {
	tests := []struct {
		name      string
		opts      ReadOptions
		expectErr bool
		errSubstr string
	}{
		{
			name: "valid default options",
			opts: NewReadOptions(
				WithReadHost("localhost"),
				WithReadPort(5432),
				WithReadDatabase("testdb"),
				WithReadUsername("postgres"),
			),
			expectErr: false,
		},
		{
			name: "empty host",
			opts: NewReadOptions(
				WithReadPort(5432),
				WithReadDatabase("testdb"),
				WithReadUsername("postgres"),
			),
			expectErr: true,
			errSubstr: "host cannot be empty",
		},
		{
			name: "invalid port zero",
			opts: ReadOptions{
				Host:     "localhost",
				Port:     0,
				Database: "testdb",
				Username: "postgres",
			},
			expectErr: true,
			errSubstr: "invalid port",
		},
		{
			name: "invalid port negative",
			opts: ReadOptions{
				Host:     "localhost",
				Port:     -1,
				Database: "testdb",
				Username: "postgres",
			},
			expectErr: true,
			errSubstr: "invalid port",
		},
		{
			name: "invalid port too high",
			opts: ReadOptions{
				Host:     "localhost",
				Port:     70000,
				Database: "testdb",
				Username: "postgres",
			},
			expectErr: true,
			errSubstr: "invalid port",
		},
		{
			name: "empty database",
			opts: NewReadOptions(
				WithReadHost("localhost"),
				WithReadPort(5432),
				WithReadUsername("postgres"),
			),
			expectErr: true,
			errSubstr: "database cannot be empty",
		},
		{
			name: "empty username",
			opts: NewReadOptions(
				WithReadHost("localhost"),
				WithReadPort(5432),
				WithReadDatabase("testdb"),
			),
			expectErr: true,
			errSubstr: "username cannot be empty",
		},
		{
			name: "negative fetch size",
			opts: ReadOptions{
				Host:      "localhost",
				Port:      5432,
				Database:  "testdb",
				Username:  "postgres",
				FetchSize: -5,
			},
			expectErr: true,
			errSubstr: "fetch_size cannot be negative",
		},
		{
			name: "negative max connections",
			opts: ReadOptions{
				Host:           "localhost",
				Port:           5432,
				Database:       "testdb",
				Username:       "postgres",
				MaxConnections: -1,
			},
			expectErr: true,
			errSubstr: "max_connections cannot be negative",
		},
		{
			name: "partitions without column",
			opts: NewReadOptions(
				WithReadHost("localhost"),
				WithReadPort(5432),
				WithReadDatabase("testdb"),
				WithReadUsername("postgres"),
				WithReadPartitions("", 0, 100, 4),
			),
			expectErr: true,
			errSubstr: "partition_column cannot be empty",
		},
		{
			name: "partitions with lower >= upper",
			opts: NewReadOptions(
				WithReadHost("localhost"),
				WithReadPort(5432),
				WithReadDatabase("testdb"),
				WithReadUsername("postgres"),
				WithReadPartitions("id", 100, 50, 4),
			),
			expectErr: true,
			errSubstr: "lower_bound (100) must be less than upper_bound (50)",
		},
		{
			name: "partitions with lower == upper",
			opts: NewReadOptions(
				WithReadHost("localhost"),
				WithReadPort(5432),
				WithReadDatabase("testdb"),
				WithReadUsername("postgres"),
				WithReadPartitions("id", 100, 100, 4),
			),
			expectErr: true,
			errSubstr: "lower_bound (100) must be less than upper_bound (100)",
		},
		{
			name: "valid partitioned options",
			opts: NewReadOptions(
				WithReadHost("localhost"),
				WithReadPort(5432),
				WithReadDatabase("testdb"),
				WithReadUsername("postgres"),
				WithReadPartitions("id", 1, 10000, 8),
				WithReadFetchSize(1000),
				WithReadMaxConnections(4),
				WithReadQueryTimeout(10*time.Minute),
			),
			expectErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.opts.Validate()
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.errSubstr)
				}
				if tc.errSubstr != "" && !strings.Contains(err.Error(), tc.errSubstr) {
					t.Fatalf("expected error containing %q, got %q", tc.errSubstr, err.Error())
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestReadOptions_ResolvePassword(t *testing.T) {
	const envKey = "TEST_BEAM_PG_PASSWORD"
	_ = os.Setenv(envKey, "env_secret_pass")
	defer os.Unsetenv(envKey)

	_ = os.Setenv("PGPASSWORD", "pgpassword_pass")
	defer os.Unsetenv("PGPASSWORD")

	// 1. PasswordEnvVar takes precedence
	opts1 := ReadOptions{
		Password:       "direct_pass",
		PasswordEnvVar: envKey,
	}
	if got := opts1.ResolvePassword(); got != "env_secret_pass" {
		t.Fatalf("expected env_secret_pass from PasswordEnvVar, got %q", got)
	}

	// 2. Direct Password takes precedence over PGPASSWORD when PasswordEnvVar is unset
	opts2 := ReadOptions{
		Password: "direct_pass",
	}
	if got := opts2.ResolvePassword(); got != "direct_pass" {
		t.Fatalf("expected direct_pass, got %q", got)
	}

	// 3. Fallback to PGPASSWORD
	opts3 := ReadOptions{}
	if got := opts3.ResolvePassword(); got != "pgpassword_pass" {
		t.Fatalf("expected pgpassword_pass from PGPASSWORD fallback, got %q", got)
	}
}

func TestPlanPartitions(t *testing.T) {
	t.Run("invalid numPartitions", func(t *testing.T) {
		_, err := planPartitions(0, 100, 0)
		if err == nil || !strings.Contains(err.Error(), "num_partitions must be > 0") {
			t.Fatalf("expected num_partitions > 0 error, got %v", err)
		}
	})

	t.Run("invalid bounds", func(t *testing.T) {
		_, err := planPartitions(100, 50, 4)
		if err == nil || !strings.Contains(err.Error(), "lower_bound (100) must be < upper_bound (50)") {
			t.Fatalf("expected lower < upper error, got %v", err)
		}
	})

	t.Run("standard partitioning correctness and disjointness", func(t *testing.T) {
		lower := int64(0)
		upper := int64(100)
		numPartitions := 4

		ranges, err := planPartitions(lower, upper, numPartitions)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(ranges) != numPartitions {
			t.Fatalf("expected %d partitions, got %d: %+v", numPartitions, len(ranges), ranges)
		}

		// Verify continuity and boundaries
		for i, r := range ranges {
			if r.Index != i {
				t.Errorf("partition %d has index %d", i, r.Index)
			}
			if i == 0 && r.Start != lower {
				t.Errorf("first partition start (%d) != lower (%d)", r.Start, lower)
			}
			if i > 0 && r.Start != ranges[i-1].End {
				t.Errorf("partition %d start (%d) != partition %d end (%d)", i, r.Start, i-1, ranges[i-1].End)
			}
			if i < numPartitions-1 {
				if r.IsLast {
					t.Errorf("partition %d should not have IsLast=true", i)
				}
			} else {
				if !r.IsLast {
					t.Errorf("last partition must have IsLast=true")
				}
				if r.End != upper {
					t.Errorf("last partition end (%d) != upper (%d)", r.End, upper)
				}
			}
		}
	})

	t.Run("overflow safety with large int64 upper bound", func(t *testing.T) {
		lower := int64(0)
		upper := int64(math.MaxInt64 - 10)
		numPartitions := 16

		ranges, err := planPartitions(lower, upper, numPartitions)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(ranges) == 0 {
			t.Fatalf("expected non-empty partitions")
		}
		last := ranges[len(ranges)-1]
		if !last.IsLast || last.End != upper {
			t.Fatalf("last partition mismatch: %+v", last)
		}
	})

	t.Run("negative signed bounds", func(t *testing.T) {
		lower := int64(-1000)
		upper := int64(1000)
		numPartitions := 5

		ranges, err := planPartitions(lower, upper, numPartitions)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(ranges) != numPartitions {
			t.Fatalf("expected %d partitions, got %d", numPartitions, len(ranges))
		}
		if ranges[0].Start != lower {
			t.Fatalf("first partition start (%d) != lower (%d)", ranges[0].Start, lower)
		}
		last := ranges[len(ranges)-1]
		if !last.IsLast || last.End != upper {
			t.Fatalf("last partition mismatch: %+v", last)
		}
	})

	t.Run("small range with excess partitions", func(t *testing.T) {
		lower := int64(0)
		upper := int64(3)
		numPartitions := 10

		ranges, err := planPartitions(lower, upper, numPartitions)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(ranges) == 0 {
			t.Fatalf("expected non-empty ranges")
		}
		last := ranges[len(ranges)-1]
		if !last.IsLast || last.End != upper {
			t.Fatalf("last partition must terminate at upper bound: %+v", last)
		}
	})
}

func TestBuildPartitionQuery(t *testing.T) {
	t.Run("intermediate partition query syntax", func(t *testing.T) {
		query, args, err := buildPartitionQuery("public.orders", "id", 0, 50, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expectedQuery := `SELECT * FROM "public"."orders" WHERE "id" >= $1 AND "id" < $2`
		if query != expectedQuery {
			t.Fatalf("expected query %q, got %q", expectedQuery, query)
		}
		if len(args) != 2 || args[0] != int64(0) || args[1] != int64(50) {
			t.Fatalf("expected args [0, 50], got %+v", args)
		}
	})

	t.Run("last partition query syntax with closed terminal bound", func(t *testing.T) {
		query, args, err := buildPartitionQuery("public.orders", "id", 50, 100, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expectedQuery := `SELECT * FROM "public"."orders" WHERE "id" >= $1 AND "id" <= $2`
		if query != expectedQuery {
			t.Fatalf("expected query %q, got %q", expectedQuery, query)
		}
		if len(args) != 2 || args[0] != int64(50) || args[1] != int64(100) {
			t.Fatalf("expected args [50, 100], got %+v", args)
		}
	})

	t.Run("sql injection rejection in table name", func(t *testing.T) {
		_, _, err := buildPartitionQuery("public.orders; DROP TABLE users--", "id", 0, 50, false)
		if err == nil {
			t.Fatalf("expected SQL injection in table name to be rejected")
		}
	})

	t.Run("sql injection rejection in partition column", func(t *testing.T) {
		_, _, err := buildPartitionQuery("public.orders", "id; DROP TABLE users--", 0, 50, false)
		if err == nil {
			t.Fatalf("expected SQL injection in partition column to be rejected")
		}
	})
}

type testOrderRecord struct {
	ID        int64     `beam:"id"`
	Customer  string    `beam:"customer_name"`
	Amount    float64   `db:"total_amount"`
	Notes     string    `column:"order_notes"`
	CreatedAt time.Time `beam:"created_at"`
}

func TestStructRowMapper(t *testing.T) {
	t.Run("valid struct mapping and projection", func(t *testing.T) {
		columns := []string{"id", "customer_name", "total_amount", "order_notes", "created_at", "unknown_extra_col"}
		mapper, err := newStructRowMapper(columns, nil, reflect.TypeOf(testOrderRecord{}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(mapper.fieldIndices) != len(columns) {
			t.Fatalf("expected %d indices, got %d", len(columns), len(mapper.fieldIndices))
		}
		// First 5 columns should match struct fields (>= 0)
		for i := 0; i < 5; i++ {
			if mapper.fieldIndices[i] < 0 {
				t.Errorf("column %s at index %d failed to map to struct field", columns[i], i)
			}
		}
		// unknown_extra_col should be projected out (-1)
		if mapper.fieldIndices[5] != -1 {
			t.Errorf("expected unknown_extra_col to have index -1, got %d", mapper.fieldIndices[5])
		}
	})

	t.Run("postgres row dynamic mapping", func(t *testing.T) {
		columns := []string{"id", "name", "val"}
		mapper, err := newStructRowMapper(columns, nil, reflect.TypeOf(PostgresRow{}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !mapper.isPostgresRow {
			t.Fatalf("expected mapper.isPostgresRow to be true")
		}
	})

	t.Run("non struct type rejected", func(t *testing.T) {
		_, err := newStructRowMapper([]string{"id"}, nil, reflect.TypeOf(42))
		if err == nil || !strings.Contains(err.Error(), "must be a struct or PostgresRow") {
			t.Fatalf("expected struct validation error, got %v", err)
		}
	})

	t.Run("nil type rejected", func(t *testing.T) {
		_, err := newStructRowMapper([]string{"id"}, nil, nil)
		if err == nil || !strings.Contains(err.Error(), "recordType cannot be nil") {
			t.Fatalf("expected nil type error, got %v", err)
		}
	})
}

func TestParseJdbcURL(t *testing.T) {
	tests := []struct {
		name         string
		url          string
		expectedHost string
		expectedPort int32
		expectedDB   string
		expectedSSL  string
		expectedUser string
		expectedPass string
		expectErr    bool
	}{
		{
			name:         "standard postgres jdbc url",
			url:          "jdbc:postgresql://localhost:5432/testdb",
			expectedHost: "localhost",
			expectedPort: 5432,
			expectedDB:   "testdb",
		},
		{
			name:         "jdbc url with query parameters",
			url:          "jdbc:postgresql://db.corp.internal:5433/prod_orders?sslmode=verify-full&user=appuser&password=apppassword",
			expectedHost: "db.corp.internal",
			expectedPort: 5433,
			expectedDB:   "prod_orders",
			expectedSSL:  "verify-full",
			expectedUser: "appuser",
			expectedPass: "apppassword",
		},
		{
			name:         "jdbc url without port defaults to 5432",
			url:          "jdbc:postgresql://postgres.host/analytics",
			expectedHost: "postgres.host",
			expectedPort: 5432,
			expectedDB:   "analytics",
		},
		{
			name:         "direct postgresql url format",
			url:          "postgresql://user:pass@remotehost:55432/seeddb?sslmode=disable",
			expectedHost: "remotehost",
			expectedPort: 55432,
			expectedDB:   "seeddb",
			expectedSSL:  "disable",
			expectedUser: "user",
			expectedPass: "pass",
		},
		{
			name:      "invalid url scheme",
			url:       "mysql://localhost:3306/mydb",
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			host, port, db, ssl, user, pass, err := parseJdbcURL(tc.url)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error for url %q, got nil", tc.url)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if host != tc.expectedHost {
				t.Errorf("host: expected %q, got %q", tc.expectedHost, host)
			}
			if port != tc.expectedPort {
				t.Errorf("port: expected %d, got %d", tc.expectedPort, port)
			}
			if db != tc.expectedDB {
				t.Errorf("database: expected %q, got %q", tc.expectedDB, db)
			}
			if tc.expectedSSL != "" && ssl != tc.expectedSSL {
				t.Errorf("sslmode: expected %q, got %q", tc.expectedSSL, ssl)
			}
			if tc.expectedUser != "" && user != tc.expectedUser {
				t.Errorf("username: expected %q, got %q", tc.expectedUser, user)
			}
			if tc.expectedPass != "" && pass != tc.expectedPass {
				t.Errorf("password: expected %q, got %q", tc.expectedPass, pass)
			}
		})
	}
}

func TestPostgreSqlReadConfig_Validation(t *testing.T) {
	lower := int64(1)
	upper := int64(1000)
	badUpper := int64(0)

	tests := []struct {
		name      string
		cfg       PostgreSqlReadConfig
		expectErr bool
		errSubstr string
	}{
		{
			name: "valid table config",
			cfg: PostgreSqlReadConfig{
				Host:     "localhost",
				Port:     5432,
				Database: "testdb",
				Table:    "public.orders",
				Username: "postgres",
			},
			expectErr: false,
		},
		{
			name: "valid query config",
			cfg: PostgreSqlReadConfig{
				Host:     "localhost",
				Port:     5432,
				Database: "testdb",
				Query:    "SELECT id, amount FROM public.orders",
				Username: "postgres",
			},
			expectErr: false,
		},
		{
			name: "canonical aliases location and jdbc_url",
			cfg: PostgreSqlReadConfig{
				JdbcUrl:  "jdbc:postgresql://remotehost:5432/analytics_db",
				Location: "public.events",
				Username: "analytics_user",
			},
			expectErr: false,
		},
		{
			name: "canonical alias read_query",
			cfg: PostgreSqlReadConfig{
				Host:      "localhost",
				Database:  "testdb",
				ReadQuery: "SELECT count(*) FROM public.orders",
				Username:  "postgres",
			},
			expectErr: false,
		},
		{
			name: "both table and query specified",
			cfg: PostgreSqlReadConfig{
				Host:     "localhost",
				Database: "testdb",
				Table:    "public.orders",
				Query:    "SELECT * FROM public.orders",
				Username: "postgres",
			},
			expectErr: true,
			errSubstr: "cannot specify both table and query",
		},
		{
			name: "neither table nor query specified",
			cfg: PostgreSqlReadConfig{
				Host:     "localhost",
				Database: "testdb",
				Username: "postgres",
			},
			expectErr: true,
			errSubstr: "either table (or location) or query (or read_query) must be specified",
		},
		{
			name: "unqualified table name rejected",
			cfg: PostgreSqlReadConfig{
				Host:     "localhost",
				Database: "testdb",
				Table:    "orders",
				Username: "postgres",
			},
			expectErr: true,
			errSubstr: "must be schema-qualified",
		},
		{
			name: "partitioning on query rejected",
			cfg: PostgreSqlReadConfig{
				Host:            "localhost",
				Database:        "testdb",
				Query:           "SELECT * FROM public.orders",
				Username:        "postgres",
				NumPartitions:   4,
				PartitionColumn: "id",
				LowerBound:      &lower,
				UpperBound:      &upper,
			},
			expectErr: true,
			errSubstr: "partitioning is supported only for table reads, not arbitrary queries",
		},
		{
			name: "partitioning missing column",
			cfg: PostgreSqlReadConfig{
				Host:          "localhost",
				Database:      "testdb",
				Table:         "public.orders",
				Username:      "postgres",
				NumPartitions: 4,
				LowerBound:    &lower,
				UpperBound:    &upper,
			},
			expectErr: true,
			errSubstr: "partition_column cannot be empty",
		},
		{
			name: "partitioning invalid bounds",
			cfg: PostgreSqlReadConfig{
				Host:            "localhost",
				Database:        "testdb",
				Table:           "public.orders",
				Username:        "postgres",
				NumPartitions:   4,
				PartitionColumn: "id",
				LowerBound:      &lower,
				UpperBound:      &badUpper,
			},
			expectErr: true,
			errSubstr: "lower_bound (1) must be less than upper_bound (0)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.errSubstr)
				}
				if tc.errSubstr != "" && !strings.Contains(err.Error(), tc.errSubstr) {
					t.Fatalf("expected error containing %q, got %q", tc.errSubstr, err.Error())
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestPostgreSqlReadProvider_Registration(t *testing.T) {
	provider, schema, ok := schematransform.DefaultRegistry().Get(ReadSchemaTransformURN)
	if !ok {
		t.Fatalf("provider %q not found in default registry", ReadSchemaTransformURN)
	}
	if provider == nil {
		t.Fatalf("provider %q is nil", ReadSchemaTransformURN)
	}
	if schema == nil {
		t.Fatalf("schema proto for %q is nil", ReadSchemaTransformURN)
	}

	if provider.Identifier() != ReadSchemaTransformURN {
		t.Errorf("expected identifier %q, got %q", ReadSchemaTransformURN, provider.Identifier())
	}
	if len(provider.InputCollectionNames()) != 0 {
		t.Errorf("expected no input collections for read source, got %v", provider.InputCollectionNames())
	}
	if len(provider.OutputCollectionNames()) != 1 || provider.OutputCollectionNames()[0] != schematransform.MainOutputTag {
		t.Errorf("expected output collection [%s], got %v", schematransform.MainOutputTag, provider.OutputCollectionNames())
	}

	// Verify key configuration schema fields exist
	requiredFields := []string{"host", "database", "table", "query", "location", "read_query", "jdbc_url", "partition_column", "num_partitions"}
	fieldMap := make(map[string]bool)
	for _, f := range schema.GetFields() {
		fieldMap[f.GetName()] = true
	}
	for _, req := range requiredFields {
		if !fieldMap[req] {
			t.Errorf("expected schema field %q in reflected SchemaTransform configuration", req)
		}
	}
}

func checkIntegrationPostgres(t *testing.T) *sql.DB {
	connStr := "host=localhost port=5432 user=beam_test dbname=postgres sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Skipf("skipping live database test: failed to open postgres connection: %v", err)
		return nil
	}
	if err := db.Ping(); err != nil {
		db.Close()
		t.Skipf("skipping live database test: postgres unreachable at localhost:5432: %v", err)
		return nil
	}
	return db
}

func TestPostgreSqlRead_TableIntegration(t *testing.T) {
	db := checkIntegrationPostgres(t)
	if db == nil {
		return
	}
	defer db.Close()

	var count int
	err := db.QueryRow("SELECT count(*) FROM test_pipelines.source_orders").Scan(&count)
	if err != nil || count == 0 {
		t.Skipf("skipping: test_pipelines.source_orders not populated: %v", err)
		return
	}

	opts := NewReadOptions(
		WithReadHost("localhost"),
		WithReadPort(5432),
		WithReadDatabase("postgres"),
		WithReadUsername("beam_test"),
		WithReadSSLMode("disable"),
		WithReadFetchSize(5),
	)

	p, s := beam.NewPipelineWithRoot()
	orders := Read(s, "test_pipelines.source_orders", reflect.TypeOf(SourceOrder{}), opts)
	passert.Count(s, orders, "tableOrders", count)

	if err := ptest.Run(p); err != nil {
		t.Fatalf("pipeline execution failed: %v", err)
	}
}

func TestPostgreSqlRead_QueryIntegration(t *testing.T) {
	db := checkIntegrationPostgres(t)
	if db == nil {
		return
	}
	defer db.Close()

	var expectedCount int
	err := db.QueryRow("SELECT count(*) FROM test_pipelines.source_orders WHERE status = 'COMPLETED'").Scan(&expectedCount)
	if err != nil {
		t.Skipf("skipping: query failed: %v", err)
		return
	}

	opts := NewReadOptions(
		WithReadHost("localhost"),
		WithReadPort(5432),
		WithReadDatabase("postgres"),
		WithReadUsername("beam_test"),
		WithReadSSLMode("disable"),
		WithReadFetchSize(5),
	)

	query := "SELECT order_id, customer_id, customer_email, amount, status, country_code, items_count, created_at FROM test_pipelines.source_orders WHERE status = 'COMPLETED'"

	p, s := beam.NewPipelineWithRoot()
	orders := Query(s, query, reflect.TypeOf(SourceOrder{}), opts)
	passert.Count(s, orders, "completedOrders", expectedCount)

	if err := ptest.Run(p); err != nil {
		t.Fatalf("pipeline execution failed: %v", err)
	}
}

func TestPostgreSqlRead_PartitionedIntegration(t *testing.T) {
	db := checkIntegrationPostgres(t)
	if db == nil {
		return
	}
	defer db.Close()

	var count int
	var minID, maxID int64
	err := db.QueryRow("SELECT count(*), coalesce(min(order_id), 0), coalesce(max(order_id), 0) FROM test_pipelines.source_orders").Scan(&count, &minID, &maxID)
	if err != nil || count == 0 {
		t.Skipf("skipping: test_pipelines.source_orders not populated: %v", err)
		return
	}

	opts := NewReadOptions(
		WithReadHost("localhost"),
		WithReadPort(5432),
		WithReadDatabase("postgres"),
		WithReadUsername("beam_test"),
		WithReadSSLMode("disable"),
		WithReadFetchSize(5),
		WithReadPartitions("order_id", minID, maxID, 4),
	)

	p, s := beam.NewPipelineWithRoot()
	orders := Read(s, "test_pipelines.source_orders", reflect.TypeOf(SourceOrder{}), opts)
	passert.Count(s, orders, "partitionedOrders", count)

	if err := ptest.Run(p); err != nil {
		t.Fatalf("partitioned pipeline execution failed: %v", err)
	}
}

func TestPostgreSqlRead_ReadRowsIntegration(t *testing.T) {
	db := checkIntegrationPostgres(t)
	if db == nil {
		return
	}
	defer db.Close()

	var count int
	err := db.QueryRow("SELECT count(*) FROM test_pipelines.source_orders").Scan(&count)
	if err != nil || count == 0 {
		t.Skipf("skipping: test_pipelines.source_orders not populated: %v", err)
		return
	}

	opts := NewReadOptions(
		WithReadHost("localhost"),
		WithReadPort(5432),
		WithReadDatabase("postgres"),
		WithReadUsername("beam_test"),
		WithReadSSLMode("disable"),
		WithReadFetchSize(5),
	)

	p, s := beam.NewPipelineWithRoot()
	rows := ReadRows(s, "test_pipelines.source_orders", opts)
	passert.Count(s, rows, "dynamicRows", count)

	if err := ptest.Run(p); err != nil {
		t.Fatalf("dynamic ReadRows pipeline execution failed: %v", err)
	}
}

func TestReadOptionsPasswordRedaction(t *testing.T) {
	opts := NewReadOptions(
		WithReadHost("db.example.com"),
		WithReadUsername("beam_reader"),
		WithReadPassword("super_secret_read_pwd"),
	)
	str := opts.String()
	if strings.Contains(str, "super_secret_read_pwd") {
		t.Fatalf("ReadOptions string leaked raw password: %s", str)
	}
	if !strings.Contains(str, "<redacted>") {
		t.Fatalf("ReadOptions string missing <redacted> token: %s", str)
	}
}
