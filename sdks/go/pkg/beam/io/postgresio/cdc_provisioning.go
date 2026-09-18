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
	"strings"
)

// DefaultHealthViewName is the view PostgresProvisioningScript creates unless
// the caller chooses another name.
const DefaultHealthViewName = "beam_cdc_health"

// ProvisioningConfig describes the server-side objects a CDC pipeline needs.
type ProvisioningConfig struct {
	// Role is the login role the pipeline authenticates as.
	Role string

	// Database is the database the role connects to. CONNECT is granted on it
	// explicitly: PUBLIC holds that privilege by default, but hardened
	// installations revoke it and the resulting failure is opaque.
	Database string

	// Publication is the publication to create over Tables.
	Publication string

	// Tables are the schema-qualified tables to publish, for example
	// "public.orders". Unqualified names are rejected: the connector pins
	// search_path on its connections, so an unqualified name cannot resolve.
	Tables []string

	// PublicationTables configures table-specific column projections and row filter predicates (PostgreSQL 15+).
	// If set, this takes precedence over Tables when defining the publication.
	PublicationTables []PublicationTableConfig

	// HealthViewName overrides DefaultHealthViewName.
	HealthViewName string

	// IncludeBackfillGrants emits the USAGE and SELECT grants uncommented.
	//
	// They are not required for change data capture. Logical decoding reads WAL
	// through the walsender rather than reading tables through the executor, so
	// a CDC-only role needs neither. PostgreSQL requires SELECT only "to be able
	// to copy the initial table data", which this connector does not do. Set
	// this only when the same role will also run a backfill by other means.
	IncludeBackfillGrants bool
}

// healthViewBody is the SELECT behind the monitoring view.
//
// Three choices here differ from the obvious formulation and each exists for a
// reason:
//
// Retention is measured from restart_lsn, the oldest LSN the slot still
// requires, not from confirmed_flush_lsn, which runs ahead of it and
// understates what the server is holding. This matches the circuit breaker, so
// the view and the cdc_slot_retained_bytes metric cannot disagree.
//
// The current position is selected by recovery state. pg_current_wal_lsn raises
// "recovery is in progress" on a standby, which would make the entire view
// error rather than return rows, and PostgreSQL 16 and later can decode from a
// standby.
//
// That selection appears once, in a LATERAL subquery, rather than being
// repeated for the byte count and the human-readable form. Duplicating it
// would mean a future edit could remove one copy and leave the other, which is
// not a hypothetical: mutation testing showed the standby test stayed green
// when one of two copies was deleted, because the string it asserted on
// survived in the other.
//
// The health column exists because retained_bytes is NULL exactly when things
// are worst. Slot invalidation nulls restart_lsn, pg_wal_lsn_diff of NULL is
// NULL, and an alert written as "retained_bytes > threshold" therefore stops
// firing at the moment the slot becomes unrecoverable. health is never NULL, so
// one predicate covers both the gradual and the terminal failure.
const healthViewBody = `SELECT
    s.slot_name,
    s.database,
    s.active,
    s.active_pid,
    s.wal_status,
    CASE
        WHEN s.wal_status = 'lost'       THEN 'lost'
        WHEN s.wal_status = 'unreserved' THEN 'unreserved'
        WHEN NOT s.active                THEN 'inactive'
        ELSE 'ok'
    END AS health,
    r.retained_bytes,
    pg_catalog.pg_size_pretty(r.retained_bytes) AS retained_pretty,
    pg_catalog.age(s.catalog_xmin) AS xmin_horizon_age,
    s.confirmed_flush_lsn,
    s.restart_lsn
FROM pg_catalog.pg_replication_slots s
CROSS JOIN LATERAL (
    SELECT pg_catalog.pg_wal_lsn_diff(
        CASE WHEN pg_catalog.pg_is_in_recovery()
             THEN pg_catalog.pg_last_wal_receive_lsn()
             ELSE pg_catalog.pg_current_wal_lsn()
        END,
        s.restart_lsn) AS retained_bytes) r
WHERE s.slot_type = 'logical'`

// PostgresProvisioningScript renders the SQL a DBA runs once, before the first
// pipeline: the login role, its grants, the publication, and a monitoring view.
//
// The connector never executes this script. It is returned as text so it can be
// read, edited and applied through whatever change process the database is
// under. Nothing in the connector issues DDL except replication slot creation,
// which is separately gated behind WithCDCCreateSlotIfMissing.
//
// The password is emitted as a psql variable rather than a literal, so the
// script can be committed to a runbook without carrying a credential. Supply it
// at apply time:
//
//	psql -v cdc_password="$(cat secret)" -f provision.sql
func PostgresProvisioningScript(cfg ProvisioningConfig) (string, error) {
	role, err := SanitizeIdentifier(cfg.Role)
	if err != nil {
		return "", fmt.Errorf("postgresio: invalid role: %w", err)
	}
	database, err := SanitizeIdentifier(cfg.Database)
	if err != nil {
		return "", fmt.Errorf("postgresio: invalid database: %w", err)
	}
	publication, err := SanitizeIdentifier(cfg.Publication)
	if err != nil {
		return "", fmt.Errorf("postgresio: invalid publication: %w", err)
	}

	viewName := cfg.HealthViewName
	if viewName == "" {
		viewName = DefaultHealthViewName
	}
	view, err := SanitizeIdentifier(viewName)
	if err != nil {
		return "", fmt.Errorf("postgresio: invalid health view name: %w", err)
	}

	if len(cfg.PublicationTables) == 0 && len(cfg.Tables) == 0 {
		return "", fmt.Errorf("postgresio: at least one table is required to create a publication")
	}

	tableClauses := make([]string, 0)
	tables := make([]string, 0)
	schemas := make([]string, 0)
	seenSchema := map[string]bool{}

	if len(cfg.PublicationTables) > 0 {
		for _, pt := range cfg.PublicationTables {
			if !strings.Contains(pt.TableName, ".") {
				return "", fmt.Errorf("postgresio: table %q must be schema-qualified as schema.table", pt.TableName)
			}
			sanitized, err := SanitizeTableIdentifier(pt.TableName)
			if err != nil {
				return "", fmt.Errorf("postgresio: invalid table %q: %w", pt.TableName, err)
			}
			tables = append(tables, sanitized)
			clause := sanitized
			if len(pt.Columns) > 0 {
				sanCols := make([]string, len(pt.Columns))
				for i, col := range pt.Columns {
					sc, err := SanitizeIdentifier(col)
					if err != nil {
						return "", fmt.Errorf("postgresio: invalid column %q: %w", col, err)
					}
					sanCols[i] = sc
				}
				clause += fmt.Sprintf(" (%s)", strings.Join(sanCols, ", "))
			}
			if pt.RowFilter != "" {
				if err := SanitizeRowFilter(pt.RowFilter); err != nil {
					return "", err
				}
				clause += fmt.Sprintf(" WHERE (%s)", strings.TrimSpace(pt.RowFilter))
			}
			tableClauses = append(tableClauses, clause)

			schema := sanitized[:strings.Index(sanitized, ".")]
			if !seenSchema[schema] {
				seenSchema[schema] = true
				schemas = append(schemas, schema)
			}
		}
	} else {
		for _, t := range cfg.Tables {
			if !strings.Contains(t, ".") {
				return "", fmt.Errorf("postgresio: table %q must be schema-qualified as schema.table: "+
					"the connector pins search_path, so an unqualified name cannot resolve", t)
			}
			sanitized, err := SanitizeTableIdentifier(t)
			if err != nil {
				return "", fmt.Errorf("postgresio: invalid table %q: %w", t, err)
			}
			tables = append(tables, sanitized)
			tableClauses = append(tableClauses, sanitized)

			schema := sanitized[:strings.Index(sanitized, ".")]
			if !seenSchema[schema] {
				seenSchema[schema] = true
				schemas = append(schemas, schema)
			}
		}
	}

	var b strings.Builder

	b.WriteString("-- Generated by Apache Beam postgresio. Review before applying.\n")
	b.WriteString("--\n")
	b.WriteString("-- Run once, as a superuser or as a role holding CREATE on the database and\n")
	b.WriteString("-- ownership of every published table. Those are the privileges needed to\n")
	b.WriteString("-- create these objects, not the privileges the pipeline runs with.\n")
	b.WriteString("--\n")
	b.WriteString("-- Supply the password at apply time so it is not stored in this file:\n")
	b.WriteString("--   psql -v cdc_password=\"$(cat secret)\" -f this_file.sql\n")
	b.WriteString("\n")

	fmt.Fprintf(&b, "CREATE ROLE %s WITH LOGIN REPLICATION PASSWORD :'cdc_password';\n", role)
	b.WriteString("\n")
	b.WriteString("-- PUBLIC holds CONNECT by default, but hardened installations revoke it.\n")
	fmt.Fprintf(&b, "GRANT CONNECT ON DATABASE %s TO %s;\n", database, role)
	b.WriteString("\n")

	fmt.Fprintf(&b, "CREATE PUBLICATION %s FOR TABLE %s;\n", publication, strings.Join(tableClauses, ", "))
	b.WriteString("\n")

	b.WriteString("-- Change data capture does not require SELECT on the published tables.\n")
	b.WriteString("-- Logical decoding reads WAL through the walsender; it does not read the\n")
	b.WriteString("-- tables through the executor. PostgreSQL requires SELECT only to copy the\n")
	b.WriteString("-- initial table data, and this connector performs no initial backfill.\n")
	b.WriteString("--\n")
	b.WriteString("-- Uncomment both statements together, and only if this same role will run a\n")
	b.WriteString("-- backfill by other means. USAGE without SELECT, or SELECT without USAGE,\n")
	b.WriteString("-- grants nothing usable.\n")
	prefix := "-- "
	if cfg.IncludeBackfillGrants {
		prefix = ""
	}
	for _, schema := range schemas {
		fmt.Fprintf(&b, "%sGRANT USAGE ON SCHEMA %s TO %s;\n", prefix, schema, role)
	}
	// Named tables rather than ALL TABLES IN SCHEMA, so the grant does not
	// silently widen as tables are added later.
	fmt.Fprintf(&b, "%sGRANT SELECT ON %s TO %s;\n", prefix, strings.Join(tables, ", "), role)
	b.WriteString("\n")

	b.WriteString("-- Monitoring view. Alert on health <> 'ok'.\n")
	b.WriteString("--\n")
	b.WriteString("-- Do not alert on retained_bytes alone: it is NULL once a slot is lost,\n")
	b.WriteString("-- because invalidation clears restart_lsn, so a threshold condition stops\n")
	b.WriteString("-- firing at the moment the slot becomes unrecoverable.\n")
	b.WriteString("--\n")
	b.WriteString("-- xmin_horizon_age is a separate failure mode from retained_bytes. A slot\n")
	b.WriteString("-- that stops advancing also blocks VACUUM from removing dead catalog\n")
	b.WriteString("-- tuples, which bloats the catalogs and adds wraparound pressure even when\n")
	b.WriteString("-- little WAL has been written.\n")
	b.WriteString("--\n")
	b.WriteString("-- The view is cluster-wide: WAL is a shared resource, so every logical slot\n")
	b.WriteString("-- matters, not only this database's. The database column attributes each.\n")
	b.WriteString("--\n")
	b.WriteString("-- DROP then CREATE, not CREATE OR REPLACE: the latter cannot add, reorder,\n")
	b.WriteString("-- retype or remove columns, so a future revision of this view would fail to\n")
	b.WriteString("-- apply on an existing installation. The DROP fails if a dependent object\n")
	b.WriteString("-- exists, which is deliberate.\n")
	fmt.Fprintf(&b, "DROP VIEW IF EXISTS %s;\n", view)
	fmt.Fprintf(&b, "CREATE VIEW %s AS\n%s;\n", view, healthViewBody)

	return b.String(), nil
}

// SanitizeRowFilter validates row filter expressions used in PostgreSQL 15+ publications,
// rejecting semicolons, SQL comments, and DDL/transaction keywords to prevent injection.
func SanitizeRowFilter(expr string) error {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return nil
	}
	if strings.Contains(trimmed, ";") {
		return fmt.Errorf("postgresio: row filter %q contains illegal semicolon", expr)
	}
	if strings.Contains(trimmed, "--") || strings.Contains(trimmed, "/*") {
		return fmt.Errorf("postgresio: row filter %q contains illegal SQL comment sequence", expr)
	}
	lower := strings.ToLower(trimmed)
	disallowed := []string{
		"drop ", "alter ", "create ", "truncate ", "commit", "rollback",
		"grant ", "revoke ", "insert ", "update ", "delete ",
	}
	for _, word := range disallowed {
		if strings.Contains(lower, word) {
			return fmt.Errorf("postgresio: row filter %q contains disallowed SQL keyword %q", expr, strings.TrimSpace(word))
		}
	}
	return nil
}
