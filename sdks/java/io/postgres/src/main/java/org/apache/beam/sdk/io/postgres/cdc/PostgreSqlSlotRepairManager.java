/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.postgres.cdc;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import org.apache.beam.sdk.io.postgres.PostgreSqlDataSourceConfiguration;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager for inspecting, validating, and automatically repairing PostgreSQL logical replication
 * slots.
 *
 * <p>Detects stale slots (e.g. {@code wal_status = 'lost'}), missing slots after primary HA
 * failovers, or timeline resets, and automatically re-establishes replication with zero manual
 * intervention.
 */
public class PostgreSqlSlotRepairManager implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(PostgreSqlSlotRepairManager.class);

  public enum SlotStatus {
    HEALTHY,
    STALE_INVALIDATED,
    MISSING
  }

  public static class SlotHealthReport implements Serializable {
    private final String slotName;
    private final SlotStatus status;
    private final @Nullable String restartLsn;
    private final @Nullable String confirmedFlushLsn;
    private final @Nullable String walStatus;
    private final boolean active;
    private final @Nullable Long activePid;

    public SlotHealthReport(
        String slotName,
        SlotStatus status,
        @Nullable String restartLsn,
        @Nullable String confirmedFlushLsn,
        @Nullable String walStatus,
        boolean active,
        @Nullable Long activePid) {
      this.slotName = slotName;
      this.status = status;
      this.restartLsn = restartLsn;
      this.confirmedFlushLsn = confirmedFlushLsn;
      this.walStatus = walStatus;
      this.active = active;
      this.activePid = activePid;
    }

    public String getSlotName() {
      return slotName;
    }

    public SlotStatus getStatus() {
      return status;
    }

    public @Nullable String getRestartLsn() {
      return restartLsn;
    }

    public @Nullable String getConfirmedFlushLsn() {
      return confirmedFlushLsn;
    }

    public @Nullable String getWalStatus() {
      return walStatus;
    }

    public boolean isActive() {
      return active;
    }

    public @Nullable Long getActivePid() {
      return activePid;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof SlotHealthReport)) return false;
      SlotHealthReport that = (SlotHealthReport) o;
      return active == that.active
          && Objects.equals(slotName, that.slotName)
          && status == that.status
          && Objects.equals(restartLsn, that.restartLsn)
          && Objects.equals(confirmedFlushLsn, that.confirmedFlushLsn)
          && Objects.equals(walStatus, that.walStatus)
          && Objects.equals(activePid, that.activePid);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          slotName, status, restartLsn, confirmedFlushLsn, walStatus, active, activePid);
    }
  }

  public static class SlotRepairResult implements Serializable {
    private final boolean repaired;
    private final String slotName;
    private final @Nullable String newConsistentLsn;
    private final SlotRepairPolicy policyApplied;
    private final String message;

    public SlotRepairResult(
        boolean repaired,
        String slotName,
        @Nullable String newConsistentLsn,
        SlotRepairPolicy policyApplied,
        String message) {
      this.repaired = repaired;
      this.slotName = slotName;
      this.newConsistentLsn = newConsistentLsn;
      this.policyApplied = policyApplied;
      this.message = message;
    }

    public boolean isRepaired() {
      return repaired;
    }

    public String getSlotName() {
      return slotName;
    }

    public @Nullable String getNewConsistentLsn() {
      return newConsistentLsn;
    }

    public SlotRepairPolicy getPolicyApplied() {
      return policyApplied;
    }

    public String getMessage() {
      return message;
    }
  }

  private final PostgreSqlDataSourceConfiguration dataSourceConfig;
  private final String slotName;
  private final String outputPlugin;
  private final SlotRepairPolicy repairPolicy;

  public PostgreSqlSlotRepairManager(
      PostgreSqlDataSourceConfiguration dataSourceConfig,
      String slotName,
      String outputPlugin,
      SlotRepairPolicy repairPolicy) {
    if (!slotName.matches("^[a-zA-Z0-9_]+$")) {
      throw new IllegalArgumentException("Invalid replication slot name: " + slotName);
    }
    this.dataSourceConfig = dataSourceConfig;
    this.slotName = slotName;
    this.outputPlugin = outputPlugin;
    this.repairPolicy = repairPolicy;
  }

  public PostgreSqlDataSourceConfiguration getDataSourceConfiguration() {
    return dataSourceConfig;
  }

  /** Inspects replication slot health using a connection from the configured data source. */
  public SlotHealthReport inspectSlotHealth() throws SQLException {
    try (Connection conn = dataSourceConfig.buildRawDataSource().getConnection()) {
      return inspectSlotHealth(conn);
    }
  }

  /** Evaluates and repairs replication slot using a connection from the configured data source. */
  public SlotRepairResult verifyAndRepairSlot() throws SQLException {
    try (Connection conn = dataSourceConfig.buildRawDataSource().getConnection()) {
      return verifyAndRepairSlot(conn);
    }
  }

  /** Inspects the replication slot and returns its health status. */
  public SlotHealthReport inspectSlotHealth(Connection connection) throws SQLException {
    String sql =
        "SELECT slot_name, plugin, active, active_pid, restart_lsn, confirmed_flush_lsn, "
            + "CASE WHEN current_setting('server_version_num')::int >= 130000 THEN wal_status ELSE 'normal' END as wal_status "
            + "FROM pg_replication_slots WHERE slot_name = ?";

    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, slotName);
      try (ResultSet rs = stmt.executeQuery()) {
        if (!rs.next()) {
          LOG.warn("Replication slot '{}' does not exist on the database.", slotName);
          return new SlotHealthReport(slotName, SlotStatus.MISSING, null, null, null, false, null);
        }

        boolean active = rs.getBoolean("active");
        long pidVal = rs.getLong("active_pid");
        Long activePid = rs.wasNull() ? null : pidVal;
        String restartLsn = rs.getString("restart_lsn");
        String confirmedFlushLsn = rs.getString("confirmed_flush_lsn");
        String walStatus = rs.getString("wal_status");

        if ("lost".equalsIgnoreCase(walStatus)) {
          LOG.error("Replication slot '{}' has been invalidated (wal_status='lost').", slotName);
          return new SlotHealthReport(
              slotName,
              SlotStatus.STALE_INVALIDATED,
              restartLsn,
              confirmedFlushLsn,
              walStatus,
              active,
              activePid);
        }

        return new SlotHealthReport(
            slotName,
            SlotStatus.HEALTHY,
            restartLsn,
            confirmedFlushLsn,
            walStatus,
            active,
            activePid);
      }
    }
  }

  /**
   * Evaluates slot health and performs automated repair if necessary according to {@link
   * org.apache.beam.sdk.io.postgres.cdc.SlotRepairPolicy}.
   */
  public SlotRepairResult verifyAndRepairSlot(Connection connection) throws SQLException {
    SlotHealthReport report = inspectSlotHealth(connection);

    if (report.getStatus() == SlotStatus.HEALTHY) {
      LOG.info("Replication slot '{}' is healthy. Resuming normal CDC stream.", slotName);
      return new SlotRepairResult(
          false, slotName, report.getConfirmedFlushLsn(), repairPolicy, "Slot is healthy");
    }

    if (repairPolicy == SlotRepairPolicy.FAIL_FAST) {
      throw new IllegalStateException(
          String.format(
              "Replication slot '%s' is degraded (status=%s, wal_status=%s). Fail-fast policy active.",
              slotName, report.getStatus(), report.getWalStatus()));
    }

    LOG.warn(
        "Initiating automated slot repair for '{}' (policy={}, previous_status={})",
        slotName,
        repairPolicy,
        report.getStatus());

    // 1. Terminate zombie walsender backend if slot is currently marked active
    if (report.getActivePid() != null && report.getActivePid() > 0) {
      LOG.info(
          "Terminating stale active walsender backend PID {} for slot '{}'",
          report.getActivePid(),
          slotName);
      try (PreparedStatement termStmt =
          connection.prepareStatement("SELECT pg_terminate_backend(?)")) {
        termStmt.setLong(1, report.getActivePid());
        termStmt.execute();
      } catch (Exception e) {
        LOG.warn(
            "Failed to terminate backend PID {}. Continuing with drop attempt.",
            report.getActivePid(),
            e);
      }
    }

    // 2. Drop stale slot if present
    if (report.getStatus() == SlotStatus.STALE_INVALIDATED) {
      try (PreparedStatement dropStmt =
          connection.prepareStatement("SELECT pg_drop_replication_slot(?)")) {
        dropStmt.setString(1, slotName);
        dropStmt.execute();
        LOG.info("Dropped invalidated stale replication slot '{}'.", slotName);
      }
    }

    // 3. Create fresh replication slot at database current consistent LSN
    String newConsistentLsn = null;
    String createSql = "SELECT * FROM pg_create_logical_replication_slot(?, ?)";
    try (PreparedStatement createStmt = connection.prepareStatement(createSql)) {
      createStmt.setString(1, slotName);
      createStmt.setString(2, outputPlugin);
      try (ResultSet rs = createStmt.executeQuery()) {
        if (rs.next()) {
          newConsistentLsn = rs.getString("consistent_point");
        }
      }
      LOG.info(
          "Successfully recreated replication slot '{}' at consistent LSN '{}'.",
          slotName,
          newConsistentLsn);
    }

    return new SlotRepairResult(
        true,
        slotName,
        newConsistentLsn,
        repairPolicy,
        "Replication slot successfully recreated at " + newConsistentLsn);
  }
}
