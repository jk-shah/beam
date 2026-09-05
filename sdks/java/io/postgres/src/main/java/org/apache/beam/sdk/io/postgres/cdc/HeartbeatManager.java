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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically generates synthetic logical messages via {@code pg_logical_emit_message} on
 * low-traffic tables to ensure write-ahead log progression and advance the Beam pipeline watermark.
 */
public class HeartbeatManager implements Serializable, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatManager.class);
  private final DataSource dataSource;
  private final long intervalSeconds;
  private transient ScheduledExecutorService executor;

  public HeartbeatManager(DataSource dataSource, long intervalSeconds) {
    this.dataSource = dataSource;
    this.intervalSeconds = intervalSeconds;
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  public void start() {
    if (intervalSeconds <= 0) {
      return;
    }
    executor = Executors.newSingleThreadScheduledExecutor();
    executor.scheduleAtFixedRate(
        this::emitHeartbeat, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
  }

  private void emitHeartbeat() {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt =
            conn.prepareStatement(
                "SELECT pg_logical_emit_message(false, 'beam_heartbeat', clock_timestamp()::text)")) {
      stmt.execute();
    } catch (Exception e) {
      LOG.warn("Failed to emit PostgreSQL CDC logical heartbeat", e);
    }
  }

  @Override
  public void close() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }
}
