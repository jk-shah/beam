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
package org.apache.beam.sdk.io.postgres.sink;

import com.zaxxer.hikari.HikariDataSource;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.io.postgres.PostgreSqlDataSourceConfiguration;

/**
 * JVM-singleton connection pool manager ensuring worker threads share bounded {@link
 * HikariDataSource} instances rather than instantiating individual pools per thread.
 */
public class ConnectionPoolManager {

  private static final ConcurrentHashMap<String, HikariDataSource> POOL_REGISTRY =
      new ConcurrentHashMap<>();

  public static HikariDataSource getDataSource(PostgreSqlDataSourceConfiguration config) {
    String poolKey =
        config.getUrl()
            + "|"
            + config.getUsername()
            + "|"
            + config.getMaxConnections()
            + "|"
            + config.getConnectionProperties().hashCode();

    return POOL_REGISTRY.computeIfAbsent(poolKey, k -> config.buildPooledDataSource());
  }

  public static void shutdownAll() {
    POOL_REGISTRY.values().forEach(HikariDataSource::close);
    POOL_REGISTRY.clear();
  }
}
