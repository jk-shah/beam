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
package org.apache.beam.sdk.io.postgres.auth;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A wrapper {@link DataSource} that dynamically invokes {@link DynamicPasswordProvider} to supply
 * fresh passwords or OAuth2/SigV4 tokens upon every physical connection creation.
 */
public class DynamicAuthPostgreSqlDataSource implements DataSource {

  private final String jdbcUrl;
  private final String username;
  private final @Nullable DynamicPasswordProvider passwordProvider;
  private final Properties connectionProperties;
  private @Nullable PrintWriter logWriter;
  private int loginTimeout = 30;

  public DynamicAuthPostgreSqlDataSource(
      String jdbcUrl,
      String username,
      @Nullable DynamicPasswordProvider passwordProvider,
      Properties connectionProperties) {
    this.jdbcUrl = jdbcUrl;
    this.username = username;
    this.passwordProvider = passwordProvider;
    this.connectionProperties = (Properties) connectionProperties.clone();
  }

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public String getUsername() {
    return username;
  }

  public @Nullable DynamicPasswordProvider getPasswordProvider() {
    return passwordProvider;
  }

  public Properties getConnectionProperties() {
    return (Properties) connectionProperties.clone();
  }

  @Override
  public Connection getConnection() throws SQLException {
    Properties props = (Properties) connectionProperties.clone();
    props.setProperty("user", username);
    if (passwordProvider != null) {
      try {
        props.setProperty("password", passwordProvider.getPassword());
      } catch (Exception e) {
        throw new SQLException("Failed to generate dynamic password for PostgreSQL connection", e);
      }
    }
    return DriverManager.getConnection(jdbcUrl, props);
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    Properties props = (Properties) connectionProperties.clone();
    props.setProperty("user", username);
    props.setProperty("password", password);
    return DriverManager.getConnection(jdbcUrl, props);
  }

  @Override
  public PrintWriter getLogWriter() {
    return logWriter;
  }

  @Override
  public void setLogWriter(PrintWriter out) {
    this.logWriter = out;
  }

  @Override
  public void setLoginTimeout(int seconds) {
    this.loginTimeout = seconds;
  }

  @Override
  public int getLoginTimeout() {
    return loginTimeout;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException("getParentLogger not supported");
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return iface.isInstance(this);
  }
}
