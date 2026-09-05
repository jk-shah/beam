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

import java.sql.SQLException;
import java.util.regex.Pattern;

/**
 * Scrubs credentials, connection secrets, and sensitive PII parameters from PostgreSQL exception
 * messages before emission to logs or Dead-Letter Queues.
 */
public class SanitizingExceptionTransformer {

  private static final Pattern PASSWORD_PATTERN =
      Pattern.compile("(?i)(password|token|secret|key)=\\S+");
  private static final Pattern IP_PATTERN = Pattern.compile("\\b(?:\\d{1,3}\\.){3}\\d{1,3}\\b");

  public static String sanitizeErrorMessage(Throwable throwable) {
    if (throwable == null) {
      return "Unknown error";
    }
    String message = throwable.getMessage();
    if (message == null || message.trim().isEmpty()) {
      return throwable.getClass().getName();
    }

    // Redact password parameters
    String sanitized = PASSWORD_PATTERN.matcher(message).replaceAll("$1=[REDACTED]");
    // Mask raw internal IP addresses if present in exception
    sanitized = IP_PATTERN.matcher(sanitized).replaceAll("xxx.xxx.xxx.xxx");

    return sanitized;
  }

  public static String extractSqlState(Throwable throwable) {
    if (throwable instanceof SQLException) {
      String state = ((SQLException) throwable).getSQLState();
      if (state != null && !state.trim().isEmpty()) {
        return state;
      }
    }
    if (throwable.getCause() instanceof SQLException) {
      String state = ((SQLException) throwable.getCause()).getSQLState();
      if (state != null && !state.trim().isEmpty()) {
        return state;
      }
    }
    return "UNKNOWN";
  }
}
