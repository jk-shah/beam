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
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Represents a PostgreSQL Range type ({@code TSRANGE}, {@code TSTZRANGE}, {@code INT4RANGE}, {@code
 * INT8RANGE}, {@code NUMRANGE}) with bound inclusivity and infinity metadata.
 *
 * @param <T> The underlying bound value type (e.g. Integer, Long, String, Instant).
 */
public class PostgreSqlRange<T> implements Serializable {

  private final @Nullable T lower;
  private final @Nullable T upper;
  private final boolean lowerInclusive;
  private final boolean upperInclusive;
  private final boolean empty;

  public PostgreSqlRange(
      @Nullable T lower,
      @Nullable T upper,
      boolean lowerInclusive,
      boolean upperInclusive,
      boolean empty) {
    this.lower = lower;
    this.upper = upper;
    this.lowerInclusive = lowerInclusive;
    this.upperInclusive = upperInclusive;
    this.empty = empty;
  }

  public static <T> PostgreSqlRange<T> empty() {
    return new PostgreSqlRange<>(null, null, false, false, true);
  }

  public static <T> PostgreSqlRange<T> of(
      @Nullable T lower, boolean lowerInclusive, @Nullable T upper, boolean upperInclusive) {
    return new PostgreSqlRange<>(lower, upper, lowerInclusive, upperInclusive, false);
  }

  public @Nullable T getLower() {
    return lower;
  }

  public @Nullable T getUpper() {
    return upper;
  }

  public boolean isLowerInclusive() {
    return lowerInclusive;
  }

  public boolean isUpperInclusive() {
    return upperInclusive;
  }

  public boolean isEmpty() {
    return empty;
  }

  public boolean isLowerUnbounded() {
    return !empty && lower == null;
  }

  public boolean isUpperUnbounded() {
    return !empty && upper == null;
  }

  /**
   * Parses standard PostgreSQL text range format: e.g. {@code "[10,20)"}, {@code "(,50]"}, {@code
   * "empty"}.
   */
  public static PostgreSqlRange<String> parse(String text) {
    String trimmed = text.trim();
    if ("empty".equalsIgnoreCase(trimmed)) {
      return PostgreSqlRange.empty();
    }

    if (trimmed.length() < 3) {
      throw new IllegalArgumentException("Invalid range format: " + text);
    }

    boolean lowerInc = trimmed.startsWith("[");
    boolean upperInc = trimmed.endsWith("]");

    String inner = trimmed.substring(1, trimmed.length() - 1);
    int commaIdx = inner.indexOf(',');
    if (commaIdx == -1) {
      throw new IllegalArgumentException("Missing comma in range string: " + text);
    }

    String lowerStr = inner.substring(0, commaIdx).trim();
    String upperStr = inner.substring(commaIdx + 1).trim();

    // Strip optional quotes
    if (lowerStr.startsWith("\"") && lowerStr.endsWith("\"") && lowerStr.length() >= 2) {
      lowerStr = lowerStr.substring(1, lowerStr.length() - 1);
    }
    if (upperStr.startsWith("\"") && upperStr.endsWith("\"") && upperStr.length() >= 2) {
      upperStr = upperStr.substring(1, upperStr.length() - 1);
    }

    String finalLower = lowerStr.isEmpty() ? null : lowerStr;
    String finalUpper = upperStr.isEmpty() ? null : upperStr;

    return PostgreSqlRange.of(finalLower, lowerInc, finalUpper, upperInc);
  }

  @Override
  public String toString() {
    if (empty) {
      return "empty";
    }
    StringBuilder sb = new StringBuilder();
    sb.append(lowerInclusive ? '[' : '(');
    sb.append(lower != null ? lower.toString() : "");
    sb.append(',');
    sb.append(upper != null ? upper.toString() : "");
    sb.append(upperInclusive ? ']' : ')');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PostgreSqlRange)) return false;
    PostgreSqlRange<?> that = (PostgreSqlRange<?>) o;
    return lowerInclusive == that.lowerInclusive
        && upperInclusive == that.upperInclusive
        && empty == that.empty
        && Objects.equals(lower, that.lower)
        && Objects.equals(upper, that.upper);
  }

  @Override
  public int hashCode() {
    return Objects.hash(lower, upper, lowerInclusive, upperInclusive, empty);
  }
}
