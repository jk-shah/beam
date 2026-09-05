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
package org.apache.beam.sdk.io.postgres;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import org.apache.beam.sdk.io.postgres.sink.SanitizingExceptionTransformer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SanitizingExceptionTransformer}. */
@RunWith(JUnit4.class)
public class PostgreSqlErrorSanitizationTest {

  @Test
  public void testRedactsPassword() {
    SQLException ex =
        new SQLException("Connection failed: password=superSecretPassword123 at host 10.0.1.45");
    String sanitized = SanitizingExceptionTransformer.sanitizeErrorMessage(ex);

    assertFalse(sanitized.contains("superSecretPassword123"));
    assertTrue(sanitized.contains("password=[REDACTED]"));
    assertFalse(sanitized.contains("10.0.1.45"));
    assertTrue(sanitized.contains("xxx.xxx.xxx.xxx"));
  }

  @Test
  public void testExtractsSqlState() {
    SQLException ex = new SQLException("Unique violation", "23505");
    assertEquals("23505", SanitizingExceptionTransformer.extractSqlState(ex));
  }
}
