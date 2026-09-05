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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicInteger;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SecretManagerDynamicPasswordProvider}. */
@RunWith(JUnit4.class)
public class SecretManagerDynamicPasswordProviderTest {

  @Test
  public void testUriParsingAndProperties() {
    SecretManagerDynamicPasswordProvider provider =
        new SecretManagerDynamicPasswordProvider(
            "sm://projects/my-project/secrets/my-db-secret/versions/latest",
            Duration.standardMinutes(10),
            (p, s, v) -> "secret123");

    assertEquals(
        "sm://projects/my-project/secrets/my-db-secret/versions/latest", provider.getSecretUri());
    assertEquals("my-project", provider.getProjectId());
    assertEquals("my-db-secret", provider.getSecretName());
    assertEquals("latest", provider.getVersion());
    assertEquals(Duration.standardMinutes(10), provider.getCacheTtl());
  }

  @Test
  public void testAlternateUriFormatParsing() {
    SecretManagerDynamicPasswordProvider provider =
        new SecretManagerDynamicPasswordProvider(
            "projects/gcp-prod/secrets/db_password/versions/3",
            Duration.standardMinutes(5),
            (p, s, v) -> "prod_secret");

    assertEquals("gcp-prod", provider.getProjectId());
    assertEquals("db_password", provider.getSecretName());
    assertEquals("3", provider.getVersion());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUriThrowsException() {
    new SecretManagerDynamicPasswordProvider("invalid-uri-format");
  }

  @Test
  public void testSecretCachingAndTtlExpiry() throws Exception {
    AtomicInteger fetchCount = new AtomicInteger(0);

    SecretManagerDynamicPasswordProvider provider =
        new SecretManagerDynamicPasswordProvider(
            "sm://projects/test-p/secrets/test-s/versions/1",
            Duration.millis(200), // Short TTL for test
            (p, s, v) -> "secret_v" + fetchCount.incrementAndGet());

    // 1. Initial fetch
    String pass1 = provider.getPassword();
    assertEquals("secret_v1", pass1);
    assertEquals(1, fetchCount.get());

    // 2. Fetch within TTL -> Cached value returned without incrementing fetchCount
    String pass2 = provider.getPassword();
    assertEquals("secret_v1", pass2);
    assertEquals(1, fetchCount.get());

    // 3. Sleep past TTL expiration -> Triggers fresh fetch
    Thread.sleep(250);
    String pass3 = provider.getPassword();
    assertEquals("secret_v2", pass3);
    assertEquals(2, fetchCount.get());
  }

  @Test
  public void testEmptySecretThrowsException() {
    SecretManagerDynamicPasswordProvider provider =
        new SecretManagerDynamicPasswordProvider(
            "sm://projects/test-p/secrets/test-s/versions/1",
            Duration.standardMinutes(5),
            (p, s, v) -> "");

    try {
      provider.getPassword();
      fail("Expected IllegalStateException for empty secret");
    } catch (Exception e) {
      assertNotNull(e.getMessage());
    }
  }
}
