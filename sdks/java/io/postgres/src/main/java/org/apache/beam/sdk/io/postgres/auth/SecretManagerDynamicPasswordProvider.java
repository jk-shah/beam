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

import java.io.Serializable;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DynamicPasswordProvider} that resolves database credentials from Google Cloud Secret
 * Manager (or compatible secret providers) and caches the secret with a configurable TTL to support
 * automatic credential rotation without pipeline restarts.
 *
 * <p>Supported URI formats:
 *
 * <ul>
 *   <li>{@code sm://projects/PROJECT_ID/secrets/SECRET_NAME/versions/VERSION}
 *   <li>{@code projects/PROJECT_ID/secrets/SECRET_NAME/versions/VERSION}
 * </ul>
 */
public class SecretManagerDynamicPasswordProvider implements DynamicPasswordProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(SecretManagerDynamicPasswordProvider.class);

  private static final Pattern SECRET_URI_PATTERN =
      Pattern.compile("(?:sm://)?projects/([^/]+)/secrets/([^/]+)/versions/([^/]+)");

  /** SPI interface for fetching the raw secret payload. */
  @FunctionalInterface
  public interface SecretFetcher extends Serializable {
    String fetchSecret(String projectId, String secretName, String version) throws Exception;
  }

  private final String secretUri;
  private final String projectId;
  private final String secretName;
  private final String version;
  private final Duration cacheTtl;
  private final SecretFetcher secretFetcher;

  private transient volatile @Nullable String cachedSecret;
  private transient volatile @Nullable Instant cacheExpiry;

  public SecretManagerDynamicPasswordProvider(
      String secretUri, Duration cacheTtl, SecretFetcher secretFetcher) {
    Matcher matcher = SECRET_URI_PATTERN.matcher(secretUri);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid Secret Manager URI: '%s'. Expected format: 'sm://projects/{project}/secrets/{secret}/versions/{version}'",
              secretUri));
    }
    this.secretUri = secretUri;
    this.projectId = matcher.group(1);
    this.secretName = matcher.group(2);
    this.version = matcher.group(3);
    this.cacheTtl = cacheTtl;
    this.secretFetcher = secretFetcher;
  }

  public SecretManagerDynamicPasswordProvider(String secretUri, Duration cacheTtl) {
    this(secretUri, cacheTtl, new DefaultCloudSecretFetcher());
  }

  public SecretManagerDynamicPasswordProvider(String secretUri) {
    this(secretUri, Duration.standardMinutes(15));
  }

  public String getSecretUri() {
    return secretUri;
  }

  public String getProjectId() {
    return projectId;
  }

  public String getSecretName() {
    return secretName;
  }

  public String getVersion() {
    return version;
  }

  public Duration getCacheTtl() {
    return cacheTtl;
  }

  @Override
  public String getPassword() throws Exception {
    Instant now = Instant.now();
    String currentSecret = cachedSecret;
    Instant currentExpiry = cacheExpiry;

    if (currentSecret != null && currentExpiry != null && now.isBefore(currentExpiry)) {
      return currentSecret;
    }

    synchronized (this) {
      now = Instant.now();
      if (cachedSecret != null && cacheExpiry != null && now.isBefore(cacheExpiry)) {
        return cachedSecret;
      }

      LOG.info("Refreshing database secret from Secret Manager: {}", secretUri);
      String fetched = secretFetcher.fetchSecret(projectId, secretName, version);
      if (fetched == null || fetched.isEmpty()) {
        throw new IllegalStateException(
            "Secret Manager returned empty secret for URI: " + secretUri);
      }

      this.cachedSecret = fetched;
      this.cacheExpiry = now.plus(cacheTtl);
      return fetched;
    }
  }

  /** Default secret fetcher invoking Google Cloud Secret Manager client via reflection. */
  public static class DefaultCloudSecretFetcher implements SecretFetcher {
    @Override
    public String fetchSecret(String projectId, String secretName, String version)
        throws Exception {
      try {
        Class<?> clientClass =
            Class.forName("com.google.cloud.secretmanager.v1.SecretManagerServiceClient");
        Class<?> nameClass = Class.forName("com.google.cloud.secretmanager.v1.SecretVersionName");
        Object secretVersionName =
            nameClass
                .getMethod("of", String.class, String.class, String.class)
                .invoke(null, projectId, secretName, version);

        Object client = clientClass.getMethod("create").invoke(null);
        try {
          Object response =
              clientClass
                  .getMethod("accessSecretVersion", nameClass)
                  .invoke(client, secretVersionName);
          Class<?> responseClass =
              Class.forName("com.google.cloud.secretmanager.v1.AccessSecretVersionResponse");
          Object payload = responseClass.getMethod("getPayload").invoke(response);
          Class<?> payloadClass = Class.forName("com.google.cloud.secretmanager.v1.SecretPayload");
          Object byteString = payloadClass.getMethod("getData").invoke(payload);
          return (String) byteString.getClass().getMethod("toStringUtf8").invoke(byteString);
        } finally {
          if (client instanceof AutoCloseable) {
            ((AutoCloseable) client).close();
          }
        }
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException(
            "com.google.cloud:google-cloud-secretmanager library is required on classpath to resolve Secret Manager URIs. "
                + "Add the dependency or provide a custom SecretFetcher.",
            e);
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SecretManagerDynamicPasswordProvider)) return false;
    SecretManagerDynamicPasswordProvider that = (SecretManagerDynamicPasswordProvider) o;
    return Objects.equals(secretUri, that.secretUri) && Objects.equals(cacheTtl, that.cacheTtl);
  }

  @Override
  public int hashCode() {
    return Objects.hash(secretUri, cacheTtl);
  }
}
