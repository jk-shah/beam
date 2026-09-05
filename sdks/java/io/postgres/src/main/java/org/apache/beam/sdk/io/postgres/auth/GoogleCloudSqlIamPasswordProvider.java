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

import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.util.Collections;

/**
 * Dynamic password provider for Google Cloud SQL for PostgreSQL using IAM database authentication.
 *
 * <p>Generates short-lived OAuth 2.0 access tokens scoped to {@code
 * https://www.googleapis.com/auth/sqlservice.admin}. All credentials and tokens are transient to
 * avoid serialization into runner execution graphs.
 */
public class GoogleCloudSqlIamPasswordProvider implements DynamicPasswordProvider {

  private static final String SQL_ADMIN_SCOPE = "https://www.googleapis.com/auth/sqlservice.admin";

  private final String instanceConnectionName;
  private transient volatile GoogleCredentials credentials;

  public GoogleCloudSqlIamPasswordProvider(String instanceConnectionName) {
    this.instanceConnectionName = instanceConnectionName;
  }

  public static GoogleCloudSqlIamPasswordProvider forInstance(String instanceConnectionName) {
    return new GoogleCloudSqlIamPasswordProvider(instanceConnectionName);
  }

  public String getInstanceConnectionName() {
    return instanceConnectionName;
  }

  @Override
  public String getPassword() throws IOException {
    if (credentials == null) {
      synchronized (this) {
        if (credentials == null) {
          credentials =
              GoogleCredentials.getApplicationDefault()
                  .createScoped(Collections.singletonList(SQL_ADMIN_SCOPE));
        }
      }
    }
    credentials.refreshIfExpired();
    return credentials.getAccessToken().getTokenValue();
  }
}
