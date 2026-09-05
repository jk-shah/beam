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

/**
 * Service Provider Interface (SPI) for dynamically acquiring database passwords or short-lived
 * cloud IAM access tokens (Google Cloud SQL IAM, AlloyDB, AWS RDS IAM).
 *
 * <p>Invoked lazily upon connection pool socket creation to ensure long-running 24/7 streaming
 * pipelines refresh credentials before 15-60 minute token expiration windows.
 */
@FunctionalInterface
public interface DynamicPasswordProvider extends Serializable {

  /**
   * Retrieves a valid password or short-lived IAM OAuth2 / SigV4 token.
   *
   * @return A valid password or authentication token string.
   * @throws Exception If token generation or network acquisition fails.
   */
  String getPassword() throws Exception;
}
