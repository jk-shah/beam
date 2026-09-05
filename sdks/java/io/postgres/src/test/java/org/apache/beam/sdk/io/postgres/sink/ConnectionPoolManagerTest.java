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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import com.zaxxer.hikari.HikariDataSource;
import java.io.Serializable;
import org.apache.beam.sdk.io.postgres.PostgreSqlDataSourceConfiguration;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ConnectionPoolManager}. */
@RunWith(JUnit4.class)
public class ConnectionPoolManagerTest implements Serializable {

  @After
  public void tearDown() {
    ConnectionPoolManager.shutdownAll();
  }

  @Test
  public void testPoolSharingAcrossThreads() {
    PostgreSqlDataSourceConfiguration config1 =
        PostgreSqlDataSourceConfiguration.create("jdbc:postgresql://localhost:5432/testdb")
            .withUsername("postgres")
            .withPassword("secret")
            .withMaxConnections(16);

    PostgreSqlDataSourceConfiguration config2 =
        PostgreSqlDataSourceConfiguration.create("jdbc:postgresql://localhost:5432/testdb")
            .withUsername("postgres")
            .withPassword("secret")
            .withMaxConnections(16);

    HikariDataSource ds1 = ConnectionPoolManager.getDataSource(config1);
    HikariDataSource ds2 = ConnectionPoolManager.getDataSource(config2);

    assertNotNull(ds1);
    assertSame(
        "DataSources with identical connection configs must share the same pool instance",
        ds1,
        ds2);
    assertEquals(16, ds1.getMaximumPoolSize());
  }
}
