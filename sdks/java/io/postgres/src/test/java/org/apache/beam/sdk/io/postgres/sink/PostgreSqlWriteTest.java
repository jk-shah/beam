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

import java.io.Serializable;
import java.util.Collections;
import org.apache.beam.sdk.io.postgres.PostgreSQLIO;
import org.apache.beam.sdk.io.postgres.PostgreSqlDataSourceConfiguration;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PostgreSqlWrite} builder and DAG assembly. */
@RunWith(JUnit4.class)
public class PostgreSqlWriteTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testWriteConfigurationProperties() {
    PostgreSqlDataSourceConfiguration config =
        PostgreSqlDataSourceConfiguration.create("jdbc:postgresql://localhost:5432/mydb")
            .withUsername("postgres")
            .withPassword("secret");

    PostgreSqlWrite write =
        PostgreSQLIO.write()
            .withDataSourceConfiguration(config)
            .to("public.orders")
            .withPrimaryKeyColumns(Collections.singletonList("id"))
            .withWriteMode(PostgreSqlWrite.WriteMode.STREAMING_UPSERT_UNNEST)
            .withBatchSize(2500)
            .withMaxBufferingDuration(Duration.standardSeconds(5))
            .build();

    assertEquals(config, write.getDataSourceConfiguration());
    assertEquals(PostgreSqlWrite.WriteMode.STREAMING_UPSERT_UNNEST, write.getWriteMode());
    assertEquals(Collections.singletonList("id"), write.getPrimaryKeyColumns());
    assertEquals(2500, write.getBatchSize());
    assertEquals(Duration.standardSeconds(5), write.getMaxBufferingDuration());
  }

  @Test
  public void testWritePipelineGraphConstruction() {
    pipeline.enableAbandonedNodeEnforcement(false);

    org.apache.beam.sdk.schemas.Schema schema =
        org.apache.beam.sdk.schemas.Schema.builder()
            .addInt32Field("id")
            .addStringField("item")
            .build();

    Row row1 = Row.withSchema(schema).addValues(1, "Item A").build();
    PCollection<Row> inputRows =
        pipeline.apply("CreateInput", Create.of(row1).withRowSchema(schema));

    PostgreSqlDataSourceConfiguration config =
        PostgreSqlDataSourceConfiguration.create("jdbc:postgresql://localhost:5432/mydb")
            .withUsername("postgres")
            .withPassword("secret");

    PostgreSqlWriteResult result =
        inputRows.apply(
            "WriteToPostgres",
            PostgreSQLIO.write()
                .withDataSourceConfiguration(config)
                .to("public.orders")
                .withPrimaryKeyColumns(Collections.singletonList("id"))
                .withWriteMode(PostgreSqlWrite.WriteMode.STREAMING_UPSERT_UNNEST)
                .build());

    assertNotNull(result.getSuccessfulRows());
    assertNotNull(result.getFailedRows());
  }
}
