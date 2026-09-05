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

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * Result object returned by {@link PostgreSqlWrite}, providing access to successfully committed
 * rows and rejected dead-letter queue (DLQ) rows.
 */
public class PostgreSqlWriteResult implements POutput {

  private final Pipeline pipeline;
  private final PCollection<Row> successfulRows;
  private final PCollection<PostgreSqlWriteError> failedRows;

  public PostgreSqlWriteResult(
      Pipeline pipeline,
      PCollection<Row> successfulRows,
      PCollection<PostgreSqlWriteError> failedRows) {
    this.pipeline = pipeline;
    this.successfulRows = successfulRows;
    this.failedRows = failedRows;
  }

  public static PostgreSqlWriteResult of(
      Pipeline pipeline,
      PCollection<Row> successfulRows,
      PCollection<PostgreSqlWriteError> failedRows) {
    return new PostgreSqlWriteResult(pipeline, successfulRows, failedRows);
  }

  public PCollection<Row> getSuccessfulRows() {
    return successfulRows;
  }

  public PCollection<PostgreSqlWriteError> getFailedRows() {
    return failedRows;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    return ImmutableMap.of(
        new TupleTag<Row>("successfulRows") {}, successfulRows,
        new TupleTag<PostgreSqlWriteError>("failedRows") {}, failedRows);
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {}
}
