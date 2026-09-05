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

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * Composite output of {@link PostgreSqlReadCDC}, providing access to the main CDC change event
 * stream as well as the Dead-Letter Queue (DLQ) for unparseable or constraint-violating records.
 */
public class PostgreSqlCdcResult implements POutput {

  private final Pipeline pipeline;
  private final PCollection<ChangeEvent<Row>> output;
  private final PCollection<Row> deadLetterQueue;
  private final TupleTag<ChangeEvent<Row>> outputTag;
  private final TupleTag<Row> dlqTag;

  public PostgreSqlCdcResult(
      Pipeline pipeline,
      PCollection<ChangeEvent<Row>> output,
      PCollection<Row> deadLetterQueue,
      TupleTag<ChangeEvent<Row>> outputTag,
      TupleTag<Row> dlqTag) {
    this.pipeline = pipeline;
    this.output = output;
    this.deadLetterQueue = deadLetterQueue;
    this.outputTag = outputTag;
    this.dlqTag = dlqTag;
  }

  public static PostgreSqlCdcResult of(
      Pipeline pipeline,
      PCollection<ChangeEvent<Row>> output,
      PCollection<Row> deadLetterQueue,
      TupleTag<ChangeEvent<Row>> outputTag,
      TupleTag<Row> dlqTag) {
    return new PostgreSqlCdcResult(pipeline, output, deadLetterQueue, outputTag, dlqTag);
  }

  /** Returns the primary stream of successfully parsed CDC change events. */
  public PCollection<ChangeEvent<Row>> getOutput() {
    return output;
  }

  /** Returns the dead-letter queue stream containing unparseable or error records. */
  public PCollection<Row> getDeadLetterQueue() {
    return deadLetterQueue;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    return ImmutableMap.of(
        outputTag, output,
        dlqTag, deadLetterQueue);
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, org.apache.beam.sdk.values.PInput input, PTransform<?, ?> transform) {}
}
