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

import org.apache.beam.sdk.io.postgres.cdc.ChangeEvent;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/**
 * Composite PTransform performing downstream tombstone anti-join reconciliation
 * ($\mathcal{K}_{target} \setminus \mathcal{K}_{source}$) during disaster recovery or slot repair.
 *
 * <p>Identifies records that exist in the target replica/sink but are absent from the source
 * database, emitting synthetic {@link ChangeEvent.OpType#DELETE} tombstone events to achieve
 * convergence.
 */
public class PostgreSqlSinkReconciler {

  public static final TupleTag<Row> TARGET_TAG = new TupleTag<Row>() {};
  public static final TupleTag<Row> SOURCE_TAG = new TupleTag<Row>() {};

  /**
   * Builds an anti-join reconciliation transform comparing target sink rows against current source
   * rows.
   *
   * @param targetRows PCollection of KV(primaryKey, row) from target sink/replica
   * @param sourceRows PCollection of KV(primaryKey, row) from current source database snapshot
   * @param schemaName Target schema name
   * @param tableName Target table name
   * @return PCollection of synthetic DELETE {@link ChangeEvent} tombstones
   */
  public static PCollection<ChangeEvent<Row>> reconcileTombstones(
      PCollection<KV<String, Row>> targetRows,
      PCollection<KV<String, Row>> sourceRows,
      String schemaName,
      String tableName) {

    return KeyedPCollectionTuple.of(TARGET_TAG, targetRows)
        .and(SOURCE_TAG, sourceRows)
        .apply("CoGroupTargetAndSource", CoGroupByKey.create())
        .apply(
            "DetectOrphanedTargetRows",
            org.apache.beam.sdk.transforms.ParDo.of(
                new DoFn<KV<String, CoGbkResult>, ChangeEvent<Row>>() {
                  @ProcessElement
                  public void processElement(
                      @Element KV<String, CoGbkResult> element,
                      OutputReceiver<ChangeEvent<Row>> receiver) {
                    CoGbkResult result = element.getValue();
                    Iterable<Row> sourceIter = result.getAll(SOURCE_TAG);
                    Iterable<Row> targetIter = result.getAll(TARGET_TAG);

                    // If present in target but absent in source, generate DELETE tombstone
                    if (!sourceIter.iterator().hasNext() && targetIter.iterator().hasNext()) {
                      for (Row orphanedTargetRow : targetIter) {
                        ChangeEvent<Row> tombstone =
                            new ChangeEvent<>(
                                ChangeEvent.OpType.DELETE,
                                schemaName,
                                tableName,
                                0L,
                                0L,
                                Instant.now(),
                                orphanedTargetRow,
                                null,
                                null);
                        receiver.output(tombstone);
                      }
                    }
                  }
                }))
        .setCoder(org.apache.beam.sdk.coders.SerializableCoder.of((Class) ChangeEvent.class));
  }
}
