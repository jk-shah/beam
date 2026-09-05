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

import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.postgres.PostgreSqlDataSourceConfiguration;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;

/**
 * Top-level {@link PTransform} for writing Beam {@link Row} collections to PostgreSQL with high
 * throughput, deterministic primary-key ordering, and Dead-Letter Queue (DLQ) support.
 */
@AutoValue
public abstract class PostgreSqlWrite extends PTransform<PCollection<Row>, PostgreSqlWriteResult> {

  public enum WriteMode {
    STREAMING_UPSERT_UNNEST,
    APPEND_ONLY_COPY,
    STAGED_COPY_UPSERT
  }

  public abstract PostgreSqlDataSourceConfiguration getDataSourceConfiguration();

  public abstract SerializableFunction<Row, String> getTableFn();

  public abstract WriteMode getWriteMode();

  public abstract List<String> getPrimaryKeyColumns();

  public abstract int getBatchSize();

  public abstract Duration getMaxBufferingDuration();

  public abstract int getNumShards();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_PostgreSqlWrite.Builder()
        .setWriteMode(WriteMode.STREAMING_UPSERT_UNNEST)
        .setPrimaryKeyColumns(Collections.emptyList())
        .setBatchSize(1000)
        .setMaxBufferingDuration(Duration.standardSeconds(2))
        .setNumShards(0);
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setDataSourceConfiguration(PostgreSqlDataSourceConfiguration config);

    public abstract Builder setTableFn(SerializableFunction<Row, String> tableFn);

    public abstract Builder setWriteMode(WriteMode writeMode);

    public abstract Builder setPrimaryKeyColumns(List<String> primaryKeyColumns);

    public abstract Builder setBatchSize(int batchSize);

    public abstract Builder setMaxBufferingDuration(Duration duration);

    public abstract Builder setNumShards(int numShards);

    public Builder withDataSourceConfiguration(PostgreSqlDataSourceConfiguration config) {
      return setDataSourceConfiguration(config);
    }

    public Builder to(String tableName) {
      return setTableFn((SerializableFunction<Row, String>) input -> tableName);
    }

    public Builder to(SerializableFunction<Row, String> tableFn) {
      return setTableFn(tableFn);
    }

    public Builder withWriteMode(WriteMode mode) {
      return setWriteMode(mode);
    }

    public Builder withPrimaryKeyColumns(List<String> primaryKeyColumns) {
      return setPrimaryKeyColumns(primaryKeyColumns);
    }

    public Builder withBatchSize(int batchSize) {
      return setBatchSize(batchSize);
    }

    public Builder withMaxBufferingDuration(Duration duration) {
      return setMaxBufferingDuration(duration);
    }

    public Builder withNumShards(int numShards) {
      return setNumShards(numShards);
    }

    public Builder withReplicationOriginName(String originName) {
      if (getDataSourceConfiguration() != null) {
        return setDataSourceConfiguration(
            getDataSourceConfiguration().withReplicationOriginName(originName));
      }
      return this;
    }

    public abstract PostgreSqlDataSourceConfiguration getDataSourceConfiguration();

    public abstract PostgreSqlWrite build();
  }

  public PostgreSqlWrite withDataSourceConfiguration(PostgreSqlDataSourceConfiguration config) {
    return toBuilder().setDataSourceConfiguration(config).build();
  }

  public PostgreSqlWrite withReplicationOriginName(String originName) {
    return toBuilder()
        .setDataSourceConfiguration(
            getDataSourceConfiguration().withReplicationOriginName(originName))
        .build();
  }

  public PostgreSqlWrite to(String tableName) {
    return toBuilder().setTableFn((SerializableFunction<Row, String>) input -> tableName).build();
  }

  public PostgreSqlWrite to(SerializableFunction<Row, String> tableFn) {
    return toBuilder().setTableFn(tableFn).build();
  }

  public PostgreSqlWrite withWriteMode(WriteMode mode) {
    return toBuilder().setWriteMode(mode).build();
  }

  public PostgreSqlWrite withPrimaryKeyColumns(List<String> primaryKeyColumns) {
    return toBuilder().setPrimaryKeyColumns(primaryKeyColumns).build();
  }

  public PostgreSqlWrite withBatchSize(int batchSize) {
    return toBuilder().setBatchSize(batchSize).build();
  }

  public PostgreSqlWrite withMaxBufferingDuration(Duration duration) {
    return toBuilder().setMaxBufferingDuration(duration).build();
  }

  public PostgreSqlWrite withNumShards(int numShards) {
    return toBuilder().setNumShards(numShards).build();
  }

  @Override
  public PostgreSqlWriteResult expand(PCollection<Row> input) {
    TupleTag<Row> successTag = new TupleTag<Row>("successfulRows") {};
    TupleTag<PostgreSqlWriteError> dlqTag = new TupleTag<PostgreSqlWriteError>("failedRows") {};

    org.apache.beam.sdk.coders.Coder<Row> rowCoder =
        input.hasSchema()
            ? org.apache.beam.sdk.coders.RowCoder.of(input.getSchema())
            : input.getCoder();

    // Map each Row to KV<TableName, Row>
    PCollection<KV<String, Row>> keyedRows =
        input
            .apply(
                "AssignDestinationTable",
                ParDo.of(
                    new DoFn<Row, KV<String, Row>>() {
                      @ProcessElement
                      public void processElement(
                          @Element Row row, OutputReceiver<KV<String, Row>> receiver) {
                        String table = getTableFn().apply(row);
                        receiver.output(KV.of(table, row));
                      }
                    }))
            .setCoder(
                org.apache.beam.sdk.coders.KvCoder.of(
                    org.apache.beam.sdk.coders.StringUtf8Coder.of(), rowCoder));

    // Micro-batch grouping with time-based flushing
    PCollection<KV<String, Iterable<Row>>> batchedRows =
        keyedRows.apply(
            "GroupIntoBatches",
            GroupIntoBatches.<String, Row>ofSize(getBatchSize())
                .withMaxBufferingDuration(getMaxBufferingDuration()));

    DoFn<KV<String, Iterable<Row>>, Row> writerDoFn;
    switch (getWriteMode()) {
      case APPEND_ONLY_COPY:
        writerDoFn =
            new CopyManagerBinaryWriterDoFn(getDataSourceConfiguration(), successTag, dlqTag);
        break;
      case STAGED_COPY_UPSERT:
        writerDoFn =
            new StagedCopyUpsertWriterDoFn(
                getDataSourceConfiguration(), getPrimaryKeyColumns(), successTag, dlqTag);
        break;
      case STREAMING_UPSERT_UNNEST:
      default:
        writerDoFn =
            new UnnestArrayUpsertWriterDoFn(
                getDataSourceConfiguration(), getPrimaryKeyColumns(), successTag, dlqTag);
        break;
    }

    PCollectionTuple writeOutputs =
        batchedRows.apply(
            "ExecutePostgresWrite",
            ParDo.of(writerDoFn).withOutputTags(successTag, TupleTagList.of(dlqTag)));

    PCollection<Row> successCollection = writeOutputs.get(successTag);
    if (input.hasSchema()) {
      successCollection.setRowSchema(input.getSchema());
    } else {
      successCollection.setCoder(rowCoder);
    }

    PCollection<PostgreSqlWriteError> dlqCollection =
        writeOutputs
            .get(dlqTag)
            .setCoder(org.apache.beam.sdk.coders.SerializableCoder.of(PostgreSqlWriteError.class));

    return PostgreSqlWriteResult.of(input.getPipeline(), successCollection, dlqCollection);
  }
}
