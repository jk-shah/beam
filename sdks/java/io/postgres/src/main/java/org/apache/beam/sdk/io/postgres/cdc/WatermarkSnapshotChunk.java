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

import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/** Descriptor for a lock-free incremental watermark snapshot chunk slice. */
@DefaultCoder(SerializableCoder.class)
public class WatermarkSnapshotChunk implements Serializable {

  private final String chunkId;
  private final String tableName;
  private final String primaryKeyColumn;
  private final long minId;
  private final long maxId;

  public WatermarkSnapshotChunk(
      String chunkId, String tableName, String primaryKeyColumn, long minId, long maxId) {
    this.chunkId = chunkId;
    this.tableName = tableName;
    this.primaryKeyColumn = primaryKeyColumn;
    this.minId = minId;
    this.maxId = maxId;
  }

  public String getChunkId() {
    return chunkId;
  }

  public String getTableName() {
    return tableName;
  }

  public String getPrimaryKeyColumn() {
    return primaryKeyColumn;
  }

  public long getMinId() {
    return minId;
  }

  public long getMaxId() {
    return maxId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof WatermarkSnapshotChunk)) return false;
    WatermarkSnapshotChunk that = (WatermarkSnapshotChunk) o;
    return minId == that.minId
        && maxId == that.maxId
        && Objects.equals(chunkId, that.chunkId)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(primaryKeyColumn, that.primaryKeyColumn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(chunkId, tableName, primaryKeyColumn, minId, maxId);
  }

  @Override
  public String toString() {
    return "WatermarkSnapshotChunk{"
        + "chunkId='"
        + chunkId
        + '\''
        + ", table='"
        + tableName
        + '\''
        + ", pk='"
        + primaryKeyColumn
        + '\''
        + ", range=["
        + minId
        + ".."
        + maxId
        + "]"
        + '}';
  }
}
