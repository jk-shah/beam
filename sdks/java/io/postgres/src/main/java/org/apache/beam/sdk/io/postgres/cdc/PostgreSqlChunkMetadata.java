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
import org.joda.time.Instant;

/**
 * Metadata descriptor for an immutable staged CDC chunk file written to Cloud Storage (GCS / S3).
 */
@DefaultCoder(SerializableCoder.class)
public class PostgreSqlChunkMetadata implements Serializable {

  private final String resourceUri;
  private final String slotName;
  private final long startLsn;
  private final long endLsn;
  private final long recordCount;
  private final long fileSizeBytes;
  private final PostgreSqlStagingFormat format;
  private final Instant creationTimestamp;

  public PostgreSqlChunkMetadata(
      String resourceUri,
      String slotName,
      long startLsn,
      long endLsn,
      long recordCount,
      long fileSizeBytes,
      PostgreSqlStagingFormat format,
      Instant creationTimestamp) {
    this.resourceUri = resourceUri;
    this.slotName = slotName;
    this.startLsn = startLsn;
    this.endLsn = endLsn;
    this.recordCount = recordCount;
    this.fileSizeBytes = fileSizeBytes;
    this.format = format;
    this.creationTimestamp = creationTimestamp;
  }

  public String getResourceUri() {
    return resourceUri;
  }

  public String getSlotName() {
    return slotName;
  }

  public long getStartLsn() {
    return startLsn;
  }

  public long getEndLsn() {
    return endLsn;
  }

  public long getRecordCount() {
    return recordCount;
  }

  public long getFileSizeBytes() {
    return fileSizeBytes;
  }

  public PostgreSqlStagingFormat getFormat() {
    return format;
  }

  public Instant getCreationTimestamp() {
    return creationTimestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PostgreSqlChunkMetadata)) return false;
    PostgreSqlChunkMetadata that = (PostgreSqlChunkMetadata) o;
    return startLsn == that.startLsn
        && endLsn == that.endLsn
        && recordCount == that.recordCount
        && fileSizeBytes == that.fileSizeBytes
        && Objects.equals(resourceUri, that.resourceUri)
        && Objects.equals(slotName, that.slotName)
        && format == that.format
        && Objects.equals(creationTimestamp, that.creationTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        resourceUri,
        slotName,
        startLsn,
        endLsn,
        recordCount,
        fileSizeBytes,
        format,
        creationTimestamp);
  }

  @Override
  public String toString() {
    return "PostgreSqlChunkMetadata{"
        + "uri='"
        + resourceUri
        + '\''
        + ", slot='"
        + slotName
        + '\''
        + ", lsn=["
        + Long.toHexString(startLsn)
        + ".."
        + Long.toHexString(endLsn)
        + "]"
        + ", records="
        + recordCount
        + ", bytes="
        + fileSizeBytes
        + ", format="
        + format
        + ", time="
        + creationTimestamp
        + '}';
  }
}
