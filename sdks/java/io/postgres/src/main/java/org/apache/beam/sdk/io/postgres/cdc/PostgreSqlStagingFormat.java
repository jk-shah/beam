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

/**
 * Supported container and serialization formats for object-store staged Change Data Capture (CDC)
 * micro-batches.
 */
public enum PostgreSqlStagingFormat {
  /**
   * Structured Apache Avro format with embedded schema and 16-byte sync markers. Standard for
   * multi-language portability and liquid Splittable DoFn splitting.
   */
  RAW_AVRO,

  /**
   * High-throughput raw PostgreSQL protocol framing with 16-byte sync markers (.pgframe-v2). Zero
   * CPU row deserialization on Tier 1 offloader, scaling past 160,000 ops/sec.
   */
  RAW_PG_FRAMES
}
