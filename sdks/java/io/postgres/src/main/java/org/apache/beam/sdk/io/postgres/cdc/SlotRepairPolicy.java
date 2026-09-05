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
 * Strategy policy for handling stale, lost, or invalidated PostgreSQL logical replication slots.
 */
public enum SlotRepairPolicy {
  /**
   * Fails the pipeline immediately with a descriptive diagnostic error when a replication slot is
   * missing, lost, or invalid.
   */
  FAIL_FAST,

  /**
   * Automatically recreates the replication slot at the database current write LSN and initiates a
   * non-blocking dual-stream online catch-up rebuild. Live streaming starts immediately while
   * background watermark chunking reconciles missing data.
   */
  AUTOMATIC_RECREATE_AND_CATCHUP,

  /**
   * Automatically recreates the replication slot at the current write LSN and streams live CDC
   * only, without performing historical backfill.
   */
  RECREATE_SLOT_LIVE_ONLY
}
