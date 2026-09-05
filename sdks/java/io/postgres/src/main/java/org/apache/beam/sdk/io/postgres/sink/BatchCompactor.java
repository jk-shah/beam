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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.values.Row;

/**
 * Compacts and canonically orders in-flight streaming micro-batches by primary key, eliminating
 * redundant intermediate updates and preventing cross-worker deadlocks ({@code SQLState 40P01}).
 */
public class BatchCompactor {

  /**
   * Deduplicates records by primary key using Last-Write-Wins (LWW) semantics, and sorts the
   * remaining rows canonically by primary key values.
   */
  public static List<Row> compactAndSort(Iterable<Row> inputRows, List<String> primaryKeyColumns) {
    if (primaryKeyColumns.isEmpty()) {
      List<Row> result = new ArrayList<>();
      inputRows.forEach(result::add);
      return result;
    }

    // Deduplicate in-batch using Last-Write-Wins (LWW)
    Map<String, Row> compactedMap = new LinkedHashMap<>();
    for (Row row : inputRows) {
      String pkKey = extractCompositeKey(row, primaryKeyColumns);
      compactedMap.put(pkKey, row);
    }

    List<Row> sortedRows = new ArrayList<>(compactedMap.values());

    // Sort canonically by primary key values to ensure deterministic lock acquisition order
    sortedRows.sort(
        (r1, r2) -> {
          for (String pkCol : primaryKeyColumns) {
            Object v1 = r1.getValue(pkCol);
            Object v2 = r2.getValue(pkCol);
            if (v1 == null && v2 == null) continue;
            if (v1 == null) return -1;
            if (v2 == null) return 1;

            if (v1 instanceof Comparable && v2 instanceof Comparable) {
              @SuppressWarnings("unchecked")
              int cmp = ((Comparable<Object>) v1).compareTo(v2);
              if (cmp != 0) return cmp;
            } else {
              int cmp = v1.toString().compareTo(v2.toString());
              if (cmp != 0) return cmp;
            }
          }
          return 0;
        });

    return sortedRows;
  }

  private static String extractCompositeKey(Row row, List<String> primaryKeyColumns) {
    StringBuilder sb = new StringBuilder();
    for (String col : primaryKeyColumns) {
      Object val = row.getValue(col);
      sb.append(val != null ? val.toString() : "null").append("|#|");
    }
    return sb.toString();
  }
}
