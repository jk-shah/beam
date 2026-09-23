// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package postgresio

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

func init() {
	beam.RegisterDoFn(&fromArrowBatchesFn{})
}

// fromArrowBatchesFn is a Beam DoFn that unpacks Arrow IPC RecordBatches into individual ChangeEvents.
type fromArrowBatchesFn struct{}

func (fn *fromArrowBatchesFn) ProcessElement(ctx context.Context, batch ArrowBatchRecord, emit func(ChangeEvent)) error {
	if len(batch.Payload) > MaxAllowedIPCMessageBytes {
		return fmt.Errorf("postgresio: Arrow IPC payload exceeds safety limit (%d > %d bytes)",
			len(batch.Payload), MaxAllowedIPCMessageBytes)
	}

	reader, err := ipc.NewReader(bytes.NewReader(batch.Payload))
	if err != nil {
		return fmt.Errorf("failed to initialize Arrow IPC stream reader: %w", err)
	}
	defer reader.Release()

	var firstLSN uint64
	if s, ok := batch.Metadata["first_lsn"]; ok {
		firstLSN, _ = strconv.ParseUint(s, 10, 64)
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading Arrow record batch: %w", err)
		}

		numRows := int(record.NumRows())
		numCols := int(record.NumCols())

		for r := 0; r < numRows; r++ {
			afterMap := make(map[string]any, numCols)
			for c := 0; c < numCols; c++ {
				colName := record.ColumnName(c)
				arr := record.Column(c)

				if arr.IsNull(r) {
					afterMap[colName] = nil
					continue
				}

				afterMap[colName] = extractArrayValue(arr, r)
			}

			event := ChangeEvent{
				Operation: OpInsert,
				Schema:    batch.Namespace,
				Table:     batch.TableName,
				LSN:       firstLSN,
				After:     afterMap,
			}
			emit(event)
		}

		record.Release()
	}

	return nil
}

func extractArrayValue(arr arrow.Array, idx int) any {
	switch a := arr.(type) {
	case *array.Boolean:
		return a.Value(idx)
	case *array.Int16:
		return a.Value(idx)
	case *array.Int32:
		return a.Value(idx)
	case *array.Int64:
		return a.Value(idx)
	case *array.Float32:
		return a.Value(idx)
	case *array.Float64:
		return a.Value(idx)
	case *array.String:
		return a.Value(idx)
	case *array.Binary:
		return a.Value(idx)
	case *array.Timestamp:
		us := int64(a.Value(idx))
		return time.UnixMicro(us).UTC()
	case *array.Date32:
		days := int64(a.Value(idx))
		return time.Unix(days*86400, 0).UTC()
	default:
		return a.ValueStr(idx)
	}
}

// FromArrowBatches converts a PCollection of ArrowBatchRecord back into a PCollection of ChangeEvents.
func FromArrowBatches(s beam.Scope, col beam.PCollection) beam.PCollection {
	s = s.Scope("postgresio.FromArrowBatches")
	return beam.ParDo(s, &fromArrowBatchesFn{}, col)
}
