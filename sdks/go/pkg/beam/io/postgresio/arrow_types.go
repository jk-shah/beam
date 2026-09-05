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
	"encoding/binary"
	"fmt"
	"io"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

const (
	// DefaultMaxBatchRows is the default maximum number of rows in an Arrow RecordBatch.
	DefaultMaxBatchRows = 5000

	// DefaultMaxBatchBytes is the default maximum uncompressed bytes before batch flush (4MB).
	DefaultMaxBatchBytes = 4 * 1024 * 1024

	// DefaultMaxActiveSchemas is the default maximum concurrent table buffers in the pool.
	DefaultMaxActiveSchemas = 64

	// MaxAllowedIPCMessageBytes is the maximum allowed byte length for an Arrow IPC stream message (64MB).
	MaxAllowedIPCMessageBytes = 64 * 1024 * 1024
)

func init() {
	beam.RegisterType(reflect.TypeOf((*ArrowBatchRecord)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*ArrowBatchOptions)(nil)).Elem())

	beam.RegisterCoder(
		reflect.TypeOf((*ArrowBatchRecord)(nil)).Elem(),
		encodeArrowBatchRecord,
		decodeArrowBatchRecord,
	)
}

// ArrowBatchOptions configures the batch flush thresholds and memory boundaries.
type ArrowBatchOptions struct {
	MaxBatchRows     int `beam:"max_batch_rows" json:"max_batch_rows"`
	MaxBatchBytes    int `beam:"max_batch_bytes" json:"max_batch_bytes"`
	MaxActiveSchemas int `beam:"max_active_schemas" json:"max_active_schemas"`
}

// NewDefaultArrowBatchOptions initializes standard production batching thresholds.
func NewDefaultArrowBatchOptions() ArrowBatchOptions {
	return ArrowBatchOptions{
		MaxBatchRows:     DefaultMaxBatchRows,
		MaxBatchBytes:    DefaultMaxBatchBytes,
		MaxActiveSchemas: DefaultMaxActiveSchemas,
	}
}

// ArrowBatchOption is a functional configuration option for ArrowBatchOptions.
type ArrowBatchOption func(*ArrowBatchOptions)

// WithArrowMaxBatchRows sets the row count threshold for triggering batch flushes.
func WithArrowMaxBatchRows(rows int) ArrowBatchOption {
	return func(o *ArrowBatchOptions) {
		if rows > 0 {
			o.MaxBatchRows = rows
		}
	}
}

// WithArrowMaxBatchBytes sets the memory byte threshold for triggering batch flushes.
func WithArrowMaxBatchBytes(b int) ArrowBatchOption {
	return func(o *ArrowBatchOptions) {
		if b > 0 {
			o.MaxBatchBytes = b
		}
	}
}

// WithArrowMaxActiveSchemas sets the maximum concurrent table schemas in the buffer pool.
func WithArrowMaxActiveSchemas(schemas int) ArrowBatchOption {
	return func(o *ArrowBatchOptions) {
		if schemas > 0 {
			o.MaxActiveSchemas = schemas
		}
	}
}

// ArrowBatchRecord encapsulates the serialized Arrow IPC RecordBatch stream payload and metadata.
type ArrowBatchRecord struct {
	SchemaID  string            `beam:"schema_id" json:"schema_id"`
	Namespace string            `beam:"namespace" json:"namespace"`
	TableName string            `beam:"table_name" json:"table_name"`
	RowCount  int64             `beam:"row_count" json:"row_count"`
	Payload   []byte            `beam:"payload" json:"payload"`
	Metadata  map[string]string `beam:"metadata" json:"metadata,omitempty"`
}

// FullTableName returns "namespace.table_name" or "table_name" if namespace is empty.
func (r ArrowBatchRecord) FullTableName() string {
	if r.Namespace == "" {
		return r.TableName
	}
	return fmt.Sprintf("%s.%s", r.Namespace, r.TableName)
}

// encodeArrowBatchRecord serializes ArrowBatchRecord into a compact binary format,
// bypassing the Beam reflection JSON coder and eliminating base64 payload bloat.
func encodeArrowBatchRecord(in ArrowBatchRecord) ([]byte, error) {
	var buf bytes.Buffer

	// 1. Write SchemaID (uint16 length + bytes)
	if err := writeString(&buf, in.SchemaID); err != nil {
		return nil, err
	}

	// 2. Write Namespace (uint16 length + bytes)
	if err := writeString(&buf, in.Namespace); err != nil {
		return nil, err
	}

	// 3. Write TableName (uint16 length + bytes)
	if err := writeString(&buf, in.TableName); err != nil {
		return nil, err
	}

	// 4. Write RowCount (int64 binary)
	if err := binary.Write(&buf, binary.BigEndian, in.RowCount); err != nil {
		return nil, fmt.Errorf("failed to write row count: %w", err)
	}

	// 5. Write Payload (uint32 length + raw bytes)
	payloadLen := uint32(len(in.Payload))
	if err := binary.Write(&buf, binary.BigEndian, payloadLen); err != nil {
		return nil, fmt.Errorf("failed to write payload length: %w", err)
	}
	if payloadLen > 0 {
		if _, err := buf.Write(in.Payload); err != nil {
			return nil, fmt.Errorf("failed to write payload: %w", err)
		}
	}

	// 6. Write Metadata (uint16 count + key/val pairs)
	metaCount := uint16(len(in.Metadata))
	if err := binary.Write(&buf, binary.BigEndian, metaCount); err != nil {
		return nil, fmt.Errorf("failed to write metadata count: %w", err)
	}
	for k, v := range in.Metadata {
		if err := writeString(&buf, k); err != nil {
			return nil, err
		}
		if err := writeString(&buf, v); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// decodeArrowBatchRecord deserializes binary bytes into an ArrowBatchRecord.
func decodeArrowBatchRecord(in []byte) (ArrowBatchRecord, error) {
	r := bytes.NewReader(in)
	var out ArrowBatchRecord

	var err error
	if out.SchemaID, err = readString(r); err != nil {
		return out, err
	}
	if out.Namespace, err = readString(r); err != nil {
		return out, err
	}
	if out.TableName, err = readString(r); err != nil {
		return out, err
	}

	if err := binary.Read(r, binary.BigEndian, &out.RowCount); err != nil {
		return out, fmt.Errorf("failed to read row count: %w", err)
	}

	var payloadLen uint32
	if err := binary.Read(r, binary.BigEndian, &payloadLen); err != nil {
		return out, fmt.Errorf("failed to read payload length: %w", err)
	}
	if payloadLen > MaxAllowedIPCMessageBytes {
		return out, fmt.Errorf("payload length %d exceeds maximum allowed %d bytes", payloadLen, MaxAllowedIPCMessageBytes)
	}
	if payloadLen > 0 {
		out.Payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(r, out.Payload); err != nil {
			return out, fmt.Errorf("failed to read payload: %w", err)
		}
	}

	var metaCount uint16
	if err := binary.Read(r, binary.BigEndian, &metaCount); err != nil {
		return out, fmt.Errorf("failed to read metadata count: %w", err)
	}
	if metaCount > 0 {
		out.Metadata = make(map[string]string, metaCount)
		for i := 0; i < int(metaCount); i++ {
			k, err := readString(r)
			if err != nil {
				return out, err
			}
			v, err := readString(r)
			if err != nil {
				return out, err
			}
			out.Metadata[k] = v
		}
	}

	return out, nil
}

func writeString(w io.Writer, s string) error {
	strBytes := []byte(s)
	if len(strBytes) > 65535 {
		return fmt.Errorf("string length exceeds uint16 maximum (%d)", len(strBytes))
	}
	length := uint16(len(strBytes))
	if err := binary.Write(w, binary.BigEndian, length); err != nil {
		return fmt.Errorf("failed to write string length: %w", err)
	}
	if length > 0 {
		if _, err := w.Write(strBytes); err != nil {
			return fmt.Errorf("failed to write string data: %w", err)
		}
	}
	return nil
}

func readString(r io.Reader) (string, error) {
	var length uint16
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return "", fmt.Errorf("failed to read string length: %w", err)
	}
	if length == 0 {
		return "", nil
	}
	b := make([]byte, length)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", fmt.Errorf("failed to read string data: %w", err)
	}
	return string(b), nil
}
