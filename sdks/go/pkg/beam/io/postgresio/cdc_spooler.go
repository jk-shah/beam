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
	"encoding/json"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
)

// MessageType indicates the stream transaction message nature.
type MessageType int

const (
	// MessageTypeMutation represents an in-flight uncommitted row mutation.
	MessageTypeMutation MessageType = iota
	// MessageTypeCommit represents a stream transaction commit.
	MessageTypeCommit
	// MessageTypeAbort represents a stream transaction abort.
	MessageTypeAbort
)

func init() {
	beam.RegisterType(reflect.TypeOf((*inFlightTransactionSpoolerFn)(nil)).Elem())
	beam.RegisterCoder(
		reflect.TypeOf((*TransactionMessage)(nil)).Elem(),
		encodeTxMessage,
		decodeTxMessage,
	)
}

func encodeTxMessage(in TransactionMessage) ([]byte, error) {
	return json.Marshal(in)
}

func decodeTxMessage(in []byte) (TransactionMessage, error) {
	var out TransactionMessage
	err := json.Unmarshal(in, &out)
	return out, err
}

// TransactionMessage wraps an in-flight mutation or transaction control signal.
type TransactionMessage struct {
	TransactionID uint32       `beam:"transaction_id" json:"transaction_id"`
	Type          MessageType  `beam:"type" json:"type"`
	Event         *ChangeEvent `beam:"event" json:"event,omitempty"`
}

// OfMutation creates a mutation TransactionMessage.
func OfMutation(xid uint32, event ChangeEvent) TransactionMessage {
	return TransactionMessage{
		TransactionID: xid,
		Type:          MessageTypeMutation,
		Event:         &event,
	}
}

// OfCommit creates a commit TransactionMessage.
func OfCommit(xid uint32) TransactionMessage {
	return TransactionMessage{
		TransactionID: xid,
		Type:          MessageTypeCommit,
	}
}

// OfAbort creates an abort TransactionMessage.
func OfAbort(xid uint32) TransactionMessage {
	return TransactionMessage{
		TransactionID: xid,
		Type:          MessageTypeAbort,
	}
}

type inFlightTransactionSpoolerFn struct {
	SpooledEvents state.Bag[ChangeEvent]
}

func newInFlightTransactionSpoolerFn() *inFlightTransactionSpoolerFn {
	return &inFlightTransactionSpoolerFn{
		SpooledEvents: state.MakeBagState[ChangeEvent]("spooled_mutations"),
	}
}

func (fn *inFlightTransactionSpoolerFn) ProcessElement(sp state.Provider, xid uint32, msg TransactionMessage, emit func(ChangeEvent)) error {
	switch msg.Type {
	case MessageTypeMutation:
		if msg.Event != nil {
			_ = fn.SpooledEvents.Add(sp, *msg.Event)
		}
	case MessageTypeCommit:
		events, _, err := fn.SpooledEvents.Read(sp)
		if err == nil {
			for _, e := range events {
				emit(e)
			}
		}
		_ = fn.SpooledEvents.Clear(sp)
	case MessageTypeAbort:
		_ = fn.SpooledEvents.Clear(sp)
	}
	return nil
}
