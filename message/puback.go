// Copyright (c) 2014 The SurgeMQ Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package message

import "fmt"

// PubackMessage is a PUBACK packet, the response to a PUBLISH Packet with QoS level 1.
type PubackMessage struct {
	header
}

var _ Message = (*PubackMessage)(nil)

// NewPubackMessage creates a new PUBACK message.
func NewPubackMessage() *PubackMessage {
	msg := &PubackMessage{}
	msg.SetType(PUBACK)

	return msg
}

// String implements Stringer interface.
func (m PubackMessage) String() string {
	return fmt.Sprintf("%s, Packet ID=%d", m.header, m.packetID)
}

// Len returns the message length.
func (m *PubackMessage) Len() int {
	if !m.dirty {
		return len(m.dbuf)
	}

	ml := m.msglen()

	if err := m.SetRemainingLength(int32(ml)); err != nil {
		return 0
	}

	return m.header.msglen() + ml
}

// Decode decodes the message.
func (m *PubackMessage) Decode(src []byte) (int, error) {
	total := 0

	n, err := m.header.decode(src[total:])
	total += n
	if err != nil {
		return total, err
	}

	//this.packetId = binary.BigEndian.Uint16(src[total:])
	m.packetID = src[total : total+2]
	total += 2

	m.dirty = false

	return total, nil
}

// Encode encodes the message.
func (m *PubackMessage) Encode(dst []byte) (int, error) {
	if !m.dirty {
		if len(dst) < len(m.dbuf) {
			return 0, fmt.Errorf("puback/Encode: Insufficient buffer size. Expecting %d, got %d", len(m.dbuf), len(dst))
		}

		return copy(dst, m.dbuf), nil
	}

	hl := m.header.msglen()
	ml := m.msglen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("puback/Encode: Insufficient buffer size. Expecting %d, got %d", hl+ml, len(dst))
	}

	if err := m.SetRemainingLength(int32(ml)); err != nil {
		return 0, err
	}

	total := 0

	n, err := m.header.encode(dst[total:])
	total += n
	if err != nil {
		return total, err
	}

	if copy(dst[total:total+2], m.packetID) != 2 {
		dst[total], dst[total+1] = 0, 0
	}
	total += 2

	return total, nil
}

func (m *PubackMessage) msglen() int {
	// packet ID
	return 2
}
