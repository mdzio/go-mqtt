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

// SubackMessage ia a SUBACK packet, sent by the Server to the Client to confirm receipt and processing
// of a SUBSCRIBE Packet.
//
// A SUBACK Packet contains a list of return codes, that specify the maximum QoS level
// that was granted in each Subscription that was requested by the SUBSCRIBE.
type SubackMessage struct {
	header

	returnCodes []byte
}

var _ Message = (*SubackMessage)(nil)

// NewSubackMessage creates a new SUBACK message.
func NewSubackMessage() *SubackMessage {
	msg := &SubackMessage{}
	msg.SetType(SUBACK)

	return msg
}

// String returns a string representation of the message.
func (m SubackMessage) String() string {
	return fmt.Sprintf("%s, Packet ID=%d, Return Codes=%v", m.header, m.PacketID(), m.returnCodes)
}

// ReturnCodes returns the list of QoS returns from the subscriptions sent in the SUBSCRIBE message.
func (m *SubackMessage) ReturnCodes() []byte {
	return m.returnCodes
}

// AddReturnCodes sets the list of QoS returns from the subscriptions sent in the SUBSCRIBE message.
// An error is returned if any of the QoS values are not valid.
func (m *SubackMessage) AddReturnCodes(ret []byte) error {
	for _, c := range ret {
		if c != QosAtMostOnce && c != QosAtLeastOnce && c != QosExactlyOnce && c != QosFailure {
			return fmt.Errorf("suback/AddReturnCode: Invalid return code %d. Must be 0, 1, 2, 0x80", c)
		}

		m.returnCodes = append(m.returnCodes, c)
	}

	m.dirty = true

	return nil
}

// AddReturnCode adds a single QoS return value.
func (m *SubackMessage) AddReturnCode(ret byte) error {
	return m.AddReturnCodes([]byte{ret})
}

// Len returns the length of the message.
func (m *SubackMessage) Len() int {
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
func (m *SubackMessage) Decode(src []byte) (int, error) {
	total := 0

	hn, err := m.header.decode(src[total:])
	total += hn
	if err != nil {
		return total, err
	}

	//this.packetId = binary.BigEndian.Uint16(src[total:])
	m.packetID = src[total : total+2]
	total += 2

	l := int(m.remlen) - (total - hn)
	m.returnCodes = src[total : total+l]
	total += len(m.returnCodes)

	for i, code := range m.returnCodes {
		if code != 0x00 && code != 0x01 && code != 0x02 && code != 0x80 {
			return total, fmt.Errorf("suback/Decode: Invalid return code %d for topic %d", code, i)
		}
	}

	m.dirty = false

	return total, nil
}

// Encode encodes the message.
func (m *SubackMessage) Encode(dst []byte) (int, error) {
	if !m.dirty {
		if len(dst) < len(m.dbuf) {
			return 0, fmt.Errorf("suback/Encode: Insufficient buffer size. Expecting %d, got %d", len(m.dbuf), len(dst))
		}

		return copy(dst, m.dbuf), nil
	}

	for i, code := range m.returnCodes {
		if code != 0x00 && code != 0x01 && code != 0x02 && code != 0x80 {
			return 0, fmt.Errorf("suback/Encode: Invalid return code %d for topic %d", code, i)
		}
	}

	hl := m.header.msglen()
	ml := m.msglen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("suback/Encode: Insufficient buffer size. Expecting %d, got %d", hl+ml, len(dst))
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

	copy(dst[total:], m.returnCodes)
	total += len(m.returnCodes)

	return total, nil
}

func (m *SubackMessage) msglen() int {
	return 2 + len(m.returnCodes)
}
