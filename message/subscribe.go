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

import (
	"bytes"
	"fmt"
	"sync/atomic"
)

// SubscribeMessage is a SUBSCRIBE packet, sent from the Client to the Server to create one or more
// Subscriptions. Each Subscription registers a Client’s interest in one or more
// Topics. The Server sends PUBLISH Packets to the Client in order to forward
// Application Messages that were published to Topics that match these Subscriptions.
// The SUBSCRIBE Packet also specifies (for each Subscription) the maximum QoS with
// which the Server can send Application Messages to the Client.
type SubscribeMessage struct {
	header

	topics [][]byte
	qos    []byte
}

var _ Message = (*SubscribeMessage)(nil)

// NewSubscribeMessage creates a new SUBSCRIBE message.
func NewSubscribeMessage() *SubscribeMessage {
	msg := &SubscribeMessage{}
	msg.SetType(SUBSCRIBE)

	return msg
}

func (m SubscribeMessage) String() string {
	msgstr := fmt.Sprintf("%s, Packet ID=%d", m.header, m.PacketID())

	for i, t := range m.topics {
		msgstr = fmt.Sprintf("%s, Topic[%d]=%q/%d", msgstr, i, string(t), m.qos[i])
	}

	return msgstr
}

// Topics returns a list of topics sent by the Client.
func (m *SubscribeMessage) Topics() [][]byte {
	return m.topics
}

// AddTopic adds a single topic to the message, along with the corresponding QoS.
// An error is returned if QoS is invalid.
func (m *SubscribeMessage) AddTopic(topic []byte, qos byte) error {
	if !ValidQos(qos) {
		return fmt.Errorf("Invalid QoS %d", qos)
	}

	var i int
	var t []byte
	var found bool

	for i, t = range m.topics {
		if bytes.Equal(t, topic) {
			found = true
			break
		}
	}

	if found {
		m.qos[i] = qos
		return nil
	}

	m.topics = append(m.topics, topic)
	m.qos = append(m.qos, qos)
	m.dirty = true

	return nil
}

// RemoveTopic removes a single topic from the list of existing ones in the message.
// If topic does not exist it just does nothing.
func (m *SubscribeMessage) RemoveTopic(topic []byte) {
	var i int
	var t []byte
	var found bool

	for i, t = range m.topics {
		if bytes.Equal(t, topic) {
			found = true
			break
		}
	}

	if found {
		m.topics = append(m.topics[:i], m.topics[i+1:]...)
		m.qos = append(m.qos[:i], m.qos[i+1:]...)
	}

	m.dirty = true
}

// TopicExists checks to see if a topic exists in the list.
func (m *SubscribeMessage) TopicExists(topic []byte) bool {
	for _, t := range m.topics {
		if bytes.Equal(t, topic) {
			return true
		}
	}

	return false
}

// TopicQos returns the QoS level of a topic. If topic does not exist, QosFailure
// is returned.
func (m *SubscribeMessage) TopicQos(topic []byte) byte {
	for i, t := range m.topics {
		if bytes.Equal(t, topic) {
			return m.qos[i]
		}
	}

	return QosFailure
}

// Qos returns the list of QoS current in the message.
func (m *SubscribeMessage) Qos() []byte {
	return m.qos
}

// Len returns the length of the message.
func (m *SubscribeMessage) Len() int {
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
func (m *SubscribeMessage) Decode(src []byte) (int, error) {
	total := 0

	hn, err := m.header.decode(src[total:])
	total += hn
	if err != nil {
		return total, err
	}

	//this.packetId = binary.BigEndian.Uint16(src[total:])
	m.packetID = src[total : total+2]
	total += 2

	remlen := int(m.remlen) - (total - hn)
	for remlen > 0 {
		t, n, err := readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}

		m.topics = append(m.topics, t)

		m.qos = append(m.qos, src[total])
		total++

		remlen = remlen - n - 1
	}

	if len(m.topics) == 0 {
		return 0, fmt.Errorf("subscribe/Decode: Empty topic list")
	}

	m.dirty = false

	return total, nil
}

// Encode encodes the message.
func (m *SubscribeMessage) Encode(dst []byte) (int, error) {
	if !m.dirty {
		if len(dst) < len(m.dbuf) {
			return 0, fmt.Errorf("subscribe/Encode: Insufficient buffer size. Expecting %d, got %d", len(m.dbuf), len(dst))
		}

		return copy(dst, m.dbuf), nil
	}

	hl := m.header.msglen()
	ml := m.msglen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("subscribe/Encode: Insufficient buffer size. Expecting %d, got %d", hl+ml, len(dst))
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

	if m.PacketID() == 0 {
		m.SetPacketID(uint16(atomic.AddUint64(&gPacketID, 1) & 0xffff))
		//this.packetId = uint16(atomic.AddUint64(&gPacketId, 1) & 0xffff)
	}

	n = copy(dst[total:], m.packetID)
	//binary.BigEndian.PutUint16(dst[total:], this.packetId)
	total += n

	for i, t := range m.topics {
		n, err := writeLPBytes(dst[total:], t)
		total += n
		if err != nil {
			return total, err
		}

		dst[total] = m.qos[i]
		total++
	}

	return total, nil
}

func (m *SubscribeMessage) msglen() int {
	// packet ID
	total := 2

	for _, t := range m.topics {
		total += 2 + len(t) + 1
	}

	return total
}
