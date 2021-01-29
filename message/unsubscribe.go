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

// UnsubscribeMessage is a UNSUBSCRIBE packet, sent by the Client to the Server, to unsubscribe from topics.
type UnsubscribeMessage struct {
	header

	topics [][]byte
}

var _ Message = (*UnsubscribeMessage)(nil)

// NewUnsubscribeMessage creates a new UNSUBSCRIBE message.
func NewUnsubscribeMessage() *UnsubscribeMessage {
	msg := &UnsubscribeMessage{}
	msg.SetType(UNSUBSCRIBE)

	return msg
}

func (m UnsubscribeMessage) String() string {
	msgstr := fmt.Sprintf("%s", m.header)

	for i, t := range m.topics {
		msgstr = fmt.Sprintf("%s, Topic%d=%s", msgstr, i, string(t))
	}

	return msgstr
}

// Topics returns a list of topics sent by the Client.
func (m *UnsubscribeMessage) Topics() [][]byte {
	return m.topics
}

// AddTopic adds a single topic to the message.
func (m *UnsubscribeMessage) AddTopic(topic []byte) {
	if m.TopicExists(topic) {
		return
	}

	m.topics = append(m.topics, topic)
	m.dirty = true
}

// RemoveTopic removes a single topic from the list of existing ones in the message.
// If topic does not exist it just does nothing.
func (m *UnsubscribeMessage) RemoveTopic(topic []byte) {
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
	}

	m.dirty = true
}

// TopicExists checks to see if a topic exists in the list.
func (m *UnsubscribeMessage) TopicExists(topic []byte) bool {
	for _, t := range m.topics {
		if bytes.Equal(t, topic) {
			return true
		}
	}

	return false
}

// Len is the length of the message.
func (m *UnsubscribeMessage) Len() int {
	if !m.dirty {
		return len(m.dbuf)
	}

	ml := m.msglen()

	if err := m.SetRemainingLength(int32(ml)); err != nil {
		return 0
	}

	return m.header.msglen() + ml
}

// Decode reads from the io.Reader parameter until a full message is decoded, or
// when io.Reader returns EOF or error. The first return value is the number of
// bytes read from io.Reader. The second is error if Decode encounters any problems.
func (m *UnsubscribeMessage) Decode(src []byte) (int, error) {
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
		remlen = remlen - n - 1
	}

	if len(m.topics) == 0 {
		return 0, fmt.Errorf("unsubscribe/Decode: Empty topic list")
	}

	m.dirty = false

	return total, nil
}

// Encode returns an io.Reader in which the encoded bytes can be read. The second
// return value is the number of bytes encoded, so the caller knows how many bytes
// there will be. If Encode returns an error, then the first two return values
// should be considered invalid.
// Any changes to the message after Encode() is called will invalidate the io.Reader.
func (m *UnsubscribeMessage) Encode(dst []byte) (int, error) {
	if !m.dirty {
		if len(dst) < len(m.dbuf) {
			return 0, fmt.Errorf("unsubscribe/Encode: Insufficient buffer size. Expecting %d, got %d", len(m.dbuf), len(dst))
		}

		return copy(dst, m.dbuf), nil
	}

	hl := m.header.msglen()
	ml := m.msglen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("unsubscribe/Encode: Insufficient buffer size. Expecting %d, got %d", hl+ml, len(dst))
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

	for _, t := range m.topics {
		n, err := writeLPBytes(dst[total:], t)
		total += n
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

func (m *UnsubscribeMessage) msglen() int {
	// packet ID
	total := 2

	for _, t := range m.topics {
		total += 2 + len(t)
	}

	return total
}
