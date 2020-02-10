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
	"fmt"
	"sync/atomic"
)

// A PublishMessage (PUBLISH Control Packet) is sent from a Client to a Server
// or from Server to a Client to transport an Application Message.
type PublishMessage struct {
	header

	topic   []byte
	payload []byte
}

var _ Message = (*PublishMessage)(nil)

// NewPublishMessage creates a new PUBLISH message.
func NewPublishMessage() *PublishMessage {
	msg := &PublishMessage{}
	msg.SetType(PUBLISH)

	return msg
}

func (m *PublishMessage) String() string {
	return fmt.Sprintf("%s, Topic=%q, Packet ID=%d, QoS=%d, Retained=%t, Dup=%t, Payload=%v",
		m.header, m.topic, m.packetId, m.QoS(), m.Retain(), m.Dup(), m.payload)
}

// Dup returns the value specifying the duplicate delivery of a PUBLISH Control Packet.
// If the DUP flag is set to 0, it indicates that this is the first occasion that the
// Client or Server has attempted to send this MQTT PUBLISH Packet. If the DUP flag is
// set to 1, it indicates that this might be re-delivery of an earlier attempt to send
// the Packet.
func (m *PublishMessage) Dup() bool {
	return ((m.Flags() >> 3) & 0x1) == 1
}

// SetDup sets the value specifying the duplicate delivery of a PUBLISH Control Packet.
func (m *PublishMessage) SetDup(v bool) {
	if v {
		m.mtypeflags[0] |= 0x8 // 00001000
	} else {
		m.mtypeflags[0] &= 247 // 11110111
	}
}

// Retain returns the value of the RETAIN flag. This flag is only used on the PUBLISH
// Packet. If the RETAIN flag is set to 1, in a PUBLISH Packet sent by a Client to a
// Server, the Server MUST store the Application Message and its QoS, so that it can be
// delivered to future subscribers whose subscriptions match its topic name.
func (m *PublishMessage) Retain() bool {
	return (m.Flags() & 0x1) == 1
}

// SetRetain sets the value of the RETAIN flag.
func (m *PublishMessage) SetRetain(v bool) {
	if v {
		m.mtypeflags[0] |= 0x1 // 00000001
	} else {
		m.mtypeflags[0] &= 254 // 11111110
	}
}

// QoS returns the field that indicates the level of assurance for delivery of an
// Application Message. The values are QosAtMostOnce, QosAtLeastOnce and QosExactlyOnce.
func (m *PublishMessage) QoS() byte {
	return (m.Flags() >> 1) & 0x3
}

// SetQoS sets the field that indicates the level of assurance for delivery of an
// Application Message. The values are QosAtMostOnce, QosAtLeastOnce and QosExactlyOnce.
// An error is returned if the value is not one of these.
func (m *PublishMessage) SetQoS(v byte) error {
	if v != 0x0 && v != 0x1 && v != 0x2 {
		return fmt.Errorf("publish/SetQoS: Invalid QoS %d", v)
	}
	p := m.QoS()
	m.mtypeflags[0] = (m.mtypeflags[0] & 249) | (v << 1) // 249 = 11111001

	// QoS can change length of message (QoS 0: without packet ID, Qos 1 and 2:
	// with packet ID)
	if (p > 0) != (v > 0) {
		m.dirty = true
	}
	return nil
}

// Topic returns the the topic name that identifies the information channel to which
// payload data is published.
func (m *PublishMessage) Topic() []byte {
	return m.topic
}

// SetTopic sets the the topic name that identifies the information channel to which
// payload data is published. An error is returned if ValidTopic() is falbase.
func (m *PublishMessage) SetTopic(v []byte) error {
	if !ValidTopic(v) {
		return fmt.Errorf("publish/SetTopic: Invalid topic name (%s). Must not be empty or contain wildcard characters", string(v))
	}

	m.topic = v
	m.dirty = true

	return nil
}

// Payload returns the application message that's part of the PUBLISH message.
func (m *PublishMessage) Payload() []byte {
	return m.payload
}

// SetPayload sets the application message that's part of the PUBLISH message.
func (m *PublishMessage) SetPayload(v []byte) {
	m.payload = v
	m.dirty = true
}

// Len returns the length of the message in bytes.
func (m *PublishMessage) Len() int {
	if !m.dirty {
		return len(m.dbuf)
	}

	ml := m.msglen()

	if err := m.SetRemainingLength(int32(ml)); err != nil {
		return 0
	}

	return m.header.msglen() + ml
}

// Decode decodes a message from bytes.
func (m *PublishMessage) Decode(src []byte) (int, error) {
	total := 0

	hn, err := m.header.decode(src[total:])
	total += hn
	if err != nil {
		return total, err
	}

	n := 0

	m.topic, n, err = readLPBytes(src[total:])
	total += n
	if err != nil {
		return total, err
	}

	if !ValidTopic(m.topic) {
		return total, fmt.Errorf("publish/Decode: Invalid topic name (%s). Must not be empty or contain wildcard characters", string(m.topic))
	}

	// The packet identifier field is only present in the PUBLISH packets where the
	// QoS level is 1 or 2
	if m.QoS() != 0 {
		//m.packetId = binary.BigEndian.Uint16(src[total:])
		m.packetId = src[total : total+2]
		total += 2
	}

	l := int(m.remlen) - (total - hn)
	m.payload = src[total : total+l]
	total += len(m.payload)

	m.dirty = false

	return total, nil
}

// Encode encodes the message into bytes.
func (m *PublishMessage) Encode(dst []byte) (int, error) {
	if !m.dirty {
		if len(dst) < len(m.dbuf) {
			return 0, fmt.Errorf("publish/Encode: Insufficient buffer size. Expecting %d, got %d", len(m.dbuf), len(dst))
		}

		return copy(dst, m.dbuf), nil
	}

	if len(m.topic) == 0 {
		return 0, fmt.Errorf("publish/Encode: Topic name is empty")
	}

	if len(m.payload) == 0 {
		return 0, fmt.Errorf("publish/Encode: Payload is empty")
	}

	ml := m.msglen()

	if err := m.SetRemainingLength(int32(ml)); err != nil {
		return 0, err
	}

	hl := m.header.msglen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("publish/Encode: Insufficient buffer size. Expecting %d, got %d", hl+ml, len(dst))
	}

	total := 0

	n, err := m.header.encode(dst[total:])
	total += n
	if err != nil {
		return total, err
	}

	n, err = writeLPBytes(dst[total:], m.topic)
	total += n
	if err != nil {
		return total, err
	}

	// The packet identifier field is only present in the PUBLISH packets where the QoS level is 1 or 2
	if m.QoS() != 0 {
		if m.PacketId() == 0 {
			m.SetPacketId(uint16(atomic.AddUint64(&gPacketId, 1) & 0xffff))
			//m.packetId = uint16(atomic.AddUint64(&gPacketId, 1) & 0xffff)
		}

		n = copy(dst[total:], m.packetId)
		//binary.BigEndian.PutUint16(dst[total:], m.packetId)
		total += n
	}

	copy(dst[total:], m.payload)
	total += len(m.payload)

	return total, nil
}

// Clone create a deep clone of the message.
func (m *PublishMessage) Clone() (*PublishMessage, error) {
	l := m.Len()
	buf := make([]byte, l)
	if _, err := m.Encode(buf); err != nil {
		return nil, err
	}
	cm := NewPublishMessage()
	if _, err := cm.Decode(buf); err != nil {
		return nil, err
	}
	return cm, nil
}

func (m *PublishMessage) msglen() int {
	total := 2 + len(m.topic) + len(m.payload)
	if m.QoS() != 0 {
		total += 2
	}

	return total
}
