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
	"encoding/binary"
	"fmt"
	"regexp"
)

// MQTT 3.1.3.1: Client Identifier
//
// The Server MUST allow ClientID’s which are between 1 and 23 UTF-8 encoded
// bytes in length, and that contain only the characters:
// "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
//
// The Server MAY allowClientId’s that contain more than 23 encoded bytes. The
// Server MAY allow ClientId’s that contain characters not included in the list
// given above.

// regular expression for the client identifier
var clientIDRegexp *regexp.Regexp

func init() {
	// regular expression for the client identifier
	//
	// (Accept all printable ASCII characters and a maximum length of 32 bytes
	// for better compatibility with various clients.)
	clientIDRegexp = regexp.MustCompile("^[[:print:]]{0,32}$")
}

// ConnectMessage represents a MQTT connect message.
// After a Network Connection is established by a Client to a Server, the first Packet
// sent from the Client to the Server MUST be a CONNECT Packet [MQTT-3.1.0-1].
//
// A Client can only send the CONNECT Packet once over a Network Connection. The Server
// MUST process a second CONNECT Packet sent from a Client as a protocol violation and
// disconnect the Client [MQTT-3.1.0-2].  See section 4.8 for information about
// handling errors.
type ConnectMessage struct {
	header

	// 7: username flag
	// 6: password flag
	// 5: will retain
	// 4-3: will QoS
	// 2: will flag
	// 1: clean session
	// 0: reserved
	connectFlags byte

	version byte

	keepAlive uint16

	protoName,
	clientID,
	willTopic,
	willMessage,
	username,
	password []byte
}

// NewConnectMessage creates a new CONNECT message.
func NewConnectMessage() *ConnectMessage {
	msg := &ConnectMessage{}
	msg.SetType(CONNECT)

	return msg
}

// String returns a string representation of the CONNECT message
func (m ConnectMessage) String() string {
	return fmt.Sprintf("%s, Connect Flags=%08b, Version=%d, KeepAlive=%d, Client ID=%q, Will Topic=%q, Will Message=%q, Username=%q, Password=%q",
		m.header,
		m.connectFlags,
		m.Version(),
		m.KeepAlive(),
		m.ClientID(),
		m.WillTopic(),
		m.WillMessage(),
		m.Username(),
		m.Password(),
	)
}

// Version returns the the 8 bit unsigned value that represents the revision level
// of the protocol used by the Client. The value of the Protocol Level field for
// the version 3.1.1 of the protocol is 4 (0x04).
func (m *ConnectMessage) Version() byte {
	return m.version
}

// SetVersion sets the version value of the CONNECT message
func (m *ConnectMessage) SetVersion(v byte) error {
	if _, ok := SupportedVersions[v]; !ok {
		return fmt.Errorf("connect/SetVersion: Invalid version number %d", v)
	}

	m.version = v
	m.dirty = true

	return nil
}

// CleanSession returns the bit that specifies the handling of the Session state.
// The Client and Server can store Session state to enable reliable messaging to
// continue across a sequence of Network Connections. This bit is used to control
// the lifetime of the Session state.
func (m *ConnectMessage) CleanSession() bool {
	return ((m.connectFlags >> 1) & 0x1) == 1
}

// SetCleanSession sets the bit that specifies the handling of the Session state.
func (m *ConnectMessage) SetCleanSession(v bool) {
	if v {
		m.connectFlags |= 0x2 // 00000010
	} else {
		m.connectFlags &= 253 // 11111101
	}

	m.dirty = true
}

// WillFlag returns the bit that specifies whether a Will Message should be stored
// on the server. If the Will Flag is set to 1 this indicates that, if the Connect
// request is accepted, a Will Message MUST be stored on the Server and associated
// with the Network Connection.
func (m *ConnectMessage) WillFlag() bool {
	return ((m.connectFlags >> 2) & 0x1) == 1
}

// SetWillFlag sets the bit that specifies whether a Will Message should be stored
// on the server.
func (m *ConnectMessage) SetWillFlag(v bool) {
	if v {
		m.connectFlags |= 0x4 // 00000100
	} else {
		m.connectFlags &= 251 // 11111011
	}

	m.dirty = true
}

// WillQos returns the two bits that specify the QoS level to be used when publishing
// the Will Message.
func (m *ConnectMessage) WillQos() byte {
	return (m.connectFlags >> 3) & 0x3
}

// SetWillQos sets the two bits that specify the QoS level to be used when publishing
// the Will Message.
func (m *ConnectMessage) SetWillQos(qos byte) error {
	if qos != QosAtMostOnce && qos != QosAtLeastOnce && qos != QosExactlyOnce {
		return fmt.Errorf("connect/SetWillQos: Invalid QoS level %d", qos)
	}

	m.connectFlags = (m.connectFlags & 231) | (qos << 3) // 231 = 11100111
	m.dirty = true

	return nil
}

// WillRetain returns the bit specifies if the Will Message is to be Retained when it
// is published.
func (m *ConnectMessage) WillRetain() bool {
	return ((m.connectFlags >> 5) & 0x1) == 1
}

// SetWillRetain sets the bit specifies if the Will Message is to be Retained when it
// is published.
func (m *ConnectMessage) SetWillRetain(v bool) {
	if v {
		m.connectFlags |= 32 // 00100000
	} else {
		m.connectFlags &= 223 // 11011111
	}

	m.dirty = true
}

// UsernameFlag returns the bit that specifies whether a user name is present in the
// payload.
func (m *ConnectMessage) UsernameFlag() bool {
	return ((m.connectFlags >> 7) & 0x1) == 1
}

// SetUsernameFlag sets the bit that specifies whether a user name is present in the
// payload.
func (m *ConnectMessage) SetUsernameFlag(v bool) {
	if v {
		m.connectFlags |= 128 // 10000000
	} else {
		m.connectFlags &= 127 // 01111111
	}

	m.dirty = true
}

// PasswordFlag returns the bit that specifies whether a password is present in the
// payload.
func (m *ConnectMessage) PasswordFlag() bool {
	return ((m.connectFlags >> 6) & 0x1) == 1
}

// SetPasswordFlag sets the bit that specifies whether a password is present in the
// payload.
func (m *ConnectMessage) SetPasswordFlag(v bool) {
	if v {
		m.connectFlags |= 64 // 01000000
	} else {
		m.connectFlags &= 191 // 10111111
	}

	m.dirty = true
}

// KeepAlive returns a time interval measured in seconds. Expressed as a 16-bit word,
// it is the maximum time interval that is permitted to elapse between the point at
// which the Client finishes transmitting one Control Packet and the point it starts
// sending the next.
func (m *ConnectMessage) KeepAlive() uint16 {
	return m.keepAlive
}

// SetKeepAlive sets the time interval in which the server should keep the connection
// alive.
func (m *ConnectMessage) SetKeepAlive(v uint16) {
	m.keepAlive = v

	m.dirty = true
}

// ClientID returns an ID that identifies the Client to the Server. Each Client
// connecting to the Server has a unique ClientID. The ClientID MUST be used by
// Clients and by Servers to identify state that they hold relating to this MQTT
// Session between the Client and the Server
func (m *ConnectMessage) ClientID() []byte {
	return m.clientID
}

// SetClientID sets an ID that identifies the Client to the Server.
func (m *ConnectMessage) SetClientID(v []byte) error {
	if len(v) > 0 && !m.validClientID(v) {
		return ErrIdentifierRejected
	}

	m.clientID = v
	m.dirty = true

	return nil
}

// WillTopic returns the topic in which the Will Message should be published to.
// If the Will Flag is set to 1, the Will Topic must be in the payload.
func (m *ConnectMessage) WillTopic() []byte {
	return m.willTopic
}

// SetWillTopic sets the topic in which the Will Message should be published to.
func (m *ConnectMessage) SetWillTopic(v []byte) {
	m.willTopic = v

	if len(v) > 0 {
		m.SetWillFlag(true)
	} else if len(m.willMessage) == 0 {
		m.SetWillFlag(false)
	}

	m.dirty = true
}

// WillMessage returns the Will Message that is to be published to the Will Topic.
func (m *ConnectMessage) WillMessage() []byte {
	return m.willMessage
}

// SetWillMessage sets the Will Message that is to be published to the Will Topic.
func (m *ConnectMessage) SetWillMessage(v []byte) {
	m.willMessage = v

	if len(v) > 0 {
		m.SetWillFlag(true)
	} else if len(m.willTopic) == 0 {
		m.SetWillFlag(false)
	}

	m.dirty = true
}

// Username returns the username from the payload. If the User Name Flag is set to 1,
// this must be in the payload. It can be used by the Server for authentication and
// authorization.
func (m *ConnectMessage) Username() []byte {
	return m.username
}

// SetUsername sets the username for authentication.
func (m *ConnectMessage) SetUsername(v []byte) {
	m.username = v

	if len(v) > 0 {
		m.SetUsernameFlag(true)
	} else {
		m.SetUsernameFlag(false)
	}

	m.dirty = true
}

// Password returns the password from the payload. If the Password Flag is set to 1,
// this must be in the payload. It can be used by the Server for authentication and
// authorization.
func (m *ConnectMessage) Password() []byte {
	return m.password
}

// SetPassword sets the username for authentication.
func (m *ConnectMessage) SetPassword(v []byte) {
	m.password = v

	if len(v) > 0 {
		m.SetPasswordFlag(true)
	} else {
		m.SetPasswordFlag(false)
	}

	m.dirty = true
}

// Len returns the length of the message.
func (m *ConnectMessage) Len() int {
	if !m.dirty {
		return len(m.dbuf)
	}

	ml := m.msglen()

	if err := m.SetRemainingLength(int32(ml)); err != nil {
		return 0
	}

	return m.header.msglen() + ml
}

// Decode decodes a connect messsage.
// For the CONNECT message, the error returned could be a ConnackReturnCode, so
// be sure to check that. Otherwise it's a generic error. If a generic error is
// returned, this Message should be considered invalid.
//
// Caller should call ValidConnackError(err) to see if the returned error is
// a Connack error. If so, caller should send the Client back the corresponding
// CONNACK message.
func (m *ConnectMessage) Decode(src []byte) (int, error) {
	total := 0

	n, err := m.header.decode(src[total:])
	if err != nil {
		return total + n, err
	}
	total += n

	if n, err = m.decodeMessage(src[total:]); err != nil {
		return total + n, err
	}
	total += n

	m.dirty = false

	return total, nil
}

// Encode encodes a connect message.
func (m *ConnectMessage) Encode(dst []byte) (int, error) {
	if !m.dirty {
		if len(dst) < len(m.dbuf) {
			return 0, fmt.Errorf("connect/Encode: Insufficient buffer size. Expecting %d, got %d", len(m.dbuf), len(dst))
		}

		return copy(dst, m.dbuf), nil
	}

	if m.Type() != CONNECT {
		return 0, fmt.Errorf("connect/Encode: Invalid message type. Expecting %d, got %d", CONNECT, m.Type())
	}

	_, ok := SupportedVersions[m.version]
	if !ok {
		return 0, ErrInvalidProtocolVersion
	}

	hl := m.header.msglen()
	ml := m.msglen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("connect/Encode: Insufficient buffer size. Expecting %d, got %d", hl+ml, len(dst))
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

	n, err = m.encodeMessage(dst[total:])
	total += n
	if err != nil {
		return total, err
	}

	return total, nil
}

func (m *ConnectMessage) encodeMessage(dst []byte) (int, error) {
	total := 0

	n, err := writeLPBytes(dst[total:], []byte(SupportedVersions[m.version]))
	total += n
	if err != nil {
		return total, err
	}

	dst[total] = m.version
	total++

	dst[total] = m.connectFlags
	total++

	binary.BigEndian.PutUint16(dst[total:], m.keepAlive)
	total += 2

	n, err = writeLPBytes(dst[total:], m.clientID)
	total += n
	if err != nil {
		return total, err
	}

	if m.WillFlag() {
		n, err = writeLPBytes(dst[total:], m.willTopic)
		total += n
		if err != nil {
			return total, err
		}

		n, err = writeLPBytes(dst[total:], m.willMessage)
		total += n
		if err != nil {
			return total, err
		}
	}

	// According to the 3.1 spec, it's possible that the usernameFlag is set,
	// but the username string is missing.
	if m.UsernameFlag() && len(m.username) > 0 {
		n, err = writeLPBytes(dst[total:], m.username)
		total += n
		if err != nil {
			return total, err
		}
	}

	// According to the 3.1 spec, it's possible that the passwordFlag is set,
	// but the password string is missing.
	if m.PasswordFlag() && len(m.password) > 0 {
		n, err = writeLPBytes(dst[total:], m.password)
		total += n
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

func (m *ConnectMessage) decodeMessage(src []byte) (int, error) {
	var err error
	n, total := 0, 0

	m.protoName, n, err = readLPBytes(src[total:])
	total += n
	if err != nil {
		return total, err
	}

	m.version = src[total]
	total++

	if verstr, ok := SupportedVersions[m.version]; !ok {
		return total, ErrInvalidProtocolVersion
	} else if verstr != string(m.protoName) {
		return total, ErrInvalidProtocolVersion
	}

	m.connectFlags = src[total]
	total++

	if m.connectFlags&0x1 != 0 {
		return total, fmt.Errorf("connect/decodeMessage: Connect Flags reserved bit 0 is not 0")
	}

	if m.WillQos() > QosExactlyOnce {
		return total, fmt.Errorf("connect/decodeMessage: Invalid QoS level (%d) for %s message", m.WillQos(), m.Name())
	}

	if !m.WillFlag() && (m.WillRetain() || m.WillQos() != QosAtMostOnce) {
		return total, fmt.Errorf("connect/decodeMessage: Protocol violation: If the Will Flag (%t) is set to 0 the Will QoS (%d) and Will Retain (%t) fields MUST be set to zero", m.WillFlag(), m.WillQos(), m.WillRetain())
	}

	if len(src[total:]) < 2 {
		return 0, fmt.Errorf("connect/decodeMessage: Insufficient buffer size. Expecting %d, got %d", 2, len(src[total:]))
	}

	m.keepAlive = binary.BigEndian.Uint16(src[total:])
	total += 2

	m.clientID, n, err = readLPBytes(src[total:])
	total += n
	if err != nil {
		return total, err
	}

	// If the Client supplies a zero-byte ClientId, the Client MUST also set CleanSession to 1
	if len(m.clientID) == 0 && !m.CleanSession() {
		return total, ErrIdentifierRejected
	}

	if len(m.clientID) > 0 && !m.validClientID(m.clientID) {
		return total, ErrIdentifierRejected
	}

	if m.WillFlag() {
		m.willTopic, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}

		m.willMessage, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
	}

	// According to the 3.1 spec, it's possible that the passwordFlag is set,
	// but the password string is missing.
	if m.UsernameFlag() && len(src[total:]) > 0 {
		m.username, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
	}

	// According to the 3.1 spec, it's possible that the passwordFlag is set,
	// but the password string is missing.
	if m.PasswordFlag() && len(src[total:]) > 0 {
		m.password, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

func (m *ConnectMessage) msglen() int {
	total := 0

	verstr, ok := SupportedVersions[m.version]
	if !ok {
		return total
	}

	// 2 bytes protocol name length
	// n bytes protocol name
	// 1 byte protocol version
	// 1 byte connect flags
	// 2 bytes keep alive timer
	total += 2 + len(verstr) + 1 + 1 + 2

	// Add the clientID length, 2 is the length prefix
	total += 2 + len(m.clientID)

	// Add the will topic and will message length, and the length prefixes
	if m.WillFlag() {
		total += 2 + len(m.willTopic) + 2 + len(m.willMessage)
	}

	// Add the username length
	// According to the 3.1 spec, it's possible that the usernameFlag is set,
	// but the user name string is missing.
	if m.UsernameFlag() && len(m.username) > 0 {
		total += 2 + len(m.username)
	}

	// Add the password length
	// According to the 3.1 spec, it's possible that the passwordFlag is set,
	// but the password string is missing.
	if m.PasswordFlag() && len(m.password) > 0 {
		total += 2 + len(m.password)
	}

	return total
}

// validClientID checks the client ID, which is a slice of bytes, to see if it's
// valid.
func (m *ConnectMessage) validClientID(cid []byte) bool {
	return clientIDRegexp.Match(cid)
}
