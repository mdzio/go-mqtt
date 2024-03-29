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

package sessions

import (
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/mdzio/go-mqtt/message"
)

var (
	errQueueEmpty  error = errors.New("queue empty")
	errWaitMessage error = errors.New("Invalid message to wait for ack")
	errAckMessage  error = errors.New("Invalid message for acking")
)

// AckMsg is a message waiting for acknowledge.
type AckMsg struct {
	// Message type of the message waiting for ack
	Mtype message.Type

	// Current state of the ack-waiting message
	State message.Type

	// Packet ID of the message. Every message that require ack'ing must have a valid
	// packet ID. Messages that have message I
	Pktid uint16

	// Slice containing the message bytes
	Msgbuf []byte

	// Slice containing the ack message bytes
	Ackbuf []byte

	// When ack cycle completes, call this function
	OnComplete interface{}
}

// Ackqueue is a growing queue implemented based on a ring buffer. As the buffer
// gets full, it will auto-grow.
//
// Ackqueue is used to store messages that are waiting for acks to come back. There
// are a few scenarios in which acks are required.
//   1. Client sends SUBSCRIBE message to server, waits for SUBACK.
//   2. Client sends UNSUBSCRIBE message to server, waits for UNSUBACK.
//   3. Client sends PUBLISH QoS 1 message to server, waits for PUBACK.
//   4. Server sends PUBLISH QoS 1 message to client, waits for PUBACK.
//   5. Client sends PUBLISH QoS 2 message to server, waits for PUBREC.
//   6. Server sends PUBREC message to client, waits for PUBREL.
//   7. Client sends PUBREL message to server, waits for PUBCOMP.
//   8. Server sends PUBLISH QoS 2 message to client, waits for PUBREC.
//   9. Client sends PUBREC message to server, waits for PUBREL.
//   10. Server sends PUBREL message to client, waits for PUBCOMP.
//   11. Client sends PINGREQ message to server, waits for PINGRESP.
type Ackqueue struct {
	size  int64
	mask  int64
	count int64
	head  int64
	tail  int64

	ping AckMsg
	ring []AckMsg
	emap map[uint16]int64

	ackdone []AckMsg

	mu sync.Mutex
}

func newAckqueue(n int) *Ackqueue {
	m := int64(n)
	if !powerOfTwo64(m) {
		m = roundUpPowerOfTwo64(m)
	}

	return &Ackqueue{
		size:    m,
		mask:    m - 1,
		count:   0,
		head:    0,
		tail:    0,
		ring:    make([]AckMsg, m),
		emap:    make(map[uint16]int64, m),
		ackdone: make([]AckMsg, 0),
	}
}

// Wait copies the message into a waiting queue, and waits for the corresponding
// ack message to be received.
func (aq *Ackqueue) Wait(msg message.Message, onComplete interface{}) error {
	aq.mu.Lock()
	defer aq.mu.Unlock()

	switch msg := msg.(type) {
	case *message.PublishMessage:
		if msg.QoS() == message.QosAtMostOnce {
			//return fmt.Errorf("QoS 0 messages don't require ack")
			return errWaitMessage
		}

		aq.insert(msg.PacketID(), msg, onComplete)

	case *message.SubscribeMessage:
		aq.insert(msg.PacketID(), msg, onComplete)

	case *message.UnsubscribeMessage:
		aq.insert(msg.PacketID(), msg, onComplete)

	case *message.PingreqMessage:
		aq.ping = AckMsg{
			Mtype:      message.PINGREQ,
			State:      message.RESERVED,
			Msgbuf:     make([]byte, 2),
			OnComplete: onComplete,
		}
		msg.Encode(aq.ping.Msgbuf)

	default:
		return errWaitMessage
	}

	return nil
}

// Ack takes the ack message supplied and updates the status of messages waiting.
func (aq *Ackqueue) Ack(msg message.Message) error {
	aq.mu.Lock()
	defer aq.mu.Unlock()

	switch msg.Type() {
	case message.PUBACK, message.PUBREC, message.PUBREL, message.PUBCOMP, message.SUBACK, message.UNSUBACK:
		// Check to see if the message w/ the same packet ID is in the queue
		i, ok := aq.emap[msg.PacketID()]
		if ok {
			// If message w/ the packet ID exists, update the message state and copy
			// the ack message
			aq.ring[i].State = msg.Type()

			ml := msg.Len()
			aq.ring[i].Ackbuf = make([]byte, ml)

			_, err := msg.Encode(aq.ring[i].Ackbuf)
			if err != nil {
				return err
			}
		}

	case message.PINGRESP:
		if aq.ping.Mtype == message.PINGREQ {
			aq.ping.State = message.PINGRESP
			aq.ping.Ackbuf = make([]byte, 2)
			msg.Encode(aq.ping.Ackbuf)
		}

	default:
		return errAckMessage
	}

	return nil
}

// Acked returns the list of messages that have completed the ack cycle.
func (aq *Ackqueue) Acked() []AckMsg {
	aq.mu.Lock()
	defer aq.mu.Unlock()

	aq.ackdone = aq.ackdone[0:0]

	if aq.ping.State == message.PINGRESP {
		aq.ackdone = append(aq.ackdone, aq.ping)
		aq.ping = AckMsg{}
	}

FORNOTEMPTY:
	for !aq.empty() {
		switch aq.ring[aq.head].State {
		case message.PUBACK, message.PUBREL, message.PUBCOMP, message.SUBACK, message.UNSUBACK:
			aq.ackdone = append(aq.ackdone, aq.ring[aq.head])
			aq.removeHead()

		default:
			break FORNOTEMPTY
		}
	}

	return aq.ackdone
}

func (aq *Ackqueue) insert(pktid uint16, msg message.Message, onComplete interface{}) error {
	if aq.full() {
		aq.grow()
	}

	if _, ok := aq.emap[pktid]; !ok {
		// message length
		ml := msg.Len()

		// ackmsg
		am := AckMsg{
			Mtype:      msg.Type(),
			State:      message.RESERVED,
			Pktid:      msg.PacketID(),
			Msgbuf:     make([]byte, ml),
			OnComplete: onComplete,
		}

		if _, err := msg.Encode(am.Msgbuf); err != nil {
			return err
		}

		aq.ring[aq.tail] = am
		aq.emap[pktid] = aq.tail
		aq.tail = aq.increment(aq.tail)
		aq.count++
	} else {
		// If packet w/ pktid already exist, then this must be a PUBLISH message
		// Other message types should never send with the same packet ID
		pm, ok := msg.(*message.PublishMessage)
		if !ok {
			return fmt.Errorf("ack/insert: duplicate packet ID for %s message", msg.Name())
		}

		// If this is a publish message, then the DUP flag must be set. This is the
		// only scenario in which we will receive duplicate messages.
		if pm.Dup() {
			return fmt.Errorf("ack/insert: duplicate packet ID for PUBLISH message, but DUP flag is not set")
		}

		// Since it's a dup, there's really nothing we need to do. Moving on...
	}

	return nil
}

func (aq *Ackqueue) removeHead() error {
	if aq.empty() {
		return errQueueEmpty
	}

	it := aq.ring[aq.head]
	// set this to empty ackmsg{} to ensure GC will collect the buffer
	aq.ring[aq.head] = AckMsg{}
	aq.head = aq.increment(aq.head)
	aq.count--
	delete(aq.emap, it.Pktid)

	return nil
}

func (aq *Ackqueue) grow() {
	if math.MaxInt64/2 < aq.size {
		panic("new size will overflow int64")
	}

	newsize := aq.size << 1
	newmask := newsize - 1
	newring := make([]AckMsg, newsize)

	if aq.tail > aq.head {
		copy(newring, aq.ring[aq.head:aq.tail])
	} else {
		copy(newring, aq.ring[aq.head:])
		copy(newring[aq.size-aq.head:], aq.ring[:aq.tail])
	}

	aq.size = newsize
	aq.mask = newmask
	aq.ring = newring
	aq.head = 0
	aq.tail = aq.count

	aq.emap = make(map[uint16]int64, aq.size)

	for i := int64(0); i < aq.tail; i++ {
		aq.emap[aq.ring[i].Pktid] = i
	}
}

func (aq *Ackqueue) len() int {
	return int(aq.count)
}

func (aq *Ackqueue) cap() int {
	return int(aq.size)
}

func (aq *Ackqueue) index(n int64) int64 {
	return n & aq.mask
}

func (aq *Ackqueue) full() bool {
	return aq.count == aq.size
}

func (aq *Ackqueue) empty() bool {
	return aq.count == 0
}

func (aq *Ackqueue) increment(n int64) int64 {
	return aq.index(n + 1)
}

func powerOfTwo64(n int64) bool {
	return n != 0 && (n&(n-1)) == 0
}

func roundUpPowerOfTwo64(n int64) int64 {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++

	return n
}
