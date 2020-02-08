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

package service

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/mdzio/go-mqtt/message"
	"github.com/mdzio/go-mqtt/sessions"
)

var (
	errDisconnect = errors.New("Disconnect")
)

// processor() reads messages from the incoming buffer and processes them
func (p *service) processor() {
	defer func() {
		// Let's recover from panic
		if r := recover(); r != nil {
			log.Errorf("(%s) Recovering from panic: %v", p.cid(), r)
		}

		p.wgStopped.Done()
		p.stop()

		log.Tracef("(%s) Processor stopped", p.cid())
	}()

	log.Tracef("(%s) Starting processor", p.cid())

	p.wgStarted.Done()

	for {
		// 1. Find out what message is next and the size of the message
		mtype, total, err := p.peekMessageSize()
		if err != nil {
			if !isEOF(err) {
				log.Warningf("(%s) Error peeking next message size: %v", p.cid(), err)
			}
			return
		}

		msg, n, err := p.peekMessage(mtype, total)
		if err != nil {
			if !isEOF(err) {
				log.Warningf("(%s) Error peeking next message: %v", p.cid(), err)
			}
			return
		}

		//log.Debugf("(%s) Received: %s", p.cid(), msg)

		p.inStat.increment(int64(n))

		// 5. Process the read message
		err = p.processIncoming(msg)
		if err != nil {
			if err != errDisconnect {
				log.Warningf("(%s) Error processing %s: %v", p.cid(), msg.Name(), err)
			} else {
				return
			}
		}

		// 7. We should commit the bytes in the buffer so we can move on
		_, err = p.in.ReadCommit(total)
		if err != nil {
			if !isEOF(err) {
				log.Errorf("(%s) Error committing %d read bytes: %v", p.cid(), total, err)
			}
			return
		}

		// 7. Check to see if done is closed, if so, exit
		if p.isDone() && p.in.Len() == 0 {
			return
		}
	}
}

func (p *service) processIncoming(msg message.Message) error {
	var err error = nil

	switch msg := msg.(type) {
	case *message.PublishMessage:
		// For PUBLISH message, we should figure out what QoS it is and process accordingly
		// If QoS == 0, we should just take the next step, no ack required
		// If QoS == 1, we should send back PUBACK, then take the next step
		// If QoS == 2, we need to put it in the ack queue, send back PUBREC
		err = p.processPublish(msg)

	case *message.PubackMessage:
		// For PUBACK message, it means QoS 1, we should send to ack queue
		p.sess.Pub1ack.Ack(msg)
		p.processAcked(p.sess.Pub1ack)

	case *message.PubrecMessage:
		// For PUBREC message, it means QoS 2, we should send to ack queue, and send back PUBREL
		if err = p.sess.Pub2out.Ack(msg); err != nil {
			break
		}

		resp := message.NewPubrelMessage()
		resp.SetPacketId(msg.PacketId())
		_, err = p.writeMessage(resp)

	case *message.PubrelMessage:
		// For PUBREL message, it means QoS 2, we should send to ack queue, and send back PUBCOMP
		if err = p.sess.Pub2in.Ack(msg); err != nil {
			break
		}

		p.processAcked(p.sess.Pub2in)

		resp := message.NewPubcompMessage()
		resp.SetPacketId(msg.PacketId())
		_, err = p.writeMessage(resp)

	case *message.PubcompMessage:
		// For PUBCOMP message, it means QoS 2, we should send to ack queue
		if err = p.sess.Pub2out.Ack(msg); err != nil {
			break
		}

		p.processAcked(p.sess.Pub2out)

	case *message.SubscribeMessage:
		// For SUBSCRIBE message, we should add subscriber, then send back SUBACK
		return p.processSubscribe(msg)

	case *message.SubackMessage:
		// For SUBACK message, we should send to ack queue
		p.sess.Suback.Ack(msg)
		p.processAcked(p.sess.Suback)

	case *message.UnsubscribeMessage:
		// For UNSUBSCRIBE message, we should remove subscriber, then send back UNSUBACK
		return p.processUnsubscribe(msg)

	case *message.UnsubackMessage:
		// For UNSUBACK message, we should send to ack queue
		p.sess.Unsuback.Ack(msg)
		p.processAcked(p.sess.Unsuback)

	case *message.PingreqMessage:
		// For PINGREQ message, we should send back PINGRESP
		resp := message.NewPingrespMessage()
		_, err = p.writeMessage(resp)

	case *message.PingrespMessage:
		p.sess.Pingack.Ack(msg)
		p.processAcked(p.sess.Pingack)

	case *message.DisconnectMessage:
		// For DISCONNECT message, we should quit
		p.sess.Cmsg.SetWillFlag(false)
		return errDisconnect

	default:
		return fmt.Errorf("(%s) Invalid message type: %s", p.cid(), msg.Name())
	}

	if err != nil {
		log.Warningf("(%s) Error processing acknowledged message: %v", p.cid(), err)
	}

	return err
}

func (p *service) processAcked(ackq *sessions.Ackqueue) {
	for _, ackmsg := range ackq.Acked() {
		// Let's get the messages from the saved message byte slices.
		msg, err := ackmsg.Mtype.New()
		if err != nil {
			log.Errorf("Error creating %s message: %v", ackmsg.Mtype, err)
			continue
		}

		if _, err := msg.Decode(ackmsg.Msgbuf); err != nil {
			log.Warningf("Error decoding %s message: %v", ackmsg.Mtype, err)
			continue
		}

		ack, err := ackmsg.State.New()
		if err != nil {
			log.Errorf("Error creating %s message: %v", ackmsg.State, err)
			continue
		}

		if _, err := ack.Decode(ackmsg.Ackbuf); err != nil {
			log.Warningf("Error decoding %s message: %v", ackmsg.State, err)
			continue
		}

		//log.Debugf("(%s) Processing acknowledged message: %v", p.cid(), ack)

		// - PUBACK if it's QoS 1 message. This is on the client side.
		// - PUBREL if it's QoS 2 message. This is on the server side.
		// - PUBCOMP if it's QoS 2 message. This is on the client side.
		// - SUBACK if it's a subscribe message. This is on the client side.
		// - UNSUBACK if it's a unsubscribe message. This is on the client side.
		switch ackmsg.State {
		case message.PUBREL:
			// If ack is PUBREL, that means the QoS 2 message sent by a remote client is
			// releassed, so let's publish it to other subscribers.
			if err = p.onPublish(msg.(*message.PublishMessage)); err != nil {
				log.Warningf("(%s) Error processing acknowledged %s message: %v", p.cid(), ackmsg.Mtype, err)
			}

		case message.PUBACK, message.PUBCOMP, message.SUBACK, message.UNSUBACK, message.PINGRESP:
			// If ack is PUBACK, that means the QoS 1 message sent by this service got
			// ack'ed. There's nothing to do other than calling onComplete() below.

			// If ack is PUBCOMP, that means the QoS 2 message sent by this service got
			// ack'ed. There's nothing to do other than calling onComplete() below.

			// If ack is SUBACK, that means the SUBSCRIBE message sent by this service
			// got ack'ed. There's nothing to do other than calling onComplete() below.

			// If ack is UNSUBACK, that means the SUBSCRIBE message sent by this service
			// got ack'ed. There's nothing to do other than calling onComplete() below.

			// If ack is PINGRESP, that means the PINGREQ message sent by this service
			// got ack'ed. There's nothing to do other than calling onComplete() below.

			err = nil

		default:
			log.Warningf("(%s) Invalid acknowledged message type: %s", p.cid(), ackmsg.State)
			continue
		}

		// Call the registered onComplete function
		if ackmsg.OnComplete != nil {
			onComplete, ok := ackmsg.OnComplete.(OnCompleteFunc)
			if !ok {
				log.Errorf("Invalid OnCompleteFunc: %v", reflect.TypeOf(ackmsg.OnComplete))
			} else if onComplete != nil {
				if err := onComplete(msg, ack, nil); err != nil {
					log.Warningf("OnCompleteFunc failed: %v", err)
				}
			}
		}
	}
}

// For PUBLISH message, we should figure out what QoS it is and process accordingly
// If QoS == 0, we should just take the next step, no ack required
// If QoS == 1, we should send back PUBACK, then take the next step
// If QoS == 2, we need to put it in the ack queue, send back PUBREC
func (p *service) processPublish(msg *message.PublishMessage) error {
	switch msg.QoS() {
	case message.QosExactlyOnce:
		p.sess.Pub2in.Wait(msg, nil)

		resp := message.NewPubrecMessage()
		resp.SetPacketId(msg.PacketId())

		_, err := p.writeMessage(resp)
		return err

	case message.QosAtLeastOnce:
		resp := message.NewPubackMessage()
		resp.SetPacketId(msg.PacketId())

		if _, err := p.writeMessage(resp); err != nil {
			return err
		}

		return p.onPublish(msg)

	case message.QosAtMostOnce:
		return p.onPublish(msg)
	}

	return fmt.Errorf("(%s) invalid message QoS %d", p.cid(), msg.QoS())
}

// For SUBSCRIBE message, we should add subscriber, then send back SUBACK
func (p *service) processSubscribe(msg *message.SubscribeMessage) error {
	resp := message.NewSubackMessage()
	resp.SetPacketId(msg.PacketId())

	// Subscribe to the different topics
	var retcodes []byte

	topics := msg.Topics()
	qos := msg.Qos()

	p.rmsgs = p.rmsgs[0:0]

	for i, t := range topics {
		rqos, err := p.topicsMgr.Subscribe(t, qos[i], &p.onpub)
		if err != nil {
			return err
		}
		p.sess.AddTopic(string(t), qos[i])

		retcodes = append(retcodes, rqos)

		// yeah I am not checking errors here. If there's an error we don't want the
		// subscription to stop, just let it go.
		p.topicsMgr.Retained(t, &p.rmsgs)
		log.Debugf("(%s) Subscribing topic %q, %d retained messages", p.cid(), string(t), len(p.rmsgs))
	}

	if err := resp.AddReturnCodes(retcodes); err != nil {
		return err
	}

	if _, err := p.writeMessage(resp); err != nil {
		return err
	}

	for _, rm := range p.rmsgs {
		if err := p.publish(rm, nil); err != nil {
			log.Warningf("(%s) Error publishing retained message: %v", p.cid(), err)
			return err
		}
	}

	return nil
}

// For UNSUBSCRIBE message, we should remove the subscriber, and send back UNSUBACK
func (p *service) processUnsubscribe(msg *message.UnsubscribeMessage) error {
	topics := msg.Topics()

	for _, t := range topics {
		p.topicsMgr.Unsubscribe(t, &p.onpub)
		p.sess.RemoveTopic(string(t))
	}

	resp := message.NewUnsubackMessage()
	resp.SetPacketId(msg.PacketId())

	_, err := p.writeMessage(resp)
	return err
}

// onPublish() is called when the server receives a PUBLISH message AND have completed
// the ack cycle. This method will get the list of subscribers based on the publish
// topic, and publishes the message to the list of subscribers.
func (p *service) onPublish(msg *message.PublishMessage) error {
	if msg.Retain() {
		if err := p.topicsMgr.Retain(msg); err != nil {
			log.Warningf("(%s) Un-/Retaining of message failed: %v", p.cid(), err)
		}
	}

	err := p.topicsMgr.Subscribers(msg.Topic(), msg.QoS(), &p.subs, &p.qoss)
	if err != nil {
		log.Errorf("(%s) Error retrieving subscribers list: %v", p.cid(), err)
		return err
	}

	msg.SetRetain(false)

	for i, s := range p.subs {
		if s != nil {
			fn := s.(*OnPublishFunc)
			// use the possibly downgraded qos
			msg.SetQoS(p.qoss[i])
			if err := (*fn)(msg); err != nil {
				log.Warningf("%v", err)
			}
		}
	}

	return nil
}
