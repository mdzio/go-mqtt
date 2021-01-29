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
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/mdzio/go-mqtt/message"
	"github.com/mdzio/go-mqtt/sessions"
	"github.com/mdzio/go-mqtt/topics"
)

const (
	minKeepAlive = 30
)

// Client is a library implementation of the MQTT client that, as best it can, complies
// with the MQTT 3.1 and 3.1.1 specs.
type Client struct {
	// The number of seconds to keep the connection live if there's no data.
	// If not set then default to 5 mins.
	KeepAlive int

	// The number of seconds to wait for the CONNACK message before disconnecting.
	// If not set then default to 2 seconds.
	ConnectTimeout int

	// The number of seconds to wait for any ACK messages before failing.
	// If not set then default to 20 seconds.
	AckTimeout int

	// The number of times to retry sending a packet if ACK is not received.
	// If no set then default to 3 retries.
	TimeoutRetries int

	svc *service
}

// Connect is for MQTT clients to open a connection to a remote server. It needs to
// know the URI, e.g., "tcp://127.0.0.1:1883", so it knows where to connect to. It also
// needs to be supplied with the MQTT CONNECT message.
func (cln *Client) Connect(uri string, msg *message.ConnectMessage) (err error) {
	cln.checkConfiguration()

	if msg == nil {
		return fmt.Errorf("msg is nil")
	}

	u, err := url.Parse(uri)
	if err != nil {
		return err
	}

	if u.Scheme != "tcp" {
		return ErrInvalidConnectionType
	}

	conn, err := net.Dial(u.Scheme, u.Host)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			conn.Close()
		}
	}()

	if msg.KeepAlive() < minKeepAlive {
		msg.SetKeepAlive(minKeepAlive)
	}

	if err = writeMessage(conn, msg); err != nil {
		return err
	}

	conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(cln.ConnectTimeout)))

	resp, err := getConnackMessage(conn)
	if err != nil {
		return err
	}

	if resp.ReturnCode() != message.ConnectionAccepted {
		return resp.ReturnCode()
	}

	cln.svc = &service{
		id:     atomic.AddUint64(&gsvcid, 1),
		client: true,
		conn:   conn,

		keepAlive:      int(msg.KeepAlive()),
		connectTimeout: cln.ConnectTimeout,
		ackTimeout:     cln.AckTimeout,
		timeoutRetries: cln.TimeoutRetries,
	}

	err = cln.getSession(cln.svc, msg, resp)
	if err != nil {
		return err
	}

	p := topics.NewMemProvider()
	topics.Register(cln.svc.sess.ID(), p)

	cln.svc.topicsMgr, err = topics.NewManager(cln.svc.sess.ID())
	if err != nil {
		return err
	}

	if err := cln.svc.start(); err != nil {
		cln.svc.stop()
		return err
	}

	cln.svc.inStat.increment(int64(msg.Len()))
	cln.svc.outStat.increment(int64(resp.Len()))

	return nil
}

// ConnectTLS is for MQTT clients to open a TLS connection to a remote server.
func (cln *Client) ConnectTLS(uri string, msg *message.ConnectMessage, cfg *tls.Config) (err error) {
	cln.checkConfiguration()

	if msg == nil {
		return fmt.Errorf("msg is nil")
	}

	u, err := url.Parse(uri)
	if err != nil {
		return err
	}

	if u.Scheme != "tcp" {
		return ErrInvalidConnectionType
	}

	conn, err := tls.Dial(u.Scheme, u.Host, cfg)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			conn.Close()
		}
	}()

	if msg.KeepAlive() < minKeepAlive {
		msg.SetKeepAlive(minKeepAlive)
	}

	if err = writeMessage(conn, msg); err != nil {
		return err
	}

	conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(cln.ConnectTimeout)))

	resp, err := getConnackMessage(conn)
	if err != nil {
		return err
	}

	if resp.ReturnCode() != message.ConnectionAccepted {
		return resp.ReturnCode()
	}

	cln.svc = &service{
		id:     atomic.AddUint64(&gsvcid, 1),
		client: true,
		conn:   conn,

		keepAlive:      int(msg.KeepAlive()),
		connectTimeout: cln.ConnectTimeout,
		ackTimeout:     cln.AckTimeout,
		timeoutRetries: cln.TimeoutRetries,
	}

	err = cln.getSession(cln.svc, msg, resp)
	if err != nil {
		return err
	}

	p := topics.NewMemProvider()
	topics.Register(cln.svc.sess.ID(), p)

	cln.svc.topicsMgr, err = topics.NewManager(cln.svc.sess.ID())
	if err != nil {
		return err
	}

	if err := cln.svc.start(); err != nil {
		cln.svc.stop()
		return err
	}

	cln.svc.inStat.increment(int64(msg.Len()))
	cln.svc.outStat.increment(int64(resp.Len()))

	return nil
}

// Publish sends a single MQTT PUBLISH message to the server. On completion, the
// supplied OnCompleteFunc is called. For QOS 0 messages, onComplete is called
// immediately after the message is sent to the outgoing buffer. For QOS 1 messages,
// onComplete is called when PUBACK is received. For QOS 2 messages, onComplete is
// called after the PUBCOMP message is received.
func (cln *Client) Publish(msg *message.PublishMessage, onComplete OnCompleteFunc) error {
	return cln.svc.publish(msg, onComplete)
}

// Subscribe sends a single SUBSCRIBE message to the server. The SUBSCRIBE message
// can contain multiple topics that the client wants to subscribe to. On completion,
// which is when the client receives a SUBACK messsage back from the server, the
// supplied onComplete funciton is called.
//
// When messages are sent to the client from the server that matches the topics the
// client subscribed to, the onPublish function is called to handle those messages.
// So in effect, the client can supply different onPublish functions for different
// topics.
func (cln *Client) Subscribe(msg *message.SubscribeMessage, onComplete OnCompleteFunc, onPublish OnPublishFunc) error {
	return cln.svc.subscribe(msg, onComplete, onPublish)
}

// Unsubscribe sends a single UNSUBSCRIBE message to the server. The UNSUBSCRIBE
// message can contain multiple topics that the client wants to unsubscribe. On
// completion, which is when the client receives a UNSUBACK message from the server,
// the supplied onComplete function is called. The client will no longer handle
// messages from the server for those unsubscribed topics.
func (cln *Client) Unsubscribe(msg *message.UnsubscribeMessage, onComplete OnCompleteFunc) error {
	return cln.svc.unsubscribe(msg, onComplete)
}

// Ping sends a single PINGREQ message to the server. PINGREQ/PINGRESP messages are
// mainly used by the client to keep a heartbeat to the server so the connection won't
// be dropped.
func (cln *Client) Ping(onComplete OnCompleteFunc) error {
	return cln.svc.ping(onComplete)
}

// Disconnect sends a single DISCONNECT message to the server. The client immediately
// terminates after the sending of the DISCONNECT message.
func (cln *Client) Disconnect() {
	//msg := message.NewDisconnectMessage()
	cln.svc.stop()
}

func (cln *Client) getSession(svc *service, req *message.ConnectMessage, resp *message.ConnackMessage) error {
	//id := string(req.ClientId())
	svc.sess = &sessions.Session{}
	return svc.sess.Init(req)
}

func (cln *Client) checkConfiguration() {
	if cln.KeepAlive == 0 {
		cln.KeepAlive = DefaultKeepAlive
	}

	if cln.ConnectTimeout == 0 {
		cln.ConnectTimeout = DefaultConnectTimeout
	}

	if cln.AckTimeout == 0 {
		cln.AckTimeout = DefaultAckTimeout
	}

	if cln.TimeoutRetries == 0 {
		cln.TimeoutRetries = DefaultTimeoutRetries
	}
}
