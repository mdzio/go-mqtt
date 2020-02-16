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
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mdzio/go-mqtt/auth"
	"github.com/mdzio/go-mqtt/message"
	"github.com/mdzio/go-mqtt/sessions"
	"github.com/mdzio/go-mqtt/topics"
)

// List of errors.
var (
	ErrInvalidConnectionType  error = errors.New("service: Invalid connection type")
	ErrInvalidSubscriber      error = errors.New("service: Invalid subscriber")
	ErrBufferNotReady         error = errors.New("service: buffer is not ready")
	ErrBufferInsufficientData error = errors.New("service: buffer has insufficient data")
)

// Default server configuration.
const (
	DefaultKeepAlive        = 300
	DefaultConnectTimeout   = 2
	DefaultAckTimeout       = 20
	DefaultTimeoutRetries   = 3
	DefaultSessionsProvider = "mem"
	DefaultAuthenticator    = "mockSuccess"
	DefaultTopicsProvider   = "mem"
)

// Server is a library implementation of the MQTT server that, as best it can, complies
// with the MQTT 3.1 and 3.1.1 specs.
type Server struct {
	// The number of seconds to keep the connection live if there's no data.
	// If not set then default to 5 mins.
	KeepAlive int

	// The number of seconds to wait for the CONNECT message before disconnecting.
	// If not set then default to 2 seconds.
	ConnectTimeout int

	// The number of seconds to wait for any ACK messages before failing.
	// If not set then default to 20 seconds.
	AckTimeout int

	// The number of times to retry sending a packet if ACK is not received.
	// If no set then default to 3 retries.
	TimeoutRetries int

	// Authenticator is the authenticator used to check username and password sent
	// in the CONNECT message. If not set then default to "mockSuccess".
	Authenticator string

	// SessionsProvider is the session store that keeps all the Session objects.
	// This is the store to check if CleanSession is set to 0 in the CONNECT message.
	// If not set then default to "mem".
	SessionsProvider string

	// TopicsProvider is the topic store that keeps all the subscription topics.
	// If not set then default to "mem".
	TopicsProvider string

	// authMgr is the authentication manager that we are going to use for authenticating
	// incoming connections
	authMgr *auth.Manager

	// sessMgr is the sessions manager for keeping track of the sessions
	sessMgr *sessions.Manager

	// topicsMgr is the topics manager for keeping track of subscriptions
	topicsMgr *topics.Manager

	// The quit channel for the server. If the server detects that this channel
	// is closed, then it's a signal for it to shutdown as well.
	quit chan struct{}

	ln    net.Listener
	lntls net.Listener

	// A list of services created by the server. We keep track of them so we can
	// gracefully shut them down if they are still alive when the server goes down.
	svcs []*service

	// Mutex for updating svcs
	mu sync.Mutex

	// A indicator on whether this server has already checked configuration
	configOnce sync.Once
}

// ListenAndServe listents to connections on the URI requested, and handles any
// incoming MQTT client sessions. It should not return until Close() is called
// or if there's some critical error that stops the server from running. The URI
// supplied should be of the form "protocol://host:port" that can be parsed by
// url.Parse(). For example, an URI could be "tcp://0.0.0.0:1883".
func (svr *Server) ListenAndServe(uri string) error {
	if err := svr.checkConfiguration(); err != nil {
		return err
	}

	u, err := url.Parse(uri)
	if err != nil {
		return err
	}

	svr.ln, err = net.Listen(u.Scheme, u.Host)
	if err != nil {
		return err
	}
	defer svr.ln.Close()

	log.Trace("Listening for MQTT connections")

	var tempDelay time.Duration // how long to sleep on accept failure

	for {
		conn, err := svr.ln.Accept()

		if err != nil {
			// http://zhen.org/blog/graceful-shutdown-of-go-net-dot-listeners/
			select {
			case <-svr.quit:
				return nil

			default:
			}

			// Borrowed from go1.3.3/src/pkg/net/http/server.go:1699
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				log.Warningf("Accept error: %v", err)
				time.Sleep(tempDelay)
				continue
			}
			return err
		}

		go svr.handleConnection(conn)
	}
}

// ListenAndServeTLS listents to connections on the URI requested, and handles
// any incoming MQTT client sessions.  It should not return until Close() is
// called or if there's some critical error that stops the server from running.
// The URI supplied should be of the form "protocol://host:port" that can be
// parsed by url.Parse(). For example, an URI could be "tcp://0.0.0.0:8883".
func (svr *Server) ListenAndServeTLS(uri string, cfg *tls.Config) error {
	if err := svr.checkConfiguration(); err != nil {
		return err
	}

	u, err := url.Parse(uri)
	if err != nil {
		return err
	}

	svr.lntls, err = tls.Listen(u.Scheme, u.Host, cfg)
	if err != nil {
		return err
	}
	defer svr.lntls.Close()

	log.Trace("Listening for Secure MQTT connections")

	var tempDelay time.Duration // how long to sleep on accept failure

	for {
		conn, err := svr.lntls.Accept()

		if err != nil {
			// http://zhen.org/blog/graceful-shutdown-of-go-net-dot-listeners/
			select {
			case <-svr.quit:
				return nil

			default:
			}

			// Borrowed from go1.3.3/src/pkg/net/http/server.go:1699
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				log.Warningf("Accept error: %v", err)
				time.Sleep(tempDelay)
				continue
			}
			return err
		}

		go svr.handleConnection(conn)
	}
}

// Publish sends a single MQTT PUBLISH message to the server. msg is modified.
func (svr *Server) Publish(msg *message.PublishMessage) error {
	if err := svr.checkConfiguration(); err != nil {
		return err
	}

	if msg.Retain() {
		if err := svr.topicsMgr.Retain(msg); err != nil {
			log.Warningf("Un-/Retaining of message failed: %v", err)
		}
	}

	var subs []interface{}
	var qoss []byte

	if err := svr.topicsMgr.Subscribers(msg.Topic(), msg.QoS(), &subs, &qoss); err != nil {
		return err
	}

	msg.SetRetain(false)

	for i, s := range subs {
		if s != nil {
			fn := s.(*OnPublishFunc)
			// use the possibly downgraded qos
			msg.SetQoS(qoss[i])
			if err := (*fn)(msg); err != nil {
				log.Warningf("%v", err)
			}
		}
	}

	return nil
}

// Subscribe registers a callback for a topic.
func (svr *Server) Subscribe(topic string, qos byte, onPublish *OnPublishFunc) error {
	log.Debugf("Subscribing topic %q with QoS %v for callback %p", topic, qos, onPublish)
	if err := svr.checkConfiguration(); err != nil {
		return err
	}
	if _, err := svr.topicsMgr.Subscribe([]byte(topic), qos, onPublish); err != nil {
		return err
	}
	return nil
}

// Unsubscribe deregisters a callback for a topic.
func (svr *Server) Unsubscribe(topic string, onPublish *OnPublishFunc) error {
	log.Debugf("Unsubscribing topic %q for callback %p", topic, onPublish)
	if err := svr.checkConfiguration(); err != nil {
		return err
	}
	if err := svr.topicsMgr.Unsubscribe([]byte(topic), onPublish); err != nil {
		return err
	}
	return nil
}

// Close terminates the server by shutting down all the client connections and closing
// the listener. It will, as best it can, clean up after itself.
func (svr *Server) Close() error {
	// By closing the quit channel, we are telling the server to stop accepting new
	// connection.
	close(svr.quit)

	// We then close the net.Listener, which will force Accept() to return if it's
	// blocked waiting for new connections.
	if svr.ln != nil {
		svr.ln.Close()
	}
	if svr.lntls != nil {
		svr.lntls.Close()
	}

	for _, svc := range svr.svcs {
		log.Tracef("Stopping service: %d", svc.id)
		svc.stop()
	}

	if svr.sessMgr != nil {
		svr.sessMgr.Close()
	}

	if svr.topicsMgr != nil {
		svr.topicsMgr.Close()
	}

	return nil
}

// HandleConnection is for the broker to handle an incoming connection from a client
func (svr *Server) handleConnection(c io.Closer) (svc *service, err error) {
	if c == nil {
		return nil, ErrInvalidConnectionType
	}

	defer func() {
		if err != nil {
			c.Close()
		}
	}()

	conn, ok := c.(net.Conn)
	if !ok {
		return nil, ErrInvalidConnectionType
	}

	log.Tracef("Client %s is connecting", conn.RemoteAddr())

	// To establish a connection, we must
	// 1. Read and decode the message.ConnectMessage from the wire
	// 2. If no decoding errors, then authenticate using username and password.
	//    Otherwise, write out to the wire message.ConnackMessage with
	//    appropriate error.
	// 3. If authentication is successful, then either create a new session or
	//    retrieve existing session
	// 4. Write out to the wire a successful message.ConnackMessage message

	// Read the CONNECT message from the wire, if error, then check to see if it's
	// a CONNACK error. If it's CONNACK error, send the proper CONNACK error back
	// to client. Exit regardless of error type.

	conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(svr.ConnectTimeout)))

	resp := message.NewConnackMessage()

	req, err := getConnectMessage(conn)
	if err != nil {
		log.Debugf("Decoding of connect message failed: %v", err)
		if cerr, ok := err.(message.ConnackCode); ok {
			resp.SetReturnCode(cerr)
			resp.SetSessionPresent(false)
			writeMessage(conn, resp)
		}
		return nil, err
	}

	// Authenticate the user, if error, return error and exit
	user := string(req.Username())
	log.Tracef("Authenticating user: %s", user)
	if err = svr.authMgr.Authenticate(user, string(req.Password())); err != nil {
		log.Debugf("Authentication of user %s failed: %v", user, err)
		resp.SetReturnCode(message.ErrBadUsernameOrPassword)
		resp.SetSessionPresent(false)
		writeMessage(conn, resp)
		return nil, err
	}

	if req.KeepAlive() == 0 {
		req.SetKeepAlive(minKeepAlive)
	}

	svc = &service{
		id:     atomic.AddUint64(&gsvcid, 1),
		client: false,

		keepAlive:      int(req.KeepAlive()),
		connectTimeout: svr.ConnectTimeout,
		ackTimeout:     svr.AckTimeout,
		timeoutRetries: svr.TimeoutRetries,

		conn:      conn,
		sessMgr:   svr.sessMgr,
		topicsMgr: svr.topicsMgr,
	}

	err = svr.getSession(svc, req, resp)
	if err != nil {
		return nil, err
	}

	resp.SetReturnCode(message.ConnectionAccepted)

	if err = writeMessage(c, resp); err != nil {
		return nil, err
	}

	svc.inStat.increment(int64(req.Len()))
	svc.outStat.increment(int64(resp.Len()))

	if err := svc.start(); err != nil {
		svc.stop()
		return nil, err
	}

	svr.mu.Lock()
	svr.svcs = append(svr.svcs, svc)
	svr.mu.Unlock()

	log.Debugf("(%s) Connection established", svc.cid())

	return svc, nil
}

func (svr *Server) checkConfiguration() error {
	var err error

	svr.configOnce.Do(func() {
		if svr.KeepAlive == 0 {
			svr.KeepAlive = DefaultKeepAlive
		}

		if svr.ConnectTimeout == 0 {
			svr.ConnectTimeout = DefaultConnectTimeout
		}

		if svr.AckTimeout == 0 {
			svr.AckTimeout = DefaultAckTimeout
		}

		if svr.TimeoutRetries == 0 {
			svr.TimeoutRetries = DefaultTimeoutRetries
		}

		if svr.Authenticator == "" {
			svr.Authenticator = "mockSuccess"
		}
		svr.authMgr, err = auth.NewManager(svr.Authenticator)
		if err != nil {
			return
		}

		if svr.SessionsProvider == "" {
			svr.SessionsProvider = "mem"
		}
		svr.sessMgr, err = sessions.NewManager(svr.SessionsProvider)
		if err != nil {
			return
		}

		if svr.TopicsProvider == "" {
			svr.TopicsProvider = "mem"
		}
		svr.topicsMgr, err = topics.NewManager(svr.TopicsProvider)

		svr.quit = make(chan struct{})
		return
	})

	return err
}

func (svr *Server) getSession(svc *service, req *message.ConnectMessage, resp *message.ConnackMessage) error {
	// If CleanSession is set to 0, the server MUST resume communications with the
	// client based on state from the current session, as identified by the client
	// identifier. If there is no session associated with the client identifier the
	// server must create a new session.
	//
	// If CleanSession is set to 1, the client and server must discard any previous
	// session and start a new one. This session lasts as long as the network c
	// onnection. State data associated with this session must not be reused in any
	// subsequent session.

	var err error

	// Check to see if the client supplied an ID, if not, generate one and set
	// clean session.
	if len(req.ClientId()) == 0 {
		req.SetClientId([]byte(fmt.Sprintf("internalclient%d", svc.id)))
		req.SetCleanSession(true)
	}

	cid := string(req.ClientId())

	// If CleanSession is NOT set, check the session store for existing session.
	// If found, return it.
	if !req.CleanSession() {
		if svc.sess, err = svr.sessMgr.Get(cid); err == nil {
			resp.SetSessionPresent(true)

			if err := svc.sess.Update(req); err != nil {
				return err
			}
		}
	}

	// If CleanSession, or no existing session found, then create a new one
	if svc.sess == nil {
		if svc.sess, err = svr.sessMgr.New(cid); err != nil {
			return err
		}

		resp.SetSessionPresent(false)

		if err := svc.sess.Init(req); err != nil {
			return err
		}
	}

	return nil
}
