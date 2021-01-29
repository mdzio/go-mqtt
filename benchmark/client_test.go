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

package benchmark

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/mdzio/go-logging"
	"github.com/mdzio/go-mqtt/message"
	"github.com/mdzio/go-mqtt/service"
)

var (
	messages    int    = 100000
	publishers  int    = 5
	subscribers int    = 5
	size        int    = 64
	topic       []byte = []byte("test")
	qos         byte   = 0
	nap         int    = 20
	host        string = "127.0.0.1"
	port        int    = 1883
	user        string = ""
	pass        string = ""
	version     int    = 4

	log = logging.Get("test")

	subdone, rcvdone, sentdone int64

	done, done2 chan struct{}

	totalSent,
	totalSentTime,
	totalRcvd,
	totalRcvdTime,
	sentSince,
	rcvdSince int64

	statMu sync.Mutex
)

func init() {
	e := os.Getenv("MQTT_BENCHMARK_HOST")
	if e != "" {
		host = e
	}
	var l logging.LogLevel
	err := l.Set(os.Getenv("LOG_LEVEL"))
	if err == nil {
		logging.SetLevel(l)
	}
}

func runClientTest(t testing.TB, cid int, wg *sync.WaitGroup, f func(*service.Client)) {
	defer wg.Done()

	if size < 10 {
		size = 10
	}

	uri := "tcp://" + host + ":" + strconv.Itoa(port)

	c := connectToServer(t, uri, cid)
	if c == nil {
		return
	}

	if f != nil {
		f(c)
	}

	c.Disconnect()
}

func connectToServer(t testing.TB, uri string, cid int) *service.Client {
	c := &service.Client{}

	msg := newConnectMessage(cid)

	err := c.Connect(uri, msg)
	if err != nil {
		t.Fatal(err)
	}

	return c
}

func newSubscribeMessage(topic string, qos byte) *message.SubscribeMessage {
	msg := message.NewSubscribeMessage()
	msg.SetPacketID(1)
	msg.AddTopic([]byte(topic), qos)

	return msg
}

func newPublishMessageLarge(qos byte) *message.PublishMessage {
	msg := message.NewPublishMessage()
	msg.SetTopic([]byte("test"))
	msg.SetPayload(make([]byte, 1024))
	msg.SetQoS(qos)

	return msg
}

func newConnectMessage(cid int) *message.ConnectMessage {
	msg := message.NewConnectMessage()
	msg.SetWillQos(1)
	msg.SetVersion(byte(version))
	msg.SetCleanSession(true)
	msg.SetClientID([]byte(fmt.Sprintf("surgemq%d", cid)))
	msg.SetKeepAlive(10)
	msg.SetWillTopic([]byte("will"))
	msg.SetWillMessage([]byte("send me home"))
	msg.SetUsername([]byte(user))
	msg.SetPassword([]byte(pass))

	return msg
}
