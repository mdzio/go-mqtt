package service

import (
	"strconv"
	"testing"

	"github.com/mdzio/go-lib/testutil"
	"github.com/mdzio/go-mqtt/message"
)

func TestClient(t *testing.T) {
	c := new(Client)

	msg := message.NewConnectMessage()
	msg.SetWillQos(1)
	msg.SetVersion(byte(4))
	msg.SetCleanSession(true)
	msg.SetClientID([]byte("test client"))
	msg.SetKeepAlive(10)
	msg.SetWillTopic([]byte("will"))
	msg.SetWillMessage([]byte("send me home"))
	msg.SetUsername([]byte(testutil.Config(t, "MQTT_SERVER_USER")))
	msg.SetPassword([]byte(testutil.Config(t, "MQTT_SERVER_PASSWD")))

	err := c.Connect(testutil.Config(t, "MQTT_SERVER_ADDR"), msg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect()
	log.Info("Connected")

	submsg := message.NewSubscribeMessage()
	if err := submsg.AddTopic([]byte("#"), 0); err != nil {
		t.Fatalf("Adding remote topic failed: %v", err)
	}

	onCompleteCalled := make(chan struct{})
	var onComplete OnCompleteFunc = func(msg, ack message.Message, err error) error {
		defer func() {
			close(onCompleteCalled)
		}()
		if err != nil {
			log.Errorf("Subscribing remote topic failed: %v", err)
		} else {
			log.Info("Subscribing remote topic succeeded")
		}
		return nil
	}

	onPublishCalled := make(chan struct{})
	var onPublish OnPublishFunc = func(pubmsg *message.PublishMessage) error {
		defer func() {
			select {
			case onPublishCalled <- struct{}{}:
			default:
			}
		}()
		rt := string(pubmsg.Topic())
		log.Infof("Incoming message on remote topic %s, retain %t with payload %s", rt, pubmsg.Retain(), string(pubmsg.Payload()))
		return nil
	}
	if err := c.Subscribe(submsg, onComplete, onPublish); err != nil {
		t.Fatalf("Subscribing remote topic failed: %v", err)
	}

	<-onCompleteCalled
	<-onPublishCalled
}

func TestClient2(t *testing.T) {
	c := new(Client)

	msg := message.NewConnectMessage()
	msg.SetVersion(byte(4))
	msg.SetCleanSession(true)
	msg.SetClientID([]byte("test client"))
	msg.SetKeepAlive(10)
	msg.SetUsername([]byte(testutil.Config(t, "MQTT_SERVER_USER")))
	msg.SetPassword([]byte(testutil.Config(t, "MQTT_SERVER_PASSWD")))

	err := c.Connect(testutil.Config(t, "MQTT_SERVER_ADDR"), msg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect()
	log.Info("Connected")

	pubmsg := message.NewPublishMessage()
	pubmsg.SetTopic([]byte("test-topic"))
	pubmsg.SetQoS(0)

	for cnt := 10; cnt >= 0; cnt-- {
		var onComplete OnCompleteFunc = func(msg, ack message.Message, err error) error {
			if err != nil {
				log.Errorf("OnComplete: Publishing failed: %v", err)
			} else {
				log.Trace("OnComplete: Publishing succeeded")
			}
			return nil
		}
		pubmsg.SetPayload([]byte(strconv.Itoa(cnt)))
		if err := c.Publish(pubmsg, onComplete); err != nil {
			t.Fatalf("Publishing failed: %v", err)
		}
	}
}
