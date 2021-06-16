package service

import (
	"testing"

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
	msg.SetUsername([]byte(""))
	msg.SetPassword([]byte(""))

	err := c.Connect("tcp://localhost:1883", msg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect()
}
