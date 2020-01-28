# go-mqtt

go-mqtt is a high performance MQTT broker and client library that aims to be fully compliant with MQTT 3.1.1 specification. This project is a continuation of the no longer maintained and further developed [SurgeMQ](https://github.com/zentures/surgemq).

## Features

* Supports QOS 0, 1 and 2 messages.
* Supports will messages.
* Supports retained messages (add/remove).
* Pretty much everything in the specification except for the list below.

## Limitations

* All features supported are in memory only. Once the server restarts everything is cleared.
  * However, all the components are written to be pluggable so one can write plugins based on the Go interfaces defined.
* Message redelivery on reconnect is not currently supported.
* Message offline queueing on disconnect is not supported. Though this is also not a specific requirement for MQTT.

## Performance

Current performance benchmark, running all publishers, subscribers and broker on a single 4-core (2.8 Ghz i7) MacBook Pro:

* Over 400,000 MPS in a 1:1 single publisher and single producer configuration.
* Over 450,000 MPS in a 20:1 fan-in configuration.
* Over 750,000 MPS in a 1:20 fan-out configuration.
* Over 700,000 MPS in a full mesh configuration with 20 clients.

## Compatibility

In addition, this library has been tested with the following client libraries, and it _seems_ to work:

* libmosquitto 1.3.5 (in C)
  * Tested with the bundled test programs msgsps_pub and msgsps_sub.
* Paho MQTT Conformance/Interoperability Testing Suite (in Python). Tested with all 10 test cases, 3 did not pass. They are:
  1. "offline messages queueing test" which is not supported by go-mqtt.
  2. "redelivery on reconnect test" which is not yet implemented by go-mqtt.
  3. "run subscribe failure test" which is not a valid test.
* Paho Go Client Library (in Go)
  * Tested with one of the tests in the library, in fact, that tests is now part of the tests for go-mqtt.
* Paho C Client library (in C)
  * Tested with most of the test cases and failed the same ones as the conformance test because the features are not yet implemented.
  * Actually I think there's a bug in the test suite as it calls the PUBLISH handler function for non-PUBLISH messages.

## Server Example

```
// Create a new server
svr := &service.Server{
    KeepAlive:        300,               // seconds
    ConnectTimeout:   2,                 // seconds
    SessionsProvider: "mem",             // keeps sessions in memory
    Authenticator:    "mockSuccess",     // always succeed
    TopicsProvider:   "mem",             // keeps topic subscriptions in memory
}

// Listen and serve connections at localhost:1883
svr.ListenAndServe("tcp://:1883")
```

## Client Example

```
// Instantiates a new Client
c := &Client{}

// Creates a new MQTT CONNECT message and sets the proper parameters
msg := message.NewConnectMessage()
msg.SetWillQos(1)
msg.SetVersion(4)
msg.SetCleanSession(true)
msg.SetClientId([]byte("go-mqtt"))
msg.SetKeepAlive(10)
msg.SetWillTopic([]byte("will"))
msg.SetWillMessage([]byte("send me home"))
msg.SetUsername([]byte("go-mqtt"))
msg.SetPassword([]byte("verysecret"))

// Connects to the remote server at 127.0.0.1 port 1883
c.Connect("tcp://127.0.0.1:1883", msg)

// Creates a new SUBSCRIBE message to subscribe to topic "abc"
submsg := message.NewSubscribeMessage()
submsg.AddTopic([]byte("abc"), 0)

// Subscribes to the topic by sending the message. The first nil in the function
// call is a OnCompleteFunc that should handle the SUBACK message from the server.
// Nil means we are ignoring the SUBACK messages. The second nil should be a
// OnPublishFunc that handles any messages send to the client because of this
// subscription. Nil means we are ignoring any PUBLISH messages for this topic.
c.Subscribe(submsg, nil, nil)

// Creates a new PUBLISH message with the appropriate contents for publishing
pubmsg := message.NewPublishMessage()
pubmsg.SetPacketId(pktid)
pubmsg.SetTopic([]byte("abc"))
pubmsg.SetPayload(make([]byte, 1024))
pubmsg.SetQoS(qos)

// Publishes to the server by sending the message
c.Publish(pubmsg, nil)

// Disconnects from the server
c.Disconnect()
```

## License

This work is licensed under the [Apache License V2](LICENSE.txt).

## Authors

The authors are listed in the file [AUTHORS.txt](AUTHORS.txt).
