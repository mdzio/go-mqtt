package service

import (
	"net"
	"net/http"

	"github.com/gorilla/websocket"
)

var wsUpgrader = websocket.Upgrader{
	Subprotocols: []string{"mqtt"},
	CheckOrigin:  func(r *http.Request) bool { return true },
}

// WebsocketHandler provides an http.Handler that forwards MQTT websocket
// requests to a plain TCP socket.
type WebsocketHandler struct {
	// target address, e.g. ":1883"
	Addr string
}

// ServeHTTP implements http.Handler.
func (wh *WebsocketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// upgrade connection to websocket
	wscon, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Debugf("Upgrade to websocket failed (remote %s): %v", r.RemoteAddr, err)
		return
	}
	log.Debugf("Websocket connection from %s", wscon.RemoteAddr())
	defer log.Tracef("Closing websocket connection from %s", wscon.RemoteAddr())
	defer wscon.Close()

	// connect to MQTT server
	tcpcon, err := net.Dial("tcp", wh.Addr)
	if err != nil {
		log.Errorf("Connecting websocket to %s failed: %v", wh.Addr, err)
		return
	}
	defer tcpcon.Close()

	// send to websocket
	go func() {
		defer wscon.Close()
		defer tcpcon.Close()
		for {
			buffer := make([]byte, 1024)
			n, err := tcpcon.Read(buffer)
			if err != nil || n == 0 {
				return
			}
			err = wscon.WriteMessage(websocket.BinaryMessage, buffer[:n])
			if err != nil {
				return
			}
		}
	}()

	// receive from websocket
	for {
		msgType, msg, err := wscon.ReadMessage()
		if err != nil {
			return
		}
		if msgType != websocket.BinaryMessage {
			log.Warning("Non binary websocket message received from %s", wscon.RemoteAddr())
		}
		n, err := tcpcon.Write(msg)
		if err != nil || n == 0 {
			return
		}
	}
}
