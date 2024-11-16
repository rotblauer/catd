package node

import (
	"encoding/json"
	"github.com/rotblauer/catd/catdb/cache"
	"github.com/rotblauer/catd/types/cattrack"
	"log"

	"github.com/olahol/melody"
)

type websocketAction string

var websocketActionPopulate websocketAction = "populate"

type broadcats struct {
	Action   websocketAction      `json:"action"`
	Features []*cattrack.CatTrack `json:"features"`
}

// m is a global melody instance.
var m *melody.Melody

// GetMelody does stuff
func GetMelody() *melody.Melody {
	return m
}

// InitMelody sets up the websocket handler.
func InitMelody() *melody.Melody {
	m = melody.New()

	// Incoming message about updated query params.
	m.HandleConnect(func(s *melody.Session) {
		log.Println("[websocket] connected", s.Request.RemoteAddr)
		for _, v := range cache.LastPushTTLCache.Items() {
			features := v.Value()
			bc := broadcats{
				Action:   websocketActionPopulate,
				Features: features,
			}
			b, _ := json.Marshal(bc)
			s.Write(b)
		}
	})
	m.HandleDisconnect(func(s *melody.Session) {
		log.Println("[websocket] disconnected", s.Request.RemoteAddr)
	})
	m.HandleError(func(s *melody.Session, e error) {
		log.Println("[websocket] error", e, s.Request.RemoteAddr)
	})
	m.HandleMessage(loggingHandler)
	return m
}

// on request
func loggingHandler(s *melody.Session, msg []byte) {
	log.Println("[websocket] message", string(msg))
}
