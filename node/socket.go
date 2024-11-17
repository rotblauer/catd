package node

import (
	"encoding/json"
	"github.com/rotblauer/catd/catdb/cache"
	"github.com/rotblauer/catd/events"
	"github.com/rotblauer/catd/types/cattrack"
	"log"
	"log/slog"

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

	// Right now don't care about incoming messages from clients. Log and drop.
	m.HandleMessage(loggingHandler)

	m.HandleDisconnect(func(s *melody.Session) {
		log.Println("[websocket] disconnected", s.Request.RemoteAddr)
	})

	m.HandleError(func(s *melody.Session, e error) {
		log.Println("[websocket] error", e, s.Request.RemoteAddr)
	})

	// Broadcast track push events (i.e. 'populate') - as received - to all connected clients.
	// This can result in invalid or duplicate cat tracks being sent to clients,
	// if they cat sends them to us.
	// Cat track population WILL ENFORCE validation and deduplication, etc. -
	// but THIS DATA IS NOT THE ULTIMATELY STORED DATA.
	// It is the data the cat sent us.
	pushes := make(chan []*cattrack.CatTrack)
	pushSub := events.HTTPPopulateFeed.Subscribe(pushes)
	go func() {
		for {
			select {
			case features := <-pushes:
				bc := broadcats{
					Action:   websocketActionPopulate,
					Features: features,
				}
				b, err := json.Marshal(bc)
				if err != nil {
					slog.Error("Failed to marshal populate event", "error", err)
					continue
				}
				if err := m.Broadcast(b); err != nil {
					slog.Warn("Failed to broadcast populate event", "error", err)
				}
			case err := <-pushSub.Err():
				slog.Error("Failed to subscribe to HTTPPopulateFeed", "error", err)
				return
			}
		}
	}()

	return m
}

// on request
func loggingHandler(s *melody.Session, msg []byte) {
	log.Println("[websocket] message", string(msg))
}