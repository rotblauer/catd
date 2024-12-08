package webd

import (
	"fmt"
	"github.com/ethereum/go-ethereum/event"
	"github.com/gorilla/mux"
	"github.com/olahol/melody"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
	"log"
	"log/slog"
	"net/http"
)

type WebDaemon struct {
	Config         *params.WebDaemonConfig
	logger         *slog.Logger
	melodyInstance *melody.Melody
	feedPopulated  event.FeedOf[[]*cattrack.CatTrack]
}

func NewWebDaemon(config *params.WebDaemonConfig) *WebDaemon {
	if config == nil {
		config = params.DefaultWebDaemonConfig()
	}
	return &WebDaemon{
		Config: config,

		logger:        slog.With("d", "web"),
		feedPopulated: event.FeedOf[[]*cattrack.CatTrack]{},
	}
}

// Run starts the HTTP server (ListenAndServe) and waits for it,
// returning any server error.
func (s *WebDaemon) Run() error {
	router := s.NewRouter()
	http.Handle("/", router)
	listeningOn := fmt.Sprintf("%s:%d", s.Config.NetAddr, s.Config.NetPort)
	log.Printf("Starting web daemon on %s", listeningOn)
	return http.ListenAndServe(listeningOn, nil)
}

func (s *WebDaemon) NewRouter() *mux.Router {

	// Handle websocket.
	s.initMelody()
	http.HandleFunc("/socat", func(w http.ResponseWriter, r *http.Request) {
		_ = s.melodyInstance.HandleRequest(w, r)
	})

	/*
		StrictSlash defines the trailing slash behavior for new routes. The initial value is false.
		When true, if the route path is "/path/", accessing "/path" will perform a redirect to the former and vice versa. In other words, your application will always see the path as specified in the route.
		When false, if the route path is "/path", accessing "/path/" will not match this route and vice versa.
		The re-direct is a HTTP 301 (Moved Permanently). Note that when this is set for routes with a non-idempotent method (e.g. POST, PUT), the subsequent re-directed request will be made as a GET by most clients. Use middleware or client settings to modify this behaviour as needed.
		Special case: when a route sets a path prefix using the PathPrefix() method, strict slash is ignored for that route because the redirect behavior can't be determined from a prefix alone. However, any subrouters created from that route inherit the original StrictSlash setting
	*/
	router := mux.NewRouter().StrictSlash(false)
	router.Use(loggingMiddleware)

	apiRoutes := router.NewRoute().Subrouter()

	// All API routes use permissive CORS settings.
	apiRoutes.Use(permissiveCorsMiddleware)

	// /ping is a simple server healthcheck endpoint
	apiRoutes.Path("/ping").HandlerFunc(pingPong)

	// TODO Ideally maybe move API URIs to /v2/paths.

	apiJSONRoutes := apiRoutes.NewRoute().Subrouter()
	jsonMiddleware := contentTypeMiddlewareFunc("application/json")
	apiJSONRoutes.Use(jsonMiddleware)

	apiJSONRoutes.Path("/last").HandlerFunc(handleLastTracks).Methods(http.MethodGet)
	apiJSONRoutes.Path("/catsnaps").HandlerFunc(handleGetCatSnaps).Methods(http.MethodGet)
	apiJSONRoutes.Path("/s2/dump").HandlerFunc(handleS2DumpLevel).Methods(http.MethodGet)
	apiJSONRoutes.Path("/s2/collect").HandlerFunc(handleS2CollectLevel).Methods(http.MethodGet)

	authenticatedAPIRoutes := apiJSONRoutes.NewRoute().Subrouter()
	authenticatedAPIRoutes.Use(tokenAuthenticationMiddleware)

	populateRoutes := authenticatedAPIRoutes.NewRoute().Subrouter()

	populateRoutes.Path("/populate/").HandlerFunc(s.handlePopulate).Methods(http.MethodPost)
	populateRoutes.Path("/populate").HandlerFunc(s.handlePopulate).Methods(http.MethodPost)

	// TODO: Proxy to the tiler daemon's RPC server

	return router
}
