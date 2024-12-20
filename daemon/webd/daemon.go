package webd

import (
	"github.com/ethereum/go-ethereum/event"
	"github.com/gorilla/mux"
	"github.com/olahol/melody"
	"github.com/rotblauer/catd/params"
	"github.com/rotblauer/catd/types/cattrack"
	"log"
	"log/slog"
	"net/http"
	"time"
)

type WebDaemon struct {
	Config         *params.WebDaemonConfig
	logger         *slog.Logger
	melodyInstance *melody.Melody
	feedPopulated  event.FeedOf[[]*cattrack.CatTrack]
	started        time.Time
}

func NewWebDaemon(config *params.WebDaemonConfig) *WebDaemon {
	logger := slog.With("daemon", "web")
	if config == nil {
		logger.Warn("No config provided, using default")
		config = params.DefaultWebDaemonConfig()
	}
	return &WebDaemon{
		Config:        config,
		logger:        logger,
		feedPopulated: event.FeedOf[[]*cattrack.CatTrack]{},
	}
}

// Run starts the HTTP server (ListenAndServe) and waits for it,
// returning any server error.
func (s *WebDaemon) Run() error {
	s.started = time.Now()
	router := s.NewRouter()
	http.Handle("/", router)
	log.Printf("Starting web daemon on %s", s.Config.Address)
	return http.ListenAndServe(s.Config.Address, nil)
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
	apiRoutes.Path("/status").HandlerFunc(s.statusReport)

	// TODO /v9000 paths?
	apiJSONRoutes := apiRoutes.NewRoute().Subrouter()
	jsonMiddleware := contentTypeMiddlewareFunc("application/json")
	apiJSONRoutes.Use(jsonMiddleware)

	apiJSONRoutes.Path("/{cat}/last.json").HandlerFunc(catIndex).Methods(http.MethodGet)
	apiJSONRoutes.Path("/{cat}/pushed.json").HandlerFunc(catPushed).Methods(http.MethodGet)
	apiJSONRoutes.Path("/{cat}/snaps.json").HandlerFunc(getCatSnaps).Methods(http.MethodGet)
	apiJSONRoutes.Path("/{cat}/s2/{level}/tracks.ndjson").HandlerFunc(s2Dump).Methods(http.MethodGet)
	apiJSONRoutes.Path("/{cat}/s2/{level}/tracks.json").HandlerFunc(s2Collect).Methods(http.MethodGet)
	apiJSONRoutes.Path("/{cat}/rgeo/{datasetRe}/plats.json").HandlerFunc(rGeoCollect).Methods(http.MethodGet)

	authenticatedAPIRoutes := apiJSONRoutes.NewRoute().Subrouter()
	authenticatedAPIRoutes.Use(tokenAuthenticationMiddleware)

	populateRoutes := authenticatedAPIRoutes.NewRoute().Subrouter()

	populateRoutes.Path("/populate/").HandlerFunc(s.populate).Methods(http.MethodPost)
	populateRoutes.Path("/populate").HandlerFunc(s.populate).Methods(http.MethodPost)

	// TODO: Proxy to the tiler daemon's RPC server

	return router
}
