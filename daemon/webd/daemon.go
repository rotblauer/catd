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

func NewWebDaemon(config *params.WebDaemonConfig) (*WebDaemon, error) {
	logger := slog.With("daemon", "web")
	if config == nil {
		logger.Warn("No config provided, using default")
		config = params.DefaultWebDaemonConfig()
	}
	if config.DataDir == "" {
		config.DataDir = params.DefaultDatadirRoot
		logger.Warn("No data dir provided, using default", "datadir", config.DataDir)
	}
	return &WebDaemon{
		Config:        config,
		logger:        logger,
		feedPopulated: event.FeedOf[[]*cattrack.CatTrack]{},
	}, nil
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
	apiRoutes.Use(permissiveCorsMiddleware)

	// All API routes use permissive CORS settings.
	apiRoutes.Path("/ping").HandlerFunc(pingPong)
	apiRoutes.Path("/status").HandlerFunc(s.statusReport)

	/*
		TODO /v9000 paths?
		FIXME Which content type for streaming NDJSON?
		https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type
		https://www.iana.org/assignments/media-types/media-types.xhtml
		https://github.com/ipfs/kubo/issues/3737
		https://stackoverflow.com/questions/57301886/what-is-the-suitable-http-content-type-for-consuming-an-asynchronous-stream-of-d
		w.Header().Set("Content-Type", "application/stream+json")
	*/
	// Middleware only runs on a route match.
	// https://github.com/gorilla/mux/issues/429
	// wtf.
	apiJSON := apiRoutes.NewRoute().Subrouter()
	apiJSON.Use(contentTypeMiddlewareFunc("application/json"))
	apiNDJSON := apiRoutes.NewRoute().Subrouter()
	apiNDJSON.Use(contentTypeMiddlewareFunc("application/x-ndjson"))

	apiJSON.Path("/{cat}/last.json").HandlerFunc(s.catIndex).Methods(http.MethodGet)
	apiJSON.Path("/{cat}/pushed.json").HandlerFunc(s.catPushedJSON).Methods(http.MethodGet)
	apiNDJSON.Path("/{cat}/pushed.ndjson").HandlerFunc(s.catPushedNDJSON).Methods(http.MethodGet)
	apiJSON.Path("/{cat}/snaps.json").HandlerFunc(s.getCatSnaps).Methods(http.MethodGet)
	apiJSON.Path("/{cat}/s2/{level}/tracks.json").HandlerFunc(s.s2Collect).Methods(http.MethodGet)
	apiNDJSON.Path("/{cat}/s2/{level}/tracks.ndjson").HandlerFunc(s.s2Dump).Methods(http.MethodGet)
	apiJSON.Path("/{cat}/rgeo/{datasetRe}/plats.json").HandlerFunc(s.rGeoCollect).Methods(http.MethodGet)

	authenticatedAPIRoutes := apiJSON.NewRoute().Subrouter()
	authenticatedAPIRoutes.Use(tokenAuthenticationMiddleware)
	populateRoutes := authenticatedAPIRoutes.NewRoute().Subrouter()
	populateRoutes.Path("/populate/").HandlerFunc(s.populate).Methods(http.MethodPost)
	populateRoutes.Path("/populate").HandlerFunc(s.populate).Methods(http.MethodPost)

	// TODO: Proxy to the tiler daemon's RPC server?

	return router
}
