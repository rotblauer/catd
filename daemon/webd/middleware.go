package webd

import (
	"fmt"
	ghandlers "github.com/gorilla/handlers"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
	"unicode/utf8"
)

// tokenAuthenticationMiddleware is a middleware that checks for a valid token in the Authorization header.
// If the token is not valid, it returns a 403 Forbidden.
// If the token is valid, it calls the next middleware (or final handler).
// If no token is set, it allows all requests.
func tokenAuthenticationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		validToken := os.Getenv("COTOKEN")
		if validToken == "" {
			log.Printf("WARN: No COTOKEN set, allowing all requests")
			next.ServeHTTP(w, r)
			return
		}

		token := r.Header.Get("AuthorizationOfCats")
		if token == "" {
			// Header token not set. Check alternate protocol, which is using a query param with the name api_token.
			// eg. catonmap.info:3001/populate/?api_token=asdfasdfb
			r.ParseForm()
			token = r.FormValue("api_token")
		}

		// Enforce token validation.
		if token != validToken {
			log.Println("Invalid token",
				"token:", fmt.Sprintf("%q", token), "validToken:", "***REDACTED***",
				"method:", r.Method, "url:", r.URL, "proto:", r.Proto,
				"host:", r.Host, "remote-addr:", r.RemoteAddr,
				"request-URI:", r.RequestURI, "content-length:", r.ContentLength,
				"user-agent:", r.UserAgent())
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		// Pass down the request to the next middleware (or final handler)
		next.ServeHTTP(w, r)
	})
}

func permissiveCorsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Do stuff here
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Add("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization")
		// Call the next handler, which can be another middleware in the chain, or the final handler.
		next.ServeHTTP(w, r)
	})
}

func contentTypeMiddlewareFunc(contentType string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Do stuff here
			w.Header().Set("Content-Type", contentType)
			// Call the next handler, which can be another middleware in the chain, or the final handler.
			next.ServeHTTP(w, r)
		})
	}
}

// https://github.com/gorilla/mux#middleware

const lowerhex = "0123456789abcdef"

func appendQuoted(buf []byte, s string) []byte {
	var runeTmp [utf8.UTFMax]byte
	for width := 0; len(s) > 0; s = s[width:] { // nolint: wastedassign //TODO: why width starts from 0and reassigned as 1
		r := rune(s[0])
		width = 1
		if r >= utf8.RuneSelf {
			r, width = utf8.DecodeRuneInString(s)
		}
		if width == 1 && r == utf8.RuneError {
			buf = append(buf, `\x`...)
			buf = append(buf, lowerhex[s[0]>>4])
			buf = append(buf, lowerhex[s[0]&0xF])
			continue
		}
		if r == rune('"') || r == '\\' { // always backslashed
			buf = append(buf, '\\')
			buf = append(buf, byte(r))
			continue
		}
		if strconv.IsPrint(r) {
			n := utf8.EncodeRune(runeTmp[:], r)
			buf = append(buf, runeTmp[:n]...)
			continue
		}
		switch r {
		case '\a':
			buf = append(buf, `\a`...)
		case '\b':
			buf = append(buf, `\b`...)
		case '\f':
			buf = append(buf, `\f`...)
		case '\n':
			buf = append(buf, `\n`...)
		case '\r':
			buf = append(buf, `\r`...)
		case '\t':
			buf = append(buf, `\t`...)
		case '\v':
			buf = append(buf, `\v`...)
		default:
			switch {
			case r < ' ':
				buf = append(buf, `\x`...)
				buf = append(buf, lowerhex[s[0]>>4])
				buf = append(buf, lowerhex[s[0]&0xF])
			case r > utf8.MaxRune:
				r = 0xFFFD
				fallthrough
			case r < 0x10000:
				buf = append(buf, `\u`...)
				for s := 12; s >= 0; s -= 4 {
					buf = append(buf, lowerhex[r>>uint(s)&0xF])
				}
			default:
				buf = append(buf, `\U`...)
				for s := 28; s >= 0; s -= 4 {
					buf = append(buf, lowerhex[r>>uint(s)&0xF])
				}
			}
		}
	}
	return buf
}

// buildCommonLogLine builds a log entry for req in Apache Common Log Format.
// ts is the timestamp with which the entry should be logged.
// status and size are used to provide the response HTTP status and size.
func buildCommonLogLine(req *http.Request, url url.URL, ts time.Time, status int, size int) []byte {
	username := "-"
	if url.User != nil {
		if name := url.User.Username(); name != "" {
			username = name
		}
	}

	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		host = req.RemoteAddr
	}

	for _, v := range req.Header.Values("X-Forwarded-For") {
		host += "->" + v
	}

	uri := req.RequestURI

	// Requests using the CONNECT method over HTTP/2.0 must use
	// the authority field (aka r.Host) to identify the target.
	// Refer: https://httpwg.github.io/specs/rfc7540.html#CONNECT
	if req.ProtoMajor == 2 && req.Method == "CONNECT" {
		uri = req.Host
	}
	if uri == "" {
		uri = url.RequestURI()
	}

	buf := make([]byte, 0, 3*(len(host)+len(username)+len(req.Method)+len(uri)+len(req.Proto)+50)/2)
	buf = append(buf, host...)
	buf = append(buf, " - "...)
	buf = append(buf, username...)
	buf = append(buf, " ["...)
	buf = append(buf, ts.Format("02/Jan/2006:15:04:05 -0700")...)
	buf = append(buf, `] "`...)
	buf = append(buf, req.Method...)
	buf = append(buf, " "...)
	buf = appendQuoted(buf, uri)
	buf = append(buf, " "...)
	buf = append(buf, req.Proto...)
	buf = append(buf, `" `...)
	buf = append(buf, strconv.Itoa(status)...)
	buf = append(buf, " "...)
	buf = append(buf, strconv.Itoa(size)...)
	return buf
}

// writeLog writes a log entry for req to w in Apache Common Log Format.
// ts is the timestamp with which the entry should be logged.
// status and size are used to provide the response HTTP status and size.
func writeLog(writer io.Writer, params ghandlers.LogFormatterParams) {
	buf := buildCommonLogLine(params.Request, params.URL, params.TimeStamp, params.StatusCode, params.Size)
	buf = append(buf, '\n')
	_, _ = writer.Write(buf)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return ghandlers.CustomLoggingHandler(os.Stdout, next, writeLog)
	// return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	// 	// Do stuff here
	// 	dump, _ := httputil.DumpRequest(r, false)
	// 	log.Println(string(dump))
	//
	// 	// Call the next handler, which can be another middleware in the chain, or the final handler.
	// 	next.ServeHTTP(w, r)
	// })
}
