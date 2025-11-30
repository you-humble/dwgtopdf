package transport

import (
	"log"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"
)

type loggingResponseWriter struct {
	http.ResponseWriter
	status int
	size   int64
}

func (lrw *loggingResponseWriter) WriteHeader(statusCode int) {
	lrw.status = statusCode
	lrw.ResponseWriter.WriteHeader(statusCode)
}

func (lrw *loggingResponseWriter) Write(b []byte) (int, error) {
	if lrw.status == 0 {
		lrw.status = http.StatusOK
	}
	n, err := lrw.ResponseWriter.Write(b)
	lrw.size += int64(n)
	return n, err
}

func LogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		lrw := &loggingResponseWriter{ResponseWriter: w}
		next.ServeHTTP(lrw, r)
		slog.Info("http_request",
			slog.String("method", r.Method),
			slog.String("url", r.URL.Path),
			slog.String("remote_addr", r.RemoteAddr),
			slog.Int("status", lrw.status),
			slog.Int64("response_size", lrw.size),
			slog.String("duration", time.Since(start).String()),
		)
	})
}

func WithRecover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("panic in handler: %v\n%s", rec, debug.Stack())
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
