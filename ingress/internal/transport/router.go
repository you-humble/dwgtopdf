package transport

import "net/http"

type Handler interface {
	convert(w http.ResponseWriter, r *http.Request)
	result(w http.ResponseWriter, r *http.Request)
	download(w http.ResponseWriter, r *http.Request)
}

type router struct {
	h Handler
}

func NewRouter(h Handler) *router {
	return &router{h: h}
}

func (r *router) MountRoutes(mux *http.ServeMux) *http.ServeMux {
	mux.HandleFunc("/convert", r.h.convert)
	mux.HandleFunc("/result/", r.h.result)
	mux.HandleFunc("/download/", r.h.download)

	return mux
}
