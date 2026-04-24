package api

import (
	"net/http"

	"github.com/gorilla/mux"
)

func setMuxVars(r *http.Request, vars map[string]string) *http.Request {
	return mux.SetURLVars(r, vars)
}
