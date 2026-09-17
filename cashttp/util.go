package cashttp

import (
	"encoding/json"
	"net/http"
	"strconv"
)

type errorBody struct {
	Error string `json:"error"`
	Msg   string `json:"message,omitempty"`
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorBody{Error: code, Msg: msg})
}

func itoa(n int) string { return strconv.Itoa(n) }
