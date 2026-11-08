/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package alpha

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/wisdomatom/stateless-graph/worker"
	"github.com/wisdomatom/stateless-graph/x"
)

type allowedMethods map[string]bool

// hasPoormansAuth checks if poorman's auth is required and if so whether the given http request has
// poorman's auth in it or not
func hasPoormansAuth(r *http.Request) bool {
	if worker.Config.AuthToken != "" && worker.Config.AuthToken != r.Header.Get(
		"X-Dgraph-AuthToken") {
		return false
	}
	return true
}

func allowedMethodsHandler(allowedMethods allowedMethods, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := allowedMethods[r.Method]; !ok {
			x.AddCorsHeaders(w)
			if r.Method == http.MethodOptions {
				return
			}
			x.SetStatus(w, x.ErrorInvalidMethod, "Invalid method")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// adminAuthHandler does some standard checks for admin endpoints.
// It returns if something is wrong. Otherwise, it lets the given handler serve the request.
func adminAuthHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !hasPoormansAuth(r) {
			x.AddCorsHeaders(w)
			x.SetStatus(w, x.ErrorUnauthorized, "Invalid X-Dgraph-AuthToken")
			return
		}

		next.ServeHTTP(w, r)
	})
}

func getAdminMux() *http.ServeMux {
	adminMux := http.NewServeMux()
	adminMux.Handle("/admin/schema", adminAuthHandler(http.HandlerFunc(adminSchemaHandler)))
	adminMux.Handle("/admin/schema/validate", schemaValidateHandler())
	adminMux.Handle("/admin/shutdown", allowedMethodsHandler(allowedMethods{http.MethodGet: true},
		adminAuthHandler(http.HandlerFunc(shutDownHandler))))
	adminMux.Handle("/admin/draining", allowedMethodsHandler(allowedMethods{
		http.MethodPut:  true,
		http.MethodPost: true,
	}, adminAuthHandler(http.HandlerFunc(drainingHandler))))
	adminMux.Handle("/admin/config/cache_mb", allowedMethodsHandler(allowedMethods{
		http.MethodGet: true,
		http.MethodPut: true,
	}, adminAuthHandler(http.HandlerFunc(memoryLimitHandler))))
	return adminMux
}

func schemaValidateHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		x.SetStatus(w, "success", "Schema is valid")
	})
}

func drainingHandler(w http.ResponseWriter, r *http.Request) {
	enableStr := r.URL.Query().Get("enable")

	enable, err := strconv.ParseBool(enableStr)
	if err != nil {
		x.SetStatus(w, x.ErrorInvalidRequest,
			"Found invalid value for the enable parameter")
		return
	}

	x.UpdateDrainingMode(enable)
	w.Header().Set("Content-Type", "application/json")
	x.Check2(w.Write([]byte(fmt.Sprintf(`{"code": "Success",`+
		`"message": "draining mode has been set to %v"}`, enable))))
}

func shutDownHandler(w http.ResponseWriter, r *http.Request) {
	x.ServerCloser.Signal()
	w.Header().Set("Content-Type", "application/json")
	x.Check2(w.Write([]byte(`{"code": "Success", "message": "Server is shutting down"}`)))
}

func memoryLimitHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		memoryLimitGetHandler(w, r)
	case http.MethodPut:
		memoryLimitPutHandler(w, r)
	}
}

func memoryLimitPutHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	memoryMB, err := strconv.ParseFloat(string(body), 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := worker.UpdateCacheMb(int64(memoryMB)); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		x.Check2(fmt.Fprint(w, err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
}

func memoryLimitGetHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if _, err := fmt.Fprintln(w, worker.Config.CacheMb); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
