package apiserver

import (
	"context"
	"encoding/json"
	"net/http"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Server exposes GPU node reaper state over HTTP.
type Server struct {
	addr   string
	client client.Client
	mux    *http.ServeMux
}

// New creates a Server that listens on addr and reads state via the given client.
func New(c client.Client, addr string) *Server {
	s := &Server{addr: addr, client: c, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /api/v1/nodes", s.handleNodes)
	s.mux.HandleFunc("GET /api/v1/nodes/{name}", s.handleNode)
	s.mux.HandleFunc("GET /api/v1/stats", s.handleStats)
	s.mux.HandleFunc("OPTIONS /", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	return s
}

// Start implements manager.Runnable; the server shuts down when ctx is cancelled.
func (s *Server) Start(ctx context.Context) error {
	srv := &http.Server{Addr: s.addr, Handler: corsMiddleware(s.mux)}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
