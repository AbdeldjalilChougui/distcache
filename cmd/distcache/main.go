// Command distcache runs one cache node in front of a fake slow backend,
// to watch ownership, peer fetches and the hot cache at work.
//
//	distcache -addr 127.0.0.1:8001 -peers http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003
//	curl 127.0.0.1:8001/get/user:42
//	curl 127.0.0.1:8001/stats
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/AbdeldjalilChougui/distcache"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8001", "listen address")
	self := flag.String("self", "", "this node's URL as peers see it (default http://<addr>)")
	peers := flag.String("peers", "", "comma-separated URLs of every node, including this one")
	cacheMB := flag.Int64("cache-mb", 64, "cache size in MiB")
	delay := flag.Duration("backend-delay", 300*time.Millisecond, "simulated backend latency")
	flag.Parse()
	if *self == "" {
		*self = "http://" + *addr
	}
	pool := distcache.NewPool(*self, distcache.PoolOptions{})
	if *peers != "" {
		pool.Set(strings.Split(*peers, ",")...)
	}
	// The "backend": slow and deterministic, so every node returns the
	// same value for a key.
	group := pool.NewGroup("demo", *cacheMB<<20, distcache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		log.Printf("backend load %q", key)
		select {
		case <-time.After(*delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if strings.HasPrefix(key, "missing") {
			return nil, distcache.ErrNotFound
		}
		return []byte("value for " + key + "\n"), nil
	}), distcache.Options{})

	mux := http.NewServeMux()
	mux.Handle(distcache.DefaultBasePath, pool)
	mux.HandleFunc("GET /get/{key...}", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		v, err := group.Get(r.Context(), r.PathValue("key"))
		w.Header().Set("X-Owner", pool.Owner(r.PathValue("key")))
		w.Header().Set("X-Took", time.Since(start).String())
		if errors.Is(err, distcache.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		w.Write(v.ByteSlice())
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		s := &group.Stats
		main, hot := group.CacheBytes()
		json.NewEncoder(w).Encode(map[string]int64{
			"gets": s.Gets.Load(), "hits": s.Hits.Load(), "hot_hits": s.HotHits.Load(),
			"peer_loads": s.PeerLoads.Load(), "peer_errors": s.PeerErrors.Load(),
			"local_loads": s.LocalLoads.Load(), "deduped": s.Deduped.Load(), "peer_served": s.PeerServed.Load(),
			"main_bytes": main, "hot_bytes": hot,
		})
	})

	srv := &http.Server{Addr: *addr, Handler: mux}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); srv.Close() }()
	log.Printf("node %s listening on %s", *self, *addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
