package distcache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/AbdeldjalilChougui/distcache/consistenthash"
)

// DefaultBasePath is where nodes serve each other.
const DefaultBasePath = "/_distcache/"

// MaxValueSize bounds a value fetched from a peer.
const MaxValueSize = 64 << 20

// Pool is one node: it owns the groups, knows the peers, picks the owner
// of each key on a consistent-hash ring, and serves other nodes over
// HTTP at /_distcache/<group>/<key>.
type Pool struct {
	self     string
	basePath string
	replicas int
	client   *http.Client

	mu      sync.RWMutex
	ring    *consistenthash.Map
	getters map[string]*httpGetter
	groups  map[string]*Group
}

// PoolOptions configures a Pool.
type PoolOptions struct {
	BasePath string       // default DefaultBasePath
	Replicas int          // virtual nodes per peer; default 50
	Client   *http.Client // default: 5s timeout
}

// NewPool creates a node whose own base URL is self (e.g.
// "http://10.0.0.1:8000"). It is an http.Handler for peer requests.
func NewPool(self string, opt PoolOptions) *Pool {
	if opt.BasePath == "" {
		opt.BasePath = DefaultBasePath
	}
	if opt.Replicas <= 0 {
		opt.Replicas = 50
	}
	if opt.Client == nil {
		opt.Client = &http.Client{Timeout: 5 * time.Second}
	}
	p := &Pool{
		self: strings.TrimRight(self, "/"), basePath: opt.BasePath, replicas: opt.Replicas, client: opt.Client,
		groups: map[string]*Group{},
	}
	p.Set(p.self)
	return p
}

// Self returns this node's URL.
func (p *Pool) Self() string { return p.self }

// Set replaces the peer list (it should include self). Every node must be
// given the same list, or they will disagree about owners (which still
// works, but caches the same key on several nodes).
func (p *Pool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ring = consistenthash.New(p.replicas, nil)
	p.getters = map[string]*httpGetter{}
	for _, peer := range peers {
		peer = strings.TrimRight(peer, "/")
		p.ring.Add(peer)
		p.getters[peer] = &httpGetter{base: peer + p.basePath, client: p.client}
	}
}

// Owner returns the URL of the node that owns key.
func (p *Pool) Owner(key string) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.ring.Get(key)
}

// PickPeer implements PeerPicker.
func (p *Pool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	owner := p.ring.Get(key)
	if owner == "" || owner == p.self {
		return nil, false
	}
	return p.getters[owner], true
}

// NewGroup creates a group on this node. Each node must create the same
// groups (with getters that return the same data).
func (p *Pool) NewGroup(name string, cacheBytes int64, getter Getter, opt Options) *Group {
	if getter == nil {
		panic("distcache: nil Getter")
	}
	g := newGroup(name, cacheBytes, getter, p, opt)
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, dup := p.groups[name]; dup {
		panic("distcache: duplicate group " + name)
	}
	p.groups[name] = g
	return g
}

// Group returns a group by name.
func (p *Pool) Group(name string) *Group {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.groups[name]
}

// ServeHTTP answers peers: GET <basePath><group>/<key>.
func (p *Pool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rest, ok := strings.CutPrefix(r.URL.EscapedPath(), p.basePath)
	if !ok || r.Method != http.MethodGet {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	gname, ekey, ok := strings.Cut(rest, "/")
	if !ok {
		http.Error(w, "want <group>/<key>", http.StatusBadRequest)
		return
	}
	gname, err1 := url.PathUnescape(gname)
	key, err2 := url.PathUnescape(ekey)
	if err1 != nil || err2 != nil {
		http.Error(w, "bad escaping", http.StatusBadRequest)
		return
	}
	g := p.Group(gname)
	if g == nil {
		http.Error(w, "no such group: "+gname, http.StatusNotFound)
		return
	}
	v, err := g.getForPeer(r.Context(), key)
	if errors.Is(err, ErrNotFound) {
		w.Header().Set("X-Distcache-Not-Found", "1")
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(v.b)
}

type httpGetter struct {
	base   string
	client *http.Client
}

// Get implements PeerGetter.
func (h *httpGetter) Get(ctx context.Context, group, key string) ([]byte, error) {
	u := h.base + url.PathEscape(group) + "/" + url.PathEscape(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxValueSize+1))
	if err != nil {
		return nil, err
	}
	switch {
	case resp.StatusCode == http.StatusNotFound && resp.Header.Get("X-Distcache-Not-Found") != "":
		return nil, ErrNotFound
	case resp.StatusCode != http.StatusOK:
		return nil, fmt.Errorf("distcache: peer %s: %s: %s", h.base, resp.Status, strings.TrimSpace(string(body)))
	case len(body) > MaxValueSize:
		return nil, errors.New("distcache: value from peer is too large")
	}
	return body, nil
}
