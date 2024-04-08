// Package distcache is a groupcache-style distributed read-through cache.
//
// Every key has one owner node, chosen by consistent hashing. A node that
// is asked for a key it doesn't own fetches it from the owner over HTTP;
// only the owner calls the (slow) Getter and keeps the value in its main
// cache. Concurrent requests for the same key are collapsed with
// singleflight, and keys that are popular on a non-owner can be kept in a
// small local "hot" cache so the owner isn't hammered.
//
// Like groupcache, there is no expiry and no Set/Delete: a value, once
// loaded, is immutable until it is evicted.
package distcache

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/AbdeldjalilChougui/distcache/lru"
	"github.com/AbdeldjalilChougui/distcache/singleflight"
)

// ErrNotFound is returned by a Getter for a key that doesn't exist. It is
// passed through peers unchanged, and not retried locally.
var ErrNotFound = errors.New("distcache: not found")

// Getter loads a value on a cache miss (from a database, a file...).
type Getter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

// GetterFunc adapts a function to Getter.
type GetterFunc func(ctx context.Context, key string) ([]byte, error)

// Get implements Getter.
func (f GetterFunc) Get(ctx context.Context, key string) ([]byte, error) { return f(ctx, key) }

// PeerPicker chooses the owner of a key. ok is false when this node is
// the owner (or there are no peers).
type PeerPicker interface {
	PickPeer(key string) (peer PeerGetter, ok bool)
}

// PeerGetter fetches a value from another node.
type PeerGetter interface {
	Get(ctx context.Context, group, key string) ([]byte, error)
}

// ByteView is an immutable view of a cached value.
type ByteView struct{ b []byte }

// Len implements lru.Value.
func (v ByteView) Len() int { return len(v.b) }

// ByteSlice returns a copy of the data.
func (v ByteView) ByteSlice() []byte { return append([]byte(nil), v.b...) }

// String returns the data as a string.
func (v ByteView) String() string { return string(v.b) }

type cache struct {
	mu  sync.Mutex
	lru *lru.Cache
}

func (c *cache) get(key string) (ByteView, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.lru.Get(key); ok {
		return v.(ByteView), true
	}
	return ByteView{}, false
}

func (c *cache) add(key string, v ByteView) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lru.Add(key, v)
}

func (c *cache) bytes() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Bytes()
}

func (c *cache) removeOldest() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lru.RemoveOldest()
}

// Stats are counters for one group.
type Stats struct {
	Gets       atomic.Int64 // every Get
	Hits       atomic.Int64 // served from the main or hot cache
	HotHits    atomic.Int64 // served from the hot cache
	PeerLoads  atomic.Int64 // fetched from the owner
	PeerErrors atomic.Int64 // owner unreachable (fell back to a local load)
	LocalLoads atomic.Int64 // Getter calls
	Deduped    atomic.Int64 // Gets that joined another in-flight load
	PeerServed atomic.Int64 // requests answered for other nodes
}

// Options tunes a Group.
type Options struct {
	// PromoteToHot decides, after a value was fetched from its owner,
	// whether to keep a copy in the local hot cache. Default: 1 in 10,
	// as in groupcache, so only frequently requested keys tend to stay.
	PromoteToHot func(key string) bool
}

// Group is a named cache namespace with one Getter.
type Group struct {
	name       string
	getter     Getter
	peers      PeerPicker
	cacheBytes int64
	promote    func(string) bool

	main cache // keys this node owns
	hot  cache // copies of popular keys owned by others
	sf   singleflight.Group

	Stats Stats
}

func newGroup(name string, cacheBytes int64, getter Getter, peers PeerPicker, opt Options) *Group {
	g := &Group{
		name: name, getter: getter, peers: peers, cacheBytes: cacheBytes, promote: opt.PromoteToHot,
		main: cache{lru: lru.New(0)}, hot: cache{lru: lru.New(0)},
	}
	if g.promote == nil {
		g.promote = func(string) bool { return rand.Intn(10) == 0 }
	}
	return g
}

// Name returns the group's name.
func (g *Group) Name() string { return g.name }

// Get returns the value for key, from a cache, the owning peer, or the
// Getter.
func (g *Group) Get(ctx context.Context, key string) (ByteView, error) {
	g.Stats.Gets.Add(1)
	if v, ok := g.lookup(key); ok {
		return v, nil
	}
	return g.load(ctx, key, true)
}

// getForPeer answers another node. It never forwards to a third node, so
// nodes with different views of the ring can't bounce a request around.
func (g *Group) getForPeer(ctx context.Context, key string) (ByteView, error) {
	g.Stats.PeerServed.Add(1)
	if v, ok := g.lookup(key); ok {
		return v, nil
	}
	return g.load(ctx, key, false)
}

func (g *Group) lookup(key string) (ByteView, bool) {
	if v, ok := g.main.get(key); ok {
		g.Stats.Hits.Add(1)
		return v, true
	}
	if v, ok := g.hot.get(key); ok {
		g.Stats.Hits.Add(1)
		g.Stats.HotHits.Add(1)
		return v, true
	}
	return ByteView{}, false
}

func (g *Group) load(ctx context.Context, key string, usePeers bool) (ByteView, error) {
	v, err, shared := g.sf.Do(key, func() (any, error) {
		// Another flight may have filled the cache while we queued.
		if v, ok := g.lookup(key); ok {
			return v, nil
		}
		if usePeers && g.peers != nil {
			if peer, ok := g.peers.PickPeer(key); ok {
				data, err := peer.Get(ctx, g.name, key)
				if err == nil {
					g.Stats.PeerLoads.Add(1)
					v := ByteView{data}
					if g.promote(key) {
						g.populate(&g.hot, key, v)
					}
					return v, nil
				}
				if errors.Is(err, ErrNotFound) {
					return nil, err
				}
				// Owner unreachable: load it ourselves rather than fail.
				g.Stats.PeerErrors.Add(1)
			}
		}
		data, err := g.getter.Get(ctx, key)
		g.Stats.LocalLoads.Add(1)
		if err != nil {
			return nil, err
		}
		v := ByteView{append([]byte(nil), data...)}
		g.populate(&g.main, key, v)
		return v, nil
	})
	if shared {
		g.Stats.Deduped.Add(1)
	}
	if err != nil {
		return ByteView{}, err
	}
	return v.(ByteView), nil
}

// populate adds to a cache and evicts until main+hot fit in cacheBytes.
// The hot cache is kept to about 1/8 of the main cache.
func (g *Group) populate(c *cache, key string, v ByteView) {
	if g.cacheBytes <= 0 {
		return
	}
	c.add(key, v)
	for {
		mb, hb := g.main.bytes(), g.hot.bytes()
		if mb+hb <= g.cacheBytes {
			return
		}
		victim := &g.main
		if hb > mb/8 {
			victim = &g.hot
		}
		victim.removeOldest()
	}
}

// CacheBytes reports the bytes used by the main and hot caches.
func (g *Group) CacheBytes() (main, hot int64) { return g.main.bytes(), g.hot.bytes() }
