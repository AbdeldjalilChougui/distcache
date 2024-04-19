package distcache

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// db is the fake slow backend shared by every node: it records which node
// loaded which key.
type db struct {
	mu    sync.Mutex
	loads map[string][]string // key -> nodes that called the getter
	delay time.Duration
}

func (d *db) getter(node string) Getter {
	return GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		time.Sleep(d.delay)
		d.mu.Lock()
		d.loads[key] = append(d.loads[key], node)
		d.mu.Unlock()
		if strings.HasPrefix(key, "missing") {
			return nil, ErrNotFound
		}
		return []byte("value-of-" + key), nil
	})
}

func (d *db) loadsOf(key string) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.loads[key]...)
}

type node struct {
	pool  *Pool
	group *Group
	srv   *httptest.Server
}

// cluster starts n nodes on 127.0.0.1, all with the same peer list.
func cluster(t *testing.T, n int, d *db, cacheBytes int64, opt Options) []*node {
	t.Helper()
	var nodes []*node
	var urls []string
	for range n {
		mux := http.NewServeMux()
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)
		p := NewPool(srv.URL, PoolOptions{})
		mux.Handle(DefaultBasePath, p)
		nodes = append(nodes, &node{pool: p, srv: srv})
		urls = append(urls, srv.URL)
	}
	for _, nd := range nodes {
		nd.pool.Set(urls...)
		nd.group = nd.pool.NewGroup("users", cacheBytes, d.getter(nd.pool.Self()), opt)
	}
	return nodes
}

func never(string) bool  { return false }
func always(string) bool { return true }

func TestEachKeyLoadedOnceByItsOwner(t *testing.T) {
	d := &db{loads: map[string][]string{}}
	nodes := cluster(t, 3, d, 1<<20, Options{PromoteToHot: never})
	ctx := context.Background()
	for round := range 3 {
		for i := range 30 {
			key := fmt.Sprintf("user:%d", i)
			nd := nodes[(i+round)%3]
			v, err := nd.group.Get(ctx, key)
			if err != nil || v.String() != "value-of-"+key {
				t.Fatalf("Get(%s) = %q, %v", key, v.String(), err)
			}
		}
	}
	owners := map[string]int{}
	for i := range 30 {
		key := fmt.Sprintf("user:%d", i)
		loads := d.loadsOf(key)
		owner := nodes[0].pool.Owner(key)
		if len(loads) != 1 || loads[0] != owner {
			t.Fatalf("%s loaded by %v, owner is %s", key, loads, owner)
		}
		owners[owner]++
	}
	if len(owners) != 3 {
		t.Fatalf("keys not spread over the nodes: %v", owners)
	}
	var peerLoads int64
	for _, nd := range nodes {
		peerLoads += nd.group.Stats.PeerLoads.Load()
	}
	if peerLoads == 0 {
		t.Fatal("no request went to a peer")
	}
}

func TestSingleflightAcrossNodes(t *testing.T) {
	d := &db{loads: map[string][]string{}, delay: 100 * time.Millisecond}
	nodes := cluster(t, 3, d, 1<<20, Options{PromoteToHot: never})
	var wg sync.WaitGroup
	var bad atomic.Int32
	for i := range 90 { // 30 concurrent requests on each node
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := nodes[i%3].group.Get(context.Background(), "celebrity")
			if err != nil || v.String() != "value-of-celebrity" {
				bad.Add(1)
			}
		}()
	}
	wg.Wait()
	if bad.Load() > 0 {
		t.Fatalf("%d requests failed", bad.Load())
	}
	if loads := d.loadsOf("celebrity"); len(loads) != 1 {
		t.Fatalf("backend hit %d times: %v", len(loads), loads)
	}
	for _, nd := range nodes {
		if nd.group.Stats.PeerLoads.Load() > 1 {
			t.Fatalf("node %s made %d peer requests for one key", nd.pool.Self(), nd.group.Stats.PeerLoads.Load())
		}
	}
	var deduped int64
	for _, nd := range nodes {
		deduped += nd.group.Stats.Deduped.Load()
	}
	if deduped == 0 {
		t.Fatal("no request was deduplicated")
	}
}

func nonOwner(nodes []*node, key string) *node {
	owner := nodes[0].pool.Owner(key)
	for _, nd := range nodes {
		if nd.pool.Self() != owner {
			return nd
		}
	}
	return nil
}

func TestHotCache(t *testing.T) {
	d := &db{loads: map[string][]string{}}
	nodes := cluster(t, 3, d, 1<<20, Options{PromoteToHot: always})
	nd := nonOwner(nodes, "hot-key")
	for range 5 {
		nd.group.Get(context.Background(), "hot-key")
	}
	if pl, hh := nd.group.Stats.PeerLoads.Load(), nd.group.Stats.HotHits.Load(); pl != 1 || hh != 4 {
		t.Fatalf("with the hot cache: %d peer loads, %d hot hits", pl, hh)
	}
	if _, hot := nd.group.CacheBytes(); hot == 0 {
		t.Fatal("hot cache is empty")
	}

	d2 := &db{loads: map[string][]string{}}
	cold := cluster(t, 3, d2, 1<<20, Options{PromoteToHot: never})
	nd = nonOwner(cold, "hot-key")
	for range 5 {
		nd.group.Get(context.Background(), "hot-key")
	}
	if pl := nd.group.Stats.PeerLoads.Load(); pl != 5 {
		t.Fatalf("without the hot cache: %d peer loads, want 5", pl)
	}
	if len(d2.loadsOf("hot-key")) != 1 {
		t.Fatal("owner should have cached the key")
	}
}

func TestOwnerDownFallsBackToLocalLoad(t *testing.T) {
	d := &db{loads: map[string][]string{}}
	nodes := cluster(t, 3, d, 1<<20, Options{PromoteToHot: never})
	key := "orphan"
	owner := nodes[0].pool.Owner(key)
	for _, nd := range nodes {
		if nd.pool.Self() == owner {
			nd.srv.Close()
		}
	}
	nd := nonOwner(nodes, key)
	v, err := nd.group.Get(context.Background(), key)
	if err != nil || v.String() != "value-of-orphan" {
		t.Fatalf("Get with the owner down: %q, %v", v.String(), err)
	}
	if nd.group.Stats.PeerErrors.Load() != 1 || nd.group.Stats.LocalLoads.Load() != 1 {
		t.Fatalf("stats: %d peer errors, %d local loads", nd.group.Stats.PeerErrors.Load(), nd.group.Stats.LocalLoads.Load())
	}
}

func TestNotFoundIsNotRetriedLocally(t *testing.T) {
	d := &db{loads: map[string][]string{}}
	nodes := cluster(t, 3, d, 1<<20, Options{})
	key := "missing-1"
	nd := nonOwner(nodes, key)
	if _, err := nd.group.Get(context.Background(), key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
	if loads := d.loadsOf(key); len(loads) != 1 || loads[0] != nodes[0].pool.Owner(key) {
		t.Fatalf("not-found key loaded by %v", loads)
	}
}

func TestEvictionKeepsCacheBounded(t *testing.T) {
	d := &db{loads: map[string][]string{}}
	nodes := cluster(t, 1, d, 400, Options{}) // single node: it owns everything
	g := nodes[0].group
	ctx := context.Background()
	for i := range 100 {
		g.Get(ctx, fmt.Sprintf("k%03d", i)) // ~17 bytes each with the key
	}
	if main, hot := g.CacheBytes(); main+hot > 400 || main == 0 {
		t.Fatalf("cache uses %d+%d bytes", main, hot)
	}
	before := len(d.loadsOf("k000"))
	g.Get(ctx, "k000") // long evicted
	g.Get(ctx, "k099") // still cached
	if len(d.loadsOf("k000")) != before+1 || len(d.loadsOf("k099")) != 1 {
		t.Fatal("LRU eviction picked the wrong keys")
	}
}

func TestKeysWithSlashesAndSpaces(t *testing.T) {
	d := &db{loads: map[string][]string{}}
	nodes := cluster(t, 2, d, 1<<20, Options{})
	key := "a/b c?d#e%f"
	nd := nonOwner(nodes, key)
	v, err := nd.group.Get(context.Background(), key)
	if err != nil || v.String() != "value-of-"+key {
		t.Fatalf("%q, %v", v.String(), err)
	}
}
