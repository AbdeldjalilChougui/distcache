# distcache

[![Go Reference](https://pkg.go.dev/badge/github.com/AbdeldjalilChougui/distcache.svg)](https://pkg.go.dev/github.com/AbdeldjalilChougui/distcache)

A groupcache-style distributed read-through cache in Go, using only the
standard library. Every key has exactly one owner node; other nodes ask
the owner over HTTP instead of hitting your database.

- **`consistenthash`**: a hash ring with virtual nodes that picks a key's
  owner.
- **`singleflight`**: collapses concurrent loads of the same key into one.
- **`lru`**: a size-bounded least-recently-used cache.
- **`distcache`**: ties them together. A `Pool` is one node (peers, ring,
  HTTP handler) and a `Group` is a cache namespace with a `Getter`, a main
  cache for owned keys and a small hot cache for popular keys owned by
  others.

## What it teaches

- **Consistent hashing**: why `hash(key) % N` reshuffles almost every key
  when N changes, while a ring moves only about 1/N, and why virtual nodes
  are needed for an even spread.
- **Thundering herds**: a cold popular key can trigger hundreds of
  identical backend queries at once. singleflight turns them into one, both
  on the requesting node and on the owner.
- **Hot keys**: the owner of a very popular key becomes a bottleneck. A
  copy of the value is kept on the requesting node (1 in 10 fetches, as
  in groupcache, so mostly frequently requested keys stay), capped at
  about 1/8 of the cache.
- **Fill-only caches**: with no Set/Delete, values never go stale between
  nodes and no invalidation protocol is needed. That only works for data
  that doesn't change under a key (put versions in the key: `user:42:v7`).

## Install

Add the library to your module:

```sh
go get github.com/AbdeldjalilChougui/distcache@latest
```

Or install the demo binary:

```sh
go install github.com/AbdeldjalilChougui/distcache/cmd/distcache@latest
```

## Quick start

Three nodes in front of a fake backend that takes 300 ms per load:

```sh
go build -o distcache ./cmd/distcache
P=http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003
./distcache -addr 127.0.0.1:8001 -peers $P &
./distcache -addr 127.0.0.1:8002 -peers $P &
./distcache -addr 127.0.0.1:8003 -peers $P &

curl -i 127.0.0.1:8001/get/user:42   # X-Owner shows which node loaded it
curl -i 127.0.0.1:8003/get/user:42   # fast: fetched from the owner's cache
curl 127.0.0.1:8001/stats
```

As a library:

```go
pool := distcache.NewPool("http://10.0.0.1:8000", distcache.PoolOptions{})
pool.Set("http://10.0.0.1:8000", "http://10.0.0.2:8000", "http://10.0.0.3:8000")
http.Handle(distcache.DefaultBasePath, pool)

users := pool.NewGroup("users", 64<<20, distcache.GetterFunc(
	func(ctx context.Context, key string) ([]byte, error) {
		return loadUserFromDB(ctx, key)
	}), distcache.Options{})

v, err := users.Get(ctx, "user:42")
```

## How it works

```
Group.Get(key)
  main cache / hot cache hit?            → return
  singleflight(key):
    owner = ring.Get(key)
    owner is another node?  GET owner/_distcache/<group>/<key>
        ok         → maybe keep a copy in the hot cache → return
        not found  → return ErrNotFound (no second load)
        unreachable→ fall through and load locally
    Getter(key) → main cache → return
  after each insert: evict LRU entries until main+hot ≤ cacheBytes
  (from the hot cache while it is over 1/8 of main)

Owner side: /_distcache/<group>/<key> → cache or singleflight(Getter)
            (never forwarded again, so different peer lists can't loop)
```

## Tests

```sh
go test -race ./...
```

- `lru`: eviction order, byte accounting, replacing and removing values.
- `consistenthash`: a hand-computable ring (exact owners, what moves when a
  node is added or removed), balance across 4 nodes with 200 virtual nodes,
  and that adding a 5th node moves about 1/5 of 40 000 keys and only to the
  new node.
- `singleflight`: 50 concurrent callers run the function once; errors and
  panics don't wedge the group.
- `distcache` with a **3-node in-process cluster** (real HTTP on
  127.0.0.1):
  - every key is loaded from the backend exactly once, by its owner, no
    matter which node is asked;
  - 90 concurrent requests for one key spread over all nodes cause one
    backend load and at most one peer request per node;
  - the hot cache serves repeated requests locally when enabled, and every
    request goes to the owner when it isn't;
  - the owner being down falls back to a local load;
  - not-found is passed through and not retried;
  - LRU eviction keeps the cache under its byte budget and drops the
    oldest keys;
  - keys with `/`, spaces, `?`, `#` and `%` survive the URL round trip.

## Status and limitations

This is a learning project.

- **No expiry, no Set, no Delete**: by design (see above). Data that
  changes needs versioned keys.
- Peer membership is a static list passed to `Pool.Set`. There's no
  discovery and no health checking; a dead owner is detected per request
  (with the HTTP client timeout) and the key is loaded locally.
- When the peer list changes, keys move to new owners and are loaded again
  there; the old copies just age out.
- The hot-cache policy is random sampling, not a real frequency counter.
- A load's context is the first caller's context: if that caller gives up,
  callers waiting on the same flight get its error.
- Plain HTTP with no authentication between nodes.

## License

MIT. See [LICENSE](LICENSE).
