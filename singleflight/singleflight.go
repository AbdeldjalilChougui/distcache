// Package singleflight collapses concurrent calls for the same key into
// one: the first caller runs the function, the others wait for its result.
// This stops a cache miss on a hot key from stampeding the backend.
package singleflight

import "sync"

type call struct {
	wg   sync.WaitGroup
	val  any
	err  error
	dups int
}

// Group deduplicates calls by key.
type Group struct {
	mu sync.Mutex
	m  map[string]*call
}

// Do runs fn once for all concurrent callers with the same key. shared
// reports whether the result was given to more than one caller.
func (g *Group) Do(key string, fn func() (any, error)) (v any, err error, shared bool) {
	g.mu.Lock()
	if g.m == nil {
		g.m = map[string]*call{}
	}
	if c, ok := g.m[key]; ok {
		c.dups++
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err, true
	}
	c := &call{}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	func() {
		defer func() {
			// A panicking fn must not leave waiters blocked forever.
			if r := recover(); r != nil {
				c.err = panicError{r}
			}
		}()
		c.val, c.err = fn()
	}()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	shared = c.dups > 0
	g.mu.Unlock()
	return c.val, c.err, shared
}

type panicError struct{ v any }

func (p panicError) Error() string { return "singleflight: function panicked" }
