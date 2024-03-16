// Package lru is a size-bounded least-recently-used cache. It is not safe
// for concurrent use; callers lock around it.
package lru

import "container/list"

// Value is anything with a size in bytes.
type Value interface {
	Len() int
}

// Cache evicts the least recently used entries once the total size of
// keys and values exceeds MaxBytes.
type Cache struct {
	MaxBytes  int64 // 0 = unlimited
	OnEvicted func(key string, v Value)

	used  int64
	ll    *list.List
	items map[string]*list.Element
}

type entry struct {
	key string
	v   Value
}

// New returns an empty cache.
func New(maxBytes int64) *Cache {
	return &Cache{MaxBytes: maxBytes, ll: list.New(), items: map[string]*list.Element{}}
}

// Get returns the value and marks it as recently used.
func (c *Cache) Get(key string) (Value, bool) {
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		return el.Value.(*entry).v, true
	}
	return nil, false
}

// Add inserts or replaces a value, then evicts as needed.
func (c *Cache) Add(key string, v Value) {
	if el, ok := c.items[key]; ok {
		e := el.Value.(*entry)
		c.used += int64(v.Len()) - int64(e.v.Len())
		e.v = v
		c.ll.MoveToFront(el)
	} else {
		c.items[key] = c.ll.PushFront(&entry{key, v})
		c.used += int64(len(key) + v.Len())
	}
	for c.MaxBytes > 0 && c.used > c.MaxBytes && c.ll.Len() > 0 {
		c.RemoveOldest()
	}
}

// Remove deletes key.
func (c *Cache) Remove(key string) {
	if el, ok := c.items[key]; ok {
		c.remove(el)
	}
}

// RemoveOldest evicts the least recently used entry.
func (c *Cache) RemoveOldest() {
	if el := c.ll.Back(); el != nil {
		c.remove(el)
	}
}

func (c *Cache) remove(el *list.Element) {
	e := el.Value.(*entry)
	c.ll.Remove(el)
	delete(c.items, e.key)
	c.used -= int64(len(e.key) + e.v.Len())
	if c.OnEvicted != nil {
		c.OnEvicted(e.key, e.v)
	}
}

// Len is the number of entries.
func (c *Cache) Len() int { return c.ll.Len() }

// Bytes is the current total size.
func (c *Cache) Bytes() int64 { return c.used }
