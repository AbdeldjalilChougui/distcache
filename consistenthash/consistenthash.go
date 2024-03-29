// Package consistenthash maps keys to nodes on a hash ring. Each node is
// placed on the ring many times (virtual nodes) so load evens out and
// adding or removing a node only moves about 1/N of the keys.
package consistenthash

import (
	"hash/crc32"
	"slices"
	"sort"
	"strconv"
)

// Hash maps bytes to a ring position.
type Hash func(data []byte) uint32

// Map is a hash ring. It is not safe for concurrent mutation.
type Map struct {
	hash     Hash
	replicas int
	ring     []uint32          // sorted positions
	owner    map[uint32]string // position -> node
	nodes    map[string]bool
}

// New returns a ring with the given number of virtual nodes per node.
// A nil hash uses CRC-32 (IEEE).
func New(replicas int, hash Hash) *Map {
	if hash == nil {
		hash = crc32.ChecksumIEEE
	}
	if replicas < 1 {
		replicas = 1
	}
	return &Map{hash: hash, replicas: replicas, owner: map[uint32]string{}, nodes: map[string]bool{}}
}

// Add places nodes on the ring.
func (m *Map) Add(nodes ...string) {
	for _, n := range nodes {
		if m.nodes[n] {
			continue
		}
		m.nodes[n] = true
		for i := range m.replicas {
			h := m.hash([]byte(strconv.Itoa(i) + "#" + n))
			if _, taken := m.owner[h]; taken {
				continue // rare collision: first node keeps the point
			}
			m.owner[h] = n
			m.ring = append(m.ring, h)
		}
	}
	slices.Sort(m.ring)
}

// Remove takes a node off the ring.
func (m *Map) Remove(node string) {
	if !m.nodes[node] {
		return
	}
	delete(m.nodes, node)
	m.ring = slices.DeleteFunc(m.ring, func(h uint32) bool {
		if m.owner[h] == node {
			delete(m.owner, h)
			return true
		}
		return false
	})
}

// Get returns the node owning key ("" for an empty ring): the first
// virtual node clockwise from the key's hash.
func (m *Map) Get(key string) string {
	if len(m.ring) == 0 {
		return ""
	}
	h := m.hash([]byte(key))
	i := sort.Search(len(m.ring), func(i int) bool { return m.ring[i] >= h })
	if i == len(m.ring) {
		i = 0
	}
	return m.owner[m.ring[i]]
}

// Nodes returns the nodes on the ring, sorted.
func (m *Map) Nodes() []string {
	out := make([]string, 0, len(m.nodes))
	for n := range m.nodes {
		out = append(out, n)
	}
	slices.Sort(out)
	return out
}
