package consistenthash

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestRingWithKnownHash(t *testing.T) {
	// Hash = the number itself, so the ring is easy to reason about.
	// Replica i of node n sits at n*10+i; a key's hash is its number.
	m := New(3, func(b []byte) uint32 {
		s := string(b)
		if i := strings.IndexByte(s, '#'); i >= 0 {
			rep, _ := strconv.Atoi(s[:i])
			node, _ := strconv.Atoi(s[i+1:])
			return uint32(node*10 + rep)
		}
		n, _ := strconv.Atoi(s)
		return uint32(n)
	})
	m.Add("2", "4", "6") // points 20,21,22, 40,41,42, 60,61,62
	cases := map[string]string{"2": "2", "11": "2", "23": "4", "27": "4", "41": "4", "50": "6", "63": "2"}
	for k, want := range cases {
		if got := m.Get(k); got != want {
			t.Errorf("Get(%s) = %s, want %s", k, got, want)
		}
	}
	m.Add("8")
	// Only keys between 62 and 82 move (to 8); the rest stay put.
	if m.Get("27") != "4" || m.Get("11") != "2" || m.Get("63") != "8" || m.Get("85") != "2" {
		t.Fatal("adding node 8 moved the wrong keys")
	}
	m.Remove("4")
	if m.Get("27") != "6" {
		t.Fatalf("after removing 4, 27 -> %s", m.Get("27"))
	}
}

func TestBalanceAndMinimalMovement(t *testing.T) {
	m := New(200, nil)
	nodes := []string{"http://a:1", "http://b:1", "http://c:1", "http://d:1"}
	m.Add(nodes...)
	const keys = 40000
	before := make([]string, keys)
	count := map[string]int{}
	for i := range keys {
		before[i] = m.Get(fmt.Sprintf("key-%d", i))
		count[before[i]]++
	}
	for _, n := range nodes {
		if c := count[n]; c < keys/4*70/100 || c > keys/4*130/100 {
			t.Fatalf("unbalanced: %v", count)
		}
	}
	m.Add("http://e:1")
	moved := 0
	for i := range keys {
		after := m.Get(fmt.Sprintf("key-%d", i))
		if after != before[i] {
			if after != "http://e:1" {
				t.Fatalf("key moved between old nodes: %s -> %s", before[i], after)
			}
			moved++
		}
	}
	// Ideal is 1/5 of the keys.
	if moved < keys/5*60/100 || moved > keys/5*140/100 {
		t.Fatalf("%d of %d keys moved, want about %d", moved, keys, keys/5)
	}
	if New(10, nil).Get("x") != "" {
		t.Fatal("empty ring should return no node")
	}
}
