package lru

import "testing"

type s string

func (v s) Len() int { return len(v) }

func TestEvictsLeastRecentlyUsed(t *testing.T) {
	var evicted []string
	c := New(int64(len("k1v1") * 3))
	c.OnEvicted = func(k string, _ Value) { evicted = append(evicted, k) }
	c.Add("k1", s("v1"))
	c.Add("k2", s("v2"))
	c.Add("k3", s("v3"))
	c.Get("k1") // k2 is now the oldest
	c.Add("k4", s("v4"))
	if _, ok := c.Get("k2"); ok || len(evicted) != 1 || evicted[0] != "k2" {
		t.Fatalf("evicted %v", evicted)
	}
	for _, k := range []string{"k1", "k3", "k4"} {
		if _, ok := c.Get(k); !ok {
			t.Fatalf("%s missing", k)
		}
	}
	if c.Bytes() != 12 || c.Len() != 3 {
		t.Fatalf("bytes %d len %d", c.Bytes(), c.Len())
	}
}

func TestReplaceAndRemove(t *testing.T) {
	c := New(0)
	c.Add("a", s("1"))
	c.Add("a", s("12345"))
	if v, _ := c.Get("a"); v.(s) != "12345" || c.Bytes() != 6 {
		t.Fatalf("replace: %v %d", v, c.Bytes())
	}
	c.Remove("a")
	if c.Len() != 0 || c.Bytes() != 0 {
		t.Fatal("remove did not account bytes")
	}
	// One oversized value evicts everything including itself.
	c2 := New(4)
	c2.Add("big", s("too large"))
	if c2.Len() != 0 {
		t.Fatal("oversized entry kept")
	}
}
