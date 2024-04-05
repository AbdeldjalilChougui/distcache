package singleflight

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDedupesConcurrentCalls(t *testing.T) {
	var g Group
	var calls atomic.Int32
	release := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan any, 50)
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err, _ := g.Do("k", func() (any, error) {
				calls.Add(1)
				<-release
				return "value", nil
			})
			if err != nil {
				t.Error(err)
			}
			results <- v
		}()
	}
	time.Sleep(50 * time.Millisecond) // let everyone pile up
	close(release)
	wg.Wait()
	close(results)
	if n := calls.Load(); n != 1 {
		t.Fatalf("fn ran %d times", n)
	}
	for v := range results {
		if v != "value" {
			t.Fatalf("got %v", v)
		}
	}
	// After the flight lands, a new call runs again.
	g.Do("k", func() (any, error) { calls.Add(1); return nil, nil })
	if calls.Load() != 2 {
		t.Fatal("result was cached instead of re-run")
	}
}

func TestErrorsAndPanics(t *testing.T) {
	var g Group
	boom := errors.New("boom")
	if _, err, _ := g.Do("e", func() (any, error) { return nil, boom }); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	if _, err, _ := g.Do("p", func() (any, error) { panic("x") }); err == nil {
		t.Fatal("panic not turned into an error")
	}
	if _, err, _ := g.Do("p", func() (any, error) { return 1, nil }); err != nil {
		t.Fatal("group stuck after a panic")
	}
}
