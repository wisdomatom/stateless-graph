package x

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestUidDoubleBufferConcurrentNonOverlapping(t *testing.T) {
	defer func() {
		SetUidLeaser(nil)
		SetNextUid(100)
	}()

	const step uint64 = 500
	const gap uint64 = 1000 // Simulate non-contiguous ranges leased by another node.
	var nextBase uint64 = 1000
	var leaseCalls int32

	leaser := func() (uint64, uint64, error) {
		atomic.AddInt32(&leaseCalls, 1)
		time.Sleep(2 * time.Millisecond)
		base := atomic.AddUint64(&nextBase, step+gap) - (step + gap)
		return base, base + step, nil
	}

	SetUidLeaser(leaser)
	s0, e0, err := leaser()
	if err != nil {
		t.Fatalf("initial lease failed: %v", err)
	}
	SetNextUidRange(s0, e0)

	const workers = 32
	const itersPerWorker = 100
	const batch uint64 = 3

	seen := sync.Map{}
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < itersPerWorker; i++ {
				start, end, err := GetNextUidsWithErr(batch)
				if err != nil {
					t.Errorf("GetNextUidsWithErr returned error: %v", err)
					return
				}
				if end-start+1 != batch {
					t.Errorf("expected %d uids, got [%d, %d]", batch, start, end)
					return
				}
				for id := start; id <= end; id++ {
					if _, loaded := seen.LoadOrStore(id, struct{}{}); loaded {
						t.Errorf("duplicate UID allocated: %d", id)
						return
					}
					// Verify id does not fall into any forbidden gap.
					offset := (id - 1000 - 1) % (step + gap)
					if offset >= step {
						t.Errorf("UID %d allocated inside forbidden inter-node gap!", id)
						return
					}
				}
			}
		}()
	}
	wg.Wait()

	if calls := atomic.LoadInt32(&leaseCalls); calls < 2 {
		t.Fatalf("expected multiple leases, got %d", calls)
	}
}

func TestUidDoubleBufferAsyncPrefetchAvoidsStall(t *testing.T) {
	defer func() {
		SetUidLeaser(nil)
		SetNextUid(100)
	}()

	const step uint64 = 100
	var base uint64 = 1000
	var leaseCalls int32

	leaser := func() (uint64, uint64, error) {
		atomic.AddInt32(&leaseCalls, 1)
		// Simulate 40ms TiKV latency.
		time.Sleep(40 * time.Millisecond)
		b := atomic.AddUint64(&base, step) - step
		return b, b + step, nil
	}

	SetUidLeaser(leaser)
	SetNextUidRange(1000, 1000+step)
	base = 1000 + step

	// Consume 60% to cross the 50% watermark and trigger async prefetch.
	if _, _, err := GetNextUidsWithErr(60); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Wait enough time for background prefetch to complete.
	time.Sleep(80 * time.Millisecond)

	if atomic.LoadInt32(&leaseCalls) != 1 {
		t.Fatalf("expected 1 background prefetch call, got %d", atomic.LoadInt32(&leaseCalls))
	}

	// Exhaust remaining 40 and cross boundary into next range — should be near-instant (no 40ms stall).
	t0 := time.Now()
	if _, _, err := GetNextUidsWithErr(40); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s, e, err := GetNextUidsWithErr(5)
	elapsed := time.Since(t0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s != 1101 || e != 1105 {
		t.Fatalf("expected [1101, 1105], got [%d, %d]", s, e)
	}
	if elapsed > 10*time.Millisecond {
		t.Fatalf("boundary switch stalled for %v; expected instant double-buffer swap", elapsed)
	}
}

func TestUidLeaseErrorDoesNotOverflowRange(t *testing.T) {
	defer func() {
		SetUidLeaser(nil)
		SetNextUid(100)
	}()

	SetUidLeaser(func() (uint64, uint64, error) {
		return 0, 0, errors.New("tikv unavailable")
	})
	SetNextUidRange(100, 105)

	if _, _, err := GetNextUidsWithErr(5); err != nil {
		t.Fatalf("expected success within range, got: %v", err)
	}
	// Wait briefly if async prefetch was triggered.
	time.Sleep(10 * time.Millisecond)

	if _, _, err := GetNextUidsWithErr(1); err == nil {
		t.Fatalf("expected error when lease fails, but got nil")
	}
}
