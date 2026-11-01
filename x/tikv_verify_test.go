package x

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/tikv/client-go/v2/txnkv"
)

func TestInspectTiKVData(t *testing.T) {
	client, err := txnkv.NewClient([]string{"127.0.0.1:2379"})
	if err != nil {
		t.Fatalf("Failed to connect to TiKV: %v", err)
	}
	defer client.Close()

	txn, err := client.Begin()
	if err != nil {
		t.Fatalf("Begin txn failed: %v", err)
	}
	defer txn.Rollback()

	it, err := txn.Iter(nil, nil)
	if err != nil {
		t.Fatalf("Iter failed: %v", err)
	}
	defer it.Close()

	totalKeys := 0
	foundUser := false
	var samples []string

	target := []byte("wisdomatompan@gmail.com")

	for it.Valid() {
		totalKeys++
		k := it.Key()
		v := it.Value()

		if totalKeys <= 10 {
			samples = append(samples, fmt.Sprintf("Key: %q (len %d)", string(k), len(k)))
		}

		if bytes.Contains(k, target) || bytes.Contains(v, target) {
			foundUser = true
			samples = append(samples, fmt.Sprintf(">>> MATCH USER DATA! Key: %q, ValueLen: %d", string(k), len(v)))
		}

		if err := it.Next(); err != nil {
			break
		}
	}

	t.Logf("====== TIKV STORAGE INSPECTION RESULT ======")
	t.Logf("Total Keys physically stored in TiKV: %d", totalKeys)
	t.Logf("Found registered user data in TiKV: %v", foundUser)
	for _, s := range samples {
		t.Logf("Sample: %s", s)
	}
	t.Logf("============================================")

	if totalKeys == 0 {
		t.Fatalf("Expected keys in TiKV, but found 0!")
	}
}
