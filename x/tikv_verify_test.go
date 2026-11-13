package x

import (
	"bytes"
	"context"
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

func TestTiKVReadYourOwnWrites(t *testing.T) {
	db, err := NewTiKVKV([]string{"127.0.0.1:2379"})
	if err != nil {
		t.Fatalf("Failed to create TiKV DB: %v", err)
	}
	defer db.Close()

	ts, err := db.GetTimestamp(context.Background())
	if err != nil {
		t.Fatalf("Failed to get timestamp: %v", err)
	}

	wTxn := db.NewTransactionAt(ts, true)
	defer wTxn.Discard()

	testKey := []byte(fmt.Sprintf("test_ryow_%d", ts))
	testVal := []byte("hello_uncommitted_world")

	err = wTxn.Set(testKey, testVal)
	if err != nil {
		t.Fatalf("wTxn.Set failed: %v", err)
	}

	prefix := []byte(fmt.Sprintf("test_prefix_%d", ts))
	err = wTxn.Set(append(prefix, []byte("_1")...), []byte("val1"))
	if err != nil {
		t.Fatalf("wTxn.Set prefix 1 failed: %v", err)
	}
	err = wTxn.Set(append(prefix, []byte("_2")...), []byte("val2"))
	if err != nil {
		t.Fatalf("wTxn.Set prefix 2 failed: %v", err)
	}

	// 1. Read on the uncommitted transaction using update=false (Read-only query)
	rTxn := db.NewTransactionAt(ts, false)
	item, err := rTxn.Get(testKey)
	if err != nil {
		t.Fatalf("Expected to read uncommitted write, got err: %v", err)
	}
	val, err := item.ValueCopy(nil)
	if err != nil {
		t.Fatalf("ValueCopy failed: %v", err)
	}
	if !bytes.Equal(val, testVal) {
		t.Fatalf("Expected %s, got %s", string(testVal), string(val))
	}

	// Verify uncommitted iterator scan
	it := rTxn.NewIterator(KVIterOpts{Prefix: prefix})
	var scannedKeys []string
	for it.Seek(prefix); it.Valid(); it.Next() {
		scannedKeys = append(scannedKeys, string(it.Item().Key()))
	}
	it.Close()
	if len(scannedKeys) != 2 {
		t.Fatalf("Expected 2 scanned keys from uncommitted write, got %d: %v", len(scannedKeys), scannedKeys)
	}

	// 2. Discard read transaction (should NOT abort write transaction)
	rTxn.Discard()

	// 3. Independent transaction (different TS) cannot see it yet
	indepTs, _ := db.GetTimestamp(context.Background())
	indepTxn := db.NewTransactionAt(indepTs, false)
	_, err = indepTxn.Get(testKey)
	if err != ErrKeyNotFound {
		t.Fatalf("Independent transaction should not see uncommitted write, got err: %v", err)
	}
	indepTxn.Discard()

	// 4. Commit write transaction
	commitTs, _ := db.GetTimestamp(context.Background())
	err = wTxn.CommitAt(commitTs, nil)
	if err != nil {
		t.Fatalf("CommitAt failed: %v", err)
	}

	// 5. Independent transaction can now see committed data
	indepTxn2 := db.NewTransactionAt(0, false)
	defer indepTxn2.Discard()
	item2, err := indepTxn2.Get(testKey)
	if err != nil {
		t.Fatalf("Expected to read committed write, got err: %v", err)
	}
	val2, _ := item2.ValueCopy(nil)
	if !bytes.Equal(val2, testVal) {
		t.Fatalf("Expected %s, got %s", string(testVal), string(val2))
	}
}
