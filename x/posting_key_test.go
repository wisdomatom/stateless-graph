/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package x

import (
	"bytes"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func TestPostingKeyEncoding(t *testing.T) {
	baseKey := []byte("test.predicate\x00\x01\x02\x03\x00")

	// 1. Delta Key Test
	ts := uint64(123456789)
	deltaKey := EncodePostingDeltaKey(baseKey, ts)
	parsedBase, tag, parsedTs, ok := SplitPostingKey(deltaKey)
	if !ok {
		t.Fatalf("Failed to parse valid delta key")
	}
	if !bytes.Equal(parsedBase, baseKey) {
		t.Errorf("Base key mismatch. Expected %x, got %x", baseKey, parsedBase)
	}
	if tag != TagPostingDelta {
		t.Errorf("Tag mismatch. Expected delta tag, got %d", tag)
	}
	if parsedTs != ts {
		t.Errorf("Timestamp mismatch. Expected %d, got %d", ts, parsedTs)
	}
	if !HasPostingPrefix(deltaKey, baseKey) {
		t.Errorf("Expected deltaKey to match baseKey prefix")
	}

	// 2. Complete Key Test
	completeKey := EncodePostingCompleteKey(baseKey)
	parsedBase, tag, parsedTs, ok = SplitPostingKey(completeKey)
	if !ok {
		t.Fatalf("Failed to parse valid complete key")
	}
	if !bytes.Equal(parsedBase, baseKey) {
		t.Errorf("Base key mismatch. Expected %x, got %x", baseKey, parsedBase)
	}
	if tag != TagPostingComplete {
		t.Errorf("Tag mismatch. Expected complete tag, got %d", tag)
	}
	if parsedTs != 0 {
		t.Errorf("Expected 0 timestamp for complete key, got %d", parsedTs)
	}
	if !HasPostingPrefix(completeKey, baseKey) {
		t.Errorf("Expected completeKey to match baseKey prefix")
	}

	// 3. Ordering Test: Complete Key MUST be sorted before Delta Key in byte order
	if bytes.Compare(completeKey, deltaKey) >= 0 {
		t.Errorf("Complete key (%x) must sort strictly BEFORE delta key (%x)", completeKey, deltaKey)
	}

	// 4. Timestamp Ordering Test: Smaller ts must sort before larger ts
	ts2 := uint64(987654321)
	deltaKey2 := EncodePostingDeltaKey(baseKey, ts2)
	if bytes.Compare(deltaKey, deltaKey2) >= 0 {
		t.Errorf("Delta key at ts=%d must sort before ts=%d", ts, ts2)
	}
}

func TestBatchGet(t *testing.T) {
	// 1. Test Badger BatchGet
	dir := t.TempDir()
	opts := badger.DefaultOptions(dir).WithLoggingLevel(badger.ERROR)
	db, err := badger.OpenManaged(opts)
	if err != nil {
		t.Fatalf("Open badger failed: %v", err)
	}
	defer db.Close()

	badgerKV := NewBadgerKV(db)
	txn := badgerKV.NewTransactionAt(100, true)

	// Write 5 keys
	for i := 0; i < 5; i++ {
		k := []byte(string(rune('a'+i)) + "_key")
		v := []byte(string(rune('a'+i)) + "_val")
		if err := txn.Set(k, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := txn.CommitAt(100, nil); err != nil {
		t.Fatal(err)
	}

	readTxn := badgerKV.NewTransactionAt(100, false)
	defer readTxn.Discard()

	keysToGet := [][]byte{[]byte("a_key"), []byte("c_key"), []byte("e_key"), []byte("not_exist")}
	res, err := readTxn.BatchGet(keysToGet)
	if err != nil {
		t.Fatalf("BatchGet failed: %v", err)
	}
	if len(res) != 3 {
		t.Fatalf("Expected 3 items in BatchGet, got %d", len(res))
	}
	if string(res["a_key"]) != "a_val" || string(res["c_key"]) != "c_val" || string(res["e_key"]) != "e_val" {
		t.Errorf("BatchGet content mismatch: %+v", res)
	}

	// 2. Test TiKV BatchGet if available
	tikvKV, err := NewTiKVKV([]string{"127.0.0.1:2379"})
	if err == nil {
		tTxn := tikvKV.NewTransactionAt(100, true)
		for i := 0; i < 5; i++ {
			k := []byte(string(rune('x'+i)) + "_key")
			v := []byte(string(rune('x'+i)) + "_val")
			if err := tTxn.Set(k, v); err != nil {
				t.Fatal(err)
			}
		}
		if err := tTxn.CommitAt(100, nil); err != nil {
			t.Fatal(err)
		}

		tReadTxn := tikvKV.NewTransactionAt(0, false)
		defer tReadTxn.Discard()

		tKeys := [][]byte{[]byte("x_key"), []byte("y_key"), []byte("z_key"), []byte("missing")}
		tRes, err := tReadTxn.BatchGet(tKeys)
		if err != nil {
			t.Fatalf("TiKV BatchGet failed: %v", err)
		}
		if len(tRes) != 3 {
			t.Fatalf("Expected 3 items in TiKV BatchGet, got %d", len(tRes))
		}
		if string(tRes["x_key"]) != "x_val" || string(tRes["y_key"]) != "y_val" || string(tRes["z_key"]) != "z_val" {
			t.Errorf("TiKV BatchGet content mismatch: %+v", tRes)
		}
		t.Logf("TiKV BatchGet successfully verified!")
	}
}

func TestTiKVIterReverse(t *testing.T) {
	tikvKV, err := NewTiKVKV([]string{"127.0.0.1:2379"})
	if err != nil {
		t.Skipf("TiKV not running, skip: %v", err)
	}
	defer tikvKV.Close()

	wTxn := tikvKV.NewTransactionAt(0, true)
	prefix := []byte("rev_test_idx:")
	k1 := []byte("rev_test_idx:2026-01-01")
	k2 := []byte("rev_test_idx:2026-05-01")
	k3 := []byte("rev_test_idx:2026-09-01")
	_ = wTxn.Set(k1, []byte("val1"))
	_ = wTxn.Set(k2, []byte("val2"))
	_ = wTxn.Set(k3, []byte("val3"))
	if err := wTxn.CommitAt(0, nil); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	rTxn := tikvKV.NewTransactionAt(0, false)
	defer rTxn.Discard()

	// Reverse iterator with Prefix
	itOpt := KVIterOpts{
		PrefetchValues: false,
		IsReverse:      true,
		Prefix:         prefix,
	}
	it := rTxn.NewIterator(itOpt)
	defer it.Close()

	seekKey := []byte("rev_test_idx:2026-06-01")
	var keysFound []string
	for it.Seek(seekKey); it.Valid(); it.Next() {
		keysFound = append(keysFound, string(it.Item().Key()))
	}
	t.Logf("Keys found: %+v", keysFound)
}
