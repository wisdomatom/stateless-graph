/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package x

import (
	"bytes"
	"encoding/binary"
)

const (
	// TagPostingComplete marks the full consolidated posting list snapshot.
	TagPostingComplete byte = 0x00
	// TagPostingDelta marks an incremental delta posting list.
	TagPostingDelta byte = 0x01
	// TagPostingSeparator separates the logical base key from the versioning tag.
	TagPostingSeparator byte = 0x00
)

// EncodePostingDeltaKey appends [0x00, 0x01, BigEndian(ts)] to the baseKey.
// Format: <baseKey> ‖ 0x00 ‖ 0x01 ‖ <uint64 ts> (total len = len(baseKey) + 10)
func EncodePostingDeltaKey(baseKey []byte, ts uint64) []byte {
	k := make([]byte, len(baseKey)+10)
	copy(k, baseKey)
	k[len(baseKey)] = TagPostingSeparator
	k[len(baseKey)+1] = TagPostingDelta
	binary.BigEndian.PutUint64(k[len(baseKey)+2:], ts)
	return k
}

// EncodePostingCompleteKey appends [0x00, 0x00] to the baseKey.
// Format: <baseKey> ‖ 0x00 ‖ 0x00 (total len = len(baseKey) + 2)
func EncodePostingCompleteKey(baseKey []byte) []byte {
	k := make([]byte, len(baseKey)+2)
	copy(k, baseKey)
	k[len(baseKey)] = TagPostingSeparator
	k[len(baseKey)+1] = TagPostingComplete
	return k
}

// PostingKeyPrefix returns the search prefix for all versions of a baseKey.
// Format: <baseKey> ‖ 0x00
func PostingKeyPrefix(baseKey []byte) []byte {
	p := make([]byte, len(baseKey)+1)
	copy(p, baseKey)
	p[len(baseKey)] = TagPostingSeparator
	return p
}

// SplitPostingKey parses a physical key into its baseKey, tag, and timestamp.
// If the key is not in versioned posting format, it returns ok = false.
func SplitPostingKey(rawKey []byte) (baseKey []byte, tag byte, ts uint64, ok bool) {
	n := len(rawKey)
	// Check for Delta: requires at least 10 bytes suffix [0x00, 0x01, uint64]
	if n >= 10 && rawKey[n-10] == TagPostingSeparator && rawKey[n-9] == TagPostingDelta {
		ts = binary.BigEndian.Uint64(rawKey[n-8:])
		return rawKey[:n-10], TagPostingDelta, ts, true
	}
	// Check for Complete: requires at least 2 bytes suffix [0x00, 0x00]
	if n >= 2 && rawKey[n-2] == TagPostingSeparator && rawKey[n-1] == TagPostingComplete {
		return rawKey[:n-2], TagPostingComplete, 0, true
	}
	return rawKey, 0, 0, false
}

// HasPostingPrefix checks if rawKey belongs to the versioned range of baseKey.
func HasPostingPrefix(rawKey, baseKey []byte) bool {
	prefix := PostingKeyPrefix(baseKey)
	return bytes.HasPrefix(rawKey, prefix)
}
