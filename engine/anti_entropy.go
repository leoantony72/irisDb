package engine

import (
	"hash/crc32"
	"iris/utils"
	"log"

	"github.com/cockroachdb/pebble"
)

// RangeDigest holds the digest (key count + XOR checksum) for a given slot range.
// The checksum is order-independent: XOR of CRC32(key + ":" + value) for every
// key-value pair whose slot falls within [Start, End].
type RangeDigest struct {
	Start    uint16
	End      uint16
	KeyCount uint64
	Checksum uint32
}

// ComputeRangeDigest scans all keys in Pebble and computes a digest for the
// slot range [start, end]. The slot for each key is determined by
// CRC16(key) % totalSlots, matching the same hashing IrisDb uses for routing.
//
// Internal metadata keys (e.g. "config:server:metadata") are excluded.
func (e *Engine) ComputeRangeDigest(start, end, totalSlots uint16) RangeDigest {
	digest := RangeDigest{
		Start: start,
		End:   end,
	}

	if e.Db == nil {
		log.Println("[ANTI_ENTROPY] ComputeRangeDigest: database is nil")
		return digest
	}

	iter, err := e.Db.NewIter(&pebble.IterOptions{})
	if err != nil {
		log.Printf("[ANTI_ENTROPY] ComputeRangeDigest: failed to create iterator: %v", err)
		return digest
	}
	defer iter.Close()

	for ok := iter.First(); ok; ok = iter.Next() {
		key := iter.Key()

		// Skip internal metadata key
		if string(key) == "config:server:metadata" {
			continue
		}

		slot := utils.CalculateCRC16(key) % totalSlots
		if slot < start || slot > end {
			continue
		}

		// Build CRC32 of "key:value" for this entry
		val := iter.Value()
		combined := make([]byte, 0, len(key)+1+len(val))
		combined = append(combined, key...)
		combined = append(combined, ':')
		combined = append(combined, val...)

		entryCRC := crc32.ChecksumIEEE(combined)

		digest.KeyCount++
		digest.Checksum ^= entryCRC
	}

	return digest
}
