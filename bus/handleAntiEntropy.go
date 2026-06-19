package bus

import (
	"fmt"
	"iris/utils"
	"log"
	"net"
)

// HandleAntiEntropy handles the ANTI_ENTROPY bus command on the replica side.
// Protocol:
//
//	Request:  ANTI_ENTROPY <start> <end>
//	Response: DIGEST <keycount> <checksum_hex>
//
// The replica computes a digest for the given slot range and returns it so the
// master can compare it with its own digest for divergence detection.
func (b *Bus) HandleAntiEntropy(conn net.Conn, parts []string) {
	if len(parts) != 3 {
		conn.Write([]byte("ERR usage: ANTI_ENTROPY <start> <end>\n"))
		return
	}

	start, err := utils.ParseUint16(parts[1])
	if err != nil {
		conn.Write([]byte("ERR invalid start value\n"))
		return
	}

	end, err := utils.ParseUint16(parts[2])
	if err != nil {
		conn.Write([]byte("ERR invalid end value\n"))
		return
	}

	totalSlots := b.server.N

	digest := b.db.ComputeRangeDigest(start, end, totalSlots)

	log.Printf("[ANTI_ENTROPY] Computed digest for range %d-%d: keys=%d checksum=%08x",
		start, end, digest.KeyCount, digest.Checksum)

	// Response: DIGEST <keycount> <checksum_hex>
	resp := fmt.Sprintf("DIGEST %d %08x\n", digest.KeyCount, digest.Checksum)
	conn.Write([]byte(resp))
}
