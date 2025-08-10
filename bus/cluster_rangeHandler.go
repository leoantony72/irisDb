package bus

import (
	"iris/config"
	"log"
	"math/rand"
)

// determineRange selects a random existing slot range to split.
// It returns the index of the selected range in s.Metadata, and the start/end of the new sub-range.
func DetermineRange(s *config.Server) (int, uint16, uint16) {
	if s.Nnode == 0 || len(s.Metadata) == 0 {
		log.Fatal("No nodes or metadata found to determine range from. Cluster is empty?")
	}

	// Iterate to find a range that can actually be split (has more than 1 slot)
	var selectedIdx int
	var selectedRange *config.SlotRange
	attempts := 0
	maxAttempts := 100 // Prevent infinite loop if all ranges are tiny

	for attempts < maxAttempts {
		selectedIdx = rand.Intn(len(s.Metadata))
		tempRange := s.Metadata[selectedIdx]
		if tempRange.End > tempRange.Start {
			selectedRange = tempRange
			break
		}
		attempts++
	}

	if selectedRange == nil {
		log.Printf("Warning: Could not find a splittable range after %d attempts. Selecting any range.", maxAttempts)
		selectedIdx = rand.Intn(len(s.Metadata)) // Fallback to any range
		selectedRange = s.Metadata[selectedIdx]
	}

	start := selectedRange.Start
	end := selectedRange.End

	mid := (start + end) / 2

	newRangeStart := mid + 1
	newRangeEnd := end

	log.Printf("Selected range for split: %d-%d (owned by %s). New node will take %d-%d. Old node keeps %d-%d.",
		selectedRange.Start, selectedRange.End, selectedRange.MasterID, newRangeStart, newRangeEnd, start, mid)

	return selectedIdx, newRangeStart, newRangeEnd
}
