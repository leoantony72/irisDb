package utils

import (
	"fmt"
	"strconv"
)

func ParseUint16(s string) (uint16, error) {
	n, err := strconv.ParseUint(s, 10, 16)
	if err != nil {
		return 0, fmt.Errorf("invalid uint16 value: %v", err)
	}
	return uint16(n), nil
}

func ParseFloat64(s string) (float64, error) {
	n, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid float64 value: %v", err)
	}
	return n, nil
}

func parseResourceScore(scoreStr string) (float64, error) {
	var score float64
	_, err := fmt.Sscanf(scoreStr, "%f", &score)
	if err != nil {
		return 0, fmt.Errorf("invalid resource score format: %w", err)
	}
	return score, nil
}
