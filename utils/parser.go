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
