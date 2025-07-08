package utils

import (
	"github.com/howeyc/crc16"
)

func CalculateCRC16(data []byte) uint16 {
	crc := crc16.Checksum(data, crc16.IBMTable)
	return crc
}
