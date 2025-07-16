package utils

import (
	"fmt"
	"net"
	"strconv"
)

func BumpPort(addr string, delta int) (string, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("invalid addr %q: %w", addr, err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", fmt.Errorf("invalid port %q: %w", portStr, err)
	}

	newPort := port + delta
	if newPort < 0 || newPort > 0xFFFF {
		return "", fmt.Errorf("resulting port %d out of range", newPort)
	}

	return net.JoinHostPort(host, strconv.Itoa(newPort)), nil
}
