package config

import (
	"net"
	"strings"
)


type TrackingConn struct {
	net.Conn
	HasError bool
}
func NewTrackingConn(c net.Conn) *TrackingConn {
	return &TrackingConn{Conn: c}
}
func (tc *TrackingConn) Write(b []byte) (int, error) {
	n, err := tc.Conn.Write(b)
	if err != nil {
		tc.HasError = true
		return n, err
	}

	str := string(b)
	if strings.Contains(str, "ERR") || strings.Contains(str, "Errr") {
		
		if !strings.Contains(str, "usage") && !strings.Contains(str, "empty command") {
			tc.HasError = true
		}
	}

	return n, err
}
