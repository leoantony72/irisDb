package connpool

import "net"

type ConnectionManager struct {
	pool       map[string]*ConnPool
	acquire    chan *AcquireREQ
	release    chan *ReleaseREQ
	removeNode chan *RemoveREQ
}

type AcquireREQ struct {
	nodeId string
}
type ReleaseREQ struct {
	nodeId string
	conn   *net.Conn
}
type RemoveREQ struct {
	nodeID string
}

type ConnPool struct {
}
