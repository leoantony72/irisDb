package main

import "log"

type Node struct {
	ServerID string
	Addr     string
}

type Metadata struct {
	RangeMap map[string]string
}
type Server struct {
	ServerID string
	Addr     string
	N        int //hosh slots 2^14
	Nnode    int
	Nodes    []*Node
	Metadata *Metadata
}

func NewServer(name string) *Server {
	ip, err := GetLocalIp()
	if err != nil {
		log.Fatalf("Coudn't Configure the Database")
	}
	addr := ip + ":8008"

	metadata := Metadata{RangeMap: make(map[string]string, 200)}
	Peers = append(Peers, &Node{ServerID: name, Addr: addr})
	node := Server{ServerID: name, Addr: addr, N: 16384, Metadata: &metadata}
	return &node
}
