package config

import (
	"iris/utils"
	"log"
)

type Node struct {
	ServerID string
	Addr     string
}

type Metadata struct {
	RangeMap []*Node
}
type Server struct {
	ServerID string
	Host     string
	Addr     string
	Port     string
	N        int //hosh slots 2^14
	Nnode    int
	Nodes    []*Node
	Metadata *Metadata
	BusPort  string
}

func NewServer(name string) *Server {
	ip, err := utils.GetLocalIp()
	if err != nil {
		log.Fatalf("Coudn't Configure the Database")
	}
	addr := ip + ":8008"

	metadata := Metadata{RangeMap: make([]*Node, 16384)}
	node := Server{ServerID: name, Addr: addr, N: 16384, Metadata: &metadata, Port: "8008", Host: ip, BusPort: "18008", Nodes: make([]*Node, 500)}
	return &node
}
