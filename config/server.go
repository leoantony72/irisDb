package config

import (
	"iris/utils"
	"log"
)

type Node struct {
	ServerID string
	Addr     string
}

type SlotRange struct {
	Start uint16
	End   uint16
	Nodes []*Node //list of master nodes
}

type Server struct {
	ServerID string
	Host     string
	Addr     string
	Port     string
	N        uint16       //hosh slots 2^14
	Nnode    uint16       //number of nodes
	Nodes    []*Node      //list of connected nodes
	Metadata []*SlotRange //hash slots
	BusPort  string
}

func NewServer(name string) *Server {
	ip, err := utils.GetLocalIp()
	if err != nil {
		log.Fatalf("Coudn't Configure the Database")
	}
	addr := ip + ":8008"

	node := Server{ServerID: name, Addr: addr, N: 16384, Metadata: make([]*SlotRange, 10), Port: "8008", Host: ip, BusPort: "18008", Nodes: make([]*Node, 500)}
	// @leoantony72 this should be updated to read from the config file or from argument
	node.Metadata = append(node.Metadata, &SlotRange{Start: 0, End: 16383, Nodes: []*Node{{ServerID: name, Addr: addr}}})
	return &node
}
