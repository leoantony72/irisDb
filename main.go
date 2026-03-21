package main

import (
	"bufio"
	"flag"
	"io"
	"log"
	"net"
	"strings"

	"iris/bus"
	"iris/config"
	"iris/engine"
	"iris/utils"
)

const (
	MASTER_FAIL_THRESHOLD = 1
)

func main() {
	clusterAddr := flag.String("cluster_server", "", "Address of a server in the cluster to join (optional)")
	node_group := flag.String("node_group", "", " Group of the node, eg: asia-ind, eu-west, us-east")
	config_file := flag.String("config_file", "", "Path to server config file(optional)")
	flag.Parse()

	var configData *utils.Config
	if *config_file != "" {
		configData = utils.ReadConfigFile(config_file)
		if configData == nil {
			log.Printf("[WARN] Failed to read config file at %s, using defaults\n", *config_file)
			configData = &utils.Config{}
		}
	} else {
		configData = &utils.Config{}
	}

	IrisDb, err := engine.NewEngine(configData.RocksDBPath)
	if err != nil {
		log.Fatalf("Failed to init Pebble DB: %v", err)
	}
	defer IrisDb.Close()
	var server *config.Server
	loaded_data, err := CheckAndLoadMetadata(IrisDb)
	if err == nil {
		server = loaded_data
		log.Printf("[INFO] Loaded server config from database. ServerID: %s\n", server.ServerID)
	} else {
		server = config.NewServer(configData, node_group)
		log.Printf("[INFO] Created new server config. ServerID: %s\n", server.ServerID)
	}

	// IMPORTANT: Always refresh the Host field with current machine IP
	// This ensures that if the server is moved to a different network or restarted
	// with a different network configuration, the Host field is up to date.
	// The Host field is used for informational purposes and cluster metadata.
	ip, err := utils.GetLocalIp()
	if err != nil {
		log.Printf("[WARN] Failed to get local IP: %v (using stored value)", err)
	} else {
		if server.Host != ip {
			log.Printf("[INFO] Updating Host IP from %s to %s", server.Host, ip)
		}
		server.Host = ip
	}

	lis, err := net.Listen("tcp", ":"+server.Port)
	if err != nil {
		log.Fatalf("Coudn't start Irisdb at port:%s, err: %s \n", server.Port, err.Error())
		//exits
	}
	server.Listener = lis
	// defer lis.Close()
	log.Printf("🍔IrisDb started at port:%s \n", server.Port)
	log.Printf("📦Server ID:%s\n", server.ServerID)
	log.Printf("🌐Host IP:%s | Addr:%s | Bus Port:%s\n", server.Host, server.Addr, server.BusPort)
	log.Printf("📊Cluster Info - Version: %d, Nodes: %d, Slot Ranges: %d\n", server.Cluster_Version, server.Nnode, server.GetSlotRangeCount())

	go bus.NewBusRoute(server, IrisDb)

	// Determine cluster address: flag takes precedence, then config file
	clusterAddrToUse := *clusterAddr
	if clusterAddrToUse == "" && configData != nil && configData.ClusterAddr != "" {
		clusterAddrToUse = configData.ClusterAddr
	}

	if clusterAddrToUse != "" {
		err := joinCluster(clusterAddrToUse, server, IrisDb)
		if err != nil {
			log.Printf("[WARNING]: failed to join cluster at %s: %v\n", clusterAddrToUse, err)
		} else {
			log.Printf("[✅] Cluster join successful. Server configuration saved to database.\n")
		}
	}

	// Start replica validator AFTER cluster metadata is loaded
	go ReplicaValidatorMiddleware(server, IrisDb)

	go server.Heartbeat()
	for {
		conn, err := lis.Accept()
		if err != nil {
			if server.ShuttingDown.Load() {
				log.Println("[INFO] Server stopped accepting connections")
				return
			}
			log.Printf("Coudn't accept connection, err:%s\n", err.Error())
			continue
		}

		server.Wg.Add(1)
		go handleConnection(conn, IrisDb, server)
	}
}

func handleConnection(conn net.Conn, db *engine.Engine, server *config.Server) {
	defer server.Wg.Done()
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("Reading err: %s", err.Error())
			}
			return
		}

		if server.ShuttingDown.Load() {
			log.Println("[INFO] Server is shutting down, closing connection")
			return
		}

		db.HandleCommand(strings.TrimSpace(line), conn, server)
	}
}
