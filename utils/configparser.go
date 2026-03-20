package utils

import (
	"encoding/json"
	"log"
	"os"
)

type Config struct {
	Port                int    `json:"port"`
	MasterFailThreshold int    `json:"master_fail_threshold"`
	NodeGroup           string `json:"node_group"`
	ClusterAddr         string `json:"cluster_addr"`
	RocksDBPath         string `json:"rocksdb_path"`
	ReplicationFactor   int    `json:"replication_factor"`
}

func ReadConfigFile(path *string) *Config {
	if path == nil || *path == "" {
		return nil
	}

	data, err := os.ReadFile(*path)
	if err != nil {
		log.Printf("Failed to read config file: %v", err)
		return nil
	}

	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		log.Printf("Failed to unmarshal config: %v", err)
		return nil
	}
	return &config
}
