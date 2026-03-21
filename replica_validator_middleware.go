package main

import (
	"iris/config"
	"iris/distributor"
	"iris/engine"
	"iris/utils"
	"log"
	"strings"
	"time"
)

func ReplicaValidatorMiddleware(server *config.Server, IrisDb *engine.Engine) {
	// Wait a moment to ensure metadata is stable
	time.Sleep(2 * time.Second)

	for {
		time.Sleep(30 * time.Second)
		log.Printf("[✅INFO]: RUNNING REPLICA VALIDATOR for Server %s\n", server.ServerID)
		if !server.ReplicationValidator() {
			log.Printf("[⚠️ WARNING]: Replication factor not met for server %s. Starting repair...\n", server.ServerID)
			mapping, isMaster := server.ForwardRepairRequestToMaster()
			if isMaster {
				handleDistributionFromMaster(mapping, server, IrisDb)
			}
		} else {
			log.Printf("[✅ SUCCESS]: Replication factor met for server %s\n", server.ServerID)
		}
	}
}

func handleDistributionFromMaster(mapping map[string][]string, s *config.Server, db *engine.Engine) {
	for key, replicas := range mapping {
		// key is "start-end"
		parts := strings.Split(key, "-")
		if len(parts) != 2 {
			log.Printf("[WARN] invalid mapping key: %q", key)
			continue
		}

		start, err := utils.ParseUint16(parts[0])
		if err != nil {
			log.Printf("[WARN] bad start in mapping key %q: %v", key, err)
			continue
		}
		end, err := utils.ParseUint16(parts[1])
		if err != nil {
			log.Printf("[WARN] bad end in mapping key %q: %v", key, err)
			continue
		}

		idx := s.FindRangeIndex(start, end)
		r, ok := s.GetSlotRangeByIndex(idx)
		if !ok {
			log.Printf("[WARN] no slot-range found for %d-%d (key=%s)", start, end, key)
			continue
		}

		log.Printf("[💖INFO] rangeMaster: %s | ServerID: %s", r.MasterID, s.ServerID)
		// only the master for this range should initiate transfers
		if r.MasterID != s.ServerID {
			continue
		}

		log.Printf("[💖INFO] MASTER")

		for _, replicaID := range replicas {
			// replicate: master -> replicaID for this range
			log.Printf("[INFO] master %s initiating transfer for range %d-%d -> %s", s.ServerID, start, end, replicaID)
			go distributor.InitiateDataTransferToReplica(replicaID, start, end, db, s)
		}
	}

}
