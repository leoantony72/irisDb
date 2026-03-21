package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"iris/config"
	"iris/engine"
	"iris/utils"
	"log"
	"net"
	"strconv"
	"strings"
)

// joinCluster connects to an existing node in the cluster and requests to join.
func joinCluster(addr string, server *config.Server, db *engine.Engine) error {
	log.Printf("Attempting to join cluster via %s...", addr)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	defer conn.Close()

	joinMsg := fmt.Sprintf("JOIN %s %s %f %s\n", server.ServerID, server.Port, server.ResourceScore, server.GetServerGroup())
	if _, err = conn.Write([]byte(joinMsg)); err != nil {
		return fmt.Errorf("failed to send JOIN message: %w", err)
	}
	log.Printf("Sent JOIN message: %s", strings.TrimSpace(joinMsg))

	reader := bufio.NewReader(conn)

	// 1) Read framed snapshot:
	//    CLUSTER_SNAPSHOT <nbytes>\n
	//    <nbytes gob(config.ClusterSnapshot)>
	header, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read snapshot header: %w", err)
	}
	header = strings.TrimSpace(header)

	hparts := strings.Fields(header)
	if len(hparts) != 2 || hparts[0] != "CLUSTER_SNAPSHOT" {
		return fmt.Errorf("expected CLUSTER_SNAPSHOT header, got: %q", header)
	}

	nbytes, err := strconv.Atoi(hparts[1])
	if err != nil || nbytes <= 0 {
		return fmt.Errorf("invalid CLUSTER_SNAPSHOT length: %q", hparts[1])
	}

	payload := make([]byte, nbytes)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return fmt.Errorf("failed to read snapshot payload (%d bytes): %w", nbytes, err)
	}

	var snap config.ClusterSnapshot
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&snap); err != nil {
		return fmt.Errorf("failed to decode snapshot payload: %w", err)
	}

	fmt.Println(snap)
	server.ApplyClusterSnapshot(snap)
	log.Printf("✅ Applied snapshot. Current metadata version: %d", server.Cluster_Version)

	// 2) Read JOIN_SUCCESS / REJOIN_SUCCESS line (text) AFTER snapshot
	responseLine, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read JOIN_SUCCESS response: %w", err)
	}

	responseLine = strings.TrimSpace(responseLine)
	parts := strings.Fields(responseLine)

	// Expected:
	//   JOIN_SUCCESS <start_slot> <end_slot> <cluster_version>
	// or:
	//   REJOIN_SUCCESS <count> <start_slot> <end_slot> ... <cluster_version>
	if len(parts) == 0 || (parts[0] != "JOIN_SUCCESS" && parts[0] != "REJOIN_SUCCESS") {
		log.Printf("Error: Unexpected JOIN response format: %q", responseLine)
		return fmt.Errorf("unexpected JOIN response: %s", responseLine)
	}

	if parts[0] == "JOIN_SUCCESS" {
		if len(parts) < 4 {
			return fmt.Errorf("invalid JOIN_SUCCESS response: %q", responseLine)
		}

		assignedStart, err := utils.ParseUint16(parts[1])
		if err != nil {
			return fmt.Errorf("invalid start slot in JOIN_SUCCESS: %w", err)
		}
		assignedEnd, err := utils.ParseUint16(parts[2])
		if err != nil {
			return fmt.Errorf("invalid end slot in JOIN_SUCCESS: %w", err)
		}

		clusterVersion, err := utils.ParseUint16(parts[3])
		if err != nil {
			return fmt.Errorf("invalid cluster version in JOIN_SUCCESS: %w", err)
		}
		server.UpdateClusterVersion(uint64(clusterVersion))

		masterNodeID, ok := server.GetServerIDFromAddr(addr)
		if !ok {
			log.Printf("[WARN] Could not determine MasterNodeID from addr %s", addr)
		}
		server.UpdateMasterNodeID(masterNodeID)

		log.Printf("✅ Successfully joined cluster via %s. This server (%s) is responsible for SlotRange [%d - %d].",
			addr, server.ServerID, assignedStart, assignedEnd)

	} else { // REJOIN_SUCCESS
		if len(parts) < 4 {
			return fmt.Errorf("invalid REJOIN_SUCCESS response: %q", responseLine)
		}

		// NOTE: original code used parts[0] as count; keep consistent with your sender.
		count, err := strconv.Atoi(parts[1])
		if err != nil || count < 0 {
			return fmt.Errorf("invalid count in REJOIN_SUCCESS: %q", parts[1])
		}

		// parts layout assumed:
		// REJOIN_SUCCESS <count> <start1> <end1> ... <cluster_version>
		var b strings.Builder
		for i := 0; i < count; i++ {
			start, _ := strconv.Atoi(parts[2+2*i])
			end, _ := strconv.Atoi(parts[3+2*i])
			fmt.Fprintf(&b, " [%d-%d]", start, end)
		}

		clusterVersion, err := utils.ParseUint16(parts[2+2*count])
		if err != nil {
			return fmt.Errorf("invalid cluster version in REJOIN_SUCCESS: %w", err)
		}
		server.UpdateClusterVersion(uint64(clusterVersion))

		log.Printf("✅ Successfully rejoined cluster via %s. This server (%s) is responsible for SlotRange %s",
			addr, server.ServerID, b.String())
	}

	// Save configuration to database after successful cluster join
	if err := db.SaveServerMetadata(server); err != nil {
		log.Printf("[WARN] Failed to save server config to database after cluster join: %v", err)
	}
	return nil
}
