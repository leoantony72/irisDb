package distributor

import (
	"bufio"
	"fmt"
	"iris/config"
	"iris/engine"
	"iris/utils"
	"log"
	"net"
	"strings"

	"github.com/cockroachdb/pebble"
)

func InitiateDataTransferToReplica(serverID string, start, end uint16, db *engine.Engine, s *config.Server) {

	// Implementation of data transfer initiation logic goes here.
	// loop through every data in this node and hash it to see if it belongs to the range start-end
	// If yes, send it to the serverID

	if db == nil || db.Db == nil {
		log.Println("InitiateDataTransferToReplica: nil db provided")
		return
	}

	iter, err := db.Db.NewIter(&pebble.IterOptions{})
	if err != nil {
		log.Printf("InitiateDataTransferToReplica: failed to create iterator: %s", err.Error())
		return
	}
	defer iter.Close()

	for ok := iter.First(); ok; ok = iter.Next() {
		key := append([]byte{}, iter.Key()...)
		val := append([]byte{}, iter.Value()...)

		if string(key) == "config:server:metadata" {
			continue
		}

		slot := utils.CalculateCRC16(key) % s.N
		if slotInRange(slot, start, end) {
			if err := sendKeyValue(serverID, key, val, s); err != nil {
				log.Printf("failed to send key %q to %s: %v", key, serverID, err)
			}
		}
	}

}

func slotInRange(slot, start, end uint16) bool {
	if start <= end {
		return slot >= start && slot <= end
	}
	return slot >= start || slot <= end
}

func sendKeyValue(serverID string, key, value []byte, s *config.Server) error {
	node, ok := s.GetConnectedNodeData(serverID)
	if !ok {
		return fmt.Errorf("node data not found for serverID: %s", serverID)
	}

	busAddr, err := utils.BumpPort(node.Addr, 10000)
	if err != nil {
		return fmt.Errorf("failed to bump port for serverID %s: %v", serverID, err)
	}

	Sconn, err := net.Dial("tcp", busAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to serverID %s at %s: %v", serverID, busAddr, err)
	}
	defer Sconn.Close()
	//MESSAGE FORMAT: INS KEY VALUE
	//RESPONSE FORMAT: ACK KEY
	msg := fmt.Sprintf("REP %s %s\n", key, value)
	_, err = Sconn.Write([]byte(msg))
	if err != nil {
		Sconn.Close()
		return err
	}

	expectedResponse := "ACK REP"

	reader := bufio.NewReader(Sconn)
	resp, _ := reader.ReadString('\n')
	resp = strings.TrimSpace(resp)
	Sconn.Close()

	if resp != expectedResponse {
		fmt.Printf("res: %s, %s\n", resp, expectedResponse)
		return fmt.Errorf("unexpected response from serverID %s: %s", serverID, resp)
	} else {
		return nil
	}
}
