package main

import (
	"iris/config"
	"iris/distributor"
	"iris/engine"
)

// AntiEntropyAdapter bridges the engine and config packages for the
// anti-entropy mechanism, satisfying config.AntiEntropyDB without
// creating circular imports.
type AntiEntropyAdapter struct {
	db     *engine.Engine
	server *config.Server
}

// ComputeRangeDigest delegates to the engine's digest computation and
// converts the result to the config package's RangeDigest type.
func (a *AntiEntropyAdapter) ComputeRangeDigest(start, end, totalSlots uint16) config.RangeDigest {
	d := a.db.ComputeRangeDigest(start, end, totalSlots)
	return config.RangeDigest{
		Start:    d.Start,
		End:      d.End,
		KeyCount: d.KeyCount,
		Checksum: d.Checksum,
	}
}

// InitiateDataTransferToReplica delegates to the distributor package's
// data transfer function to re-sync a diverged replica.
func (a *AntiEntropyAdapter) InitiateDataTransferToReplica(replicaID string, start, end uint16) {
	distributor.InitiateDataTransferToReplica(replicaID, start, end, a.db, a.server)
}
