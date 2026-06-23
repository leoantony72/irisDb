package config

import (
	"iris/gossip"
	"log"
	"time"
)

func (s *Server) RunResourceScoreUpdater(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if s.ShuttingDown.Load() {
				log.Println("[RESOURCE_SCORE] Updater stopping: server shutting down")
				return
			}

			newScore := s.DetermineResourceScore(".")

			s.mu.Lock()
			self, ok := s.Nodes[s.ServerID]
			if !ok {
				s.mu.Unlock()
				log.Printf("[RESOURCE_SCORE] Self node %s not found in Nodes map", s.ServerID)
				continue
			}

			self.ResourceScoreVersion++
			newVersion := self.ResourceScoreVersion
			self.ResourceScore = newScore
			s.ResourceScore = newScore
			s.mu.Unlock()

			log.Printf("[RESOURCE_SCOR] Updated self score: %.6f (version=%d)", newScore, newVersion)
			if s.Gossip != nil {
				event := gossip.ResourceScoreEvent{
					NodeID:  s.ServerID,
					Score:   newScore,
					Version: newVersion,
				}
				select {
				case s.Gossip.ResourceScoreEvents <- event:
				default:
					log.Println("[RESOURCE_SCORE] ResourceScoreEvents chanel full, dropping event")
				}
			}
		}
	}
}
