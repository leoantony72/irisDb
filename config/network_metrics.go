package config


func (s *Server) ComputeNetworkFactor() float64 {
	ct, cs, clUs, pt, ps, plUs := s.Net.Snapshot()
	clientRate := 1.0
	if ct > 0 {
		clientRate = float64(cs) / float64(ct)
		if clientRate > 1.0 {
			clientRate = 1.0
		}
	}
	peerRate := 1.0
	if pt > 0 {
		peerRate = float64(ps) / float64(pt)
		if peerRate > 1.0 {
			peerRate = 1.0
		}
	}
	totalRequests := ct + pt
	totalLatencyUs := clUs + plUs
	var avgLatencyMs float64
	if totalRequests > 0 {
		avgLatencyMs = float64(totalLatencyUs) / float64(totalRequests) / 1000.0
	}
	latencyScore := 1.0 / (1.0 + avgLatencyMs)

	return 0.4*clientRate + 0.4*peerRate + 0.2*latencyScore
}
