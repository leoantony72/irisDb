package config

func (S *Server) UpdateClusterVersion(newVersion uint64) {
	S.mu.Lock()
	defer S.mu.Unlock()
	S.Cluster_Version = newVersion
}
