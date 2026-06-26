package bus

import (
	"net"
	"strconv"
)

// GET KEY VERSIONID
func (b *Bus) HandleGet(conn net.Conn, parts []string) {
	if len(parts) != 3 {
		_, _ = conn.Write([]byte("ERR: Not Enough Arguments\n"))
		return
	}
	key := parts[1]
	version := parts[2]

	cluster_version := b.server.GetClusterVersion()

	parsedVersion, err := strconv.ParseUint(version, 10, 64)
	if err != nil {
		_, _ = conn.Write([]byte("ERR: Invalid Version ID\n"))
		return
	}

	//this could be a bottleneck: as metadata sync could take time
	if parsedVersion != cluster_version {
		if parsedVersion < cluster_version {
			//sender node has outdated version of metadata //tell them to update the version
			_, _ = conn.Write([]byte("ERR: Metadata is Outdated. Please Update metadata\n"))
		} else {
			//this servers metadata is outdated
			//req to the master for the new metadata
			err = b.server.RequestMetadataSnapShot()
			if err != nil {

				return
			}
		}
	}
	val, err := b.db.Get(key)
	if err != nil {
		conn.Write([]byte("NOTFOUND\n"))
		return
	}
	//msg: VAL
	conn.Write([]byte(val + "\n"))
}
