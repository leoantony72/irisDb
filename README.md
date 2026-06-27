# IrisDb

IrisDb is a distributed, fault-tolerant, horizontally scalable key-value database built in Go. It uses CockroachDB’s **Pebble** storage engine under the hood for highly efficient local data persistence. The system uses consistent sharding over 16,384 virtual slots, a two-phase node joining consensus, active peer gossip, replica verification, automated failover elections, and background anti-entropy sync.

Medium article on the architecture: https://medium.com/@leoantony102/consistent-hashing-isnt-enough-so-i-added-resource-aware-slot-distribution-07ca71adfaac

---

## Architecture Overview

IrisDb employs a decentralized, shared-nothing architecture consisting of master and replica nodes grouped in geographical zones (Node Groups).

```
                      +-------------------+
                      |   Client TCP      |
                      +---------+---------+
                                |
                                v
                      +-------------------+
                      |   Router / Node   |
                      +---+-----------+---+
                          |           |
             (Inter-Node) |           | (Replication/Heartbeats)
             Bus Protocol |           |
                          v           v
                  +-------+---+   +---+-------+
                  |  Node 2   |   |  Node 3   |
                  +-----------+   +-----------+
```

### 1. Storage Engine
Each database node embeds **Pebble** (LSM-tree database engine). Write operations are committed to Pebble synchronously, ensuring transaction safety and durability.

### 2. Sharding & Partitioning (Slot Range System)
* **Consistent Hashing**: The key space is partitioned into **16,384 virtual slots** (0–16383).
* **CRC16 Mapping**: Keys are mapped to slots using CRC16 hashing: `Slot = CRC16(key) % 16384`.
* **Dynamic Slot Allocation**: When a new node joins the cluster, a range is dynamically split and half of the slots are assigned to the new node via a cluster-wide consensus flow.

### 3. Consensus & Cluster Membership
* **Two-Phase Join Protocol**: Joining a cluster utilizes a two-phase prepare/commit protocol over the inter-node communication bus to guarantee state consistency across all live nodes.
  1. **Prepare Phase**: The coordinator validates the join request, computes slot allocations, and prepares metadata.
  2. **Commit Phase**: The metadata is synchronized, slot ownership is updated, and data replication ranges are set.
* **Graceful Departures**: When a node shuts down gracefully, it broadcasts a `LEAVE` command, triggers replica redistribution, promotions, and flushes data before shutting down.

### 4. Dynamic Resource Scoring System
To optimize slot distribution and replica placement, IrisDb tracks node performance and hardware capacity using a dynamic scoring model:
* **Capacity Score**: Calculated based on log-scaled system resources:
  $$\text{CapacityScore} = 0.5 \times \log_2(1 + \text{RAM}_{\text{GiB}}) + 0.3 \times \text{CPU}_{\text{Cores}} + 0.2 \times \log_2(1 + \text{Disk}_{\text{GiB}})$$
* **Network Factor**: A real-time multiplier ($[0.0, 1.0]$) reflecting communication health:
  $$\text{NetworkFactor} = 0.4 \times \text{ClientSuccessRate} + 0.4 \times \text{PeerSuccessRate} + 0.2 \times \text{LatencyScore}$$
  where $\text{LatencyScore} = \frac{1}{1 + \text{AvgLatency}_{\text{ms}}}$. New nodes default to a factor of `1.0` to avoid cold-start penalties.
* **Combined Resource Score**: 
  $$\text{ResourceScore} = \text{CapacityScore} \times \text{NetworkFactor}$$
* **Usage**:
  * **Range Splitting**: The slot range owned by the master node with the *lowest* Resource Score is chosen for splitting first.
  * **Replica Selection**: Replicas are assigned to nodes with the *highest* Resource Scores to ensure backups live on the healthiest, most capable machines.

### 5. Replication & Fault Tolerance
* **Master-Replica Setup**: Every slot range has a single Master node and a configurable number of Replica nodes (`replication_factor`).
* **Active Replication**: Write operations (`SET`, `DEL`) are replicated synchronously over the inter-node bus. The write is acknowledged to the client only after the replication factor threshold is met.
* **Failure Detection & Elections**:
  * Nodes send periodic **Heartbeats** over the bus.
  * If a master node fails to respond within a threshold, a peer initiates a **suspect-leader** flow.
  * A quorum-based election is triggered. The node collects votes, and upon winning, broadcasts the new cluster metadata version.
* **Replica Validator Middleware**: A background process running every 30 seconds checks for under-replicated ranges, automatically selecting healthy candidates based on Resource Scores to restore replication redundancy.

### 6. Anti-Entropy Consistency Engine
To combat silent data corruption or synchronization gaps:
* A background task runs every 60 seconds on master nodes.
* It computes a XOR checksum and key count for all key-value pairs belonging to its owned slot ranges.
* It queries replicas for their digests over the bus. If a discrepancy is detected (diverged replica), it triggers an active, background range re-synchronization.

---

## Inter-Node Bus & Client Protocol

IrisDb separates client requests and inter-node coordination into two dedicated TCP ports.

### Client TCP Protocol (Port `P`)
Line-based text protocol for applications:
* `SET <key> <value>`: Store a key-value pair.
* `GET <key>`: Retrieve a key-value pair.
* `DEL <key>`: Delete a key-value pair.
* `KEYS`: Return all keys in the database.
* `SHOW`: Render a detailed summary of cluster metadata, slots, nodes, and states.
* `SHUTDOWN`: Safe, graceful node decommissioning.

If a client sends a command for a key belonging to a slot owned by another node, IrisDb automatically routes the query or notifies the client where to redirect.

### Inter-Node Bus Protocol (Port `P + 10000`)
Used exclusively for node-to-node administration:
* `JOIN <nodeID> <addr>`: Solicit cluster inclusion.
* `PREPARE <messageID> <targetNode> <slots>`: Initiate 2PC slot split/rebalancing.
* `COMMIT <messageID>`: Commit metadata changes.
* `REPLICATION <key> <val>`: Sync a write to a replica.
* `HEARTBEAT <senderID>`: Keep-alive and status beacon.
* `ANTI_ENTROPY <startSlot> <endSlot>`: Solicit range digest checks.

---

## Configuration

The database can be configured using a JSON configuration file. Below is the configuration structure:

```json
{
  "port": 8001,
  "master_fail_threshold": 3,
  "node_group": "us-east-1",
  "cluster_addr": "192.168.1.50:8001",
  "rocksdb_path": "./data/pebble-db",
  "replication_factor": 2
}
```

### Fields:
| Field | Type | Description |
|---|---|---|
| `port` | `int` | Main client TCP port. The bus port will automatically bind to `port + 10000`. |
| `master_fail_threshold` | `int` | Number of missed heartbeats before initiating a master failover. |
| `node_group` | `string` | Geographical or logical division (e.g. data center zone). |
| `cluster_addr` | `string` | Optional address of an existing node in the cluster to join on startup. |
| `rocksdb_path` | `string` | Filepath for local Pebble LSM-tree database storage. |
| `replication_factor` | `int` | Target replica count for each slot range. |

---

## Getting Started

### Prerequisites
* **Go**: 1.24.0 or newer.

### Installation
Clone the repository and build the binary:
```bash
git clone https://github.com/leoantony72/irisDb.git
cd irisDb
go build -o irisdb main.go
```

### Running a Standalone Node
Start the initial bootstrap node:
```bash
./irisdb -node_group local-group -config_file ./config.json
```
If no config file is passed, IrisDb will auto-allocate a port and start with default configurations.

### Running a Cluster
1. **Start the Bootstrap Node (Node A)**:
   ```bash
   ./irisdb -node_group group-1
   ```
   Assume Node A starts on port `8001`.

2. **Join with a Second Node (Node B)**:
   ```bash
   ./irisdb -node_group group-1 -cluster_server "127.0.0.1:8001"
   ```
   Node B connects to Node A, triggers the Two-Phase Join Protocol, splits the slot ranges, and begins handling its half of the cluster slot traffic.

---

## Code Quality & Verification
* Run unit and integration tests:
  ```bash
  go test ./...
  ```
* Check test coverage:
  ```bash
  go test -coverprofile=coverage.out ./...
  go tool cover -html=coverage.out
  ```
