# IrisDb Architecture Documentation

## 1. Core Components

### Database Engine (`engine` package)

- Uses CockroachDB's Pebble as the underlying storage engine
- Handles basic operations: SET, GET, DELETE
- Supports automatic fallback paths for database initialization
- Implements data persistence with synchronous writes

### Configuration (`config` package)

- Manages server configuration and node information
- Handles dynamic port allocation for both main and bus communication
- Maintains cluster metadata and node states
- Implements replication configuration

### Bus System (`bus` package)

- Manages inter-node communication
- Handles cluster operations
- Implements distributed consensus for node joins
- Manages metadata synchronization
- Handles data replication

### Utilities (`utils` package)

- Network utilities (IP address management, port management)
- Data hashing (CRC16)
- Data parsing and validation

## 2. Distributed Architecture

### Node Structure

- Each node contains:
  - Unique Server ID (UUID)
  - Host address and port
  - Bus port (offset by 10000 from main port)
  - Local Pebble DB instance
  - Cluster metadata

### Cluster Management

1. **Slot Range System**

   - Uses a 16384 slot range (0-16383)
   - Slots are distributed among nodes
   - Uses CRC16 for key-slot mapping

2. **Node States**

   - Master nodes (primary data holders)
   - Replica nodes (data backups)
   - Supports dynamic node addition

3. **Metadata Management**
   - Versioned cluster metadata
   - Distributed metadata synchronization
   - Slot range tracking
   - Node membership information

## 3. Consensus Protocol

### Two-Phase Join Protocol

1. **Prepare Phase**

   - Validates join request
   - Allocates slot ranges
   - Updates cluster metadata
   - Ensures consistency across nodes

2. **Commit Phase**
   - Finalizes node addition
   - Updates cluster state
   - Synchronizes metadata
   - Triggers replication if needed

## 4. Data Distribution

### Key Distribution

- Uses CRC16 hashing for key distribution
- Consistent hashing for slot allocation
- Dynamic slot reallocation during node joins

### Replication

- Master-Replica architecture
- Configurable replication factor
- Synchronous replication for write operations
- Replica management during cluster changes

## 5. Communication Protocols

### Client Protocol

- TCP-based communication
- Text-based command protocol
- Supports basic operations (SET, GET, DEL)
- Automatic request routing to correct node

### Inter-Node Protocol (Bus)

- Separate bus port for node communication
- Handles:
  - JOIN operations
  - PREPARE/COMMIT consensus
  - Metadata synchronization
  - Data replication
  - Insert forwarding

## 6. Fault Tolerance

- Supports node failure detection
- Maintains data redundancy through replication
- Versioned metadata for consistency
- Transaction safety through Pebble DB

## 7. Scalability Features

- Dynamic cluster expansion
- Automatic slot rebalancing
- Load distribution through consistent hashing
- Configurable replication factor
- Separate ports for client and cluster traffic

## 8. Command Implementation

### Write Operations

```
SET key value
- Hash key to determine slot
- Route to master node
- Replicate to replica nodes
- Return acknowledgment
```

### Read Operations

```
GET key
- Hash key to determine slot
- Route to appropriate node
- Return value or NOTFOUND
```

### Cluster Operations

```
JOIN nodeid port
PREPARE messageid targetnode slots
COMMIT messageid
```

This architecture provides a robust, distributed key-value store with support for horizontal scaling, data replication, and fault tolerance. The system is designed to maintain consistency while allowing dynamic cluster membership changes.
