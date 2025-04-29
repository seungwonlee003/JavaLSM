# JavaLSM: An LSM Tree Based Key-Value Storage in Java

## Introduction
JavaLSM is an LSM tree based storage engine written in Java. It offers a simple key-value interface and is designed to be used as an embedded storage engine.  

## Features
1. Simple key-value interface: put(key, value), get(key), delete(key).
2. Multi-component LSM tree: includes in-memory write buffer and disk-based components for persistence.
3. Read-optimized: uses bloom filter and index blocks to speed up reads.
4. Concurrent reads: uses fine grained locks to allow reads during expensive compaction operations.
5. Automatic compaction: automatically compacts disk-based tables using a full-level leveled compaction strategy to control read and write amplication.
6. Crash recovery: uses write-ahead log to allow for crash recovery for in-memory write buffer.

## Usage
### Opening a DB instance

### Writing to the DB

### Reading from the DB

### Deleting from the DB

### Closing the DB
