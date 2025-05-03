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
Create a DB instance by calling its constructor. No arguments are required. By default, Write-Ahead Logging (WAL) is enabled, and both the memtable and SSTable sizes are set to 16GB.
```
DB db = new DB();
```
### Writing to the DB
Use the put method to insert or update entries in the DB. It accepts two String parameters: key and value. Make sure the combined size of the key-value pair does not exceed the default block size of 4KB.
```
db.put(key, value);
```
### Reading from the DB
Use the get method to retrieve a value by its key. It accepts a String parameter and returns the corresponding value.
```
db.get(key);
```
### Deleting from the DB
Use the delete method to remove a key and its associated value from the DB. It accepts a String parameter representing the key.
```
db.delete(key);
```
### Closing the DB
Always call the close method when you're done using the DB. This will gracefully shut down background tasks (like compaction) and finalize the WAL file. It takes no parameters.
```
db.close();
```
