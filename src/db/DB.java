package db;

import memtable.MemtableService;
import sstable.SSTableService;

import java.io.IOException;

public class DB {
    private final MemtableService memtableService;
    private final SSTableService sstableService;

    public DB() throws IOException {
        Manifest manifest = new Manifest();
        this.memtableService = new MemtableService();
        this.sstableService = new SSTableService(manifest);
        CompactionService compactionService = new CompactionService(memtableService, manifest);
    }

    public String get(String key){
        String value = memtableService.get(key);
        if(value != null) return value;
        return sstableService.get(key);
    }

    public void put(String key, String value){
        memtableService.put(key, value);
    }

    public void delete(String key){
        memtableService.delete(key);
    }
}
