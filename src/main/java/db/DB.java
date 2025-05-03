package db;

import compaction.CompactionService;
import memtable.MemtableService;
import sstable.SSTableService;

import java.io.IOException;

public class DB {
    public final MemtableService memtableService;
    public final SSTableService sstableService;
    public final Manifest manifest;
    public final CompactionService compactionService;

    public DB() throws IOException {
        manifest = new Manifest();
        this.memtableService = new MemtableService(manifest);
        this.sstableService = new SSTableService(manifest);
        this.compactionService = new CompactionService(memtableService, manifest);
    }

    public String get(String key){
        String value = memtableService.get(key);
        if(value != null) return value;
        return sstableService.get(key);
    }

    public void put(String key, String value) throws IOException {
        memtableService.put(key, value);
    }

    public void delete(String key){
        memtableService.delete(key);
    }

    public void display(){
        manifest.displayManifestFile();
    }
}