package main.db;

import main.memtable.MemtableService;
import main.sstable.SSTableService;

import java.io.IOException;

public class DB {
    public final MemtableService memtableService;
    public final SSTableService sstableService;
    public final Manifest manifest;

    public DB() throws IOException {
        manifest = new Manifest();
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

    public void display(){
        manifest.displayManifestFile();
    }
}