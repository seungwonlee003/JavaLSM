package db;

import memtable.Memtable;
import memtable.MemtableService;
import sstable.SSTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public class CompactionService {

    private final MemtableService memtableService;
    private final Manifest manifest;
    private final ScheduledExecutorService memtableFlusher;
    private final ScheduledExecutorService compactionRunner;

    public CompactionService(MemtableService memtableService,
                             Manifest manifest) {
        this.memtableService = memtableService;
        this.manifest = manifest;

        memtableFlusher = Executors.newSingleThreadScheduledExecutor();
        memtableFlusher.scheduleAtFixedRate(
                () -> {
                    try {
                        flushMemtables();
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to flush memtables", e);
                    }
                },
                0,
                1000,
                TimeUnit.MILLISECONDS
        );

        compactionRunner = Executors.newSingleThreadScheduledExecutor();
        compactionRunner.scheduleAtFixedRate(
                () -> {
                    try {
                        runCompaction();
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to run compaction", e);
                    }
                },
                0,
                500,
                TimeUnit.MILLISECONDS
        );
    }

    // Reads are only blocked during flushQueue removal and manifest updates to prevent inconsistent reads.
    private void flushMemtables() throws IOException {
        Memtable mem = getFlushableMemtable();
        if (mem == null) {
            return;
        }

        System.out.println("Flushing memtables...");

        SSTable sstable = createSSTableFromMemtable(mem);
        updateFlushQueueAndManifest(mem, sstable);
    }

    private Memtable getFlushableMemtable() {
        Lock readLock = memtableService.getLock().readLock();
        readLock.lock();
        try {
            if (!memtableService.hasFlushableMemtable()) {
                return null;
            }
            return memtableService.peekFlushableMemtable();
        } finally {
            readLock.unlock();
        }
    }

    private SSTable createSSTableFromMemtable(Memtable mem) throws IOException {
        return SSTable.createSSTableFromMemtable(mem);
    }

    private void updateFlushQueueAndManifest(Memtable mem, SSTable sstable) {
        Lock writeLock = memtableService.getLock().writeLock();
        writeLock.lock();
        try {
            memtableService.removeFlushableMemtable(mem); // Modify flushQueue
            Lock manifestWriteLock = manifest.getLock().writeLock();
            manifestWriteLock.lock();
            try {
                manifest.addSSTable(0, sstable); // Update manifest
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                manifestWriteLock.unlock();
            }
        } finally {
            writeLock.unlock();
        }
    }

    // Reads are only blocked during manifest updates to prevent inconsistent SSTable references.
    private void runCompaction() throws IOException {
        int maxLevel = manifest.maxLevel();

        for (int level = 0; level <= maxLevel; level++) {
            List<SSTable> tablesToCompact = getTablesToCompact(level);
            if (tablesToCompact == null) {
                continue;
            }

            List<SSTable> newTables = compactTables(tablesToCompact);

            updateManifest(level, tablesToCompact, newTables);
        }
    }

    private List<SSTable> getTablesToCompact(int level) {
        Lock readLock = manifest.getLock().readLock();
        readLock.lock();
        try {
            List<SSTable> currentLevelTables = manifest.getSSTables(level);
            // level: 0 size = 4, level: 1 size = 20, level: 2 size = 100, level: 3 size = 500... etc
            if (currentLevelTables.size() <= 4 * Math.pow(5, level)) {
                return null;
            }
            List<SSTable> tablesToMerge = new ArrayList<>(currentLevelTables);
            int nextLevel = level + 1;
            List<SSTable> nextLevelTables = manifest.getSSTables(nextLevel);
            tablesToMerge.addAll(nextLevelTables);
            return tablesToMerge;
        } finally {
            readLock.unlock();
        }
    }

    private List<SSTable> compactTables(List<SSTable> tablesToMerge) throws IOException {
        return SSTable.sortedRun("./data", tablesToMerge);
    }

    private void updateManifest(int level, List<SSTable> oldTables, List<SSTable> newTables) throws IOException {
        Lock writeLock = manifest.getLock().writeLock();
        writeLock.lock();
        try {
            manifest.replace(level, newTables);
            for (SSTable table : oldTables) {
                table.delete();
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void stop() {
        memtableFlusher.shutdown();
        compactionRunner.shutdown();
    }
}
