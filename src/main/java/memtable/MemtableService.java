package memtable;

import sstable.SSTable;
import util.Manifest;
import util.WAL;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemtableService {
    private final Manifest manifest;
    private Memtable activeMemtable;
    private WAL activeWAL;
    private final Queue<Memtable> flushQueue = new ConcurrentLinkedQueue<>();
    private static final int MEMTABLE_SIZE_THRESHOLD = 4 * 1024 * 1024;
    private static final String TOMBSTONE = "<TOMBSTONE>";
    private boolean disableFlush = false;
    public final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public MemtableService(Manifest manifest) throws IOException {
        this.manifest = manifest;
        if (!manifest.walPaths.isEmpty()) {
            List<String> walPaths = manifest.walPaths;
            int lastIndex = walPaths.size() - 1;
            activeMemtable = new Memtable();
            activeWAL = new WAL(walPaths.get(lastIndex));
            WAL.replay(activeMemtable, walPaths.get(lastIndex));
            for (int i = 0; i < lastIndex; i++) {
                Memtable queuedMemtable = new Memtable();
                WAL.replay(queuedMemtable, walPaths.get(i));
                flushQueue.add(queuedMemtable);
            }
        } else {
            activeMemtable = new Memtable();
            activeWAL = new WAL(generateWALFilePath());
            manifest.addWAL(activeWAL.getFilePath());
        }
    }

    public String get(String key) {
        Lock readLock = rwLock.readLock();
        readLock.lock();
        try {
            String v = activeMemtable.get(key);
            if (v != null) return v;

            for (Memtable m : flushQueue) {
                v = m.get(key);
                if (v != null) return v;
            }
            return null;
        } finally {
            readLock.unlock();
        }
    }

    public void put(String key, String value) throws IOException {
        rwLock.writeLock().lock();
        manifest.getLock().writeLock().lock();
        try {
            activeWAL.writeEntry(key, value);
            activeMemtable.put(key, value);
            if (!disableFlush && activeMemtable.size() > MEMTABLE_SIZE_THRESHOLD) {
                rotateMemtable();
            }
        } finally {
            manifest.getLock().writeLock().unlock();
            rwLock.writeLock().unlock();
        }
    }

    public void delete(String key) {
        rwLock.writeLock().lock();
        manifest.getLock().writeLock().lock();
        try {
            activeWAL.writeEntry(key, TOMBSTONE);
            activeMemtable.put(key, TOMBSTONE);
            if (!disableFlush && activeMemtable.size() > MEMTABLE_SIZE_THRESHOLD) {
                rotateMemtable();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            manifest.getLock().writeLock().unlock();
            rwLock.writeLock().unlock();
        }
    }

    public void rotateMemtable() throws IOException {
        flushQueue.add(activeMemtable);
        activeWAL.close();

        activeMemtable = new Memtable();
        activeWAL = new WAL(generateWALFilePath());
        manifest.addWAL(activeWAL.getFilePath());
    }

    public boolean hasFlushableMemtable() {
        return !flushQueue.isEmpty();
    }

    public Memtable peekFlushableMemtable() {
        return flushQueue.peek();
    }

    public void removeFlushableMemtable(Memtable memtable) throws IOException {
        flushQueue.remove(memtable);
        // Assume the WAL to remove is the oldest in the manifest's walPaths that corresponds to the flushed memtable
        if (!manifest.walPaths.isEmpty()) {
            String walPathToRemove = manifest.walPaths.get(0); // Oldest WAL
            manifest.removeWAL(walPathToRemove);
            new WAL(walPathToRemove).delete();
        }
    }

    public ReadWriteLock getLock() {
        return rwLock;
    }

    private String generateWALFilePath() {
        return "./data" + "/wal-" + System.nanoTime() + ".log";
    }

    public void setDisableFlush(boolean disableFlush) {
        this.disableFlush = disableFlush;
    }

    // flushes all remaining memtable in the flushQueue into SSTable
    public void flushAllRemaining() throws IOException {
        rwLock.writeLock().lock();
        try {
            // Flush active memtable if it has data
            if (activeMemtable.size() > 0) {
                rotateMemtable(); // Move active to flushQueue
            }

            // Flush all memtables in the queue
            while (!flushQueue.isEmpty()) {
                Memtable mem = flushQueue.poll();
                SSTable sstable = SSTable.createSSTableFromMemtable(mem);

                Lock manifestLock = manifest.getLock().writeLock();
                manifestLock.lock();
                try {
                    manifest.addSSTable(0, sstable);
                    if (!manifest.walPaths.isEmpty()) {
                        String walToRemove = manifest.walPaths.remove(0);
                        new WAL(walToRemove).delete();
                        manifest.persist();
                    }
                } finally {
                    manifestLock.unlock();
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void close() throws IOException {
        activeWAL.close();
        flushAllRemaining();
    }
}