package memtable;

import db.Manifest;
import wal.WAL;
import wal.WalEntry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemtableService {
    public final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private Memtable activeMemtable;
    private WAL activeWAL;
    private final Manifest manifest;
    
    private final Queue<Memtable> flushQueue = new ArrayDeque<>();
    private static final int MEMTABLE_SIZE_THRESHOLD = 20; // in bytes
    private static final String TOMBSTONE = "<TOMBSTONE>";

    // has to be able to rebuild the flushqueue from the manifest when the server crashes
    public MemtableService(Manifest manifest) throws IOException {
        this.manifest = manifest;
        this.activeMemtable = new Memtable();
        rebuildFromWAL();
        initializeActiveWAL();
    }

    private void initializeActiveWAL() throws IOException {
        try {
            this.activeWAL = new WAL(manifest.getNextWALFileName());
            manifest.addWALFile(activeWAL.getFileName());
        } catch (IOException e) {
            throw new IOException("Failed to initialize active WAL", e);
        }
    }

    private void rebuildFromWAL() throws IOException {
        for (String walFile : manifest.getWALFiles()) {
            Memtable memtable = new Memtable();
            WAL wal = new WAL(walFile);
            for (WalEntry entry : wal.readAll()) {
                memtable.put(entry.getKey(), entry.getValue());
            }
            flushQueue.add(memtable);
        }
    }

    public String get(String key) {
        rwLock.readLock().lock();
        try {
            String v = activeMemtable.get(key);
            if (v != null) return v;

            for (Memtable m : flushQueue) {
                v = m.get(key);
                if (v != null) return v;
            }
            return null;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void put(String key, String value) {
        rwLock.writeLock().lock();
        try {
            activeWAL.append(key, value);
            activeMemtable.put(key, value);
            if (activeMemtable.size() >= MEMTABLE_SIZE_THRESHOLD) {
                rotateMemtable();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void delete(String key) {
        rwLock.writeLock().lock();
        try {
            activeWAL.append(key, TOMBSTONE);
            activeMemtable.put(key, TOMBSTONE);
            if (activeMemtable.size() >= MEMTABLE_SIZE_THRESHOLD) {
                rotateMemtable();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void rotateMemtable() throws IOException {
        manifest.addWALFile(activeWAL.getFileName());
        flushQueue.add(activeMemtable);
        activeMemtable = new Memtable();
        activeWAL = new WAL(manifest.getNextWALFileName());
        manifest.addWALFile(activeWAL.getFileName());
    }

    public boolean hasFlushableMemtable() {
        return !flushQueue.isEmpty();
    }

    public Memtable peekFlushableMemtable() {
        return flushQueue.peek();
    }

    // WAL delete logic from file and manifest reference
    public void removeFlushableMemtable(Memtable memtable) throws IOException {
        flushQueue.remove(memtable);
        String walFile = manifest.getWALFiles().get(flushQueue.size());
        manifest.removeWALFile(walFile);
        Files.deleteIfExists(Paths.get(walFile));
    }

    public ReadWriteLock getLock(){
        return rwLock;
    }
}
