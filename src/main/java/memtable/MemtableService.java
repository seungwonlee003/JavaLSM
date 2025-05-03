package memtable;

import db.Manifest;
import util.WAL;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemtableService {
    public final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private Memtable activeMemtable;
    private WAL activeWAL;
    private final Queue<Memtable> flushQueue = new ArrayDeque<>();
    private static final int MEMTABLE_SIZE_THRESHOLD = 50;
    private static final String TOMBSTONE = "<TOMBSTONE>";
    private boolean disableFlush = false;  // Initialized to false
    private final Manifest manifest;

    public MemtableService(Manifest manifest) throws IOException {
        this.manifest = manifest;
        // Check if walPaths has WAL memtables
        if (!manifest.walPaths.isEmpty()) {
            // If walPaths has WALs, add the first WAL in the list to the active memtable
            // and the remaining WALs to the flush queue in increasing index order
            List<String> walPaths = manifest.walPaths;
            activeMemtable = new Memtable();
            activeWAL = new WAL(walPaths.get(0));
            WAL.replay(activeMemtable, walPaths.get(0));
            for (int i = 1; i < walPaths.size(); i++) {
                Memtable queuedMemtable = new Memtable();
                WAL.replay(queuedMemtable, walPaths.get(i));
                flushQueue.add(queuedMemtable);
            }
        } else {
            // If no WALs exist, initialize an empty active memtable and a new WAL
            activeMemtable = new Memtable();
            activeWAL = new WAL(generateWALFilePath());
            manifest.addWAL(activeWAL.getFilePath());
        }
    }

    public void setDisableFlush(boolean disableFlush) {
        this.disableFlush = disableFlush;
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

    public void put(String key, String value) throws IOException {
        rwLock.writeLock().lock();
        try {
            activeWAL.writeEntry(key, value);
            activeMemtable.put(key, value);
            if (!disableFlush && activeMemtable.size() > MEMTABLE_SIZE_THRESHOLD) {
                rotateMemtable();
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void delete(String key) {
        rwLock.writeLock().lock();
        try {
            activeWAL.writeEntry(key, TOMBSTONE);
            activeMemtable.put(key, TOMBSTONE);
            if (!disableFlush && activeMemtable.size() > MEMTABLE_SIZE_THRESHOLD) {
                rotateMemtable();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void rotateMemtable() throws IOException {
        flushQueue.add(activeMemtable);
        activeMemtable = new Memtable();
        activeWAL.close();
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
            System.out.println("Removed " + walPathToRemove);
        }
    }

    public ReadWriteLock getLock(){
        return rwLock;
    }

    private String generateWALFilePath() {
        return "./data" + "/wal-" + System.nanoTime() + ".log";
    }
}