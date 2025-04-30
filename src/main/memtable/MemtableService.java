package main.memtable;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemtableService {
    public final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private Memtable activeMemtable;
    private final Queue<Memtable> flushQueue = new ArrayDeque<>();
    private static final int MEMTABLE_SIZE_THRESHOLD = 1 * 1024 * 1024;
    private static final String TOMBSTONE = "<TOMBSTONE>";

    public MemtableService() {
        this.activeMemtable = new Memtable();
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
            activeMemtable.put(key, value);
            if (activeMemtable.size() > MEMTABLE_SIZE_THRESHOLD) {
                rotateMemtable();
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void delete(String key) {
        rwLock.writeLock().lock();
        try {
            activeMemtable.put(key, TOMBSTONE);
            if (activeMemtable.size() > MEMTABLE_SIZE_THRESHOLD) {
                rotateMemtable();
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void rotateMemtable() {
        flushQueue.add(activeMemtable);
        activeMemtable = new Memtable();
    }

    public boolean hasFlushableMemtable() {
        return !flushQueue.isEmpty();
    }

    public Memtable peekFlushableMemtable() {
        return flushQueue.peek();
    }

    public void removeFlushableMemtable(Memtable memtable) {
        flushQueue.remove(memtable);
    }

    public ReadWriteLock getLock(){
        return rwLock;
    }
}