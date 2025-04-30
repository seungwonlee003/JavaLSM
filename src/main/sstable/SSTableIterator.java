package main.sstable;

import main.util.IOUtils;

import java.io.*;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class SSTableIterator implements Iterator<Map.Entry<String, String>> {
    private final RandomAccessFile file;
    private boolean closed;

    public SSTableIterator(SSTable sstable) {
        try {
            this.file = new RandomAccessFile(sstable.getFilePath(), "r");
            this.closed = false;
        } catch (IOException e) {
            throw new RuntimeException("Failed to open SSTable file for iteration", e);
        }
    }

    @Override
    public boolean hasNext() {
        try {
            return !closed && file.getFilePointer() < file.length();
        } catch (IOException e) {
            throw new RuntimeException("Error checking file pointer", e);
        }
    }

    @Override
    public Map.Entry<String, String> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        try {
            String key = IOUtils.readString(file);
            String value = IOUtils.readString(file);
            return new AbstractMap.SimpleEntry<>(key, value);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read next entry from SSTable", e);
        }
    }

    public void close() {
        if (!closed) {
            try {
                file.close();
                closed = true;
            } catch (IOException e) {
                throw new RuntimeException("Failed to close SSTableIterator", e);
            }
        }
    }
}