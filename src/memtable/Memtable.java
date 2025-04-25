package memtable;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class Memtable implements Iterable<Map.Entry<String, String>> {
    // change it to skip list?
    private final NavigableMap<String, String> table = new TreeMap<>();
    private long sizeBytes = 0L;

    // add replaying WAL to restore memtable logic in the constructor

    public String get(String key) {
        return table.get(key);
    }

    public void put(String key, String value) {
        String old = table.put(key, value);
        if (old != null) {
            sizeBytes -= estimateSize(key, old);
        }
        sizeBytes += estimateSize(key, value);
    }

    public long size(){
        return sizeBytes;
    }

    public Iterator<Map.Entry<String, String>> iterator() {
        return table.entrySet().iterator();
    }

    private long estimateSize(String key, String value) {
        int keyLen = key.getBytes(StandardCharsets.UTF_8).length;
        int valueLen = value.getBytes(StandardCharsets.UTF_8).length;
        return keyLen + valueLen;
    }
}
