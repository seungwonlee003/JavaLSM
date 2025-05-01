package memtable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class MemtableTest {
    private Memtable memtable;

    @BeforeEach
    public void setup() {
        memtable = new Memtable();
    }

    @Test
    public void shouldPutAndGet() {
        memtable.put("key1", "value1");
        assertEquals("value1", memtable.get("key1"));
        assertNull(memtable.get("key2"));
    }

    @Test
    public void shouldUpdateSizeOnPut() {
        memtable.put("key1", "value1");
        long expectedSize = 4 + "key1".getBytes().length + 4 + "value1".getBytes().length;
        assertEquals(expectedSize, memtable.size());

        memtable.put("key1", "newvalue");
        expectedSize = 4 + "key1".getBytes().length + 4 + "newvalue".getBytes().length;
        assertEquals(expectedSize, memtable.size());
    }

    @Test
    public void shouldIterateInOrder() {
        memtable.put("key2", "value2");
        memtable.put("key1", "value1");
        memtable.put("key3", "value3");

        Iterator<Map.Entry<String, String>> it = memtable.iterator();
        assertTrue(it.hasNext());
        assertEquals(Map.entry("key1", "value1"), it.next());
        assertTrue(it.hasNext());
        assertEquals(Map.entry("key2", "value2"), it.next());
        assertTrue(it.hasNext());
        assertEquals(Map.entry("key3", "value3"), it.next());
        assertFalse(it.hasNext());
    }
}