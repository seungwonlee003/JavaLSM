package sstable;

import memtable.Memtable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SSTableConstructTest {

    private static Path dataDir;

    @BeforeAll
    static void setup() throws IOException {
        // Ensure default data directory exists for SSTable output
        dataDir = Path.of("data");
        if (!Files.exists(dataDir)) {
            Files.createDirectory(dataDir);
        }
    }

    @AfterAll
    static void teardown() throws IOException {
        // cleanup data directory if empty
        if (Files.exists(dataDir) && Files.list(dataDir).findAny().isEmpty()) {
            Files.delete(dataDir);
        }
    }

    @Test
    void shouldConstructFromMemtableAndVerifyContents() throws IOException {
        // Prepare a Memtable with known entries
        Memtable memtable = new Memtable();
        List<Map.Entry<String, String>> entries = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            String key = String.format("key%02d", i);
            String value = "value" + i;
            memtable.put(key, value);
            entries.add(Map.entry(key, value));
        }

        // Create SSTable from the memtable
        SSTable sstable = SSTable.createSSTableFromMemtable(memtable);

        // Verify BloomFilter contains all keys
        for (var e : entries) {
            assertTrue(sstable.mightContain(e.getKey()),
                    "Expected bloom filter to contain key " + e.getKey());
        }

        // Verify that get(key) returns the correct value
        for (var e : entries) {
            assertEquals(e.getValue(), sstable.get(e.getKey()),
                    "Expected value for key " + e.getKey());
        }

        // Verify getAllEntries returns all entries in order
        var actualEntries = sstable.getAllEntries();
        assertEquals(entries.size(), actualEntries.size(),
                "Entry count should match");
        for (int i = 0; i < entries.size(); i++) {
            assertEquals(entries.get(i).getKey(), actualEntries.get(i).getKey(),
                    "Key at index " + i + " should match");
            assertEquals(entries.get(i).getValue(), actualEntries.get(i).getValue(),
                    "Value at index " + i + " should match");
        }

        assertNull(sstable.get("key00"), "Below minKey should return null");
        assertNull(sstable.get("zzzz"), "Above maxKey should return null");

        sstable.delete();
    }
}
