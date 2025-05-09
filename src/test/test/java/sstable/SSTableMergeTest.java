package sstable;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import util.BloomFilterUtil;
import util.IOUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

public class SSTableMergeTest {

    @TempDir
    static Path tempDirectory;
    static SSTable first, second, merged;
    static List<Map.Entry<String, String>> firstEntries, secondEntries, expectedEntries;

    @BeforeAll
    public static void setup() throws IOException {
        firstEntries = new ArrayList<>();
        secondEntries = new ArrayList<>();
        expectedEntries = new ArrayList<>();

        // Pad keys to two digits for consistent string comparison
        // First SSTable: keys 01-10, values 1-10
        for (int i = 1; i <= 10; i++) {
            String key = String.format("%02d", i);
            String value = String.valueOf(i);
            firstEntries.add(Map.entry(key, value));
        }

        // Second SSTable: keys 05-20, values 6-21
        for (int i = 5; i <= 20; i++) {
            String key = String.format("%02d", i);
            String value = String.valueOf(i + 1);
            secondEntries.add(Map.entry(key, value));
        }

        // Expected: keys 01-04 from first, keys 05-20 from second (newer values)
        for (int i = 1; i <= 4; i++) {
            String key = String.format("%02d", i);
            String value = String.valueOf(i);
            expectedEntries.add(Map.entry(key, value));
        }
        for (int i = 5; i <= 20; i++) {
            String key = String.format("%02d", i);
            String value = String.valueOf(i + 1);
            expectedEntries.add(Map.entry(key, value));
        }

        // Create SSTables
        first = createSSTable(tempDirectory.toString() + "/sstable1.sst", firstEntries);
        second = createSSTable(tempDirectory.toString() + "/sstable2.sst", secondEntries);

        // Perform merge - newly written sstable is on index 0
        List<SSTable> mergedTables = SSTable.sortedRun(tempDirectory.toString(), List.of(second, first));
        merged = mergedTables.get(0);
    }

    @Test
    public void shouldMergeAndGetItems() throws IOException {
        for (var entry : expectedEntries) {
            String val = merged.get(entry.getKey());
            assertNotNull(val);
            assertEquals(entry.getValue(), val);
        }
    }

    @Test
    public void shouldMergeItemsInOrder() throws IOException {
        List<Map.Entry<String, String>> actualEntries = merged.getAllEntries();

        assertEquals(expectedEntries.size(), actualEntries.size());
        for (int i = 0; i < expectedEntries.size(); i++) {
            assertEquals(expectedEntries.get(i).getKey(), actualEntries.get(i).getKey());
            assertEquals(expectedEntries.get(i).getValue(), actualEntries.get(i).getValue());
        }
    }

    @AfterAll
    public static void teardown() throws IOException {
        merged.delete();
        first.delete();
        second.delete();
    }

    private static SSTable createSSTable(String filePath, List<Map.Entry<String, String>> entries) throws IOException {
        BloomFilterUtil bloomFilterUtil = new BloomFilterUtil(1000, 3);
        TreeMap<String, BlockInfo> index = new TreeMap<>(); // Default string comparison is fine with padded keys
        String minKey = null;
        String maxKey = null;

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            long blockStart = 0L;
            long blockLength = 0L;
            String blockFirstKey = null;

            for (var entry : entries) {
                String key = entry.getKey();
                String value = entry.getValue();
                byte[] keyBytes = key.getBytes();
                byte[] valueBytes = IOUtils.serializeValue(value);
                long recLen = 4 + keyBytes.length + 4 + valueBytes.length;

                if (blockLength + recLen > 50 && blockFirstKey != null) {
                    index.put(blockFirstKey, new BlockInfo(blockStart, blockLength));
                    blockStart = file.getFilePointer();
                    blockLength = 0L;
                    blockFirstKey = null;
                }

                if (blockFirstKey == null) {
                    blockFirstKey = key;
                }

                file.writeInt(keyBytes.length);
                file.write(keyBytes);
                file.writeInt(valueBytes.length);
                file.write(valueBytes);

                bloomFilterUtil.add(key);
                if (minKey == null) {
                    minKey = key;
                }
                maxKey = key;

                blockLength += recLen;
            }

            if (blockFirstKey != null) {
                index.put(blockFirstKey, new BlockInfo(blockStart, blockLength));
            }
        }

        return new SSTable(filePath, bloomFilterUtil, index, minKey, maxKey);
    }
}