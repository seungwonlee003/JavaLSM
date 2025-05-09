package sstable;

import memtable.Memtable;
import util.BloomFilterUtil;
import util.IOUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class SSTable {
    private final String filePath;
    public final BloomFilterUtil bloomFilterUtil;
    private final NavigableMap<String, BlockInfo> index;
    private String minKey;
    private String maxKey;
    private static final int BLOCK_SIZE = 4000;
    private static final int SSTABLE_SIZE_THRESHOLD = 4 * 1024 * 1024;

    public SSTable(String filePath) throws IOException {
        this.filePath = filePath;
        this.index = new TreeMap<>();
        this.bloomFilterUtil = new BloomFilterUtil(36000, 0.03);
        this.minKey = null;
        this.maxKey = null;
        init();
    }

    public SSTable(String filePath, BloomFilterUtil bloomFilterUtil, NavigableMap<String, BlockInfo> index, String minKey, String maxKey) {
        this.filePath = filePath;
        this.bloomFilterUtil = bloomFilterUtil;
        this.index = index;
        this.minKey = minKey;
        this.maxKey = maxKey;
    }

    public void init() throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            long blockStart = 0L;
            long blockLength = 0L;
            String blockFirstKey = null;

            while (file.getFilePointer() < file.length()) {
                long recordStart = file.getFilePointer();
                String key = IOUtils.readString(file); // Reads key length and key bytes
                int valueLength = file.readInt();     // Read value length (4 bytes)
                file.skipBytes(valueLength);          // Skip value bytes
                long recLen = file.getFilePointer() - recordStart;

                if (minKey == null) {
                    minKey = key;
                }
                maxKey = key;

                if (blockLength + recLen > BLOCK_SIZE && blockFirstKey != null) {
                    index.put(blockFirstKey, new BlockInfo(blockStart, blockLength));
                    blockStart = recordStart;
                    blockLength = 0L;
                    blockFirstKey = null;
                }

                if (blockFirstKey == null) {
                    blockFirstKey = key;
                }

                bloomFilterUtil.add(key);
                blockLength += recLen;
            }

            if (blockFirstKey != null) {
                index.put(blockFirstKey, new BlockInfo(blockStart, blockLength));
            }
        }
    }

    public static SSTable createSSTableFromMemtable(Memtable memtable) throws IOException {
        String filePath = "./data/sstable_" + System.nanoTime() + ".sst";
        BloomFilterUtil bloomFilterUtil = new BloomFilterUtil(36000, 0.03);
        TreeMap<String, BlockInfo> index = new TreeMap<>();
        String minKey = null;
        String maxKey = null;

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            Iterator<Map.Entry<String, String>> entries = memtable.iterator();
            long blockStart = 0L;
            long blockLength = 0L;
            String blockFirstKey = null;

            while (entries.hasNext()) {
                Map.Entry<String, String> entry = entries.next();
                String key = entry.getKey();
                String value = entry.getValue();
                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = IOUtils.serializeValue(value);
                long recLen = 4 + keyBytes.length + 4 + valueBytes.length;

                if (blockLength + recLen > BLOCK_SIZE && blockFirstKey != null) {
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

    public static List<SSTable> sortedRun(String dataDir, List<SSTable> tables) throws IOException {
        SSTableIterator[] iterators = new SSTableIterator[tables.size()];
        for (int i = 0; i < tables.size(); i++) {
            iterators[i] = new SSTableIterator(tables.get(i));
        }

        PriorityQueue<SSTableEntry> queue = new PriorityQueue<>(new Comparator<SSTableEntry>() {
            @Override
            public int compare(SSTableEntry e1, SSTableEntry e2) {
                int keyCompare = e1.key.compareTo(e2.key);
                if (keyCompare != 0) {
                    return keyCompare;
                }
                return Integer.compare(e1.sstableNumber, e2.sstableNumber);
            }
        });

        for (int i = 0; i < iterators.length; i++) {
            if (iterators[i].hasNext()) {
                Map.Entry<String, String> nextEntry = iterators[i].next();
                queue.offer(new SSTableEntry(nextEntry, i));
            }
        }

        List<SSTable> newSSTables = new ArrayList<>();
        List<Map.Entry<String, String>> buffer = new ArrayList<>();
        long currentSize = 0;
        String lastKey = null;

        while (!queue.isEmpty()) {
            SSTableEntry entry = queue.poll();
            String key = entry.key;
            String value = entry.value;

            if (lastKey == null || !lastKey.equals(key)) {
                lastKey = key;
                if (!"<TOMBSTONE>".equals(value)) {
                    buffer.add(new AbstractMap.SimpleEntry<>(key, value));
                    currentSize += 4 + key.getBytes(StandardCharsets.UTF_8).length +
                            4 + value.getBytes(StandardCharsets.UTF_8).length;
                }
                if (currentSize > SSTABLE_SIZE_THRESHOLD) {
                    newSSTables.add(createSSTableFromBuffer(dataDir, buffer));
                    buffer.clear();
                    currentSize = 0;
                }
            }

            int idx = entry.sstableNumber;
            if (iterators[idx].hasNext()) {
                Map.Entry<String, String> nextEntry = iterators[idx].next();
                queue.offer(new SSTableEntry(nextEntry, idx));
            }
        }

        if (!buffer.isEmpty()) {
            newSSTables.add(createSSTableFromBuffer(dataDir, buffer));
        }

        for (SSTableIterator iterator : iterators) {
            iterator.close();
        }

        return newSSTables;
    }

    private static SSTable createSSTableFromBuffer(String dataDir, List<Map.Entry<String, String>> buffer) throws IOException {
        String filePath = dataDir + "/sstable_" + System.nanoTime() + ".sst";
        BloomFilterUtil bloomFilterUtil = new BloomFilterUtil(36000, 0.03);
        TreeMap<String, BlockInfo> index = new TreeMap<>();
        String minKey = null;
        String maxKey = null;

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            long blockStart = 0L;
            long blockLength = 0L;
            String blockFirstKey = null;

            for (Map.Entry<String, String> entry : buffer) {
                String key = entry.getKey();
                String value = entry.getValue();
                byte[] keyBytes = IOUtils.serializeValue(key);
                byte[] valueBytes = IOUtils.serializeValue(value);
                long recLen = 4 + keyBytes.length + 4 + valueBytes.length;

                if (blockLength + recLen > BLOCK_SIZE && blockFirstKey != null) {
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

    public boolean mightContain(String key) {
        return bloomFilterUtil.mightContain(key);
    }

    public String get(String key) {
        if (key.compareTo(minKey) < 0 || key.compareTo(maxKey) > 0) {
            return null;
        }

        if (!bloomFilterUtil.mightContain(key)) {
            return null;
        }

        Map.Entry<String, BlockInfo> indexEntry = index.floorEntry(key);
        if (indexEntry == null) {
            return null;
        }

        BlockInfo blockInfo = indexEntry.getValue();

        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            file.seek(blockInfo.offset);
            byte[] blockData = new byte[(int) blockInfo.length];
            file.readFully(blockData);

            try (ByteArrayInputStream bais = new ByteArrayInputStream(blockData);
                 DataInputStream dis = new DataInputStream(bais)) {
                while (dis.available() > 0) {
                    int keyLength = dis.readInt();
                    byte[] keyBytes = new byte[keyLength];
                    dis.readFully(keyBytes);
                    String currentKey = new String(keyBytes, StandardCharsets.UTF_8);

                    int valueLength = dis.readInt();
                    byte[] valueBytes = new byte[valueLength];
                    dis.readFully(valueBytes);

                    if (currentKey.equals(key)) {
                        return IOUtils.deserializeValue(valueBytes);
                    }
                    if (currentKey.compareTo(key) > 0) {
                        return null;
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read SSTable: " + filePath, e);
        }
        return null;
    }

    public void delete() {
        File file = new File(filePath);
        if (file.exists() && !file.delete()) {
            throw new RuntimeException("Failed to delete SSTable: " + filePath);
        }
    }

    public String getFilePath() {
        return filePath;
    }

    // For testing
    public List<Map.Entry<String, String>> getAllEntries() throws IOException {
        List<Map.Entry<String, String>> entries = new ArrayList<>();
        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            for (BlockInfo blockInfo : index.values()) {
                file.seek(blockInfo.offset);
                byte[] blockData = new byte[(int) blockInfo.length];
                file.readFully(blockData);

                try (ByteArrayInputStream bais = new ByteArrayInputStream(blockData);
                     DataInputStream dis = new DataInputStream(bais)) {
                    while (dis.available() > 0) {
                        int keyLength = dis.readInt();
                        byte[] keyBytes = new byte[keyLength];
                        dis.readFully(keyBytes);
                        String key = new String(keyBytes, StandardCharsets.UTF_8);

                        int valueLength = dis.readInt();
                        byte[] valueBytes = new byte[valueLength];
                        dis.readFully(valueBytes);
                        String value = IOUtils.deserializeValue(valueBytes);

                        entries.add(new AbstractMap.SimpleEntry<>(key, value));
                    }
                }
            }
        }
        return entries;
    }
}