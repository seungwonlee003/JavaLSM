package util;

import memtable.Memtable;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class WAL {
    private final String filePath;
    private final DataOutputStream outputStream;

    public WAL(String filePath) throws IOException {
        this.filePath = filePath;
        this.outputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filePath, true)));
    }

    public void writeEntry(String key, String value) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        outputStream.writeInt(keyBytes.length);
        outputStream.write(keyBytes);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        outputStream.writeInt(valueBytes.length);
        outputStream.write(valueBytes);
        outputStream.flush();
    }

    public static void replay(Memtable memtable, String filePath) throws IOException {
        try (DataInputStream inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(filePath)))) {
            while (inputStream.available() > 0) {
                int keyLength = inputStream.readInt();
                byte[] keyBytes = new byte[keyLength];
                inputStream.readFully(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                int valueLength = inputStream.readInt();
                byte[] valueBytes = new byte[valueLength];
                inputStream.readFully(valueBytes);
                String value = new String(valueBytes, StandardCharsets.UTF_8);

                memtable.put(key, value);  // Apply key-value pair to Memtable
            }
        }
    }

    public void close() throws IOException {
        outputStream.close();
    }

    public void delete() throws IOException {
        close();
        Files.deleteIfExists(Paths.get(filePath));
    }

    public String getFilePath() {
        return filePath;
    }
}