package util;

import memtable.Memtable;

import java.io.*;
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
        IOUtils.writeString(outputStream, key);
        IOUtils.writeString(outputStream, value);
        outputStream.flush();
    }

    public static void replay(Memtable memtable, String filePath) throws IOException {
        try (DataInputStream inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(filePath)))) {
            while (inputStream.available() > 0) {
                String key = IOUtils.readString(inputStream);
                String value = IOUtils.readString(inputStream);
                memtable.put(key, value);
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