package wal;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class WAL {
    private final String fileName;
    private static final int BUFFER_SIZE = 4096; // 4KB

    public WAL(String fileName) throws IOException {
        this.fileName = fileName;
        Files.createDirectories(Paths.get(fileName).getParent());
    }

    public void append(String key, String value) throws IOException {
        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(
                        new FileOutputStream(fileName, true), BUFFER_SIZE))) {
            dos.writeUTF(key);
            dos.writeUTF(value);
        }
    }

    public List<WalEntry> readAll() throws IOException {
        List<WalEntry> entries = new ArrayList<>();
        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(
                        new FileInputStream(fileName)))) {
            while (true) {
                try {
                    String key = dis.readUTF();
                    String value = dis.readUTF();
                    entries.add(new WalEntry(key, value));
                } catch (EOFException e) {
                    break;
                }
            }
        } catch (FileNotFoundException e) {
            // File doesn't exist yet
        }
        return entries;
    }

    public String getFileName() {
        return fileName;
    }
}