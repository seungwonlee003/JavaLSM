package db;

import sstable.SSTable;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manifest {
    private final String filePath;
    private final String current;
    private final Map<Integer, List<SSTable>> levelMap = new HashMap<>();
    public final List<String> walPaths = new ArrayList<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public Manifest() throws IOException {
        this.filePath = "./data";
        this.current = filePath + "/CURRENT";

        try {
            Files.createDirectories(Paths.get(filePath));
        } catch (IOException e) {
            throw new IOException("Failed to create data directory: " + filePath, e);
        }

        Path currentPath = Paths.get(current);
        if (Files.exists(currentPath)) {
            String manifestFile = Files.readString(currentPath).trim();
            loadManifest(manifestFile);
        } else {
            String manifestFile = generateManifestFileName(1);
            persistToFile(manifestFile);
            Files.writeString(currentPath, manifestFile);
        }
    }

    private void loadManifest(String manifestFile) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath + "/" + manifestFile))) {
            Map<Integer, List<String>> serializedMap = (Map<Integer, List<String>>) ois.readObject();
            for (Map.Entry<Integer, List<String>> entry : serializedMap.entrySet()) {
                int level = entry.getKey();
                List<SSTable> sstables = new ArrayList<>();
                for (String sstablePath : entry.getValue()) {
                    try {
                        sstables.add(new SSTable(sstablePath));
                    } catch (IOException e) {
                        System.err.println("Failed to load SSTable: " + sstablePath + " - " + e.getMessage());
                    }
                }
                levelMap.put(level, sstables);
            }
            List<String> loadedWalPaths = (List<String>) ois.readObject();
            walPaths.addAll(loadedWalPaths);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize manifest: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new IOException("Failed to read manifest file: " + manifestFile, e);
        }
    }

    public void persist() throws IOException {
        rwLock.writeLock().lock();
        try {
            String newManifestFile = generateNextManifestFileName();
            persistToFile(newManifestFile);
            Files.writeString(Paths.get(current), newManifestFile);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void persistToFile(String manifestFile) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(
                new FileOutputStream(filePath + "/" + manifestFile))) {
            Map<Integer, List<String>> serializedMap = new HashMap<>();
            for (Map.Entry<Integer, List<SSTable>> entry : levelMap.entrySet()) {
                List<String> sstablePaths = new ArrayList<>();
                for (SSTable sstable : entry.getValue()) {
                    sstablePaths.add(sstable.getFilePath());
                }
                serializedMap.put(entry.getKey(), sstablePaths);
            }
            oos.writeObject(serializedMap);
            oos.writeObject(new ArrayList<>(walPaths));
        }
    }

    private String generateManifestFileName(int number) {
        return String.format("MANIFEST-%06d", number);
    }

    private String generateNextManifestFileName() throws IOException {
        int maxNumber = 1;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(filePath), "MANIFEST-*")) {
            for (Path path : stream) {
                String fileName = path.getFileName().toString();
                int number = Integer.parseInt(fileName.substring("MANIFEST-".length()));
                maxNumber = Math.max(maxNumber, number);
            }
        } catch (NumberFormatException e) {
            throw new IOException("Invalid manifest file name format", e);
        }
        return generateManifestFileName(maxNumber + 1);
    }

    public ReadWriteLock getLock() {
        return rwLock;
    }

    public void addWAL(String walPath) throws IOException {
        rwLock.writeLock().lock();
        try {
            // add to the last index
            walPaths.add(walPath);
            persist();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void removeWAL(String walPath) throws IOException {
        rwLock.writeLock().lock();
        try {
            walPaths.remove(walPath);
            persist();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void addSSTable(int level, SSTable sstable) throws IOException {
        rwLock.writeLock().lock();
        try {
            levelMap.computeIfAbsent(level, k -> new ArrayList<>()).add(0, sstable);
            persist();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public List<SSTable> getSSTables(int level) {
        rwLock.readLock().lock();
        try {
            return new ArrayList<>(levelMap.getOrDefault(level, new ArrayList<>()));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public int maxLevel() {
        rwLock.readLock().lock();
        try {
            return levelMap.isEmpty() ? -1 : Collections.max(levelMap.keySet());
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void replace(int levelToClear, List<SSTable> newTables) throws IOException {
        rwLock.writeLock().lock();
        try {
            levelMap.remove(levelToClear);
            levelMap.remove(levelToClear + 1);
            levelMap.computeIfAbsent(levelToClear + 1, k -> new ArrayList<>()).addAll(newTables);
            persist();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void displayManifestFile() {
        rwLock.readLock().lock();
        try {
            System.out.println("===== Manifest Contents =====");
            // Display WAL files
            System.out.println("WAL Files:");
            if (walPaths.isEmpty()) {
                System.out.println("  (None)");
            } else {
                for (int i = 0; i < walPaths.size(); i++) {
                    System.out.println("  [" + i + "] " + walPaths.get(i));
                }
            }

            if (levelMap.isEmpty()) {
                System.out.println("Manifest is empty.");
                return;
            }
            System.out.println("===== SSTables by Level =====");
            for (Map.Entry<Integer, List<SSTable>> entry : levelMap.entrySet()) {
                int level = entry.getKey();
                List<SSTable> sstables = entry.getValue();
                System.out.println("Level " + level + ":");
                for (int i = 0; i < sstables.size(); i++) {
                    SSTable sstable = sstables.get(i);
                    System.out.println("  [" + i + "] " + sstable.getFilePath());
                    System.out.println("    Contents:");
                    try {
                        List<Map.Entry<String, String>> entries = sstable.getAllEntries();
                        if (entries.isEmpty()) {
                            System.out.println("      (Empty)");
                        } else {
                            for (Map.Entry<String, String> kv : entries) {
                                System.out.println("      Key: " + kv.getKey() + ", Value: " + kv.getValue());
                            }
                        }
                    } catch (IOException e) {
                        System.out.println("      (Error reading contents: " + e.getMessage() + ")");
                    }
                }
            }
            System.out.println("=============================");
        } finally {
            rwLock.readLock().unlock();
        }
    }
}