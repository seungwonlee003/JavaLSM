package main.util;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

public class IOUtils {
    public static String readString(RandomAccessFile f) throws IOException {
        int len = f.readInt();
        byte[] buf = new byte[len];
        f.readFully(buf);
        return new String(buf, StandardCharsets.UTF_8);
    }

    public static byte[] readBytes(RandomAccessFile f) throws IOException {
        int len = f.readInt();
        byte[] buf = new byte[len];
        f.readFully(buf);
        return buf;
    }

    public static byte[] serializeValue(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static String deserializeValue(byte[] valueBytes) {
        return valueBytes.length > 0 ? new String(valueBytes, StandardCharsets.UTF_8) : null;
    }
}