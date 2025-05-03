package util;

import java.io.DataInput;
import java.io.DataOutput;
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

    public static String readString(DataInput in) throws IOException {
        int len = in.readInt();
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static void writeString(DataOutput out, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static byte[] serializeValue(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static String deserializeValue(byte[] valueBytes) {
        return new String(valueBytes, StandardCharsets.UTF_8);
    }
}