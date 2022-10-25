package keydb;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Represents a single entry (key-value pair) that can be stored
 * and retrieved from the database.
 */
public record Entry(String key, String value) {
    private static final Charset CHARSET = StandardCharsets.UTF_8;

    public static Entry of(final Map.Entry<String, String> entry) {
        return new Entry(entry.getKey(), entry.getValue());
    }

    public static Entry read(final DataInputStream dataInput) throws IOException {
        final int keyLength = dataInput.readInt();
        final int valueLength = dataInput.readInt();
        final byte[] keyBuffer = new byte[keyLength];
        final byte[] valueBuffer = new byte[valueLength];
        final int readKey = dataInput.read(keyBuffer, 0, keyLength);
        final int readValue = dataInput.read(valueBuffer, 0, valueLength);
        if (readKey != keyLength || readValue != valueLength) {
            throw new RuntimeException("Unexpected end of file");
        }
        final String key = new String(keyBuffer, CHARSET);
        final String value = new String(valueBuffer, CHARSET);
        return new Entry(key, value);
    }

    public long write(final DataOutputStream dataOutput) throws IOException {
        final byte[] keyBytes = key.getBytes(CHARSET);
        final byte[] valueBytes = value.getBytes(CHARSET);
        dataOutput.writeInt(keyBytes.length);
        dataOutput.writeInt(valueBytes.length);
        dataOutput.write(keyBytes);
        dataOutput.write(valueBytes);
        return 8 + keyBytes.length + valueBytes.length;
    }
}
