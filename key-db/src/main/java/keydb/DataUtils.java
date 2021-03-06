package keydb;

import io.vavr.Tuple;
import io.vavr.Tuple2;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public final class DataUtils {

    static Entry readEntry(final DataInputStream dataInput) throws IOException {
        final int keyLength = dataInput.readInt();
        final int valueLength = dataInput.readInt();
        final byte[] keyBuffer = new byte[keyLength];
        final byte[] valueBuffer = new byte[valueLength];
        final int readKey = dataInput.read(keyBuffer, 0, keyLength);
        final int readValue = dataInput.read(valueBuffer, 0, valueLength);
        if (readKey != keyLength || readValue != valueLength) {
            throw new RuntimeException("Unexpected end of file");
        }
        final String key = new String(keyBuffer, Config.CHARSET);
        final String value = new String(valueBuffer, Config.CHARSET);
        return new Entry(key, value);
    }

    static long writeEntry(final DataOutputStream dataOutput, final Entry entry) throws IOException {
        final byte[] keyBytes = entry.getKey().getBytes(Config.CHARSET);
        final byte[] valueBytes = entry.getValue().getBytes(Config.CHARSET);
        dataOutput.writeInt(keyBytes.length);
        dataOutput.writeInt(valueBytes.length);
        dataOutput.write(keyBytes);
        dataOutput.write(valueBytes);
        return 8 + keyBytes.length + valueBytes.length;
    }

    static Tuple2<String, Long> readIndex(final DataInputStream dataInput) throws IOException {
        final int keyLength = dataInput.readInt();
        final byte[] keyBuffer = new byte[keyLength];
        final int readKey = dataInput.read(keyBuffer, 0, keyLength);
        if (readKey != keyLength) {
            throw new RuntimeException("Unexpected end of file");
        }
        final long byteOffset = dataInput.readLong();
        final String key = new String(keyBuffer, Config.CHARSET);
        return Tuple.of(key, byteOffset);
    }

    public static void writeIndex(final Tuple2<String, Long> index, final DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeInt(index._1.getBytes(Config.CHARSET).length);
        dataOutputStream.write(index._1.getBytes(Config.CHARSET));
        dataOutputStream.writeLong(index._2);
    }
}
