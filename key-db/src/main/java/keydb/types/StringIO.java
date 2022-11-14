package keydb.types;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class StringIO implements ValueIO<String> {

    private static final Charset CHARSET = StandardCharsets.UTF_8;


    @Override
    public long write(final String value, final DataOutputStream output) throws IOException {
        final byte[] valueBytes = value.getBytes(CHARSET);
        output.writeInt(valueBytes.length);
        output.write(valueBytes);
        return 4 + valueBytes.length;
    }

    @Override
    public String read(final DataInputStream input) throws IOException {
        final int valueLength = input.readInt();
        final byte[] valueBuffer = new byte[valueLength];
        final int readValue = input.read(valueBuffer, 0, valueLength);
        if (readValue != valueLength) {
            throw new RuntimeException("Unexpected end of file");
        }
        return new String(valueBuffer, CHARSET);
    }

    @Override
    public boolean canHandle(final Class<?> clazz) {
        return clazz.equals(String.class);
    }
}
