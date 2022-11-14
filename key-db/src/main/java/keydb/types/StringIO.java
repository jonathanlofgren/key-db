package keydb.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringIO implements ValueIO<String> {

    @Override
    public void write(final String value, final DataOutput output) throws IOException {
        output.writeUTF(value);
    }

    @Override
    public String read(final DataInput input) throws IOException {
        return input.readUTF();
    }

    @Override
    public boolean canHandle(final Class<?> clazz) {
        return clazz.equals(String.class);
    }
}
