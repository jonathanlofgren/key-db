package keydb;

import keydb.types.ValueIO;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Represents a single entry (key-value pair) that can be stored
 * and retrieved from the database.
 */
@EqualsAndHashCode
@RequiredArgsConstructor
public final class Entry<T extends Comparable<T>> {
    private final T key;
    private final T value;

    public static <P extends Comparable<P>> Entry<P> of(final Map.Entry<P, P> entry) {
        return new Entry<>(entry.getKey(), entry.getValue());
    }

    public static <P extends Comparable<P>> Entry<P> read(final DataInputStream dataInput, final ValueIO<P> valueIO) throws IOException {
        final P key = valueIO.read(dataInput);
        final P value = valueIO.read(dataInput);
        return new Entry<>(key, value);
    }

    public long write(final DataOutputStream dataOutput, final ValueIO<T> valueIO) throws IOException {
        final int sizeBefore = dataOutput.size();
        valueIO.write(key, dataOutput);
        valueIO.write(value, dataOutput);
        return dataOutput.size() - sizeBefore;
    }

    public T key() {
        return key;
    }

    public T value() {
        return value;
    }
}
