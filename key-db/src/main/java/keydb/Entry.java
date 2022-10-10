package keydb;

import lombok.Value;

import java.util.Map;

/**
 * Represents a single entry (key-value pair) that can be stored
 * and retrieved from the database.
 */
@Value
public class Entry {
    String key;
    String value;

    public static Entry of(final Map.Entry<String, String> entry) {
        return new Entry(entry.getKey(), entry.getValue());
    }
}
