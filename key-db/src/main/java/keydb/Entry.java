package keydb;

import lombok.Value;

import java.util.Map;

@Value
public class Entry {
    String key;
    String value;

    public static Entry of(final Map.Entry<String, String> entry) {
        return new Entry(entry.getKey(), entry.getValue());
    }
}
