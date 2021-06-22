package keydb;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class Config {

    // The size at which the MemTable will be persisted to disk
    // once it exceeds it
    public static final long MEMTABLE_FLUSH_SIZE_BYTES = 2000000;

    // Approximate number of bytes between the indices that are created
    // when a MemTable is persisted to form a new segment
    public static final long BYTES_PER_INDEX = 4096;

    // Charset used when serializing the String key/value pairs
    public static final Charset CHARSET = StandardCharsets.UTF_8;
}
