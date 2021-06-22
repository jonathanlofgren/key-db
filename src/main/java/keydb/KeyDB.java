package keydb;

import java.nio.file.Path;
import java.util.Optional;

// Layout:
//
// db/
//      memtable
//      segments/
//              0/
//                  index
//                  data
//              1/
//                  index
//                  data

public class KeyDB {

    private final MemTable memTable;
    private final Path path;

    private KeyDB(final Path path) {
        this.memTable = createMemTable(path);
        this.path = path;
    }

    public static KeyDB from(final Path path) {
        return new KeyDB(path);
    }

    public void set(final String key, final String value) {
        memTable.set(key, value);
    }

    public Optional<String> get(final String key) {
        return memTable.get(key);
    }

    private MemTable createMemTable(final Path path) {
        return new MemTable(path.resolve("memtable"));
    }
}
