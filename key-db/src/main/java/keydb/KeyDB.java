package keydb;

import io.vavr.control.Try;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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

@RequiredArgsConstructor
public class KeyDB {

    @NonNull
    private final Path rootPath;
    @NonNull
    private final MemTable memTable;
    @NonNull
    private final List<Segment> segments;

    public void set(final String key, final String value) {
        memTable.set(key, value);
    }

    public Optional<String> get(final String key) {
        return memTable.get(key);
    }

    public int numSegments() {
        return segments.size();
    }

    /**
     * Create or load a KeyDB from a path
     *
     * @param path root path of the database to create/load
     * @return the KeyDB instance
     */
    public static Try<KeyDB> from(final Path path) {
        return Try.of(() -> Files.isDirectory(path))
                .mapTry(exists -> {
                    if (exists) {
                        return loadDB(path);
                    } else {
                        return createDB(path);
                    }
                });
    }

    private static KeyDB createDB(final Path path) throws IOException {
        Files.createDirectory(path);
        Files.createDirectory(segmentDir(path));
        return new KeyDB(path, createMemTable(path), new ArrayList<>());
    }

    private static KeyDB loadDB(final Path rootPath) throws IOException {
        validateDB(rootPath);

        final List<Segment> segments = Files.list(segmentDir(rootPath))
                .map(Segment::from)
                .map(Try::get)
                .sorted(Comparator.comparing(Segment::getId).reversed())
                .collect(Collectors.toList());

        return new KeyDB(rootPath, createMemTable(rootPath), segments);
    }

    private static void validateDB(final Path path) throws IOException {
        if (!Files.isDirectory(segmentDir(path))) {
            throw new FileNotFoundException("No segment directory found");
        }
    }

    private static MemTable createMemTable(final Path path) {
        return new MemTable(path.resolve("memtable"));
    }

    private static Path segmentDir(final Path path) {
        return path.resolve("segments");
    }
}
