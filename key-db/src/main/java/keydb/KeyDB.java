package keydb;

import io.vavr.control.Option;
import io.vavr.control.Try;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
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
    private MemTable memTable;
    @NonNull
    private final Deque<Segment> segments;

    public void set(final String key, final String value) {
        memTable.set(key, value);

        if (memTable.getSize() >= Config.MEMTABLE_FLUSH_SIZE_BYTES) {
            segments.push(memTable.writeSegment(segmentDir(rootPath), getNextSegmentId()).get());
            memTable = emptyMemTable(rootPath);
        }
    }

    public Option<String> get(final String key) {
        return memTable.get(key).orElse(() -> segments.stream()
                .map(segment -> segment.get(key).get())
                .filter(value -> !value.isEmpty())
                .findFirst()
                .orElse(Option.none()));
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
        return Try.of(() -> {
            if (Files.isDirectory(path)) {
                return loadDB(path);
            } else {
                return createDB(path);
            }
        });
    }

    private Integer getNextSegmentId() {
        return segments.isEmpty() ? 0 : segments.peek().getId() + 1;
    }

    private static KeyDB createDB(final Path rootPath) throws IOException {
        Files.createDirectory(rootPath);
        Files.createDirectory(segmentDir(rootPath));
        return new KeyDB(rootPath, emptyMemTable(rootPath), new ArrayDeque<>());
    }

    private static KeyDB loadDB(final Path rootPath) throws IOException {
        validateDB(rootPath);

        final Deque<Segment> segments = Files.list(segmentDir(rootPath))
                .map(Segment::from)
                .map(Try::get)
                .sorted(Comparator.comparing(Segment::getId).reversed())
                .collect(Collectors.toCollection(ArrayDeque::new));

        return new KeyDB(rootPath, loadOrCreateMemTable(rootPath), segments);
    }

    private static MemTable loadOrCreateMemTable(final Path rootPath) {
        if (Files.isRegularFile(memTablePath(rootPath))) {
            return MemTable.from(memTablePath(rootPath)).get();
        }
        return emptyMemTable(rootPath);
    }

    private static void validateDB(final Path path) throws IOException {
        if (!Files.isDirectory(segmentDir(path))) {
            throw new FileNotFoundException("No segment directory found");
        }
    }

    private static MemTable emptyMemTable(final Path dir) {
        return new MemTable(memTablePath(dir));
    }

    private static Path memTablePath(final Path path) {
        return path.resolve("memtable");
    }

    private static Path segmentDir(final Path path) {
        return path.resolve("segments");
    }
}
