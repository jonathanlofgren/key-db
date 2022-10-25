package keydb;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vavr.control.Option;
import io.vavr.control.Try;
import keydb.config.DBConfig;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// Layout:
//
// db/
//      memtable
//      config.json
//      segments/
//              0/
//                  index
//                  data
//              1/
//                  index
//                  data

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class KeyDB {

    private final Path rootPath;
    private MemTable memTable;
    private final Deque<Segment> segments;
    @Getter
    private final DBConfig config;


    public static KeyDB create(final Path rootPath) {
        return create(rootPath, DBConfig.builder().build());
    }

    public static KeyDB create(final Path rootPath, final DBConfig config) {
        return createDB(rootPath, config);
    }

    public static KeyDB load(final Path rootPath) throws IOException {
        validateDB(rootPath);

        final DBConfig config = objectMapper().readValue(
                Files.readString(configPath(rootPath), StandardCharsets.UTF_8), DBConfig.class);

        try (final Stream<Path> files = Files.list(segmentDir(rootPath))) {
            final Deque<Segment> segments = files
                    .map(Segment::from)
                    .map(Try::get)
                    .sorted(Comparator.comparing(Segment::getId).reversed())
                    .collect(Collectors.toCollection(ArrayDeque::new));
            return new KeyDB(rootPath, loadOrCreateMemTable(rootPath), segments, config);
        }
    }

    public void put(final String key, final String value) {
        memTable.put(key, value);

        if (memTable.getSize() >= config.getMemTableFlushSizeBytes()) {
            segments.push(memTable.writeSegment(segmentDir(rootPath), getNextSegmentId(), config).get());
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

    private Integer getNextSegmentId() {
        return segments.isEmpty() ? 0 : segments.peek().getId() + 1;
    }

    @SneakyThrows
    private static KeyDB createDB(final Path rootPath, final DBConfig config) {
        Files.createDirectory(rootPath);
        Files.createDirectory(segmentDir(rootPath));
        Files.writeString(configPath(rootPath),
                objectMapper().writeValueAsString(config),
                StandardOpenOption.CREATE_NEW);

        return new KeyDB(rootPath, emptyMemTable(rootPath), new ArrayDeque<>(), config);
    }

    private static MemTable loadOrCreateMemTable(final Path rootPath) {
        if (Files.isRegularFile(memTablePath(rootPath))) {
            return MemTable.from(memTablePath(rootPath)).get();
        }
        return emptyMemTable(rootPath);
    }

    private static void validateDB(final Path path) throws FileNotFoundException {
        if (!Files.isDirectory(segmentDir(path))) {
            throw new FileNotFoundException("No segment directory found");
        }
    }

    private static ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    private static MemTable emptyMemTable(final Path dir) {
        return new MemTable(memTablePath(dir));
    }

    private static Path configPath(final Path rootPath) {
        return rootPath.resolve("config.json");
    }

    private static Path memTablePath(final Path path) {
        return path.resolve("memtable");
    }

    private static Path segmentDir(final Path path) {
        return path.resolve("segments");
    }
}
