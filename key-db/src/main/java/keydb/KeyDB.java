package keydb;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vavr.control.Option;
import io.vavr.control.Try;
import keydb.config.DBConfig;
import keydb.types.ValueIO;
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
public class KeyDB<T extends Comparable<T>> {

    private final Path rootPath;
    private MemTable<T> memTable;
    private final Deque<Segment<T>> segments;
    private final ValueIO<T> valueIO;
    @Getter
    private final DBConfig config;


    public static <P extends Comparable<P>> KeyDB<P> create(final Path rootPath, final Class<P> type) {
        return create(rootPath, DBConfig.builder().build(), type);
    }

    public static <P extends Comparable<P>> KeyDB<P> create(final Path rootPath, final DBConfig config, final Class<P> type) {
        return createDB(rootPath, config, type);
    }

    public static <P extends Comparable<P>> KeyDB<P> load(final Path rootPath, final Class<P> type) throws IOException {
        validateDB(rootPath);

        final DBConfig config = objectMapper().readValue(
                Files.readString(configPath(rootPath), StandardCharsets.UTF_8), DBConfig.class);

        final ValueIO<P> io = ValueIO.getProvider(type);

        try (final Stream<Path> files = Files.list(segmentDir(rootPath))) {
            final Deque<Segment<P>> segments = files
                    .map(path -> Segment.from(path, io))
                    .map(Try::get)
                    .sorted()
                    .collect(Collectors.toCollection(ArrayDeque::new));
            return new KeyDB<>(rootPath, loadOrCreateMemTable(rootPath, io), segments, io, config);
        }
    }

    public void put(final T key, final T value) {
        memTable.put(key, value);

        if (memTable.getSize() >= config.getMemTableFlushSizeBytes()) {
            segments.push(memTable.writeSegment(segmentDir(rootPath), getNextSegmentId(), config).get());
            memTable = new MemTable<>(memTablePath(rootPath), valueIO);
        }
    }

    public Option<T> get(final T key) {
        return memTable.get(key).orElse(() -> segments.stream()
                .map(segment -> segment.get(key, valueIO).get())
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
    private static <P extends Comparable<P>> KeyDB<P> createDB(final Path rootPath, final DBConfig config, final Class<P> type) {
        final ValueIO<P> io = ValueIO.getProvider(type);
        Files.createDirectory(rootPath);
        Files.createDirectory(segmentDir(rootPath));
        Files.writeString(configPath(rootPath),
                objectMapper().writeValueAsString(config),
                StandardOpenOption.CREATE_NEW);

        return new KeyDB<>(rootPath, new MemTable<>(memTablePath(rootPath), io), new ArrayDeque<>(), io, config);
    }

    private static <P extends Comparable<P>> MemTable<P> loadOrCreateMemTable(final Path rootPath, final ValueIO<P> io) {
        if (Files.isRegularFile(memTablePath(rootPath))) {
            return MemTable.from(memTablePath(rootPath), io).get();
        }
        return new MemTable<>(memTablePath(rootPath), io);
    }

    private static void validateDB(final Path path) throws FileNotFoundException {
        if (!Files.isDirectory(segmentDir(path))) {
            throw new FileNotFoundException("No segment directory found");
        }
    }

    private static ObjectMapper objectMapper() {
        return new ObjectMapper();
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
