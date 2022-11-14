package keydb;

import io.vavr.control.Option;
import io.vavr.control.Try;
import keydb.config.DBConfig;
import keydb.file.InputFileManager;
import keydb.file.OutputFileManager;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

@EqualsAndHashCode(exclude = {"fileManager"})
public class MemTable<T extends Comparable<T>> implements AutoCloseable {

    private final SortedMap<T, T> data;
    private final Path logPath;
    private final OutputFileManager fileManager;
    @Getter
    private long size;

    @SneakyThrows
    MemTable(final Path logPath) {
        this.data = new TreeMap<>();
        this.logPath = logPath;
        this.fileManager = new OutputFileManager(logPath, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        this.size = 0;
    }

    public void put(final T key, final T value) {
        put(new Entry<>(key, value));
    }

    public void put(final Entry<T> entry) {
        setNoWrite(entry);
        writeToLog(entry);
    }

    public Option<T> get(final T key) {
        return Option.of(data.get(key));
    }

    private void setNoWrite(final Entry<T> entry) {
        data.put(entry.key(), entry.value());
    }

    private void writeToLog(final Entry<T> entry) {
        fileManager.runWithOutput(dataOutputStream -> {
            incrementSize(entry.write(dataOutputStream));
            dataOutputStream.flush();
        });
    }

    public static <P extends Comparable<P>> Try<MemTable<P>> from(final Path path) {
        return Try.of(() -> {
            if (!Files.isRegularFile(path)) {
                throw new NoSuchFileException(path.toString());
            }
            final MemTable<P> memTable = new MemTable<>(path);

            try (final InputFileManager inputFileManager = new InputFileManager(path)) {
                inputFileManager.acceptInputUntilEndOfFile(
                        dataInputStream -> memTable.setNoWrite(Entry.read(dataInputStream)));
            }
            return memTable;
        });
    }

    public Try<Segment<T>> writeSegment(final Path path, final Integer id, final DBConfig config) {
        return Try.of(() -> {
            close();
            Files.delete(logPath);
            final Path segmentDir = path.resolve(id.toString());
            Files.createDirectory(segmentDir);

            return FileUtils.applyWithOutput(Segment.getDataPath(segmentDir), dataOutputStream -> {
                final SparseIndex<T> index = writeDataAndCreateIndex(dataOutputStream, config);
                index.write(Segment.getIndexPath(segmentDir));
                return new Segment<>(index, segmentDir, id);
            });
        });
    }

    private SparseIndex<T> writeDataAndCreateIndex(final DataOutputStream dataOutput, final DBConfig config) throws IOException {
        long bytesWrittenTotal = 0;
        final SparseIndex<T> index = new SparseIndex<>();
        long bytesWrittenSinceLastIndex = config.getBytesPerIndex();

        for (final Map.Entry<T, T> entry : data.entrySet()) {
            if (bytesWrittenSinceLastIndex >= config.getBytesPerIndex()) {
                index.insert(entry.getKey(), bytesWrittenTotal);
                bytesWrittenSinceLastIndex = 0;
            }
            final long bytesWritten = Entry.of(entry).write(dataOutput);
            bytesWrittenTotal += bytesWritten;
            bytesWrittenSinceLastIndex += bytesWritten;
        }
        return index;
    }

    private void incrementSize(final long incr) {
        size += incr;
    }

    @Override
    public void close() throws Exception {
        fileManager.close();
    }
}
