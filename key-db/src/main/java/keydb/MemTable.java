package keydb;

import io.vavr.control.Option;
import io.vavr.control.Try;
import keydb.file.InputFileManager;
import keydb.file.OutputFileManager;
import lombok.EqualsAndHashCode;
import lombok.Getter;

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
public class MemTable implements AutoCloseable {

    private final SortedMap<String, String> data;
    private final Path logPath;
    private final OutputFileManager fileManager;
    @Getter
    private long size;

    MemTable(final Path logPath) {
        this.data = new TreeMap<>();
        this.logPath = logPath;
        this.fileManager = new OutputFileManager(logPath, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        this.size = 0;
    }

    public void put(final String key, final String value) {
        put(new Entry(key, value));
    }

    public void put(final Entry entry) {
        setNoWrite(entry);
        writeToLog(entry);
    }

    public Option<String> get(final String key) {
        return Option.of(data.get(key));
    }

    private void setNoWrite(final Entry entry) {
        data.put(entry.getKey(), entry.getValue());
        incrementSize(entry.getKey().length() * 2 + entry.getValue().length() * 2 + 8);
    }

    private void writeToLog(final Entry entry) {
        fileManager.runWithOutput(dataOutputStream -> {
            DataUtils.writeEntry(dataOutputStream, entry);
            dataOutputStream.flush();
        });
    }

    public static Try<MemTable> from(final Path path) {
        return Try.of(() -> {
            if (!Files.isRegularFile(path)) {
                throw new NoSuchFileException(path.toString());
            }
            final MemTable memTable = new MemTable(path);

            try (final InputFileManager inputFileManager = new InputFileManager(path)) {
                inputFileManager.acceptInputUntilEndOfFile(
                        dataInputStream -> memTable.setNoWrite(DataUtils.readEntry(dataInputStream)));
            }
            return memTable;
        });
    }

    public Try<Segment> writeSegment(final Path path, final Integer id) {
        return Try.of(() -> {
            close();
            Files.delete(logPath);
            final Path segmentDir = path.resolve(String.valueOf(id));
            Files.createDirectory(segmentDir);

            return FileUtils.applyWithOutput(Segment.getDataPath(segmentDir), dataOutputStream -> {
                final SparseIndex index = writeDataAndCreateIndex(dataOutputStream);
                index.write(Segment.getIndexPath(segmentDir));
                return new Segment(index, segmentDir, id);
            });
        });
    }

    private SparseIndex writeDataAndCreateIndex(final DataOutputStream dataOutput) throws IOException {
        long bytesWrittenTotal = 0;
        final SparseIndex index = new SparseIndex();
        long bytesWrittenSinceLastIndex = Config.BYTES_PER_INDEX;

        for (final Map.Entry<String, String> entry : data.entrySet()) {
            if (bytesWrittenSinceLastIndex >= Config.BYTES_PER_INDEX) {
                index.insert(entry.getKey(), bytesWrittenTotal);
                bytesWrittenSinceLastIndex = 0;
            }
            final long bytesWritten = DataUtils.writeEntry(dataOutput, Entry.of(entry));
            bytesWrittenTotal += bytesWritten;
            bytesWrittenSinceLastIndex += bytesWritten;
        }
        return index;
    }

    private void incrementSize(final int s) {
        size += s;
    }

    @Override
    public void close() throws Exception {
        fileManager.close();
    }
}
