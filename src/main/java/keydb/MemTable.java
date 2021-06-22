package keydb;

import io.vavr.control.Try;
import lombok.EqualsAndHashCode;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

@EqualsAndHashCode
public class MemTable {

    private final SortedMap<String, String> data;
    private final Path logPath;
    private int size;

    MemTable(final Path logPath) {
        this.data = new TreeMap<>();
        this.logPath = logPath;
        this.size = 0;
    }

    public void set(final String key, final String value) {
        set(new Entry(key, value));
    }

    public void set(final Entry entry) {
        setNoWrite(entry);
        writeToLog(entry);
    }

    public Optional<String> get(final String key) {
        return Optional.ofNullable(data.get(key));
    }

    private void setNoWrite(final Entry entry) {
        data.put(entry.getKey(), entry.getValue());
        incrementSize(entry.getKey().length() * 2 + entry.getValue().length() * 2 + 8);
    }

    private void writeToLog(final Entry entry) {
        // TODO: should keep file handle open to avoid overhead
        Try.run(() -> {
            FileUtils.runWithOutput(logPath, dataOutputStream -> {
                        DataUtils.writeEntry(dataOutputStream, entry);
                        dataOutputStream.flush();
                    },
                    StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        });
    }

    public static Try<MemTable> from(final Path path) {
        return Try.of(() -> {
            final MemTable memTable = new MemTable(path);

            FileUtils.acceptInputUntilEndOfFile(path, dataInputStream -> {
                memTable.setNoWrite(DataUtils.readEntry(dataInputStream));
            });

            return memTable;
        });
    }

    public Try<Segment> writeSegment(final Path rootPath) {
        return Try.of(() -> {
            Files.createDirectory(rootPath);
            final long id = Long.parseLong(rootPath.getFileName().toString());

            return FileUtils.applyWithOutput(Segment.getDataPath(rootPath), dataOutputStream -> {
                final SparseIndex index = writeDataAndCreateIndex(dataOutputStream);
                index.write(Segment.getIndexPath(rootPath));
                return new Segment(index, rootPath, id);
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
            final long bytesWritten = DataUtils.writeEntry(dataOutput, new Entry(entry.getKey(), entry.getValue()));
            bytesWrittenTotal += bytesWritten;
            bytesWrittenSinceLastIndex += bytesWritten;
        }
        return index;
    }

    public long getSize() {
        return size;
    }

    private void incrementSize(final int s) {
        size += s;
    }
}
