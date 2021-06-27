package keydb;

import io.vavr.control.Option;
import io.vavr.control.Try;
import lombok.EqualsAndHashCode;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
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

    public Option<String> get(final String key) {
        return Option.of(data.get(key));
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

    public Try<Segment> writeSegment(final Path path, final Integer id) {
        return Try.of(() -> {
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
