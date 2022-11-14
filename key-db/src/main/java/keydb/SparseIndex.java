package keydb;

import io.vavr.control.Try;
import keydb.file.InputFileManager;
import keydb.file.OutputFileManager;
import keydb.types.ValueIO;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

@EqualsAndHashCode
@ToString
@Getter
public class SparseIndex<T extends Comparable<T>> {
    private final List<Index<T>> indices;

    private final ValueIO<T> valueIO;

    public SparseIndex(final ValueIO<T> valueIO) {
        this.indices = new ArrayList<>();
        this.valueIO = valueIO;
    }

    public void insert(final T key, final long byteOffset) {
        insert(new Index<>(key, byteOffset));
    }

    public long getStartSearchByteOffset(final T key) {
        final int searchIndex = Collections.binarySearch(
                indices, new Index<>(key, 0), Comparator.comparing(Index::key));

        if (searchIndex < 0) {
            final int insertAt = -searchIndex - 1;
            return (insertAt == 0) ? 0 : indices.get(insertAt - 1).offset();
        } else {
            return indices.get(searchIndex).offset();
        }
    }

    public Try<Path> write(final Path path) {
        return Try.of(() -> Files.createFile(path))
            .andThenTry(() -> {
                try (final OutputFileManager outputFileManager = new OutputFileManager(path)) {
                    outputFileManager.runWithOutput(dataOutputStream -> {
                            // TODO: clean
                            for (final Index<T> index : indices) {
                                index.write(dataOutputStream, valueIO);
                            }
                        }
                    );
                }
            }
        );
    }

    public static <P extends Comparable<P>> Try<SparseIndex<P>> from(final Path path, final ValueIO<P> valueIO) {
        final SparseIndex<P> sparseIndex = new SparseIndex<>(valueIO);

        return Try.of(() -> {
            try (final InputFileManager inputFileManager = new InputFileManager(path)) {
                inputFileManager.acceptInputUntilEndOfFile(
                        dataInputStream -> sparseIndex.insert(Index.read(dataInputStream, valueIO)));
                return sparseIndex;
            }
        });
    }

    private void insert(final Index<T> index) {
        indices.add(index);
    }

    public record Index<T>(T key, long offset) {

        public void write(final DataOutput output, final ValueIO<T> valueIO) throws IOException {
            valueIO.write(key, output);
            output.writeLong(offset);
        }

        public static <P> Index<P> read(final DataInput input, final ValueIO<P> valueIO) throws IOException {
            final P key = valueIO.read(input);
            final long byteOffset = input.readLong();
            return new Index<P>(key, byteOffset);
        }
    }
}
