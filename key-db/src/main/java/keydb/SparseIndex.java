package keydb;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Try;
import keydb.file.InputFileManager;
import keydb.file.OutputFileManager;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

    SparseIndex() {
        indices = new ArrayList<>();
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

    /**
     * @param path
     * @return
     */
    public Try<Path> write(final Path path) {
        return Try.of(() -> Files.createFile(path))
            .andThenTry(() -> {
                try (final OutputFileManager outputFileManager = new OutputFileManager(path)) {
                    outputFileManager.runWithOutput(dataOutputStream -> {
                            // TODO: clean
                            for (final Index<?> index : indices) {
                                index.write(dataOutputStream);
                            }
                        }
                    );
                }
            }
        );
    }

    public static <P extends Comparable<P>> Try<SparseIndex<P>> from(final Path path) {
        final SparseIndex<P> sparseIndex = new SparseIndex<>();

        return Try.of(() -> {
            try (final InputFileManager inputFileManager = new InputFileManager(path)) {
                inputFileManager.acceptInputUntilEndOfFile(
                        dataInputStream -> sparseIndex.insert(Index.read(dataInputStream)));
                return sparseIndex;
            }
        });
    }

    private void insert(final Index<T> index) {
        indices.add(index);
    }

    public record Index<T>(T key, long offset) {

        public void write(final DataOutputStream dataOutputStream) throws IOException {
            final byte[] keyBytes = ((String) key).getBytes(StandardCharsets.UTF_8);
            dataOutputStream.writeInt(keyBytes.length);
            dataOutputStream.write(keyBytes);
            dataOutputStream.writeLong(offset);
        }

        public static <P> Index<P> read(final DataInputStream dataInput) throws IOException {
            final int keyLength = dataInput.readInt();
            final byte[] keyBuffer = new byte[keyLength];
            final int readKey = dataInput.read(keyBuffer, 0, keyLength);
            if (readKey != keyLength) {
                throw new RuntimeException("Unexpected end of file");
            }
            final long byteOffset = dataInput.readLong();
            final P s = (P) new String(keyBuffer, StandardCharsets.UTF_8);
            return new Index<P>(s, byteOffset);
        }
    }
}
