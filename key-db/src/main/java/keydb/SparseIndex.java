package keydb;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Try;
import keydb.file.InputFileManager;
import keydb.file.OutputFileManager;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

@EqualsAndHashCode
@ToString
@Getter
public class SparseIndex {
    private final List<Tuple2<String, Long>> indices;

    SparseIndex() {
        indices = new ArrayList<>();
    }

    public void insert(final String key, final long byteOffset) {
        indices.add(Tuple.of(key, byteOffset));
    }

    private void insert(final Tuple2<String, Long> index) {
        insert(index._1, index._2);
    }

    public long getStartSearchByteOffset(final String key) {
        final int searchIndex = Collections.binarySearch(indices, Tuple.of(key, null),
                Comparator.comparing((Tuple2<String, Long> e) -> e._1));

        if (searchIndex < 0) {
            final int insertAt = -searchIndex - 1;
            return (insertAt == 0) ? 0 : indices.get(insertAt - 1)._2;
        } else {
            return indices.get(searchIndex)._2;
        }
    }

    public Try<Path> write(final Path path) {
        return Try.of(() -> Files.createFile(path))
                .andThenTry(() -> {
                    try (final OutputFileManager outputFileManager = new OutputFileManager(path)) {
                        outputFileManager.runWithOutput(dataOutputStream -> {
                                    for (final Tuple2<String, Long> index : indices) {
                                        DataUtils.writeIndex(index, dataOutputStream);
                                    }
                                }
                        );
                    }
                });
    }

    public static Try<SparseIndex> from(final Path path) {
        final SparseIndex sparseIndex = new SparseIndex();

        return Try.of(() -> {
            try (final InputFileManager inputFileManager = new InputFileManager(path)) {
                inputFileManager.acceptInputUntilEndOfFile(
                        dataInputStream -> sparseIndex.insert(DataUtils.readIndex(dataInputStream)));
                return sparseIndex;
            }
        });
    }
}
