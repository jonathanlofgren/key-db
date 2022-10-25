package keydb;

import io.vavr.control.Option;
import io.vavr.control.Try;
import lombok.Cleanup;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

@RequiredArgsConstructor
@Getter
@EqualsAndHashCode
public class Segment {

    private final SparseIndex index;
    private final Path rootPath;
    private final Integer id;

    public Try<Option<String>> get(final String key) {
        return Try.of(() -> {
            @Cleanup final InputStream inputStream = Files.newInputStream(getDataPath(rootPath));
            @Cleanup final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
            @Cleanup final DataInputStream dataInputStream = new DataInputStream(bufferedInputStream);

            dataInputStream.skip(index.getStartSearchByteOffset(key));

            while (true) {
                try {
                    final Entry entry = Entry.read(dataInputStream);
                    final int order = entry.key().compareTo(key);

                    if (order > 0) {
                        break;
                    } else if (order == 0) {
                        return Option.some(entry.value());
                    }
                } catch (final EOFException e) {
                    break;
                }
            }

            return Option.none();
        });
    }

    public static Try<Segment> from(final Path rootPath) {
        return Try.of(() -> {
            if (!Files.isDirectory(rootPath)) {
                throw new NoSuchFileException(rootPath.toString());
            }

            final Integer id = Integer.parseInt(rootPath.getFileName().toString());
            final SparseIndex index = SparseIndex.from(getIndexPath(rootPath)).get();

            return new Segment(index, rootPath, id);
        });
    }

    public static Path getIndexPath(final Path root) {
        return root.resolve("index");
    }

    public static Path getDataPath(final Path root) {
        return root.resolve("data");
    }
}
