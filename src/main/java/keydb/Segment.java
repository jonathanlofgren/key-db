package keydb;

import io.vavr.control.Option;
import io.vavr.control.Try;
import lombok.Cleanup;
import lombok.Value;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

@Value
public class Segment {

    SparseIndex index;
    Path rootPath;
    long id;

    public Try<Option<String>> get(final String key) {
        return Try.of(() -> {
            @Cleanup final InputStream inputStream = Files.newInputStream(getDataPath(rootPath));
            @Cleanup final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
            @Cleanup final DataInputStream dataInputStream = new DataInputStream(bufferedInputStream);

            dataInputStream.skip(index.getStartSearchByteOffset(key));

            while (true) {
                try {
                    final Entry entry = DataUtils.readEntry(dataInputStream);
                    final int order = entry.getKey().compareTo(key);

                    if (order > 0) {
                        break;
                    } else if (order == 0) {
                        return Option.some(entry.getValue());
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

            final long id = Long.parseLong(rootPath.getFileName().toString());
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
