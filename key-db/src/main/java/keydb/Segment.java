package keydb;

import io.vavr.control.Option;
import io.vavr.control.Try;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.EOFException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

@RequiredArgsConstructor
@Getter
@EqualsAndHashCode
public class Segment<T extends Comparable<T>> implements Comparable<Segment<T>> {

    private final SparseIndex<T> index;
    private final Path rootPath;
    private final Integer id;

    public Try<Option<T>> get(final T key) {
        return Try.of(() -> FileUtils.applyWithInput(getDataPath(rootPath), dataInputStream -> {
            dataInputStream.skip(index.getStartSearchByteOffset(key));

            while (true) {
                try {
                    final Entry<T> entry = Entry.read(dataInputStream);
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
        }));
    }

    public static <P extends Comparable<P>> Try<Segment<P>> from(final Path rootPath) {
        return Try.of(() -> {
            if (!Files.isDirectory(rootPath)) {
                throw new NoSuchFileException(rootPath.toString());
            }

            final Integer id = Integer.parseInt(rootPath.getFileName().toString());
            final SparseIndex<P> index = SparseIndex.<P>from(getIndexPath(rootPath)).get();

            return new Segment<>(index, rootPath, id);
        });
    }

    public static Path getIndexPath(final Path root) {
        return root.resolve("index");
    }

    public static Path getDataPath(final Path root) {
        return root.resolve("data");
    }

    @Override
    public int compareTo(final Segment<T> anotherSegment) {
        return anotherSegment.id.compareTo(id);
    }
}
