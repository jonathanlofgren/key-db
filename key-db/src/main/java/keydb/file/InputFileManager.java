package keydb.file;

import io.vavr.CheckedConsumer;
import lombok.SneakyThrows;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class InputFileManager extends FileManager<DataInputStream> {

    public InputFileManager(final Path path) throws IOException {
        super(new DataInputStream(new BufferedInputStream(Files.newInputStream(path))));
    }

    @SneakyThrows
    public void acceptInputUntilEndOfFile(final CheckedConsumer<DataInputStream> consumer) {
        while (true) {
            try {
                consumer.accept(resource);
            } catch (final EOFException e) {
                break;
            }
        }
    }
}
