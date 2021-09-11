package keydb.file;

import io.vavr.CheckedConsumer;
import lombok.SneakyThrows;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.nio.file.Files;
import java.nio.file.Path;

public class InputFileManager extends FileManager {

    private final DataInputStream dataInputStream;

    @SneakyThrows
    public InputFileManager(final Path path) {
        dataInputStream = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
    }

    @SneakyThrows
    public void acceptInputUntilEndOfFile(final CheckedConsumer<DataInputStream> consumer) {
        while (true) {
            try {
                consumer.accept(dataInputStream);
            } catch (final EOFException e) {
                break;
            }
        }
    }

    @Override
    public void close() throws Exception {
        dataInputStream.close();
    }
}
