package keydb.file;

import io.vavr.CheckedConsumer;
import lombok.SneakyThrows;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;

public class OutputFileManager extends FileManager {

    private final DataOutputStream dataOutputStream;

    @SneakyThrows
    public OutputFileManager(final Path path, final OpenOption... options) {
        dataOutputStream = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path, options)));
    }

    @SneakyThrows
    public void runWithOutput(final CheckedConsumer<DataOutputStream> consumer) {
        consumer.accept(dataOutputStream);
    }

    @Override
    public void close() throws Exception {
        dataOutputStream.close();
    }
}
