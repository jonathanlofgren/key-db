package keydb;

import io.vavr.CheckedFunction1;
import lombok.Cleanup;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;

public final class FileUtils {

    public static <T> T applyWithInput(
            final Path path,
            final CheckedFunction1<DataInputStream, T> consumer) throws Throwable {
        @Cleanup final InputStream inputStream = Files.newInputStream(path);
        @Cleanup final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        @Cleanup final DataInputStream dataInputStream = new DataInputStream(bufferedInputStream);

        return consumer.apply(dataInputStream);
    }

    public static <R> R applyWithOutput(
            final Path path,
            final CheckedFunction1<DataOutputStream, R> function,
            final OpenOption... options) throws Throwable {
        @Cleanup final OutputStream outputStream = Files.newOutputStream(path, options);
        @Cleanup final BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);
        @Cleanup final DataOutputStream dataOutputStream = new DataOutputStream(bufferedOutputStream);
        return function.apply(dataOutputStream);
    }
}
