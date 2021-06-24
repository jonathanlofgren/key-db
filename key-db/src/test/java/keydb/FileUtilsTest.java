package keydb;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FileUtilsTest extends TestBase {

    @Nested
    class AcceptInputUntilEndOfFile {

        @Test
        void testItCanReadFromFile() throws Throwable {
            final Path path = getPath("/home/user/some_file");
            final byte[] byteArray = {1, 2, 3, 4, 5, 6};
            Files.write(path, byteArray);

            final List<Byte> bytes = new ArrayList<>();

            FileUtils.acceptInputUntilEndOfFile(path, (input) -> {
                bytes.add(input.readByte());
            });

            assertThat(bytes.toArray()).isEqualTo(byteArray);
        }

    }

    @Nested
    class DoWithOutput {

        @Test
        void testIt() throws Throwable {
            final Path path = getPath("/home/user/some_file");

            FileUtils.runWithOutput(path, (output) -> {
                output.writeDouble(23.2);
                output.writeDouble(23.2);
            });
        }
    }
}