package keydb;

import keydb.file.OutputFileManager;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.file.StandardOpenOption;

class OutputFileManagerTest extends TestBase {

    @Nested
    class runWithOutput {

        @Test
        void itRunsGivenClosureOnOutputStream() throws Exception {
            try (final OutputFileManager fileManager = new OutputFileManager(
                    getPath("/home/user/some_file"),
                    StandardOpenOption.APPEND,
                    StandardOpenOption.CREATE)) {
                fileManager.runWithOutput(output -> output.writeInt(100));
            }
        }
    }
}
