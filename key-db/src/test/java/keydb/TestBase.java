package keydb;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.jupiter.api.AfterEach;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;

public class TestBase {

    private final FileSystem fileSystem = Jimfs.newFileSystem(Configuration.unix()
            .toBuilder()
            .setWorkingDirectory("/home/user")
            .build());

    @AfterEach
    void afterEach() throws IOException {
        fileSystem.close();
    }

    Path getPath(final String path) {
        return fileSystem.getPath(path);
    }
}
