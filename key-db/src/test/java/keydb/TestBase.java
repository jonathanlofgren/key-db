package keydb;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

public class TestBase {

    public static Map<String, String> keyValues;
    private final FileSystem fileSystem = Jimfs.newFileSystem(Configuration.unix()
            .toBuilder()
            .setWorkingDirectory("/home/user")
            .build());

    @BeforeAll
    public static void generateKeyValues() {
        keyValues = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            final String key = UUID.randomUUID().toString().substring(0, 10);
            final String value = UUID.randomUUID().toString();
            keyValues.put(key, value);
        }
    }

    @AfterEach
    void afterEach() throws IOException {
        fileSystem.close();
    }

    Path getPath(final String path) {
        return fileSystem.getPath(path);
    }

    protected void forKeyValues(final BiConsumer<String, String> consumer) {
        for (final Map.Entry<String, String> entry : keyValues.entrySet()) {
            consumer.accept(entry.getKey(), entry.getValue());
        }
    }
}
