package keydb;

import io.vavr.control.Try;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class KeyDBTest extends TestBase {

    @Nested
    class From {

        @Nested
        class WhenDatabaseDoesNotExist {

            @Test
            void itShouldCreateDBAndSegmentsFolder() {
                final Path path = getPath("/home/user/new_db");
                final KeyDB db = KeyDB.from(path).get();

                assertThat(Files.isDirectory(path)).isTrue();
                assertThat(Files.isDirectory(path.resolve("segments"))).isTrue();
                assertThat(db.numSegments()).isEqualTo(0);
            }
        }

        @Nested
        class WhenDatabaseExists {

            @Nested
            class WhenInvalidDatabase {

                Path path = getPath("/home/user/db");

                @BeforeEach
                void setUp() {
                    Try.run(() -> Files.createDirectory(path));
                }

                @Test
                void itShouldReturnError() {
                    assertThat(KeyDB.from(path).isFailure()).isTrue();
                }
            }

            @Nested
            class WHenValidDatabase {

                Path path = getPath("/home/user/db");

                @BeforeEach
                void setUp() throws IOException {
                    Files.createDirectory(path);
                    Files.createDirectory(path.resolve("segments"));

                    for (int i = 0; i < 3; i++) {
                        Files.createDirectory(path.resolve("segments/" + i));
                        Files.createFile(path.resolve("segments/" + i + "/data"));
                        Files.createFile(path.resolve("segments/" + i + "/index"));
                    }
                }

                @Test
                void itShouldReturnKeyDB() {
                    final KeyDB db = KeyDB.from(path).get();
                    assertThat(db.numSegments()).isEqualTo(3);
                }
            }
        }
    }

}
