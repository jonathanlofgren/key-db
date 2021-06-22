package keydb;

import io.vavr.Tuple2;
import io.vavr.control.Try;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class SparseIndexTest extends TestBase {

    SparseIndex subject;
    Path existing_path;
    Path not_existing_path;

    @BeforeEach
    void setup() {
        existing_path = getPath("/home/user/index");
        not_existing_path = getPath("/home/user/no_index");

        subject = new SparseIndex();
        subject.insert("B", 20);
        subject.insert("D", 200);
        subject.insert("K", 1000);
        subject.insert("W", 240000);

        Try.of(() -> Files.createFile(existing_path))
                .andThenTry(() -> {
                    final OutputStream output = Files.newOutputStream(existing_path);
                    final DataOutputStream stream = new DataOutputStream(output);

                    for (final Tuple2<String, Long> index : subject.getIndices()) {
                        stream.writeInt(index._1.getBytes(Config.CHARSET).length);
                        stream.write(index._1.getBytes(Config.CHARSET));
                        stream.writeLong(index._2);
                    }
                });
    }

    @Nested
    class Write {

        @Nested
        class WhenFileDoesNotExist {
            @Test
            void itCreatesFileWithCorrectContents() throws IOException {
                assertThat(subject.write(not_existing_path).get()).isEqualTo(not_existing_path);
                assertThat(Files.readAllBytes(not_existing_path)).isEqualTo(Files.readAllBytes(existing_path));
            }
        }

        @Nested
        class WenFileAlreadyExists {

            @Test
            void itShouldReturnFalse() {
                assertThat(subject.write(existing_path).isFailure()).isTrue();
            }
        }
    }

    @Nested
    class From {
        @Nested
        class WhenFileDoesNotExist {
            @Test
            void itReturnsFailure() {
                assertThat(SparseIndex.from(not_existing_path).isFailure()).isTrue();
            }
        }

        @Nested
        class WhenFileExists {

            @Test
            void itShouldReturnSuccessWithExpectedInstance() {
                assertThat(SparseIndex.from(existing_path).get()).isEqualTo(subject);
            }
        }
    }

    @Test
    void getStartSearchByteOffset() {
        assertThat(subject.getStartSearchByteOffset("A")).isEqualTo(0);
        assertThat(subject.getStartSearchByteOffset("B")).isEqualTo(20);
        assertThat(subject.getStartSearchByteOffset("C")).isEqualTo(20);
        assertThat(subject.getStartSearchByteOffset("Z")).isEqualTo(240000);
    }
}