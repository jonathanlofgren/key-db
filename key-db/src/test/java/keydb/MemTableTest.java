package keydb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class MemTableTest extends TestBase {

    private MemTable subject;
    private final Path not_existing_path = getPath("/home/user/no_memtable");
    private final Path existing_path = getPath("/home/user/memtable");

    @BeforeEach
    void createMemTable() {
        subject = new MemTable(existing_path);

        // Bytes: 100 * (10 * 2 + 36 * 2 + 8) = 10kB
        for (int i = 0; i < 1000; i++) {
            subject.set(UUID.randomUUID().toString().substring(0, 10), UUID.randomUUID().toString());
        }
    }

    @Nested
    class Set {
        @Test
        void itWritesAsEntriesAreSet() {
            final MemTable memTable = new MemTable(not_existing_path);
            memTable.set("hey", "bob");
            assertThat(Files.isRegularFile(not_existing_path)).isTrue();
            memTable.set("another", "alice");
            memTable.set("{}", "100");
        }

    }

    @Nested
    class Size {
        @Test
        void itReturnsExpectedSize() {
            final MemTable memTable = new MemTable(not_existing_path);

            assertThat(memTable.getSize()).isEqualTo(0);
            memTable.set("hey", "bob");
            assertThat(memTable.getSize()).isEqualTo(6 * 2 + 8);
            memTable.set("another", "alice");
            assertThat(memTable.getSize()).isEqualTo(6 * 2 + 8 + 12 * 2 + 8);
            memTable.set("{}", "100");
            assertThat(memTable.getSize()).isEqualTo(6 * 2 + 8 + 12 * 2 + 8 + 5 * 2 + 8);

        }
    }

    @Nested
    class Get {
        @Test
        void itReturnsCorrectData() {
            final MemTable memTable = new MemTable(not_existing_path);

            assertThat(memTable.get("hello").isPresent()).isFalse();
            memTable.set("hello", "there");
            assertThat(memTable.get("hello").get()).isEqualTo("there");
        }
    }

    @Nested
    class From {
        @Nested
        class WhenFileDoesNotExist {
            @Test
            void itReturnsFailure() {
                assertThat(MemTable.from(not_existing_path).isFailure()).isTrue();
            }
        }

        @Nested
        class WhenFileExists {
            @Test
            void itReturnsCorrectMemTable() {
                final MemTable actual = MemTable.from(existing_path).get();
                assertThat(actual).isEqualTo(subject);
                assertThat(actual.getSize()).isEqualTo(100000);
            }
        }
    }

    @Nested
    class WriteSegment {

        @Test
        void itWritesNewSegmentFiles() {
            final Path path = getPath("/home/user/100");

            final Segment segment = subject.writeSegment(path).get();

            assertThat(segment.getId()).isEqualTo(100);
            assertThat(segment.getRootPath()).isEqualTo(path);
            assertThat(Files.isDirectory(path)).isTrue();
            assertThat(Files.isRegularFile(path.resolve("data"))).isTrue();
            assertThat(Files.isRegularFile(path.resolve("index"))).isTrue();
        }

        @Test
        void itCreatesIndexAtFirstKey() {
            final Path path = getPath("/home/user/100");
            subject.writeSegment(path);

            final SparseIndex index = SparseIndex.from(path.resolve("index")).get();

            assertThat(index.getIndices().size()).isGreaterThan(1);
            assertThat(index.getIndices()).first().matches(ix -> ix._2 == 0);
        }
    }
}
