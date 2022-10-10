package keydb;

import io.vavr.control.Try;
import keydb.config.DBConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class SegmentTest extends TestBase {

    Segment subject;
    Map<String, String> map;
    Path not_existing_segment_path = getPath("/home/user/10/");
    Path existing_segment_path = getPath("/home/user/200/");
    Path invalid_path = getPath("/home/user/123abc/");

    @BeforeEach
    void setup() {
        map = new HashMap<>();
        final MemTable memTable = new MemTable(getPath("/home/user/memtable"));
        for (int i = 0; i < 100; i++) {
            final String key = UUID.randomUUID().toString().substring(0, 10);
            final String value = UUID.randomUUID().toString();
            memTable.put(key, value);
            map.put(key, value);
        }
        subject = memTable.writeSegment(getPath("/home/user"), 200, DBConfig.builder().build()).get();
        Try.of(() -> Files.createDirectory(invalid_path));
    }

    @Nested
    class Get {

        @Test
        void testItCanGetAllKeysWithCorrectValues() {
            map.forEach((key, value) -> {
                assertThat(subject.get(key).get().get()).isEqualTo(value);
            });
        }
    }

    @Nested
    class From {
        @Nested
        class WhenFileDoesNotExist {
            @Test
            void itReturnsFailure() {
                assertThat(Segment.from(not_existing_segment_path).isFailure()).isTrue();
            }
        }

        @Nested
        class WhenFileExists {

            @Test
            void itShouldReturnSuccessWithCorrectInstance() {
                final Segment segment = Segment.from(existing_segment_path).get();
                assertThat(segment).isEqualTo(subject);
            }
        }

        @Nested
        class WhenInvalidName {

            @Test
            void itReturnsFailureWithFormatException() {
                assertThat(Segment.from(invalid_path).failed().get())
                        .isExactlyInstanceOf(NumberFormatException.class);
            }
        }
    }

    @Test
    void getIndexPath() {
        assertThat(Segment.getIndexPath(getPath("/home/user/0"))).isEqualTo(
                getPath("/home/user/0/index"));
    }

    @Test
    void getDataPath() {
        assertThat(Segment.getDataPath(getPath("/home/user/0"))).isEqualTo(
                getPath("/home/user/0/data"));
    }
}
