package keydb;

import io.vavr.control.Option;
import io.vavr.control.Try;
import keydb.config.DBConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class SegmentTest extends TestBase {

    @Test
    void get_returns_correct_values() {
        final MemTable memTable = new MemTable(getPath("/home/user/memtable"));
        forKeyValues(memTable::put);
        final Segment sut = memTable.writeSegment(getPath("/home/user/"), 1, DBConfig.builder().build()).get();

        forKeyValues((key, value) -> assertThat(sut.get(key).get()).isEqualTo(Option.of(value)));
    }

    @Test
    void creating_from_non_existing_path_fails() {
        assertThat(Segment.from(getPath("/home/user/memtable")).failed().get())
                .isExactlyInstanceOf(NoSuchFileException.class)
                .hasMessage("/home/user/memtable");
    }

    @Test
    void creating_from_disk_returns_matching_segment() {
        final MemTable memTable = new MemTable(getPath("/home/user/memtable"));
        forKeyValues(memTable::put);
        final Segment segment = memTable.writeSegment(getPath("/home/user/"), 1, DBConfig.builder().build()).get();

        final Segment sut = Segment.from(getPath("/home/user/1")).get();

        assertThat(sut).isEqualTo(segment);
    }
}
