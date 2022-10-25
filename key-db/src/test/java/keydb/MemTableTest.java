package keydb;

import io.vavr.control.Option;
import io.vavr.control.Try;
import keydb.config.DBConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class MemTableTest extends TestBase {

    private final Path some_path = getPath("/home/user/memtable");

    @Test
    void return_none_when_key_does_not_exist() throws Exception {
        try (final MemTable sut = new MemTable(some_path)) {
            final Option<String> key = sut.get("key");

            assertThat(key).isEqualTo(Option.none());
        }
    }

    @Test
    void returns_correct_value_when_key_exists() throws Exception {
        try (final MemTable sut = new MemTable(some_path)) {
            sut.put("hey", "bob");

            final Option<String> value = sut.get("hey");

            assertThat(value).isEqualTo(Option.of("bob"));
        }
    }

    @Test
    void an_empty_table_has_size_zero() throws Exception {
        try (final MemTable sut = new MemTable(some_path)) {
            final long size = sut.getSize();

            assertThat(size).isZero();
        }
    }

    @Test
    void a_table_with_data_has_correct_size() throws Exception {
        try (final MemTable sut = new MemTable(some_path)) {
            sut.put("hey", "bob");

            final long size = sut.getSize();

            assertThat(size).isEqualTo(6 * 2 + 8);
        }
    }

    @Test
    void table_can_be_created_from_previous_logfile() throws Exception {
        final MemTable table = getMemTableWithData();
        table.close();

        final MemTable sut = MemTable.from(some_path).get();

        forKeyValues((key, value) -> assertThat(sut.get(key)).isEqualTo(Option.of(value)));
    }

    @Test
    void creating_from_non_existing_file_throws_exception() {
        final Try<MemTable> sut = MemTable.from(some_path);

        assertThat(sut.getCause()).isExactlyInstanceOf(NoSuchFileException.class)
                .hasMessage(some_path.toString());
    }

    @Test
    void read_data_from_written_segment() {
        final int id = 1;
        final MemTable sut = getMemTableWithData();

        final Segment segment = sut.writeSegment(getPath("/home/user"),
                id,
                DBConfig.builder().build()).get();

        assertThat(segment.getId()).isEqualTo(id);
        forKeyValues((key, value) -> assertThat(segment.get(key).get()).isEqualTo(Option.of(value)));
    }

    private MemTable getMemTableWithData() {
        final MemTable sut = new MemTable(some_path);
        forKeyValues(sut::put);
        return sut;
    }
}
