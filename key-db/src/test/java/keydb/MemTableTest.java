package keydb;

import io.vavr.control.Option;
import io.vavr.control.Try;
import keydb.config.DBConfig;
import keydb.types.ValueIO;
import org.junit.jupiter.api.Test;

import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class MemTableTest extends TestBase {

    private final Path some_path = getPath("/home/user/memtable");

    private final ValueIO<String> valueIO = ValueIO.getProvider(String.class);

    @Test
    void return_none_when_key_does_not_exist() throws Exception {
        try (final MemTable<String> sut = getMemTable()) {
            final Option<String> key = sut.get("key");

            assertThat(key).isEqualTo(Option.none());
        }
    }

    @Test
    void returns_correct_value_when_key_exists() throws Exception {
        try (final MemTable<String> sut = getMemTable()) {
            sut.put("hey", "bob");

            final Option<String> value = sut.get("hey");

            assertThat(value).isEqualTo(Option.of("bob"));
        }
    }

    @Test
    void an_empty_table_has_size_zero() throws Exception {
        try (final MemTable<String> sut = getMemTable()) {
            final long size = sut.getSize();

            assertThat(size).isZero();
        }
    }

    @Test
    void a_table_with_data_has_correct_size() throws Exception {
        try (final MemTable<String> sut = getMemTable()) {
            sut.put("hey", "bob");

            final long size = sut.getSize();

            assertThat(size).isEqualTo(2 + 8);
        }
    }

    @Test
    void table_can_be_created_from_previous_logfile() throws Exception {
        final MemTable<String> table = getMemTableWithData();
        table.close();

        final MemTable<String> sut = MemTable.from(some_path, valueIO).get();

        forKeyValues((key, value) -> assertThat(sut.get(key)).isEqualTo(Option.of(value)));
    }

    @Test
    void creating_from_non_existing_file_throws_exception() {
        final Try<MemTable<String>> sut = MemTable.from(some_path, valueIO);

        assertThat(sut.getCause()).isExactlyInstanceOf(NoSuchFileException.class)
                .hasMessage(some_path.toString());
    }

    @Test
    void read_data_from_written_segment() {
        final int id = 1;
        final MemTable<String> sut = getMemTableWithData();

        final Segment<String> segment = sut.writeSegment(getPath("/home/user"),
                id,
                DBConfig.builder().build()).get();

        assertThat(segment.getId()).isEqualTo(id);
        forKeyValues((key, value) -> assertThat(segment.get(key, valueIO).get()).isEqualTo(Option.of(value)));
    }

    private MemTable<String> getMemTableWithData() {
        final MemTable<String> sut = getMemTable();
        forKeyValues(sut::put);
        return sut;
    }

    private MemTable<String> getMemTable() {
        return new MemTable<>(some_path, valueIO);
    }
}
