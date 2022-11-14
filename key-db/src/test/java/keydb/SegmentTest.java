package keydb;

import com.google.common.collect.ImmutableList;
import io.vavr.control.Option;
import io.vavr.control.Try;
import keydb.config.DBConfig;
import keydb.types.ValueIO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class SegmentTest extends TestBase {

    private final ValueIO<String> valueIO = ValueIO.getProvider(String.class);

    @Test
    void get_returns_correct_values() {
        final MemTable<String> memTable = getMemTable();
        forKeyValues(memTable::put);
        final Segment<String> sut = memTable.writeSegment(getPath("/home/user/"), 1, DBConfig.builder().build()).get();

        forKeyValues((key, value) -> assertThat(sut.get(key, valueIO).get()).isEqualTo(Option.of(value)));
    }

    @Test
    void creating_from_non_existing_path_fails() {
        assertThat(Segment.from(getPath("/home/user/memtable"), valueIO).failed().get())
                .isExactlyInstanceOf(NoSuchFileException.class)
                .hasMessage("/home/user/memtable");
    }

    @Test
    void creating_from_disk_returns_matching_segment() {
        final MemTable<String> memTable = getMemTable();
        forKeyValues(memTable::put);
        final Segment<String> segment = memTable.writeSegment(getPath("/home/user/"), 1, DBConfig.builder().build()).get();

        final Segment<String> sut = Segment.from(getPath("/home/user/1"), valueIO).get();

        assertThat(sut).isEqualTo(segment);
    }

    @Test
    void is_sortable_in_order_of_most_recent_segment_to_oldest() {
        final Segment<String> one = new Segment<>(new SparseIndex<>(valueIO), getPath("one"), 1);
        final Segment<String> two = new Segment<>(new SparseIndex<>(valueIO), getPath("two"), 2);
        final Segment<String> three = new Segment<>(new SparseIndex<>(valueIO), getPath("three"), 3);
        final List<Segment<String>> segments = ImmutableList.of(two, one, three);

        assertThat(segments.stream().sorted()).containsExactly(three, two, one);
    }

    private MemTable<String> getMemTable() {
        return new MemTable<>(getPath("/home/user/memtable"), valueIO);
    }
}
