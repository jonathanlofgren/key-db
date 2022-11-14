package keydb;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class SparseIndexTest extends TestBase {

    @Test
    void returns_correct_byte_offsets_for_keys() {
        final SparseIndex<String> sut = buildIndex();

        assertThat(sut.getStartSearchByteOffset("A")).isEqualTo(0);
        assertThat(sut.getStartSearchByteOffset("B")).isEqualTo(20);
        assertThat(sut.getStartSearchByteOffset("C")).isEqualTo(20);
        assertThat(sut.getStartSearchByteOffset("L")).isEqualTo(1000);
        assertThat(sut.getStartSearchByteOffset("Z")).isEqualTo(240000);
    }

    @Test
    void can_write_and_read_back_index_correctly() {
        final SparseIndex<String> sut = buildIndex();
        final Path writePath = getPath("/home/user/index");

        final Path writtenPath = sut.write(writePath).get();
        final SparseIndex<String> newIndex = SparseIndex.<String>from(writePath).get();

        assertThat(writtenPath).isEqualTo(writePath);
        assertThat(newIndex).isEqualTo(sut);
    }

    private SparseIndex<String> buildIndex() {
        final SparseIndex<String> index = new SparseIndex<>();
        index.insert("B", 20);
        index.insert("D", 200);
        index.insert("K", 1000);
        index.insert("W", 240000);
        return index;
    }
}