package keydb;

import io.vavr.control.Option;
import keydb.config.DBConfig;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class KeyDBTest extends TestBase {

    @Test
    void empty_database_has_no_segments() {
        final KeyDB sut = KeyDB.create(getPath("db"));

        final int segments = sut.numSegments();

        assertThat(segments).isEqualTo(0);
    }

    @Test
    void empty_database_does_not_have_key() {
        final KeyDB sut = KeyDB.create(getPath("db"));

        final Option<String> value = sut.get("key");

        assertThat(value).isEqualTo(Option.none());
    }

    @Test
    void put_and_get_value() {
        final KeyDB sut = KeyDB.create(getPath("db"));
        sut.put("key1", "value1");

        final Option<String> value = sut.get("key1");

        assertThat(value).isEqualTo(Option.of("value1"));
    }

    @Test
    void flush_to_new_segment_every_byte() {
        final KeyDB sut = createDBWithOneKeyPerSegment();

        final int segments = sut.numSegments();
        final Option<String> value = sut.get("key1");

        assertThat(segments).isEqualTo(4);
        assertThat(value).isEqualTo(Option.of("value1"));
    }

    @Test
    void loading_non_existent_database_throws_error() {
        assertThatThrownBy(() -> KeyDB.load(getPath("db")))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void read_from_loaded_existing_database() throws IOException {
        createDBWithOneKeyPerSegment();

        final KeyDB sut = KeyDB.load(getPath("db"));
        final int segments = sut.numSegments();
        final Option<String> value = sut.get("key1");

        assertThat(segments).isEqualTo(4);
        assertThat(value).isEqualTo(Option.of("value1"));
    }

    @Test
    void config_is_persisted_with_database() throws IOException {
        createDBWithOneKeyPerSegment();

        final KeyDB sut = KeyDB.load(getPath("db"));
        final DBConfig config = sut.getConfig();

        assertThat(config).isEqualTo(DBConfig.builder()
                .memTableFlushSizeBytes(1)
                .build());
    }

    @Test
    void the_latest_value_overwrites_previous_values() {
        final KeyDB db = createDBWithOneKeyPerSegment();
        db.put("key1", "newValue1");
        db.put("key2", "newValue2");

        assertThat(db.get("key1")).isEqualTo(Option.of("newValue1"));
        assertThat(db.get("key2")).isEqualTo(Option.of("newValue2"));
    }

    private KeyDB createDBWithOneKeyPerSegment() {
        final KeyDB sut = KeyDB.create(getPath("db"), DBConfig.builder()
                .memTableFlushSizeBytes(1)
                .build());
        sut.put("key1", "value1");
        sut.put("key2", "value2");
        sut.put("key3", "value3");
        sut.put("key4", "value4");
        return sut;
    }
}
