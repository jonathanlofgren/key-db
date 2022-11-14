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
        final KeyDB<String> sut = KeyDB.create(getPath("db"), String.class);

        final int segments = sut.numSegments();

        assertThat(segments).isEqualTo(0);
    }

    @Test
    void empty_database_does_not_have_key() {
        final KeyDB<String> sut = KeyDB.create(getPath("db"), String.class);

        final Option<String> value = sut.get("key");

        assertThat(value).isEqualTo(Option.none());
    }

    @Test
    void put_and_get_value() {
        final KeyDB<String> sut = KeyDB.create(getPath("db"), String.class);
        sut.put("key1", "value1");

        final Option<String> value = sut.get("key1");

        assertThat(value).isEqualTo(Option.of("value1"));
    }

    @Test
    void flush_to_new_segment_every_byte() {
        final KeyDB<String> sut = createDBWithOneKeyPerSegment();

        final int segments = sut.numSegments();
        final Option<String> value = sut.get("key1");

        assertThat(segments).isEqualTo(4);
        assertThat(value).isEqualTo(Option.of("value1"));
    }

    @Test
    void loading_non_existent_database_throws_error() {
        assertThatThrownBy(() -> KeyDB.load(getPath("db"), String.class))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void read_from_loaded_existing_database() throws IOException {
        createDBWithOneKeyPerSegment();

        final KeyDB<String> sut = KeyDB.load(getPath("db"), String.class);
        final int segments = sut.numSegments();
        final Option<String> value = sut.get("key1");

        assertThat(segments).isEqualTo(4);
        assertThat(value).isEqualTo(Option.of("value1"));
    }

    @Test
    void config_is_persisted_with_database() throws IOException {
        createDBWithOneKeyPerSegment();

        final KeyDB<String> sut = KeyDB.load(getPath("db"), String.class);
        final DBConfig config = sut.getConfig();

        assertThat(config).isEqualTo(DBConfig.builder()
                .memTableFlushSizeBytes(1)
                .build());
    }

    @Test
    void the_latest_value_overwrites_previous_values() {
        final KeyDB<String> db = createDBWithOneKeyPerSegment();
        db.put("key1", "newValue1");
        db.put("key2", "newValue2");

        assertThat(db.get("key1")).isEqualTo(Option.of("newValue1"));
        assertThat(db.get("key2")).isEqualTo(Option.of("newValue2"));
    }

    @Test
    void the_latest_value_overwrites_previous_values_on_reloaded_db() throws IOException {
        final KeyDB<String> db = createDBWithOneKeyPerSegment();
        db.put("key1", "newValue1");
        db.put("key2", "newValue2");

        final KeyDB<String> sut = KeyDB.load(getPath("db"), String.class);

        assertThat(sut.get("key1")).isEqualTo(Option.of("newValue1"));
        assertThat(sut.get("key2")).isEqualTo(Option.of("newValue2"));
    }

    private KeyDB<String> createDBWithOneKeyPerSegment() {
        final KeyDB<String> sut = KeyDB.create(getPath("db"), DBConfig.builder()
                .memTableFlushSizeBytes(1)
                .build(), String.class);
        sut.put("key1", "value1");
        sut.put("key2", "value2");
        sut.put("key3", "value3");
        sut.put("key4", "value4");
        return sut;
    }
}
