package keydb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class KeyDBTest extends TestBase {

    KeyDB db;

    @BeforeEach
    void setUpDB() {
        db = KeyDB.from(getPath("home/user/db"));
        db.set("key1", "val1");
    }

    @Nested
    class WhenKeyExists {

        @Test
        void TestGetReturnsOptionalWithValue() {
            final Optional<String> value = db.get("key1");
            assertThat(value.get()).isEqualTo("val1");
        }
    }

    @Nested
    class WhenKeyDoesNotExists {

        @Test
        void TestGetReturnsEmptyOptional() {
            assertThat(db.get("val2")).isEqualTo(Optional.empty());
        }
    }
}
