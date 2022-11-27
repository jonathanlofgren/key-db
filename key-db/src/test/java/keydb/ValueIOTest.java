package keydb;

import keydb.types.StringIO;
import keydb.types.ValueIO;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ValueIOTest {

    @Test
    void can_load_StringIO() {
        final ValueIO<String> valueIO = ValueIO.getProvider(String.class);

        assertThat(valueIO).isInstanceOf(StringIO.class);
    }
}
