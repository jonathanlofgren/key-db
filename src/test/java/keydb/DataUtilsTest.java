package keydb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataUtilsTest {

    @Nested
    class readEntry {

        @Nested
        class whenUnexpectedEndOfFile {

            DataInputStream inputStream;

            @BeforeEach
            void setUp() throws IOException {
                final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                final DataOutputStream stream = new DataOutputStream(byteArrayOutputStream);
                stream.writeInt(5);
                stream.writeInt(10);
                stream.writeUTF("hello");
                stream.writeUTF("there");
                inputStream = new DataInputStream(
                        new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
            }

            @Test
            void itThrowsRuntimeException() {
                assertThatThrownBy(() -> DataUtils.readEntry(inputStream))
                        .isExactlyInstanceOf(RuntimeException.class)
                        .hasMessage("Unexpected end of file");
            }
        }
    }

    @Nested
    class whenReadAndWriteTogether {

        private List<Entry> entries;

        @BeforeEach
        void setUp() {
            entries = new ArrayList<>();
            entries.add(new Entry("hey", "there"));
            entries.add(new Entry("foo", "bar"));
            entries.add(new Entry("alice", "and bob"));
        }

        @Test
        void canWriteAndReadBackExpectedEntries() throws IOException {
            final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            final DataOutputStream outputStream = new DataOutputStream(byteStream);

            for (final Entry entry : entries) {
                DataUtils.writeEntry(outputStream, entry);
            }

            final ArrayList<Entry> readEntries = new ArrayList<>();
            final DataInputStream inputStream = new DataInputStream(
                    new ByteArrayInputStream(byteStream.toByteArray()));

            readEntries.add(DataUtils.readEntry(inputStream));
            readEntries.add(DataUtils.readEntry(inputStream));
            readEntries.add(DataUtils.readEntry(inputStream));

            assertThat(readEntries).isEqualTo(entries);
            assertThat(inputStream.available()).isEqualTo(0);
        }
    }
}