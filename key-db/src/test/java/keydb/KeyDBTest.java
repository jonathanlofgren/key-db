package keydb;

import io.vavr.control.Option;
import io.vavr.control.Try;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class KeyDBTest extends TestBase {

    @Nested
    class Get {

        KeyDB db;
        MemTable mockMemTable;
        Deque<Segment> segments;
        Segment oldSegment;
        Segment newSegment;

        @BeforeEach
        void setUp() {
            oldSegment = mock(Segment.class);
            newSegment = mock(Segment.class);
            segments = new ArrayDeque<>();
            segments.push(oldSegment);
            segments.push(newSegment);
            mockMemTable = mock(MemTable.class);
            db = new KeyDB(getPath("/home/user/db"), mockMemTable, segments);

            when(mockMemTable.get("a")).thenReturn(Option.of("1"));
            when(mockMemTable.get("b")).thenReturn(Option.of("2"));
            when(mockMemTable.get("c")).thenReturn(Option.none());
            when(mockMemTable.get("d")).thenReturn(Option.none());
            when(mockMemTable.get("e")).thenReturn(Option.none());
            when(mockMemTable.get("f")).thenReturn(Option.none());

            when(newSegment.get("c")).thenReturn(Try.success(Option.of("3 in segment 0")));
            when(newSegment.get("d")).thenReturn(Try.success(Option.none()));
            when(newSegment.get("f")).thenReturn(Try.success(Option.none()));
            when(oldSegment.get("c")).thenReturn(Try.success(Option.of("3 in segment 1")));
            when(oldSegment.get("d")).thenReturn(Try.success(Option.of("4 in segment 1")));
            when(oldSegment.get("f")).thenReturn(Try.success(Option.none()));
        }

        @Nested
        class WhenKeyInMemTable {

            @Test
            void itShouldReturnValueFromMemTableOrElseNothing() {
                assertThat(db.get("a")).isEqualTo(Option.of("1"));
                assertThat(db.get("b")).isEqualTo(Option.of("2"));
                verify(mockMemTable).get("a");
                verify(mockMemTable).get("b");
                verifyNoMoreInteractions(mockMemTable, newSegment, oldSegment);
            }
        }

        @Nested
        class WhenKeyInSegments {

            @Test
            void itShouldReturnValueFromTheFirstContainingSegment() {
                assertThat(db.get("c")).isEqualTo(Option.of("3 in segment 0"));
                assertThat(db.get("d")).isEqualTo(Option.of("4 in segment 1"));
                assertThat(db.get("f")).isEqualTo(Option.none());

                verify(newSegment).get("c");
                verify(newSegment).get("d");
                verify(oldSegment).get("d");
                verify(newSegment).get("f");
                verify(oldSegment).get("f");
                verifyNoMoreInteractions(newSegment, oldSegment);
            }
        }

    }

    @Nested
    class Put {

        KeyDB db;
        MemTable mockMemTable;
        Deque<Segment> segments;

        @BeforeEach
        void setUp() {
            segments = new ArrayDeque<>();
            mockMemTable = mock(MemTable.class);
            db = new KeyDB(getPath("/home/user/"), mockMemTable, segments);
            Try.run(() -> Files.createDirectory(getPath("/home/user/db/segments/")));
        }

        @Nested
        class WhenMemTableSizeBelowThreshold {

            @BeforeEach
            void setUp() {
                when(mockMemTable.getSize()).thenReturn(Config.MEMTABLE_FLUSH_SIZE_BYTES - 1);
            }

            @Test
            void itShouldOnlySaveToMemTable() {
                db.put("foo", "bar");

                verify(mockMemTable).put("foo", "bar");
                verify(mockMemTable).getSize();
                assertThat(db.numSegments()).isEqualTo(0);
                verifyNoMoreInteractions(mockMemTable);
            }
        }

        @Nested
        class WhenMemTableSizeAboveThreshold {

            @BeforeEach
            void setUp() {
                when(mockMemTable.getSize()).thenReturn(Config.MEMTABLE_FLUSH_SIZE_BYTES);
                when(mockMemTable.writeSegment(any(), any())).thenReturn(Try.success(mock(Segment.class)));
            }

            @Test
            void itShouldFlushToSegmentAndCreateNewMemTable() {
                db.put("foo", "bar");

                verify(mockMemTable).put("foo", "bar");
                verify(mockMemTable).getSize();
                verify(mockMemTable).writeSegment(getPath("/home/user/segments"), 0);
                assertThat(db.numSegments()).isEqualTo(1);
                verifyNoMoreInteractions(mockMemTable);
            }
        }
    }

    @Nested
    class From {

        @Nested
        class WhenDatabaseDoesNotExist {

            @Test
            void itShouldCreateDBAndSegmentsFolder() {
                final Path path = getPath("/home/user/new_db");
                final KeyDB db = KeyDB.from(path).get();

                assertThat(Files.isDirectory(path)).isTrue();
                assertThat(Files.isDirectory(path.resolve("segments"))).isTrue();
                assertThat(db.numSegments()).isEqualTo(0);
            }
        }

        @Nested
        class WhenDatabaseExists {

            @Nested
            class WhenInvalidDatabase {

                Path path = getPath("/home/user/db");

                @BeforeEach
                void setUp() {
                    Try.run(() -> Files.createDirectory(path));
                }

                @Test
                void itShouldReturnError() {
                    assertThat(KeyDB.from(path).isFailure()).isTrue();
                }
            }

            @Nested
            class WHenValidDatabase {

                Path path = getPath("/home/user/db");

                @BeforeEach
                void setUp() throws IOException {
                    Files.createDirectory(path);
                    Files.createDirectory(path.resolve("segments"));

                    for (int i = 0; i < 3; i++) {
                        Files.createDirectory(path.resolve("segments/" + i));
                        Files.createFile(path.resolve("segments/" + i + "/data"));
                        Files.createFile(path.resolve("segments/" + i + "/index"));
                    }

                    // Write some data to memTable file
                    final MemTable memtable = new MemTable(path.resolve("memtable"));
                    memtable.put("a", "1");
                    memtable.put("b", "2");
                }

                @Test
                void itShouldReturnKeyDBWithSegments() {
                    final KeyDB db = KeyDB.from(path).get();
                    assertThat(db.numSegments()).isEqualTo(3);
                }

                @Test
                void itShouldLoadMemTableFromFile() {
                    final KeyDB db = KeyDB.from(path).get();

                    assertThat(db.get("a")).isEqualTo(Option.of("1"));
                    assertThat(db.get("b")).isEqualTo(Option.of("2"));
                    assertThat(db.get("c")).isEqualTo(Option.none());
                }
            }
        }
    }

}
