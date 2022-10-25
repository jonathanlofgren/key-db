package keydb;

import org.apache.commons.io.FileUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class ReadBenchmarks {

    private static final Path TEST_DB_PATH = Path.of("testdb_read");
    private static final Boolean CLEAR_TEST_DB = false;
    private static final int HUNDRED_THOUSAND = 100_000;

    public KeyDB db;
    public Set<String> data;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        if (Files.isDirectory(TEST_DB_PATH)) {
            FileUtils.deleteDirectory(TEST_DB_PATH.toFile());
        }
        db = KeyDB.create(TEST_DB_PATH);

        // Bytes: 100_000 * (14 * 2 + 36 * 2) = 10_000_000 = 10 MB (5 segments)
        data = new HashSet<>();
        for (int i = 0; i < HUNDRED_THOUSAND; i++) {
            final String key = UUID.randomUUID().toString().substring(0, 14);
            final String value = UUID.randomUUID().toString();
            db.put(key, value);
            data.add(key);
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() throws IOException {
        if (CLEAR_TEST_DB) {
            FileUtils.deleteDirectory(TEST_DB_PATH.toFile());
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void getHundredThousandKeys(final WriteBenchmarks bench, final Blackhole bh) {
        for (final String key: data) {
            bh.consume(db.get(key));
        }
    }
}
