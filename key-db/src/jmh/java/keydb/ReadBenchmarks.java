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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@State(Scope.Benchmark)
public class ReadBenchmarks extends BenchmarkBase {

    private static final Path TEST_DB_PATH = Path.of("testdb_read");
    private static final Boolean CLEAR_TEST_DB = false;
    private static final int HUNDRED_THOUSAND = 100_000;

    public KeyDB db;
    public HashMap<String, String> data;
    public Set<String> missingKeys;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        if (Files.isDirectory(TEST_DB_PATH)) {
            FileUtils.deleteDirectory(TEST_DB_PATH.toFile());
        }
        db = KeyDB.create(TEST_DB_PATH);
        // Bytes: 100_000 * 50 = 5_000_000 = 5 MB (~2.5 segments)
        data = generateData(HUNDRED_THOUSAND);
        populateDatabase(db, data);
        missingKeys = new HashSet<>(generateData(HUNDRED_THOUSAND).values());
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
    public void getHundredThousandExistingKeys(final WriteBenchmarks bench, final Blackhole bh) {
        for (final String key: data.keySet()) {
            bh.consume(db.get(key));
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void getHundredThousandMissingKeys(final WriteBenchmarks bench, final Blackhole bh) {
        for (final String key: missingKeys) {
            bh.consume(db.get(key));
        }
    }
}
