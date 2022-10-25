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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class WriteBenchmarks extends BenchmarkBase {

    private static final Path TEST_DB_PATH = Path.of("testdb_write");
    private static final Boolean CLEAR_TEST_DB = false;
    private static final int TEN_THOUSAND = 10_000;

    public KeyDB db;
    public HashMap<String, String> data;

    @Setup(Level.Iteration)
    public void setUp() throws IOException {
        if (Files.isDirectory(TEST_DB_PATH)) {
            FileUtils.deleteDirectory(TEST_DB_PATH.toFile());
        }
        db = KeyDB.create(TEST_DB_PATH);
        data = generateData(TEN_THOUSAND);
    }

    @TearDown(Level.Iteration)
    public void tearDown() throws IOException {
        if (CLEAR_TEST_DB) {
            FileUtils.deleteDirectory(TEST_DB_PATH.toFile());
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void write1MegaByte(final WriteBenchmarks bench, final Blackhole bh) {
        for (int i = 0; i < TEN_THOUSAND; i++) {
            bench.db.put(UUID.randomUUID().toString().substring(0, 14), UUID.randomUUID().toString());
        }
    }
}
