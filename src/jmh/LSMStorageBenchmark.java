package jmh;

import db.DB;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class LSMStorageBenchmark {
    private DB engine;
    // 1 million entries
    private final int numEntries = 1_000_000;
    private Random random;

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    @Setup
    public void setup() throws IOException {
        engine = new DB();
        random = new Random(12345); // fixed seed for reproducibility
        for (int i = 0; i < numEntries; i++) {
            engine.put("key" + i, "value" + i);
        }
    }

    @Benchmark
    public void testPut() {
        int i = random.nextInt(numEntries);
        engine.put("key" + i, "newValue" + i);
    }

    @Benchmark
    public String testGet() {
        int i = random.nextInt(numEntries);
        return engine.get("key" + i);
    }
}
