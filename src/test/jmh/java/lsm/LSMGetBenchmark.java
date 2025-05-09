package lsm;

import core.DB;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import sstable.SSTableGetBenchmark;

import java.io.IOException;
import java.util.Base64;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class LSMGetBenchmark {
    @Param({"1000000"})
    public int keyCount;

    public byte[][] keys;
    public byte[][] values;
    public DB db;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        db = new DB();
        keys = new byte[keyCount][16];
        values = new byte[keyCount][100];

        Random r = new Random(12345);
        for (int i = 0; i < keyCount; i++) {
            r.nextBytes(keys[i]);
            String keyStr = Base64.getEncoder().encodeToString(keys[i]);
            r.nextBytes(values[i]);
            String valueStr = Base64.getEncoder().encodeToString(values[i]);
            db.put(keyStr, valueStr);
        }
    }

    @TearDown
    public void tearDown() throws Exception {
        db.close(); // Calls CompactionService.stop()
    }

    @Benchmark
    public void positiveGet(Blackhole bh) {
        int idx = ThreadLocalRandom.current().nextInt(keyCount);
        String keyStr = Base64.getEncoder().encodeToString(keys[idx]);
        String result = db.get(keyStr);
        bh.consume(result);
    }

    @Benchmark
    public void negativeGet(Blackhole bh) {
        // pick an existing key, then perturb so it’s guaranteed *not* in the DB
        int idx = ThreadLocalRandom.current().nextInt(keyCount);
        byte[] base = keys[idx];
        byte[] miss = base.clone();
        miss[0] ^= 0xFF; // flip one bit

        String missingKey = Base64.getEncoder().encodeToString(miss);
        String result = db.get(missingKey); // should return null
        bh.consume(result);
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(LSMGetBenchmark.class.getSimpleName())
                .forks(1)
                .jvmArgs("-Xmx2g", "-Xms2g", "-XX:+UseG1GC")
                .build();
        new Runner(opt).run();
    }
}
