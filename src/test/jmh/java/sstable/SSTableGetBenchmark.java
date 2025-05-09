package sstable;

import core.DB;
import lsm.LSMGetBenchmark;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

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
public class SSTableGetBenchmark {
    @Param({"1000000"})
    public int keyCount;

    public byte[][] keys;
    public byte[][] values;
    public DB db;

    @Setup(Level.Trial)
    public void setup() throws IOException, InterruptedException {
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
        // Flush the active MemTable into SSTable so SSTable contains all the entries
        db.compactionService.forceFlush();
    }

    @TearDown
    public void tearDown() throws Exception {
        db.close(); // Calls CompactionService.stop()
    }

    @Benchmark
    public void positiveGet(Blackhole bh) {
        int idx = ThreadLocalRandom.current().nextInt(keyCount);
        String keyStr = Base64.getEncoder().encodeToString(keys[idx]);
        String result = db.sstableService.get(keyStr);
        bh.consume(result);  // prevent JVM from optimizing away
    }

    @Benchmark
    public void negativeGet(Blackhole bh) {
        int idx = ThreadLocalRandom.current().nextInt(keyCount);
        byte[] base = keys[idx];
        byte[] miss = base.clone();
        miss[0] ^= 0xFF;

        String missingKey = Base64.getEncoder().encodeToString(miss);
        String result = db.sstableService.get(missingKey);  // should return null
        bh.consume(result);
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(SSTableGetBenchmark.class.getSimpleName())
                .forks(1)
                .jvmArgs("-Xms7g", "-Xmx7g")
                .build();

        new Runner(opt).run();
    }
}
