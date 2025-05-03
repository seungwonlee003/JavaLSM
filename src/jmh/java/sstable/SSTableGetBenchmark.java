package sstable;

import core.DB;
import org.openjdk.jmh.annotations.*;
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
        values = new byte[keyCount][1024];

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

    @Benchmark
    public String positiveGet() {
        int idx = ThreadLocalRandom.current().nextInt(keyCount);
        String keyStr = Base64.getEncoder().encodeToString(keys[idx]);
        return db.sstableService.get(keyStr);
    }

    @Benchmark
    public String negativeGet() {
        int idx = ThreadLocalRandom.current().nextInt(keyCount);
        byte[] base = keys[idx];
        byte[] miss = base.clone();
        miss[0] ^= 0xFF;

        String missingKey = Base64.getEncoder().encodeToString(miss);
        return db.sstableService.get(missingKey);  // should return null
    }
}
