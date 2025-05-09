package lsm;

import core.DB;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.Base64;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class LSMPutBenchmark {

    public DB db;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        db = new DB();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        db.close();
    }

    @Benchmark
    public void put() throws IOException {
        byte[] keyBytes = new byte[16];
        ThreadLocalRandom.current().nextBytes(keyBytes);
        String keyStr = Base64.getEncoder().encodeToString(keyBytes);

        byte[] valBytes = new byte[100];
        ThreadLocalRandom.current().nextBytes(valBytes);
        String valStr = Base64.getEncoder().encodeToString(valBytes);

        db.put(keyStr, valStr);
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(LSMPutBenchmark.class.getSimpleName()) // Fixed: Use correct class name
                .forks(1)
                .jvmArgs("-Xmx2g", "-Xms2g", "-XX:+UseG1GC") // 2GB heap, G1GC for better GC
                .build();
        new Runner(opt).run();
    }
}
