package lsm;

import core.DB;
import org.openjdk.jmh.annotations.*;

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

    @Benchmark
    public void put() throws IOException {
        byte[] keyBytes = new byte[16];
        ThreadLocalRandom.current().nextBytes(keyBytes);
        String keyStr = Base64.getEncoder().encodeToString(keyBytes);

        byte[] valBytes = new byte[1024];
        ThreadLocalRandom.current().nextBytes(valBytes);
        String valStr = Base64.getEncoder().encodeToString(valBytes);

        db.put(keyStr, valStr);
    }
}
