package util;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.nio.charset.StandardCharsets;

public class BloomFilterUtil {
    private final BloomFilter<String> bloomFilter;

    public BloomFilterUtil(long expectedInsertions, double fpp) {
        this.bloomFilter = BloomFilter.create(
                Funnels.stringFunnel(StandardCharsets.UTF_8),
                expectedInsertions,
                fpp
        );
    }

    public void add(String key) {
        bloomFilter.put(key);
    }

    public boolean mightContain(String key) {
        return bloomFilter.mightContain(key);
    }
}