package io.tapstate.runtime.engine.nest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two readings of a namespace's commits are the same reading when every count and every bucket agrees. The
 * buckets are arrays, and a record compares arrays by identity unless told otherwise: two identical readings
 * taken a moment apart would otherwise never be equal, and a check for "nothing moved" would always say
 * something had.
 */
class StateCommitReadingsCompareByContentTest {

    private static NestStateStats.Commits reading(long firstKeyBucket) {
        return new NestStateStats.Commits(3, new long[] {firstKeyBucket, 1}, 7, new long[] {2, 0}, 2,
                new long[] {0, 3}, 1_500, 42);
    }

    @Test
    void readingsWithTheSameBucketsAreEqualAndHashAlike() {
        assertThat(reading(2)).isEqualTo(reading(2)).hasSameHashCodeAs(reading(2));
        assertThat(reading(2).toString()).contains("keyBuckets=[2, 1]", "durationNanos=1500");
    }

    @Test
    void oneBucketApartIsADifferentReading() {
        assertThat(reading(2)).isNotEqualTo(reading(3)).isNotEqualTo("a reading");
    }
}
