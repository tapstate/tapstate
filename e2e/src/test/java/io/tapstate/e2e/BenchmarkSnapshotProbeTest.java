package io.tapstate.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class BenchmarkSnapshotProbeTest {
    private static final Set<Long> SOURCE_KEYS = Set.of(10L, 11L);

    @Test
    void takesTheLastConfirmedSourceKeyRegardlessOfCallbackOrder() {
        var reading = BenchmarkSnapshotProbe.verify(SOURCE_KEYS, List.of("10\t1020", "11\t1010"), 1000, 1100);
        assertEquals(20, reading.durationMillis());
        assertEquals(1020, reading.finalAckEpochMillis());
        assertEquals(2, reading.confirmed());
    }

    @Test
    void requiresEveryExpectedKeyExactlyOnceWithinTheWindow() {
        assertThrows(IllegalArgumentException.class, () -> BenchmarkSnapshotProbe.verify(
                SOURCE_KEYS, List.of("10\t1020"), 1000, 1100));
        assertThrows(IllegalArgumentException.class, () -> BenchmarkSnapshotProbe.verify(
                SOURCE_KEYS, List.of("10\t1020", "10\t1020"), 1000, 1100));
        assertThrows(IllegalArgumentException.class, () -> BenchmarkSnapshotProbe.verify(
                SOURCE_KEYS, List.of("10\t1020", "100\t1020"), 1000, 1100));
        assertThrows(IllegalArgumentException.class, () -> BenchmarkSnapshotProbe.verify(
                SOURCE_KEYS, List.of("10\t999", "11\t1020"), 1000, 1100));
    }
}
