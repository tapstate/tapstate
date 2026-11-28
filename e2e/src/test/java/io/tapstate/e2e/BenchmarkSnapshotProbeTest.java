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

    @Test
    void filteredTailCanFinishAfterTheFinalWrite() {
        var reading = BenchmarkSnapshotProbe.verifyFiltered(
                SOURCE_KEYS, List.of("10\t1010", "11\t1050"), List.of("10\t1030"), 1000, 1100);
        assertEquals(50, reading.durationMillis());
        assertEquals(1050, reading.finalReadEpochMillis());
        assertEquals(1030, reading.finalAckEpochMillis());
        assertEquals(1, reading.confirmed());
    }

    @Test
    void rejectsFilteredSnapshotWithMissingInputOrInvalidOutput() {
        var reads = List.of("10\t1010", "11\t1050");
        assertThrows(IllegalArgumentException.class, () -> BenchmarkSnapshotProbe.verifyFiltered(
                SOURCE_KEYS, List.of("10\t1010"), List.of("10\t1030"), 1000, 1100));
        assertThrows(IllegalArgumentException.class, () -> BenchmarkSnapshotProbe.verifyFiltered(
                SOURCE_KEYS, reads, List.of("10\t1030", "11\t1060"), 1000, 1100));
        assertThrows(IllegalArgumentException.class, () -> BenchmarkSnapshotProbe.verifyFiltered(
                SOURCE_KEYS, reads, List.of("10\t1005"), 1000, 1100));
    }

    @Test
    void identifiesExpandedWritesByParentAndElement() {
        var sources = List.of("10\t1010", "11\t1050");
        var outputs = Set.of("10/0", "10/1", "11/0");
        var reading = BenchmarkSnapshotProbe.verifyUnwind(SOURCE_KEYS, outputs, sources,
                List.of("10/0\t1020", "10/1\t1030", "11/0\t1065"), 1000, 1100);
        assertEquals(65, reading.durationMillis());
        assertEquals(1050, reading.finalReadEpochMillis());
        assertEquals(1065, reading.finalAckEpochMillis());
        assertEquals(3, reading.confirmed());
        assertThrows(IllegalArgumentException.class, () -> BenchmarkSnapshotProbe.verifyUnwind(
                SOURCE_KEYS, outputs, sources, List.of("10/0\t1020", "10/1\t1030", "11/2\t1065"), 1000, 1100));
    }
}
