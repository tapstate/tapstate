package io.tapstate.e2e;

import static io.tapstate.e2e.BenchmarkLatencyProbe.correlate;
import static io.tapstate.e2e.BenchmarkLatencyProbe.digest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.tapstate.e2e.BenchmarkLatencyProbe.Confirmation;
import io.tapstate.e2e.BenchmarkLatencyProbe.Event;
import io.tapstate.e2e.benchmark.MongoWorkloadGenerator.Operations;
import io.tapstate.e2e.benchmark.MongoWorkloadGenerator.StreamReport;
import java.util.List;
import org.junit.jupiter.api.Test;

class BenchmarkLatencyProbeTest {
    private static final Event WARMUP = new Event(1, 5, 1_000);
    private static final Event UPDATE = new Event(1, 5, 2_000);
    private static final Event DELETE = new Event(2, 5, 2_000);
    private static final Event INSERT = new Event(0, 7, 2_000);
    private static final List<Confirmation> SOURCES = observed(WARMUP, UPDATE, DELETE, INSERT);

    @Test
    void ordersSourceWindowsByIndependentHistoryAndKeepsOnlyConfirmedSamplesThroughDrain() {
        var acks = List.of(new Confirmation(INSERT, 2_601), new Confirmation(DELETE, 2_600),
                new Confirmation(WARMUP, 1_050), new Confirmation(UPDATE, 2_050));
        var actual = correlate(SOURCES, acks, window(observed(WARMUP)), window(observed(UPDATE, DELETE, INSERT)), 2_000, 3_000, 2_600);
        assertEquals(List.of(new LiveLatencyEligibility.Sample("1", 50_000_000),
                new LiveLatencyEligibility.Sample("2", 600_000_000)), actual);
    }

    @Test
    void rejectsReplaysWrongSourceWindowAndAcknowledgementsWithoutUniqueSource() {
        var scored = window(observed(UPDATE, DELETE, INSERT));
        var warmup = window(observed(WARMUP));
        var duplicate = new Confirmation(UPDATE, 2_100);
        var wrongEvent = new Confirmation(new Event(1, 6, 2_000), 2_100);
        assertThrows(IllegalArgumentException.class, () -> correlate(SOURCES, List.of(duplicate, duplicate), warmup, scored, 2_000, 3_000, 2_600));
        assertThrows(IllegalArgumentException.class, () -> correlate(SOURCES, List.of(wrongEvent), warmup, scored, 2_000, 3_000, 2_600));
        assertThrows(IllegalArgumentException.class, () -> correlate(SOURCES, List.of(), warmup,
                window(observed(DELETE, UPDATE, INSERT)), 2_000, 3_000, 2_600));
        assertThrows(IllegalArgumentException.class, () -> correlate(observed(WARMUP, UPDATE, UPDATE), List.of(),
                warmup, window(observed(UPDATE, UPDATE)), 2_000, 3_000, 2_600));
        assertThrows(IllegalArgumentException.class, () -> correlate(SOURCES, List.of(), warmup, scored,
                1_000, 3_000, 2_600));
        assertThrows(IllegalArgumentException.class, () -> correlate(SOURCES, List.of(), warmup, scored,
                1_500, 2_000, 2_600));
        assertThrows(IllegalArgumentException.class, () -> correlate(SOURCES, List.of(new Confirmation(UPDATE, 1_999)),
                warmup, scored, 2_000, 3_000, 2_600));
    }

    private static List<Confirmation> observed(Event... events) {
        return java.util.Arrays.stream(events).map(event -> new Confirmation(event, 0)).toList();
    }

    private static StreamReport window(List<Confirmation> events) {
        var count = new Operations(events.size(), 0, 0);
        return new StreamReport(count, count, count, null, null, null, null, events.size(), 0, null, digest(events), null);
    }
}
