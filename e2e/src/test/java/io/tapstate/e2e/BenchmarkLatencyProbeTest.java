package io.tapstate.e2e;

import static io.tapstate.e2e.BenchmarkLatencyProbe.correlate;
import static io.tapstate.e2e.BenchmarkLatencyProbe.digest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.mongodb.client.MongoDatabase;
import io.tapstate.e2e.BenchmarkLatencyProbe.Confirmation;
import io.tapstate.e2e.BenchmarkLatencyProbe.Event;
import io.tapstate.e2e.benchmark.MongoWorkloadGenerator.Live;
import io.tapstate.e2e.benchmark.MongoWorkloadGenerator.Operations;
import io.tapstate.e2e.benchmark.MongoWorkloadGenerator.PhaseReport;
import io.tapstate.e2e.benchmark.MongoWorkloadGenerator.Position;
import io.tapstate.e2e.benchmark.MongoWorkloadGenerator.StreamReport;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
        var duplicateAcks = List.of(duplicate, duplicate);
        var unmatchedAck = List.of(new Confirmation(new Event(1, 6, 2_000), 2_100));
        var earlyAck = List.of(new Confirmation(UPDATE, 1_999));
        List<Confirmation> noAcks = List.of();
        var reordered = window(observed(DELETE, UPDATE, INSERT));
        var duplicateSources = observed(WARMUP, UPDATE, UPDATE);
        var duplicateScored = window(observed(UPDATE, UPDATE));
        var misorderedWarmup = new Event(1, 5, 3_000);
        var misorderedSources = observed(misorderedWarmup, UPDATE, DELETE, INSERT);
        var misorderedWindow = window(observed(misorderedWarmup));

        assertThrows(IllegalArgumentException.class, () -> correlate(SOURCES, duplicateAcks, warmup, scored, 2_000, 3_000, 2_600));
        assertThrows(IllegalArgumentException.class, () -> correlate(SOURCES, unmatchedAck, warmup, scored, 2_000, 3_000, 2_600));
        assertThrows(IllegalArgumentException.class, () -> correlate(SOURCES, noAcks, warmup, reordered, 2_000, 3_000, 2_600));
        assertThrows(IllegalArgumentException.class, () -> correlate(duplicateSources, noAcks, warmup, duplicateScored, 2_000, 3_000, 2_600));
        assertThrows(IllegalArgumentException.class, () -> correlate(misorderedSources, noAcks, misorderedWindow, scored, 2_000, 3_000, 2_600));
        assertThrows(IllegalArgumentException.class, () -> correlate(SOURCES, noAcks, warmup, scored, 1_500, 2_000, 2_600));
        assertThrows(IllegalArgumentException.class, () -> correlate(SOURCES, earlyAck, warmup, scored, 2_000, 3_000, 2_600));
    }

    @Test
    void scoresSourceHistoryAcrossFixtureClockBoundaries(@TempDir Path directory) throws Exception {
        var warm = new Event(0, 1, 1_000);
        var first = new Event(0, 2, 1_000);
        var last = new Event(0, 3, 3_000);
        var warmup = phase(1_800, 1_950, warm);
        var measured = phase(2_005, 2_990, first, last);
        var live = new Live(null, null, null, null, warmup, measured);
        Path witness = directory.resolve("copy");
        Files.writeString(witness, "0\t1\t1000\n0\t2\t1000\n0\t3\t3000\n");
        Files.writeString(witness.resolveSibling("copy.ack"), "0\t1\t1000\t1960\n0\t2\t1000\t2200\n0\t3\t3000\t3100\n");
        var mongo = (MongoDatabase) Proxy.newProxyInstance(MongoDatabase.class.getClassLoader(),
                new Class<?>[]{MongoDatabase.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "withTimeout" -> proxy;
                    case "runCommand" -> new Document("localTime", new Date());
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        try (var probe = new BenchmarkLatencyProbe(witness, mongo)) {
            probe.start();
            var result = probe.finish(live, "records", 2, 100, 1, Duration.ofSeconds(2));
            assertNull(result.reason());
            assertEquals(1_000L, result.startEpochMillis());
            assertEquals(4_000L, result.endExclusiveEpochMillis());
            assertEquals(1_200_000_000L, result.reading().p95Nanos());
        }
    }

    private static PhaseReport phase(long start, long end, Event... events) {
        var entries = observed(events);
        var count = new Operations(entries.size(), 0, 0);
        var first = new Position(null, new BsonTimestamp((int) (events[0].epochMillis() / 1000), 1));
        var last = new Position(null, new BsonTimestamp((int) (events[events.length - 1].epochMillis() / 1000), 1));
        var stream = new StreamReport(count, count, count, null, first, last, null, events.length, 0,
                null, digest(entries), null);
        return new PhaseReport("fixture", Instant.ofEpochMilli(start), Instant.ofEpochMilli(end), 0, 0,
                null, null, null, false, 0, 0, null, Map.of("records", stream), true);
    }

    private static List<Confirmation> observed(Event... events) {
        return java.util.Arrays.stream(events).map(event -> new Confirmation(event, 0)).toList();
    }

    private static StreamReport window(List<Confirmation> events) {
        var count = new Operations(events.size(), 0, 0);
        return new StreamReport(count, count, count, null, null, null, null, events.size(), 0, null, digest(events), null);
    }
}
