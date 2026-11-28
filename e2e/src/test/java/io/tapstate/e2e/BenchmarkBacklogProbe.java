package io.tapstate.e2e;

import io.tapstate.e2e.BenchmarkLatencyProbe.Confirmation;
import io.tapstate.e2e.BenchmarkLatencyProbe.Event;
import io.tapstate.e2e.benchmark.MongoWorkloadGenerator.PhaseReport;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.bson.BsonDocument;

/** Verifies the bounded source history and successful writer callbacks of supported backlog cases. */
final class BenchmarkBacklogProbe implements AutoCloseable {
    static final String PROTOCOL = "mongo-backlog-pdk-ack-v1";
    private final Path source;
    private final Predicate<Event> expectsWrite;
    private boolean started;

    BenchmarkBacklogProbe(Path source, boolean filtersOddKeys) {
        this.source = source;
        expectsWrite = event -> !filtersOddKeys || BenchmarkOperatorOracle.evenKey(event.key());
    }

    void start() {
        try {
            Files.createFile(sibling(".active"));
            started = true;
        } catch (IOException unavailable) {
            started = false;
        }
    }

    @Override
    public void close() {
        try {
            Files.deleteIfExists(sibling(".active"));
        } catch (IOException unavailable) {
            // The isolated fixture directory and OP process are cleaned up by the caller.
        }
    }

    Reading finish(PhaseReport batch, long startEpochMillis, long startNanos) throws IOException {
        close();
        long observedEpochMillis = System.currentTimeMillis();
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        // Both JVMs use this host's wall clock. The fixture also checks it against its monotonic clock.
        if (!started || Math.abs(observedEpochMillis - startEpochMillis - elapsedMillis) > 10) {
            throw new IllegalArgumentException("The witness did not start or the fixture clock changed during catch-up");
        }
        var records = new ArrayList<SourceRecord>();
        for (String line : Files.readAllLines(source)) {
            String[] fields = line.split("\t", -1);
            if (fields.length != 6) {
                throw new IllegalArgumentException("Unrecognized source backlog evidence");
            }
            records.add(new SourceRecord(fields[3], BenchmarkLatencyProbe.event(fields),
                    BsonDocument.parse(fields[4]), Long.parseLong(fields[5])));
        }
        return verify(batch, records, BenchmarkLatencyProbe.read(sibling(".ack"), true),
                startEpochMillis, observedEpochMillis, expectsWrite);
    }

    record SourceRecord(String stream, Event event, BsonDocument token, long readEpochMillis) {}
    record SourceTail(int events, BsonDocument token, long readEpochMillis) {}
    record Reading(long durationMillis, long startEpochMillis, long targetAckEpochMillis,
                   Map<String, SourceTail> sourceTails, int sourceEvents, int confirmedWrites) {}

    static Reading verify(PhaseReport batch, List<SourceRecord> sourceRecords, List<Confirmation> confirmations,
            long startEpochMillis, long observedEpochMillis, Predicate<Event> expectsWrite) {
        if (!batch.complete() || batch.streams().isEmpty()) {
            throw new IllegalArgumentException("The fixed source batch is incomplete");
        }
        var recordsByStream = new HashMap<String, List<SourceRecord>>();
        var readTimes = new HashMap<Event, Long>();
        var tokens = new HashSet<BsonDocument>();
        for (var record : sourceRecords) {
            if (record.readEpochMillis() < startEpochMillis || record.readEpochMillis() > observedEpochMillis
                    || readTimes.putIfAbsent(record.event(), record.readEpochMillis()) != null
                    || !tokens.add(record.token())) {
                throw new IllegalArgumentException("A source observation was out of window, ambiguous or replayed");
            }
            recordsByStream.computeIfAbsent(record.stream(), ignored -> new ArrayList<>()).add(record);
        }
        if (!recordsByStream.keySet().equals(batch.streams().keySet())) {
            throw new IllegalArgumentException("The connector did not observe exactly the declared source streams");
        }
        var sourceTails = new LinkedHashMap<String, SourceTail>();
        for (var input : batch.streams().entrySet()) {
            var report = input.getValue();
            var records = recordsByStream.get(input.getKey());
            var events = records.stream().map(record -> new Confirmation(record.event(), 0)).toList();
            var first = records.getFirst();
            var last = records.getLast();
            if (report.first() == null || report.last() == null || records.size() != report.observed().total()
                    || !report.first().resumeToken().equals(first.token())
                    || !report.last().resumeToken().equals(last.token())
                    || !BenchmarkLatencyProbe.digest(events).equals(report.sourceClockSequenceSha256())) {
                throw new IllegalArgumentException("Connector history did not match the independently observed batch for "
                        + input.getKey());
            }
            sourceTails.put(input.getKey(), new SourceTail(records.size(), last.token(), last.readEpochMillis()));
        }

        var confirmed = new HashSet<Event>();
        long lastAck = startEpochMillis;
        for (var confirmation : confirmations) {
            Long readTime = readTimes.get(confirmation.event());
            if (readTime == null || !expectsWrite.test(confirmation.event()) || !confirmed.add(confirmation.event())
                    || confirmation.ackMillis() < readTime || confirmation.ackMillis() > observedEpochMillis) {
                throw new IllegalArgumentException("A target confirmation was out of window, ambiguous or outside the batch");
            }
            lastAck = Math.max(lastAck, confirmation.ackMillis());
        }
        long requiredWrites = readTimes.keySet().stream().filter(expectsWrite).count();
        if (confirmed.size() != requiredWrites || lastAck <= startEpochMillis) {
            throw new IllegalArgumentException("The target did not confirm the entire batch");
        }
        long lastRead = sourceTails.values().stream().mapToLong(SourceTail::readEpochMillis).max().orElseThrow();
        return new Reading(Math.max(lastRead, lastAck) - startEpochMillis, startEpochMillis, lastAck,
                Map.copyOf(sourceTails), sourceRecords.size(), confirmed.size());
    }

    private Path sibling(String suffix) {
        return source.resolveSibling(source.getFileName() + suffix);
    }
}
