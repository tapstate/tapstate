package io.tapstate.e2e;

import com.mongodb.MongoException;
import com.mongodb.client.MongoDatabase;
import io.tapstate.e2e.benchmark.MongoWorkloadGenerator.Live;
import io.tapstate.e2e.benchmark.MongoWorkloadGenerator.StreamReport;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.bson.Document;

/** Reconciles the one-input, one-output Copy witness with independent MongoDB change history. */
final class BenchmarkLatencyProbe implements AutoCloseable {
    static final String PROTOCOL = "mongo-copy-pdk-ack-v1";
    private static final long CLOCK_ALLOWANCE_MILLIS = 100;
    private final Path source;
    private final MongoDatabase mongo;
    private boolean initialClock;

    BenchmarkLatencyProbe(Path source, MongoDatabase mongo) {
        this.source = source;
        this.mongo = mongo;
    }

    void start() {
        initialClock = clockMatches();
        try {
            Files.createFile(path(".active"));
        } catch (IOException unavailable) {
            initialClock = false;
        }
    }

    @Override
    public void close() {
        try {
            Files.deleteIfExists(path(".active"));
        } catch (IOException unavailable) {
            // The fixture directory is isolated and its OP process is already stopping.
        }
    }

    // The unchanged MongoDB connector stamps the event's cluster-time second. Leave an
    // short gap to reduce ambiguous repetitions of the same operation and key.
    void awaitScoringBoundary() {
        long wait = 1000 - System.currentTimeMillis() % 1000;
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(wait);
        long remaining;
        while ((remaining = end - System.nanoTime()) > 0 && !Thread.currentThread().isInterrupted()) {
            LockSupport.parkNanos(remaining);
        }
    }

    record Result(LiveLatencyEligibility.Reading reading, String reason, Long startEpochMillis, Long endExclusiveEpochMillis) {}

    Result finish(Live live, String stream, int minimumP95, int minimumP99, double coverage, Duration drain) {
        var measured = live.measured().streams().get(stream);
        int expected = measured.observed().total();
        try {
            Files.deleteIfExists(path(".active"));
            if (!initialClock || !clockMatches()) {
                return insufficient(expected, minimumP95, coverage, "MongoDB and fixture clocks did not agree");
            }
            var warmup = live.warmup().streams().get(stream);
            if (measured.first() == null || measured.last() == null) {
                return insufficient(expected, minimumP95, coverage, "The measured source history has no event timestamps");
            }
            long start = measured.first().clusterTime().getTime() * 1000L;
            long finish = (measured.last().clusterTime().getTime() + 1L) * 1000;
            long end = live.measured().writesFinishedAt().plus(drain).toEpochMilli();
            List<Confirmation> observed = read(source, false);
            List<Confirmation> confirmed = read(path(".ack"), true);
            List<LiveLatencyEligibility.Sample> samples = correlate(observed, confirmed, warmup, measured, start, finish, end);
            return new Result(LiveLatencyEligibility.assess(expected, samples, true, minimumP95, minimumP99, coverage), null, start, finish);
        } catch (IOException | IllegalArgumentException unavailable) {
            return insufficient(expected, minimumP95, coverage, unavailable.getMessage());
        }
    }

    private boolean clockMatches() {
        long before = System.currentTimeMillis();
        try {
            var localTime = mongo.withTimeout(2, TimeUnit.SECONDS).runCommand(new Document("hello", 1)).getDate("localTime");
            long after = System.currentTimeMillis();
            return localTime != null && localTime.getTime() >= before - CLOCK_ALLOWANCE_MILLIS
                    && localTime.getTime() <= after + CLOCK_ALLOWANCE_MILLIS;
        } catch (MongoException unavailable) {
            return false;
        }
    }

    private static Result insufficient(int expected, int minimum, double coverage, String reason) {
        return new Result(LiveLatencyEligibility.assess(expected, List.of(), false, minimum, coverage), reason, null, null);
    }

    record Event(int operation, long key, long epochMillis) {}
    record Confirmation(Event event, long ackMillis) {}

    static List<Confirmation> read(Path path, boolean ack) throws IOException {
        var parsed = new ArrayList<Confirmation>();
        for (String line : Files.readAllLines(path)) {
            String[] fields = line.split("\t", -1);
            if (fields.length != (ack ? 4 : 3)) throw new IllegalArgumentException("Unrecognized probe evidence");
            int operation = Integer.parseInt(fields[0]);
            long epochMillis = Long.parseLong(fields[2]);
            if (operation < 0 || operation > 2 || epochMillis <= 0 || epochMillis % 1000 != 0) {
                throw new IllegalArgumentException("Unexpected MongoDB connector event time or operation");
            }
            parsed.add(new Confirmation(new Event(operation, Long.parseLong(fields[1]), epochMillis),
                    ack ? Long.parseLong(fields[3]) : 0));
        }
        return parsed;
    }

    static List<LiveLatencyEligibility.Sample> correlate(List<Confirmation> source, List<Confirmation> target,
            StreamReport warmup, StreamReport measured, long scoringStart, long scoringEnd, long drainEnd) {
        int warmEvents = warmup == null ? 0 : warmup.observed().total();
        int scoredEvents = measured.observed().total();
        if (source.size() != warmEvents + scoredEvents
                || (warmEvents > 0 && !digest(source.subList(0, warmEvents)).equals(warmup.sourceClockSequenceSha256()))
                || !digest(source.subList(warmEvents, source.size())).equals(measured.sourceClockSequenceSha256())) {
            throw new IllegalArgumentException("Connector events did not match independently observed source windows");
        }
        var distinct = new HashSet<Event>();
        for (int index = 0; index < source.size(); index++) {
            Event event = source.get(index).event();
            // Independent sequence digests disambiguate cohorts sharing the boundary second.
            boolean invalidTime = index < warmEvents
                    ? event.epochMillis() > scoringStart
                    : event.epochMillis() < scoringStart || event.epochMillis() >= scoringEnd;
            if (invalidTime) {
                throw new IllegalArgumentException("The independent source cohort did not match the connector's time window");
            }
            if (!distinct.add(event)) {
                throw new IllegalArgumentException("A source identity is ambiguous or replayed");
            }
        }
        Map<Event, Long> acknowledgements = new HashMap<>();
        for (var targetAck : target) {
            Event event = targetAck.event();
            if (!distinct.contains(event) || acknowledgements.putIfAbsent(event, targetAck.ackMillis()) != null) {
                throw new IllegalArgumentException("A target acknowledgement has no unique source event");
            }
            if (targetAck.ackMillis() < event.epochMillis()) {
                throw new IllegalArgumentException("A target acknowledgement preceded its source timestamp");
            }
        }
        var samples = new ArrayList<LiveLatencyEligibility.Sample>();
        for (int index = warmEvents; index < source.size(); index++) {
            Event event = source.get(index).event();
            Long ack = acknowledgements.get(event);
            if (ack != null && ack <= drainEnd) {
                samples.add(new LiveLatencyEligibility.Sample(Integer.toString(index),
                        TimeUnit.MILLISECONDS.toNanos(ack - event.epochMillis())));
            }
        }
        return samples;
    }

    static String digest(List<Confirmation> events) {
        MessageDigest sha;
        try {
            sha = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
        var bytes = ByteBuffer.allocate(17);
        for (var event : events) {
            bytes.clear().put((byte) event.event().operation()).putLong(event.event().key())
                    .putLong(event.event().epochMillis());
            sha.update(bytes.array());
        }
        return HexFormat.of().formatHex(sha.digest());
    }

    private Path path(String suffix) {
        return source.resolveSibling(source.getFileName() + suffix);
    }
}
