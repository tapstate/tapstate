package io.tapstate.e2e;

import com.mongodb.client.MongoCollection;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

/** Measures snapshot completion from confirmation of all independently read source keys. */
final class BenchmarkSnapshotProbe implements AutoCloseable {
    static final String PROTOCOL = "mongo-copy-snapshot-pdk-ack-v1";
    static final String KEY_PRESERVING_PROTOCOL = "mongo-key-preserving-snapshot-pdk-ack-v1";
    static final String FILTER_PROTOCOL = "mongo-filter-snapshot-pdk-read-ack-v1";
    static final String UNION_PROTOCOL = "mongo-union-snapshot-pdk-ack-v1";
    static final String UNWIND_PROTOCOL = "mongo-unwind-snapshot-pdk-read-ack-v1";

    enum Mode { DIRECT, FILTERED, UNWIND }

    private final Mode mode;
    private final Path sourceTrace;
    private final Path active;
    private final Path acknowledgements;
    private boolean started;

    BenchmarkSnapshotProbe(Path witness, Mode mode) {
        this.mode = mode;
        sourceTrace = witness;
        active = witness.resolveSibling(witness.getFileName() + ".active");
        acknowledgements = witness.resolveSibling(witness.getFileName() + ".ack");
    }

    void start() {
        try {
            Files.createFile(active);
            started = true;
        } catch (IOException unavailable) {
            started = false;
        }
    }

    @Override
    public void close() {
        try {
            Files.deleteIfExists(active);
        } catch (IOException unavailable) {
            // The isolated fixture directory and OP process are cleaned up by the caller.
        }
    }

    Reading finish(List<MongoCollection<BsonDocument>> sources, Set<String> expectedTargetKeys,
            long startEpochMillis, long startNanos) throws IOException {
        close();
        long limit = System.currentTimeMillis();
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        if (!started || Math.abs(limit - startEpochMillis - elapsedMillis) > 10) {
            throw new IllegalArgumentException("The witness did not start or the fixture clock changed during the snapshot");
        }
        var expected = new HashSet<Long>();
        for (var source : sources) {
            try (var rows = source.find().projection(new BsonDocument("_id", new BsonInt32(1))).cursor()) {
                while (rows.hasNext()) {
                    if (!expected.add(rows.next().getNumber("_id").longValue())) {
                        throw new IllegalArgumentException("The snapshot sources have overlapping keys");
                    }
                }
            }
        }
        var writes = Files.readAllLines(acknowledgements);
        return switch (mode) {
            case DIRECT -> verify(expected, writes, startEpochMillis, limit);
            case FILTERED -> verifyFiltered(expected, Files.readAllLines(sourceTrace), writes, startEpochMillis, limit);
            case UNWIND -> verifyUnwind(expected, expectedTargetKeys, Files.readAllLines(sourceTrace), writes,
                    startEpochMillis, limit);
        };
    }

    record Reading(long durationMillis, long finalAckEpochMillis, int confirmed, Long finalReadEpochMillis) {}

    static Reading verify(Set<Long> expected, List<String> acknowledgements, long startEpochMillis, long limitEpochMillis) {
        var confirmed = readTrace(expected, acknowledgements, startEpochMillis, limitEpochMillis, "target write", Long::parseLong);
        long lastAck = lastTimestamp(confirmed, startEpochMillis);
        return new Reading(lastAck - startEpochMillis, lastAck, confirmed.size(), null);
    }

    static Reading verifyFiltered(Set<Long> expected, List<String> reads, List<String> writes,
            long startEpochMillis, long limitEpochMillis) {
        var source = readTrace(expected, reads, startEpochMillis, limitEpochMillis, "source read", Long::parseLong);
        var outputKeys = expected.stream().filter(BenchmarkOperatorOracle::evenKey)
                .collect(java.util.stream.Collectors.toSet());
        var target = readTrace(outputKeys, writes, startEpochMillis, limitEpochMillis, "target write", Long::parseLong);
        target.forEach((key, time) -> {
            if (time < source.get(key)) {
                throw new IllegalArgumentException("A target acknowledgement preceded its source read");
            }
        });
        long lastRead = lastTimestamp(source, startEpochMillis);
        long lastAck = lastTimestamp(target, startEpochMillis);
        return new Reading(Math.max(lastRead, lastAck) - startEpochMillis, lastAck, target.size(), lastRead);
    }

    static Reading verifyUnwind(Set<Long> expectedParents, Set<String> expectedOutputs,
            List<String> reads, List<String> writes, long startEpochMillis, long limitEpochMillis) {
        var source = readTrace(expectedParents, reads, startEpochMillis, limitEpochMillis, "source read", Long::parseLong);
        var target = readTrace(expectedOutputs, writes, startEpochMillis, limitEpochMillis, "target write", Function.identity());
        target.forEach((key, time) -> {
            long parent = Long.parseLong(key.substring(0, key.indexOf('/')));
            Long read = source.get(parent);
            if (read == null || time < read) {
                throw new IllegalArgumentException("An element acknowledgement lacks a preceding parent read");
            }
        });
        long lastRead = lastTimestamp(source, startEpochMillis);
        long lastAck = lastTimestamp(target, startEpochMillis);
        return new Reading(Math.max(lastRead, lastAck) - startEpochMillis, lastAck, target.size(), lastRead);
    }

    private static <K> Map<K, Long> readTrace(Set<K> expected, List<String> lines, long startEpochMillis,
            long limitEpochMillis, String boundary, Function<String, K> decode) {
        var timestamps = new HashMap<K, Long>();
        for (String line : lines) {
            String[] parts = line.split("\t", -1);
            if (parts.length != 2) {
                throw new IllegalArgumentException("Unrecognized snapshot " + boundary + " evidence");
            }
            K key = decode.apply(parts[0]);
            long time = Long.parseLong(parts[1]);
            if (!expected.contains(key) || timestamps.putIfAbsent(key, time) != null
                    || time < startEpochMillis || time > limitEpochMillis) {
                throw new IllegalArgumentException("A snapshot " + boundary + " was unexpected, repeated or out of window");
            }
        }
        if (timestamps.size() != expected.size()) {
            throw new IllegalArgumentException("The snapshot " + boundary + " trace did not cover every expected key");
        }
        return timestamps;
    }

    private static long lastTimestamp(Map<?, Long> timestamps, long startEpochMillis) {
        return timestamps.values().stream().mapToLong(Long::longValue).filter(time -> time > startEpochMillis).max()
                .orElseThrow(() -> new IllegalArgumentException("The snapshot trace has no completion timestamp"));
    }
}
