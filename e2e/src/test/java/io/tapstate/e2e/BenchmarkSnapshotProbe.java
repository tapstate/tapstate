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
import org.bson.BsonDocument;
import org.bson.BsonInt32;

/** Measures snapshot completion from confirmation of all independently read source keys. */
final class BenchmarkSnapshotProbe implements AutoCloseable {
    static final String PROTOCOL = "mongo-copy-snapshot-pdk-ack-v1";
    static final String KEY_PRESERVING_PROTOCOL = "mongo-key-preserving-snapshot-pdk-ack-v1";
    static final String FILTER_PROTOCOL = "mongo-filter-snapshot-pdk-read-ack-v1";
    private final Path sourceTrace;
    private final Path active;
    private final Path acknowledgements;
    private boolean started;

    BenchmarkSnapshotProbe(Path witness, boolean filtered) {
        sourceTrace = filtered ? witness : null;
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

    Reading finish(MongoCollection<BsonDocument> source, long startEpochMillis, long startNanos) throws IOException {
        close();
        long limit = System.currentTimeMillis();
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        if (!started || Math.abs(limit - startEpochMillis - elapsedMillis) > 10) {
            throw new IllegalArgumentException("The witness did not start or the fixture clock changed during the snapshot");
        }
        var expected = source.find().projection(new BsonDocument("_id", new BsonInt32(1)))
                .map(row -> row.getNumber("_id").longValue()).into(new HashSet<>());
        var writes = Files.readAllLines(acknowledgements);
        return sourceTrace == null ? verify(expected, writes, startEpochMillis, limit)
                : verifyFiltered(expected, Files.readAllLines(sourceTrace), writes, startEpochMillis, limit);
    }

    record Reading(long durationMillis, long finalAckEpochMillis, int confirmed, Long finalReadEpochMillis) {}

    static Reading verify(Set<Long> expected, List<String> acknowledgements, long startEpochMillis, long limitEpochMillis) {
        var confirmed = readTrace(expected, acknowledgements, startEpochMillis, limitEpochMillis, "target write");
        long lastAck = lastTimestamp(confirmed, startEpochMillis);
        return new Reading(lastAck - startEpochMillis, lastAck, confirmed.size(), null);
    }

    static Reading verifyFiltered(Set<Long> expected, List<String> reads, List<String> writes,
            long startEpochMillis, long limitEpochMillis) {
        var source = readTrace(expected, reads, startEpochMillis, limitEpochMillis, "source read");
        var outputKeys = expected.stream().filter(BenchmarkOperatorOracle::evenKey)
                .collect(java.util.stream.Collectors.toSet());
        var target = readTrace(outputKeys, writes, startEpochMillis, limitEpochMillis, "target write");
        target.forEach((key, time) -> {
            if (time < source.get(key)) {
                throw new IllegalArgumentException("A target acknowledgement preceded its source read");
            }
        });
        long lastRead = lastTimestamp(source, startEpochMillis);
        long lastAck = lastTimestamp(target, startEpochMillis);
        return new Reading(Math.max(lastRead, lastAck) - startEpochMillis, lastAck, target.size(), lastRead);
    }

    private static Map<Long, Long> readTrace(Set<Long> expected, List<String> lines, long startEpochMillis,
            long limitEpochMillis, String boundary) {
        var timestamps = new HashMap<Long, Long>();
        for (String line : lines) {
            String[] parts = line.split("\t", -1);
            if (parts.length != 2) {
                throw new IllegalArgumentException("Unrecognized snapshot " + boundary + " evidence");
            }
            long key = Long.parseLong(parts[0]);
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

    private static long lastTimestamp(Map<Long, Long> timestamps, long startEpochMillis) {
        return timestamps.values().stream().mapToLong(Long::longValue).filter(time -> time > startEpochMillis).max()
                .orElseThrow(() -> new IllegalArgumentException("The snapshot trace has no completion timestamp"));
    }
}
