package io.tapstate.e2e;

import com.mongodb.client.MongoCollection;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

/** Measures Copy snapshot completion from confirmation of all independently read source keys. */
final class BenchmarkSnapshotProbe implements AutoCloseable {
    static final String PROTOCOL = "mongo-copy-snapshot-pdk-ack-v1";
    private final Path active;
    private final Path acknowledgements;
    private boolean started;

    BenchmarkSnapshotProbe(Path witness) {
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
        return verify(expected, Files.readAllLines(acknowledgements), startEpochMillis, limit);
    }

    record Reading(long durationMillis, long finalAckEpochMillis, int confirmed) {}

    static Reading verify(Set<Long> expected, List<String> acknowledgements, long startEpochMillis, long limitEpochMillis) {
        var confirmed = new HashSet<Long>();
        long lastAck = startEpochMillis;
        for (String acknowledgement : acknowledgements) {
            String[] parts = acknowledgement.split("\t", -1);
            if (parts.length != 2) {
                throw new IllegalArgumentException("Unrecognized snapshot acknowledgement evidence");
            }
            long key = Long.parseLong(parts[0]);
            long time = Long.parseLong(parts[1]);
            if (!expected.contains(key) || !confirmed.add(key) || time < startEpochMillis || time > limitEpochMillis) {
                throw new IllegalArgumentException("A snapshot acknowledgement was unexpected, repeated or out of window");
            }
            lastAck = Math.max(lastAck, time);
        }
        if (confirmed.size() != expected.size() || lastAck <= startEpochMillis) {
            throw new IllegalArgumentException("The target did not confirm the entire snapshot");
        }
        return new Reading(lastAck - startEpochMillis, lastAck, confirmed.size());
    }
}
