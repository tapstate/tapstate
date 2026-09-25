package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Reads instance-wide Mongo command counters around one measured fork window. The process under test
 * needs no instrumentation, so the same adapter can observe either application JAR. The delta also
 * includes harness traffic and any other client of the Mongo instance; forks must run serially on the
 * dedicated test replica set for a comparison to be meaningful.
 */
final class BenchmarkMongoCommandSampler implements AutoCloseable {

    enum Family {
        READ,
        WRITE,
        SCHEMA,
        TRANSACTION,
        OTHER
    }

    private static final Set<String> READ = Set.of(
            "find", "aggregate", "getMore", "count", "distinct", "listCollections", "listIndexes");
    private static final Set<String> WRITE = Set.of(
            "insert", "update", "delete", "findAndModify", "bulkWrite");
    private static final Set<String> SCHEMA = Set.of(
            "create", "createIndexes", "drop", "dropDatabase", "dropIndexes", "collMod",
            "renameCollection");
    private static final Set<String> TRANSACTION = Set.of("commitTransaction", "abortTransaction");

    record Snapshot(String host, long pid, long uptimeMillis, Map<String, Long> commandTotals) {
        Snapshot {
            commandTotals = Map.copyOf(commandTotals);
        }
    }

    record Summary(Map<String, Long> byCommand, Map<Family, Long> byFamily, long elapsedMillis) {
        Summary {
            byCommand = Map.copyOf(byCommand);
            byFamily = Map.copyOf(byFamily);
        }

        long totalCommands() {
            return byCommand.values().stream().mapToLong(Long::longValue).sum();
        }
    }

    private final MongoClient client;
    private Snapshot beginning;
    private long startedAtNanos;
    private boolean finished;
    private boolean closed;

    private BenchmarkMongoCommandSampler(MongoClient client) {
        this.client = client;
    }

    static BenchmarkMongoCommandSampler open(String mongoUri) {
        Objects.requireNonNull(mongoUri, "Mongo URI");
        return new BenchmarkMongoCommandSampler(MongoClients.create(new ConnectionString(mongoUri)));
    }

    /** Read the baseline after all fork setup and before issuing measured source SQL. */
    synchronized void start() {
        if (beginning != null || finished || closed) {
            throw new IllegalStateException("Mongo command sampler can start only once");
        }
        beginning = read();
        startedAtNanos = System.nanoTime();
    }

    /** Read the end counter after target ACK and its change-stream barrier. */
    synchronized Summary finish() {
        if (beginning == null || finished || closed) {
            throw new IllegalStateException("Mongo command sampler has no open measured window");
        }
        long endedAtNanos = System.nanoTime();
        long elapsedNanos = endedAtNanos - startedAtNanos;
        if (elapsedNanos < 0) {
            throw new AssertionError("Mongo command sample clock moved backward");
        }
        finished = true;
        Snapshot end = read();
        return difference(beginning, end, elapsedNanos / 1_000_000);
    }

    private Snapshot read() {
        try {
            Document status = client.getDatabase("admin").runCommand(new Document("serverStatus", 1));
            return parse(status);
        } catch (RuntimeException failure) {
            throw new AssertionError("Mongo command counters are unavailable", failure);
        }
    }

    static Snapshot parse(Document status) {
        if (status == null || !(status.get("host") instanceof String host) || host.isBlank()
                || !(status.get("pid") instanceof Number pid)
                || !(status.get("uptimeMillis") instanceof Number uptime)
                || !(status.get("metrics") instanceof Document metrics)
                || !(metrics.get("commands") instanceof Document commands)) {
            throw new AssertionError("serverStatus has no complete command-counter header");
        }
        if (pid.longValue() < 0 || uptime.longValue() < 0
                || pid.doubleValue() != pid.longValue()
                || uptime.doubleValue() != uptime.longValue()) {
            throw new AssertionError("serverStatus has invalid process identity or uptime");
        }
        Map<String, Long> totals = new HashMap<>();
        commands.forEach((name, value) -> {
            Number total;
            if (value instanceof Document command && command.get("total") instanceof Number count) {
                total = count;
            } else if ("<UNKNOWN>".equals(name) && value instanceof Number count) {
                // Mongo 7 reports this one catch-all command counter as a scalar.
                total = count;
            } else {
                throw new AssertionError("serverStatus command counter is unavailable: " + name
                        + " = " + value);
            }
            if (total.longValue() < 0 || total.doubleValue() != total.longValue()) {
                throw new AssertionError("serverStatus command counter is invalid: " + name
                        + " = " + value);
            }
            totals.put(name, total.longValue());
        });
        if (totals.isEmpty()) {
            throw new AssertionError("serverStatus returned no command counters");
        }
        return new Snapshot(host, pid.longValue(), uptime.longValue(), totals);
    }

    static Summary difference(Snapshot before, Snapshot after, long elapsedMillis) {
        Objects.requireNonNull(before, "before command snapshot");
        Objects.requireNonNull(after, "after command snapshot");
        if (!before.host().equals(after.host()) || before.pid() != after.pid()
                || after.uptimeMillis() < before.uptimeMillis() || elapsedMillis < 0) {
            throw new AssertionError("Mongo command counters crossed a server restart or invalid window");
        }
        Map<String, Long> commands = new TreeMap<>();
        Map<Family, Long> families = new EnumMap<>(Family.class);
        for (Family family : Family.values()) {
            families.put(family, 0L);
        }
        for (Map.Entry<String, Long> entry : before.commandTotals().entrySet()) {
            if (!after.commandTotals().containsKey(entry.getKey())) {
                throw new AssertionError("Mongo command counter disappeared: " + entry.getKey());
            }
        }
        for (Map.Entry<String, Long> entry : after.commandTotals().entrySet()) {
            String name = entry.getKey();
            long first = before.commandTotals().getOrDefault(name, 0L);
            long delta = entry.getValue() - first;
            if (delta < 0) {
                throw new AssertionError("Mongo command counter moved backward: " + name);
            }
            // Both reads issue serverStatus; do not charge the adapter's own command to the fork.
            if (delta == 0 || "serverStatus".equals(name)) {
                continue;
            }
            commands.put(name, delta);
            Family family = familyOf(name);
            families.put(family, Math.addExact(families.get(family), delta));
        }
        return new Summary(commands, families, elapsedMillis);
    }

    private static Family familyOf(String command) {
        if (READ.contains(command)) {
            return Family.READ;
        }
        if (WRITE.contains(command)) {
            return Family.WRITE;
        }
        if (SCHEMA.contains(command)) {
            return Family.SCHEMA;
        }
        return TRANSACTION.contains(command) ? Family.TRANSACTION : Family.OTHER;
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            client.close();
        }
    }
}
