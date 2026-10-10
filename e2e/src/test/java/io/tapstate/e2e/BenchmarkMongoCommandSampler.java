package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

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
    private final Supplier<Snapshot> source;
    private final Supplier<Snapshot> checkpointSource;
    private final LongSupplier nanoTime;
    private final ConnectionString connection;
    private Snapshot beginning;
    private long startedAtNanos;
    private boolean finished;
    private boolean closed;
    private boolean checkpointRequested;
    private ExecutorService checkpointWorker;
    private CheckpointWaiter checkpointWaiter = Future::get;
    private volatile CheckpointRead retainedCheckpointRead;
    private volatile Checkpoint completedCheckpoint;

    private BenchmarkMongoCommandSampler(MongoClient client, ConnectionString connection) {
        this.client = client;
        this.connection = connection;
        source = this::read;
        checkpointSource = this::readCheckpoint;
        nanoTime = io.tapstate.adapters.pdk.PdkBenchmarkClock::nanoTime;
    }

    private BenchmarkMongoCommandSampler(Supplier<Snapshot> source, LongSupplier nanoTime) {
        client = null;
        connection = null;
        this.source = Objects.requireNonNull(source, "command source");
        checkpointSource = source;
        this.nanoTime = Objects.requireNonNull(nanoTime, "command clock");
    }

    static BenchmarkMongoCommandSampler from(Supplier<Snapshot> source, LongSupplier nanoTime) {
        return new BenchmarkMongoCommandSampler(source, nanoTime);
    }

    @FunctionalInterface interface CheckpointWaiter {
        Checkpoint await(Future<Checkpoint> read, long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException;
    }

    static BenchmarkMongoCommandSampler fromWithCheckpointWaiter(Supplier<Snapshot> source, LongSupplier nanoTime,
            CheckpointWaiter waiter) {
        var sampler = from(source, nanoTime); sampler.checkpointWaiter = Objects.requireNonNull(waiter); return sampler;
    }

    static BenchmarkMongoCommandSampler open(String mongoUri) {
        Objects.requireNonNull(mongoUri, "Mongo URI");
        ConnectionString connection = new ConnectionString(mongoUri);
        return new BenchmarkMongoCommandSampler(MongoClients.create(connection), connection);
    }

    /** Read the baseline after all fork setup and before issuing measured source SQL. */
    synchronized void start() {
        if (beginning != null || finished || closed) {
            throw new IllegalStateException("Mongo command sampler can start only once");
        }
        beginning = source.get();
        startedAtNanos = nanoTime.getAsLong();
    }

    /** Read the end counter after target ACK and its change-stream barrier. */
    synchronized Summary finish() {
        if (beginning == null || finished || closed) {
            throw new IllegalStateException("Mongo command sampler has no open measured window");
        }
        long endedAtNanos = nanoTime.getAsLong();
        long elapsedNanos = endedAtNanos - startedAtNanos;
        if (elapsedNanos < 0) {
            throw new AssertionError("Mongo command sample clock moved backward");
        }
        finished = true;
        Snapshot end = source.get();
        return difference(beginning, end, elapsedNanos / 1_000_000);
    }

    /** This calibration read retains the original baseline and leaves the later full finish open. */
    Checkpoint checkpoint() {
        var result = new CompletableFuture<Checkpoint>();
        synchronized (this) {
            if (beginning == null || finished || closed || checkpointRequested) {
                throw checkpointFailure(CheckpointReason.STATE, new IllegalStateException("Mongo checkpoint needs one open unused calibration window"));
            }
            checkpointRequested = true;
            checkpointWorker = Executors.newSingleThreadExecutor(Thread.ofPlatform().daemon()
                    .name("benchmark-command-checkpoint").factory());
            Snapshot baseline = beginning; long baselineAt = startedAtNanos;
            try {
                checkpointWorker.execute(() -> {
                    try { result.complete(readCheckpoint(baseline, baselineAt)); }
                    catch (Throwable unavailable) { result.completeExceptionally(unavailable); }
                    finally { checkpointWorker.shutdown(); }
                });
            } catch (RuntimeException scheduling) { throw checkpointFailure(CheckpointReason.SCHEDULING_FAILURE, scheduling); }
        }
        // No sampler monitor is held while an external read or the bounded owner wait is active.
        try { return checkpointWaiter.await(result, 5, TimeUnit.SECONDS); }
        catch (TimeoutException timeout) { throw checkpointFailure(CheckpointReason.WAIT_TIMEOUT, timeout); }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt(); throw checkpointFailure(CheckpointReason.WAIT_INTERRUPTED, interrupted);
        } catch (ExecutionException failed) {
            if (failed.getCause() instanceof CheckpointFailure diagnosed) { throw diagnosed; }
            throw checkpointFailure(CheckpointReason.READ_FAILURE, failed.getCause());
        }
    }

    private Checkpoint readCheckpoint(Snapshot baseline, long baselineAt) {
        long began = nanoTime.getAsLong();
        retainedCheckpointRead = new CheckpointRead(began, OptionalLong.empty(), Optional.empty(), "NONE");
        Snapshot actual = null; Throwable primary = null;
        try { actual = checkpointSource.get(); }
        catch (Throwable unavailable) { primary = unavailable; }
        long ended;
        try { ended = nanoTime.getAsLong(); }
        catch (Throwable unavailable) {
            retainedCheckpointRead = new CheckpointRead(began, OptionalLong.empty(), Optional.ofNullable(actual), unavailable.getClass().getName());
            if (primary != null) { primary.addSuppressed(unavailable); } else { primary = unavailable; }
            throw checkpointFailure(CheckpointReason.READ_FAILURE, primary);
        }
        retainedCheckpointRead = new CheckpointRead(began, OptionalLong.of(ended), Optional.ofNullable(actual),
                primary == null ? "NONE" : primary.getClass().getName());
        if (primary != null) { throw checkpointFailure(CheckpointReason.READ_FAILURE, primary); }
        try {
            if (Math.subtractExact(ended, began) < 0 || Math.subtractExact(began, baselineAt) < 0) {
                throw new AssertionError("Mongo checkpoint clock moved backward");
            }
            Checkpoint checkpoint = new Checkpoint(difference(baseline, actual, Math.subtractExact(began, baselineAt) / 1_000_000), began, ended);
            completedCheckpoint = checkpoint; return checkpoint;
        } catch (Throwable invalid) { throw checkpointFailure(CheckpointReason.INVALID_COUNTERS, invalid); }
    }

    record Checkpoint(Summary summary, long startedAtNanos, long completedAtNanos) { }
    record CheckpointRead(long startedAtNanos, OptionalLong completedAtNanos, Optional<Snapshot> snapshot, String failureType) { }
    enum CheckpointReason { STATE, WAIT_TIMEOUT, WAIT_INTERRUPTED, READ_FAILURE, INVALID_COUNTERS, SCHEDULING_FAILURE }
    Optional<Checkpoint> checkpointEvidence() { return Optional.ofNullable(completedCheckpoint); }
    Optional<CheckpointRead> checkpointReadEvidence() { return Optional.ofNullable(retainedCheckpointRead); }

    private CheckpointFailure checkpointFailure(CheckpointReason reason, Throwable cause) {
        return new CheckpointFailure(reason, checkpointReadEvidence(), checkpointEvidence(), cause);
    }

    static final class CheckpointFailure extends AssertionError {
        private final CheckpointReason reason;
        private final Optional<CheckpointRead> read;
        private final Optional<Checkpoint> checkpoint;
        CheckpointFailure(CheckpointReason reason, Optional<CheckpointRead> read, Optional<Checkpoint> checkpoint, Throwable cause) {
            super("Mongo command checkpoint was refused: " + reason, cause);
            this.reason = reason; this.read = read; this.checkpoint = checkpoint;
        }
        CheckpointReason reason() { return reason; }
        Optional<CheckpointRead> readEvidence() { return read; }
        Optional<Checkpoint> checkpointEvidence() { return checkpoint; }
        Map<String, Object> retainedEvidence() {
            var result = new LinkedHashMap<String, Object>(); result.put("state", "UNKNOWN"); result.put("reason", reason.name());
            result.put("actualReadAvailable", read.isPresent()); result.put("completedCheckpointAvailable", checkpoint.isPresent());
            read.ifPresent(actual -> {
                var evidence = new LinkedHashMap<String, Object>(); evidence.put("startedAtNanos", actual.startedAtNanos());
                evidence.put("state", actual.completedAtNanos().isPresent() ? "COMPLETED_READ_FACTS" : "PENDING_READ");
                actual.completedAtNanos().ifPresent(value -> evidence.put("completedAtNanos", value));
                actual.snapshot().ifPresent(value -> evidence.put("snapshot", Map.of("host", value.host(), "pid", value.pid(),
                        "uptimeMillis", value.uptimeMillis(), "commandTotals", value.commandTotals())));
                evidence.put("failureType", actual.failureType()); result.put("actualRead", Map.copyOf(evidence));
            });
            checkpoint.ifPresent(actual -> result.put("completedCheckpoint", Map.of("startedAtNanos", actual.startedAtNanos(),
                    "completedAtNanos", actual.completedAtNanos(), "byCommand", actual.summary().byCommand(),
                    "elapsedMillis", actual.summary().elapsedMillis())));
            BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> result.put(flag, false));
            return Map.copyOf(result);
        }
    }

    private Snapshot read() {
        return read(client);
    }

    /** Only this optional client has local read limits; the normal start/finish client is unchanged. */
    private Snapshot readCheckpoint() {
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connection)
                .applyToSocketSettings(socket -> socket.connectTimeout(5, TimeUnit.SECONDS).readTimeout(5, TimeUnit.SECONDS))
                .applyToClusterSettings(cluster -> cluster.serverSelectionTimeout(5, TimeUnit.SECONDS)).build();
        try (MongoClient owned = MongoClients.create(settings)) { return read(owned); }
    }

    private static Snapshot read(MongoClient client) {
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
            if (checkpointWorker != null) { checkpointWorker.shutdownNow(); }
            if (client != null) { client.close(); }
        }
    }
}
