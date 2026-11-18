package io.tapstate.e2e.benchmark;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.tapstate.e2e.benchmark.WorkloadChanges.Counts;
import io.tapstate.e2e.benchmark.WorkloadChanges.Event;
import io.tapstate.e2e.benchmark.WorkloadRecipe.Mix;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonTimestamp;

/** Generates source data only. The caller owns the isolated source database and its cleanup. */
public final class MongoWorkloadGenerator {
    private static final Duration IO_TIMEOUT = Duration.ofSeconds(10);

    public record HotKeys(int keyPercent, int eventPercent) {
        public HotKeys {
            if (keyPercent <= 0 || keyPercent >= 100 || eventPercent < 0 || eventPercent > 100) {
                throw new IllegalArgumentException("Hot key region must be 1-99% and event share 0-100%");
            }
        }
    }

    public record LiveWindow(int eventsPerSecond, Duration warmup, Duration measurement) {
        public LiveWindow {
            if (eventsPerSecond <= 0 || eventsPerSecond > 1_000_000 || warmup.isNegative()
                    || measurement.isNegative() || measurement.isZero()) {
                throw new IllegalArgumentException("Live windows require a positive rate and measurement duration");
            }
        }
    }

    public record Operations(int inserts, int updates, int deletes) {
        Operations(Counts counts) {
            this(counts.inserts(), counts.updates(), counts.deletes());
        }

        public int total() {
            return inserts + updates + deletes;
        }
    }

    public record Position(BsonDocument resumeToken, BsonTimestamp clusterTime) {
    }

    public record Latency(long requests, long minNanos, double meanNanos, long maxNanos) {
    }

    public record StreamReport(Operations planned, Operations acknowledged, Operations observed,
                               Position startExclusive, Position first, Position last,
                               int submittedOperations, long hotKeySelections, String sourceSequenceSha256, String error) {
    }

    public record PhaseReport(String name, Instant startedAt, Instant writesFinishedAt, long writeNanos, long historyNanos, Long generatorProcessCpuNanos,
                              Integer requestedEventsPerSecond, double actualSubmittedEventsPerSecond,
                              double actualAcknowledgedEventsPerSecond, Latency sourceRequestLatency,
                              Map<String, StreamReport> streams, boolean complete) {
    }

    public record SourceSettings(String namespace, String mongoVersion, String replicaSet,
                                 String writeConcern, boolean preImages, String runId,
                                 WorkloadRecipe.Identity recipe, int batchSize, Map<String, Integer> stringBaseLengths,
                                 Map<String, Integer> initialRows) {
    }

    public record Preparation(SourceSettings source, long writeNanos, long inspectionNanos,
                              Long generatorProcessCpuNanos, Map<String, SourceInspector.Table> tables) {
    }

    public record Backlog(SourceSettings source, Mix mix, HotKeys hotKeys, PhaseReport batch) {
    }

    public record Live(SourceSettings source, Mix mix, HotKeys hotKeys, LiveWindow window, PhaseReport warmup, PhaseReport measured) {
    }

    private final MongoDatabase database;
    private final WorkloadRecipe recipe;
    private final String runId;
    private final int batchSize;
    private final AtomicBoolean preparationStarted = new AtomicBoolean();
    private final Map<String, WorkloadRows> rows = new LinkedHashMap<>();

    public MongoWorkloadGenerator(MongoDatabase isolatedSource, WorkloadRecipe recipe, String runId, int batchSize) {
        if (runId == null || runId.isBlank() || batchSize < 1 || batchSize > 1000) {
            throw new IllegalArgumentException("A run ID and a source batch size in 1-1000 are required");
        }
        this.database = isolatedSource.withWriteConcern(WriteConcern.MAJORITY)
                .withReadConcern(ReadConcern.MAJORITY).withTimeout(IO_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        this.recipe = Objects.requireNonNull(recipe);
        this.runId = runId;
        this.batchSize = batchSize;
        recipe.inputs.forEach(input -> rows.put(input.name(), new WorkloadRows(recipe, input)));
    }

    /** Suggests deterministic calibration candidates. Pass the chosen lengths explicitly to prepare. */
    public Map<String, Integer> calibrateStringLengths() {
        Map<String, Integer> lengths = new LinkedHashMap<>();
        rows.forEach((name, generator) -> lengths.put(name, generator.calibrate()));
        return Map.copyOf(lengths);
    }

    /** Seeds fresh collections, then inspects the stored BSON independently of the insert loop. */
    public Prepared prepare(Map<String, Integer> stringLengths) {
        Map<String, Integer> lengths = Map.copyOf(stringLengths);
        if (!lengths.keySet().equals(rows.keySet()) || lengths.values().stream()
                .anyMatch(length -> length < 1 || length > recipe.scalar.rowBytes())) {
            throw new IllegalArgumentException("Supply calibrated string base lengths for " + rows.keySet());
        }
        if (!preparationStarted.compareAndSet(false, true)) {
            throw new IllegalArgumentException("A generator preparation may be attempted once");
        }
        var existing = database.listCollectionNames().into(new HashSet<>());
        if (recipe.inputs.stream().anyMatch(input -> existing.contains(input.name()))) {
            throw new IllegalArgumentException("Workload collections already exist in source database " + database.getName());
        }
        var hello = database.runCommand(new BsonDocument("hello", new org.bson.BsonInt32(1)), BsonDocument.class);
        if (recipe.preImages && !hello.containsKey("setName")) {
            throw new IllegalArgumentException("CDC workload requires a MongoDB replica set");
        }
        String version = database.runCommand(new BsonDocument("buildInfo", new org.bson.BsonInt32(1)),
                BsonDocument.class).getString("version").getValue();
        Long cpu = cpuNanos();
        long started = System.nanoTime();
        Map<String, Integer> initialRows = new LinkedHashMap<>();
        for (var input : recipe.inputs) {
            createCollection(input.name());
            initialRows.put(input.name(), input.rows());
            int written = 0;
            var generator = rows.get(input.name());
            try {
                while (written < input.rows()) {
                    int count = Math.min(batchSize, input.rows() - written);
                    var batch = new ArrayList<BsonDocument>(count);
                    for (int index = 0; index < count; index++) {
                        batch.add(generator.row(input.firstKey() + written + index, lengths.get(input.name())));
                    }
                    if (!collection(input.name()).insertMany(batch).wasAcknowledged()) {
                        throw new IllegalStateException("Unacknowledged source insert");
                    }
                    written += count;
                }
            } catch (MongoException error) {
                throw new IllegalStateException(input.name() + ": " + written
                        + " rows acknowledged before the failed source batch", error);
            }
        }
        long writeNanos = System.nanoTime() - started;
        var settings = new SourceSettings(database.getName(), version,
                hello.containsKey("setName") ? hello.getString("setName").getValue() : null,
                "majority", recipe.preImages, runId, recipe.identity, batchSize, lengths, Map.copyOf(initialRows));
        started = System.nanoTime();
        var tables = SourceInspector.inspect(database, recipe, batchSize);
        return new Prepared(new Preparation(settings, writeNanos, System.nanoTime() - started, cpuElapsed(cpu), tables));
    }

    private void createCollection(String name) {
        var command = new BsonDocument("create", new org.bson.BsonString(name))
                .append("writeConcern", WriteConcern.MAJORITY.asDocument());
        if (recipe.preImages) {
            command.put("changeStreamPreAndPostImages", new BsonDocument("enabled", BsonBoolean.TRUE));
        }
        database.runCommand(command);
        if (recipe.preImages && !database.listCollections(BsonDocument.class)
                .filter(new BsonDocument("name", new org.bson.BsonString(name))).first()
                .getDocument("options").getDocument("changeStreamPreAndPostImages").getBoolean("enabled").getValue()) {
            throw new IllegalStateException("Source pre-images are disabled for " + name);
        }
    }

    private MongoCollection<BsonDocument> collection(String name) {
        return database.getCollection(name, BsonDocument.class);
    }

    public final class Prepared {
        private final Preparation preparation;
        private final AtomicBoolean used = new AtomicBoolean();

        private Prepared(Preparation preparation) {
            this.preparation = preparation;
        }

        public Preparation preparation() {
            return preparation;
        }

        /** Call after the catch-up reader's initial state is ready and its formal batch is still untouched. */
        public Backlog prewrite(int events, Mix override, HotKeys hotKeys) {
            if (events <= 0) {
                throw new IllegalArgumentException("A backlog must contain source events");
            }
            Mix mix = mix(override, hotKeys);
            var changes = new WorkloadChanges(recipe, hotKeys);
            var counts = changes.allocate(events, mix);
            changes.validate(changes.allocate(0, mix), counts);
            claim();
            return new Backlog(preparation.source(), mix, hotKeys,
                    runPhase("backlog", changes.phase(counts), null, null, preparation.source().stringBaseLengths()));
        }

        /** Call after CDC is ready. The callback separates warm-up from scoring and may await target readiness. */
        public Live inject(LiveWindow window, Mix override, HotKeys hotKeys, Runnable afterWarmup) {
            Objects.requireNonNull(afterWarmup);
            Mix mix = mix(override, hotKeys);
            var changes = new WorkloadChanges(recipe, hotKeys);
            var warmup = changes.allocate(eventsFor(window.eventsPerSecond(), window.warmup()), mix);
            var scored = changes.allocate(eventsFor(window.eventsPerSecond(), window.measurement()), mix);
            if (scored.stream().allMatch(counts -> counts.total() == 0)) {
                throw new IllegalArgumentException("The measurement duration must admit at least one scheduled event");
            }
            changes.validate(warmup, scored);
            claim();
            var lengths = preparation.source().stringBaseLengths();
            var warm = runPhase("warmup", changes.phase(warmup), window.eventsPerSecond(), window.warmup(), lengths);
            var measurementStreams = changes.phase(scored);
            String blocked = null;
            if (warm.complete()) {
                try {
                    afterWarmup.run();
                } catch (RuntimeException failure) {
                    blocked = "Warm-up readiness check failed: " + failure;
                }
            } else {
                blocked = "Warm-up incomplete";
            }
            var measure = blocked == null
                    ? runPhase("measurement", measurementStreams, window.eventsPerSecond(), window.measurement(), lengths)
                    : notRun(measurementStreams, window.eventsPerSecond(), blocked);
            return new Live(preparation.source(), mix, hotKeys, window, warm, measure);
        }

        private void claim() {
            if (!used.compareAndSet(false, true)) {
                throw new IllegalStateException("Start a fresh preparation for another change workload");
            }
        }
    }

    private Mix mix(Mix override, HotKeys hotKeys) {
        return switch (recipe.changeMode) {
            case NONE -> throw new IllegalArgumentException("This case has no incremental workload");
            case BASIC -> {
                if (hotKeys != null || (override != null && !recipe.allowMixOverride)) {
                    throw new IllegalArgumentException("This case's incremental recipe cannot be overridden");
                }
                yield override == null ? recipe.mix : override;
            }
            case SPECIAL -> {
                if (override == null || override.insert() != 0) {
                    throw new IllegalArgumentException("The specialized change recipe needs a calibrated update/delete mix");
                }
                yield override;
            }
            case STEADY -> {
                if (hotKeys != null || override != null && !override.equals(recipe.mix)) {
                    throw new IllegalArgumentException("The steady-state recipe only updates existing keys");
                }
                yield recipe.mix;
            }
        };
    }

    private static int eventsFor(int rate, Duration duration) {
        return Math.toIntExact(Math.multiplyExact(duration.toNanos(), rate) / TimeUnit.SECONDS.toNanos(1));
    }

    private PhaseReport runPhase(String name, List<WorkloadChanges.Stream> streams, Integer rate,
                                 Duration liveDuration, Map<String, Integer> lengths) {
        var states = new ArrayList<StreamState>();
        try {
            for (var stream : streams) {
                states.add(new StreamState(stream, collection(stream.input.name())));
            }
            var delays = new Delays();
            Long cpu = cpuNanos();
            Instant startedAt = Instant.now();
            long start = System.nanoTime();
            if (rate == null) {
                prewriteBatches(states, lengths, delays);
            } else {
                injectPaced(states, lengths, rate, liveDuration, delays, start);
            }
            long duration = System.nanoTime() - start;
            Instant writesFinishedAt = Instant.now();
            Long writerCpu = cpuElapsed(cpu);
            long historyStarted = System.nanoTime();
            for (var state : states) {
                state.readHistory();
            }
            return report(name, states, rate, startedAt, writesFinishedAt, duration, System.nanoTime() - historyStarted, writerCpu, delays);
        } finally {
            states.forEach(StreamState::close);
        }
    }

    private void prewriteBatches(List<StreamState> states, Map<String, Integer> lengths, Delays delays) {
        boolean remaining;
        do {
            remaining = false;
            for (var state : states) {
                var batch = new ArrayList<WriteModel<BsonDocument>>(batchSize);
                var events = new ArrayList<Event>(batchSize);
                while (batch.size() < batchSize) {
                    var event = state.stream.next();
                    if (event == null) {
                        break;
                    }
                    events.add(event);
                    batch.add(write(state.name(), event, lengths.get(state.name())));
                }
                if (!batch.isEmpty()) {
                    remaining = true;
                    if (!state.write(batch, events, delays)) {
                        return;
                    }
                }
            }
        } while (remaining);
    }

    private void injectPaced(List<StreamState> states, Map<String, Integer> lengths, int rate, Duration liveDuration,
                             Delays delays, long start) {
        long interval = TimeUnit.SECONDS.toNanos(1) / rate;
        long total = states.stream().mapToLong(state -> state.stream.requested.total()).sum();
        long deadline = start + liveDuration.toNanos();
        long next = start;
        for (long sent = 0; sent < total;) {
            for (var state : states) {
                if (state.submitted == state.stream.requested.total()) {
                    continue;
                }
                long delay;
                while ((delay = next - System.nanoTime()) > 0 && !Thread.currentThread().isInterrupted()) {
                    LockSupport.parkNanos(delay);
                }
                long now = System.nanoTime();
                if (now >= deadline || Thread.currentThread().isInterrupted()) {
                    return;
                }
                var event = state.stream.next();
                if (!state.write(List.of(write(state.name(), event, lengths.get(state.name()))), List.of(event), delays)) {
                    return;
                }
                sent++;
                next = now - next >= interval ? now + interval : next + interval;
            }
        }
        long remainder;
        while ((remainder = deadline - System.nanoTime()) > 0 && !Thread.currentThread().isInterrupted()) {
            LockSupport.parkNanos(remainder);
        }
    }

    private WriteModel<BsonDocument> write(String name, Event event, int length) {
        var match = new BsonDocument("_id", new BsonInt64(event.key()));
        return switch (event.kind()) {
            case INSERT -> new InsertOneModel<>(rows.get(name).row(event.key(), length));
            case UPDATE -> new UpdateOneModel<>(match, rows.get(name).mutation(event.key(), event.ordinal(), length));
            case DELETE -> new DeleteOneModel<>(match);
        };
    }

    private static PhaseReport report(String name, List<StreamState> states, Integer rate, Instant startedAt, Instant writesFinishedAt,
                                      long writeNanos, long historyNanos, Long cpu, Delays delays) {
        Map<String, StreamReport> results = new LinkedHashMap<>();
        boolean complete = true;
        long submitted = 0;
        long acknowledged = 0;
        for (var state : states) {
            results.put(state.name(), state.report());
            complete &= state.error == null && state.stream.requested.equals(state.acknowledged)
                    && state.acknowledged.equals(state.observed);
            submitted += state.submitted;
            acknowledged += state.acknowledged.total();
        }
        double seconds = writeNanos / 1_000_000_000.0;
        return new PhaseReport(name, startedAt, writesFinishedAt, writeNanos, historyNanos, cpu, rate,
                seconds > 0 ? submitted / seconds : 0, seconds > 0 ? acknowledged / seconds : 0,
                delays.report(), Map.copyOf(results), complete);
    }

    private static PhaseReport notRun(List<WorkloadChanges.Stream> streams, int rate, String reason) {
        Map<String, StreamReport> result = new LinkedHashMap<>();
        var zero = new Operations(0, 0, 0);
        for (var stream : streams) {
            result.put(stream.input.name(), new StreamReport(new Operations(stream.requested), zero, zero,
                    null, null, null, 0, 0, null, reason));
        }
        return new PhaseReport("measurement", null, null, 0, 0, null, rate, 0, 0, new Delays().report(), Map.copyOf(result), false);
    }

    private static Long cpuNanos() {
        return ProcessHandle.current().info().totalCpuDuration().map(Duration::toNanos).orElse(null);
    }

    private static Long cpuElapsed(Long start) {
        Long end = cpuNanos();
        return start == null || end == null ? null : Math.max(0, end - start);
    }

    private static final class Delays {
        private long count;
        private long total;
        private long min = Long.MAX_VALUE;
        private long max;

        void record(long nanos) {
            count++;
            total += nanos;
            min = Math.min(min, nanos);
            max = Math.max(max, nanos);
        }

        Latency report() {
            return new Latency(count, count == 0 ? 0 : min, count == 0 ? 0 : (double) total / count, max);
        }
    }

    private static final class StreamState {
        final WorkloadChanges.Stream stream;
        private final MongoCollection<BsonDocument> collection;
        private final MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor;
        private final Position start;
        private final long startingHotKeys;
        private Position first;
        private Position last;
        private Counts acknowledged = new Counts(0, 0, 0);
        private Counts observed = new Counts(0, 0, 0);
        private final ByteBuffer eventBytes = ByteBuffer.allocate(9);
        private final MessageDigest submittedSequence = SourceInspector.sha256();
        private final MessageDigest observedSequence = SourceInspector.sha256();
        private String sourceSequenceSha256;
        private int submitted;
        private String error;

        StreamState(WorkloadChanges.Stream stream, MongoCollection<BsonDocument> collection) {
            this.stream = stream;
            this.collection = collection;
            startingHotKeys = stream.keys.hotSelections;
            cursor = collection.watch(BsonDocument.class).maxAwaitTime(100, TimeUnit.MILLISECONDS).cursor();
            try {
                // The sync driver exposes the post-batch resume token after the first cursor poll.
                if (cursor.tryNext() != null || cursor.getResumeToken() == null) {
                    throw new IllegalStateException("Could not establish an idle source boundary for " + name());
                }
                start = new Position(cursor.getResumeToken(), null);
            } catch (RuntimeException failure) {
                cursor.close();
                throw failure;
            }
        }

        String name() {
            return stream.input.name();
        }

        boolean write(List<WriteModel<BsonDocument>> batch, List<Event> events, Delays delays) {
            var expected = counts(batch);
            submitted += batch.size();
            events.forEach(event -> appendEvent(submittedSequence, event.kind(), event.key()));
            long startNanos = System.nanoTime();
            try {
                Counts actual = counts(collection.bulkWrite(batch));
                acknowledged = acknowledged.plus(actual);
                if (!expected.equals(actual)) {
                    error = "Acknowledged operations " + actual + " differ from submitted operations " + expected;
                }
            } catch (MongoBulkWriteException failure) {
                if (failure.getWriteConcernError() == null) {
                    acknowledged = acknowledged.plus(counts(failure.getWriteResult()));
                }
                error = failure.toString();
            } catch (MongoException failure) {
                error = failure.toString();
            } finally {
                delays.record(System.nanoTime() - startNanos);
            }
            return error == null;
        }

        void readHistory() {
            int maximum = acknowledged.total();
            long idleDeadline = System.nanoTime() + IO_TIMEOUT.toNanos();
            try {
                while (observed.total() < maximum && System.nanoTime() < idleDeadline) {
                    var event = cursor.tryNext();
                    if (event == null) {
                        continue;
                    }
                    var kind = switch (event.getOperationType()) {
                        case INSERT -> WorkloadChanges.Kind.INSERT;
                        case UPDATE -> WorkloadChanges.Kind.UPDATE;
                        case DELETE -> WorkloadChanges.Kind.DELETE;
                        default -> throw new IllegalStateException("Unexpected source event " + event.getOperationType());
                    };
                    observed = observed.plus(switch (kind) {
                        case INSERT -> new Counts(1, 0, 0);
                        case UPDATE -> new Counts(0, 1, 0);
                        case DELETE -> new Counts(0, 0, 1);
                    });
                    appendEvent(observedSequence, kind, event.getDocumentKey().getInt64("_id").getValue());
                    last = new Position(event.getResumeToken(), event.getClusterTime());
                    if (first == null) {
                        first = last;
                    }
                    idleDeadline = System.nanoTime() + IO_TIMEOUT.toNanos();
                }
                var history = observedSequence.digest();
                sourceSequenceSha256 = HexFormat.of().formatHex(history);
                if (error == null && (!observed.equals(acknowledged)
                        || !MessageDigest.isEqual(history, submittedSequence.digest()))) {
                    error = "Change stream observations differ from the submitted operation sequence";
                }
            } catch (RuntimeException failure) {
                if (error == null) {
                    error = failure.toString();
                }
            }
        }

        StreamReport report() {
            return new StreamReport(new Operations(stream.requested), new Operations(acknowledged), new Operations(observed),
                    start, first, last, submitted, stream.keys.hotSelections - startingHotKeys, sourceSequenceSha256, error);
        }

        void close() {
            cursor.close();
        }

        private void appendEvent(MessageDigest digest, WorkloadChanges.Kind kind, long key) {
            eventBytes.clear().put((byte) kind.ordinal()).putLong(key);
            digest.update(eventBytes.array());
        }

        private static Counts counts(BulkWriteResult result) {
            return new Counts(result.getInsertedCount(), result.getModifiedCount(), result.getDeletedCount());
        }

        private static Counts counts(List<WriteModel<BsonDocument>> models) {
            int inserts = 0;
            int updates = 0;
            int deletes = 0;
            for (var model : models) {
                switch (model) {
                    case InsertOneModel<?> ignored -> inserts++;
                    case UpdateOneModel<?> ignored -> updates++;
                    case DeleteOneModel<?> ignored -> deletes++;
                    default -> throw new IllegalStateException("Unexpected source write model " + model);
                }
            }
            return new Counts(inserts, updates, deletes);
        }
    }
}
