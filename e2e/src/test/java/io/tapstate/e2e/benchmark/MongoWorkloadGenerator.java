package io.tapstate.e2e.benchmark;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.Filters;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;

/** Generates source data only. The caller owns the isolated source database and its cleanup. */
public final class MongoWorkloadGenerator {
    private static final Duration IO_TIMEOUT = Duration.ofSeconds(10);
    private static final String HISTORY_MARKERS = "benchmark_source_history";

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
                               Position startExclusive, Position first, Position last, Position historyBoundary,
                               int submittedOperations, long hotKeySelections, String sourceSequenceSha256,
                               String sourceClockSequenceSha256, String error) {
    }

    public record PhaseReport(String name, Instant startedAt, Instant writesFinishedAt, long writeNanos, long historyNanos, Long generatorProcessCpuNanos,
                              Integer requestedEventsPerSecond, Long scheduledWindowNanos, boolean acknowledgedAfterWindow,
                              double actualSubmittedEventsPerSecond,
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
    private final LongSupplier nanoClock;
    private final LongConsumer waitNanos;
    private final AtomicBoolean preparationStarted = new AtomicBoolean();
    private final Map<String, WorkloadRows> rows = new LinkedHashMap<>();

    public MongoWorkloadGenerator(MongoDatabase isolatedSource, WorkloadRecipe recipe, String runId, int batchSize) {
        this(isolatedSource, recipe, runId, batchSize, System::nanoTime, LockSupport::parkNanos);
    }

    public MongoWorkloadGenerator(MongoDatabase isolatedSource, WorkloadRecipe recipe, String runId, int batchSize,
            LongSupplier nanoClock, LongConsumer waitNanos) {
        if (runId == null || runId.isBlank() || batchSize < 1 || batchSize > 1000) {
            throw new IllegalArgumentException("A run ID and a source batch size in 1-1000 are required");
        }
        this.database = isolatedSource.withWriteConcern(WriteConcern.MAJORITY)
                .withReadConcern(ReadConcern.MAJORITY).withTimeout(IO_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        this.recipe = Objects.requireNonNull(recipe);
        this.runId = runId;
        this.batchSize = batchSize;
        this.nanoClock = Objects.requireNonNull(nanoClock);
        this.waitNanos = Objects.requireNonNull(waitNanos);
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
        private WorkloadChanges steadyChanges;

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
            return inject(window, override, hotKeys, afterWarmup, () -> {});
        }

        /** The end callback runs after writes finish, before independent history verification. */
        public Live inject(LiveWindow window, Mix override, HotKeys hotKeys, Runnable afterWarmup, Runnable afterWrites) {
            Objects.requireNonNull(afterWarmup);
            return injectWithWarmupReport(window, override, hotKeys, report -> afterWarmup.run(), afterWrites);
        }

        /** Supplies the independently verified warm-up report before the measurement stream opens. */
        public Live injectWithWarmupReport(LiveWindow window, Mix override, HotKeys hotKeys,
                Consumer<PhaseReport> afterWarmup, Runnable afterWrites) {
            return inject(window, mix(override, hotKeys), hotKeys, afterWarmup, afterWrites,
                    new WorkloadChanges(recipe, hotKeys), false);
        }

        /** Retains the deterministic mutation sequence across restarts of an update-only workload. */
        public synchronized Live injectSteadyCycle(LiveWindow window, Runnable afterWarmup, Runnable afterWrites) {
            Objects.requireNonNull(afterWarmup);
            return injectSteadyCycleWithWarmupReport(window, report -> afterWarmup.run(), afterWrites);
        }

        public synchronized Live injectSteadyCycleWithWarmupReport(LiveWindow window,
                Consumer<PhaseReport> afterWarmup, Runnable afterWrites) {
            if (recipe.changeMode != WorkloadRecipe.Changes.STEADY) {
                throw new IllegalArgumentException("Repeated cycles require an update-only workload");
            }
            if (steadyChanges == null) {
                claim();
                steadyChanges = new WorkloadChanges(recipe, null);
            }
            return inject(window, recipe.mix, null, afterWarmup, afterWrites, steadyChanges, true);
        }

        private Live inject(LiveWindow window, Mix mix, HotKeys hotKeys, Consumer<PhaseReport> afterWarmup, Runnable afterWrites,
                            WorkloadChanges changes, boolean continuing) {
            Objects.requireNonNull(afterWarmup);
            Objects.requireNonNull(afterWrites);
            var warmup = changes.allocate(eventsFor(window.eventsPerSecond(), window.warmup()), mix);
            var scored = changes.allocate(eventsFor(window.eventsPerSecond(), window.measurement()), mix);
            if (scored.stream().allMatch(counts -> counts.total() == 0)) {
                throw new IllegalArgumentException("The measurement duration must admit at least one scheduled event");
            }
            changes.validate(warmup, scored);
            if (!continuing) {
                claim();
            }
            var lengths = preparation.source().stringBaseLengths();
            var warm = runPhase("warmup", changes.phase(warmup), window.eventsPerSecond(), window.warmup(), lengths);
            var measurementStreams = changes.phase(scored);
            String blocked = null;
            if (warm.complete()) {
                try {
                    afterWarmup.accept(warm);
                } catch (RuntimeException | AssertionError failure) {
                    blocked = "Warm-up readiness check failed: " + failure;
                }
            } else {
                blocked = "Warm-up incomplete";
            }
            var measure = blocked == null
                    ? runPhase("measurement", measurementStreams, window.eventsPerSecond(), window.measurement(), lengths, afterWrites)
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
        return runPhase(name, streams, rate, liveDuration, lengths, () -> {});
    }

    private PhaseReport runPhase(String name, List<WorkloadChanges.Stream> streams, Integer rate,
                                 Duration liveDuration, Map<String, Integer> lengths, Runnable afterWrites) {
        var states = new ArrayList<StreamState>();
        try {
            for (var stream : streams) {
                states.add(new StreamState(stream, collection(stream.input.name()), database));
            }
        } catch (MongoException | IllegalStateException setupFailure) {
            closeStreams(states, setupFailure);
            return notRun(name, streams, rate, "Source history setup failed: " + setupFailure);
        } catch (RuntimeException | Error failure) {
            closeStreams(states, failure);
            throw failure;
        }
        var delays = new Delays();
        Instant startedAt = Instant.now();
        Instant writesFinishedAt;
        long duration;
        long historyNanos;
        Long scheduledWindow = liveDuration == null ? null : liveDuration.toNanos();
        boolean acknowledgedAfterWindow = false;
        Long writerCpu;
        Throwable primary = null;
        try {
            Long cpu = cpuNanos();
            long start = nanoClock.getAsLong();
            if (rate == null) {
                prewriteBatches(states, lengths, delays);
            } else {
                acknowledgedAfterWindow = injectPaced(states, lengths, rate, scheduledWindow, delays, start);
            }
            duration = nanoClock.getAsLong() - start;
            if (scheduledWindow != null) {
                for (var state : states) {
                    if (duration < scheduledWindow || Thread.currentThread().isInterrupted()) {
                        state.recordError("The live write window ended before its scheduled boundary");
                    } else if (state.submitted == 0 && state.stream.requested.total() > 0) {
                        state.recordError("No source events were submitted during the live write window");
                    }
                }
            }
            writesFinishedAt = Instant.now();
            writerCpu = cpuElapsed(cpu);
            String callbackError = null;
            try {
                afterWrites.run();
            } catch (RuntimeException | AssertionError failure) {
                callbackError = "After-writes boundary check failed: " + failure;
            }
            long historyStarted = System.nanoTime();
            for (var state : states) {
                state.readHistory();
                if (callbackError != null) state.recordError(callbackError);
            }
            historyNanos = System.nanoTime() - historyStarted;
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            closeStreams(states, primary);
        }
        return report(name, states, rate, scheduledWindow, acknowledgedAfterWindow,
                startedAt, writesFinishedAt, duration, historyNanos, writerCpu, delays);
    }

    private static void closeStreams(List<StreamState> states, Throwable primary) {
        for (var state : states) {
            try {
                state.close();
            } catch (RuntimeException cleanup) {
                state.recordError("Could not close source history cursor: " + cleanup);
                if (primary != null && primary != cleanup) {
                    primary.addSuppressed(cleanup);
                }
            }
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

    private boolean injectPaced(List<StreamState> states, Map<String, Integer> lengths, int rate, long windowNanos,
                                Delays delays, long start) {
        long interval = TimeUnit.SECONDS.toNanos(1) / rate;
        long total = states.stream().mapToLong(state -> state.stream.requested.total()).sum();
        long deadline = start + windowNanos;
        long next = start;
        for (long sent = 0; sent < total;) {
            for (var state : states) {
                if (state.submitted == state.stream.requested.total()) {
                    continue;
                }
                long due = Math.min(next, deadline);
                long delay;
                while ((delay = due - nanoClock.getAsLong()) > 0 && !Thread.currentThread().isInterrupted()) {
                    waitNanos.accept(delay);
                }
                if (nanoClock.getAsLong() >= deadline || Thread.currentThread().isInterrupted()) {
                    return false;
                }
                var event = state.stream.next();
                var command = write(state.name(), event, lengths.get(state.name()));
                long now = nanoClock.getAsLong();
                if (now >= deadline || Thread.currentThread().isInterrupted()) {
                    return false;
                }
                if (!state.write(List.of(command), List.of(event), delays)) {
                    return false;
                }
                if (nanoClock.getAsLong() > deadline) {
                    return true;
                }
                sent++;
                // A missed interval does not create an obligation to catch up to the planned total.
                next = now - next >= interval ? now + interval : next + interval;
            }
        }
        long remainder;
        while ((remainder = deadline - nanoClock.getAsLong()) > 0 && !Thread.currentThread().isInterrupted()) {
            waitNanos.accept(remainder);
        }
        return false;
    }

    private WriteModel<BsonDocument> write(String name, Event event, int length) {
        var match = new BsonDocument("_id", new BsonInt64(event.key()));
        return switch (event.kind()) {
            case INSERT -> new InsertOneModel<>(rows.get(name).row(event.key(), length));
            case UPDATE -> new UpdateOneModel<>(match, rows.get(name).mutation(event.key(), event.ordinal(), length));
            case DELETE -> new DeleteOneModel<>(match);
        };
    }

    private static PhaseReport report(String name, List<StreamState> states, Integer rate, Long scheduledWindow,
                                      boolean acknowledgedAfterWindow, Instant startedAt, Instant writesFinishedAt,
                                      long writeNanos, long historyNanos, Long cpu, Delays delays) {
        Map<String, StreamReport> results = new LinkedHashMap<>();
        boolean complete = true;
        long submitted = 0;
        long acknowledged = 0;
        for (var state : states) {
            results.put(state.name(), state.report());
            complete &= state.error == null && state.submitted == state.acknowledged.total()
                    && state.acknowledged.equals(state.observed)
                    && (scheduledWindow != null || state.stream.requested.equals(state.acknowledged));
            submitted += state.submitted;
            acknowledged += state.acknowledged.total();
        }
        long activeNanos = scheduledWindow == null ? writeNanos : Math.min(writeNanos, scheduledWindow);
        double seconds = activeNanos / 1_000_000_000.0;
        return new PhaseReport(name, startedAt, writesFinishedAt, writeNanos, historyNanos, cpu, rate,
                scheduledWindow, acknowledgedAfterWindow,
                seconds > 0 ? submitted / seconds : 0, seconds > 0 ? acknowledged / seconds : 0,
                delays.report(), Map.copyOf(results), complete);
    }

    private static PhaseReport notRun(List<WorkloadChanges.Stream> streams, int rate, String reason) {
        return notRun("measurement", streams, rate, reason);
    }

    private static PhaseReport notRun(String name, List<WorkloadChanges.Stream> streams, Integer rate, String reason) {
        Map<String, StreamReport> result = new LinkedHashMap<>();
        var zero = new Operations(0, 0, 0);
        for (var stream : streams) {
            result.put(stream.input.name(), new StreamReport(new Operations(stream.requested), zero, zero,
                    null, null, null, null, 0, 0, null, null, reason));
        }
        return new PhaseReport(name, null, null, 0, 0, null, rate, null, false, 0, 0, new Delays().report(), Map.copyOf(result), false);
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
        private final MongoCollection<BsonDocument> markers;
        private final BsonString markerId = new BsonString(UUID.randomUUID().toString());
        private final MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor;
        private final Position start;
        private final long startingHotKeys;
        private Position first;
        private Position last;
        private Position historyBoundary;
        private Counts acknowledged = new Counts(0, 0, 0);
        private Counts observed = new Counts(0, 0, 0);
        private final ByteBuffer eventBytes = ByteBuffer.allocate(9);
        private final MessageDigest submittedSequence = SourceInspector.sha256();
        private final MessageDigest observedSequence = SourceInspector.sha256();
        private final ByteBuffer timedEventBytes = ByteBuffer.allocate(17);
        private final MessageDigest observedClockSequence = SourceInspector.sha256();
        private String sourceSequenceSha256;
        private String sourceClockSequenceSha256;
        private int submitted;
        private boolean unknownAcknowledgement;
        private String error;

        StreamState(WorkloadChanges.Stream stream, MongoCollection<BsonDocument> collection, MongoDatabase database) {
            this.stream = stream;
            this.collection = collection;
            this.markers = database.getCollection(HISTORY_MARKERS, BsonDocument.class);
            startingHotKeys = stream.keys.hotSelections;
            cursor = database.watch(List.of(Aggregates.match(Filters.or(
                            Filters.eq("ns.coll", name()),
                            Filters.and(Filters.eq("ns.coll", HISTORY_MARKERS), Filters.eq("documentKey._id", markerId))))),
                    BsonDocument.class).maxAwaitTime(100, TimeUnit.MILLISECONDS).cursor();
            try {
                // The sync driver exposes the post-batch resume token after the first cursor poll.
                if (cursor.tryNext() != null || cursor.getResumeToken() == null) {
                    throw new IllegalStateException("Could not establish an idle source boundary for " + name());
                }
                start = new Position(cursor.getResumeToken(), null);
            } catch (RuntimeException failure) {
                try {
                    cursor.close();
                } catch (RuntimeException cleanup) {
                    if (cleanup != failure) {
                        failure.addSuppressed(cleanup);
                    }
                }
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
                } else {
                    unknownAcknowledgement = true;
                }
                error = failure.toString();
            } catch (MongoException failure) {
                unknownAcknowledgement = true;
                error = failure.toString();
            } finally {
                delays.record(System.nanoTime() - startNanos);
            }
            return error == null;
        }

        void readHistory() {
            if (unknownAcknowledgement) {
                try {
                    // Observe a majority-ordered prefix without altering the workload collection.
                    // A timed-out write can still arrive later, so the phase keeps the original failure.
                    markers.insertOne(new BsonDocument("_id", markerId));
                } catch (MongoException failure) {
                    recordError("Could not write the source history marker: " + failure);
                }
            }
            long idleDeadline = System.nanoTime() + IO_TIMEOUT.toNanos();
            try {
                while ((unknownAcknowledgement || observed.total() < acknowledged.total())
                        && System.nanoTime() < idleDeadline) {
                    var event = cursor.tryNext();
                    if (event == null) {
                        continue;
                    }
                    if (HISTORY_MARKERS.equals(event.getNamespace().getCollectionName())) {
                        historyBoundary = new Position(event.getResumeToken(), event.getClusterTime());
                        break;
                    }
                    var kind = switch (event.getOperationType()) {
                        case INSERT -> WorkloadChanges.Kind.INSERT;
                        case UPDATE, REPLACE -> WorkloadChanges.Kind.UPDATE;
                        case DELETE -> WorkloadChanges.Kind.DELETE;
                        default -> throw new IllegalStateException("Unexpected source event " + event.getOperationType());
                    };
                    observed = observed.plus(switch (kind) {
                        case INSERT -> new Counts(1, 0, 0);
                        case UPDATE -> new Counts(0, 1, 0);
                        case DELETE -> new Counts(0, 0, 1);
                    });
                    long key = event.getDocumentKey().getInt64("_id").getValue();
                    appendEvent(observedSequence, kind, key);
                    timedEventBytes.clear().put((byte) kind.ordinal()).putLong(key)
                            .putLong(event.getClusterTime().getTime() * 1000L);
                    observedClockSequence.update(timedEventBytes.array());
                    last = new Position(event.getResumeToken(), event.getClusterTime());
                    if (first == null) {
                        first = last;
                    }
                    idleDeadline = System.nanoTime() + IO_TIMEOUT.toNanos();
                }
                var history = observedSequence.digest();
                sourceSequenceSha256 = HexFormat.of().formatHex(history);
                sourceClockSequenceSha256 = HexFormat.of().formatHex(observedClockSequence.digest());
                if (unknownAcknowledgement && historyBoundary == null) {
                    recordError("Source history incomplete: the majority boundary was not observed");
                }
                if (error == null && (!observed.equals(acknowledged)
                        || !MessageDigest.isEqual(history, submittedSequence.digest()))) {
                    error = "Change stream observations differ from the submitted operation sequence";
                }
            } catch (RuntimeException failure) {
                recordError("Could not read source history: " + failure);
            }
        }

        void recordError(String detail) {
            error = error == null ? detail : error + " | " + detail;
        }

        StreamReport report() {
            return new StreamReport(new Operations(stream.requested), new Operations(acknowledged), new Operations(observed),
                    start, first, last, historyBoundary, submitted, stream.keys.hotSelections - startingHotKeys,
                    sourceSequenceSha256, sourceClockSequenceSha256, error);
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
