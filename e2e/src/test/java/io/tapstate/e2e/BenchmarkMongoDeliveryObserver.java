package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import org.bson.Document;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Reads physical target changes outside the product and timestamps their delivery on this JVM's
 * monotonic clock. A caller opens one observer per target before issuing measured source SQL.
 *
 * <p>Opening the database change-stream cursor is the readiness barrier: the aggregate has reached
 * Mongo before this method returns, so no startup sleep is needed. The cursor covers a collection that
 * might not exist yet and an observer-only barrier collection in the same database. The caller supplies
 * each source batch's {@link System#nanoTime()} reading immediately before executing that batch's SQL.
 * The resulting latency includes source execution, connector transit, target write and cursor delivery.
 * It does not isolate time spent in any one of those components.
 *
 * <p>One key names one physical target document. Required writes are matched in registration order; an
 * unmeasured terminal may allow one named refinement whose presence depends on source arrival order.
 * Unregistered or duplicate target writes remain failures.
 */
final class BenchmarkMongoDeliveryObserver implements AutoCloseable {

    private static final String BARRIER_COLLECTION = "_benchmark_delivery_barriers";
    private static final Duration CURSOR_MAX_AWAIT = Duration.ofMillis(100);
    private static final Set<String> TARGET_METADATA = Set.of(
            "create", "createIndexes", "dropIndexes", "modify");

    enum Kind {
        INSERT,
        UPDATE
    }

    record ExpectedChange(String key, Kind kind) {
        ExpectedChange {
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("target change key is required");
            }
            Objects.requireNonNull(kind, "target change kind");
        }
    }

    record Delivery(String key, Kind kind, long issuedAtNanos, long observedAtNanos, long durationNanos) {
        Delivery {
            if (durationNanos < 0) {
                throw new IllegalArgumentException("delivery duration must not be negative");
            }
        }
    }

    /** Stable across fresh databases and application jars; counts repeated physical target writes. */
    record ObservedKey(String phaseId, String targetId, String key, Kind kind) {}

    private record Pending(String phaseId, ExpectedChange change, long issuedAtNanos, boolean measured) {
    }

    private record OptionalChange(String phaseId, ExpectedChange change) {
    }

    private final Object lock = new Object();
    private final String targetId;
    private final String targetCollection;
    private final String databaseName;
    private final Function<Document, String> keyOf;
    private final MongoClient client;
    private final MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor;
    private final Thread reader;
    private final Map<String, ArrayDeque<Pending>> pendingByKey = new HashMap<>();
    private final Map<String, ArrayDeque<OptionalChange>> optionalByKey = new HashMap<>();
    private final List<Delivery> deliveries = new ArrayList<>();
    private final Map<ObservedKey, Long> observedCoverage = new HashMap<>();

    private String activePhase;
    private int phaseExpected;
    private int phaseObserved;
    private int returnedDeliveries;
    private String pendingBarrierId;
    private boolean barrierSeen;
    private boolean checkpointing;
    private boolean legacyFinished;
    private boolean closed;
    private AssertionError failure;

    private BenchmarkMongoDeliveryObserver(String uri, String targetId, String targetCollection,
                                           Function<Document, String> keyOf) {
        ConnectionString address = new ConnectionString(uri);
        this.databaseName = Objects.requireNonNull(address.getDatabase(), "target URI has no database");
        this.targetId = Objects.requireNonNull(targetId, "target identity");
        this.targetCollection = Objects.requireNonNull(targetCollection, "target collection");
        this.keyOf = Objects.requireNonNull(keyOf, "target key extractor");
        this.client = MongoClients.create(address);
        try {
            this.cursor = client.getDatabase(databaseName).watch()
                    .fullDocument(FullDocument.UPDATE_LOOKUP)
                    .maxAwaitTime(CURSOR_MAX_AWAIT.toMillis(), TimeUnit.MILLISECONDS)
                    .cursor();
        } catch (RuntimeException e) {
            client.close();
            throw e;
        }
        this.reader = Thread.ofVirtual().name("benchmark-target-change-stream").start(this::readChanges);
    }

    static BenchmarkMongoDeliveryObserver open(
            BenchmarkWorkloadDefinitions.TargetExpectation target,
            String externalTargetUri,
            String managedViewsUri,
            Function<Document, String> keyOf) {
        String uri = target.location() == BenchmarkWorkloadDefinitions.TargetLocation.MANAGED_VIEW
                ? managedViewsUri : externalTargetUri;
        return new BenchmarkMongoDeliveryObserver(uri, target.pipelineId() + "/" + target.table(),
                target.table(), keyOf);
    }

    /** Register before the corresponding source SQL begins, using its captured monotonic start time. */
    void expectBatch(long issuedAtNanos, List<ExpectedChange> expected) {
        expectBatch("legacy-measured", issuedAtNanos, expected);
    }

    /** Register one measured batch before its source SQL begins. */
    void expectBatch(String phaseId, long issuedAtNanos, List<ExpectedChange> expected) {
        Objects.requireNonNull(expected, "expected target changes");
        if (expected.isEmpty()) {
            throw new IllegalArgumentException("an issued batch must name target changes");
        }
        if (System.nanoTime() - issuedAtNanos < 0) {
            throw new IllegalArgumentException("source batch issue time is in the future");
        }
        register(phaseId, issuedAtNanos, expected, true);
    }

    /** Terminal writes are checked for identity and kind but never enter measured latency samples. */
    void expectUnmeasured(String phaseId, List<ExpectedChange> expected) {
        Objects.requireNonNull(expected, "expected target changes");
        if (expected.isEmpty()) {
            throw new IllegalArgumentException("an unmeasured phase must name target changes");
        }
        register(phaseId, 0, expected, false);
    }

    /** Allows one unmeasured write after a required terminal insert, without requiring it. */
    void allowOptionalUnmeasured(String phaseId, List<ExpectedChange> allowed) {
        Objects.requireNonNull(allowed, "optional terminal target changes");
        synchronized (lock) {
            requireOpenAndReady();
            if (!Objects.equals(activePhase, phaseId)) {
                throw new IllegalStateException("optional changes must join the registered terminal phase");
            }
            for (ExpectedChange change : allowed) {
                if (!pendingByKey.containsKey(change.key())) {
                    throw new IllegalArgumentException("optional change has no required target key");
                }
                optionalByKey.computeIfAbsent(change.key(), ignored -> new ArrayDeque<>())
                        .addLast(new OptionalChange(phaseId, change));
            }
        }
    }

    private void register(String phaseId, long issuedAtNanos, List<ExpectedChange> expected, boolean measured) {
        if (phaseId == null || phaseId.isBlank()) {
            throw new IllegalArgumentException("a target change phase is required");
        }
        synchronized (lock) {
            requireOpenAndReady();
            if (activePhase == null) {
                activePhase = phaseId;
            } else if (!activePhase.equals(phaseId)) {
                throw new IllegalStateException("target changes for " + activePhase + " are not checkpointed");
            }
            for (ExpectedChange change : expected) {
                pendingByKey.computeIfAbsent(change.key(), ignored -> new ArrayDeque<>())
                        .addLast(new Pending(phaseId, change, issuedAtNanos, measured));
                phaseExpected = Math.addExact(phaseExpected, 1);
            }
        }
    }

    /**
     * Reconcile one phase after its source terminal is target-ACKed. The stream remains live across the
     * next phase and through the final terminal ACK; an unregistered write between barriers is a failure.
     * A target with no expected changes may checkpoint an otherwise active phase with zero deliveries.
     */
    List<Delivery> checkpoint(String phaseId, Duration timeout) {
        Objects.requireNonNull(timeout, "checkpoint timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("checkpoint timeout must be positive");
        }
        if (phaseId == null || phaseId.isBlank()) {
            throw new IllegalArgumentException("a checkpoint phase is required");
        }
        String id;
        synchronized (lock) {
            requireOpenAndReady();
            if (activePhase == null) {
                activePhase = phaseId;
            } else if (!activePhase.equals(phaseId)) {
                throw new IllegalStateException("target changes for " + activePhase + " are not checkpointed");
            }
            checkpointing = true;
            pendingBarrierId = UUID.randomUUID().toString();
            barrierSeen = false;
            id = pendingBarrierId;
        }
        MongoCollection<Document> barriers = client.getDatabase(databaseName)
                .getCollection(BARRIER_COLLECTION);
        try {
            barriers.insertOne(new Document("_id", id));
        } catch (RuntimeException writeFailure) {
            synchronized (lock) {
                fail("target change-stream barrier write failed", writeFailure);
                throw failure;
            }
        }

        long deadline = System.nanoTime() + timeout.toNanos();
        List<Delivery> phaseDeliveries;
        synchronized (lock) {
            while (failure == null && !barrierSeen) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new AssertionError("target change-stream barrier did not arrive before " + timeout);
                }
                try {
                    TimeUnit.NANOSECONDS.timedWait(lock, remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted while awaiting target change-stream barrier", e);
                }
            }
            if (failure != null) {
                throw failure;
            }
            if (phaseObserved != phaseExpected) {
                throw new AssertionError("target change-stream reached its barrier with "
                        + (phaseExpected - phaseObserved) + " missing deliveries in " + phaseId);
            }
            phaseDeliveries = List.copyOf(deliveries.subList(returnedDeliveries, deliveries.size()));
            returnedDeliveries = deliveries.size();
            activePhase = null;
            phaseExpected = 0;
            phaseObserved = 0;
            optionalByKey.clear();
            pendingBarrierId = null;
            barrierSeen = false;
            checkpointing = false;
        }
        barriers.deleteOne(Filters.eq("_id", id));
        return phaseDeliveries;
    }

    /** Compatibility path for a single measured phase; callers may migrate to repeated checkpoints. */
    List<Delivery> finish(Duration timeout) {
        synchronized (lock) {
            if (legacyFinished) {
                throw new IllegalStateException("single-phase observer already finished");
            }
        }
        List<Delivery> result = checkpoint("legacy-measured", timeout);
        synchronized (lock) {
            legacyFinished = true;
        }
        return result;
    }

    /** Required target coverage; an allowed terminal refinement does not change fork correctness. */
    Map<ObservedKey, Long> observedCoverage() {
        synchronized (lock) {
            if (failure != null) {
                throw failure;
            }
            if (activePhase != null || checkpointing) {
                throw new IllegalStateException("target change phase has not reached its checkpoint");
            }
            return Map.copyOf(observedCoverage);
        }
    }

    private void readChanges() {
        try {
            while (true) {
                synchronized (lock) {
                    if (closed || failure != null) {
                        return;
                    }
                }
                ChangeStreamDocument<Document> change = cursor.tryNext();
                if (change != null) {
                    accept(change, System.nanoTime());
                }
            }
        } catch (RuntimeException e) {
            synchronized (lock) {
                if (!closed) {
                    fail("target change-stream reader failed", e);
                }
            }
        }
    }

    private void accept(ChangeStreamDocument<Document> change, long observedAtNanos) {
        MongoNamespace namespace = change.getNamespace();
        if (namespace == null || !databaseName.equals(namespace.getDatabaseName())) {
            return;
        }
        OperationType operation = change.getOperationType();
        if (BARRIER_COLLECTION.equals(namespace.getCollectionName())) {
            Document body = change.getFullDocument();
            synchronized (lock) {
                if (operation == OperationType.INSERT && body != null
                        && Objects.equals(pendingBarrierId, body.getString("_id"))) {
                    barrierSeen = true;
                    lock.notifyAll();
                }
            }
            return;
        }
        if (!targetCollection.equals(namespace.getCollectionName())) {
            return;
        }
        // Collection/index metadata is not a row delivery; unknown operations still fail closed.
        if (operation == OperationType.OTHER
                && TARGET_METADATA.contains(change.getOperationTypeString())) {
            return;
        }
        if (operation != OperationType.INSERT && operation != OperationType.UPDATE
                && operation != OperationType.REPLACE) {
            synchronized (lock) {
                fail("unexpected target operation " + operation + " on " + targetCollection, null);
            }
            return;
        }
        Document body = change.getFullDocument();
        if (body == null) {
            synchronized (lock) {
                fail("target change has no full document on " + targetCollection, null);
            }
            return;
        }
        String key;
        try {
            key = keyOf.apply(body);
        } catch (RuntimeException e) {
            synchronized (lock) {
                fail("target key extraction failed on " + targetCollection, e);
            }
            return;
        }
        synchronized (lock) {
            ArrayDeque<Pending> pending = pendingByKey.get(key);
            if (pending == null) {
                fail("unmatched target change for key " + key, null);
                return;
            }
            if (pending.isEmpty()) {
                Kind actual = operation == OperationType.INSERT ? Kind.INSERT : Kind.UPDATE;
                if (!acceptOptional(key, actual)) {
                    fail("extra or duplicate target change for key " + key, null);
                }
                return;
            }
            Pending expected = pending.removeFirst();
            Kind actual = operation == OperationType.INSERT ? Kind.INSERT : Kind.UPDATE;
            if (actual != expected.change().kind()) {
                fail("target operation mismatch for key " + key + ": expected "
                        + expected.change().kind() + " but saw " + actual, null);
                return;
            }
            if (!expected.phaseId().equals(activePhase)) {
                fail("target change belongs to an uncheckpointed phase for key " + key, null);
                return;
            }
            if (expected.measured()) {
                long duration = observedAtNanos - expected.issuedAtNanos();
                if (duration < 0) {
                    fail("target change preceded its source batch for key " + key, null);
                    return;
                }
                deliveries.add(new Delivery(key, actual, expected.issuedAtNanos(), observedAtNanos, duration));
            }
            observedCoverage.merge(new ObservedKey(expected.phaseId(), targetId, key, actual), 1L,
                    Math::addExact);
            phaseObserved = Math.addExact(phaseObserved, 1);
            lock.notifyAll();
        }
    }

    private boolean acceptOptional(String key, Kind actual) {
        ArrayDeque<OptionalChange> allowed = optionalByKey.get(key);
        if (allowed == null || allowed.isEmpty()) {
            return false;
        }
        OptionalChange candidate = allowed.removeFirst();
        if (!candidate.phaseId().equals(activePhase) || candidate.change().kind() != actual) {
            fail("optional terminal change mismatch for key " + key, null);
        }
        return true;
    }

    private void fail(String message, Throwable cause) {
        if (failure == null) {
            failure = new AssertionError(message, cause);
            lock.notifyAll();
        }
    }

    private void requireOpenAndReady() {
        if (closed || legacyFinished) {
            throw new IllegalStateException("target change-stream window is closed");
        }
        if (checkpointing) {
            throw new IllegalStateException("target change-stream checkpoint is in progress");
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            lock.notifyAll();
        }
        try {
            cursor.close();
        } finally {
            try {
                reader.join(Duration.ofSeconds(2));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                try {
                    if (pendingBarrierId != null) {
                        client.getDatabase(databaseName).getCollection(BARRIER_COLLECTION)
                                .deleteOne(Filters.eq("_id", pendingBarrierId));
                    }
                } finally {
                    client.close();
                }
            }
        }
    }
}
