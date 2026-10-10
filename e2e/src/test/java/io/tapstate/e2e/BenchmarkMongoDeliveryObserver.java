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
import io.tapstate.core.common.JsonWriter;

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
import java.util.function.Supplier;

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
    static final String CLOCK_REJECTION_EVIDENCE_PROPERTY = "tapstate.e2e.benchmark.operation-clock-rejection-evidence";
    static final String NATIVE_OPERATION_WALL_EVIDENCE_PROPERTY = "tapstate.e2e.benchmark.native-operation-wall-evidence";

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

    record Delivery(String key, Kind kind, long issuedAtNanos, long observedAtNanos, long durationNanos,
                    Long serverOperationWallMillis) {
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
    private final List<Map<String, Object>> slowReads = new ArrayList<>();
    private long readCalls;
    private long readNanos;
    private long acceptNanos;
    private long previousIterationEnded;
    private final OperationOrder operationOrder = new OperationOrder();
    private final BenchmarkTargetClock.WallSamples operationWalls = new BenchmarkTargetClock.WallSamples();
    private final BenchmarkWitnessReadGate readGate;
    private BenchmarkOperationClockEvidence clockRejectionEvidence;
    private Throwable clockRecordingFailure;
    private boolean nativeOperationWallEvidenceRequested;

    static final class OperationOrder {
        private org.bson.BsonTimestamp previous;

        boolean accept(org.bson.BsonTimestamp current) {
            if (current == null || previous != null && current.compareTo(previous) < 0) { return false; }
            previous = current;
            return true;
        }

        org.bson.BsonTimestamp previousClusterTime() { return previous; }
    }

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
                                           Function<Document, String> keyOf, boolean deferred) {
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
        this.readGate = new BenchmarkWitnessReadGate(deferred);
        this.reader = Thread.ofVirtual().name("benchmark-target-change-stream").start(this::readChanges);
    }

    static BenchmarkMongoDeliveryObserver open(
            BenchmarkWorkloadDefinitions.TargetExpectation target,
            String externalTargetUri,
            String managedViewsUri,
            Function<Document, String> keyOf) {
        return open(target, externalTargetUri, managedViewsUri, keyOf, false);
    }

    static BenchmarkMongoDeliveryObserver open(
            BenchmarkWorkloadDefinitions.TargetExpectation target,
            String externalTargetUri,
            String managedViewsUri,
            Function<Document, String> keyOf, boolean deferred) {
        String uri = target.location() == BenchmarkWorkloadDefinitions.TargetLocation.MANAGED_VIEW
                ? managedViewsUri : externalTargetUri;
        return new BenchmarkMongoDeliveryObserver(uri, target.pipelineId() + "/" + target.table(),
                target.table(), keyOf, deferred);
    }

    void releaseAfterOwnMeasuredAck(long acknowledgedAtNanos) {
        readGate.releaseAfterOwnAck(acknowledgedAtNanos);
    }

    Map<String, Object> readScheduleEvidence() {
        synchronized (lock) {
            if (clockRejectionEvidence == null) { return readGate.evidence(); }
            var out = new java.util.LinkedHashMap<>(readGate.evidence());
            out.put("operationClockRefusalEvidence", clockRejectionEvidence.evidence());
            if (nativeOperationWallEvidenceRequested) { out.put("nativeOperationWallEvidenceRequested", true); }
            return java.util.Collections.unmodifiableMap(out);
        }
    }
    boolean readDeferred() { return readGate.deferred(); }

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
                clockRejectionEvidence = null;
                clockRecordingFailure = null;
                nativeOperationWallEvidenceRequested = false;
                if (measured && Boolean.getBoolean(CLOCK_REJECTION_EVIDENCE_PROPERTY)) {
                    clockRejectionEvidence = new BenchmarkOperationClockEvidence(
                            databaseName + "." + targetCollection, targetId, phaseId);
                    nativeOperationWallEvidenceRequested = Boolean.getBoolean(NATIVE_OPERATION_WALL_EVIDENCE_PROPERTY);
                }
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
            readGate.completedPhase(phaseId, phaseExpected, phaseObserved);
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
            if (!readGate.awaitFirstRead()) { return; }
            boolean firstRead = true;
            while (true) {
                synchronized (lock) {
                    if (closed || failure != null) {
                        return;
                    }
                }
                long started = System.nanoTime();
                if (firstRead) {
                    if (!readGate.beforeFirstTryNext(started)) { return; }
                    firstRead = false;
                }
                long schedulingGap = previousIterationEnded == 0 ? 0 : started - previousIterationEnded;
                ChangeStreamDocument<Document> change = cursor.tryNext();
                long completed = System.nanoTime();
                if (change != null) {
                    accept(change, started, completed);
                }
                long accepted = System.nanoTime();
                previousIterationEnded = accepted;
                synchronized (lock) {
                    readCalls++; readNanos += completed - started; acceptNanos += accepted - completed;
                    if ((schedulingGap >= TimeUnit.MILLISECONDS.toNanos(50)
                            || completed - started >= TimeUnit.MILLISECONDS.toNanos(150)
                            || accepted - completed >= TimeUnit.MILLISECONDS.toNanos(50)) && slowReads.size() < 256) {
                        slowReads.add(Map.of("startedAtNanos", started, "completedAtNanos", completed,
                                "acceptCompletedAtNanos", accepted, "hadEvent", change != null,
                                "beforeReadSchedulingGapNanos", schedulingGap,
                                "observerJvmGcCollectionMillis", java.lang.management.ManagementFactory.getGarbageCollectorMXBeans()
                                        .stream().mapToLong(bean -> Math.max(0, bean.getCollectionTime())).sum()));
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            synchronized (lock) {
                if (!closed) { fail("target change-stream read gate interrupted", e); }
            }
        } catch (RuntimeException | AssertionError e) {
            synchronized (lock) {
                if (!closed) {
                    fail("target change-stream reader failed", e);
                }
            }
        }
    }

    Map<String, Object> diagnosticReadCosts() {
        synchronized (lock) {
            return Map.of("target", targetId, "readCalls", readCalls, "readNanos", readNanos,
                    "acceptNanos", acceptNanos, "slowReads", List.copyOf(slowReads),
                    "scope", "EXTERNAL_TARGET_OBSERVER_READ_AND_MATCH_PROCESSING");
        }
    }

    private void accept(ChangeStreamDocument<Document> change, long startedReadNanos, long observedAtNanos) {
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
                Long operationWall = change.getWallTime() == null ? null : change.getWallTime().getValue();
                if (!operationOrder.accept(change.getClusterTime())) {
                    fail("target logical operation time is missing or moved backward within its actual change stream"
                            + "; namespace=" + namespace + "; target=" + targetId + "; phase=" + activePhase
                            + "; key=" + key + "; previousClusterTime=" + operationOrder.previousClusterTime()
                            + "; currentWallMillis=" + operationWall + "; clusterTime=" + change.getClusterTime()
                            + "; observedAtNanos=" + observedAtNanos, null); return;
                }
                if (BenchmarkTargetClock.rejectOperationDate(operationWall, operationWalls,
                        clockRejectionEvidence != null)) {
                    AssertionError original = new AssertionError("target operation clock is missing or moved backward beyond clock uncertainty"
                            + "; namespace=" + namespace + "; target=" + targetId + "; phase=" + activePhase
                            + "; key=" + key + "; highWaterMillis=" + operationWalls.highWaterMillis()
                            + "; currentWallMillis=" + operationWall + "; clusterTime=" + change.getClusterTime()
                            + "; uncertaintyMillis=" + BenchmarkTargetClock.ENDPOINT_RESOLUTION_ERROR_MILLIS
                            + BenchmarkTargetClock.OPERATION_DATE_REFUSAL_SCOPE);
                    if (clockRejectionEvidence != null) {
                        recordRejectedClockEvent(clockRejectionEvidence, clockRecordingFailure,
                                () -> clockEvent(change, key, startedReadNanos, observedAtNanos, null), original);
                        try { System.out.println("benchmark-operation-clock-refusal=" + JsonWriter.write(clockRejectionEvidence.evidence())); }
                        catch (RuntimeException | Error recording) { if (recording != original) { original.addSuppressed(recording); } }
                    }
                    if (failure == null) { failure = original; lock.notifyAll(); }
                    return;
                }
                if (clockRejectionEvidence != null && clockRecordingFailure == null) {
                    clockRecordingFailure = recordAcceptedClockEvent(clockRejectionEvidence,
                            () -> clockEvent(change, key, startedReadNanos, observedAtNanos, System.nanoTime()));
                }
                deliveries.add(new Delivery(key, actual, expected.issuedAtNanos(), observedAtNanos, duration, operationWall));
            }
            observedCoverage.merge(new ObservedKey(expected.phaseId(), targetId, key, actual), 1L,
                    Math::addExact);
            phaseObserved = Math.addExact(phaseObserved, 1);
            lock.notifyAll();
        }
    }

    static Throwable recordAcceptedClockEvent(BenchmarkOperationClockEvidence recorder,
            Supplier<Map<String, Object>> event) {
        try { recorder.accepted(event.get()); return null; }
        catch (RuntimeException | Error recording) {
            try { recorder.recordingFailed(recording); }
            catch (RuntimeException | Error freezing) { if (freezing != recording) { recording.addSuppressed(freezing); } }
            return recording;
        }
    }

    static void recordRejectedClockEvent(BenchmarkOperationClockEvidence recorder, Throwable priorRecordingFailure,
            Supplier<Map<String, Object>> event, AssertionError original) {
        if (priorRecordingFailure != null && priorRecordingFailure != original) {
            original.addSuppressed(priorRecordingFailure);
        }
        try {
            if (priorRecordingFailure == null) { recorder.rejected(event.get(), original.getMessage()); }
            else { recorder.authoritativeRejectionAfterRecordingFailure(original.getMessage()); }
        } catch (RuntimeException | Error recording) {
            if (recording != original) { original.addSuppressed(recording); }
            try {
                recorder.recordingFailed(recording);
                recorder.authoritativeRejectionAfterRecordingFailure(original.getMessage());
            } catch (RuntimeException | Error freezing) { if (freezing != original) { original.addSuppressed(freezing); } }
        }
    }

    /** Decoded native event metadata only; no document payload or additional target read. */
    private Map<String, Object> clockEvent(ChangeStreamDocument<Document> change, String key,
            long startedReadNanos, long observedNanos, Long acceptedNanos) {
        Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("namespace", change.getNamespace().getFullName()); out.put("targetId", targetId);
        out.put("phaseId", activePhase); out.put("key", key); out.put("operationType", change.getOperationTypeString());
        var timestamp = change.getClusterTime();
        out.put("clusterTime", timestamp == null ? Map.of("status", "MISSING")
                : Map.of("seconds", Integer.toUnsignedLong(timestamp.getTime()), "increment", Integer.toUnsignedLong(timestamp.getInc())));
        out.put("wallTime", change.getWallTime() == null ? Map.of("status", "MISSING") : change.getWallTime().getValue());
        out.put("resumeToken", change.getResumeToken() == null ? Map.of("status", "MISSING") : change.getResumeToken().toJson());
        if (nativeOperationWallEvidenceRequested) {
            out.put("documentKey", change.getDocumentKey() == null ? Map.of("status", "MISSING")
                    : change.getDocumentKey().toJson(org.bson.json.JsonWriterSettings.builder()
                            .outputMode(org.bson.json.JsonMode.EXTENDED).build()));
        }
        out.put("startedReadNanos", startedReadNanos); out.put("completedReadNanos", observedNanos);
        out.put("observedNanos", observedNanos); out.put("acceptedNanos", acceptedNanos);
        return java.util.Collections.unmodifiableMap(out);
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

    /** No native metadata query runs until the original reader has stopped with a clock refusal. */
    private void retainNativeOperationWalls(boolean readerStopped) {
        Map<String, Object> snapshot;
        AssertionError original;
        synchronized (lock) {
            if (!nativeOperationWallEvidenceRequested) { return; }
            snapshot = clockRejectionEvidence == null ? null : clockRejectionEvidence.evidence();
            original = failure;
        }
        try {
            Map<String, Object> evidence;
            if (!readerStopped || snapshot == null || original == null || !"REJECTED".equals(snapshot.get("state"))) {
                evidence = Map.of("state", "UNKNOWN", "reason", "NO_COMPLETE_STOPPED_READER_CLOCK_REFUSAL",
                        "lookupAttempts", 0, "rootCause", "UNKNOWN", "performanceAcceptanceEligible", false);
            } else {
                evidence = BenchmarkNativeOperationWallEvidence.lookup(snapshot,
                        new BenchmarkNativeOperationWallLookup(client, databaseName + "." + targetCollection),
                        System::nanoTime, original);
            }
            System.out.println("benchmark-native-operation-wall=" + JsonWriter.write(evidence));
        } catch (RuntimeException | Error recording) {
            if (original != null && recording != original) { original.addSuppressed(recording); }
            try {
                System.out.println("benchmark-native-operation-wall=" + JsonWriter.write(Map.of(
                        "state", "UNKNOWN", "reason", "NATIVE_METADATA_RETENTION_FAILED",
                        "failureType", recording.getClass().getSimpleName(), "rootCause", "UNKNOWN",
                        "performanceAcceptanceEligible", false)));
            } catch (RuntimeException | Error printing) {
                if (original != null && printing != original) { original.addSuppressed(printing); }
            }
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
        readGate.close();
        try {
            cursor.close();
        } finally {
            boolean readerStopped = false;
            try {
                readerStopped = reader.join(Duration.ofSeconds(2));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                try {
                    retainNativeOperationWalls(readerStopped);
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
