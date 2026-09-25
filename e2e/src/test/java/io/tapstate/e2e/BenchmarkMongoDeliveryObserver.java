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
 * <p>One key names one physical target document. Repeated writes to that document are matched against
 * the key's expectations in registration order. The caller must know every insert/update owed by the
 * workload; an unregistered target write is a failure, including an extra write of a known key.
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

    private record Pending(ExpectedChange change, long issuedAtNanos) {
    }

    private final Object lock = new Object();
    private final String targetCollection;
    private final String databaseName;
    private final Function<Document, String> keyOf;
    private final MongoClient client;
    private final MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor;
    private final Thread reader;
    private final Map<String, ArrayDeque<Pending>> pendingByKey = new HashMap<>();
    private final List<Delivery> deliveries = new ArrayList<>();

    private int expectedCount;
    private String barrierId;
    private boolean barrierSeen;
    private boolean sealed;
    private boolean closed;
    private AssertionError failure;

    private BenchmarkMongoDeliveryObserver(String uri, String targetCollection,
                                           Function<Document, String> keyOf) {
        ConnectionString address = new ConnectionString(uri);
        this.databaseName = Objects.requireNonNull(address.getDatabase(), "target URI has no database");
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
        return new BenchmarkMongoDeliveryObserver(uri, target.table(), keyOf);
    }

    /** Register before the corresponding source SQL begins, using its captured monotonic start time. */
    void expectBatch(long issuedAtNanos, List<ExpectedChange> expected) {
        Objects.requireNonNull(expected, "expected target changes");
        if (expected.isEmpty()) {
            throw new IllegalArgumentException("an issued batch must name target changes");
        }
        if (System.nanoTime() - issuedAtNanos < 0) {
            throw new IllegalArgumentException("source batch issue time is in the future");
        }
        synchronized (lock) {
            requireOpenAndUnsealed();
            for (ExpectedChange change : expected) {
                pendingByKey.computeIfAbsent(change.key(), ignored -> new ArrayDeque<>())
                        .addLast(new Pending(change, issuedAtNanos));
                expectedCount++;
            }
        }
    }

    /**
     * Call only after the target-ACK counter covers every expected output in this measured phase. The
     * barrier is an acknowledged write in the same database stream as the target changes, so seeing it
     * proves all earlier target writes in that stream have been inspected. The source terminal ACK and
     * final target count/checksum are checked separately after the measured phases.
     */
    List<Delivery> finish(Duration timeout) {
        Objects.requireNonNull(timeout, "finish timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("finish timeout must be positive");
        }
        synchronized (lock) {
            requireOpenAndUnsealed();
            if (expectedCount == 0) {
                throw new IllegalStateException("no target changes registered");
            }
            sealed = true;
            barrierId = UUID.randomUUID().toString();
        }
        MongoCollection<Document> barriers = client.getDatabase(databaseName)
                .getCollection(BARRIER_COLLECTION);
        barriers.insertOne(new Document("_id", barrierId));

        long deadline = System.nanoTime() + timeout.toNanos();
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
            if (deliveries.size() != expectedCount) {
                throw new AssertionError("target change-stream reached its barrier with "
                        + (expectedCount - deliveries.size()) + " missing deliveries");
            }
            return List.copyOf(deliveries);
        }
    }

    private void readChanges() {
        try {
            while (true) {
                synchronized (lock) {
                    if (closed || failure != null || barrierSeen) {
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
                        && Objects.equals(barrierId, body.getString("_id"))) {
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
                fail("extra or duplicate target change for key " + key, null);
                return;
            }
            Pending expected = pending.removeFirst();
            Kind actual = operation == OperationType.INSERT ? Kind.INSERT : Kind.UPDATE;
            if (actual != expected.change().kind()) {
                fail("target operation mismatch for key " + key + ": expected "
                        + expected.change().kind() + " but saw " + actual, null);
                return;
            }
            long duration = observedAtNanos - expected.issuedAtNanos();
            if (duration < 0) {
                fail("target change preceded its source batch for key " + key, null);
                return;
            }
            deliveries.add(new Delivery(key, actual, expected.issuedAtNanos(), observedAtNanos, duration));
            lock.notifyAll();
        }
    }

    private void fail(String message, Throwable cause) {
        if (failure == null) {
            failure = new AssertionError(message, cause);
            lock.notifyAll();
        }
    }

    private void requireOpenAndUnsealed() {
        if (closed || sealed) {
            throw new IllegalStateException("target change-stream window is closed");
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
                    if (barrierId != null) {
                        client.getDatabase(databaseName).getCollection(BARRIER_COLLECTION)
                                .deleteOne(Filters.eq("_id", barrierId));
                    }
                } finally {
                    client.close();
                }
            }
        }
    }
}
