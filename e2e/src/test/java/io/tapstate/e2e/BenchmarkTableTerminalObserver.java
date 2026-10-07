package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import io.tapstate.adapters.mongostore.MongoStorePort;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Retains only registered operation-time table markers, independently of later log retention. */
final class BenchmarkTableTerminalObserver implements AutoCloseable {
    record Marker(String id, String ring, String op, long rowId, String field, Object value) {
        Marker {
            if (id == null || id.isBlank() || id.length() > 256 || ring == null || ring.isBlank()
                    || ring.length() > 512 || !List.of("i", "u").contains(op) || rowId < 0
                    || field == null || !field.matches("[A-Za-z_][A-Za-z0-9_]{0,127}")
                    || !(value instanceof Number || value instanceof String text && text.length() <= 256)) {
                throw new IllegalArgumentException("a bounded exact table marker is required");
            }
        }
    }

    record Point(String markerId, String ring, long epoch, long seq, String sourceToken) { }
    private record Key(String ring, long seq) { }
    private final MongoClient client;
    private final MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor;
    private final Map<String, Marker> markers = new LinkedHashMap<>();
    private final Map<String, Point> captured = new LinkedHashMap<>();
    private final Map<Key, String> keys = new LinkedHashMap<>();
    private final Thread reader;
    private AssertionError failure;
    private AssertionError closeFailure;
    private boolean closed;
    private boolean terminated;
    private long events;
    private long eventBytes;
    private long pollCalls;
    private long pollNanos;
    private final Map<String, Map<String, Object>> provenance = new LinkedHashMap<>();

    static BenchmarkTableTerminalObserver open(String uri, String database, List<Marker> markers) {
        return new BenchmarkTableTerminalObserver(uri, database, markers);
    }

    private BenchmarkTableTerminalObserver(String uri, String database, List<Marker> registered) {
        if (registered == null || registered.isEmpty() || registered.size() > 16) {
            throw new IllegalArgumentException("table marker registration must contain one to sixteen entries");
        }
        List<Bson> selected = new ArrayList<>();
        for (Marker marker : registered) {
            if (markers.putIfAbsent(marker.id(), marker) != null) {
                throw new IllegalArgumentException("duplicate registered marker identity");
            }
            if (markers.values().stream().filter(other -> other.ring().equals(marker.ring())
                    && other.op().equals(marker.op()) && other.rowId() == marker.rowId()
                    && other.field().equals(marker.field()) && same(other.value(), marker.value())).count() != 1) {
                throw new IllegalArgumentException("marker selectors must be unambiguous");
            }
            // INSERT and REPLACE carry their event-time image. UPDATE is retained only to reject rewrites.
            selected.add(Filters.and(Filters.eq("documentKey._id.ring", marker.ring()),
                    Filters.or(Filters.and(Filters.eq("operationType", "insert"),
                                    Filters.or(Filters.eq("fullDocument.after.id", marker.rowId()),
                                            Filters.eq("fullDocument.after.id.__tapstate_carried", marker.rowId()))),
                            Filters.in("operationType", "replace", "update"))));
        }
        client = MongoClients.create(uri);
        try {
            cursor = client.getDatabase(database).getCollection(MongoStorePort.SRS_LOG)
                    .watch(List.of(Aggregates.match(Filters.or(Filters.or(selected),
                            Filters.in("operationType", "drop", "rename", "dropDatabase", "invalidate")))))
                    .maxAwaitTime(100, TimeUnit.MILLISECONDS).batchSize(16).cursor();
        } catch (RuntimeException | Error startFailure) {
            client.close();
            throw startFailure;
        }
        reader = Thread.ofVirtual().name("benchmark-table-terminal-observer").start(this::read);
    }

    private void read() {
        try {
            while (true) {
                synchronized (this) { if (closed || failure != null) { return; } }
                long started = System.nanoTime();
                ChangeStreamDocument<Document> event = cursor.tryNext();
                synchronized (this) { pollCalls++; pollNanos += System.nanoTime() - started; }
                if (event != null) { accept(event); }
            }
        } catch (RuntimeException | AssertionError observerFailure) {
            synchronized (this) {
                if (!closed) { failure = new AssertionError("table marker observation failed", observerFailure); }
                notifyAll();
            }
        }
    }

    private synchronized void accept(ChangeStreamDocument<Document> event) {
        if (++events > 64) { throw new AssertionError("table marker event budget exceeded"); }
        if (List.of("drop", "rename", "dropDatabase", "invalidate").contains(event.getOperationTypeString())) {
            throw new AssertionError("table marker observation stream was invalidated");
        }
        if (event.getDocumentKey() == null || event.getDocumentKey().get("_id") == null
                || !event.getDocumentKey().get("_id").isDocument()) {
            throw new AssertionError("table marker has no exact log key");
        }
        var rawKey = event.getDocumentKey().getDocument("_id");
        if (!rawKey.containsKey("ring") || !rawKey.get("ring").isString()
                || !rawKey.containsKey("seq") || !(rawKey.get("seq").isInt32() || rawKey.get("seq").isInt64())) {
            throw new AssertionError("table marker log key is incomplete");
        }
        Key key = new Key(rawKey.getString("ring").getValue(), rawKey.getNumber("seq").longValue());
        if (keys.containsKey(key)) { throw new AssertionError("a retained marker log key was rewritten"); }
        if (event.getOperationType() == OperationType.UPDATE) {
            return;
        }
        if (event.getOperationType() != OperationType.INSERT && event.getOperationType() != OperationType.REPLACE) {
            throw new AssertionError("unexpected table marker operation");
        }
        Document record = event.getFullDocument();
        if (record == null || !(record.get("after") instanceof Document after)) {
            throw new AssertionError("table marker has no operation-time row image");
        }
        int bytes = record.toJson().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        if (bytes > 65_536 || eventBytes + bytes > 2 * 1024 * 1024) {
            throw new AssertionError("table marker evidence exceeded its retained byte budget");
        }
        eventBytes += bytes;
        if (!(record.get("_id") instanceof Document storedKey)
                || !key.ring().equals(storedKey.get("ring"))
                || !(storedKey.get("seq") instanceof Integer || storedKey.get("seq") instanceof Long)
                || !(storedKey.get("seq") instanceof Number storedSeq)
                || storedSeq.longValue() != key.seq()) {
            throw new AssertionError("operation-time marker image differs from its exact log key");
        }
        Object rowId = portable(after.get("id"));
        for (Marker marker : markers.values()) {
            if (!marker.ring().equals(key.ring()) || !same(rowId, marker.rowId())) { continue; }
            if (!marker.op().equals(record.getString("op"))
                    || !same(portable(after.get(marker.field())), marker.value())) { continue; }
            if (!(record.get("epoch") instanceof Integer || record.get("epoch") instanceof Long)
                    || !(record.get("epoch") instanceof Number epoch) || epoch.longValue() < 1 || key.seq() < 0) {
                throw new AssertionError("marker has no proven original capture order");
            }
            Object token = record.get("srcToken");
            if (token != null && (!(token instanceof String text) || text.isBlank() || text.length() > 65_536)) {
                throw new AssertionError("marker source token is invalid or exceeds its bound");
            }
            if (captured.containsKey(marker.id()) || keys.putIfAbsent(key, marker.id()) != null) {
                throw new AssertionError("duplicate table marker or reused log key");
            }
            captured.put(marker.id(), new Point(marker.id(), key.ring(), epoch.longValue(), key.seq(), (String) token));
            if (event.getResumeToken() == null || event.getClusterTime() == null) {
                throw new AssertionError("table marker event has no stream provenance");
            }
            String resume = event.getResumeToken().toJson();
            if (resume.length() > 4096) { throw new AssertionError("table marker resume token exceeded its bound"); }
            provenance.put(marker.id(), Map.of("resumeToken", resume, "clusterTime", event.getClusterTime().toString(),
                    "operation", event.getOperationTypeString(), "operationTimeRecordBytes", bytes,
                    "recordSha256", sha256(record.toJson(org.bson.json.JsonWriterSettings.builder()
                            .outputMode(org.bson.json.JsonMode.EXTENDED).build()))));
            notifyAll();
        }
    }

    synchronized Point await(String markerId, Duration timeout) throws InterruptedException {
        if (!markers.containsKey(markerId) || timeout == null || timeout.isZero() || timeout.isNegative()
                || timeout.compareTo(Duration.ofMinutes(5)) > 0) {
            throw new IllegalArgumentException("registered marker and positive bounded wait required");
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!captured.containsKey(markerId)) {
            check();
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) { throw new AssertionError("the exact source marker never entered the durable table log"); }
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
        }
        check();
        return captured.get(markerId);
    }

    synchronized void check() {
        if (failure != null) { throw failure; }
        if (closed) { throw new AssertionError("table marker observer is already closed"); }
    }

    synchronized Map<String, Object> evidence() {
        check();
        return Map.of("registeredMarkers", markers.size(), "retainedMarkers", captured.size(),
                "events", events, "recordBytes", eventBytes, "pollCalls", pollCalls, "pollNanos", pollNanos,
                "costScope", "EXTERNAL_CHANGE_STREAM_OBSERVER", "markers", Map.copyOf(provenance));
    }

    private static String sha256(String value) {
        try {
            return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }

    private static Object portable(Object value) {
        return value instanceof Document carried && carried.containsKey("__tapstate_carried")
                ? carried.get("__tapstate_carried") : value;
    }

    private static boolean same(Object left, Object right) {
        return left instanceof Number && right instanceof Number
                ? new BigDecimal(left.toString()).compareTo(new BigDecimal(right.toString())) == 0
                : Objects.equals(left, right);
    }

    @Override
    public synchronized void close() {
        if (terminated) {
            if (closeFailure != null) { throw closeFailure; }
            return;
        }
        closed = true;
        notifyAll();
        try { cursor.close(); }
        catch (RuntimeException | AssertionError rejected) {
            closeFailure = new AssertionError("table marker cursor close failed", rejected);
        }
        // Release the monitor while joining: the reader must reacquire it to observe closed.
        try {
            long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
            while (reader.isAlive() && System.nanoTime() < deadline) { wait(20); }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            if (closeFailure == null) { closeFailure = new AssertionError("table marker close was interrupted", interrupted); }
            else { closeFailure.addSuppressed(interrupted); }
        }
        try { client.close(); }
        catch (RuntimeException | AssertionError rejected) {
            if (closeFailure == null) { closeFailure = new AssertionError("table marker client close failed", rejected); }
            else { closeFailure.addSuppressed(rejected); }
        }
        terminated = !reader.isAlive();
        if (!terminated) {
            AssertionError timeout = new AssertionError("table marker observer did not terminate within its close budget");
            if (closeFailure == null) { closeFailure = timeout; } else { closeFailure.addSuppressed(timeout); }
        }
        if (failure != null) {
            if (closeFailure != null && closeFailure != failure) { failure.addSuppressed(closeFailure); }
            closeFailure = failure;
        }
        if (closeFailure != null) { throw closeFailure; }
    }
}
