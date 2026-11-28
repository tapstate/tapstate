package io.tapstate.e2e;

import com.mongodb.WriteConcern;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/** An owned fixture delays only writes made by the target connector. */
final class BenchmarkTargetLimit implements AutoCloseable {
    private final MongoDatabase admin;
    private final String application;
    private final String namespace;
    private final int blockMillis;
    private boolean armed;
    private Long firstEntry;
    private long armedAtNanos;

    BenchmarkTargetLimit(MongoDatabase admin, String application, String namespace, int blockMillis) {
        this.admin = admin.withTimeout(3, TimeUnit.SECONDS);
        this.application = application;
        this.namespace = namespace;
        this.blockMillis = blockMillis;
    }

    static MongoDBContainer newReplicaSet() {
        var container = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--setParameter", "enableTestCommands=1");
        String owner = System.getProperty("tapstate.e2e.resource-owner");
        if (owner != null) {
            container.withLabel("io.tapstate.e2e.resource-owner", owner);
        }
        return container;
    }

    void arm() {
        arm(List.of("insert", "update", "delete"));
    }

    void holdSnapshotWrites() {
        arm(List.of("insert", "update"));
    }

    private void arm(List<String> commands) {
        var data = new Document("failCommands", commands)
                .append("appName", application).append("namespace", namespace)
                .append("blockConnection", true).append("blockTimeMS", blockMillis);
        armedAtNanos = System.nanoTime();
        armed = true;
        var response = admin.runCommand(new Document("configureFailPoint", "failCommand")
                .append("mode", "alwaysOn").append("data", data));
        firstEntry = count(response);
    }

    void awaitHeldWrite(Duration timeout) {
        // The driver's client operation timeout supplies maxTimeMS to the server.
        admin.withTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
                .runCommand(new Document("waitForFailPoint", "failCommand").append("timesEntered", firstEntry + 1));
    }

    Set<String> heldSnapshotWrites() {
        var answer = admin.runCommand(new Document("currentOp", 1).append("active", true)
                .append("appName", application));
        var result = new HashSet<String>();
        int separator = namespace.indexOf('.');
        String database = namespace.substring(0, separator);
        String collection = namespace.substring(separator + 1);
        for (Document operation : answer.getList("inprog", Document.class, List.of())) {
            var command = operation.get("command", Document.class);
            if (command != null && database.equals(command.getString("$db"))
                    && (collection.equals(command.getString("insert")) || collection.equals(command.getString("update")))
                    && operation.get("opid") != null) {
                result.add(operation.get("opid").toString());
            }
        }
        return Set.copyOf(result);
    }

    Set<String> overlappingWrites(Set<String> originalOperations) {
        if (originalOperations.isEmpty()) return Set.of();
        var current = heldSnapshotWrites();
        // A write cannot enter this hold before the fixture arms it.
        if (System.nanoTime() - armedAtNanos >= TimeUnit.MILLISECONDS.toNanos(blockMillis)) return Set.of();
        var overlap = new HashSet<>(originalOperations);
        overlap.retainAll(current);
        return Set.copyOf(overlap);
    }

    long release() {
        return disarm() - firstEntry;
    }

    private long disarm() {
        // The server can await commands already inside the failpoint before updating its mode.
        var response = admin.withTimeout(blockMillis + 3_000L, TimeUnit.MILLISECONDS)
                .runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
        armed = false;
        return count(response);
    }

    private static long count(Document response) {
        return response.get("count", Number.class).longValue();
    }

    /** Counts majority-visible target writes at a server-side boundary before the limit is released. */
    static final class TargetWindow implements AutoCloseable {
        private static final String FRONTIER = "benchmark_target_frontier";
        private static final int ITERATION_MILLIS = 500;
        private static final long MARKER_TIMEOUT_SECONDS = 3;
        private static final long READER_TIMEOUT_SECONDS = 30;
        private final MongoDatabase database;
        private final String target;
        private MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor;

        TargetWindow(MongoDatabase database, MongoCollection<BsonDocument> target) {
            this.database = database.withTimeout(MARKER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            this.target = target.getNamespace().getCollectionName();
        }

        void begin() {
            database.createCollection(FRONTIER);
            cursor = database.withTimeout(ITERATION_MILLIS, TimeUnit.MILLISECONDS)
                    .watch(List.of(Aggregates.match(Filters.in("ns.coll", target, FRONTIER))),
                    BsonDocument.class).maxAwaitTime(200, TimeUnit.MILLISECONDS).batchSize(128).cursor();
        }

        record Frontier(long eventsRead, long markerWriteNanos, Long cursorReadNanos, String error) {
        }

        Frontier countThroughBoundary() {
            long markerStart = System.nanoTime();
            try {
                database.getCollection(FRONTIER, BsonDocument.class).withWriteConcern(WriteConcern.MAJORITY)
                        .insertOne(new BsonDocument("_id", new BsonInt32(1)));
            } catch (MongoOperationTimeoutException timeout) {
                return new Frontier(0, System.nanoTime() - markerStart, null,
                        "Timed out writing the majority-visible target frontier marker");
            }
            long readerStart = System.nanoTime();
            long markerNanos = readerStart - markerStart;
            // Drain the existing cursor to the stop-time marker without moving the counting boundary.
            long deadline = readerStart + TimeUnit.SECONDS.toNanos(READER_TIMEOUT_SECONDS);
            long count = 0;
            try {
                while (deadline - System.nanoTime() > TimeUnit.MILLISECONDS.toNanos(ITERATION_MILLIS)) {
                    var event = cursor.tryNext();
                    if (event != null) {
                        if (event.getNamespace().getCollectionName().equals(FRONTIER)) {
                            return new Frontier(count, markerNanos, System.nanoTime() - readerStart, null);
                        }
                        count++;
                    }
                }
            } catch (MongoOperationTimeoutException timeout) {
                return new Frontier(count, markerNanos, System.nanoTime() - readerStart,
                        "Target change stream poll timed out before the majority-visible frontier");
            }
            return new Frontier(count, markerNanos, System.nanoTime() - readerStart,
                    "Target change stream did not reach its majority-visible frontier before the reader deadline");
        }

        @Override
        public void close() {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public void close() {
        if (armed) {
            disarm();
        }
    }
}
