package io.tapstate.e2e;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import java.util.List;
import java.util.OptionalLong;
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
    private Long firstEntry;

    BenchmarkTargetLimit(MongoDatabase admin, String application, String namespace, int blockMillis) {
        this.admin = admin;
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
        var data = new Document("failCommands", List.of("insert", "update", "delete"))
                .append("appName", application).append("namespace", namespace)
                .append("blockConnection", true).append("blockTimeMS", blockMillis);
        var response = admin.runCommand(new Document("configureFailPoint", "failCommand")
                .append("mode", "alwaysOn").append("data", data));
        firstEntry = count(response);
    }

    long release() {
        var response = admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
        long delayed = count(response) - firstEntry;
        firstEntry = null;
        return delayed;
    }

    private static long count(Document response) {
        return response.get("count", Number.class).longValue();
    }

    /** Counts majority-visible target writes at a server-side boundary before the limit is released. */
    static final class TargetWindow implements AutoCloseable {
        private static final String FRONTIER = "benchmark_target_frontier";
        private final MongoDatabase database;
        private final String target;
        private MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor;

        TargetWindow(MongoDatabase database, MongoCollection<BsonDocument> target) {
            this.database = database;
            this.target = target.getNamespace().getCollectionName();
        }

        void begin() {
            database.createCollection(FRONTIER);
            cursor = database.watch(List.of(Aggregates.match(Filters.in("ns.coll", target, FRONTIER))),
                    BsonDocument.class).maxAwaitTime(200, TimeUnit.MILLISECONDS).batchSize(128).cursor();
        }

        OptionalLong countThroughBoundary() {
            database.getCollection(FRONTIER, BsonDocument.class).withWriteConcern(WriteConcern.MAJORITY)
                    .insertOne(new BsonDocument("_id", new BsonInt32(1)));
            long count = 0;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (System.nanoTime() < deadline) {
                var event = cursor.tryNext();
                if (event != null) {
                    if (event.getNamespace().getCollectionName().equals(FRONTIER)) {
                        return OptionalLong.of(count);
                    }
                    count++;
                }
            }
            return OptionalLong.empty();
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
        if (firstEntry != null) {
            release();
        }
    }
}
