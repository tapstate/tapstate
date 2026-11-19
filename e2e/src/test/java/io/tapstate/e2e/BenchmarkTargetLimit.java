package io.tapstate.e2e;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
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
        private final MongoDatabase admin;
        private final MongoCollection<BsonDocument> target;
        private MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor;

        TargetWindow(MongoDatabase admin, MongoCollection<BsonDocument> target) {
            this.admin = admin;
            this.target = target;
        }

        void begin() {
            cursor = target.watch().maxAwaitTime(200, TimeUnit.MILLISECONDS).batchSize(128).cursor();
        }

        long countThroughBoundary() {
            var clock = admin.runCommand(new BsonDocument("ping", new org.bson.BsonInt32(1)), BsonDocument.class)
                    .getDocument("$clusterTime").getTimestamp("clusterTime");
            long count = 0;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (System.nanoTime() < deadline) {
                var event = cursor.tryNext();
                if (event == null || event.getClusterTime().compareTo(clock) > 0) {
                    return count;
                }
                count++;
            }
            throw new IllegalStateException("Target observation did not reach its boundary before timeout");
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
