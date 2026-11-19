package io.tapstate.e2e;

import com.mongodb.client.MongoDatabase;
import java.util.List;
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

    @Override
    public void close() {
        if (firstEntry != null) {
            release();
        }
    }
}
