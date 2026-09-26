package io.tapstate.e2e;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.UpdateOptions;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Proves a store-only write fault can target one namespace without touching other store commands. */
@RequiresDocker
class MongoObservationFailpointIT {

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    @Test
    void namespaceAndAppFilterAffectOnlyTheObservationCollection() {
        String uri = MONGO.getReplicaSetUrl("observation_failpoint_probe");
        String victimUri = uri + (uri.contains("?") ? "&" : "?")
                + "appName=observation-failpoint-probe";
        try (MongoClient admin = MongoClients.create(uri);
                MongoClient victim = MongoClients.create(victimUri)) {
            admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", "alwaysOn")
                    .append("data", new Document("failCommands", List.of("update"))
                            .append("appName", "observation-failpoint-probe")
                            .append("namespace", "observation_failpoint_probe.pipeline_observation")
                            .append("errorCode", 2)));
            try {
                var database = victim.getDatabase("observation_failpoint_probe");
                database.getCollection("another_store_collection")
                        .updateOne(new Document("_id", "safe"),
                                new Document("$set", new Document("value", 1)),
                                new UpdateOptions().upsert(true));
                assertThat(database.getCollection("another_store_collection")
                        .countDocuments()).isEqualTo(1);
                assertThatThrownBy(() -> database.getCollection("pipeline_observation")
                        .updateOne(new Document("_id", "failed"),
                                new Document("$set", new Document("value", 1)),
                                new UpdateOptions().upsert(true)))
                        .isInstanceOf(MongoException.class);
            } finally {
                admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                        .append("mode", "off"));
            }
        }
    }
}
