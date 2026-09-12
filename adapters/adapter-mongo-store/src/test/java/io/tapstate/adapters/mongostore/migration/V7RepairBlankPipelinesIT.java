package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The changeset that repairs blank pipeline artifacts written without source: []. */
@RequiresDocker
class V7RepairBlankPipelinesIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");
    private static final DslParser PARSER = new DslParser();
    private static final CanonicalWriter WRITER = new CanonicalWriter();

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    private static MongoClient client;

    @AfterAll
    static void closeClient() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void repairsAStoredBlankPipelineAndRetakesItsHash() {
        MongoDatabase database = freshDatabase("v7_blank_pipeline");
        SystemCollections.ARTIFACTS.on(database).insertOne(legacyBlankPipeline());

        new V7RepairBlankPipelines().up(database, ChangeSet.Fence.HELD);

        Document repaired = SystemCollections.ARTIFACTS.on(database)
                .find(new Document("_id", "p911")).first();
        assertThat(repaired).isNotNull();
        Document body = repaired.get("body", Document.class);
        assertThat(body.get("source")).isEqualTo(List.of());
        PipelineResource resource = (PipelineResource) PARSER.fromTree(body);
        assertThat(WRITER.write(resource)).isEqualTo("""
                version: tapstate/v1
                kind: pipeline
                id: p911
                source: []
                """);
        assertThat(repaired.getString("contentHash")).isEqualTo(CanonicalHash.of(resource));
    }

    @Test
    void runningItTwiceDoesNotRewriteAnAlreadyRepairedPipeline() {
        MongoDatabase database = freshDatabase("v7_blank_pipeline_twice");
        SystemCollections.ARTIFACTS.on(database).insertOne(legacyBlankPipeline());

        V7RepairBlankPipelines changeset = new V7RepairBlankPipelines();
        changeset.up(database, ChangeSet.Fence.HELD);
        Document afterFirst = SystemCollections.ARTIFACTS.on(database)
                .find(new Document("_id", "p911")).first();
        changeset.up(database, ChangeSet.Fence.HELD);

        assertThat(SystemCollections.ARTIFACTS.on(database)
                .find(new Document("_id", "p911")).first()).isEqualTo(afterFirst);
        assertThat(changeset.dryRunSummary(database)).contains("no blank pipeline");
    }

    private static Document legacyBlankPipeline() {
        return new Document("_id", "p911")
                .append("kind", "pipeline")
                .append("body", new Document("version", "tapstate/v1")
                        .append("kind", "pipeline")
                        .append("id", "p911"))
                .append("contentHash", "1edf01f1dea550a8ece2d831cb5aca4fbe19867e906714b55511eac06b8bf277");
    }

    private static MongoDatabase freshDatabase(String name) {
        if (client == null) {
            client = MongoClients.create(REPLICA_SET.getReplicaSetUrl());
        }
        MongoDatabase database = client.getDatabase(name);
        database.drop();
        return database;
    }
}
