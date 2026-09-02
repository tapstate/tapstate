package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The changeset that moves stored artifacts off text, against a real server.
 *
 * <p>What it has to get right is not the rewriting — that is a loop — but the two things a half-done
 * run would hide: every document that could not be read is named in one go, and none of the readable
 * ones is written when any of them fails. A collection holding both shapes at once has one version
 * number covering it, so the next start would read it as finished.
 */
@RequiresDocker
class V2StructuredArtifactsIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");
    private static final CanonicalWriter WRITER = new CanonicalWriter();
    private static final DslParser PARSER = new DslParser();

    private static final String ORDERS = """
            version: tapstate/v1
            kind: source
            id: orders
            connector: mysql
            config:
              host: localhost
              port: 3306
            """;

    private static final String CUSTOMERS = """
            version: tapstate/v1
            kind: source
            id: customers
            connector: mysql
            """;

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
    void movesEveryTextBodyToStructureAndRetakesTheHashOverIt() {
        MongoDatabase database = freshDatabase("v2_moves");
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        seedTextBodied(artifacts, ORDERS);
        seedTextBodied(artifacts, CUSTOMERS);

        new V2StructuredArtifacts().up(database);

        Resource orders = PARSER.parse(ORDERS);
        Document stored = artifacts.find(new Document("_id", "orders")).first();
        // Compared by what the body binds to rather than by document equality: the store hands nested
        // maps back as its own document type, so an equal structure is not an equal object.
        assertThat(PARSER.fromTree((Document) stored.get("body"))).isEqualTo(orders);
        assertThat(stored.get("canonical"))
                .as("the text is gone, not kept beside the structure as a second copy")
                .isNull();
        assertThat(stored.getString("contentHash"))
                .as("re-taken over the structure")
                .isEqualTo(CanonicalHash.of(orders))
                .isNotEqualTo(textHashOf(ORDERS));
        assertThat(indexNames(artifacts))
                .as("the index declared on this row arrives with this changeset: the changeset that "
                        + "built the first indexes has already run wherever this one is needed")
                .contains(SystemCollections.ARTIFACTS.indexes().get(0).indexName());
    }

    @Test
    void runningItAgainstAnAlreadyMovedStoreChangesNothing() {
        MongoDatabase database = freshDatabase("v2_twice");
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        seedTextBodied(artifacts, ORDERS);
        new V2StructuredArtifacts().up(database);
        Document afterFirst = artifacts.find(new Document("_id", "orders")).first();

        new V2StructuredArtifacts().up(database);

        assertThat(artifacts.find(new Document("_id", "orders")).first()).isEqualTo(afterFirst);
    }

    @Test
    void oneUnreadableBodyStopsTheWholeRunAndNamesEveryDocumentItCouldNotRead() {
        MongoDatabase database = freshDatabase("v2_refuses");
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        seedTextBodied(artifacts, ORDERS);
        artifacts.insertOne(new Document("_id", "broken_one").append("kind", "source")
                .append("canonical", "not: [valid"));
        artifacts.insertOne(new Document("_id", "broken_two").append("kind", "source")
                .append("canonical", "version: tapstate/v9\nkind: source\nid: broken_two\n"));

        Throwable thrown = catchThrowable(() -> new V2StructuredArtifacts().up(database));

        assertThat(thrown).hasMessageContaining("broken_one").hasMessageContaining("broken_two");
        assertThat(artifacts.find(new Document("_id", "orders")).first().getString("canonical"))
                .as("nothing is written when any document fails: a collection holding both shapes has "
                        + "one version number over it, and the next start reads that as finished")
                .isEqualTo(canonical(ORDERS));
    }

    @Test
    void aStoreWithNothingLeftToMoveSaysSoWithoutTouchingIt() {
        MongoDatabase database = freshDatabase("v2_dryrun");
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        seedTextBodied(artifacts, ORDERS);

        assertThat(new V2StructuredArtifacts().dryRunSummary(database)).contains("1 artifact");
        new V2StructuredArtifacts().up(database);
        assertThat(new V2StructuredArtifacts().dryRunSummary(database)).contains("no artifact");
    }

    /** One document in the shape a store written before this changeset holds. */
    private static void seedTextBodied(MongoCollection<Document> artifacts, String raw) {
        Resource resource = PARSER.parse(raw);
        artifacts.insertOne(new Document("_id", resource.id())
                .append("kind", resource.kind())
                .append("canonical", canonical(raw))
                .append("contentHash", textHashOf(raw)));
    }

    private static String canonical(String raw) {
        return WRITER.write(PARSER.parse(raw));
    }

    /** The hash a store written before this changeset holds: taken over the text, not the structure. */
    private static String textHashOf(String raw) {
        return CanonicalHash.ofText(canonical(raw));
    }

    private static List<String> indexNames(MongoCollection<Document> collection) {
        List<String> names = new ArrayList<>();
        for (Document index : collection.listIndexes()) {
            names.add(index.getString("name"));
        }
        return names;
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
