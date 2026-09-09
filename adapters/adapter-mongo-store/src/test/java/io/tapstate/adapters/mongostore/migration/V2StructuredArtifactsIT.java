package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.dsl.DslException;
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

    /**
     * A source exactly as the last release emitted it: {@code options} was an accepted, emitted field,
     * and this build defines no engine option at all. Written out rather than rendered through this
     * build's writer -- rendering it here would produce the one shape that cannot exhibit the fault,
     * which is why every fixture above is blind to it.
     */
    private static final String RELEASED_WITH_OPTIONS = """
            version: tapstate/v1
            kind: source
            id: src_ora
            connector: oracle
            config:
              host: 10.20.0.15
              port: 1521
            mode: cdc
            tables: [ORDERS, CUSTOMERS]
            options:
              include_ddl: true
            """;

    /** A view as the last release accepted one: it had no required primary_key, and this build does. */
    private static final String RELEASED_WITHOUT_PRIMARY_KEY = """
            version: tapstate/v1
            kind: view
            id: hr
            storage:
              hot:
                ttl: P1D
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

        new V2StructuredArtifacts().up(database, ChangeSet.Fence.HELD);

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
        new V2StructuredArtifacts().up(database, ChangeSet.Fence.HELD);
        Document afterFirst = artifacts.find(new Document("_id", "orders")).first();

        new V2StructuredArtifacts().up(database, ChangeSet.Fence.HELD);

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

        Throwable thrown = catchThrowable(() -> new V2StructuredArtifacts().up(database, ChangeSet.Fence.HELD));

        assertThat(thrown).hasMessageContaining("broken_one").hasMessageContaining("broken_two");
        assertThat(artifacts.find(new Document("_id", "orders")).first().getString("canonical"))
                .as("nothing is written when any document fails: a collection holding both shapes has "
                        + "one version number over it, and the next start reads that as finished")
                .isEqualTo(canonical(ORDERS));
    }

    @Test
    void aBodyTheLastReleaseWroteIsReadThroughTheFieldThisBuildRetired() {
        MongoDatabase database = freshDatabase("v2_retired");
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        seedAsWritten(artifacts, "src_ora", "source", RELEASED_WITH_OPTIONS);

        new V2StructuredArtifacts().up(database, ChangeSet.Fence.HELD);

        Document stored = artifacts.find(new Document("_id", "src_ora")).first();
        assertThat(stored).isNotNull();
        assertThat(stored.get("canonical"))
                .as("it moved: refusing it instead leaves the store with no forward path at all, since "
                        + "this changeset is the only thing that reads the text and the server does not "
                        + "start until it succeeds")
                .isNull();
        Document body = (Document) stored.get("body");
        assertThat(body.get("options"))
                .as("the field is retired, so it is dropped rather than carried into a structure that "
                        + "has nowhere to put it")
                .isNull();
        assertThat(PARSER.fromTree(body).id())
                .as("everything else survives the drop")
                .isEqualTo("src_ora");
        assertThat(body.getString("connector")).isEqualTo("oracle");
    }

    @Test
    void aFieldThisBuildNewlyRequiresIsRefusedNamingTheFieldAndTheCode() {
        MongoDatabase database = freshDatabase("v2_required");
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        seedAsWritten(artifacts, "hr", "view", RELEASED_WITHOUT_PRIMARY_KEY);

        Throwable thrown = catchThrowable(() -> new V2StructuredArtifacts().up(database, ChangeSet.Fence.HELD));

        assertThat(thrown)
                .as("dropping is for a field this build retired; nothing here can invent a key nobody "
                        + "wrote, so this one is still refused -- but with what an operator acts on")
                .hasMessageContaining("hr")
                .hasMessageContaining("dsl.missing-field")
                .hasMessageContaining("primary_key");
        assertThat(thrown.getCause())
                .as("the position and the stack survive the bare throw the runner re-codes")
                .isInstanceOf(DslException.class);
    }

    @Test
    void aStoreWithNothingLeftToMoveSaysSoWithoutTouchingIt() {
        MongoDatabase database = freshDatabase("v2_dryrun");
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        seedTextBodied(artifacts, ORDERS);

        assertThat(new V2StructuredArtifacts().dryRunSummary(database)).contains("1 artifact");
        new V2StructuredArtifacts().up(database, ChangeSet.Fence.HELD);
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

    /**
     * One document holding the text a previous release wrote, stored verbatim.
     *
     * <p>{@link #seedTextBodied} cannot serve here: it renders through this build's writer, so whatever
     * it seeds is already in this build's grammar -- the one shape a grammar narrowing cannot show up
     * in. The hash is the text one a store of that vintage holds.
     */
    private static void seedAsWritten(MongoCollection<Document> artifacts, String id, String kind,
            String asWritten) {
        artifacts.insertOne(new Document("_id", id)
                .append("kind", kind)
                .append("canonical", asWritten)
                .append("contentHash", CanonicalHash.ofText(asWritten)));
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
