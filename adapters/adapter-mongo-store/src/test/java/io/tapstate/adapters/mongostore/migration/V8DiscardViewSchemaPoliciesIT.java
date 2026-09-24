package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.MongoArtifactStore;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** The changeset that carries stored view artifacts past the retired schema policy. */
@RequiresDocker
class V8DiscardViewSchemaPoliciesIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");
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
    void startupMakesBothReleasedViewShapesReadableAndRetakesTheirHashes() {
        MongoDatabase database = freshDatabase("v8_released_views");
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        artifacts.insertMany(List.of(releasedInlineView(), releasedReusableView()));
        SystemCollections.SYSTEM_META.on(database)
                .insertOne(new Document("_id", "schema").append("installedVersion", 7));
        MongoArtifactStore store = new MongoArtifactStore(client, artifacts);

        assertUnreadable(store, "orders", "view.schema");
        assertUnreadable(store, "enforced_view", "schema");

        MigrationRunner.migrate(database);

        Resource pipeline = store.get("orders").orElseThrow();
        Resource view = store.get("enforced_view").orElseThrow();
        assertThat(WRITER.write(pipeline)).isEqualTo("""
                version: tapstate/v1
                kind: pipeline
                id: orders
                source: orders_src
                view:
                  id: order_state
                  from: orders_src
                  primary_key: order_id
                """);
        assertThat(WRITER.write(view)).isEqualTo("""
                version: tapstate/v1
                kind: view
                id: enforced_view
                primary_key: id
                """);
        assertCarried(artifacts, pipeline, "body.view.schema",
                "d88ce94a4f440d991cdac3db9c959cdfb03c625ede0bb3c36d91698440db236d");
        assertCarried(artifacts, view, "body.schema",
                "6e27aef5aebaaf660f7dbcf5d82cc481f01a1ac4012592cd227030ca32fae7fb");
        assertThat(MigrationRunner.inspect(database).installed()).isEqualTo(MigrationRunner.SUPPORTED_VERSION);
    }

    @Test
    void anotherInvalidFieldStopsBeforeAnySchemaPolicyIsRemoved() {
        MongoDatabase database = freshDatabase("v8_other_invalid_field");
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        Document readableAfterRemoval = releasedInlineView();
        Document stillInvalid = releasedReusableView();
        stillInvalid.get("body", Document.class).append("unknown", true);
        artifacts.insertMany(List.of(readableAfterRemoval, stillInvalid));

        Throwable failure = catchThrowable(() ->
                new V8DiscardViewSchemaPolicies().up(database, ChangeSet.Fence.HELD));

        assertThat(failure).isNotNull();
        assertThat(artifacts.find(new Document("_id", "orders")).first()).isEqualTo(readableAfterRemoval);
        assertThat(artifacts.find(new Document("_id", "enforced_view")).first()).isEqualTo(stillInvalid);
    }

    @Test
    void aRepeatedRunLeavesCarriedAndUnrelatedSchemaFieldsUntouched() {
        MongoDatabase database = freshDatabase("v8_repeat");
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        Document legacy = releasedInlineView();
        Document unrelated = new Document("_id", "source")
                .append("kind", "source")
                .append("body", new Document("version", "tapstate/v1")
                        .append("kind", "source")
                        .append("id", "source")
                        .append("connector", "postgres")
                        .append("config", new Document("schema", "public")))
                .append("contentHash", "unchanged");
        artifacts.insertMany(List.of(legacy, unrelated));
        V8DiscardViewSchemaPolicies changeset = new V8DiscardViewSchemaPolicies();

        assertThat(changeset.dryRunSummary(database)).contains("1 stored view artifact");
        changeset.up(database, ChangeSet.Fence.HELD);
        Document afterFirst = artifacts.find(new Document("_id", "orders")).first();
        changeset.up(database, ChangeSet.Fence.HELD);

        assertThat(artifacts.find(new Document("_id", "orders")).first()).isEqualTo(afterFirst);
        assertThat(artifacts.find(new Document("_id", "source")).first()).isEqualTo(unrelated);
        assertThat(changeset.dryRunSummary(database)).contains("no stored view artifact");
    }

    private static void assertUnreadable(MongoArtifactStore store, String id, String field) {
        Throwable failure = catchThrowable(() -> store.get(id));
        assertThat(failure).isInstanceOf(TapstateException.class);
        TapstateException unreadable = (TapstateException) failure;
        assertThat(unreadable.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
        assertThat(unreadable.args()).containsEntry("id", id).containsEntry("field", field);
    }

    private static void assertCarried(MongoCollection<Document> artifacts, Resource resource,
            String retiredPath, String expectedHash) {
        Document stored = artifacts.find(new Document("_id", resource.id())).first();
        assertThat(stored).isNotNull();
        assertThat(stored.getEmbedded(List.of(retiredPath.split("\\.")), Object.class)).isNull();
        assertThat(stored.getString("contentHash"))
                .isEqualTo(expectedHash)
                .isEqualTo(CanonicalHash.of(resource));
    }

    /** Exact structures and hashes emitted by the released canonical writer. */
    private static Document releasedInlineView() {
        return new Document("_id", "orders")
                .append("kind", "pipeline")
                .append("body", new Document("version", "tapstate/v1")
                        .append("kind", "pipeline")
                        .append("id", "orders")
                        .append("source", "orders_src")
                        .append("view", new Document("id", "order_state")
                                .append("from", "orders_src")
                                .append("primary_key", "order_id")
                                .append("schema", new Document("enforce", true)
                                        .append("evolution", "additive"))))
                .append("contentHash", "cbd82a82d5173379699fc926a5f9d2821475aa737d310aa1f8837a42fe21b326");
    }

    private static Document releasedReusableView() {
        return new Document("_id", "enforced_view")
                .append("kind", "view")
                .append("body", new Document("version", "tapstate/v1")
                        .append("kind", "view")
                        .append("id", "enforced_view")
                        .append("primary_key", "id")
                        .append("schema", new Document("enforce", true)
                                .append("evolution", "additive")))
                .append("contentHash", "2c181b4a5ab50a441baa317fd00917819976341b61376a0208fbd67ed97453b8");
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
