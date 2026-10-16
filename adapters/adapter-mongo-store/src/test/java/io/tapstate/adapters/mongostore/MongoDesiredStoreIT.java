package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Witnesses the pipeline desired-state store against a real Mongo replica-set: a saved desired intent
 * reads back equal through the real bson encode / decode, an absent pipeline reads back empty, and a
 * re-save of the same pipeline replaces in place (last write wins) rather than accumulating documents.
 * Where Docker is absent this aborts on a developer machine and fails in CI, where a skip would be a
 * green build that ran nothing.
 */
@RequiresDocker
class MongoDesiredStoreIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    @Test
    void savedDesiredReadsBackEqual() {
        withStore((store, collection) -> {
            DesiredState want = new DesiredState("orders_sync", PipelineState.RUNNING, "rev-abc");
            store.save(want);

            Optional<DesiredState> read = store.read("orders_sync");
            assertThat(read).contains(want);
        });
    }

    @Test
    void readReturnsEmptyForAnAbsentPipeline() {
        withStore((store, collection) -> assertThat(store.read("nope")).isEmpty());
    }

    @Test
    void reSaveOfTheSamePipelineReplacesInPlace() {
        withStore((store, collection) -> {
            store.save(new DesiredState("orders_sync", PipelineState.RUNNING, "rev-1"));
            DesiredState changed = new DesiredState("orders_sync", PipelineState.STOPPED, "rev-2");
            store.save(changed);

            assertThat(collection.countDocuments()).isEqualTo(1);
            assertThat(store.read("orders_sync")).contains(changed);
        });
    }

    @Test
    void pipelineIdsReturnsEveryStoredPipelineId() {
        withStore((store, collection) -> {
            store.save(new DesiredState("orders_sync", PipelineState.RUNNING, "rev-1"));
            store.save(new DesiredState("users_sync", PipelineState.PAUSED, "rev-2"));

            assertThat(store.pipelineIds()).containsExactlyInAnyOrder("orders_sync", "users_sync");
        });
    }

    @Test
    void pipelineIdsReturnsEmptyWhenNoIntentIsStored() {
        withStore((store, collection) -> assertThat(store.pipelineIds()).isEmpty());
    }

    @Test
    void pipelineIdsSurvivesACorruptDocument() {
        withStore((store, collection) -> {
            store.save(new DesiredState("orders_sync", PipelineState.RUNNING, "rev-1"));
            // A document whose target state this version does not recognize: listing ids reads only the
            // _id, so the corrupt row is enumerated rather than failing the whole reconcile set.
            collection.insertOne(new Document("_id", "legacy")
                    .append("targetState", "TELEPORTING").append("revision", "rev-x"));

            assertThat(store.pipelineIds()).containsExactlyInAnyOrder("orders_sync", "legacy");
        });
    }

    @Test
    void aDeletedIntentIsGoneFromTheReconcileSetAndNotJustFromTheRead() {
        withStore((store, collection) -> {
            store.save(new DesiredState("orders_sync", PipelineState.RUNNING, "rev-1"));
            store.save(new DesiredState("payments_sync", PipelineState.RUNNING, "rev-1"));

            store.delete("orders_sync");

            assertThat(store.read("orders_sync")).isEmpty();
            // pipelineIds is what the converge side reconciles, so an id surviving here would keep a
            // removed pipeline in the reconcile loop even though nothing can be read for it.
            assertThat(store.pipelineIds()).containsExactly("payments_sync");
            assertThat(collection.countDocuments()).isEqualTo(1);
        });
    }

    @Test
    void deletingAnAbsentPipelineIsANoOpAndLeavesTheOthersAlone() {
        withStore((store, collection) -> {
            store.save(new DesiredState("orders_sync", PipelineState.RUNNING, "rev-1"));

            store.delete("never_existed");
            store.delete("orders_sync");
            store.delete("orders_sync");

            assertThat(collection.countDocuments()).isZero();
        });
    }

    private interface StoreTest {
        void run(MongoDesiredStore store, MongoCollection<Document> collection);
    }

    /** Runs a test body against a fresh desired store over a clean collection on the real replica-set. */
    private static void withStore(StoreTest test) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoCollection<Document> collection = client.getDatabase("tapstate").getCollection("pipeline_desired");
            collection.drop();
            test.run(new MongoDesiredStore(collection), collection);
        }
    }
}
