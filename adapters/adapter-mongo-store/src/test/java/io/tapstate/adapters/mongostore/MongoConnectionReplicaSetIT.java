package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.EpochCas;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Witnesses the store contract against a real Mongo — the pure {@link EpochCas} contract must equal
 * what a real replica-set does, so the two cannot drift (the production store lands later and must
 * hold to this). Where Docker is absent this aborts on a developer machine and fails in CI, where a
 * skip would be a green build that ran nothing.
 *
 * <ul> <li>a real replica-set passes {@code verify()};</li> <li>a standalone server is reported {@code
 * store.not-replica-set};</li> <li>a real conditional {@code findOneAndUpdate} on the epoch matches
 * {@link EpochCas}: the matching write applies and bumps the epoch, a stale write is fenced and does
 * not overwrite, and the epoch advances monotonically.</li> </ul>
 */
@RequiresDocker
class MongoConnectionReplicaSetIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    @Test
    void verifySucceedsAgainstARealReplicaSet() {
        // The Testcontainers Mongo speaks plaintext; TLS is opt-in, so a plaintext URL connects with
        // no flag. The self-signed TLS trust chain itself is witnessed in MongoConnectionTest, and a
        // real Mongo-over-TLS run is exercised by the deploy TLS profile.
        MongoConnectionSettings settings = new MongoConnectionSettings(
                REPLICA_SET.getReplicaSetUrl(), null, Duration.ofSeconds(5));
        try (MongoConnection connection = new MongoConnection(settings)) {
            connection.verify();
        }
    }

    @Test
    void verifySucceedsWhenAddressingTheMemberDirectly() {
        // A containerized server cannot use topology discovery: the set member advertises the address
        // its host reaches it by, which resolves to nothing inside another container. Addressing the
        // member directly skips discovery, and this asserts the topology gate still passes when it
        // does. The gate reads setName out of the hello response -- a field the server reports about
        // itself -- so it is indifferent to the driver running a single-server topology. Were the gate
        // ever rewritten to inspect the driver's topology type instead, it would start rejecting every
        // direct connection and this test would catch it.
        MongoConnectionSettings settings = new MongoConnectionSettings(
                REPLICA_SET.getReplicaSetUrl() + "?directConnection=true", null, Duration.ofSeconds(5));
        try (MongoConnection connection = new MongoConnection(settings)) {
            connection.verify();
        }
    }

    @Test
    void aDirectConnectionStillCommitsAndRollsBackATransaction() {
        // Direct addressing must not cost the one capability the replica set is required for. Sessions
        // depend on the server reporting a logical session timeout, which a set member does whether or
        // not the driver discovered the set, so both the commit and the abort path are witnessed here
        // rather than assumed.
        String uri = REPLICA_SET.getReplicaSetUrl() + "?directConnection=true";
        try (MongoClient client = MongoClients.create(uri)) {
            MongoCollection<Document> collection = client.getDatabase("tapstate").getCollection("txn_direct");

            try (var session = client.startSession()) {
                session.startTransaction();
                collection.insertOne(session, new Document("_id", "committed").append("v", 1));
                session.commitTransaction();
            }
            assertThat(collection.find(new Document("_id", "committed")).first())
                    .as("a committed transaction is visible after a direct connection commit")
                    .isNotNull();

            try (var session = client.startSession()) {
                session.startTransaction();
                collection.insertOne(session, new Document("_id", "aborted").append("v", 1));
                session.abortTransaction();
            }
            assertThat(collection.find(new Document("_id", "aborted")).first())
                    .as("an aborted transaction leaves nothing behind")
                    .isNull();
        }
    }

    @Test
    void verifyReportsAStandaloneAsNotAReplicaSet() {
        try (GenericContainer<?> standalone = new GenericContainer<>(MONGO_IMAGE).withExposedPorts(27017)) {
            standalone.start();
            String uri = "mongodb://" + standalone.getHost() + ":" + standalone.getMappedPort(27017) + "/tapstate";
            MongoConnectionSettings settings = new MongoConnectionSettings(uri, null, Duration.ofSeconds(5));
            try (MongoConnection connection = new MongoConnection(settings)) {
                TapstateException ex = catchThrowableOfType(connection::verify, TapstateException.class);
                assertThat(ex).as("a standalone server fails the replica-set check").isNotNull();
                assertThat(ex.code()).isEqualTo(StoreError.NOT_REPLICA_SET);
            }
        }
    }

    @Test
    void realConditionalUpdateMatchesTheEpochCasContract() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoCollection<Document> checkpoints = client.getDatabase("tapstate").getCollection("checkpoints");
            String pipelineId = "orders-sync";
            Instant t0 = Instant.parse("2026-07-01T00:00:00Z");

            // seed epoch 0
            CheckpointDoc initial = CheckpointDoc.initial(pipelineId, "{\"state\":\"NEW\"}", t0);
            checkpoints.insertOne(toDocument(initial));

            // owner-A swaps with the expected epoch 0 -> applies, epoch -> 1 (matches the contract)
            CasOutcome pureApplied = EpochCas.swap(initial, 0, "{\"state\":\"RUNNING\"}", t0.plusSeconds(1));
            Document realApplied = conditionalSwap(checkpoints, pipelineId, 0, "{\"state\":\"RUNNING\"}", t0.plusSeconds(1));
            assertThat(pureApplied).isInstanceOf(CasOutcome.Applied.class);
            assertThat(realApplied).as("a matching-epoch write applies on the real replica-set").isNotNull();
            assertThat(realApplied.getLong("epoch"))
                    .isEqualTo(((CasOutcome.Applied) pureApplied).next().epoch())
                    .isEqualTo(1L);

            CheckpointDoc afterA = ((CasOutcome.Applied) pureApplied).next();

            // owner-B still believes epoch is 0 -> fenced on both the contract and the real store
            CasOutcome pureFenced = EpochCas.swap(afterA, 0, "{\"state\":\"STOPPED\"}", t0.plusSeconds(2));
            Document realFenced = conditionalSwap(checkpoints, pipelineId, 0, "{\"state\":\"STOPPED\"}", t0.plusSeconds(2));
            assertThat(pureFenced).isInstanceOf(CasOutcome.Fenced.class);
            assertThat(((CasOutcome.Fenced) pureFenced).currentEpoch()).isEqualTo(1L);
            assertThat(realFenced).as("a stale-epoch write is fenced (no matching document)").isNull();

            // the winner's write is intact and the epoch advances monotonically 1 -> 2
            Document stored = checkpoints.find(new Document("_id", pipelineId)).first();
            assertThat(stored).isNotNull();
            assertThat(stored.getLong("epoch")).isEqualTo(1L);
            assertThat(stored.getString("stateJson")).isEqualTo("{\"state\":\"RUNNING\"}");

            Document realSecond = conditionalSwap(checkpoints, pipelineId, 1, "{\"state\":\"PAUSED\"}", t0.plusSeconds(3));
            assertThat(realSecond).isNotNull();
            assertThat(realSecond.getLong("epoch")).isEqualTo(2L);
        }
    }

    /** The real CAS primitive: swap only when the stored epoch equals {@code expected}, bumping it. */
    private static Document conditionalSwap(
            MongoCollection<Document> checkpoints, String pipelineId, long expected, String nextStateJson, Instant touch) {
        Document filter = new Document("_id", pipelineId).append("epoch", expected);
        Document update = new Document("$set",
                new Document("stateJson", nextStateJson).append("touchMillis", touch.toEpochMilli()))
                .append("$inc", new Document("epoch", 1L));
        return checkpoints.findOneAndUpdate(filter, update,
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
    }

    private static Document toDocument(CheckpointDoc doc) {
        return new Document("_id", doc.pipelineId())
                .append("stateJson", doc.stateJson())
                .append("epoch", doc.epoch())
                .append("touchMillis", doc.touchTime().toEpochMilli());
    }
}
