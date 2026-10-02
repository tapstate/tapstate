package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.CreateCollectionOptions;
import io.tapstate.adapters.mongostore.MongoDesiredStore;
import io.tapstate.adapters.mongostore.MongoStateStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.adapters.mongostore.MongoWorkloadClaimStore;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static io.tapstate.e2e.NativeCoordinationProfile.Family.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real Mongo calibration of attribution, production mutation classification, and retained evidence. */
@RequiresDocker
class NativeCoordinationProfileIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void productionColdWarmAndLeaseCommandsAreCountedWithoutOtherClientsOrNamespaces() {
        String database = database();
        String otherDatabase = database();
        String uri = MONGO.getReplicaSetUrl(database);
        prepare(uri, database, otherDatabase);
        try (MongoClient admin = MongoClients.create(uri)) {
            try (NativeCoordinationProfile profile = NativeCoordinationProfile.openOwned(uri);
                    MongoClient actor = MongoClients.create(profile.nativeUri());
                    MongoClient foreign = MongoClients.create(uri + (uri.contains("?") ? "&" : "?") + "appName=foreign-profile-client")) {
                var store = store(actor, database);
                String cluster = "profile-cluster";
                String pipeline = "profile-pipeline";
                assertThat(store.advanceStandalone(cluster, pipeline)).hasValue(1);
                assertThat(store(foreign, database).advanceStandalone(cluster, "foreign-client")).hasValue(1);
                assertThat(store(actor, otherDatabase).advanceStandalone(cluster, "foreign-namespace")).hasValue(1);
                var cold = profile.boundary("cold");
                assertThat(cold.count(ADVANCE_STANDALONE)).isEqualTo(2);
                assertThat(cold.operations()).allSatisfy(operation -> assertThat(operation.key().resourceId()).isEqualTo(pipeline));
                assertThat(cold.operations()).extracting(NativeCoordinationProfile.Operation::upsert)
                        .containsExactly(false, true);

                assertThat(store.currentGeneration(cluster, pipeline)).hasValue(1);
                assertThat(store.advanceStandalone(cluster, pipeline)).hasValue(2);
                var warm = profile.boundary("warm");
                assertThat(warm.count(ADVANCE_STANDALONE)).isEqualTo(1);
                assertThat(warm.operations().stream().filter(operation -> operation.family() == ADVANCE_STANDALONE))
                        .allSatisfy(operation -> assertThat(operation.upsert()).isFalse());
                assertThat(store.currentGeneration(cluster, pipeline)).hasValue(2);

                // A separate absent claim makes the acquire's cold miss and upsert unambiguous.
                var key = new WorkloadClaimKey(cluster, WorkloadClaimType.PIPELINE_ACTUATION, pipeline + "-leased");
                var acquired = store.acquire(key, new WorkloadOwner("profile-node", "profile-boot"), 7, Duration.ofMinutes(1));
                assertThat(acquired.acquired()).isTrue();
                var acquisition = profile.boundary("acquire");
                assertThat(acquisition.count(ACQUIRE)).isEqualTo(2);
                var renewed = store.renew(acquired.claim(), Duration.ofMinutes(1)).orElseThrow();
                assertThat(profile.boundary("renew").count(RENEW)).isEqualTo(1);
                var advanced = store.advanceExecution(renewed, 7, Set.of("profile-node")).orElseThrow();
                assertThat(profile.boundary("claim-guarded-advance").count(ADVANCE_UNDER_CLAIM)).isEqualTo(1);
                var failed = store.recordExecutionFailure(advanced, false).orElseThrow();
                assertThat(profile.boundary("execution-failure-record").count(RECORD_EXECUTION_FAILURE)).isEqualTo(1);
                assertThat(store.release(failed)).isTrue();
                var release = profile.boundary("release");
                assertThat(release.count(RELEASE)).isEqualTo(1);
                assertThat(release.evidence()).containsEntry("transactionCommitProven", false)
                        .containsEntry("performanceAcceptanceEligible", false);
            }
            assertThat(admin.getDatabase(database).runCommand(new Document("profile", -1)).get("was")).isEqualTo(0);
            assertThat(admin.getDatabase(database).listCollectionNames()).doesNotContain("system.profile");
        } finally { drop(uri, database, otherDatabase); }
    }

    @Test
    void publicStopReservationGuardsKeepTheAdmittedGenerationWithoutCreatingALease() {
        String database = database();
        String uri = MONGO.getReplicaSetUrl(database);
        prepare(uri, database);
        try (MongoClient admin = MongoClients.create(uri)) {
            admin.getDatabase(database).createCollection(MongoStorePort.PIPELINE_STATE);
            admin.getDatabase(database).createCollection(MongoStorePort.PIPELINE_DESIRED);
            try (NativeCoordinationProfile profile = NativeCoordinationProfile.openOwned(uri);
                    MongoClient actor = MongoClients.create(profile.nativeUri())) {
                var owned = actor.getDatabase(database);
                var claims = store(actor, database);
                String cluster = "profile-guard-cluster";
                String pipeline = "profile-guard-pipeline";
                long generation = claims.advanceStandalone(cluster, pipeline).orElseThrow();
                var state = new MongoStateStore(actor, owned.getCollection(MongoStorePort.PIPELINE_STATE),
                        owned.getCollection(MongoStorePort.PIPELINE_DESIRED), owned.getCollection(MongoStorePort.WORKLOAD_CLAIMS));
                var desired = new MongoDesiredStore(owned.getCollection(MongoStorePort.PIPELINE_DESIRED));
                Instant at = Instant.now();
                state.create(pipeline, StateJson.of(PipelineState.NEW), at);
                var before = state.read(pipeline).orElseThrow();
                var intent = new DesiredState(pipeline, PipelineState.STOPPED, "profile-guard-revision");
                desired.save(intent);
                assertThat(profile.boundary("guard-setup").count(ADVANCE_STANDALONE)).isEqualTo(2);
                // No native job is invented: the public API guards an actually allocated generation.
                var proposal = StopReservation.stopping(pipeline, UUID.randomUUID().toString(), before.epoch(), intent,
                        new StopReservation.Source(cluster, null, null), StopReservation.CounterPolicy.freeze(before, intent),
                        StopAuthority.standalone(cluster, generation));
                assertThat(state.reserveStop(before, proposal, at.plusSeconds(1))).contains(proposal);
                assertThat(state.readStopReservation(pipeline)).contains(proposal);
                var key = new NativeCoordinationProfile.Key(cluster, WorkloadClaimType.PIPELINE_ACTUATION.name(), pipeline);
                assertPublicGuard(profile.boundary("reserve-guard"), key, generation);
                assertThat(claims.currentGeneration(cluster, pipeline)).hasValue(generation);
                Document reserved = owned.getCollection(MongoStorePort.WORKLOAD_CLAIMS)
                        .find(new Document("resourceType", key.resourceType()).append("resourceId", pipeline)).first();
                assertThat(reserved).isNotNull().doesNotContainKeys("ownerNodeId", "ownerBootId", "claimGeneration", "leaseUntil");
                assertThat(((Number) reserved.get("fencedAppends")).longValue()).isEqualTo(1);
                var completed = state.completeStop(proposal, at.plusSeconds(2)).orElseThrow();
                assertThat(completed.stateJson()).isEqualTo(StateJson.of(PipelineState.STOPPED));
                assertThat(state.readStopReservation(pipeline)).isEmpty();
                assertPublicGuard(profile.boundary("complete-guard"), key, generation);
                assertThat(claims.currentGeneration(cluster, pipeline)).hasValue(generation);
                Document stopped = owned.getCollection(MongoStorePort.WORKLOAD_CLAIMS)
                        .find(new Document("resourceType", key.resourceType()).append("resourceId", pipeline)).first();
                assertThat(stopped).isNotNull().doesNotContainKeys("ownerNodeId", "ownerBootId", "claimGeneration", "leaseUntil");
                assertThat(((Number) stopped.get("fencedAppends")).longValue()).isEqualTo(2);
            }
        } finally { drop(uri, database); }
    }

    private static void assertPublicGuard(NativeCoordinationProfile.Boundary boundary,
            NativeCoordinationProfile.Key key, long generation) {
        assertThat(boundary.count(CLAIM_WRITE_GUARD, key)).isEqualTo(1);
        for (var family : NativeCoordinationProfile.Family.values()) {
            if (family == READ || family == CONTROL || family == SCHEMA_WRITE || family == CLAIM_WRITE_GUARD) { continue; }
            assertThat(boundary.count(family)).as("public guard does not allocate or lease: %s", family).isZero();
        }
        assertThat(boundary.operations().stream().filter(operation -> operation.family() == CLAIM_WRITE_GUARD))
                .singleElement().satisfies(operation -> {
                    assertThat(operation.command()).isEqualTo("update");
                    assertThat(operation.key()).isEqualTo(key);
                    assertThat(operation.upsert()).isFalse();
                    assertThat(operation.guardedExecutionGeneration()).isEqualTo(generation);
                    assertThat(operation.transactionScope()).isEqualTo(NativeCoordinationProfile.TransactionScope.UNKNOWN);
                    assertThat(operation.serverCounts()).containsEntry("nMatched", 1L)
                            .containsEntry("nModified", 1L).containsEntry("nUpserted", 0L);
                });
        assertThat(boundary.evidence()).containsEntry("transactionCommitProven", false)
                .containsEntry("performanceAcceptanceEligible", false);
    }

    @Test
    void aRecordLimitCannotTurnAdditionalPhysicalCommandsIntoASmallerCount() {
        String database = database();
        String uri = MONGO.getReplicaSetUrl(database);
        prepare(uri, database);
        try (NativeCoordinationProfile profile = NativeCoordinationProfile.openOwned(uri,
                new NativeCoordinationProfile.Limits(4L * 1024 * 1024, 3));
                MongoClient actor = MongoClients.create(profile.nativeUri())) {
            var store = store(actor, database);
            assertThat(store.advanceStandalone("profile-cluster", "profile-pipeline")).hasValue(1);
            assertThat(store.advanceStandalone("profile-cluster", "profile-pipeline")).hasValue(2);
            assertThatThrownBy(() -> profile.boundary("overflow")).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("record limit exceeded");
        } finally { drop(uri, database); }
    }

    @Test
    void cappedRolloverFailsEvenWhenTheRetainedRecordCountIsBelowTheLimit() {
        String database = database();
        String uri = MONGO.getReplicaSetUrl(database);
        prepare(uri, database);
        try (NativeCoordinationProfile profile = NativeCoordinationProfile.openOwned(uri,
                new NativeCoordinationProfile.Limits(4096, 512));
                MongoClient actor = MongoClients.create(profile.nativeUri())) {
            var store = store(actor, database);
            assertThat(store.advanceStandalone("profile-cluster", "profile-pipeline")).hasValue(1);
            for (int i = 0; i < 128; i++) { assertThat(store.currentGeneration("profile-cluster", "profile-pipeline")).hasValue(1); }
            assertThatThrownBy(() -> profile.boundary("rollover")).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("anchor was lost");
        } finally { drop(uri, database); }
    }

    @Test
    void anUnknownNonmatchingMutationFailsWithoutChangingClaimTruth() {
        String database = database();
        String uri = MONGO.getReplicaSetUrl(database);
        prepare(uri, database);
        try (NativeCoordinationProfile profile = NativeCoordinationProfile.openOwned(uri);
                MongoClient actor = MongoClients.create(profile.nativeUri())) {
            var store = store(actor, database);
            assertThat(store.advanceStandalone("profile-cluster", "profile-pipeline")).hasValue(1);
            assertThat(profile.boundary("known-cold").count(ADVANCE_STANDALONE)).isEqualTo(2);
            var unchanged = actor.getDatabase(database).getCollection(MongoStorePort.WORKLOAD_CLAIMS)
                    .updateOne(new Document("_id", new Document("nativeProfileAbsent", UUID.randomUUID().toString())),
                            new Document("$set", new Document("unknownMutation", true)));
            assertThat(unchanged.getMatchedCount()).isZero();
            assertThat(unchanged.getModifiedCount()).isZero();
            assertThatThrownBy(() -> profile.boundary("unknown-write")).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("unsupported workload mutation command: update");
            assertThat(store.currentGeneration("profile-cluster", "profile-pipeline")).hasValue(1);
        } finally { drop(uri, database); }
    }

    @Test
    void anEmptyOutAggregationIsStillAnUnrecognisedWrite() {
        unknownAggregateCannotBeRead("$out");
    }

    @Test
    void anEmptyMergeAggregationIsStillAnUnrecognisedWrite() {
        unknownAggregateCannotBeRead("$merge");
    }

    private static void unknownAggregateCannotBeRead(String stage) {
        String database = database();
        String uri = MONGO.getReplicaSetUrl(database);
        prepare(uri, database);
        try (NativeCoordinationProfile profile = NativeCoordinationProfile.openOwned(uri);
                MongoClient actor = MongoClients.create(profile.nativeUri())) {
            var store = store(actor, database);
            assertThat(store.advanceStandalone("profile-cluster", "profile-pipeline")).hasValue(1);
            assertThat(profile.boundary("known-cold").count(ADVANCE_STANDALONE)).isEqualTo(2);
            Object destination = stage.equals("$out") ? "profile_destination"
                    : new Document("into", "profile_destination");
            actor.getDatabase(database).getCollection(MongoStorePort.WORKLOAD_CLAIMS).aggregate(List.of(
                    new Document("$match", new Document("_id",
                            new Document("nativeProfileAbsent", UUID.randomUUID().toString()))),
                    new Document(stage, destination))).toCollection();
            assertThatThrownBy(() -> profile.boundary("unknown-aggregation-write"))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("unsupported aggregate workload mutation");
            assertThat(store.currentGeneration("profile-cluster", "profile-pipeline")).hasValue(1);
        } finally { drop(uri, database); }
    }

    @Test
    void collectionReplacementIsRejectedAndCleanupPreservesTheReplacement() {
        String database = database();
        String uri = MONGO.getReplicaSetUrl(database);
        try (MongoClient admin = MongoClients.create(uri);
                NativeCoordinationProfile profile = NativeCoordinationProfile.openOwned(uri)) {
            var owned = admin.getDatabase(database);
            owned.runCommand(new Document("profile", 0));
            owned.getCollection("system.profile").drop();
            owned.createCollection("system.profile", new CreateCollectionOptions().capped(true).sizeInBytes(4L * 1024 * 1024));
            assertThatThrownBy(() -> profile.boundary("replaced")).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("collection was replaced");
            assertThatThrownBy(profile::close).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("replacement preserved");
            assertThat(owned.listCollectionNames()).contains("system.profile");
            assertThat(owned.runCommand(new Document("profile", -1)).get("was")).isEqualTo(0);
        } finally { drop(uri, database); }
    }

    private static MongoWorkloadClaimStore store(MongoClient client, String database) {
        return new MongoWorkloadClaimStore(client.getDatabase(database).getCollection(MongoStorePort.WORKLOAD_CLAIMS));
    }

    private static String database() { return "native_profile_" + UUID.randomUUID().toString().replace("-", ""); }

    private static void prepare(String uri, String... databases) {
        try (MongoClient client = MongoClients.create(uri)) {
            for (String database : databases) {
                client.getDatabase(database).createCollection(MongoStorePort.WORKLOAD_CLAIMS);
            }
        }
    }

    private static void drop(String uri, String... databases) {
        try (MongoClient client = MongoClients.create(uri)) {
            for (String database : databases) { client.getDatabase(database).drop(); }
        }
    }
}
