package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.spi.store.ClusterNodeReservation;
import io.tapstate.spi.store.ClusterExecutionMember;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterNativeMember;
import io.tapstate.spi.store.ClusterNativeObservation;
import io.tapstate.spi.store.ExecutionProfile;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Replica-set witnesses for profile/session atomicity and the durable business deadline bound. */
@RequiresDocker
class ClusterProfileStoreIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final Duration TTL = Duration.ofMinutes(5);
    private static final ExecutionProfile PROFILE = profile("one");

    @Test
    void differentProfilesRacingForFirstGenerationProduceOneWinner() throws Exception {
        try (Fixture fixture = new Fixture()) {
            List<ClusterNodeReservation> results = race(fixture, profile("one"), profile("two"));
            assertThat(results).filteredOn(ClusterNodeReservation::acquired).hasSize(1);
            assertThat(results).filteredOn(result -> result.outcome() == ClusterNodeReservation.Outcome.INCOMPATIBLE)
                    .hasSize(1);
            assertThat(fixture.profiles.profile("east").orElseThrow().generation()).isEqualTo(1);
            assertThat(fixture.profiles.nodes("east")).hasSize(1);
            assertThat(fixture.claims.countDocuments()).isEqualTo(1);
        }
    }

    @Test
    void duplicateNodeBootsRaceWithoutAcquiringASecondLease() throws Exception {
        try (Fixture fixture = new Fixture(); var workers = Executors.newFixedThreadPool(2)) {
            CountDownLatch start = new CountDownLatch(1);
            var first = workers.submit(() -> { start.await(); return fixture.reserve("same", "boot-one", PROFILE); });
            var second = workers.submit(() -> { start.await(); return fixture.reserve("same", "boot-two", PROFILE); });
            start.countDown();
            List<ClusterNodeReservation> results = List.of(first.get(30, TimeUnit.SECONDS), second.get(30, TimeUnit.SECONDS));
            assertThat(results).filteredOn(ClusterNodeReservation::acquired).hasSize(1);
            assertThat(results).filteredOn(result -> result.outcome() == ClusterNodeReservation.Outcome.NODE_IN_USE)
                    .hasSize(1);
            assertThat(fixture.profiles.nodes("east")).hasSize(1);
        }
    }

    @Test
    void allExpiredSessionsAllowExactlyOneNextProfileAndOldRenewalCannotReviveIt() throws Exception {
        try (Fixture fixture = new Fixture()) {
            WorkloadClaim old = fixture.reserve("old", "boot-old", PROFILE).node().registration().nodeSession();
            fixture.claims.updateMany(new Document("resourceType", "NODE_SESSION"),
                    List.of(new Document("$set", new Document("leaseUntil", "$$NOW"))));
            fixture.expireHorizon();

            List<ClusterNodeReservation> results = race(fixture, profile("next-one"), profile("next-two"));

            assertThat(results).filteredOn(ClusterNodeReservation::acquired).hasSize(1);
            assertThat(fixture.profiles.profile("east").orElseThrow().generation()).isEqualTo(2);
            assertThat(fixture.client.getDatabase(fixture.database).getCollection("profiles").find().first()
                    .getDate("legacyAuthorityRetiredAt")).isNotNull();
            assertThat(fixture.workloads.renew(old, TTL)).isEmpty();
            assertThat(fixture.profiles.markJoined(old, "stale-uuid", "old:5701")).isFalse();
        }
    }

    @Test
    void renewalAndNextGenerationRaceCannotBothWin() throws Exception {
        try (Fixture fixture = new Fixture(); var workers = Executors.newFixedThreadPool(2)) {
            WorkloadClaim old = fixture.reserve("old", "boot-old", PROFILE).node().registration().nodeSession();
            CountDownLatch start = new CountDownLatch(1);
            var renew = workers.submit(() -> { start.await(); return fixture.workloads.renew(old, TTL); });
            var incompatible = workers.submit(() -> { start.await(); return fixture.reserve("new", "boot-new", profile("new")); });
            start.countDown();
            assertThat(renew.get(30, TimeUnit.SECONDS)).isPresent();
            assertThat(incompatible.get(30, TimeUnit.SECONDS).outcome())
                    .isEqualTo(ClusterNodeReservation.Outcome.INCOMPATIBLE);
            assertThat(fixture.profiles.profile("east").orElseThrow().generation()).isEqualTo(1);
        }
    }

    @Test
    void businessLeaseCannotOutliveShorterNodeSessionAndEarlyReleaseRevokesIt() {
        try (Fixture fixture = new Fixture()) {
            WorkloadClaim node = fixture.profiles.reserve("east", new WorkloadOwner("a", "boot-a"),
                    URI.create("http://a:8080"), PROFILE, Duration.ofSeconds(30)).node().registration().nodeSession();
            WorkloadClaim pipeline = fixture.workloads.acquire(
                    new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, "orders"), node.owner(), 1,
                    Duration.ofMinutes(10)).claim();
            assertThat(pipeline.profileGeneration()).isEqualTo(node.profileGeneration());
            assertThat(pipeline.leaseUntil()).isBeforeOrEqualTo(node.leaseUntil());
            WorkloadClaim renewed = fixture.workloads.renew(pipeline, Duration.ofMinutes(10)).orElseThrow();
            assertThat(renewed.leaseUntil()).isBeforeOrEqualTo(node.leaseUntil());

            assertThat(fixture.workloads.release(node)).isTrue();

            assertThat(fixture.workloads.read(pipeline.key()).orElseThrow().leased()).isFalse();
            assertThat(fixture.claims.find(WorkloadClaimDocuments.live(WorkloadClaimFence.from(renewed))).first()).isNull();
            assertThat(fixture.reserve("b", "boot-b", profile("next")).outcome())
                    .isEqualTo(ClusterNodeReservation.Outcome.AUTHORIZATION_HORIZON_ACTIVE);
            fixture.expireHorizon();
            WorkloadClaim successor = fixture.reserve("b", "boot-b", profile("next")).node().registration().nodeSession();
            assertThat(successor.profileGeneration()).isEqualTo(2);
            assertThat(fixture.workloads.renew(renewed, TTL)).isEmpty();
            assertThat(fixture.workloads.advanceExecution(renewed, 1, java.util.Set.of("a"))).isEmpty();
        }
    }

    @Test
    void earlyReleasePreservesThePromisedAuthorizationHorizonEvenForTheSameProfile() {
        try (Fixture fixture = new Fixture()) {
            WorkloadClaim node = fixture.reserve("a", "boot-a", PROFILE).node().registration().nodeSession();
            assertThat(fixture.workloads.release(node)).isTrue();

            assertThat(fixture.reserve("b", "boot-b", PROFILE).outcome())
                    .isEqualTo(ClusterNodeReservation.Outcome.AUTHORIZATION_HORIZON_ACTIVE);
            assertThat(fixture.profiles.profile("east").orElseThrow().generation()).isEqualTo(1);
            assertThat(fixture.profiles.nodes("east")).hasSize(1);
            fixture.expireHorizon();

            assertThat(fixture.reserve("b", "boot-b", PROFILE).profile().generation()).isEqualTo(2);
        }
    }

    @Test
    void joinEvidenceBelongsToOneBootAndSurvivesLiveObservationAbsence() {
        try (Fixture fixture = new Fixture()) {
            WorkloadClaim one = fixture.reserve("a", "boot-one", PROFILE).node().registration().nodeSession();
            fixture.reserve("b", "boot-b", PROFILE);
            assertThat(fixture.profiles.markJoined(one, "uuid-one", "a:5701")).isTrue();
            assertThat(fixture.profiles.nodes("east")).filteredOn(node -> node.registration().nodeSession().owner().nodeId().equals("a"))
                    .allSatisfy(node -> {
                        assertThat(node.leased()).isTrue();
                        assertThat(node.registration().joined()).isTrue();
                        assertThat(node.registration().memberUuid()).isEqualTo("uuid-one");
                    });
            fixture.workloads.release(one);
            WorkloadClaim two = fixture.reserve("a", "boot-two", PROFILE).node().registration().nodeSession();
            assertThat(two.profileGeneration()).isEqualTo(1);
            assertThat(two.claimGeneration()).isGreaterThan(one.claimGeneration());
            assertThat(fixture.profiles.nodes("east")).filteredOn(node -> node.registration().nodeSession().owner().nodeId().equals("a"))
                    .allSatisfy(node -> {
                        assertThat(node.registration().joined()).isFalse();
                        assertThat(node.registration().memberUuid()).isNull();
                    });
        }
    }

    @Test
    void legacyBusinessLeaseBlocksProtocolInstallationUntilItExpires() {
        try (Fixture fixture = new Fixture()) {
            MongoWorkloadClaimStore legacy = new MongoWorkloadClaimStore(fixture.claims);
            WorkloadClaim old = legacy.acquire(new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, "orders"),
                    new WorkloadOwner("old", "legacy"), 1, TTL).claim();
            assertThat(fixture.reserve("new", "boot-new", PROFILE).outcome())
                    .isEqualTo(ClusterNodeReservation.Outcome.LEGACY_LEASES_ACTIVE);
            assertThat(fixture.profiles.profile("east")).isEmpty();
            assertThat(fixture.profiles.nodes("east")).isEmpty();
            assertThat(legacy.release(old)).isTrue();
            assertThat(fixture.reserve("new", "boot-new", PROFILE).outcome())
                    .isEqualTo(ClusterNodeReservation.Outcome.LEGACY_LEASES_ACTIVE);
            fixture.claims.updateMany(new Document(), List.of(new Document("$set",
                    new Document("retiredAuthorizationUntil", "$$NOW"))));
            assertThat(fixture.reserve("new", "boot-new", PROFILE).acquired()).isTrue();
        }
    }

    private static List<ClusterNodeReservation> race(Fixture fixture, ExecutionProfile one, ExecutionProfile two) throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        try (var workers = Executors.newFixedThreadPool(2)) {
            var first = workers.submit(() -> { start.await(); return fixture.reserve("a", "boot-a", one); });
            var second = workers.submit(() -> { start.await(); return fixture.reserve("b", "boot-b", two); });
            start.countDown();
            return List.of(first.get(30, TimeUnit.SECONDS), second.get(30, TimeUnit.SECONDS));
        }
    }

    @Test
    void serverStampedPeerViewsQualifyTheCompletePairWithoutDeletingTheThirdIdentity() {
        try (Fixture fixture = new Fixture()) {
            Map<String, WorkloadClaim> nodes = fixture.nativeNodes();
            ClusterNativeObservation pair = fixture.observation(nodes, Set.of("b", "c"));
            java.time.Instant serverBefore = fixture.now();
            assertThat(fixture.profiles.recordNativeView(nodes.get("b"), 1, pair, TTL)).isTrue();
            assertThat(fixture.profiles.recordNativeView(nodes.get("c"), 1, pair, TTL)).isTrue();
            WorkloadClaim recovery = fixture.recovery(nodes.get("b"));
            var qualified = fixture.qualify(recovery);
            assertThat(qualified).isPresent();
            assertThat(qualified.orElseThrow().nodeIds()).containsExactlyInAnyOrder("b", "c");
            assertThat(fixture.profiles.nodes("east")).hasSize(3).allSatisfy(node -> assertThat(node.leased()).isTrue());
            assertThat(fixture.membershipDocuments.find().first().getList("activeNodeIds", String.class))
                    .containsExactlyInAnyOrder("a", "b", "c");
            var before = fixture.profiles.nativeView("east", "b").orElseThrow();
            assertThat(before.validUntil()).isBeforeOrEqualTo(nodes.get("b").leaseUntil());
            assertThat(before.observedAt()).isBetween(serverBefore, fixture.now());
            assertThat(fixture.profiles.recordNativeView(nodes.get("b"), 1, pair, TTL)).isTrue();
            var after = fixture.profiles.nativeView("east", "b").orElseThrow();
            assertThat(after.viewRevision()).isEqualTo(before.viewRevision() + 1);
            assertThat(after.cohortHash()).isEqualTo(before.cohortHash());
            assertThat(fixture.qualify(recovery)).isPresent();
        }
    }

    @Test
    void publicationRejectsStalePublisherAndPeerTuplesWithoutReplacingTheLatestFact() {
        try (Fixture fixture = new Fixture()) {
            Map<String, WorkloadClaim> nodes = fixture.nativeNodes();
            WorkloadClaim publisher = nodes.get("b");
            ClusterNativeObservation pair = fixture.observation(nodes, Set.of("b", "c"));
            assertThat(fixture.profiles.recordNativeView(publisher, 1, pair, TTL)).isTrue();
            var retained = fixture.profiles.nativeView("east", "b").orElseThrow();
            for (var stale : List.of(withFence(publisher, publisher.owner(), publisher.claimGeneration() + 1,
                            publisher.executionGeneration(), publisher.topologyRevision(), publisher.profileGeneration()),
                    withFence(publisher, publisher.owner(), publisher.claimGeneration(), publisher.executionGeneration() + 1,
                            publisher.topologyRevision(), publisher.profileGeneration()),
                    withFence(publisher, publisher.owner(), publisher.claimGeneration(), publisher.executionGeneration(),
                            publisher.topologyRevision() + 1, publisher.profileGeneration()),
                    withFence(publisher, new WorkloadOwner("b", "another-boot"), publisher.claimGeneration(),
                            publisher.executionGeneration(), publisher.topologyRevision(), publisher.profileGeneration()))) {
                assertThat(fixture.profiles.recordNativeView(stale, 1, pair, TTL)).isFalse();
            }
            assertThat(fixture.profiles.recordNativeView(publisher, 2, pair, TTL)).isFalse();
            assertThat(fixture.profiles.recordNativeView(withFence(publisher, publisher.owner(), publisher.claimGeneration(),
                    publisher.executionGeneration(), publisher.topologyRevision(), publisher.profileGeneration() + 1), 1, pair, TTL)).isFalse();
            var otherAttributes = new LinkedHashMap<>(pair.profile().profile().attributes());
            otherAttributes.put("buildVersion", "different");
            var otherProfile = new ClusterExecutionProfile("east", pair.profile().generation(), new ExecutionProfile(1, otherAttributes));
            assertThat(fixture.profiles.recordNativeView(publisher, 1,
                    new ClusterNativeObservation(otherProfile, pair.membersByNode()), TTL)).isFalse();
            var wrongPeer = new LinkedHashMap<>(pair.membersByNode());
            wrongPeer.put("c", new ClusterNativeMember(new ClusterExecutionMember("c", "boot-c", "another-uuid"), "c:5701"));
            assertThat(fixture.profiles.recordNativeView(publisher, 1,
                    new ClusterNativeObservation(pair.profile(), wrongPeer), TTL)).isFalse();
            wrongPeer.put("c", new ClusterNativeMember(new ClusterExecutionMember("c", "boot-c", "uuid-c"), "c:5702"));
            assertThat(fixture.profiles.recordNativeView(publisher, 1,
                    new ClusterNativeObservation(pair.profile(), wrongPeer), TTL)).isFalse();
            assertThat(fixture.profiles.nativeView("east", "b")).contains(retained);
            assertThat(fixture.profiles.markJoined(publisher, "uuid-b-new", "b:5701")).isTrue();
            assertThat(fixture.profiles.nativeView("east", "b")).isEmpty();
            assertThat(fixture.profiles.recordNativeView(publisher, 1, pair, TTL)).isFalse();
            var refreshed = new LinkedHashMap<>(pair.membersByNode());
            refreshed.put("b", new ClusterNativeMember(new ClusterExecutionMember("b", "boot-b", "uuid-b-new"), "b:5701"));
            assertThat(fixture.profiles.recordNativeView(publisher, 1,
                    new ClusterNativeObservation(pair.profile(), refreshed), TTL)).isTrue();
            assertThat(fixture.workloads.release(publisher)).isTrue();
            WorkloadClaim replacement = fixture.reserve("b", "boot-b-next", pair.profile().profile()).node().registration().nodeSession();
            assertThat(replacement.claimGeneration()).isGreaterThan(publisher.claimGeneration());
            assertThat(fixture.profiles.nativeView("east", "b")).isEmpty();
            assertThat(fixture.profiles.recordNativeView(publisher, 1, pair, TTL)).isFalse();
        }
    }

    @Test
    void peerAgreementIsRequiredAndSingletonFactsDoNotLowerTheCommittedDenominator() {
        try (Fixture fixture = new Fixture()) {
            Map<String, WorkloadClaim> nodes = fixture.nativeNodes();
            WorkloadClaim recovery = fixture.recovery(nodes.get("b"));
            ClusterNativeObservation pair = fixture.observation(nodes, Set.of("b", "c"));
            assertThat(fixture.profiles.recordNativeView(nodes.get("b"), 1, pair, TTL)).isTrue();
            assertThat(fixture.qualify(recovery)).isEmpty();
            assertThat(fixture.profiles.recordNativeView(nodes.get("c"), 1, fixture.observation(nodes, nodes.keySet()), TTL)).isTrue();
            assertThat(fixture.qualify(recovery)).isEmpty();
            assertThat(fixture.profiles.recordNativeView(nodes.get("c"), 1, pair, TTL)).isTrue();
            assertThat(fixture.qualify(recovery)).isPresent();
            assertThat(fixture.profiles.recordNativeView(nodes.get("b"), 1, fixture.observation(nodes, Set.of("b")), TTL)).isTrue();
            assertThat(fixture.qualify(recovery)).isEmpty();
            assertThat(fixture.membershipDocuments.find().first().getList("activeNodeIds", String.class)).hasSize(3);
            assertThat(fixture.profiles.nodes("east")).allSatisfy(node -> assertThat(node.leased()).isTrue());
        }
    }

    @Test
    void observationExpiryAndMissingPortBindingRemainUnknownWhileNodeLeasesStayLive() throws Exception {
        try (Fixture fixture = new Fixture()) {
            Map<String, WorkloadClaim> nodes = fixture.nativeNodes();
            WorkloadClaim recovery = fixture.recovery(nodes.get("b"));
            ClusterNativeObservation pair = fixture.observation(nodes, Set.of("b", "c"));
            MongoClusterProfileStore unbound = new MongoClusterProfileStore(fixture.client, fixture.profileDocuments,
                    fixture.claims, fixture.registryDocuments);
            assertThat(unbound.recordNativeView(nodes.get("b"), 1, pair, TTL)).isFalse();
            assertThat(fixture.profiles.recordNativeView(nodes.get("b"), 1, pair, Duration.ofSeconds(2))).isTrue();
            assertThat(fixture.profiles.recordNativeView(nodes.get("c"), 1, pair, Duration.ofSeconds(2))).isTrue();
            assertThat(fixture.qualify(recovery)).isPresent();
            long bound = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (fixture.qualify(recovery).isPresent() && System.nanoTime() < bound) { Thread.sleep(10); }
            assertThat(fixture.qualify(recovery)).isEmpty();
            assertThat(fixture.profiles.nativeView("east", "b")).isPresent();
            assertThat(fixture.profiles.nodes("east")).allSatisfy(node -> assertThat(node.leased()).isTrue());
        }
        try (Fixture legacy = new Fixture()) {
            Map<String, WorkloadClaim> nodes = legacy.nativeNodes(PROFILE);
            ClusterNativeObservation pair = legacy.observation(nodes, Set.of("b", "c"));
            assertThat(legacy.profiles.recordNativeView(nodes.get("b"), 1, pair, TTL)).isFalse();
            assertThat(legacy.profiles.nativeView("east", "b")).isEmpty();
            assertThat(legacy.qualify(legacy.recovery(nodes.get("b")))).isEmpty();
        }
    }

    private static WorkloadClaim withFence(WorkloadClaim claim, WorkloadOwner owner, long generation,
            long executionGeneration, long topologyRevision, long profileGeneration) {
        return new WorkloadClaim(claim.key(), owner, generation, executionGeneration, topologyRevision, claim.leaseUntil(),
                0, 0, Set.of(), 0, false, profileGeneration);
    }

    private static ExecutionProfile profile(String build) {
        return new ExecutionProfile(1, Map.of("buildVersion", build, "cooperativeThreads", "4"));
    }

    private static final class Fixture implements AutoCloseable {
        private final MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl());
        private final String database = "profile_" + UUID.randomUUID().toString().replace("-", "");
        private final MongoCollection<Document> claims = client.getDatabase(database).getCollection("workload_claims");
        private final MongoCollection<Document> profileDocuments = client.getDatabase(database).getCollection("profiles");
        private final MongoCollection<Document> registryDocuments = client.getDatabase(database).getCollection("node_registry");
        private final MongoCollection<Document> membershipDocuments = client.getDatabase(database).getCollection("membership");
        private final MongoClusterProfileStore profiles = new MongoClusterProfileStore(client,
                profileDocuments, claims, registryDocuments, membershipDocuments);
        private final MongoWorkloadClaimStore workloads = new MongoWorkloadClaimStore(claims, profiles);

        private ClusterNodeReservation reserve(String node, String boot, ExecutionProfile proposed) {
            return profiles.reserve("east", new WorkloadOwner(node, boot), URI.create("http://" + node + ":8080"), proposed, TTL);
        }

        private Map<String, WorkloadClaim> nativeNodes() {
            ExecutionProfile nativeProfile = new ExecutionProfile(1, Map.of("buildVersion", "native",
                    "cooperativeThreads", "4", "profile-protocol", "2", "haProfile", "PRODUCTION_HA", "bootstrapMinMembers", "3"));
            return nativeNodes(nativeProfile);
        }

        private Map<String, WorkloadClaim> nativeNodes(ExecutionProfile proposed) {
            Map<String, WorkloadClaim> result = new LinkedHashMap<>();
            for (String node : List.of("a", "b", "c")) {
                WorkloadClaim session = reserve(node, "boot-" + node, proposed).node().registration().nodeSession();
                assertThat(profiles.markJoined(session, "uuid-" + node, node + ":5701")).isTrue();
                result.put(node, session);
            }
            assertThat(new MongoClusterMembershipStore(membershipDocuments, profiles)
                    .initializeProfile(result.get("a"), 0, result.keySet())).isPresent();
            return Map.copyOf(result);
        }

        private ClusterNativeObservation observation(Map<String, WorkloadClaim> nodes, Set<String> ids) {
            Map<String, ClusterNativeMember> members = new LinkedHashMap<>();
            ids.stream().sorted().forEach(id -> members.put(id, new ClusterNativeMember(
                    new ClusterExecutionMember(id, nodes.get(id).owner().bootId(), "uuid-" + id), id + ":5701")));
            return new ClusterNativeObservation(profiles.profile("east").orElseThrow(), members);
        }

        private WorkloadClaim recovery(WorkloadClaim owner) {
            return workloads.acquire(new WorkloadClaimKey("east", WorkloadClaimType.CLUSTER_RECOVERY, "east"),
                    owner.owner(), 1, TTL).claim();
        }

        private java.time.Instant now() {
            return profileDocuments.aggregate(List.of(new Document("$match", new Document("_id", "east")),
                    new Document("$project", new Document("now", "$$NOW")))).first().getDate("now").toInstant();
        }

        private java.util.Optional<MongoClusterProfileStore.QualifiedNativeCohort> qualify(WorkloadClaim current) {
            return profiles.transaction(session -> profiles.guardNativeCohort(session, WorkloadClaimFence.from(current),
                    profiles.profile("east").orElseThrow(), membershipDocuments.find(session, new Document("_id", "east")).first()));
        }

        private void expireHorizon() {
            client.getDatabase(database).getCollection("profiles").updateOne(new Document("_id", "east"),
                    List.of(new Document("$set", new Document("authorizationUntil", "$$NOW"))));
        }

        @Override
        public void close() {
            client.getDatabase(database).drop();
            client.close();
        }
    }
}
