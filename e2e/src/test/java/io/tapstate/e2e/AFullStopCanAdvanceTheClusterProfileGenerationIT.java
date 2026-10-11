package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.connection.ClusterConnectionMode;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.core.ClusterMemberState;
import io.tapstate.control.core.ClusterMemberView;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.testsupport.DockerGate;
import io.tapstate.testsupport.MongoHandshakeGate;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** All old authority expires on Mongo time before one racing boot opens the next immutable profile. */
class AFullStopCanAdvanceTheClusterProfileGenerationIT {
    private static final List<String> NODES = List.of("node-a", "node-b", "node-c");
    private static final Duration SESSION = Duration.ofSeconds(60);
    private static final Duration BOUND = Duration.ofMinutes(4);
    private static final String NEXT_TIER = "full-stop-next-profile";
    private static final Map<String, String> NEXT = Map.of("tapstate.cluster.execution-profile.heap-tier", NEXT_TIER);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void expiredSessionsLetExactlyOneRacingCandidateAdvanceTheProfile() throws Exception {
        String uri = SharedMongo.replicaSetUrl("e2e_full_stop_profile_state");
        try (MongoClient mongo = MongoClients.create(uri);
                PartitionableCluster cluster = PartitionableCluster.start(uri, "full-stop-profile", NODES, SESSION)) {
            MongoDatabase store = mongo.getDatabase(new ConnectionString(uri).getDatabase());
            ControlPlane first = cluster.member(NODES.getFirst());
            awaitActive(first);
            Document original = profile(store, cluster.clusterId());
            long generation = original.get("generation", Number.class).longValue();
            String hash = original.getString("hash");
            Map<String, String> oldBoots = new LinkedHashMap<>();
            for (String node : NODES) {
                oldBoots.put(node, nodeSession(store, cluster.clusterId(), node).getString("ownerBootId"));
            }
            assertThat(liveSessions(store, cluster.clusterId())).isEqualTo(NODES.size());
            List<Long> generationChanges = new ArrayList<>();
            AdmissionWitness witness = new AdmissionWitness(store, cluster.clusterId());

            try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> changes =
                    SystemCollections.CLUSTER_EXECUTION_PROFILES.on(store).watch()
                            .maxAwaitTime(100, TimeUnit.MILLISECONDS).cursor()) {
                try {
                    drain(changes, cluster.clusterId(), generationChanges, witness);
                    long negativeDeadline = System.nanoTime() + BOUND.toNanos();
                    ConnectionString upstream = new ConnectionString(uri);
                    assertThat(upstream.getHosts()).hasSize(1);
                    assertThat(MongoClientSettings.builder().applyConnectionString(upstream).build()
                            .getClusterSettings().getMode()).isEqualTo(ClusterConnectionMode.SINGLE);
                    String host = upstream.getHosts().getFirst();
                    java.net.URI address = java.net.URI.create("mongodb://" + host);
                    assertThat(address.getHost()).isIn("localhost", "127.0.0.1");
                    assertThat(uri).startsWith("mongodb://" + host + "/");
                    String collection = SystemCollections.CLUSTER_EXECUTION_PROFILES.on(store).getNamespace().getCollectionName();
                    try (MongoHandshakeGate gate = MongoHandshakeGate.beforeProfileReservation("127.0.0.1", address.getPort(),
                            upstream.getDatabase(), collection, cluster.clusterId())) {
                        String candidateUri = "mongodb://127.0.0.1:" + gate.port() + uri.substring(("mongodb://" + host).length());
                        witness.snapshot("before-early-launch");
                        try (RealProcessServer early = cluster.prepareCandidate(NODES.getFirst(), NEXT, candidateUri)) {
                            try {
                                witness.candidate(early);
                                witness.snapshot("after-early-launch");
                                gate.awaitProfileReservation(negativeRemaining(negativeDeadline));
                                witness.requestBoundary("reservation-request-held", gate);
                                assertThat(gate.capturedProfileRequests()).isEqualTo(1);
                                assertThat(gate.forwardedProfileRequests()).isZero();
                                assertThat(early.isAlive()).isTrue();
                                assertThat(liveSessions(store, cluster.clusterId())).isEqualTo(NODES.size());
                                witness.snapshot("before-kill");
                                cluster.killAll();
                                witness.requestBoundary("after-kill-before-request-release", gate);
                                assertThat(NODES).allSatisfy(node -> assertThat(cluster.isAlive(node)).isFalse());
                                assertThat(gate.forwardedProfileRequests()).isZero();
                                assertThat(liveSessions(store, cluster.clusterId())).isEqualTo(NODES.size());
                                assertThat(SystemCollections.CLUSTER_EXECUTION_PROFILES.on(store).find(new Document("_id", cluster.clusterId())
                                        .append("generation", generation).append("hash", hash)
                                        .append("$expr", new Document("$gt", List.of("$authorizationUntil", "$$NOW")))).first()).isNotNull();
                                assertThat(NODES).allSatisfy(node -> assertThat(nodeSession(store, cluster.clusterId(), node)
                                        .getString("ownerBootId")).isEqualTo(oldBoots.get(node)));
                                negativeRemaining(negativeDeadline);
                                gate.release();
                                Await.until("a mixed profile to be refused while old node authority remains", negativeRemaining(negativeDeadline),
                                        () -> {
                                            drain(changes, cluster.clusterId(), generationChanges, witness);
                                            return !early.isAlive();
                                        }, () -> "early candidate output:\n" + early.tail());
                                negativeRemaining(negativeDeadline);
                                witness.requestBoundary("early-candidate-returned", gate);
                                assertThat(gate.forwardedProfileRequests()).isPositive();
                                assertThat(early.exitValue()).isNotZero();
                                assertThat(Files.readString(early.output())).contains("boot.execution-profile-incompatible")
                                        .contains(hash).doesNotContain("boot.node-id-in-use");
                                assertThat(liveSessions(store, cluster.clusterId()))
                                        .as("the negative control still lies inside a real old lease window").isPositive();
                                assertThat(profile(store, cluster.clusterId()).get("generation", Number.class).longValue()).isEqualTo(generation);
                                assertThat(profile(store, cluster.clusterId()).getString("hash")).isEqualTo(hash);
                                drain(changes, cluster.clusterId(), generationChanges, witness);
                                assertThat(generationChanges).isEmpty();
                            } finally { gate.release(); }
                        }
                    }

                    Await.until("all old node sessions and their promised authorization horizon to expire on Mongo time", BOUND,
                            () -> liveSessions(store, cluster.clusterId()) == 0
                                    && SystemCollections.CLUSTER_EXECUTION_PROFILES.on(store).find(new Document("_id", cluster.clusterId())
                                            .append("$expr", new Document("$lte", List.of("$authorizationUntil", "$$NOW")))).first() != null,
                            () -> "live sessions=" + liveSessions(store, cluster.clusterId())
                                    + ", profile=" + profile(store, cluster.clusterId()));
                    for (String node : NODES) {
                        assertThat(nodeSession(store, cluster.clusterId(), node).getString("ownerBootId")).isEqualTo(oldBoots.get(node));
                    }
                    // The existing fixture starts every owned process before any health wait or login.
                    cluster.relaunchAll(NEXT);
                    ControlPlane reopened = cluster.member(NODES.getFirst());
                    awaitActive(reopened);
                    Document current = profile(store, cluster.clusterId());
                    long nextGeneration = current.get("generation", Number.class).longValue();
                    String nextHash = current.getString("hash");
                    assertThat(nextGeneration).isEqualTo(generation + 1);
                    assertThat(nextHash).isNotEqualTo(hash);
                    assertThat(current.get("attributes", Document.class).getString("heapTier")).isEqualTo(NEXT_TIER);
                    Await.until("the profile change stream to carry the actual single CAS advance", BOUND,
                            () -> {
                                drain(changes, cluster.clusterId(), generationChanges, witness);
                                return !generationChanges.isEmpty();
                            }, () -> "generation mutations=" + generationChanges);
                    drain(changes, cluster.clusterId(), generationChanges, witness);
                    assertThat(generationChanges).containsExactly(nextGeneration);

                    Document committed = SystemCollections.CLUSTER_MEMBERSHIP.on(store).find(new Document("_id", cluster.clusterId())).first();
                    assertThat(committed).isNotNull();
                    assertThat(committed.get("profileGeneration", Number.class).longValue()).isEqualTo(nextGeneration);
                    assertThat(committed.getList("activeNodeIds", String.class)).containsExactlyInAnyOrderElementsOf(NODES);
                    for (String node : NODES) {
                        ControlPlane control = cluster.member(node);
                        assertThat(control.clusterStatus().clusterId()).isEqualTo(cluster.clusterId());
                        assertThat(control.clusterStatus().profileGeneration()).isEqualTo(nextGeneration);
                        assertThat(control.clusterStatus().profileHash()).isEqualTo(nextHash);
                        assertThat(control.clusterProfile().generation()).isEqualTo(nextGeneration);
                        assertThat(control.clusterProfile().hash()).isEqualTo(nextHash);
                        Document session = nodeSession(store, cluster.clusterId(), node);
                        assertThat(session.get("profileGeneration", Number.class).longValue()).isEqualTo(nextGeneration);
                        assertThat(session.getString("ownerBootId")).isNotEqualTo(oldBoots.get(node));
                        assertThat(session.getString("ownerNodeId")).isEqualTo(node);
                        Document registry = SystemCollections.CLUSTER_NODE_REGISTRY.on(store)
                                .find(new Document("clusterId", cluster.clusterId()).append("nodeId", node)).first();
                        assertThat(registry).isNotNull();
                        assertThat(registry.get("profileGeneration", Number.class).longValue()).isEqualTo(nextGeneration);
                        assertThat(registry.getString("hash")).isEqualTo(nextHash);
                        assertThat(registry.getString("bootId")).isEqualTo(session.getString("ownerBootId"));
                        assertThat(registry.getBoolean("joined")).isTrue();
                        List<ClusterMemberView> observed = control.clusterStatus().members().stream()
                                .filter(member -> member.nodeId().equals(node)).toList();
                        assertThat(observed).singleElement().satisfies(member -> {
                            assertThat(member.live()).isTrue();
                            assertThat(member.sessionLeased()).isTrue();
                            assertThat(member.sessionBootId()).isEqualTo(session.getString("ownerBootId"));
                            assertThat(member.bootId()).isEqualTo(session.getString("ownerBootId"));
                            assertThat(member.profileGeneration()).isEqualTo(nextGeneration);
                            assertThat(member.profileHash()).isEqualTo(nextHash);
                            assertThat(member.memberUuid()).isEqualTo(registry.getString("memberUuid"));
                        });
                    }
                    assertThat(liveSessions(store, cluster.clusterId())).isEqualTo(NODES.size());
                    drain(changes, cluster.clusterId(), generationChanges, witness);
                    assertThat(generationChanges).containsExactly(nextGeneration);
                } catch (AssertionError | RuntimeException failure) {
                    witness.snapshot("assertion-failure");
                    witness.save(failure);
                    throw failure;
                }
            }
        }
    }

    private static Duration negativeRemaining(long deadline) {
        long remaining = deadline - System.nanoTime();
        assertThat(remaining).as("preparation and refusal share the original negative-phase budget").isPositive();
        return Duration.ofNanos(remaining);
    }

    /** The exact generation field mutation, rather than an update-lookup of whichever profile is current later. */
    private static void drain(MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor,
            String clusterId, List<Long> generations, AdmissionWitness witness) {
        for (ChangeStreamDocument<Document> change; (change = cursor.tryNext()) != null;) {
            if (change.getDocumentKey() == null || !clusterId.equals(change.getDocumentKey().getString("_id").getValue())) continue;
            witness.event(change);
            if (change.getOperationType() == OperationType.UPDATE) {
                BsonValue generation = change.getUpdateDescription().getUpdatedFields().get("generation");
                if (generation != null) {
                    assertThat(generation.isNumber()).isTrue();
                    generations.add(generation.asNumber().longValue());
                }
            } else {
                throw new AssertionError("an installed immutable profile changed without a generation-field update: "
                        + change.getOperationType());
            }
        }
    }

    /** Passive facts from this owned scene; reads and output never supply an admission decision. */
    private static final class AdmissionWitness {
        private final MongoDatabase store;
        private final String clusterId;
        private final List<Document> snapshots = new ArrayList<>();
        private final List<Document> events = new ArrayList<>();
        private final List<Document> generationEvents = new ArrayList<>();
        private Document candidate;
        private int omittedEvents;

        AdmissionWitness(MongoDatabase store, String clusterId) {
            this.store = store.withReadPreference(com.mongodb.ReadPreference.primary())
                    .withReadConcern(com.mongodb.ReadConcern.MAJORITY).withTimeout(2, TimeUnit.SECONDS);
            this.clusterId = clusterId;
        }

        void candidate(RealProcessServer process) {
            candidate = new Document("pid", process.pid()).append("nodeId", NODES.getFirst())
                    .append("requestedHeapTier", NEXT_TIER);
            var arguments = ProcessHandle.of(process.pid()).flatMap(handle -> handle.info().arguments());
            if (arguments.isPresent()) {
                List<String> prefixes = List.of("--tapstate.cluster.execution-profile.heap-tier=",
                        "--tapstate.cluster.mode=", "--tapstate.cluster.id=", "--tapstate.cluster.node-id=");
                candidate.append("actualAdmissionArguments", java.util.Arrays.stream(arguments.orElseThrow())
                        .filter(argument -> prefixes.stream().anyMatch(argument::startsWith)).toList());
            } else {
                candidate.append("actualAdmissionArgumentsUnknown", true);
            }
        }

        void snapshot(String phase) {
            Document snapshot = new Document("phase", phase);
            try {
                Document fields = new Document("_id", 1).append("generation", 1).append("hash", 1)
                        .append("authorizationUntil", 1).append("formatVersion", 1)
                        .append("attributes.heapTier", 1).append("attributes.systemDataVersion", 1)
                        .append("attributes.profile-protocol", 1).append("writeSerial", 1).append("mongoNow", 1);
                snapshot.append("profile", SystemCollections.CLUSTER_EXECUTION_PROFILES.on(store).aggregate(List.of(
                        new Document("$match", new Document("_id", clusterId)),
                        new Document("$set", new Document("mongoNow", "$$NOW")),
                        new Document("$project", fields))).maxTime(2, TimeUnit.SECONDS).first());
            } catch (RuntimeException failed) {
                snapshot.append("profileUnknown", failed.getClass().getName());
            }
            try {
                Document fields = new Document("_id", 1).append("clusterId", 1).append("resourceType", 1)
                        .append("resourceId", 1).append("ownerNodeId", 1).append("ownerBootId", 1)
                        .append("claimGeneration", 1).append("executionGeneration", 1).append("profileGeneration", 1)
                        .append("topologyRevision", 1).append("leaseUntil", 1).append("retiredAuthorizationUntil", 1)
                        .append("mongoNow", 1);
                snapshot.append("nodeSessions", SystemCollections.WORKLOAD_CLAIMS.on(store).aggregate(List.of(
                        new Document("$match", new Document("clusterId", clusterId)
                                .append("resourceType", WorkloadClaimType.NODE_SESSION.name())),
                        new Document("$set", new Document("mongoNow", "$$NOW")),
                        new Document("$project", fields))).maxTime(2, TimeUnit.SECONDS).into(new ArrayList<>()));
            } catch (RuntimeException failed) {
                snapshot.append("nodeSessionsUnknown", failed.getClass().getName());
            }
            snapshots.add(snapshot);
        }

        void requestBoundary(String phase, MongoHandshakeGate gate) {
            snapshot(phase);
            snapshots.getLast().append("capturedProfileRequests", gate.capturedProfileRequests())
                    .append("forwardedProfileRequests", gate.forwardedProfileRequests());
        }

        void event(ChangeStreamDocument<Document> change) {
            Document updated = new Document();
            if (change.getUpdateDescription() != null) {
                Document actual = Document.parse(change.getUpdateDescription().getUpdatedFields().toJson());
                for (String field : List.of("generation", "hash", "authorizationUntil", "writeSerial")) {
                    if (actual.containsKey(field)) updated.append(field, actual.get(field));
                }
                if (actual.get("attributes") instanceof Document attributes) {
                    Document fields = new Document();
                    for (String field : List.of("heapTier", "systemDataVersion", "profile-protocol")) {
                        if (attributes.containsKey(field)) fields.append(field, attributes.get(field));
                    }
                    updated.append("attributes", fields);
                }
            }
            if (updated.isEmpty()) return;
            Document observed = new Document("operationType", change.getOperationType().name())
                    .append("clusterTime", change.getClusterTime() == null ? null : change.getClusterTime().toString())
                    .append("wallTime", change.getWallTime() == null ? null : new java.util.Date(change.getWallTime().getValue()))
                    .append("updatedFields", updated);
            if (updated.containsKey("generation")) generationEvents.add(observed);
            if (events.size() < 128) events.add(observed);
            else omittedEvents++;
        }

        void save(Throwable primary) {
            try {
                java.nio.file.Path directory = java.nio.file.Path.of("target", "failure-scenes", "profile-admission", clusterId);
                Files.createDirectories(directory);
                Files.writeString(directory.resolve("admission-boundary.json"), new Document("clusterId", clusterId)
                        .append("sessionMillis", SESSION.toMillis()).append("candidate", candidate)
                        .append("snapshots", snapshots).append("profileEvents", events)
                        .append("generationEvents", generationEvents).append("omittedProfileEvents", omittedEvents)
                        .toJson(org.bson.json.JsonWriterSettings.builder().indent(true).build()));
            } catch (RuntimeException | java.io.IOException failed) {
                primary.addSuppressed(failed);
            }
        }
    }

    private static void awaitActive(ControlPlane control) {
        Await.until("all new boots to join one live compatible profile", BOUND,
                () -> control.clusterStatus().members().stream().filter(member -> member.state() == ClusterMemberState.ACTIVE
                        && Boolean.TRUE.equals(member.live())).map(ClusterMemberView::nodeId)
                        .collect(Collectors.toSet()).equals(Set.copyOf(NODES)),
                () -> "members=" + control.clusterStatus().members());
    }

    private static Document profile(MongoDatabase store, String clusterId) {
        Document profile = SystemCollections.CLUSTER_EXECUTION_PROFILES.on(store).find(new Document("_id", clusterId)).first();
        assertThat(profile).isNotNull();
        assertThat(profile.getDate("authorizationUntil")).isNotNull();
        assertThat(profile.getString("hash")).isNotBlank();
        return profile;
    }

    private static Document nodeSession(MongoDatabase store, String clusterId, String node) {
        Document session = SystemCollections.WORKLOAD_CLAIMS.on(store).find(new Document("clusterId", clusterId)
                .append("resourceType", WorkloadClaimType.NODE_SESSION.name()).append("resourceId", node)).first();
        assertThat(session).isNotNull();
        return session;
    }

    private static long liveSessions(MongoDatabase store, String clusterId) {
        return SystemCollections.WORKLOAD_CLAIMS.on(store).countDocuments(new Document("clusterId", clusterId)
                .append("resourceType", WorkloadClaimType.NODE_SESSION.name())
                .append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW"))));
    }
}
