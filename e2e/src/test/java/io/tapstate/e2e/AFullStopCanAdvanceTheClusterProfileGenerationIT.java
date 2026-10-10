package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.core.ClusterMemberState;
import io.tapstate.control.core.ClusterMemberView;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.testsupport.DockerGate;
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

            try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> changes =
                    SystemCollections.CLUSTER_EXECUTION_PROFILES.on(store).watch()
                            .maxAwaitTime(100, TimeUnit.MILLISECONDS).cursor()) {
                drain(changes, cluster.clusterId(), generationChanges);
                cluster.killAll();
                assertThat(NODES).allSatisfy(node -> assertThat(cluster.isAlive(node)).isFalse());
                assertThat(liveSessions(store, cluster.clusterId())).isPositive();
                RealProcessServer early = cluster.launchCandidate(NODES.getFirst(), NEXT);
                Await.until("a mixed profile to be refused while old node authority remains", BOUND,
                        () -> !early.isAlive(), () -> "early candidate output:\n" + early.tail());
                assertThat(early.exitValue()).isNotZero();
                assertThat(Files.readString(early.output())).contains("boot.execution-profile-incompatible")
                        .contains(hash).doesNotContain("boot.node-id-in-use");
                assertThat(liveSessions(store, cluster.clusterId()))
                        .as("the negative control still lies inside a real old lease window").isPositive();
                assertThat(profile(store, cluster.clusterId()).get("generation", Number.class).longValue()).isEqualTo(generation);
                assertThat(profile(store, cluster.clusterId()).getString("hash")).isEqualTo(hash);
                drain(changes, cluster.clusterId(), generationChanges);
                assertThat(generationChanges).isEmpty();

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
                            drain(changes, cluster.clusterId(), generationChanges);
                            return !generationChanges.isEmpty();
                        }, () -> "generation mutations=" + generationChanges);
                drain(changes, cluster.clusterId(), generationChanges);
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
                drain(changes, cluster.clusterId(), generationChanges);
                assertThat(generationChanges).containsExactly(nextGeneration);
            }
        }
    }

    /** The exact generation field mutation, rather than an update-lookup of whichever profile is current later. */
    private static void drain(MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor,
            String clusterId, List<Long> generations) {
        for (ChangeStreamDocument<Document> change; (change = cursor.tryNext()) != null;) {
            if (change.getDocumentKey() == null || !clusterId.equals(change.getDocumentKey().getString("_id").getValue())) continue;
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
