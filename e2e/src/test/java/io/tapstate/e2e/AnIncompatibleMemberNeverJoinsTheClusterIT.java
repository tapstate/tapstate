package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.core.ClusterMemberState;
import io.tapstate.control.core.ClusterMemberView;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** A mismatched profile is refused before a member can bind, while compatible admission still works. */
class AnIncompatibleMemberNeverJoinsTheClusterIT {
    private static final List<String> NODES = List.of("node-a", "node-b", "node-c");
    private static final String CANDIDATE = "node-c";
    private static final Duration BOUND = Duration.ofMinutes(3);
    private static final Duration SESSION = Duration.ofSeconds(60);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void anIncompatibleBootIsRefusedBeforeItsFixedMemberPortIsNeeded() throws IOException {
        String uri = SharedMongo.replicaSetUrl("e2e_incompatible_profile_state");
        try (MongoClient mongo = MongoClients.create(uri);
                PartitionableCluster cluster = PartitionableCluster.start(uri, "incompatible-profile", NODES, SESSION)) {
            MongoDatabase store = mongo.getDatabase(new ConnectionString(uri).getDatabase());
            ControlPlane surviving = cluster.member(NODES.getFirst());
            awaitActive(surviving, NODES);
            Document profile = profile(store, cluster.clusterId());
            Document committed = membership(store, cluster.clusterId());
            assertThat(committed.getList("activeNodeIds", String.class)).containsExactlyInAnyOrderElementsOf(NODES);
            ClusterMemberView old = member(surviving, CANDIDATE);
            // Process arguments are read while the owned process is alive; no URI or other setting is reported.
            InetSocketAddress physicalListener = listenerOf(cluster.processCarrying(CANDIDATE));
            cluster.kill(CANDIDATE);
            awaitActive(surviving, NODES.stream().filter(node -> !node.equals(CANDIDATE)).toList());
            Document oldSession = nodeSession(store, cluster.clusterId(), CANDIDATE);
            assertThat(liveSessions(store, cluster.clusterId())).isPositive();

            // A real exclusive bind makes member creation-before-validation fail for a different reason.
            // The product config fixes this port and disables port auto-increment.
            try (ServerSocket reserved = new ServerSocket()) {
                reserved.setReuseAddress(false);
                reserved.bind(physicalListener);
                RealProcessServer rejected = cluster.launchCandidate(CANDIDATE,
                        Map.of("tapstate.cluster.execution-profile.heap-tier", "incompatible-profile-probe"));
                Await.until("the mismatched profile to exit before member binding", BOUND,
                        () -> !rejected.isAlive(), () -> "candidate output:\n" + rejected.tail());
                assertThat(rejected.exitValue()).isNotZero();
                assertThat(Files.readString(rejected.output())).contains("boot.execution-profile-incompatible")
                        .contains(profile.getString("hash")).doesNotContain("boot.hazelcast-unavailable")
                        .doesNotContain("boot.node-id-in-use");
                assertThat(liveSessions(store, cluster.clusterId()))
                        .as("the surviving boots still hold the profile when the refusal is read").isPositive();
                Document retained = profile(store, cluster.clusterId());
                assertThat(retained.get("generation")).isEqualTo(profile.get("generation"));
                assertThat(retained.getString("hash")).isEqualTo(profile.getString("hash"));
                assertThat(retained.get("attributes", Document.class)).isEqualTo(profile.get("attributes", Document.class));
                assertThat(membership(store, cluster.clusterId()).getList("activeNodeIds", String.class))
                        .containsExactlyInAnyOrderElementsOf(committed.getList("activeNodeIds", String.class));
                Document retainedSession = nodeSession(store, cluster.clusterId(), CANDIDATE);
                assertThat(retainedSession.getString("ownerBootId")).isEqualTo(oldSession.getString("ownerBootId"));
                assertThat(retainedSession.get("claimGeneration")).isEqualTo(oldSession.get("claimGeneration"));
                assertThat(retainedSession.get("profileGeneration")).isEqualTo(oldSession.get("profileGeneration"));
                for (String node : NODES.stream().filter(id -> !id.equals(CANDIDATE)).toList()) {
                    assertThat(cluster.member(node).clusterStatus().members().stream()
                            .filter(candidate -> candidate.state() == ClusterMemberState.ACTIVE && Boolean.TRUE.equals(candidate.live()))
                            .map(ClusterMemberView::nodeId)).doesNotContain(CANDIDATE).hasSize(2);
                }
                assertThat(member(surviving, CANDIDATE).memberUuid()).isEqualTo(old.memberUuid());
            }

            Await.until("the killed stable id's own session to expire on the Mongo clock", BOUND,
                    () -> SystemCollections.WORKLOAD_CLAIMS.on(store).find(sessionKey(cluster.clusterId(), CANDIDATE)
                            .append("$expr", new Document("$lte", List.of("$leaseUntil", "$$NOW")))).first() != null,
                    () -> "old session=" + nodeSession(store, cluster.clusterId(), CANDIDATE));
            ControlPlane admitted = cluster.relaunch(CANDIDATE);
            awaitActive(admitted, NODES);
            ClusterMemberView replacement = member(admitted, CANDIDATE);
            assertThat(replacement.bootId()).isNotEqualTo(old.bootId());
            assertThat(replacement.memberUuid()).isNotEqualTo(old.memberUuid());
            assertThat(replacement.profileGeneration()).isEqualTo(profile.get("generation", Number.class).longValue());
            assertThat(replacement.profileHash()).isEqualTo(profile.getString("hash"));
            assertThat(admitted.clusterStatus().clusterId()).isEqualTo(cluster.clusterId());
            assertThat(profile(store, cluster.clusterId()).get("generation")).isEqualTo(profile.get("generation"));
            assertThat(admitted.clusterStatus().members().stream().filter(node -> CANDIDATE.equals(node.nodeId()))).hasSize(1);
        }
    }

    private static InetSocketAddress listenerOf(RealProcessServer server) {
        String[] arguments = ProcessHandle.of(server.pid()).orElseThrow().info().arguments()
                .orElseThrow(() -> new AssertionError("the owned process does not expose its fixed member launch settings"));
        Map<String, String> settings = new LinkedHashMap<>();
        for (String argument : arguments) {
            if (argument.startsWith("--tapstate.hz.bind-address=") || argument.startsWith("--tapstate.hz.member-port=")) {
                int equals = argument.indexOf('=');
                settings.put(argument.substring(2, equals), argument.substring(equals + 1));
            }
        }
        assertThat(settings).containsOnlyKeys("tapstate.hz.bind-address", "tapstate.hz.member-port");
        return new InetSocketAddress(settings.get("tapstate.hz.bind-address"), Integer.parseInt(settings.get("tapstate.hz.member-port")));
    }

    private static void awaitActive(ControlPlane control, List<String> expected) {
        Await.until("the exact live active members " + expected, BOUND,
                () -> control.clusterStatus().members().stream()
                        .filter(member -> member.state() == ClusterMemberState.ACTIVE && Boolean.TRUE.equals(member.live()))
                        .map(ClusterMemberView::nodeId).collect(Collectors.toSet()).equals(Set.copyOf(expected)),
                () -> "members=" + control.clusterStatus().members());
    }

    private static ClusterMemberView member(ControlPlane control, String node) {
        List<ClusterMemberView> found = control.clusterStatus().members().stream().filter(member -> node.equals(member.nodeId())).toList();
        assertThat(found).hasSize(1);
        return found.getFirst();
    }

    private static Document profile(MongoDatabase store, String clusterId) {
        Document profile = SystemCollections.CLUSTER_EXECUTION_PROFILES.on(store).find(new Document("_id", clusterId)).first();
        assertThat(profile).isNotNull();
        assertThat(profile.getString("hash")).isNotBlank();
        return profile;
    }

    private static Document membership(MongoDatabase store, String clusterId) {
        Document membership = SystemCollections.CLUSTER_MEMBERSHIP.on(store).find(new Document("_id", clusterId)).first();
        assertThat(membership).isNotNull();
        return membership;
    }

    private static Document sessionKey(String clusterId, String node) {
        return new Document("clusterId", clusterId).append("resourceType", WorkloadClaimType.NODE_SESSION.name()).append("resourceId", node);
    }

    private static Document nodeSession(MongoDatabase store, String clusterId, String node) {
        Document session = SystemCollections.WORKLOAD_CLAIMS.on(store).find(sessionKey(clusterId, node)).first();
        assertThat(session).isNotNull();
        return session;
    }

    private static long liveSessions(MongoDatabase store, String clusterId) {
        return SystemCollections.WORKLOAD_CLAIMS.on(store).countDocuments(new Document("clusterId", clusterId)
                .append("resourceType", WorkloadClaimType.NODE_SESSION.name())
                .append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW"))));
    }
}
