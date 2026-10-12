package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.tapstate.e2e.ClusterLifecycleProbe.Phase.*;
import static org.assertj.core.api.Assertions.assertThat;

/** Actual initial cluster actions are observed while membership is independently read from the product. */
class InitialClusterLifecycleIT {
    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void theRealTwoMemberFixturePreparesAndLaunchesBothBeforeAnyHealthWait() {
        List<ClusterLifecycleProbe.Event> events = new ArrayList<>();
        Map<String, ProcessHandle> processes = new LinkedHashMap<>();
        String id;
        try (ClusterLifecycleProbe.Scope ignored = ClusterLifecycleProbe.install(event -> collect(event, events, processes))) {
            String store = SharedMongo.replicaSetUrl(database("two"));
            try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "observed-two")) {
                id = cluster.first().clusterId();
                assertThat(cluster.awaitBothMembers()).containsExactly(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
                assertThat(cluster.second().clusterMemberNodeIds())
                        .containsExactly(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
                assertThat(cluster.second().clusterId()).isEqualTo(id);
                assertThat(cluster.first().healthy()).isTrue();
                assertThat(cluster.second().healthy()).isTrue();
            }
        }
        assertLifecycle(events, processes, id, List.of(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B));
    }

    @Test
    void theRealThreeMemberFixturePreparesAndLaunchesAllBeforeAnyHealthWait() {
        List<String> nodes = List.of("node-a", "node-b", "node-c");
        List<ClusterLifecycleProbe.Event> events = new ArrayList<>();
        Map<String, ProcessHandle> processes = new LinkedHashMap<>();
        String id;
        try (ClusterLifecycleProbe.Scope ignored = ClusterLifecycleProbe.install(event -> collect(event, events, processes))) {
            String store = SharedMongo.replicaSetUrl(database("three"));
            try (PartitionableCluster cluster = PartitionableCluster.start(store, "observed-three", nodes)) {
                id = cluster.clusterId();
                assertThat(cluster.awaitMembers("node-a", 3)).containsExactlyInAnyOrderElementsOf(nodes);
                for (String node : nodes) {
                    assertThat(cluster.member(node).clusterMemberNodeIds()).containsExactlyInAnyOrderElementsOf(nodes);
                    assertThat(cluster.member(node).clusterId()).isEqualTo(id);
                    assertThat(cluster.member(node).healthy()).isTrue();
                }
                assertThat(cluster.unattributedConnections()).isZero();
            }
        }
        assertLifecycle(events, processes, id, nodes);
    }

    private static void collect(ClusterLifecycleProbe.Event event, List<ClusterLifecycleProbe.Event> events,
            Map<String, ProcessHandle> processes) {
        events.add(event);
        if (event.phase() == LAUNCHED) {
            processes.put(event.launch(), ProcessHandle.of(event.pid())
                    .orElseThrow(() -> new AssertionError("the owned launch has no observable process")));
        }
    }

    private static void assertLifecycle(List<ClusterLifecycleProbe.Event> events, Map<String, ProcessHandle> processes,
            String clusterId, List<String> nodes) {
        String clusterHash = ClusterLifecycleProbe.hash(clusterId);
        assertThat(events).isNotEmpty().allSatisfy(event -> assertThat(event.cluster()).isEqualTo(clusterHash));
        List<ClusterLifecycleProbe.Event> prepared = phase(events, PREPARED);
        List<ClusterLifecycleProbe.Event> launched = phase(events, LAUNCHED);
        assertThat(prepared).hasSize(nodes.size());
        assertThat(launched).hasSize(nodes.size());
        assertThat(prepared.stream().map(ClusterLifecycleProbe.Event::node).toList())
                .containsExactlyInAnyOrderElementsOf(nodes);
        assertThat(prepared.stream().map(ClusterLifecycleProbe.Event::launch).toList()).doesNotHaveDuplicates();
        assertThat(prepared.stream().map(ClusterLifecycleProbe.Event::httpPort).toList()).doesNotHaveDuplicates();
        assertThat(prepared.stream().map(ClusterLifecycleProbe.Event::memberPort).toList()).doesNotHaveDuplicates();
        assertThat(prepared.stream().map(ClusterLifecycleProbe.Event::work).toList()).doesNotHaveDuplicates();
        assertThat(prepared.stream().map(ClusterLifecycleProbe.Event::staging).toList()).doesNotHaveDuplicates();
        for (ClusterLifecycleProbe.Phase phase : List.of(HEALTH_STARTED, HEALTH_PASSED, QUIT_REQUESTED, EXIT_CONFIRMED)) {
            List<ClusterLifecycleProbe.Event> selected = phase(events, phase);
            assertThat(selected).hasSize(nodes.size());
            assertThat(selected.stream().map(ClusterLifecycleProbe.Event::launch).toList())
                    .containsExactlyInAnyOrderElementsOf(prepared.stream().map(ClusterLifecycleProbe.Event::launch).toList());
        }
        assertThat(last(events, PREPARED)).as("every descriptor exists before the first actual launch")
                .isLessThan(first(events, LAUNCHED));
        assertThat(last(events, LAUNCHED)).as("every actual JVM starts before the first health wait")
                .isLessThan(first(events, HEALTH_STARTED));
        assertThat(last(events, QUIT_REQUESTED)).as("all final quit requests precede the first confirmed exit")
                .isLessThan(first(events, EXIT_CONFIRMED));
        for (ClusterLifecycleProbe.Event event : prepared) {
            assertThat(event.httpPort()).isPositive();
            assertThat(event.memberPort()).isPositive();
            assertThat(event.work()).matches("[0-9a-f]{64}");
            assertThat(event.output()).matches("[0-9a-f]{64}");
            assertThat(event.staging()).matches("[0-9a-f]{64}");
            assertThat(event.parameters()).matches("[0-9a-f]{64}");
            List<ClusterLifecycleProbe.Event> own = events.stream().filter(item -> item.launch().equals(event.launch())).toList();
            assertThat(own).allSatisfy(item -> {
                assertThat(item.node()).isEqualTo(event.node());
                assertThat(item.work()).isEqualTo(event.work());
                assertThat(item.staging()).isEqualTo(event.staging());
                assertThat(item.parameters()).isEqualTo(event.parameters());
            });
            long pid = own.stream().filter(item -> item.phase() == LAUNCHED).findFirst().orElseThrow().pid();
            assertThat(pid).isPositive();
            assertThat(own.stream().filter(item -> item.phase() != PREPARED).map(ClusterLifecycleProbe.Event::pid).toList())
                    .containsOnly(pid);
            assertThat(processes.get(event.launch()).isAlive()).isFalse();
        }
    }

    private static List<ClusterLifecycleProbe.Event> phase(List<ClusterLifecycleProbe.Event> events,
            ClusterLifecycleProbe.Phase phase) {
        return events.stream().filter(event -> event.phase() == phase).toList();
    }

    private static int first(List<ClusterLifecycleProbe.Event> events, ClusterLifecycleProbe.Phase phase) {
        for (int index = 0; index < events.size(); index++) if (events.get(index).phase() == phase) return index;
        throw new AssertionError("missing fixture phase " + phase);
    }

    private static int last(List<ClusterLifecycleProbe.Event> events, ClusterLifecycleProbe.Phase phase) {
        for (int index = events.size() - 1; index >= 0; index--) if (events.get(index).phase() == phase) return index;
        throw new AssertionError("missing fixture phase " + phase);
    }

    private static String database(String name) {
        return "e2e_initial_probe_" + name + "_" + UUID.randomUUID().toString().replace("-", "");
    }
}
