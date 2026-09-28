package io.tapstate.app;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import io.tapstate.control.core.ClusterMemberView;
import io.tapstate.control.core.ClusterPipelineTopologyService;
import io.tapstate.control.core.ClusterTopologyService;
import io.tapstate.control.core.ClusterTopologyView;
import io.tapstate.control.core.ClusterVertexView;
import io.tapstate.control.core.LivePipelineProcessor;
import io.tapstate.control.core.LivePipelineRun;
import io.tapstate.control.core.LivePipelineVertex;
import io.tapstate.control.core.PipelineCaptures;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Where a pipeline's work is running is the cluster's answer, not the answer of whoever submitted it --
 * and the engine reports it in a shape that has two ways of being read wrongly with total confidence.
 *
 * <p>The first is a vertex pinned to one member. The engine still creates an instance of it on every
 * other member, and that instance does nothing; a reader that counted them would report a pinned vertex
 * as running everywhere, which is the opposite of what pinning it means. Those instances are told apart
 * by the type the engine names for them, and this case is what stops that recognition from rotting: if
 * the engine ever stops naming them that way, the pinned vertex below reports two workers.
 *
 * <p>The second is that the readings are collected periodically and per member, so for the first seconds
 * of a run they have arrived from some members and not others. This case waits for both, and asserts the
 * answer says which members it was assembled from -- because during that window a narrower answer is
 * being assembled rather than being true.
 *
 * <p>Two members, really started, because neither question exists on one.
 */
class EveryMemberCanSayWhereAPipelinesWorkIsRunningTest {

    private static final String CLUSTER = "pipeline-placement-test";
    private static final String PIPELINE = "orders";

    /** The engine's own delay before it would re-plan a run for a changed cluster. */
    private static final Duration SCALE_UP_DELAY =
            Duration.ofMillis(new JetConfig().getScaleUpDelayMillis());

    @Test
    void eitherMemberAnswersTheSamePlacementAndAPinnedVertexRunsInOnePlace() throws Exception {
        int[] ports = twoFreePorts();
        HazelcastInstance first = start(ports[0], ports[0], "node-a", "boot-a1");
        HazelcastInstance second = null;
        try {
            second = start(ports[1], ports[0], "node-b", "boot-b1");
            awaitMembers(first, 2);
            awaitMembers(second, 2);
            first.getJet().newJob(pinnedAndSpreadDag(), new JobConfig().setName(PIPELINE).setAutoScaling(false));

            LivePipelineRun fromFirst = awaitMeasuredFromBothMembers(first);
            LivePipelineRun fromSecond = awaitMeasuredFromBothMembers(second);

            assertThat(fromSecond.vertices())
                    .as("the same placement from either member, which is what makes it the cluster's "
                            + "answer rather than the answer of whoever was asked")
                    .isEqualTo(fromFirst.vertices());
            assertThat(fromFirst.executionId())
                    .as("and the execution these processors belong to, which is how a restarted run is "
                            + "told from the one it replaced")
                    .isNotNull();
            assertThat(fromFirst.measuredFrom())
                    .as("assembled from both members; short of that the placement below is still "
                            + "arriving, and nothing else in the answer would say so")
                    .hasSize(2);

            assertThat(workers(fromFirst, "pinned"))
                    .as("a vertex pinned to one member does its work in one place. The engine puts an "
                            + "instance on every other member that does nothing, and reporting those "
                            + "would say a pinned vertex runs everywhere")
                    .hasSize(1);
            assertThat(vertex(fromFirst, "pinned").processors())
                    .as("the instances on the other members are seen and classified, not dropped before "
                            + "they are looked at -- which is what keeps this case able to notice if the "
                            + "engine stops naming them the way it does")
                    .hasSize(2);
            assertThat(workers(fromFirst, "spread").stream()
                            .map(LivePipelineProcessor::memberUuid).collect(Collectors.toSet()))
                    .as("while an ordinary vertex really does run on both members, so the two shapes "
                            + "are distinguished rather than one rule being applied to both")
                    .hasSize(2);
            assertThat(workers(fromFirst, "spread")).extracting(LivePipelineProcessor::index)
                    .as("indices are given across the whole execution, so two members' instances can be "
                            + "told apart; numbered within each member they would collide")
                    .doesNotHaveDuplicates();
        } finally {
            if (second != null) {
                second.shutdown();
            }
            first.shutdown();
        }
    }

    @Test
    void aRunThatHasEndedIsNotReportedAsStillPlaced() throws Exception {
        int[] ports = twoFreePorts();
        HazelcastInstance only = start(ports[0], ports[0], "node-a", "boot-a1");
        try {
            only.getJet().newJob(pinnedAndSpreadDag(), new JobConfig().setName(PIPELINE).setAutoScaling(false));
            awaitMeasuredFromBothMembers(only);

            only.getJet().getJob(PIPELINE).cancel();
            awaitNoRun(only);

            assertThat(new HazelcastLivePipelineRuns(only).runs())
                    .as("the engine keeps a terminal job under its name, so a bare listing would keep "
                            + "reporting a stopped pipeline as running on the member that last ran it")
                    .isEmpty();
        } finally {
            only.shutdown();
        }
    }

    @Test
    void aMemberThatTurnsUpAfterTheRunStartedIsShownAsAwaitingARebalance() throws Exception {
        // The whole of "I added a machine and nothing is happening on it". The run keeps the members it
        // was planned over, and this is where that becomes something an operator can read rather than
        // infer. It is asked of the topology, not of the member driving the run: only that member
        // remembers what it planned over, and an answer only one node can give is not the cluster's.
        int[] ports = twoFreePorts();
        HazelcastInstance first = start(ports[0], ports[0], "node-a", "boot-a1");
        HazelcastInstance joiner = null;
        try {
            first.getJet().newJob(
                    pinnedAndSpreadDag(), new JobConfig().setName(PIPELINE).setAutoScaling(false));
            String executionBefore = awaitMeasuredFromBothMembers(first).executionId();

            joiner = start(ports[1], ports[0], "node-b", "boot-b1");
            awaitMembers(first, 2);
            // Past the engine's own delay before it would re-plan a run for a changed cluster, so this
            // is "it was not given any" rather than "it has not been given any yet".
            Thread.sleep(SCALE_UP_DELAY.plusSeconds(2).toMillis());

            ClusterTopologyView topology = topologyOf(first).topology();

            assertThat(topology.members()).extracting(ClusterMemberView::nodeId)
                    .as("both members are in the cluster -- what follows is not about reachability")
                    .containsExactly("node-a", "node-b");
            assertThat(topology.pipelines()).hasSize(1);
            assertThat(topology.pipelines().get(0).vertices())
                    .extracting(ClusterVertexView::executionId)
                    .as("the run was not disturbed: same execution throughout, so what follows is about "
                            + "a member being left out rather than about a run that was re-planned")
                    .containsOnly(executionBefore);
            assertThat(topology.pipelines().get(0).awaitingRebalance())
                    .as("and the one that arrived after the run started is carrying none of it, which "
                            + "is what the member list on its own cannot say")
                    .containsExactly("node-b");
            assertThat(topologyOf(joiner).topology().pipelines().get(0).awaitingRebalance())
                    .as("the member that is waiting says the same about itself, because the answer is "
                            + "read off the run rather than off what any one member remembers")
                    .containsExactly("node-b");
        } finally {
            if (joiner != null) {
                joiner.shutdown();
            }
            first.shutdown();
        }
    }

    /** The topology read face over a real member, with nothing committed and nothing fenced. */
    private static ClusterTopologyService topologyOf(HazelcastInstance member) {
        return new ClusterTopologyService(
                new HazelcastLiveClusterMembers(member),
                null,
                new ClusterPipelineTopologyService(
                        new HazelcastLivePipelineRuns(member), PipelineCaptures.none(), null,
                        desiredOf(PIPELINE), CLUSTER),
                CLUSTER);
    }

    private static DesiredStore desiredOf(String... pipelineIds) {
        return new DesiredStore() {
            @Override
            public void save(DesiredState state) {
                throw new UnsupportedOperationException("a read face writes nothing");
            }

            @Override
            public Optional<DesiredState> read(String pipelineId) {
                return Optional.empty();
            }

            @Override
            public List<String> pipelineIds() {
                return List.of(pipelineIds);
            }

            @Override
            public void delete(String pipelineId) {
                throw new UnsupportedOperationException("a read face writes nothing");
            }
        };
    }

    private static LivePipelineVertex vertex(LivePipelineRun run, String name) {
        return run.vertices().stream().filter(v -> v.name().equals(name)).findFirst().orElseThrow();
    }

    private static List<LivePipelineProcessor> workers(LivePipelineRun run, String vertex) {
        return vertex(run, vertex).processors().stream()
                .filter(LivePipelineProcessor::working).toList();
    }

    /**
     * The run once every member has reported it. The engine collects its readings on its own cadence,
     * so a run is known to exist several seconds before its placement is, and asserting on the first
     * answer would assert on half of one.
     */
    private static LivePipelineRun awaitMeasuredFromBothMembers(HazelcastInstance member)
            throws InterruptedException {
        int members = member.getCluster().getMembers().size();
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        LivePipelineRun last = null;
        while (System.nanoTime() < deadline) {
            Optional<LivePipelineRun> run = new HazelcastLivePipelineRuns(member).runs().stream()
                    .filter(r -> r.pipelineId().equals(PIPELINE)).findFirst();
            if (run.isPresent()) {
                last = run.get();
                if (last.measuredFrom().size() == members && !last.vertices().isEmpty()) {
                    return last;
                }
            }
            Thread.sleep(100);
        }
        throw new AssertionError("the run was never measured from all " + members
                + " members; the last answer was " + last);
    }

    private static void awaitNoRun(HazelcastInstance member) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            if (member.getJet().getJob(PIPELINE).getStatus().isTerminal()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("the run never ended");
    }

    /** One vertex the engine pins to a single member, and one it is free to spread over both. */
    private static DAG pinnedAndSpreadDag() {
        DAG dag = new DAG();
        Vertex pinned = dag.newVertex("pinned", ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) ForeverSource::new)));
        Vertex spread = dag.newVertex(
                "spread", ProcessorSupplier.of((SupplierEx<Processor>) Swallow::new));
        dag.edge(Edge.between(pinned, spread).distributed());
        return dag;
    }

    /** A member started the way the assembly starts one. */
    private static HazelcastInstance start(int memberPort, int seedPort, String nodeId, String bootId) {
        HazelcastProperties properties = new HazelcastProperties();
        properties.setClusterName(CLUSTER);
        properties.setMemberPort(memberPort);
        properties.getDiscovery().setMode(HazelcastProperties.DiscoveryMode.TCP_IP);
        properties.getDiscovery().getTcpIp().setSeeds(List.of("127.0.0.1:" + seedPort));
        ClusterMemberPreflight.Identity identity = new ClusterMemberPreflight.Identity(
                CLUSTER, nodeId, URI.create("https://" + nodeId + ".example:8443"),
                nodeSession(nodeId, bootId));
        return HazelcastConfiguration.startMember(() -> Hazelcast.newHazelcastInstance(
                HazelcastConfiguration.identify(
                        HazelcastConfiguration.memberConfig(properties), identity)));
    }

    private static WorkloadClaim nodeSession(String nodeId, String bootId) {
        return new WorkloadClaim(
                new WorkloadClaimKey(CLUSTER, WorkloadClaimType.NODE_SESSION, nodeId),
                new WorkloadOwner(nodeId, bootId), 1, 0, 1, Instant.now().plusSeconds(30));
    }

    private static int[] twoFreePorts() throws Exception {
        try (ServerSocket first = new ServerSocket(0); ServerSocket second = new ServerSocket(0)) {
            return new int[] {first.getLocalPort(), second.getLocalPort()};
        }
    }

    private static void awaitMembers(HazelcastInstance member, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (member.getCluster().getMembers().size() != expected && System.nanoTime() < deadline) {
            Thread.sleep(25);
        }
        assertThat(member.getCluster().getMembers()).hasSize(expected);
    }

    /** A source that never ends, so the run stays up while it is being read. */
    private static final class ForeverSource extends AbstractProcessor {

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            try {
                Thread.sleep(5);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
    }

    private static final class Swallow extends AbstractProcessor {

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            return true;
        }
    }
}
