package io.tapstate.control.core;

import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.ExecutionPlans;
import io.tapstate.core.lifecycle.ExecutionPlan;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/**
 * The pipeline half joins three things that are true in different places: what the cluster was asked to
 * run, who has taken ownership of it, and where its work is actually happening. Each of the cases below
 * is about a way the join can be right about two of them and wrong about the third.
 */
class ClusterPipelineTopologyServiceTest {

    private static final String CLUSTER = "cluster-a";
    private static final String UUID_A = "8b0a1e6e-0000-4000-8000-00000000000a";
    private static final String UUID_B = "8b0a1e6e-0000-4000-8000-00000000000b";
    private static final Instant MEASURED = Instant.parse("2026-09-19T08:30:00Z");
    private static final List<ClusterMemberView> MEMBERS = List.of(
            member("node-a", UUID_A, ClusterMemberState.ACTIVE),
            member("node-b", UUID_B, ClusterMemberState.ACTIVE));

    private final FakeClaims claims = new FakeClaims();

    @Test
    void aPipelineTheClusterWasAskedToRunIsListedEvenWhenNothingIsRunningIt() {
        claims.put(WorkloadClaimType.PIPELINE_ACTUATION, "orders", "node-b", "boot-b1", 3, 7, true);
        ClusterPipelineTopologyService topology = new ClusterPipelineTopologyService(
                LivePipelineRuns.none(), PipelineCaptures.none(), claims, desired("orders"), CLUSTER);

        assertThat(topology.pipelines(MEMBERS))
                .extracting(ClusterPipelineView::pipelineId, ClusterPipelineView::measuredAt)
                .as("a list built from what is executing would be missing the pipeline that is supposed "
                        + "to be executing and is not, which is the first one anybody looks for")
                .containsExactly(tuple("orders", null));
    }

    @Test
    void theControllerAndEachCaptureCarryTheirOwnGenerations() {
        claims.put(WorkloadClaimType.PIPELINE_ACTUATION, "orders", "node-b", "boot-b1", 3, 7, true);
        claims.put(WorkloadClaimType.CAPTURE, "capture-1", "node-a", "boot-a1", 1, 4, true);
        ClusterPipelineTopologyService topology = new ClusterPipelineTopologyService(
                LivePipelineRuns.none(), pipelineId -> List.of("capture-1"), claims,
                desired("orders"), CLUSTER);

        ClusterPipelineView pipeline = topology.pipelines(MEMBERS).get(0);

        assertThat(pipeline.controllerClaim())
                .isEqualTo(new ClusterClaimView("orders", "node-b", "boot-b1", 3, 7, 4, true));
        assertThat(pipeline.captureClaims())
                .as("a capture's generations fence one reader of a source from the next, which is not "
                        + "the same question as which actuation of the pipeline is current -- one pair "
                        + "hung off the pipeline would have to mean the controller's")
                .containsExactly(new ClusterClaimView("capture-1", "node-a", "boot-a1", 1, 4, 4, true));
    }

    @Test
    void aCaptureNobodyHasClaimedIsAbsentRatherThanListedAsUnowned() {
        claims.put(WorkloadClaimType.CAPTURE, "capture-1", "node-a", "boot-a1", 1, 1, true);
        ClusterPipelineTopologyService topology = new ClusterPipelineTopologyService(
                LivePipelineRuns.none(), pipelineId -> List.of("capture-1", "capture-2"), claims,
                desired("orders"), CLUSTER);

        assertThat(topology.pipelines(MEMBERS).get(0).captureClaims())
                .extracting(ClusterClaimView::resourceId)
                .as("the ids are derived from the stored contract, so one with no claim filed exists "
                        + "and is unowned; an entry with a blank owner would read as a claim")
                .containsExactly("capture-1");
    }

    @Test
    void aClaimWhoseLeaseHasLapsedStillNamesItsLastOwnerAndSaysTheLeaseIsGone() {
        claims.put(WorkloadClaimType.PIPELINE_ACTUATION, "orders", "node-b", "boot-b1", 3, 7, false);
        ClusterPipelineTopologyService topology = new ClusterPipelineTopologyService(
                LivePipelineRuns.none(), PipelineCaptures.none(), claims, desired("orders"), CLUSTER);

        assertThat(topology.pipelines(MEMBERS).get(0).controllerClaim())
                .as("the record outlives its holder, and who held it last is exactly what a reader "
                        + "chasing a dead owner needs -- dropping it would leave the pipeline ownerless "
                        + "just when the owner is the question")
                .isEqualTo(new ClusterClaimView("orders", "node-b", "boot-b1", 3, 7, 4, false));
    }

    @Test
    void aVertexPinnedToOneMemberReportsOneProcessorRatherThanOnePerMember() {
        ClusterPipelineTopologyService topology = new ClusterPipelineTopologyService(
                runs(new LivePipelineVertex("source", List.of(
                        new LivePipelineProcessor(0, UUID_B, true),
                        new LivePipelineProcessor(1, UUID_A, false)))),
                PipelineCaptures.none(), claims, desired("orders"), CLUSTER);

        ClusterVertexView vertex = topology.pipelines(MEMBERS).get(0).vertices().get(0);

        assertThat(vertex.processors())
                .as("the engine puts an instance that does nothing on every member a pinned vertex is "
                        + "not running on; counting those reports a pinned vertex as running everywhere")
                .containsExactly(new ClusterProcessorView(0, UUID_B, "node-b"));
        assertThat(vertex.effective()).isEqualTo(1);
        assertThat(vertex.requested())
                .as("what ran is not what was asked for: a run with no plan recorded asked for nothing, and "
                        + "reporting the one processor that happened to run as the request would make the two "
                        + "impossible to compare")
                .isNull();
        assertThat(vertex.computedLocal())
                .as("and nothing worked out a per-member value for it, for the same reason")
                .isNull();
    }

    @Test
    void aVertexRunningAtItsNodesWidthSaysTheTargetAndTheCountItsPlanWorkedOut() {
        claims.put(WorkloadClaimType.PIPELINE_ACTUATION, "orders", "node-b", "boot-b1", 3, 7, true);
        ClusterPipelineTopologyService topology = new ClusterPipelineTopologyService(
                runs(working("route.serve.s"), working("serve.s"), working("serve.s:gather")),
                PipelineCaptures.none(), claims, desired("orders"), CLUSTER, plannedAt(7L));

        List<ClusterVertexView> vertices = topology.pipelines(MEMBERS).get(0).vertices();

        assertThat(vertices).extracting(ClusterVertexView::name, ClusterVertexView::requested,
                        ClusterVertexView::computedLocal)
                .as("the sink and its router run at the sink's width; a vertex the plan does not name "
                        + "asked for nothing")
                .containsExactly(tuple("route.serve.s", 8, 3), tuple("serve.s", 8, 3),
                        tuple("serve.s:gather", null, null));
    }

    @Test
    void aPlanWrittenForAnEarlierRunSaysNothingAboutTheRunExecuting() {
        claims.put(WorkloadClaimType.PIPELINE_ACTUATION, "orders", "node-b", "boot-b1", 3, 7, true);
        ClusterPipelineTopologyService topology = new ClusterPipelineTopologyService(
                runs(working("serve.s")), PipelineCaptures.none(), claims, desired("orders"), CLUSTER,
                plannedAt(6L));

        ClusterVertexView vertex = topology.pipelines(MEMBERS).get(0).vertices().get(0);

        assertThat(vertex.requested())
                .as("its widths were worked out for another run, and would put an old answer beside a new one")
                .isNull();
        assertThat(vertex.computedLocal()).isNull();
    }

    /** One working processor of {@code name}, on the second member. */
    private static LivePipelineVertex working(String name) {
        return new LivePipelineVertex(name, List.of(new LivePipelineProcessor(0, UUID_B, true)));
    }

    /** The plan of an execution of {@code orders} with the given generation: its sink eight wide, three a member. */
    private static ExecutionPlans plannedAt(Long executionGeneration) {
        ExecutionPlan plan = new ExecutionPlan("orders", 3L, executionGeneration, 4L, List.of("node-a", "node-b",
                "node-c"), List.of(new ExecutionPlan.Node("serve.s", 8, "explicit", "native", 3, 3, 9,
                        List.of("rounded-up"), 1024, 0L, List.of("route.serve.s", "serve.s"))), MEASURED);
        return pipelineIds -> pipelineIds.contains("orders") ? Map.of("orders", plan) : Map.of();
    }

    @Test
    void aProcessorNamesTheStableNodeAsWellAsTheEnginesIdentityForThisRunOfIt() {
        ClusterPipelineTopologyService topology = new ClusterPipelineTopologyService(
                runs(new LivePipelineVertex("serve", List.of(
                        new LivePipelineProcessor(0, UUID_A, true),
                        new LivePipelineProcessor(1, "8b0a1e6e-0000-4000-8000-00000000000c", true)))),
                PipelineCaptures.none(), claims, desired("orders"), CLUSTER);

        assertThat(topology.pipelines(MEMBERS).get(0).vertices().get(0).processors())
                .extracting(ClusterProcessorView::memberUuid, ClusterProcessorView::nodeId)
                .as("a claim names the stable id, so a reader joining placement to ownership needs it "
                        + "here; a member that has not said who it is answers null rather than being "
                        + "given the engine's identity as if it were a node name")
                .containsExactly(
                        tuple(UUID_A, "node-a"),
                        tuple("8b0a1e6e-0000-4000-8000-00000000000c", null));
    }

    @Test
    void aPictureAssembledFromOnlySomeMembersSaysSo() {
        ClusterPipelineTopologyService topology = new ClusterPipelineTopologyService(
                () -> List.of(new LivePipelineRun("orders", "exec-1", MEASURED, Set.of(UUID_B),
                        List.of(new LivePipelineVertex("serve",
                                List.of(new LivePipelineProcessor(0, UUID_B, true)))))),
                PipelineCaptures.none(), claims, desired("orders"), CLUSTER);

        ClusterPipelineView pipeline = topology.pipelines(MEMBERS).get(0);

        assertThat(pipeline.measuredFrom())
                .as("readings are collected per member, so for the first seconds of a run the picture is "
                        + "assembled from some members and not others; without this a half-measured run "
                        + "and a genuinely narrow one are the same answer")
                .containsExactly(UUID_B);
        assertThat(pipeline.measuredAt()).isEqualTo(MEASURED);
    }

    @Test
    void pipelinesComeBackInAStableOrder() {
        ClusterPipelineTopologyService topology = new ClusterPipelineTopologyService(
                LivePipelineRuns.none(), PipelineCaptures.none(), claims,
                desired("shipments", "orders"), CLUSTER);

        assertThat(topology.pipelines(MEMBERS))
                .extracting(ClusterPipelineView::pipelineId)
                .as("the store's own order is not one; a list that reshuffles between reads cannot be "
                        + "diffed by anybody")
                .containsExactly("orders", "shipments");
    }

    @Test
    void withNothingFencingThemPipelinesAreListedWithNoOwnerRatherThanAFabricatedOne() {
        ClusterPipelineTopologyService topology = new ClusterPipelineTopologyService(
                LivePipelineRuns.none(), pipelineId -> List.of("capture-1"), null,
                desired("orders"), CLUSTER);

        ClusterPipelineView pipeline = topology.pipelines(MEMBERS).get(0);

        assertThat(pipeline.controllerClaim())
                .as("a single node owns everything it runs and there is nobody to take it from; naming "
                        + "it as the holder of a claim it never filed would invent a fence")
                .isNull();
        assertThat(pipeline.captureClaims()).isEmpty();
    }

    /**
     * The face answers for every pipeline in the cluster at once, and the store it reads the claims from is
     * the one carrying every renewal in the cluster. Asked a claim at a time, that is a round trip per
     * pipeline and per capture, one after the other: a cost that grows with the cluster, which nothing
     * reports but the clock and which every answer on a small cluster hides.
     */
    @Test
    void aReadPutsOneQuestionToEachPortHoweverManyPipelinesAndCapturesItReports() {
        for (String pipelineId : List.of("invoices", "orders", "shipments")) {
            claims.put(WorkloadClaimType.PIPELINE_ACTUATION, pipelineId, "node-b", "boot-b1", 3, 7, true);
            claims.put(WorkloadClaimType.CAPTURE, "capture-" + pipelineId, "node-a", "boot-a1", 1, 1, true);
        }
        claims.put(WorkloadClaimType.CAPTURE, "capture-shared", "node-a", "boot-a1", 1, 1, true);
        AtomicInteger claimQuestions = new AtomicInteger();
        AtomicInteger captureQuestions = new AtomicInteger();
        ClusterPipelineTopologyService topology = new ClusterPipelineTopologyService(
                LivePipelineRuns.none(),
                counting(PipelineCaptures.class,
                        pipelineId -> List.of("capture-" + pipelineId, "capture-shared"), captureQuestions),
                counting(WorkloadClaimStore.class, claims, claimQuestions),
                desired("orders", "shipments", "invoices"), CLUSTER);

        List<ClusterPipelineView> pipelines = topology.pipelines(MEMBERS);

        // It really did report every claim: a read that looked nothing up would ask nothing either, and
        // the count below would be the count this is looking for.
        assertThat(pipelines)
                .extracting(pipeline -> pipeline.controllerClaim().resourceId())
                .containsExactly("invoices", "orders", "shipments");
        assertThat(pipelines)
                .extracting(pipeline -> pipeline.captureClaims().stream()
                        .map(ClusterClaimView::resourceId).toList())
                .containsExactly(
                        List.of("capture-invoices", "capture-shared"),
                        List.of("capture-orders", "capture-shared"),
                        List.of("capture-shared", "capture-shipments"));
        assertThat(new Questions(claimQuestions.get(), captureQuestions.get()))
                .as("one question to the claim store for every claim the answer reports, and one to the "
                        + "captures for every pipeline, so that what the pipelines share is read once")
                .isEqualTo(new Questions(1, 1));
    }

    @Test
    void withNothingToLookAClaimUpInNeitherTheClaimsNorTheCapturesAreAskedAnything() {
        AtomicInteger claimQuestions = new AtomicInteger();
        AtomicInteger captureQuestions = new AtomicInteger();
        PipelineCaptures captures = counting(
                PipelineCaptures.class, pipelineId -> List.of("capture-1"), captureQuestions);
        ClusterPipelineTopologyService unfenced = new ClusterPipelineTopologyService(
                LivePipelineRuns.none(), captures, null, desired("orders", "shipments"), CLUSTER);
        ClusterPipelineTopologyService unnamed = new ClusterPipelineTopologyService(
                LivePipelineRuns.none(), captures, counting(WorkloadClaimStore.class, claims, claimQuestions),
                desired("orders", "shipments"), null);

        assertThat(unfenced.pipelines(MEMBERS))
                .extracting(ClusterPipelineView::pipelineId, ClusterPipelineView::controllerClaim,
                        ClusterPipelineView::captureClaims)
                .containsExactly(tuple("orders", null, List.of()), tuple("shipments", null, List.of()));
        assertThat(unnamed.pipelines(MEMBERS))
                .extracting(ClusterPipelineView::pipelineId, ClusterPipelineView::controllerClaim,
                        ClusterPipelineView::captureClaims)
                .containsExactly(tuple("orders", null, List.of()), tuple("shipments", null, List.of()));
        assertThat(new Questions(claimQuestions.get(), captureQuestions.get()))
                .as("a capture is reported by its claim; with no claim store, or no cluster to name a claim "
                        + "in, every capture worked out would be read off the store only to be dropped")
                .isEqualTo(new Questions(0, 0));
    }

    @Test
    void aMemberThatJoinedAfterTheRunStartedIsAwaitingRebalance() {
        ClusterPipelineTopologyService topology = new ClusterPipelineTopologyService(
                runs(new LivePipelineVertex("serve",
                        List.of(new LivePipelineProcessor(0, UUID_B, true)))),
                PipelineCaptures.none(), claims, desired("orders"), CLUSTER);

        assertThat(topology.pipelines(MEMBERS).get(0).awaitingRebalance())
                .as("a run keeps the members it was planned over, so a member that arrived afterwards "
                        + "is given nothing until somebody asks for a rebalance -- and 'I added a "
                        + "machine and nothing happened' is the question this answers")
                .containsExactly("node-a");
    }

    @Test
    void aMemberHoldingOnlyAPlaceholderIsPartOfTheRunAndNotAwaitingAnything() {
        ClusterPipelineTopologyService topology = new ClusterPipelineTopologyService(
                runs(new LivePipelineVertex("source", List.of(
                        new LivePipelineProcessor(0, UUID_B, true),
                        new LivePipelineProcessor(1, UUID_A, false)))),
                PipelineCaptures.none(), claims, desired("orders"), CLUSTER);

        assertThat(topology.pipelines(MEMBERS).get(0).awaitingRebalance())
                .as("carrying no work and not being in the run are different things: a member the run "
                        + "was planned over holds an instance of every vertex, doing nothing where the "
                        + "vertex is pinned elsewhere. Reading those as absent would report every "
                        + "member of a pinned run as waiting for a rebalance")
                .isEmpty();
    }

    @Test
    void aMemberStillJoiningTheClusterIsNotReportedAsAwaitingARebalance() {
        ClusterPipelineTopologyService topology = new ClusterPipelineTopologyService(
                runs(new LivePipelineVertex("serve",
                        List.of(new LivePipelineProcessor(0, UUID_B, true)))),
                PipelineCaptures.none(), claims, desired("orders"), CLUSTER);

        assertThat(topology.pipelines(List.of(
                        member("node-a", UUID_A, ClusterMemberState.JOINING),
                        member("node-b", UUID_B, ClusterMemberState.ACTIVE)))
                        .get(0).awaitingRebalance())
                .as("it has been given no work because it is not yet entitled to any, which is a "
                        + "different thing and one the member half already says")
                .isEmpty();
    }

    @Test
    void anUnmeasuredRunSaysNobodyIsAwaitingRatherThanEverybody() {
        ClusterPipelineTopologyService topology = new ClusterPipelineTopologyService(
                () -> List.of(new LivePipelineRun("orders", null, null, Set.of(), List.of())),
                PipelineCaptures.none(), claims, desired("orders"), CLUSTER);

        assertThat(topology.pipelines(MEMBERS).get(0).awaitingRebalance())
                .as("with no reading behind it, every member holds no instance -- and reporting the "
                        + "whole cluster as waiting for a rebalance is the loudest possible way to say "
                        + "nothing has been measured yet")
                .isEmpty();
    }

    private static ClusterMemberView member(String nodeId, String uuid, ClusterMemberState state) {
        return new ClusterMemberView(
                nodeId, uuid, "boot-1", "[127.0.0.1]:5701", "https://" + nodeId + ".example", state);
    }

    private static LivePipelineRuns runs(LivePipelineVertex... vertices) {
        return () -> List.of(new LivePipelineRun(
                "orders", "exec-1", MEASURED, Set.of(UUID_A, UUID_B), List.of(vertices)));
    }

    private static DesiredStore desired(String... pipelineIds) {
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

    /**
     * {@code port}, answering as {@code answering} does and counting every question put to it, whichever of
     * the port's methods asked it: what is measured is how often the caller goes to the store, not which
     * door it goes through.
     */
    private static <T> T counting(Class<T> port, T answering, AtomicInteger questions) {
        return port.cast(Proxy.newProxyInstance(
                port.getClassLoader(), new Class<?>[] {port}, (proxy, method, arguments) -> {
                    if (method.getDeclaringClass() != Object.class) {
                        questions.incrementAndGet();
                    }
                    try {
                        return method.invoke(answering, arguments);
                    } catch (InvocationTargetException thrown) {
                        throw thrown.getCause();
                    }
                }));
    }

    /** How many questions one read put to the claim store and to the captures. */
    private record Questions(int claimStore, int captures) {
    }

    /** Claims by key, with the lease already decided, because what is under test is the join. */
    private static final class FakeClaims implements WorkloadClaimStore {

        private final Map<WorkloadClaimKey, WorkloadClaimReading> byKey = new HashMap<>();

        void put(WorkloadClaimType type, String resourceId, String nodeId, String bootId,
                long claimGeneration, long executionGeneration, boolean leased) {
            WorkloadClaimKey key = new WorkloadClaimKey(CLUSTER, type, resourceId);
            byKey.put(key, new WorkloadClaimReading(
                    new WorkloadClaim(key, new WorkloadOwner(nodeId, bootId),
                            claimGeneration, executionGeneration, 4, MEASURED),
                    leased ? Duration.ofSeconds(21) : Duration.ofSeconds(-3)));
        }

        @Override
        public WorkloadClaimAttempt acquire(
                WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
            throw new UnsupportedOperationException("a read face takes nothing");
        }

        @Override
        public Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
            throw new UnsupportedOperationException("a read face takes nothing");
        }

        @Override
        public boolean release(WorkloadClaim expected) {
            throw new UnsupportedOperationException("a read face takes nothing");
        }

        @Override
        public Optional<WorkloadClaim> advanceExecution(WorkloadClaim expected, long revision) {
            throw new UnsupportedOperationException("a read face takes nothing");
        }

        @Override
        public Optional<WorkloadClaimReading> read(WorkloadClaimKey key) {
            return Optional.ofNullable(byKey.get(key));
        }
    }
}
