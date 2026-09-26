package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import io.tapstate.core.common.TapstateException;
import io.tapstate.control.core.PipelineIncarnationService;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.srs.CaptureHealth;
import io.tapstate.runtime.srs.CaptureId;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.ExecutionGenerationStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The assembly-layer binding from the lifecycle actuator seam to the Jet engine and the capture coordinator,
 * driven against a real embedded member. It runs a pipeline through start -> pause -> resume -> stop over an
 * idle stand-in topology and proves the verb composition: start fills the capture before submitting the job
 * that reads it; pause and resume are engine-only (the capture keeps running); stop cancels the job before
 * stopping the capture behind it. The same actuator drives the store-backed topology production runs, by
 * pipeline id alone.
 */
class EngineLifecycleActuatorTest {

    @Test
    void startBindsTheArtifactIncarnationToItsDurableExecutionGenerationForObservations() {
        List<String> events = new CopyOnWriteArrayList<>();
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        ArtifactStore artifacts = new ArtifactStore() {
            @Override public void saveAll(List<io.tapstate.core.model.Resource> resources) {
                throw new UnsupportedOperationException();
            }
            @Override public Optional<io.tapstate.core.model.Resource> get(String id) {
                return Optional.empty();
            }
            @Override public List<io.tapstate.core.model.Resource> list() { return List.of(); }
            @Override public Optional<String> ensurePipelineIncarnationId(String id, String candidate) {
                assertThat(id).isEqualTo(PIPE);
                return Optional.of("inc-a");
            }
        };
        EngineLifecycleActuator actuator = new EngineLifecycleActuator(new Engine(member),
                new RecordingDagSource(events), new RecordingCaptureCoordinator(events), teardown(),
                PipelineActuationOwnership.single("single", new InMemoryWorkloadClaimStore()),
                new PipelineIncarnationService(artifacts), scopes);

        actuator.start(PIPE);

        assertThat(scopes.current(PIPE)).contains(new io.tapstate.spi.store.ObservationStore.Scope("inc-a", 1));
        assertThat(events).containsExactly("startCapture:" + PIPE, "buildDag:" + PIPE);
    }

    private static final String PIPE = "orders-pipe";

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    @DisplayName("start captures then submits, pause/resume touch only the engine, stop cancels then stops capture")
    void composesCaptureAndEngineAcrossTheFullLifecycle() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        RecordingDagSource dagSource = new RecordingDagSource(events);
        InMemoryWorkloadClaimStore generations = new InMemoryWorkloadClaimStore();
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(new Engine(member), dagSource,
                coordinator, teardown(), PipelineActuationOwnership.single("single", generations));
        // At stop the coordinator awaits the pipeline's job going terminal; that only happens if the cancel ran
        // before the capture stop, so it discriminates the stop ordering rather than racing it.
        coordinator.jobTerminalProbe = () -> awaitTerminal(member.getJet().getJob(PIPE));
        coordinator.jobAbsentProbe = () -> member.getJet().getJob(PIPE) == null;

        actuator.start(PIPE);
        // Capture opens and fills the ring before the DAG is built and submitted against that generation.
        assertThat(events).containsExactly("startCapture:" + PIPE, "buildDag:" + PIPE);
        assertThat(coordinator.jobWasAbsentAtStart).isTrue();
        assertThat(dagSource.fences).as("even a standalone submission has a durable run identity")
                .singleElement().isNotNull();
        assertThat(dagSource.fences.getFirst().claimGeneration()).isZero();
        assertThat(dagSource.fences.getFirst().executionGeneration()).isEqualTo(1);
        Job job = member.getJet().getJob(PIPE);
        assertThat(job).as("start submits a job named by the pipeline id").isNotNull();
        awaitStatus(job, JobStatus.RUNNING);

        actuator.pause(PIPE);
        awaitStatus(job, JobStatus.SUSPENDED);
        actuator.resume(PIPE);
        awaitStatus(job, JobStatus.RUNNING);
        // Pause and resume are engine-only: the capture keeps running, so the coordinator is never touched.
        assertThat(events).containsExactly("startCapture:" + PIPE, "buildDag:" + PIPE);
        assertThat(generations.executionGeneration(standaloneKey())).isEqualTo(1);

        actuator.stop(PIPE, true);
        awaitStatus(job, JobStatus.FAILED); // Jet reports a cancelled job as FAILED
        // Stop cancels the job, then stops the capture behind it: the job was already terminal when capture stopped.
        assertThat(events).containsExactly(
                "startCapture:" + PIPE, "buildDag:" + PIPE,
                "stopCapture:" + PIPE + "[purge][jobTerminal]");
        assertThat(generations.executionGeneration(standaloneKey())).isEqualTo(1);
    }

    @Test
    void aJoblessRestartClosesAnOldCaptureBeforeOpeningTheNewReader() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        coordinator.activeCapture = true;
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), new RecordingDagSource(events), coordinator, teardown());

        actuator.start(PIPE);

        assertThat(events).containsExactly(
                "stopCapture:" + PIPE + "[keep][jobLive]",
                "startCapture:" + PIPE,
                "buildDag:" + PIPE);
        assertThat(coordinator.activeCapture).isTrue();
    }

    @Test
    void aDuplicateStartWithALiveJobKeepsItsCapture() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        InMemoryWorkloadClaimStore generations = new InMemoryWorkloadClaimStore();
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), new RecordingDagSource(events), coordinator, teardown(),
                PipelineActuationOwnership.single("single", generations));

        actuator.start(PIPE);
        awaitStatus(member.getJet().getJob(PIPE), JobStatus.RUNNING);
        actuator.start(PIPE);

        assertThat(events).containsExactly("startCapture:" + PIPE, "buildDag:" + PIPE);
        assertThat(generations.executionGeneration(standaloneKey()))
                .as("the live job was not submitted a second time").isEqualTo(1);
    }

    @Test
    void anUnconfirmedStandaloneGenerationRefusesBeforeCaptureOrJobSubmission() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        RecordingDagSource dagSource = new RecordingDagSource(events);
        ExecutionGenerationStore unavailable = new ExecutionGenerationStore() {
            @Override
            public Optional<WorkloadClaim> advanceUnderClaim(WorkloadClaim expected, long revision) {
                throw new UnsupportedOperationException();
            }

            @Override
            public OptionalLong advanceStandalone(String clusterId, String pipelineId) {
                throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "unreachable"), null);
            }
        };
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), dagSource, coordinator, teardown(),
                PipelineActuationOwnership.single("single", unavailable));

        assertThatThrownBy(() -> actuator.start(PIPE))
                .isInstanceOfSatisfying(TapstateException.class, failure ->
                        assertThat(failure.code()).isEqualTo(ActuationError.EXECUTION_GENERATION_UNAVAILABLE));
        assertThat(events).isEmpty();
        assertThat(member.getJet().getJob(PIPE)).isNull();
    }

    @Test
    void aCancelledStartClosesItsCaptureWithoutSubmittingOrPurging() {
        for (boolean cancelDuringBuild : List.of(false, true)) {
            List<String> events = new CopyOnWriteArrayList<>();
            RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
            RecordingDagSource dagSource = new RecordingDagSource(events);
            coordinator.interruptAfterStart = !cancelDuringBuild;
            dagSource.interruptDuringBuild = cancelDuringBuild;
            LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                    new Engine(member), dagSource, coordinator, teardown());

            try {
                actuator.start(PIPE);
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
            } finally {
                Thread.interrupted();
            }
            assertThat(coordinator.activeCapture).isFalse();
            assertThat(member.getJet().getJob(PIPE)).isNull();
            assertThat(events).contains("startCapture:" + PIPE,
                    "stopCapture:" + PIPE + "[keep][jobLive]");
            if (cancelDuringBuild) {
                assertThat(events).contains("buildDag:" + PIPE);
            } else {
                assertThat(events).doesNotContain("buildDag:" + PIPE);
            }
        }
    }

    @Test
    void aPreparedStartPassesTheSameCursorTokenToCapture() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        RecordingDagSource dagSource = new RecordingDagSource(events);
        dagSource.artifactSnapshot = new InMemoryArtifactStore();
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), dagSource, coordinator, teardown());

        actuator.start(PIPE);

        assertThat(dagSource.preparedTokens).hasSize(1);
        assertThat(coordinator.captureTokens).containsExactly(dagSource.preparedTokens.getFirst());
    }

    /**
     * A start the capture side gives back submits nothing and throws nothing. The pipeline is left carrying
     * no job, which is what the next pass starts again -- rather than a job over a ring nobody opened, or a
     * failure recorded for a capture another member is still opening.
     */
    @Test
    void aStartTheCaptureGivesBackSubmitsNothing() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        coordinator.givesTheStartBack = true;
        RecordingDagSource dagSource = new RecordingDagSource(events);
        LifecycleActuator actuator =
                TestEngineLifecycleActuators.create(new Engine(member), dagSource, coordinator, teardown());

        actuator.start(PIPE);

        assertThat(events).containsExactly("startCapture:" + PIPE);
        assertThat(member.getJet().getJob(PIPE)).as("no job was submitted").isNull();
        assertThat(actuator.isCarryingAJob(PIPE)).isFalse();
    }

    @Test
    void surfacesACaptureFailureThroughTheSeamWhenTheEngineJobReportsNone() {
        RuntimeException boom = new RuntimeException("cdc tail died");
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(new CopyOnWriteArrayList<>());
        coordinator.captureFailure = boom;
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), new IdleDagSource(), coordinator, teardown());

        // No job was submitted, so the engine reports no failure; the cdc capture's death still surfaces through
        // the seam the converge loop reads, which is what drives a pipeline whose tail died into FAILED even
        // though its Jet job keeps running over a ring gone quiet.
        assertThat(actuator.failure(PIPE)).contains(boom);
    }

    @Test
    void validatesBeforeCaptureOrSubmission() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        RecordingDagSource dagSource = new RecordingDagSource(events);
        TapstateException refused = new TapstateException(
                ActuationError.SOURCE_SCHEMA_NOT_DISCOVERED, Map.of("source", "orders_src"), null);
        dagSource.validation = () -> {
            events.add("validate:" + PIPE);
            throw refused;
        };
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), dagSource, coordinator, teardown());

        assertThatThrownBy(() -> actuator.start(PIPE)).isSameAs(refused);
        assertThat(events).containsExactly("validate:" + PIPE);
    }

    @Test
    void passesThePreparedArtifactSnapshotToCapture() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        RecordingDagSource dagSource = new RecordingDagSource(events);
        ArtifactStore snapshot = ReadOnlyArtifactSnapshot.capture(new InMemoryArtifactStore());
        dagSource.artifactSnapshot = snapshot;
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), dagSource, coordinator, teardown());
        coordinator.jobTerminalProbe = () -> awaitTerminal(member.getJet().getJob(PIPE));

        actuator.start(PIPE);
        try {
            assertThat(coordinator.artifactSnapshot).isSameAs(snapshot);
        } finally {
            actuator.stop(PIPE, true);
        }
    }

    @Test
    void theTopologyAStartBuildsIsHeldToThatStartsOwnRun() {
        // The run's generation is taken before the first side effect; the topology is built afterwards, once
        // placement and any outstanding teardown are settled. Everything else a start does sits between the
        // two, and nothing local ties them together -- so a build handed no generation is the quiet failure
        // here: it reaches every external effect unfenced, and a run nothing fences is a run nothing can
        // later stop from writing beside the run that replaced it.
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        RecordingDagSource dagSource = new RecordingDagSource(events);
        PipelineActuationOwnership ownership = clusteredOwnership();
        assertThat(ownership.permit(PIPE).granted())
                .as("the member has to hold the pipeline before a start of it can be fenced at all")
                .isTrue();
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), dagSource, coordinator, teardown(), ownership);
        coordinator.jobTerminalProbe = () -> awaitTerminal(member.getJet().getJob(PIPE));

        actuator.start(PIPE);
        try {
            assertThat(dagSource.fences).as("the topology was built exactly once").hasSize(1);
            ExecutionFence fence = dagSource.fences.get(0);
            assertThat(fence).as("the build was given a run to be held to, not none").isNotNull();
            assertThat(fence.pipelineId()).isEqualTo(PIPE);
            assertThat(fence.executionGeneration())
                    .as("and it is this start's own generation, the one begun a moment earlier")
                    .isEqualTo(1);
        } finally {
            actuator.stop(PIPE, true);
        }
    }

    @Test
    void reportsNoFailureWhenNeitherTheJobNorTheCaptureHasFailed() {
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(new CopyOnWriteArrayList<>());
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), new IdleDagSource(), coordinator, teardown());

        assertThat(actuator.failure(PIPE)).isEmpty();
    }

    /**
     * A hold that landed before the load reached the target cannot be carried on from in place, so this
     * resume rebuilds: a stop that keeps, then a start.
     *
     * <p>The rows a load has read but not delivered reach the source vertex through a member-local hand-off
     * that vertex consumes once, and a resume restarts the job under a guarantee that keeps no execution
     * state -- so the vertex that comes back finds the hand-off empty over a capture that has moved on, and
     * reads nothing at all while reporting healthy. A start is what fills it again.
     *
     * <p>Asserted on the verbs and their order, which is the whole of what this seam decides. That the stop
     * is the keeping one is asserted with them and is load-bearing: a purging stop would throw away the
     * position of a pipeline nobody asked to clear.
     */
    @Test
    void aResumeOverALoadThatHasNotReachedTheTargetRebuildsInsteadOfCarryingOn() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        RecordingDagSource dagSource = new RecordingDagSource(events);
        InMemoryWorkloadClaimStore generations = new InMemoryWorkloadClaimStore();
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(new Engine(member), dagSource,
                coordinator, teardown(), PipelineActuationOwnership.single("single", generations));
        coordinator.jobTerminalProbe = () -> awaitTerminal(member.getJet().getJob(PIPE));

        actuator.start(PIPE);
        Job held = member.getJet().getJob(PIPE);
        awaitStatus(held, JobStatus.RUNNING);
        actuator.pause(PIPE);
        awaitStatus(held, JobStatus.SUSPENDED);

        coordinator.loadDelivered = false;
        actuator.resume(PIPE);

        assertThat(events).containsExactly(
                "startCapture:" + PIPE, "buildDag:" + PIPE,
                "stopCapture:" + PIPE + "[keep][jobTerminal]",
                "startCapture:" + PIPE, "buildDag:" + PIPE);
        Job rebuilt = member.getJet().getJob(PIPE);
        assertThat(rebuilt).as("the rebuild submits a job of its own").isNotSameAs(held);
        awaitStatus(rebuilt, JobStatus.RUNNING);
        assertThat(dagSource.fences).extracting(ExecutionFence::executionGeneration).containsExactly(1L, 2L);
        assertThat(generations.executionGeneration(standaloneKey())).isEqualTo(2);
    }

    /**
     * The same hold over a pipeline that reads its source once and opens no tail. It rebuilds too, and for
     * the same reason: what a resume cannot carry on from is the load, and a load is no less unfinished for
     * having no tail behind it.
     *
     * <p>Its rows travel exactly as any other load's do -- appended to the member-local hand-off by the
     * capture, taken from it by the source vertex, which consumes what is there once. A resume re-runs the
     * topology under a guarantee that keeps no execution state, so the vertex that comes back finds the
     * hand-off empty and there is nothing behind it to fill it again: no tail, and a capture that has
     * already returned. It emits nothing at all, for ever, with the job running, the pipeline reporting
     * healthy and nothing thrown -- and every row the hold caught in flight is absent from the target for
     * good.
     *
     * <p>Driven through the store-backed coordinator rather than the stand-in above, because the stand-in
     * is told the answer and this case is about how that answer is reached. A read that opens no tail opens
     * no chain either, so nothing durable records what its load delivered, and this is exactly the shape
     * where the question has to be answered without a record to answer it from.
     *
     * <p>Asserted on the capture being run a second time, which is the whole of what a rebuild is for here:
     * the run that refills the hand-off the resumed vertex is about to read.
     */
    @Test
    void aResumeOverASnapshotOnlyLoadThatHasNotReachedTheTargetRebuildsAsWell() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders")), null, null));
        artifacts.save(new PipelineResource(PIPE, null, List.of(SourceRef.spec("orders_src", true)), null, null,
                new ServeBlock.Inline(null, FromRef.literal("orders_src"),
                        List.of(new SyncElement("sync_1", "orders_src", null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.SNAPSHOT_ONLY, "earliest"), null));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        int[] captures = {0};
        // What a snapshot-only run really hands back: rows read, and no chain, because it opens no tail.
        CaptureStarter starter = (spec, passthrough) -> {
            captures[0]++;
            return new CaptureRun(Optional.empty(), false, 2L, Map.of("orders", 2L),
                    Optional.empty(), Optional.of(() -> { }), new CaptureHealth());
        };
        PipelineCaptureCoordinator coordinator = new StoreBackedPipelineCaptureCoordinator(
                store, starter, new SrsCoordinator(store.meta()), new SnapshotBuffer());
        LifecycleActuator actuator = TestEngineLifecycleActuators.create(
                new Engine(member), new IdleDagSource(), coordinator, teardown());

        actuator.start(PIPE);
        Job held = member.getJet().getJob(PIPE);
        awaitStatus(held, JobStatus.RUNNING);
        actuator.pause(PIPE);
        awaitStatus(held, JobStatus.SUSPENDED);

        actuator.resume(PIPE);

        assertThat(captures[0])
                .as("the resumed vertex reads the hand-off, and only a second capture run refills it")
                .isEqualTo(2);
    }

    /**
     * The state teardown these cases run against: the stand-in topology keeps no state, so what this drops
     * is nothing. It is here because the actuator will not be built without one, which is the point.
     */
    /** A member that holds its cluster's pipelines, so a start of one is granted a run to be fenced to. */
    private PipelineActuationOwnership clusteredOwnership() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        ClusterMembershipGate gate = new ClusterMembershipGate(properties);
        gate.install(new ClusterMembership("cluster-a", 7, Set.of("node-a", "node-b", "node-c")));
        gate.canCommit(Set.of("node-a", "node-b"));
        ClusterWorkloadClaims claims = new ClusterWorkloadClaims(new InMemoryWorkloadClaimStore(), gate);
        return new PipelineActuationOwnership(
                "cluster-a", new WorkloadOwner("node-a", "boot-a"), gate, claims,
                Duration.ofSeconds(30), Duration.ofSeconds(10), () -> 0L);
    }

    private NestStateTeardown teardown() {
        return new NestStateTeardown(member, new InMemoryKeyedStateStore(), new InMemoryNestDeadLetterStore());
    }

    private static WorkloadClaimKey standaloneKey() {
        return new WorkloadClaimKey("single", WorkloadClaimType.PIPELINE_ACTUATION, PIPE);
    }

    private static void awaitStatus(Job job, JobStatus expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        JobStatus last = null;
        while (System.nanoTime() < deadline) {
            last = job.getStatus();
            if (last == expected) {
                return;
            }
            sleep();
        }
        throw new AssertionError("job did not reach " + expected + " within budget; last status was " + last);
    }

    /** Polls for the job going terminal within a budget; true if it does, false on timeout. */
    private static boolean awaitTerminal(Job job) {
        if (job == null) {
            return false;
        }
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            if (job.getStatus().isTerminal()) {
                return true;
            }
            sleep();
        }
        return false;
    }

    private static void sleep() {
        try {
            Thread.sleep(25);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Records each capture verb into a shared event log; at stop it records whether the job was already terminal. */
    private static final class RecordingCaptureCoordinator implements PipelineCaptureCoordinator {

        private final List<String> events;
        private Supplier<Boolean> jobTerminalProbe = () -> false;
        private Throwable captureFailure;
        private boolean loadDelivered = true;
        private ArtifactStore artifactSnapshot;
        private Supplier<Boolean> jobAbsentProbe = () -> true;
        private boolean jobWasAbsentAtStart;
        private boolean givesTheStartBack;
        private boolean interruptAfterStart;
        private boolean activeCapture;
        private final List<String> captureTokens = new CopyOnWriteArrayList<>();

        RecordingCaptureCoordinator(List<String> events) {
            this.events = events;
        }

        @Override
        public void startCapture(String pipelineId) {
            jobWasAbsentAtStart = jobAbsentProbe.get();
            events.add("startCapture:" + pipelineId);
            if (givesTheStartBack) {
                throw new RingNotOpenYet(CaptureId.of(
                        new CaptureConfig("mysql", Map.of("host", "h"), List.of("orders")), null));
            }
            activeCapture = true;
            if (interruptAfterStart) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void startCapture(String pipelineId, ArtifactStore artifactSnapshot) {
            this.artifactSnapshot = artifactSnapshot;
            startCapture(pipelineId);
        }

        @Override
        public void startCapture(String pipelineId, ArtifactStore artifactSnapshot, String cursorWriterToken) {
            captureTokens.add(cursorWriterToken);
            startCapture(pipelineId, artifactSnapshot);
        }

        @Override
        public void stopCapture(String pipelineId, boolean purgeState) {
            events.add("stopCapture:" + pipelineId + (purgeState ? "[purge]" : "[keep]")
                    + (jobTerminalProbe.get() ? "[jobTerminal]" : "[jobLive]"));
            activeCapture = false;
        }

        @Override
        public boolean hasActiveCapture(String pipelineId) {
            return activeCapture;
        }

        @Override
        public Optional<Throwable> captureFailure(String pipelineId) {
            return Optional.ofNullable(captureFailure);
        }

        @Override
        public boolean loadDelivered(String pipelineId) {
            return loadDelivered;
        }
    }

    /** Records each topology request into the shared log and returns the idle stand-in topology. */
    private static final class RecordingDagSource implements DagSource {
        private final List<String> events;
        private final IdleDagSource idle = new IdleDagSource();
        private final List<ExecutionFence> fences = new CopyOnWriteArrayList<>();
        private Runnable validation = () -> {
        };
        private ArtifactStore artifactSnapshot;
        private boolean interruptDuringBuild;
        private final List<String> preparedTokens = new CopyOnWriteArrayList<>();

        RecordingDagSource(List<String> events) {
            this.events = events;
        }

        @Override
        public void validateStart(String pipelineId) {
            validation.run();
        }

        @Override
        public StartPreparation prepareStart(String pipelineId, String defaultDatabase) {
            if (artifactSnapshot == null) {
                StartPreparation prepared = DagSource.super.prepareStart(pipelineId, defaultDatabase);
                preparedTokens.add(prepared.cursorWriterToken());
                return prepared;
            }
            validateStart(pipelineId);
            StartPreparation prepared = new StartPreparation(
                    capacityOf(pipelineId), stateLocations(pipelineId, defaultDatabase),
                    Optional.of(artifactSnapshot),
                    fence -> dagFor(pipelineId, fence));
            preparedTokens.add(prepared.cursorWriterToken());
            return prepared;
        }

        /** Keeps no state, so there is nothing for a budget to be applied to. */
        @Override
        public NestCapacity capacityOf(String pipelineId) {
            return NestCapacity.none();
        }

        @Override
        public DAG dagFor(String pipelineId) {
            events.add("buildDag:" + pipelineId);
            if (interruptDuringBuild) {
                Thread.currentThread().interrupt();
            }
            return idle.dagFor(pipelineId);
        }

        /**
         * Kept out of the shared event log on purpose: the log is asserted verbatim by the cases about verb
         * ordering, and a run generation in it would make every one of those cases read as being about this.
         */
        @Override
        public DAG dagFor(String pipelineId, ExecutionFence fence) {
            fences.add(fence);
            return dagFor(pipelineId);
        }

        @Override
        public java.util.List<io.tapstate.core.lifecycle.PipelineStateHolding> stateHeldBy(
                String pipelineId) {
            return idle.stateHeldBy(pipelineId);
        }
    }
}
