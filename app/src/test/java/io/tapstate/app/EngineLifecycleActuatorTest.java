package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import io.tapstate.core.common.TapstateException;
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
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * The assembly-layer binding from the lifecycle actuator seam to the Jet engine and the capture coordinator,
 * driven against a real embedded member. It runs a pipeline through start -> pause -> resume -> stop over an
 * idle stand-in topology and proves the verb composition: start fills the capture before submitting the job
 * that reads it; pause and resume are engine-only (the capture keeps running); stop cancels the job before
 * stopping the capture behind it. The same actuator drives the store-backed topology production runs, by
 * pipeline id alone.
 */
class EngineLifecycleActuatorTest {

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
        LifecycleActuator actuator =
                new EngineLifecycleActuator(new Engine(member), dagSource, coordinator, teardown());
        // At stop the coordinator awaits the pipeline's job going terminal; that only happens if the cancel ran
        // before the capture stop, so it discriminates the stop ordering rather than racing it.
        coordinator.jobTerminalProbe = () -> awaitTerminal(member.getJet().getJob(PIPE));
        coordinator.jobAbsentProbe = () -> member.getJet().getJob(PIPE) == null;

        actuator.start(PIPE);
        // Capture opens and fills the ring before the DAG is built and submitted against that generation.
        assertThat(events).containsExactly("startCapture:" + PIPE, "buildDag:" + PIPE);
        assertThat(coordinator.jobWasAbsentAtStart).isTrue();
        Job job = member.getJet().getJob(PIPE);
        assertThat(job).as("start submits a job named by the pipeline id").isNotNull();
        awaitStatus(job, JobStatus.RUNNING);

        actuator.pause(PIPE);
        awaitStatus(job, JobStatus.SUSPENDED);
        actuator.resume(PIPE);
        awaitStatus(job, JobStatus.RUNNING);
        // Pause and resume are engine-only: the capture keeps running, so the coordinator is never touched.
        assertThat(events).containsExactly("startCapture:" + PIPE, "buildDag:" + PIPE);

        actuator.stop(PIPE, true);
        awaitStatus(job, JobStatus.FAILED); // Jet reports a cancelled job as FAILED
        // Stop cancels the job, then stops the capture behind it: the job was already terminal when capture stopped.
        assertThat(events).containsExactly(
                "startCapture:" + PIPE, "buildDag:" + PIPE,
                "stopCapture:" + PIPE + "[purge][jobTerminal]");
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
                new EngineLifecycleActuator(new Engine(member), dagSource, coordinator, teardown());

        actuator.start(PIPE);

        assertThat(events).containsExactly("startCapture:" + PIPE);
        assertThat(member.getJet().getJob(PIPE)).as("no job was submitted").isNull();
        assertThat(actuator.isCarryingAJob(PIPE)).isFalse();
    }

    /**
     * A start on a member that still has a capture open for the pipeline, with no job carrying it, closes that
     * capture before it opens one. The capture was left by a run that ended without a stop -- a job lost with a
     * member, replaced by this member before it can see that job failed -- whose sources had begun taking the
     * load it handed them. A source of the new run cannot vouch for a load another run began, so the table
     * would never land in it; closed first, and keeping the pipeline's position, the capture opens a load for
     * this run.
     */
    @Test
    void aStartOverACaptureARunLeftOpenWithNoStopClosesItFirst() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        coordinator.capturing = true;
        coordinator.jobTerminalProbe = () -> member.getJet().getJob(PIPE) == null;
        RecordingDagSource dagSource = new RecordingDagSource(events);
        LifecycleActuator actuator =
                new EngineLifecycleActuator(new Engine(member), dagSource, coordinator, teardown());

        actuator.start(PIPE);

        assertThat(events).containsExactly(
                "stopCapture:" + PIPE + "[keep][jobTerminal]", "startCapture:" + PIPE, "buildDag:" + PIPE);
        assertThat(member.getJet().getJob(PIPE)).as("the run was submitted after the capture was opened again")
                .isNotNull();
    }

    /**
     * A start works its run out before it opens the capture: a start refused there - for a width it cannot honour
     * - has opened no capture, so it has joined no mining chain and left no consumer on one to hold back the other
     * pipelines reading the same tables.
     */
    @Test
    void aStartRefusedWhileItsRunIsPlannedOpensNoCapture() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        RecordingDagSource dagSource = new RecordingDagSource(events);
        dagSource.planning = () -> {
            throw new TapstateException(ActuationError.NO_SAFE_PARALLELISM, Map.of(
                    "pipeline", PIPE, "node", "w", "requested", 1000, "members", 1,
                    "candidates", "1000 per member breaks max-local-parallelism"), null);
        };
        LifecycleActuator actuator =
                new EngineLifecycleActuator(new Engine(member), dagSource, coordinator, teardown());

        assertThatThrownBy(() -> actuator.start(PIPE))
                .isInstanceOfSatisfying(TapstateException.class,
                        refused -> assertThat(refused.code()).isEqualTo(ActuationError.NO_SAFE_PARALLELISM));

        assertThat(events).as("nothing was opened for a start refused while its run was planned").isEmpty();
        assertThat(member.getJet().getJob(PIPE)).isNull();
    }

    /**
     * A start refused while its prerequisites are checked has taken no run of its own, so the claim still names the
     * run before it. The member driving the pipeline is told, so the failure the refusal records is judged as that
     * refusal - not by the earlier run, which after a member left would read as the departure ending it again.
     */
    @Test
    void aStartRefusedBeforeItTakesARunIsJudgedAsThatRefusal() {
        RecordingDagSource dagSource = new RecordingDagSource(new CopyOnWriteArrayList<>());
        TapstateException refused = new TapstateException(ActuationError.SOURCE_TABLE_NOT_DISCOVERED,
                Map.of("source", "orders_src", "table", "orders"), null);
        dagSource.validation = () -> {
            throw refused;
        };
        PipelineActuationOwnership ownership = clusteredOwnership();
        assertThat(ownership.permit(PIPE).granted()).isTrue();
        assertThat(ownership.beginExecution(PIPE).allowed()).as("the run before this start").isTrue();
        LifecycleActuator actuator = new EngineLifecycleActuator(new Engine(member), dagSource,
                new RecordingCaptureCoordinator(new CopyOnWriteArrayList<>()), teardown(), ownership);

        assertThatThrownBy(() -> actuator.start(PIPE)).isSameAs(refused);

        assertThat(ownership.departure(PIPE, Duration.ofSeconds(90).toNanos()))
                .isEqualTo(PipelineActuationOwnership.Departure.START_REFUSED);
    }

    @Test
    void aPrepareRefusalOnAPendingAllocationIsNotMaskedAsAPreExecutionRefusal() {
        assertPendingRefusalIsReported(false);
    }

    @Test
    void aBeginRefusalOnAPendingAllocationKeepsTheAlreadyIssuedExecutionCause() {
        assertPendingRefusalIsReported(true);
    }

    private void assertPendingRefusalIsReported(boolean duringBegin) {
        PipelineActuationOwnership ownership = spy(clusteredOwnership());
        try {
            assertThat(ownership.permit(PIPE).granted()).isTrue();
            PipelineActuationOwnership.Execution pending = ownership.beginExecution(PIPE);
            assertThat(pending.allowed()).isTrue();
            TapstateException refusal = new TapstateException(io.tapstate.runtime.engine.EngineError.EXECUTION_COHORT_CHANGED_BEFORE_START,
                    Map.of("pipeline", PIPE, "reason", "live-membership", "planned", "[a, b]", "actual", "[a, b, c]"), null);
            var observedExecution = new AtomicReference<PipelineActuationOwnership.Execution>();
            var observedCause = new AtomicReference<TapstateException>();
            var unallocatedCause = new AtomicReference<TapstateException>();
            PipelineExecutionAdmission admission = new PipelineExecutionAdmission() {
                @Override public Optional<PipelineActuationOwnership.Execution> pendingExecution(String pipeline) {
                    return Optional.of(pending);
                }
                @Override public boolean prepare(String pipeline, DagSource.PlannedStart planned,
                        PipelineActuationOwnership actor) {
                    if (!duringBegin) { throw refusal; }
                    return true;
                }
                @Override public PipelineActuationOwnership.Execution begin(String pipeline, DagSource.PlannedStart planned,
                        PipelineActuationOwnership actor) {
                    throw refusal;
                }
                @Override public void failedAfterAllocation(String pipeline, PipelineActuationOwnership.Execution execution,
                        TapstateException failure) {
                    observedExecution.set(execution);
                    observedCause.set(failure);
                }
                @Override public void refused(String pipeline, TapstateException failure) {
                    unallocatedCause.set(failure);
                }
            };
            LifecycleActuator actuator = new EngineLifecycleActuator(new Engine(member),
                    new RecordingDagSource(new CopyOnWriteArrayList<>()), new RecordingCaptureCoordinator(new CopyOnWriteArrayList<>()),
                    teardown(), ownership, ExecutionPlanRecorder.NONE, java.time.Clock.systemUTC(), ConnectorReadiness.NONE, admission);

            assertThatThrownBy(() -> actuator.start(PIPE)).isSameAs(refusal);

            assertThat(observedExecution.get()).isSameAs(pending);
            assertThat(observedCause.get()).isSameAs(refusal);
            assertThat(unallocatedCause.get()).isNull();
            verify(ownership, never()).startRefusedBeforeItsRun(PIPE);
            assertThat(member.getJet().getJob(PIPE)).isNull();
        } finally {
            ownership.stopForShutdown();
        }
    }

    /** A rejected resource plan must not spend an execution generation or open a source. */
    @Test
    void aStartRefusedWhilePlanningTakesNoExecutionGeneration() {
        RecordingDagSource dagSource = new RecordingDagSource(new CopyOnWriteArrayList<>());
        dagSource.planning = () -> {
            throw new TapstateException(ActuationError.NO_SAFE_PARALLELISM, Map.of(
                    "pipeline", PIPE, "node", "w", "requested", 1000, "members", 1,
                    "candidates", "1000 per member breaks max-local-parallelism"), null);
        };
        PipelineActuationOwnership ownership = clusteredOwnership();
        assertThat(ownership.permit(PIPE).granted()).isTrue();
        LifecycleActuator actuator = new EngineLifecycleActuator(new Engine(member), dagSource,
                new RecordingCaptureCoordinator(new CopyOnWriteArrayList<>()), teardown(), ownership);

        assertThatThrownBy(() -> actuator.start(PIPE)).isInstanceOf(TapstateException.class);

        assertThat(ownership.heldExecutionGeneration(PIPE))
                .as("capacity and shape refusal happen before the execution allocator")
                .isZero();
        assertThat(member.getJet().getJob(PIPE)).isNull();
    }

    /** A start that plans its run does so before it opens the capture its topology is then built over. */
    @Test
    void aStartPlansItsRunBeforeItOpensTheCapture() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        RecordingDagSource dagSource = new RecordingDagSource(events);
        dagSource.planning = () -> events.add("planStart:" + PIPE);
        LifecycleActuator actuator =
                new EngineLifecycleActuator(new Engine(member), dagSource, coordinator, teardown());

        actuator.start(PIPE);

        assertThat(events).containsExactly("planStart:" + PIPE, "startCapture:" + PIPE, "buildDag:" + PIPE);
    }

    /**
     * A second start while a job carries the pipeline leaves its open capture alone: the capture is that job's,
     * and closing it would take the load from under a run that is reading it.
     */
    @Test
    void aStartOverACaptureARunningJobReadsLeavesItOpen() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        RecordingDagSource dagSource = new RecordingDagSource(events);
        LifecycleActuator actuator =
                new EngineLifecycleActuator(new Engine(member), dagSource, coordinator, teardown());
        actuator.start(PIPE);
        awaitStatus(member.getJet().getJob(PIPE), JobStatus.RUNNING);
        coordinator.capturing = true;

        actuator.start(PIPE);

        assertThat(events).containsExactly(
                "startCapture:" + PIPE, "buildDag:" + PIPE, "startCapture:" + PIPE, "buildDag:" + PIPE);
    }

    @Test
    void surfacesACaptureFailureThroughTheSeamWhenTheEngineJobReportsNone() {
        RuntimeException boom = new RuntimeException("cdc tail died");
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(new CopyOnWriteArrayList<>());
        coordinator.captureFailure = boom;
        LifecycleActuator actuator = new EngineLifecycleActuator(
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
        LifecycleActuator actuator = new EngineLifecycleActuator(
                new Engine(member), dagSource, coordinator, teardown());

        assertThatThrownBy(() -> actuator.start(PIPE)).isSameAs(refused);
        assertThat(events).containsExactly("validate:" + PIPE);
    }

    @Test
    void passesThePreparedArtifactSnapshotToCapture() {
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        RecordingDagSource dagSource = new RecordingDagSource(events);
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders")), null, null));
        artifacts.save(new PipelineResource(PIPE, null, List.of(SourceRef.spec("orders_src", true)), null, null,
                new ServeBlock.Inline(null, FromRef.literal("orders_src"), List.of(), null, null), null, null));
        ArtifactStore snapshot = ReadOnlyArtifactSnapshot.capture(artifacts);
        dagSource.artifactSnapshot = snapshot;
        LifecycleActuator actuator = new EngineLifecycleActuator(
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
        LifecycleActuator actuator = new EngineLifecycleActuator(
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

    /**
     * A start holds the pass from the moment it takes its run's generation until it submits the job, and it
     * can take longer than a lease: an overloaded host, a slow source, many tables. The claim behind the run
     * is renewed throughout, so the run it submits is one its own members let write. Renewed only between
     * passes, the claim ran out under the start, and the members refused the run it had just submitted.
     */
    @Test
    void aStartThatOutlastsALeaseSubmitsARunItsOwnMembersAuthorize() {
        InMemoryWorkloadClaimStore store = new InMemoryWorkloadClaimStore();
        AtomicLong nanos = new AtomicLong();
        PipelineActuationOwnership ownership = clusteredOwnership(store, nanos::get, "node-a");
        assertThat(ownership.permit(PIPE).granted()).isTrue();
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        RecordingDagSource dagSource = new RecordingDagSource(events);
        // Inside the capture start, while the pass is held: only the renewer moves, as it does on its own thread.
        coordinator.jobAbsentProbe = () -> {
            for (int second = 0; second <= 30; second += 5) {
                store.elapse(Duration.ofSeconds(5));
                nanos.addAndGet(Duration.ofSeconds(5).toNanos());
                ownership.renewDue();
            }
            return true;
        };
        LifecycleActuator actuator = new EngineLifecycleActuator(
                new Engine(member), dagSource, coordinator, teardown(), ownership);
        coordinator.jobTerminalProbe = () -> awaitTerminal(member.getJet().getJob(PIPE));

        actuator.start(PIPE);
        try (ExecutionAuthorization guard =
                     new ExecutionAuthorization("cluster-a", store, Duration.ofSeconds(10), nanos::get)) {
            assertThat(member.getJet().getJob(PIPE)).as("the run was submitted").isNotNull();
            assertThat(guard.authorized(dagSource.fences.get(0)))
                    .as("and its members may write for it, more than a lease after its generation was taken")
                    .isTrue();
        } finally {
            actuator.stop(PIPE, true);
        }
    }

    /**
     * A start whose claim was taken over while it ran submits nothing. Its members would refuse the run at its
     * first write; submitted, the run dies as it starts, and the member now holding the pipeline reads that
     * death as the pipeline's. The capture the start opened is closed again keeping the position, so the
     * holder opens its own over it.
     */
    @Test
    void aStartWhoseClaimIsTakenOverWhileItRunsSubmitsNothingAndClosesTheCaptureItOpened() {
        InMemoryWorkloadClaimStore store = new InMemoryWorkloadClaimStore();
        AtomicLong nanos = new AtomicLong();
        PipelineActuationOwnership nodeA = clusteredOwnership(store, nanos::get, "node-a");
        PipelineActuationOwnership nodeB = clusteredOwnership(store, nanos::get, "node-b");
        assertThat(nodeA.permit(PIPE).granted()).isTrue();
        List<String> events = new CopyOnWriteArrayList<>();
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(events);
        RecordingDagSource dagSource = new RecordingDagSource(events);
        // Nothing on this member renews for longer than a lease -- its store out of reach, or the process
        // paused -- and another member takes the pipeline over in the meantime.
        coordinator.jobAbsentProbe = () -> {
            store.elapse(Duration.ofSeconds(31));
            nanos.addAndGet(Duration.ofSeconds(31).toNanos());
            assertThat(nodeB.permit(PIPE).granted()).as("another member took the pipeline over").isTrue();
            return true;
        };
        LifecycleActuator actuator = new EngineLifecycleActuator(
                new Engine(member), dagSource, coordinator, teardown(), nodeA);

        actuator.start(PIPE);

        assertThat(member.getJet().getJob(PIPE))
                .as("no run was submitted over a claim this member no longer holds").isNull();
        assertThat(events)
                .as("and the capture it opened was closed again, keeping the pipeline's position")
                .containsSubsequence("startCapture:" + PIPE, "stopCapture:" + PIPE + "[keep][jobLive]");
    }

    @Test
    void reportsNoFailureWhenNeitherTheJobNorTheCaptureHasFailed() {
        RecordingCaptureCoordinator coordinator = new RecordingCaptureCoordinator(new CopyOnWriteArrayList<>());
        LifecycleActuator actuator = new EngineLifecycleActuator(
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
        LifecycleActuator actuator =
                new EngineLifecycleActuator(new Engine(member), dagSource, coordinator, teardown());
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
        LifecycleActuator actuator = new EngineLifecycleActuator(
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
        return clusteredOwnership(new InMemoryWorkloadClaimStore(), () -> 0L, "node-a");
    }

    /** The same, as one of several members over {@code store}, on the monotonic clock {@code nanos}. */
    private static PipelineActuationOwnership clusteredOwnership(
            InMemoryWorkloadClaimStore store, LongSupplier nanos, String node) {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        ClusterMembershipGate gate = new ClusterMembershipGate(properties);
        gate.install(new ClusterMembership("cluster-a", 7, Set.of("node-a", "node-b", "node-c")));
        gate.canCommit(Set.of("node-a", "node-b"));
        ClusterWorkloadClaims claims = new ClusterWorkloadClaims(store, gate);
        return new PipelineActuationOwnership(
                "cluster-a", new WorkloadOwner(node, "boot-" + node), gate, claims,
                Duration.ofSeconds(30), Duration.ofSeconds(10), nanos);
    }

    private NestStateTeardown teardown() {
        return new NestStateTeardown(member, new InMemoryKeyedStateStore(), new InMemoryNestDeadLetterStore());
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
        private boolean capturing;

        RecordingCaptureCoordinator(List<String> events) {
            this.events = events;
        }

        @Override
        public boolean isCapturing(String pipelineId) {
            return capturing;
        }

        @Override
        public void startCapture(String pipelineId) {
            jobWasAbsentAtStart = jobAbsentProbe.get();
            events.add("startCapture:" + pipelineId);
            if (givesTheStartBack) {
                throw new RingNotOpenYet(CaptureId.of(
                        new CaptureConfig("mysql", Map.of("host", "h"), List.of("orders")), null));
            }
        }

        @Override
        public void startCapture(String pipelineId, ArtifactStore artifactSnapshot) {
            this.artifactSnapshot = artifactSnapshot;
            startCapture(pipelineId);
        }

        @Override
        public void stopCapture(String pipelineId, boolean purgeState) {
            events.add("stopCapture:" + pipelineId + (purgeState ? "[purge]" : "[keep]")
                    + (jobTerminalProbe.get() ? "[jobTerminal]" : "[jobLive]"));
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
        /** Run as the start is planned, when set: what a store-backed source works out before the capture. */
        private Runnable planning;

        RecordingDagSource(List<String> events) {
            this.events = events;
        }

        @Override
        public void validateStart(String pipelineId) {
            validation.run();
        }

        @Override
        public StartPreparation prepareStart(String pipelineId, String defaultDatabase) {
            if (planning != null) {
                validateStart(pipelineId);
                return new StartPreparation(
                        capacityOf(pipelineId), stateLocations(pipelineId, defaultDatabase), Optional.empty(),
                        () -> {
                            planning.run();
                            return fence -> plannedDagFor(pipelineId, fence);
                        },
                        Map.of());
            }
            if (artifactSnapshot == null) {
                return DagSource.super.prepareStart(pipelineId, defaultDatabase);
            }
            validateStart(pipelineId);
            return new StartPreparation(
                    capacityOf(pipelineId), stateLocations(pipelineId, defaultDatabase),
                    Optional.of(artifactSnapshot),
                    fence -> plannedDagFor(pipelineId, fence));
        }

        /** Keeps no state, so there is nothing for a budget to be applied to. */
        @Override
        public NestCapacity capacityOf(String pipelineId) {
            return NestCapacity.none();
        }

        @Override
        public DAG dagFor(String pipelineId) {
            events.add("buildDag:" + pipelineId);
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
