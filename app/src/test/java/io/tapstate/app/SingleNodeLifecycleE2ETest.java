package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.control.core.PipelineStatus;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.MemberOutOfMemory;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.ClusterIdentity;
import io.tapstate.spi.store.ClusterIdentityStore;
import io.tapstate.spi.store.ClusterMembershipStore;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.StorePort;
import io.tapstate.runtime.engine.nest.NestSettings;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static io.tapstate.core.lifecycle.PipelineState.FAILED;
import static io.tapstate.core.lifecycle.PipelineState.PAUSED;
import static io.tapstate.core.lifecycle.PipelineState.RUNNING;
import static io.tapstate.core.lifecycle.PipelineState.STOPPED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.awaitility.Awaitility.await;

/**
 * A single-node integration of the lifecycle chain the {@code --role=all} process runs: desired intent ->
 * converge loop -> Jet job, and converge loop -> observation -> read faces, over one embedded member and
 * explicit in-memory store fixtures. The member, node lease, ownership, authorization and driver use their
 * production factories; this does not establish real CONFIG/CACHE domain assembly. It drives a pipeline through the four verbs (and a
 * re-dig) and witnesses, at each step, that the mapped Jet operation actually took effect, that the actual
 * state converged with a strictly increasing fencing epoch, and that the store-backed read faces serve the
 * converged state. A healthy run publishes no failure metric at all here; the remaining metrics and the
 * snapshot face are honestly empty, their sources being the capture and transform planes, which merge
 * later. The artificial-failover fencing witness (epoch monotonic under a
 * competing writer, a stale write rejected) lives in the converger and core unit tests; this integration
 * witnesses the epoch advancing once per real actuated transition.
 */
class SingleNodeLifecycleE2ETest {

    private static final String PIPE = "orders-pipe";
    private static final String REV = "rev-1";
    private static final Instant T0 = Instant.parse("2026-07-13T00:00:00Z");

    private HazelcastInstance member;
    private InMemoryStorePort storePort;
    private ConvergenceDriver driver;
    private PipelineObservationQueryService readFaces;
    private InMemoryWorkloadClaimStore claims;
    private NodeSessionLease nodeLease;
    private ExecutionAuthorization authorization;
    private ActuationClaimRenewer claimRenewer;
    private PipelineActuationOwnership ownership;
    private String clusterId;
    private ClusterMembershipGate membershipGate;


    @BeforeEach
    void wireTheChain() throws Exception {
        clusterId = "single-lifecycle-" + java.util.UUID.randomUUID();
        var properties = new ClusterProperties();
        properties.setWorkloadClaimRenewInterval(Duration.ofMillis(100));
        var hz = new HazelcastProperties(); hz.getJet().setCooperativeThreadCount(2);
        var identities = mock(ClusterIdentityStore.class); when(identities.find()).thenReturn(Optional.of(new ClusterIdentity(clusterId)));
        var memberships = mock(ClusterMembershipStore.class); when(memberships.read(clusterId)).thenReturn(Optional.empty());
        claims = new InMemoryWorkloadClaimStore();
        var beans = new DefaultListableBeanFactory(); beans.registerSingleton("claims", claims);
        beans.registerSingleton("identities", identities); beans.registerSingleton("memberships", memberships);
        var gate = new ClusterMembershipGate(properties); membershipGate = gate; var wiring = new HazelcastConfiguration();
        member = wiring.hazelcastMember(hz, properties, new ControlEndpointProperties(), null, null, null, null,
                NestSettings.defaults(), null, null, null, beans.getBeanProvider(ClusterIdentityStore.class),
                beans.getBeanProvider(WorkloadClaimStore.class), gate, BootId.fresh(), beans.getBeanProvider(StorePort.class),
                beans.getBeanProvider(ClusterMembershipStore.class));
        nodeLease = wiring.nodeSessionLease(member);
        var dataPlane = new DataPlaneActuationConfiguration();
        authorization = dataPlane.executionAuthorization(member, properties, claims, nodeLease, gate);
        ownership = dataPlane.pipelineActuationOwnership(member, properties, gate, new ClusterWorkloadClaims(claims, gate));
        claimRenewer = dataPlane.actuationClaimRenewer(ownership, properties);
        await().atMost(Duration.ofSeconds(8)).until(gate::businessEligible);

        Clock clock = Clock.fixed(T0, ZoneOffset.UTC);
        storePort = new InMemoryStorePort();
        Engine engine = new Engine(member);
        // The plans a start records and a stop lets go of are kept on the member, as the server keeps them, so a
        // verb that asks the member about them where it can no longer answer is caught here.
        EngineLifecycleActuator actuator =
                new EngineLifecycleActuator(engine, new IdleDagSource(), new NoOpCaptureCoordinator(),
                        new NestStateTeardown(member, storePort.keyedState(), storePort.nestDeadLetters()),
                        ownership, new HazelcastExecutionPlans(member), clock);
        PipelineConverger converger = new PipelineConverger(storePort.desired(), storePort.state(), actuator, clock);
        ObservationPublisher publisher = new ObservationPublisher(storePort.state(), storePort.observations());
        driver = new RuntimeConvergenceConfiguration().convergenceDriver(converger, storePort, publisher, null,
                MetricsExport.none(), gate, ownership, properties, nodeLease);
        readFaces = new PipelineObservationQueryService(
                new ArtifactQueryService(storePort.artifacts()), storePort.observations());
    }

    @AfterEach
    void stopMember() {
        if (claimRenewer != null) claimRenewer.close();
        if (ownership != null) ownership.retain(List.of());
        if (authorization != null) authorization.close();
        if (nodeLease != null) nodeLease.close();
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    @DisplayName("start -> pause -> resume -> stop drives the Jet job, converges actual state with a monotonic epoch, and serves the read faces")
    void theFullLifecycleDrivesJetAndConvergesAndServesTheReadFaces() {
        desire(RUNNING);
        Job job = member.getJet().getJob(PIPE);
        assertThat(job).as("start submits the pipeline's Jet job").isNotNull();
        var claimed = claims.read(new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION, PIPE)).orElseThrow().claim();
        assertThat(claimed.executionGeneration()).isPositive();
        assertThat(authorization.require(new ExecutionFence(PIPE, claimed.claimGeneration(), claimed.executionGeneration()))).isNotNull();
        awaitStatus(job, JobStatus.RUNNING);
        assertActualState(RUNNING, 1);
        assertReadFaceReports(PIPE, RUNNING);
        // The assembled path stamps when the reading was taken. Without it a run whose publisher stopped
        // and a run whose state has not changed are the same bytes -- and nothing else in this case would
        // notice, because every assertion here holds just as well over a reading from four minutes ago.
        assertThat(readFaces.status(PIPE).observedAt())
                .as("the converge pass records when it observed the pipeline")
                .isNotNull();
        assertThat(readFaces.metrics(PIPE).metrics())
                .as("a healthy run has counted no failure, so it publishes none -- absent, not zero")
                .isEmpty();
        assertThat(readFaces.snapshot(PIPE).snapshot()).as("snapshot source is not wired yet").isEmpty();

        desire(PAUSED);
        awaitStatus(job, JobStatus.SUSPENDED);
        assertActualState(PAUSED, 2);
        assertReadFaceReports(PIPE, PAUSED);

        desire(RUNNING);
        awaitStatus(job, JobStatus.RUNNING);
        assertActualState(RUNNING, 3);
        assertReadFaceReports(PIPE, RUNNING);

        desire(STOPPED);
        awaitStatus(job, JobStatus.FAILED); // Jet reports a cancelled job as FAILED
        assertActualState(STOPPED, 4);
        assertReadFaceReports(PIPE, STOPPED);
    }

    @Test
    @DisplayName("a re-dig — stop then start — cancels the job and submits a fresh running one")
    void aReDigCancelsThenSubmitsAFreshJob() {
        desire(RUNNING);
        awaitStatus(member.getJet().getJob(PIPE), JobStatus.RUNNING);
        desire(STOPPED);
        awaitStatus(member.getJet().getJob(PIPE), JobStatus.FAILED);

        desire(RUNNING); // the re-dig: stop discarded the old job, start replays from the source

        Job fresh = member.getJet().getJob(PIPE);
        awaitStatus(fresh, JobStatus.RUNNING);
        assertActualState(RUNNING, 3); // 1 = RUNNING, 2 = STOPPED, 3 = RUNNING again
    }

    /**
     * A pipeline paused when its engine is lost for want of memory is failed with that cause, as a running
     * one is. Its job was held on the member and went with it, so nothing is left for a resume to continue;
     * it used to go on reading PAUSED while the server reported itself unhealthy, and turned FAILED only once
     * somebody resumed it.
     *
     * <p>The paused reading comes first, so that a pipeline which never got as far as paused cannot pass for
     * one failed out of it. The tick after is read too: the intent still says PAUSED, and a loop that went
     * back to holding the job would be refused and fail the pipeline again on every tick.
     */
    @Test
    @DisplayName("a pipeline paused when its engine is lost for want of memory reads FAILED with that cause, and stays there")
    void aPipelinePausedWhenItsEngineIsLostForWantOfMemoryReadsFailed() {
        MemberOutOfMemory.watch(member);
        desire(RUNNING);
        Job job = member.getJet().getJob(PIPE);
        awaitStatus(job, JobStatus.RUNNING);
        desire(PAUSED);
        awaitStatus(job, JobStatus.SUSPENDED);
        driver.reconcile(); // a tick while the engine is up and holds the job: nothing to do
        assertActualState(PAUSED, 2);
        assertReadFaceReports(PIPE, PAUSED);

        var held = claims.read(new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION, PIPE)).orElseThrow().claim();
        var originalExecution = new ExecutionFence(PIPE, held.claimGeneration(), held.executionGeneration());
        assertThat(authorization.require(originalExecution)).isNotNull();
        claimRenewer.close();
        long renewalDue = System.nanoTime() + Duration.ofMillis(100).toNanos();
        runOutOfMemoryOnAMemberThread();
        await().atMost(Duration.ofSeconds(1)).until(() -> System.nanoTime() - renewalDue >= 0);
        ownership.renewDue();
        assertThat(ownership.heldExecutionGeneration(PIPE))
                .as("the original control claim survives a renewal before the failed engine is observed")
                .isEqualTo(held.executionGeneration());
        assertThat(MemberOutOfMemory.of(member))
                .as("the member's own out-of-memory handling took it down, and the held job with it")
                .isPresent();
        assertThat(nodeLease.active()).as("the control process still has its original node proof").isTrue();
        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertThat(membershipGate.businessEligible()).as("a stopped member cannot admit business work").isFalse(),
                () -> assertThatThrownBy(() -> authorization.require(originalExecution)).isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED)));
        assertThat(new ClusterWorkloadClaims(claims, membershipGate).acquire(
                new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION, "new-pipeline"),
                held.owner(), held.topologyRevision(), Duration.ofSeconds(30))).isEmpty();
        assertThat(new ClusterWorkloadClaims(claims, membershipGate).advanceExecution(
                held, held.topologyRevision(), java.util.Set.of(held.owner().nodeId()))).isEmpty();
        driver.reconcile();

        assertThat(readFaces.status(PIPE))
                .returns(FAILED, PipelineStatus::state)
                .extracting(PipelineStatus::failure)
                .as("and the status says what took the engine down")
                .returns("engine.out-of-memory", ObservationFailure::code);
        assertActualState(FAILED, 3);

        driver.reconcile();

        assertActualState(FAILED, 3);
        assertThat(readFaces.status(PIPE).failure())
                .as("the next tick keeps the cause rather than failing the pipeline again over another")
                .returns("engine.out-of-memory", ObservationFailure::code);
    }

    /** Saves the pipeline's desired target and runs one reconcile pass, the tick the scheduled driver makes. */
    private void desire(PipelineState target) {
        storePort.desired().save(new DesiredState(PIPE, target, REV));
        driver.reconcile();
    }

    private void assertActualState(PipelineState expected, long epoch) {
        CheckpointDoc doc = storePort.state().read(PIPE).orElseThrow();
        assertThat(doc.stateJson()).isEqualTo(StateJson.of(expected));
        assertThat(doc.epoch()).as("the fencing epoch advances once per converged transition").isEqualTo(epoch);
    }

    private static void awaitStatus(Job job, JobStatus expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        JobStatus last = null;
        while (System.nanoTime() < deadline) {
            last = job.getStatus();
            if (last == expected) {
                return;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new AssertionError("job did not reach " + expected + " within budget; last status was " + last);
    }

    /**
     * Lets an out-of-memory error escape one of the member's own threads, which is how the member hears of it,
     * and returns once the member's out-of-memory handling is done with it. The error is the one the collector
     * raises when it gives up, which the handling acts on however full the heap is.
     */
    private static void runOutOfMemoryOnAMemberThread() {
        Thread memberThread = new HazelcastManagedThread(() -> {
            throw new OutOfMemoryError("GC overhead limit exceeded");
        }, "test-member-out-of-memory");
        memberThread.start();
        try {
            assertThat(memberThread.join(Duration.ofSeconds(60)))
                    .as("the member thread hands its out-of-memory error over and ends")
                    .isTrue();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while the out-of-memory handling ran", interrupted);
        }
    }

    /**
     * The state the read face reports, compared on the fields this case is about.
     *
     * <p>Not whole-record equality: the observation now carries when it was taken, so a record built here
     * to compare against would have to invent a time, and would fail on whatever time it invented. What
     * this case is about is the state the converger reached and the absence of a failure over it -- and
     * that the reading is stamped at all is asserted once, separately, where it means something.
     */
    private void assertReadFaceReports(String pipelineId, PipelineState expected) {
        assertThat(readFaces.status(pipelineId))
                .returns(pipelineId, PipelineStatus::pipelineId)
                .returns(expected, PipelineStatus::state)
                .returns(null, PipelineStatus::failure);
    }

}
