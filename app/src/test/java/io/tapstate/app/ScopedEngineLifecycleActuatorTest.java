package io.tapstate.app;

import com.hazelcast.jet.core.DAG;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.engine.ExecutionShape;
import io.tapstate.runtime.engine.OperatorRunManifest;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.spi.store.*;
import org.junit.jupiter.api.Test;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ScopedEngineLifecycleActuatorTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster",1,"cache","p",1);
    private static final WorkloadOwner OWNER = new WorkloadOwner("node","boot");
    private final Engine engine = mock(Engine.class);
    private final Engine.Run previous = mock(Engine.Run.class), submitted = mock(Engine.Run.class);
    private final DagSource dags = mock(DagSource.class);
    private final PipelineCaptureCoordinator captures = mock(PipelineCaptureCoordinator.class);
    private final PipelineCaptureCoordinator fixedCapture = mock(PipelineCaptureCoordinator.class);
    private final PipelineActuationOwnership ownership = mock(PipelineActuationOwnership.class);
    private final AtomicBoolean active = new AtomicBoolean(true);
    private final AtomicReference<WorkloadClaimFence> held = new AtomicReference<>(claim(1));
    private final List<String> events = new ArrayList<>();
    private final ScopedEngineLifecycleActuator.Submissions submissions = new ScopedEngineLifecycleActuator.Submissions();
    private final List<OperatorRunManifest> released = new ArrayList<>();
    private final List<OperatorRunManifest> prepared = new ArrayList<>();
    private final List<PipelineConvergencePass> rebuilds = new ArrayList<>();
    private final List<ExecutionFence> forgottenPlans = new ArrayList<>();
    private final PipelineStoreViews views = mock(PipelineStoreViews.class);
    private final PipelineConvergencePass pass;
    private DagSource.StartPreparation preparation;
    private Runnable onPrepare = () -> {};
    private Runnable onRelease = () -> {};
    private Runnable onDatabasePrepare = () -> {};

    ScopedEngineLifecycleActuatorTest() {
        var intent = new PipelineIntentSnapshot(SCOPE,1,new DesiredState("p",PipelineState.RUNNING,"hash",false));
        pass = new PipelineConvergencePass(intent,held.get(),views,() -> { if (!active.get()) throw fenced(); });
        doAnswer(call -> {
            events.add("database:"+call.getArgument(1));
            assertThat((WorkloadClaimFence)call.getArgument(2)).isEqualTo(claim(2));
            assertThat(java.util.UUID.fromString(call.getArgument(3)).toString()).isEqualTo((String)call.getArgument(3));
            onDatabasePrepare.run(); ((Runnable)call.getArgument(4)).run(); return null;
        }).when(views).prepareOperatorDatabase(eq(SCOPE),anyString(),any(),anyString(),any());
        when(ownership.proveClaim(eq(SCOPE),any())).thenAnswer(call -> active.get() && held.get().equals(call.getArgument(1)));
        when(ownership.proveStartClaim(eq(SCOPE),any())).thenAnswer(call -> active.get() && held.get().equals(call.getArgument(1)));
        when(engine.captureRun("p")).thenReturn(Optional.of(previous));
        when(previous.operatorManifest()).thenReturn(manifest(1));
        when(previous.awaitTerminal(any())).thenReturn(true); when(previous.isCarryingAJob()).thenReturn(true);
        when(submitted.awaitTerminal(any())).thenReturn(true); when(submitted.isCarryingAJob()).thenReturn(true);
        when(captures.forRun(any(),any(),any())).thenAnswer(call -> { events.add("bind:"+((ExecutionFence)call.getArgument(2)).executionGeneration()); return fixedCapture; });
        doAnswer(call -> { events.add("capture"); return null; }).when(fixedCapture).startCapture(eq("p"),any(ArtifactStore.class),any(SourceModelSnapshot.class));
        when(fixedCapture.stopCaptureIfPresent(anyString(), anyBoolean())).thenCallRealMethod();
        when(fixedCapture.loadDelivered("p")).thenReturn(true);
        when(fixedCapture.captureBindings()).thenAnswer(call -> new PipelineCaptureBindings(new ExecutionFence(SCOPE,held.get()),Map.of(),false));
        when(ownership.beginExecution(eq(SCOPE),any())).thenAnswer(call -> {
            WorkloadClaimFence before = call.getArgument(1); assertThat(before).isEqualTo(held.get());
            var next = claim(before.executionGeneration()+1); held.set(next); events.add("advance");
            return new PipelineActuationOwnership.Execution(true,new ExecutionFence(SCOPE,next),7L,next);
        });
        when(engine.submitRun(eq("p"),any(DAG.class),anyMap(),any(NestSettings.class),any(OperatorRunManifest.class)))
                .thenAnswer(call -> { events.add("submit"); return submitted; });
        var pipeline = new PipelineResource("p",null,List.of(),null,null,null,null,null);
        var coordinates = OperatorCoordinatePlan.compile(SCOPE,pipeline,"cache",step -> null,Map.of(),NestSettings.defaults());
        preparation = new DagSource.StartPreparation(DagSource.NestCapacity.none(),Set.of(),Optional.of(mock(ArtifactStore.class)),
                () -> new DagSource.PlannedTopology(List.of("node"),fence -> {
                    events.add("build"); return new DagSource.PlannedDag(new DAG(),ExecutionShape.totalOne(),List.of("node"),Map.of(),Map.of());
                }),Map.of(),Optional.of(SourceModelSnapshot.capture("p",List.of(),mock(SchemaStore.class))),Optional.of(coordinates));
        when(dags.prepareStart(eq(pass),eq("cache"))).thenAnswer(call -> { events.add("prepare"); return preparation; });
        when(dags.prepareStart(any(PipelineConvergencePass.class), anyString(), any())).thenCallRealMethod();
        when(fixedCapture.prepareCaptureStart(anyString(), any(ArtifactStore.class), any(SourceModelSnapshot.class)))
                .thenCallRealMethod();
    }
    private ScopedEngineLifecycleActuator.Ports ports() {
        return new ScopedEngineLifecycleActuator.Ports((original,capacities,members,guard) -> {
            events.add("operators"); prepared.add(original); assertThat(members).containsExactly("node");
            onPrepare.run(); guard.run();
        },original -> { onRelease.run(); released.add(original); },(original,manifest) -> rebuilds.add(original),(original,command) -> new ExecutionPlanRecorder() {
            @Override public void record(io.tapstate.core.lifecycle.ExecutionPlan plan) { events.add("record"); }
            @Override public void forget(String pipeline) { assertThat(pipeline).isEqualTo(original.pipelineId()); forgottenPlans.add(original); }
            @Override public void forget(io.tapstate.core.lifecycle.ExecutionPlan expected) {
                assertThat(expected.pipelineId()).isEqualTo(original.pipelineId()); forgottenPlans.add(original);
            }
        });
    }
    private ScopedEngineLifecycleActuator actuator() {
        return new ScopedEngineLifecycleActuator(pass,engine,dags,captures,ownership,"cache",ConnectorReadiness.NONE,Clock.systemUTC(),ports(),submissions);
    }
    private EngineLifecycleActuator assembly() {
        var teardown = mock(NestStateTeardown.class); when(teardown.defaultDatabase()).thenReturn("cache");
        return new EngineLifecycleActuator(engine,dags,captures,teardown,ownership);
    }
    private static WorkloadClaimFence claim(long execution) {
        return new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"p"),OWNER,3,execution,7);
    }
    private static OperatorRunManifest manifest(long execution) { return new OperatorRunManifest("p",new ExecutionFence(SCOPE,claim(execution)),List.of()); }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); }

    @Test void anExplicitStartAfterAJoinRetiresTheOldRunBeforeAdvancingItsPassToTheNewTopology() {
        var oldGuard = pass.guardOriginal(claim(1));
        var next = new WorkloadClaimFence(claim(2).key(), OWNER, 3, 2, 8);
        when(previous.awaitTerminal(any())).thenAnswer(call -> { events.add("terminal"); return true; });
        when(ownership.beginExecution(eq(SCOPE), any())).thenAnswer(call -> {
            assertThat((WorkloadClaimFence) call.getArgument(1)).isEqualTo(claim(1));
            assertThat(events).contains("terminal");
            held.set(next);
            return new PipelineActuationOwnership.Execution(true, new ExecutionFence(SCOPE, next), 8L, next);
        });
        actuator().start("p");
        assertThat(pass.claim()).isEqualTo(next);
        assertThatThrownBy(oldGuard::run).isInstanceOf(TapstateException.class);
        assertThat(prepared).singleElement().satisfies(value ->
                assertThat(value.executionIdentity()).isEqualTo(new ExecutionFence(SCOPE, next)));
        verify(previous).cancel();
        verify(engine).submitRun(eq("p"), any(), anyMap(), any(), any(OperatorRunManifest.class));
    }

    @Test void aWaitingSharedRingKeepsTheDirectRetryOnItsOriginalDeadline() {
        var time = new java.util.concurrent.atomic.AtomicLong();
        var retry = new CaptureReadRetry(new io.tapstate.core.common.RetryWindow(time::get));
        var fact = new io.tapstate.core.common.FailureFact(new java.io.IOException("source read was interrupted"));
        var policy = new io.tapstate.core.common.RetryPolicy(true, Duration.ofSeconds(2), Duration.ofSeconds(8));
        retry.interrupted(fact);
        time.set(Duration.ofSeconds(3).toNanos());
        var root = mock(ScopedPipelineCaptureCoordinator.class);
        when(root.forRun(any(), any(), any())).thenReturn(fixedCapture);
        when(root.directRetry(new ExecutionFence(SCOPE, claim(1)))).thenReturn(Optional.of(retry));
        when(root.directRetry(new ExecutionFence(SCOPE, claim(2)))).thenReturn(Optional.of(retry));
        when(root.streamRetryPolicy()).thenReturn(policy);
        doThrow(new RingNotOpenYet(new io.tapstate.runtime.srs.CaptureId("shared-capture")))
                .when(fixedCapture).startCapture(eq("p"), any(ArtifactStore.class), any(SourceModelSnapshot.class));

        var actor = new ScopedEngineLifecycleActuator(pass, engine, dags, root, ownership, "cache",
                ConnectorReadiness.NONE, Clock.systemUTC(), ports(), submissions);
        assertThat(actor.recoverSource("p", () -> true)).contains(new LifecycleActuator.SourceRecovery(null));
        assertThat(retry.decision(policy)).isEqualTo(new CaptureReadRetry.Pending(fact, Duration.ofSeconds(2)));
        verify(root).carryDirectRetries(new ExecutionFence(SCOPE, claim(1)), new ExecutionFence(SCOPE, claim(2)));
        verify(root, never()).clearDirectRetries(SCOPE);
        verify(engine, never()).submitRun(anyString(), any(DAG.class), anyMap(), any(NestSettings.class),
                any(OperatorRunManifest.class));

        time.set(Duration.ofSeconds(8).toNanos());
        assertThat(retry.decision(policy)).isEqualTo(new CaptureReadRetry.Pending(fact, null));
        assertThat(actor.recoverSource("p", () -> true)).contains(new LifecycleActuator.SourceRecovery(fact));
        verify(root).clearDirectRetries(SCOPE);
    }

    @Test void aCancelledPredecessorKeepsTheAttemptedExecutionsRetryAndPartialCapture() {
        var time = new java.util.concurrent.atomic.AtomicLong();
        var retry = new CaptureReadRetry(new io.tapstate.core.common.RetryWindow(time::get));
        var policy = new io.tapstate.core.common.RetryPolicy(true, Duration.ofSeconds(2), Duration.ofSeconds(8));
        var failedRead = new io.tapstate.core.common.FailureFact(new java.io.IOException("source interrupted"));
        retry.interrupted(failedRead);
        time.set(Duration.ofSeconds(3).toNanos());
        var unavailable = new TapstateException(EngineError.JOB_FAILED,
                Map.of("pipeline", "p", "cause", "replacement reader still unavailable"), null);
        var replacementFailure = new io.tapstate.core.common.FailureFact(unavailable);
        retry.interrupted(replacementFailure);
        held.set(claim(2));
        when(previous.status()).thenReturn(com.hazelcast.jet.core.JobStatus.FAILED);
        when(previous.isCarryingAJob()).thenReturn(false);
        when(previous.failure()).thenReturn(Optional.empty());
        var root = mock(ScopedPipelineCaptureCoordinator.class);
        var partialCapture = mock(PipelineCaptureCoordinator.class);
        when(partialCapture.stopCaptureIfPresent("p", false)).thenCallRealMethod();
        var attempted = new ExecutionFence(SCOPE, claim(2));
        var episodes = new java.util.concurrent.ConcurrentHashMap<ExecutionFence, CaptureReadRetry>();
        episodes.put(attempted, retry);
        when(root.forRun(any(), any(), any())).thenAnswer(call ->
                attempted.equals(call.getArgument(2)) ? partialCapture : fixedCapture);
        when(root.directRetry(any())).thenAnswer(call -> {
            var episode = episodes.get(call.getArgument(0));
            return episode != null && episode.waiting() ? Optional.of(episode) : Optional.empty();
        });
        when(root.hasDirectRetryFor(any(), any())).thenAnswer(call -> {
            var episode = episodes.get(call.getArgument(0));
            return episode != null && episode.waitingFor(call.getArgument(1));
        });
        when(root.streamRetryPolicy()).thenReturn(policy);
        doAnswer(call -> {
            var episode = episodes.remove(call.getArgument(0));
            if (episode != null) episodes.put(call.getArgument(1), episode);
            return null;
        }).when(root).carryDirectRetries(any(), any());
        doAnswer(call -> { episodes.clear(); return null; }).when(root).clearDirectRetries(SCOPE);
        doAnswer(call -> {
            retry.interrupted(replacementFailure);
            throw unavailable;
        }).when(fixedCapture).startCapture(eq("p"), any(ArtifactStore.class), any(SourceModelSnapshot.class));
        var nextPass = new PipelineConvergencePass(pass.intent(), held.get(), views, () -> {});
        when(dags.prepareStart(eq(nextPass), eq("cache"))).thenReturn(preparation);
        var actor = new ScopedEngineLifecycleActuator(nextPass, engine, dags, root, ownership, "cache",
                ConnectorReadiness.NONE, Clock.systemUTC(), ports(), submissions);
        verify(root).forRun(SCOPE, claim(2), attempted);
        var state = mock(StateStore.class);
        when(state.read("p")).thenReturn(Optional.of(new io.tapstate.core.lifecycle.CheckpointDoc("p",
                io.tapstate.core.lifecycle.StateJson.of(PipelineState.RUNNING), 1,
                java.time.Instant.parse("2026-11-24T00:00:00Z"))));
        var converger = new io.tapstate.runtime.scheduler.PipelineConverger(mock(DesiredStore.class), state,
                actor, Clock.systemUTC());

        time.set(Duration.ofSeconds(4).toNanos());
        var result = converger.convergeScoped(nextPass.intent(), state, actor,
                io.tapstate.runtime.scheduler.RebuildAdmission.never(), ignored -> {}, () -> true);
        assertThat(result.status()).isEqualTo(io.tapstate.runtime.scheduler.ConvergeStatus.RECOVERING);
        assertThat(held.get()).isEqualTo(claim(2));
        assertThat(retry.decision(policy)).isEqualTo(new CaptureReadRetry.Pending(replacementFailure, Duration.ofSeconds(1)));
        verify(ownership, never()).beginExecution(eq(SCOPE), any());

        time.set(Duration.ofSeconds(5).toNanos());
        assertThat(actor.recoverSource("p", () -> true)).contains(new LifecycleActuator.SourceRecovery(null));
        var order = inOrder(partialCapture, ownership);
        order.verify(partialCapture).stopCapture("p", false);
        order.verify(ownership).beginExecution(SCOPE, claim(2));
        verify(root).carryDirectRetries(attempted, new ExecutionFence(SCOPE, claim(3)));
        assertThat(episodes.get(new ExecutionFence(SCOPE, claim(3)))).isSameAs(retry);
        verify(previous, never()).cancel();
        verify(root, never()).clearDirectRetries(SCOPE);

        time.set(Duration.ofSeconds(8).toNanos());
        assertThat(actor.recoverSource("p", () -> true)).contains(new LifecycleActuator.SourceRecovery(replacementFailure));
        assertThat(episodes).isEmpty();
    }

    @Test void aPendingRetryCannotDiscardALivePredecessorBeforeRetirement() {
        held.set(claim(2));
        when(previous.status()).thenReturn(com.hazelcast.jet.core.JobStatus.RUNNING);
        var root = mock(ScopedPipelineCaptureCoordinator.class);
        when(root.forRun(any(), any(), any())).thenReturn(fixedCapture);
        when(root.directRetry(new ExecutionFence(SCOPE, claim(2)))).thenReturn(Optional.of(new CaptureReadRetry()));
        var nextPass = new PipelineConvergencePass(pass.intent(), held.get(), views, () -> {});
        var actor = new ScopedEngineLifecycleActuator(nextPass, engine, dags, root, ownership, "cache",
                ConnectorReadiness.NONE, Clock.systemUTC(), ports(), submissions);
        verify(root).forRun(SCOPE, claim(2), new ExecutionFence(SCOPE, claim(1)));
        actor.stop("p", false);
        verify(previous).cancel();
        verify(previous).awaitTerminal(any());
    }

    @Test void aCurrentTerminalJobKeepsItsOwnFailureWhileARetryIsPending() {
        when(previous.status()).thenReturn(com.hazelcast.jet.core.JobStatus.FAILED);
        var root = mock(ScopedPipelineCaptureCoordinator.class);
        when(root.forRun(any(), any(), any())).thenReturn(fixedCapture);
        var failed = new io.tapstate.core.common.FailureFact(new TapstateException(EngineError.JOB_FAILED,
                Map.of("pipeline", "p", "cause", "current job failed"), null));
        when(previous.failureFact()).thenReturn(Optional.of(failed));
        var retry = new CaptureReadRetry();
        retry.interrupted(failed);
        when(root.directRetry(new ExecutionFence(SCOPE, claim(1)))).thenReturn(Optional.of(retry));
        var actor = new ScopedEngineLifecycleActuator(pass, engine, dags, root, ownership, "cache",
                ConnectorReadiness.NONE, Clock.systemUTC(), ports(), submissions);
        assertThat(actor.failureFact("p")).containsSame(failed);
        verify(root).forRun(SCOPE, claim(1), new ExecutionFence(SCOPE, claim(1)));
    }

    @Test void aSharedCaptureWaitReleasesItsUnsubmittedHotBindingsAcrossPasses() {
        var placement = new OperatorPlacement(SCOPE, "cache", OperatorPlacement.Resource.NEST_STATE,
                "nest", "[]", "state");
        var capacity = new io.tapstate.runtime.engine.OperatorMapCapacity(placement, 512);
        var coordinates = mock(OperatorCoordinatePlan.class);
        when(coordinates.bind(any())).thenAnswer(call -> {
            ExecutionFence fence = call.getArgument(0);
            return new OperatorRunManifest("p", fence, List.of(new io.tapstate.runtime.engine.OperatorMapBinding(
                    placement, fence, fence.operatorExecutionKey())));
        });
        when(coordinates.capacities()).thenReturn(List.of(capacity));
        when(coordinates.nestDatabases()).thenReturn(Map.of());
        preparation = new DagSource.StartPreparation(preparation.capacity(), preparation.stateLocations(),
                preparation.artifactSnapshot(), preparation.planner(), preparation.sinkConnectors(), preparation.sourceModels(),
                Optional.of(coordinates));
        var member = mock(com.hazelcast.core.HazelcastInstance.class);
        when(member.getUserContext()).thenReturn(new java.util.concurrent.ConcurrentHashMap<>());
        when(member.getDistributedObjects()).thenReturn(List.of());
        when(member.getConfig()).thenReturn(new com.hazelcast.config.Config());
        var registry = io.tapstate.runtime.engine.OperatorMapBindings.bindTo(member,
                (binding, active) -> mock(KeyedStateStore.class));
        var installed = new ArrayList<OperatorRunManifest>();
        var releasedHot = new ArrayList<OperatorRunManifest>();
        var originalPorts = ports();
        var recording = new ScopedEngineLifecycleActuator.Ports((original, capacities, members, guard) -> {
            guard.run();
            io.tapstate.runtime.engine.OperatorMapConfigurations.install(member, original, capacities);
            installed.add(original);
        }, original -> {
            original.bindings().forEach(registry::release);
            releasedHot.add(original);
        }, originalPorts.rebuild(), originalPorts.plans());
        doThrow(new RingNotOpenYet(new io.tapstate.runtime.srs.CaptureId("shared-owner-retrying")))
                .when(fixedCapture).startCapture(eq("p"), any(ArtifactStore.class), any(SourceModelSnapshot.class));
        new ScopedEngineLifecycleActuator(pass, engine, dags, captures, ownership, "cache",
                ConnectorReadiness.NONE, Clock.systemUTC(), recording, submissions).start("p");
        assertThat(registry.originals(SCOPE)).isEmpty();
        assertThat(releasedHot).contains(installed.getFirst());
        assertThat(installed.getFirst().executionIdentity()).isEqualTo(new ExecutionFence(SCOPE, claim(2)));

        when(engine.captureRun("p")).thenReturn(Optional.empty());
        var restartPass = new PipelineConvergencePass(pass.intent(), held.get(), views, () -> {});
        when(dags.prepareStart(eq(restartPass), eq("cache"))).thenReturn(preparation);
        new ScopedEngineLifecycleActuator(restartPass, engine, dags, captures, ownership, "cache",
                ConnectorReadiness.NONE, Clock.systemUTC(), recording, submissions).start("p");
        var stopIntent = new PipelineIntentSnapshot(SCOPE, 2,
                new DesiredState("p", PipelineState.STOPPED, "hash", false));
        var stopPass = new PipelineConvergencePass(stopIntent, held.get(), views, () -> {});
        new ScopedEngineLifecycleActuator(stopPass, engine, dags, captures, ownership, "cache",
                ConnectorReadiness.NONE, Clock.systemUTC(), recording, submissions).stop("p", false);
        verify(dags).retireCoveredKeyMoves(stopPass);
        assertThat(registry.originals(SCOPE)).isEmpty();
        assertThat(releasedHot).containsAll(installed);
        assertThat(installed).hasSize(2);
        verify(engine, never()).submitRun(anyString(), any(DAG.class), anyMap(), any(), any(OperatorRunManifest.class));
    }

    @Test void anUnreleasedWaitingManifestRemainsAvailableToFailedStartCleanup() {
        doThrow(new RingNotOpenYet(new io.tapstate.runtime.srs.CaptureId("shared-owner-retrying")))
                .when(fixedCapture).startCapture(eq("p"), any(ArtifactStore.class), any(SourceModelSnapshot.class));
        var attempts = new java.util.concurrent.atomic.AtomicInteger();
        var unavailable = new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "hot release interrupted"), null);
        onRelease = () -> {
            if (!prepared.isEmpty() && attempts.incrementAndGet() == 1) throw unavailable;
        };
        assertThatThrownBy(() -> actuator().start("p")).isSameAs(unavailable);
        assertThat(attempts).hasValue(2);
        assertThat(released).containsExactly(manifest(1), manifest(2));
    }

    @Test void aRefusedRecoveryBeforeReplacementSurfacesTheStartFailure() {
        var time = new java.util.concurrent.atomic.AtomicLong();
        var retry = new CaptureReadRetry(new io.tapstate.core.common.RetryWindow(time::get));
        var otherSource = new CaptureReadRetry(new io.tapstate.core.common.RetryWindow(time::get));
        retry.interrupted(new io.tapstate.core.common.FailureFact(new java.io.IOException("source interrupted")));
        otherSource.interrupted(new io.tapstate.core.common.FailureFact(new java.io.IOException("another source interrupted")));
        time.set(Duration.ofSeconds(3).toNanos());
        var root = mock(ScopedPipelineCaptureCoordinator.class);
        when(root.forRun(any(), any(), any())).thenReturn(fixedCapture);
        when(root.directRetry(new ExecutionFence(SCOPE, claim(1))))
                .thenAnswer(call -> Optional.of(retry.waiting() ? retry : otherSource));
        when(root.streamRetryPolicy()).thenReturn(new io.tapstate.core.common.RetryPolicy(
                true, Duration.ofSeconds(2), Duration.ofSeconds(8)));
        var refused = new TapstateException(EngineError.JOB_FAILED,
                Map.of("pipeline", "p", "cause", "recovery preparation refused"), null);
        when(dags.prepareStart(eq(pass), eq("cache"))).thenThrow(refused);
        var actor = new ScopedEngineLifecycleActuator(pass, engine, dags, root, ownership, "cache",
                ConnectorReadiness.NONE, Clock.systemUTC(), ports(), submissions);

        assertThat(actor.recoverSource("p", () -> true)).hasValueSatisfying(result ->
                assertThat(result.failure()).isNotNull().satisfies(fact -> assertThat(fact.cause()).isSameAs(refused)));
        verify(root).clearDirectRetries(SCOPE);
        verify(ownership, never()).beginExecution(eq(SCOPE), any());
    }

    @Test void aMatchingReplacementFailureKeepsItsPendingRetry() {
        var time = new java.util.concurrent.atomic.AtomicLong();
        var retry = new CaptureReadRetry(new io.tapstate.core.common.RetryWindow(time::get));
        var policy = new io.tapstate.core.common.RetryPolicy(true, Duration.ofSeconds(2), Duration.ofSeconds(8));
        retry.interrupted(new io.tapstate.core.common.FailureFact(new java.io.IOException("source interrupted")));
        time.set(Duration.ofSeconds(3).toNanos());
        var root = mock(ScopedPipelineCaptureCoordinator.class);
        when(root.forRun(any(), any(), any())).thenReturn(fixedCapture);
        when(root.directRetry(new ExecutionFence(SCOPE, claim(1)))).thenReturn(Optional.of(retry));
        when(root.streamRetryPolicy()).thenReturn(policy);
        var failure = new TapstateException(EngineError.JOB_FAILED,
                Map.of("pipeline", "p", "cause", "source still unavailable"), null);
        var fact = new io.tapstate.core.common.FailureFact(failure);
        doAnswer(call -> {
            retry.interrupted(fact);
            throw failure;
        }).when(fixedCapture).startCapture(eq("p"), any(ArtifactStore.class), any(SourceModelSnapshot.class));
        when(root.hasDirectRetryFor(eq(new ExecutionFence(SCOPE, claim(2))), any()))
                .thenAnswer(call -> retry.waitingFor(call.getArgument(1)));
        var actor = new ScopedEngineLifecycleActuator(pass, engine, dags, root, ownership, "cache",
                ConnectorReadiness.NONE, Clock.systemUTC(), ports(), submissions);

        assertThat(actor.recoverSource("p", () -> true)).contains(new LifecycleActuator.SourceRecovery(null));
        assertThat(retry.decision(policy)).isEqualTo(new CaptureReadRetry.Pending(fact, Duration.ofSeconds(2)));
        assertThat(retry.waitingFor(new TapstateException(EngineError.JOB_FAILED,
                Map.of("pipeline", "p", "cause", "source still unavailable"), null))).isFalse();
        verify(root, never()).clearDirectRetries(SCOPE);
    }

    @Test void failedSubmissionKeepsItsDiagnosticReadableAfterInvalidatingActuation() {
        var failure = new TapstateException(EngineError.JOB_FAILED,
                Map.of("pipeline", "p", "cause", "submission refused"), null);
        var fact = io.tapstate.core.common.FailureFact.unavailable(failure);
        when(fixedCapture.openingFailureFact("p", failure)).thenReturn(fact);
        when(engine.submitRun(eq("p"), any(DAG.class), anyMap(), any(NestSettings.class), any(OperatorRunManifest.class)))
                .thenThrow(failure);
        var actor = actuator();
        assertThatThrownBy(() -> actor.start("p")).isSameAs(failure);
        assertThat(actor.actuationFailureFact("p", failure)).isSameAs(fact);
        assertThatThrownBy(() -> actor.actuationFailureFact("other", failure)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> actor.stop("p", false)).isInstanceOf(TapstateException.class);
    }

    @Test void targetAcknowledgementAloneCannotCompleteAStillRunningOriginalJob() {
        boundedCapture();
        assertThat(actuator().completed("p")).isFalse();
        verify(previous,never()).cancel();
        verify(fixedCapture,never()).stopCapture(anyString(),anyBoolean());
    }

    @Test void aSuccessfulBoundedJobStillCarriesThePipelineWhileItsReceiptsArrive() {
        boundedCapture();
        var actor = actuator();
        assertThat(actor.completion("p")).isEmpty();

        when(previous.completed()).thenReturn(true);
        when(previous.isCarryingAJob()).thenReturn(false);
        when(fixedCapture.loadDelivered("p")).thenReturn(false);
        assertThat(actor.isCarryingAJob("p")).isTrue();
        assertThat(actor.completion("p")).isEmpty();

        when(fixedCapture.loadDelivered("p")).thenReturn(true);
        assertThat(actor.completion("p")).contains(new LifecycleActuator.Completion("71", claim(1)));
    }

    @Test void aTerminalJobWithNoBoundedCaptureDoesNotCarryThePipeline() {
        when(previous.completed()).thenReturn(true);
        when(previous.isCarryingAJob()).thenReturn(false);
        when(fixedCapture.isCapturing("p")).thenReturn(true);

        assertThat(actuator().isCarryingAJob("p")).isFalse();
    }

    private void boundedCapture() {
        when(fixedCapture.isCapturing("p")).thenReturn(true);
        when(fixedCapture.captureBindings()).thenReturn(new PipelineCaptureBindings(new ExecutionFence(SCOPE,claim(1)),
                Map.of(),Map.of(),Map.of("source",new PipelineCaptureBindings.SnapshotOnlySource("snapshot",Set.of("orders"),1)),false));
        when(previous.jobId()).thenReturn(71L);
    }

    @Test void completionCarriesTheOriginalJobAndClaimWithoutCancellingAReplacement() {
        boundedCapture(); when(previous.completed()).thenReturn(true); when(previous.isCarryingAJob()).thenReturn(false);
        var actor=actuator(); var proof=actor.completion("p").orElseThrow();
        assertThat(proof).isEqualTo(new LifecycleActuator.Completion("71",claim(1)));
        var replacement=mock(Engine.Run.class); when(engine.captureRun("p")).thenReturn(Optional.of(replacement));
        actor.complete("p",proof);
        verify(fixedCapture).stopCapture("p",false); assertThat(released).containsExactly(manifest(1));
        assertThat(forgottenPlans).containsExactly(new ExecutionFence(SCOPE,claim(1)));
        verify(engine,times(1)).captureRun("p"); verify(replacement,never()).cancel();
        verify(previous,never()).cancel();
    }

    @Test void completedCleanupCanRetryAfterItsCaptureAndAckEvidenceHaveBeenReleased() {
        boundedCapture(); when(previous.completed()).thenReturn(true); when(previous.isCarryingAJob()).thenReturn(false);
        var actor=actuator(); var proof=actor.completion("p").orElseThrow();
        doAnswer(call -> {
            when(fixedCapture.isCapturing("p")).thenReturn(false);
            doThrow(new AssertionError("released capture must not be required again")).when(fixedCapture).captureBindings();
            return null;
        }).when(fixedCapture).stopCapture("p",false);
        onRelease=() -> { throw fenced(); };
        assertThatThrownBy(() -> actor.complete("p",proof)).isInstanceOf(TapstateException.class);
        onRelease=() -> {};
        actor.retryCompletion("p");
        assertThat(released).containsExactly(manifest(1));
        assertThat(forgottenPlans).containsExactly(new ExecutionFence(SCOPE,claim(1)));
        verify(fixedCapture,times(2)).stopCapture("p",false); verify(previous,never()).cancel();
    }

    @Test void aCompletedCheckpointStillSettlesCaptureAndHotResourcesAfterTheJobWasCancelled() {
        when(previous.isCarryingAJob()).thenReturn(false);
        var desired=mock(io.tapstate.spi.store.DesiredStore.class);var state=mock(io.tapstate.spi.store.StateStore.class);
        when(desired.read("p")).thenReturn(Optional.of(pass.intent().desired()));
        when(state.read("p")).thenReturn(Optional.of(io.tapstate.core.lifecycle.CheckpointDoc.initial("p",
                io.tapstate.core.lifecycle.StateJson.of(io.tapstate.core.lifecycle.PipelineState.COMPLETED),java.time.Instant.now())));
        new io.tapstate.runtime.scheduler.PipelineConverger(desired,state,actuator(),Clock.systemUTC()).converge("p");
        verify(fixedCapture).stopCapture("p",false);assertThat(released).containsExactly(manifest(1));
        assertThat(forgottenPlans).containsExactly(new ExecutionFence(SCOPE,claim(1)));verify(dags,never()).prepareStart(any(PipelineConvergencePass.class),anyString());
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void startOnlyReprovesPreviousCaptureWhenItWasStopped(boolean retired) {
        when(engine.captureRun("p")).thenReturn(Optional.empty());
        doReturn(retired).when(fixedCapture).stopCaptureIfPresent("p", false);
        var localPorts = new ScopedEngineLifecycleActuator.Ports((m, c, n, g) -> {}, released::add,
                (p, m) -> {}, (f, c) -> ExecutionPlanRecorder.NONE);
        var actor = new ScopedEngineLifecycleActuator(pass, engine, dags, captures, ownership, "cache",
                ConnectorReadiness.NONE, Clock.systemUTC(), localPorts, submissions);
        var boundary = new IllegalStateException("execution boundary");
        when(ownership.beginExecution(eq(SCOPE), any())).thenThrow(boundary);
        assertThatThrownBy(() -> actor.start("p")).isSameAs(boundary);
        // One proof at Start entry and one at old-instance cleanup. A real cleanup adds a post-proof.
        verify(ownership, times(retired ? 3 : 2)).proveStartClaim(SCOPE, claim(1));
        verify(fixedCapture).stopCaptureIfPresent("p", false);
        verify(previous, never()).cancel();
        assertThat(released).isEmpty();
    }

    @Test void ordinaryStopUsesTheCapturedJobAndNeverResolvesItsReplacementOrPurgesColdState() {
        var actor = actuator(); var replacement = mock(Engine.Run.class);
        when(engine.captureRun("p")).thenReturn(Optional.of(replacement));
        actor.stop("p",false);
        verify(previous).cancel(); verify(previous).awaitTerminal(Duration.ofSeconds(30));
        verify(replacement,never()).cancel(); verify(engine,times(1)).captureRun("p");
        verify(engine,never()).cancel(anyString()); verify(engine,never()).awaitTerminal(anyString(),any());
        verify(fixedCapture).stopCapture("p",false); assertThat(released).containsExactly(manifest(1)); assertThat(rebuilds).isEmpty();
        assertThat(forgottenPlans).containsExactly(new ExecutionFence(SCOPE, claim(1)));
    }
    @Test void revokedAndForeignCommandsCannotEvenBindAJob() {
        active.set(false); assertThatThrownBy(this::actuator).isInstanceOf(TapstateException.class);
        verify(engine,never()).captureRun(anyString());
    }
    @Test void aLaterPhysicalGenerationCannotBeCapturedByAnEarlierPass() {
        var future = new PipelineIntentScope("cluster",1,"cache","p",2);
        when(previous.operatorManifest()).thenReturn(new OperatorRunManifest("p",new ExecutionFence(future,claim(1)),List.of()));
        assertThatThrownBy(this::actuator).isInstanceOf(TapstateException.class); verify(previous,never()).cancel();
        verify(captures,never()).forRun(any(),any(),any());
    }
    @Test void aJobFromALaterTopologyCannotBeCancelledByAnEarlierCommand() {
        var future = new WorkloadClaimFence(claim(1).key(),OWNER,3,1,8);
        when(previous.operatorManifest()).thenReturn(new OperatorRunManifest("p",new ExecutionFence(SCOPE,future),List.of()));
        assertThatThrownBy(this::actuator).isInstanceOf(TapstateException.class);
        verify(previous,never()).cancel(); verify(captures,never()).forRun(any(),any(),any());
    }
    @Test void aPurgeCommandCannotBeReusedEvenWhenItsCallbackHasNotChangedTheCacheClaim() {
        var actor = actuator(); assertThatThrownBy(() -> actor.stop("p",true)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> actor.start("p")).isInstanceOf(TapstateException.class);
        verify(ownership,never()).beginExecution(eq(SCOPE),any()); verify(dags,never()).prepareStart(eq(pass),anyString());
    }
    @Test void anUnknownSubmissionKeepsItsOwnCaptureAndHotBindings() {
        when(engine.submitRun(eq("p"),any(),anyMap(),any(),any(OperatorRunManifest.class))).thenThrow(fenced());
        assertThatThrownBy(() -> actuator().start("p")).isInstanceOf(TapstateException.class);
        verify(fixedCapture,times(1)).stopCapture("p",false);
        assertThat(released).containsExactly(manifest(1));
    }
    @Test void anUnknownSubmissionCannotBeRetriedThroughTheSameActor() {
        when(engine.submitRun(eq("p"),any(),anyMap(),any(),any(OperatorRunManifest.class))).thenThrow(fenced());
        var actor = actuator(); assertThatThrownBy(() -> actor.start("p")).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> actor.start("p")).isInstanceOf(TapstateException.class);
        verify(ownership,times(1)).beginExecution(eq(SCOPE),any());
    }
    @Test void anUnknownSubmissionCannotBecomeNoWriterInTheNextPassOfTheSameAssembly() {
        var assembly = assembly(); var actor = assembly.forPass(pass,ports());
        when(engine.submitRun(eq("p"),any(),anyMap(),any(),any(OperatorRunManifest.class))).thenThrow(fenced());
        assertThatThrownBy(() -> actor.start("p")).isInstanceOf(TapstateException.class);
        when(engine.captureRun("p")).thenReturn(Optional.empty());
        var next = new PipelineConvergencePass(pass.intent(),held.get(),mock(PipelineStoreViews.class),() -> {});
        assertThatThrownBy(() -> assembly.forPass(next,ports())).isInstanceOf(TapstateException.class);
        assertThat(released).containsExactly(manifest(1));
    }
    @Test void aLaterPassCanResolveAnUnknownSubmissionOnlyByObservingItsExactOriginalManifest() {
        var assembly = assembly(); var actor = assembly.forPass(pass,ports());
        when(engine.submitRun(eq("p"),any(),anyMap(),any(),any(OperatorRunManifest.class))).thenThrow(fenced());
        assertThatThrownBy(() -> actor.start("p")).isInstanceOf(TapstateException.class);
        when(submitted.operatorManifest()).thenReturn(manifest(2)); when(engine.captureRun("p")).thenReturn(Optional.of(submitted));
        var next = new PipelineConvergencePass(pass.intent(),held.get(),mock(PipelineStoreViews.class),() -> {});
        var resolved = assembly.forPass(next,ports()); assertThat(resolved.isCarryingAJob("p")).isTrue();
        resolved.stop("p",false); verify(submitted).cancel(); assertThat(released).containsExactly(manifest(1),manifest(2));
        verify(captures,times(1)).runOnlyWhileDriving(any());
    }
    @Test void aDifferentJobCannotSettleAnUnknownSubmissionEvenIfThePipelineIdMatches() {
        var assembly = assembly(); var actor = assembly.forPass(pass,ports());
        when(engine.submitRun(eq("p"),any(),anyMap(),any(),any(OperatorRunManifest.class))).thenThrow(fenced());
        assertThatThrownBy(() -> actor.start("p")).isInstanceOf(TapstateException.class);
        when(submitted.operatorManifest()).thenReturn(new OperatorRunManifest("p",new ExecutionFence(SCOPE,claim(1)),List.of()));
        when(engine.captureRun("p")).thenReturn(Optional.of(submitted));
        var next = new PipelineConvergencePass(pass.intent(),held.get(),mock(PipelineStoreViews.class),() -> {});
        assertThatThrownBy(() -> assembly.forPass(next,ports())).isInstanceOf(TapstateException.class); verify(submitted,never()).cancel();
    }
    @Test void controllingAnOlderCaptureKeepsTheNewCommandsOwnPhysicalScopeExplicit() {
        var nextScope = new PipelineIntentScope("cluster",1,"cache","p",2);
        var nextPass = new PipelineConvergencePass(new PipelineIntentSnapshot(nextScope,2,pass.intent().desired()),claim(1),
                mock(PipelineStoreViews.class),() -> {});
        when(ownership.proveClaim(nextScope,claim(1))).thenReturn(true);
        new ScopedEngineLifecycleActuator(nextPass,engine,dags,captures,ownership,"cache",ConnectorReadiness.NONE,Clock.systemUTC(),ports(),submissions);
        verify(captures).forRun(nextScope,claim(1),new ExecutionFence(SCOPE,claim(1)));
        verify(captures,never()).forRun(eq(SCOPE),any(),any());
    }
    @Test void aNewerExecutionDoesNotInheritThePreviousCapturesRefusal() {
        held.set(claim(2));
        var next = new PipelineConvergencePass(pass.intent(), claim(2), views, () -> {});
        var oldFailure = new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED, Map.of("pipeline", "p"), null);
        when(fixedCapture.captureFailure("p")).thenReturn(Optional.of(oldFailure));
        var actor = new ScopedEngineLifecycleActuator(next, engine, dags, captures, ownership, "cache",
                ConnectorReadiness.NONE, Clock.systemUTC(), ports(), submissions);

        assertThat(actor.failure("p"))
                .as("the current command cannot inherit a fenced predecessor's capture failure").isEmpty();
        assertThat(actor.isCarryingAJob("p")).isFalse();
        verify(captures).forRun(SCOPE, claim(2), new ExecutionFence(SCOPE, claim(1)));
        verify(previous, never()).failure();
        verify(fixedCapture, never()).captureFailure("p");
    }
    @Test void aCurrentDeferredCaptureStillReportsItsOwnFailure() {
        doThrow(new RingNotOpenYet(new io.tapstate.runtime.srs.CaptureId("capture"))).when(fixedCapture)
                .startCapture(eq("p"), any(ArtifactStore.class), any(SourceModelSnapshot.class));
        var currentFailure = new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED, Map.of("pipeline", "p"), null);
        when(fixedCapture.captureFailureFact("p")).thenReturn(Optional.of(new io.tapstate.core.common.FailureFact(currentFailure, Map.of("message", "current capture"))));
        var actor = actuator();
        actor.start("p");

        assertThat(actor.failure("p")).containsSame(currentFailure);
        assertThat(actor.failureFact("p")).hasValueSatisfying(fact ->
                assertThat(fact.diagnostic()).containsEntry("message", "current capture"));
        verify(previous, never()).failure();
        verify(engine, never()).submitRun(anyString(), any(), anyMap(), any(), any(OperatorRunManifest.class));
    }
    @Test void callbacksCannotAdoptANewerCommandOrControlAnyJobAfterIt() {
        var actor = actuator(); held.set(claim(2));
        assertThatThrownBy(() -> actor.stop("p",false)).isInstanceOf(TapstateException.class);
        verify(previous,never()).cancel(); verify(fixedCapture,never()).stopCapture(anyString(),anyBoolean()); assertThat(released).isEmpty();
    }
    @Test void stopWaitsForItsOriginalWriterBeforeReleasingCaptureAndMaps() {
        when(previous.awaitTerminal(any())).thenReturn(false); var actor = actuator();
        assertThatThrownBy(() -> actor.stop("p",false)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(EngineError.JOB_STILL_ENDING));
        verify(fixedCapture,never()).stopCapture(anyString(),anyBoolean()); assertThat(released).isEmpty();
    }
    @Test void explicitPurgeUsesTheOriginalRebuildCommandAndCannotStartAgainOnItsOldScope() {
        var actor = actuator();
        assertThatThrownBy(() -> actor.stop("p",true)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.PIPELINE_INTENT_FENCED));
        assertThat(rebuilds).containsExactly(pass); verify(engine,never()).submitRun(anyString(),any(),anyMap(),any(),any(OperatorRunManifest.class));
        verify(fixedCapture,never()).stopCapture("p",true);
    }
    @Test void startPreparesExactMembersBeforeCaptureAndBuildAndCarriesItsActualAdvancedClaim() {
        var actor = actuator(); actor.start("p");
        assertThat(events).containsSubsequence("prepare","advance","bind:2","operators","capture","build","submit");
        assertThat(pass.claim()).isEqualTo(claim(2)); assertThat(prepared).containsExactly(manifest(2));
        verify(dags,never()).prepareStart(anyString(),anyString());
        verify(ownership).beginExecution(SCOPE,claim(1)); verify(ownership,never()).beginExecution(anyString());
        verify(engine).submitRun(eq("p"),any(),anyMap(),any(),eq(manifest(2)));
    }
    @Test void aStartDoesNotRepeatRemoteProofsForLocalPreparation() {
        var actor = actuator();
        clearInvocations(ownership);

        actor.start("p");

        long proofs = mockingDetails(ownership).getInvocations().stream()
                .filter(call -> Set.of("proveClaim", "proveStartClaim").contains(call.getMethod().getName()))
                .count();
        // This fixture retires an old run, installs operators, starts capture and submits Jet.
        assertThat(proofs).isLessThanOrEqualTo(13);
        verify(engine).submitRun(eq("p"), any(), anyMap(), any(), eq(manifest(2)));
    }

    @Test void ownershipLostDuringDagBuildCannotPublishOrSubmit() {
        preparation = new DagSource.StartPreparation(preparation.capacity(), Set.of(), preparation.artifactSnapshot(),
                () -> new DagSource.PlannedTopology(List.of("node"), fence -> {
                    held.set(claim(3));
                    return new DagSource.PlannedDag(new DAG(), ExecutionShape.totalOne(), List.of("node"), Map.of(), Map.of());
                }), Map.of(), preparation.sourceModels(), preparation.operatorPlan());

        assertThatThrownBy(() -> actuator().start("p")).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        assertThat(events).doesNotContain("record", "submit");
        verify(engine, never()).submitRun(anyString(), any(), anyMap(), any(), any(OperatorRunManifest.class));
    }

    @Test void aCaptureResultFromAnotherExecutionCannotBuildOrSubmitTheNewRun() {
        when(fixedCapture.captureBindings()).thenReturn(new PipelineCaptureBindings(new ExecutionFence(SCOPE,claim(1)),Map.of(),false));
        assertThatThrownBy(() -> actuator().start("p")).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        assertThat(events).doesNotContain("build","submit");
        verify(engine,never()).submitRun(anyString(),any(),anyMap(),any(),any(OperatorRunManifest.class));
    }

    @Test void everyDistinctNestDatabaseIsPreparedAfterAdvanceAndBeforeMembersEvenForPassthroughNests() {
        withNestDatabases(); var actor = actuator(); actor.start("p");
        assertThat(events).containsSubsequence("advance","bind:2","database:extra-cache","database:second-cache","operators","capture","build","submit");
        verify(views,times(1)).prepareOperatorDatabase(eq(SCOPE),eq("extra-cache"),eq(claim(2)),anyString(),any());
        verify(views,times(1)).prepareOperatorDatabase(eq(SCOPE),eq("second-cache"),eq(claim(2)),anyString(),any());
        assertThat(prepared.getFirst().bindings()).as("passthrough Nests have shape placement but no state maps").isEmpty();
    }
    @Test void aDatabasePreparationRefusalOpensNoMemberMapsCaptureOrJob() {
        withNestDatabases(); onDatabasePrepare = () -> {throw fenced();};
        assertThatThrownBy(() -> actuator().start("p")).isInstanceOf(TapstateException.class);
        assertThat(prepared).isEmpty();
        verify(fixedCapture,never()).startCapture(anyString(),any(ArtifactStore.class),any(SourceModelSnapshot.class));
        verify(engine,never()).submitRun(anyString(),any(),anyMap(),any(),any(OperatorRunManifest.class));
    }
    private void withNestDatabases() {
        var root = new io.tapstate.core.model.NestRoot("s",List.of("id"),null,null,List.of());
        var steps = new ArrayList<io.tapstate.core.model.Step>();
        for (var entry : Map.of("one","extra-cache","two","extra-cache","three","second-cache").entrySet()) {
            var body = new io.tapstate.core.model.TransformBody.Nest(null,null,null,null,new io.tapstate.core.model.NestStateStorage(entry.getValue()),root);
            steps.add(io.tapstate.core.model.Step.inline(entry.getKey(),io.tapstate.core.model.FromClause.aliases(
                    Map.of("s",io.tapstate.core.model.FromRef.literal("source"))),body,null));
        }
        var pipeline = new PipelineResource("p",null,List.of(io.tapstate.core.model.SourceRef.bare("source")),steps,null,null,null,null);
        var plan = OperatorCoordinatePlan.compile(SCOPE,pipeline,"cache",alias -> new io.tapstate.runtime.engine.nest.NestTable("rows",List.of("id")),Map.of(),NestSettings.defaults());
        preparation = new DagSource.StartPreparation(preparation.capacity(),preparation.stateLocations(),preparation.artifactSnapshot(),
                preparation.planner(),preparation.sinkConnectors(),preparation.sourceModels(),Optional.of(plan));
    }
    @Test void firstExecutionZeroStillRequiresTheOriginalNodeAdmittedPipelineClaim() {
        held.set(claim(0));
        var zero = new PipelineConvergencePass(pass.intent(),held.get(),mock(PipelineStoreViews.class),() -> {});
        when(engine.captureRun("p")).thenReturn(Optional.empty());
        var ports = new ScopedEngineLifecycleActuator.Ports((m,c,n,g) -> {},released::add,(p,m) -> {},(f,c) -> ExecutionPlanRecorder.NONE);
        var actor = new ScopedEngineLifecycleActuator(zero,engine,dags,captures,ownership,"cache",ConnectorReadiness.NONE,Clock.systemUTC(),ports,submissions);
        assertThat(actor.isCarryingAJob("p")).isFalse(); verify(captures,never()).forRun(any(),any(),any());
    }
    @Test void operatorPreparationLosingItsOriginalClaimOpensNoCaptureOrJob() {
        onPrepare = () -> held.set(claim(3)); var actor = actuator();
        assertThatThrownBy(() -> actor.start("p")).isInstanceOf(TapstateException.class);
        verify(fixedCapture,never()).startCapture(anyString(),any(ArtifactStore.class),any(SourceModelSnapshot.class));
        verify(engine,never()).submitRun(anyString(),any(),anyMap(),any(),any(OperatorRunManifest.class));
    }
    @Test void connectorReadinessOverlapsPlanningButOldRunWaitsForBoth() {
        var checking = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var checked = new AtomicBoolean();
        ConnectorReadiness readiness = (pipelineId, ids) -> {
            assertThat(ids).containsExactly("sink");
            checking.countDown();
            await(release);
            checked.set(true);
            return Set.of();
        };
        doAnswer(call -> {
            call.<java.util.function.BiConsumer<Set<String>, Map<String, String>>>getArgument(2)
                    .accept(Set.of("sink"), Map.of());
            try {
                await(checking);
                verify(previous, never()).cancel();
                var p = preparation;
                return new DagSource.StartPreparation(p.capacity(), p.stateLocations(), p.artifactSnapshot(), p.planner(),
                        Map.of("target", "sink"), p.sourceModels(), p.operatorPlan());
            } finally {
                release.countDown();
            }
        }).when(dags).prepareStart(eq(pass), eq("cache"), any());
        when(previous.awaitTerminal(any())).thenAnswer(call -> {
            assertThat(checked.get()).isTrue();
            return true;
        });
        new ScopedEngineLifecycleActuator(pass, engine, dags, captures, ownership, "cache",
                readiness, Clock.systemUTC(), ports(), submissions).start("p");
        verify(previous).cancel();
        verify(engine).submitRun(eq("p"), any(), anyMap(), any(), any(OperatorRunManifest.class));
    }

    @Test void readOnlyCaptureInputsOverlapMapsAndCaptureOpensOnlyAfterBoth() {
        var preparingCapture = new java.util.concurrent.CountDownLatch(1);
        var mapsReady = new java.util.concurrent.CountDownLatch(1);
        doAnswer(call -> {
            preparingCapture.countDown();
            await(mapsReady);
            return (Runnable) () -> fixedCapture.startCapture("p", call.getArgument(1), call.getArgument(2));
        }).when(fixedCapture).prepareCaptureStart(eq("p"), any(ArtifactStore.class), any(SourceModelSnapshot.class));
        onPrepare = () -> {
            try {
                await(preparingCapture);
                verify(fixedCapture, never()).startCapture(anyString(), any(ArtifactStore.class), any(SourceModelSnapshot.class));
            } finally {
                mapsReady.countDown();
            }
        };
        actuator().start("p");
        verify(fixedCapture).startCapture(eq("p"), any(ArtifactStore.class), any(SourceModelSnapshot.class));
        verify(engine).submitRun(eq("p"), any(), anyMap(), any(), any(OperatorRunManifest.class));
    }

    private static void await(java.util.concurrent.CountDownLatch step) {
        try {
            assertThat(step.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }

    @Test void aTopologyWithoutAnOriginalMemberSnapshotCannotStartCapture() {
        preparation = new DagSource.StartPreparation(preparation.capacity(),Set.of(),preparation.artifactSnapshot(),
                () -> fence -> new DagSource.PlannedDag(new DAG(),ExecutionShape.totalOne(),List.of("node"),Map.of(),Map.of()),
                Map.of(),preparation.sourceModels(),preparation.operatorPlan());
        assertThatThrownBy(() -> actuator().start("p")).isInstanceOf(TapstateException.class);
        verify(fixedCapture,never()).startCapture(anyString(),any(ArtifactStore.class),any(SourceModelSnapshot.class));
    }
    @Test void aDeferredCaptureSubmitsNoJobAndDoesNotReadThePreviousJobsFailure() {
        doThrow(new RingNotOpenYet(new io.tapstate.runtime.srs.CaptureId("capture"))).when(fixedCapture)
                .startCapture(eq("p"),any(ArtifactStore.class),any(SourceModelSnapshot.class));
        var actor = actuator(); actor.start("p"); assertThat(actor.isCarryingAJob("p")).isFalse(); assertThat(actor.failure("p")).isEmpty();
        verify(previous,never()).failure(); verify(engine,never()).submitRun(anyString(),any(),anyMap(),any(),any(OperatorRunManifest.class));
    }
    @Test void aSubmissionThatLosesAuthorityIsCancelledThroughOnlyItsReturnedConcreteHandle() {
        when(engine.submitRun(eq("p"),any(),anyMap(),any(),any(OperatorRunManifest.class))).thenAnswer(call -> { held.set(claim(3)); return submitted; });
        assertThatThrownBy(() -> actuator().start("p")).isInstanceOf(TapstateException.class);
        verify(submitted).cancel(); verify(submitted).awaitTerminal(any()); verify(engine,never()).cancel(anyString());
    }
    @Test void otherPipelineIdsAreRefusedBeforeAnyVerbAndNeverLookedUp() {
        var actor = actuator(); assertThatThrownBy(() -> actor.pause("foreign")).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> actor.start("foreign")).isInstanceOf(TapstateException.class); verify(previous,never()).suspend();
        verify(engine,never()).captureRun("foreign");
    }
    @Test void unadaptedCaptureCoordinatorsCannotFallBackToTheirBareIdLifecycle() {
        PipelineCaptureCoordinator legacy = new PipelineCaptureCoordinator() {
            @Override public void startCapture(String id) { throw new AssertionError("bare start"); }
            @Override public void stopCapture(String id,boolean purge) { throw new AssertionError("bare stop"); }
        };
        assertThatThrownBy(() -> legacy.forRun(SCOPE,claim(1),new ExecutionFence(SCOPE,claim(1)))).isInstanceOf(TapstateException.class);
    }
    private ScopedEngineLifecycleActuator actuatorWithLocalCaptureLease() {
        var original = ports();
        var local = new ScopedEngineLifecycleActuator.Ports(original.operators(), original.releaseHot(), original.rebuild(),
                original.plans(), ignored -> true);
        return new ScopedEngineLifecycleActuator(pass, engine, dags, captures, ownership, "cache", ConnectorReadiness.NONE,
                Clock.systemUTC(), local, submissions);
    }

    @Test void localCapturePreparationLeaseCannotReplaceTheLaterJetSubmissionProof() {
        var checkedAfterCapture = new AtomicBoolean();
        when(ownership.proveStartClaim(eq(SCOPE), any())).thenAnswer(call -> {
            if (events.contains("capture")) checkedAfterCapture.set(true);
            return active.get() && held.get().equals(call.getArgument(1));
        });
        when(ownership.proveClaim(eq(SCOPE), any())).thenAnswer(call ->
                !events.contains("record") && active.get() && held.get().equals(call.getArgument(1)));
        assertThatThrownBy(() -> actuatorWithLocalCaptureLease().start("p")).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        assertThat(events).contains("capture", "build", "record");
        assertThat(checkedAfterCapture).isTrue();
        verify(engine, never()).submitRun(anyString(), any(DAG.class), anyMap(), any(NestSettings.class), any(OperatorRunManifest.class));
    }

    @Test void localCapturePreparationLeaseCannotReplaceTheCaptureBindingProof() {
        when(ownership.proveStartClaim(eq(SCOPE), any())).thenAnswer(call ->
                !events.contains("capture") && active.get() && held.get().equals(call.getArgument(1)));
        assertThatThrownBy(() -> actuatorWithLocalCaptureLease().start("p")).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        assertThat(events).contains("capture").doesNotContain("build", "record", "submit");
        verify(engine, never()).submitRun(anyString(), any(DAG.class), anyMap(), any(NestSettings.class), any(OperatorRunManifest.class));
    }

}
