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
        when(engine.captureRun("p")).thenReturn(Optional.of(previous));
        when(previous.operatorManifest()).thenReturn(manifest(1));
        when(previous.awaitTerminal(any())).thenReturn(true); when(previous.isCarryingAJob()).thenReturn(true);
        when(submitted.awaitTerminal(any())).thenReturn(true); when(submitted.isCarryingAJob()).thenReturn(true);
        when(captures.forRun(any(),any(),any())).thenAnswer(call -> { events.add("bind:"+((ExecutionFence)call.getArgument(2)).executionGeneration()); return fixedCapture; });
        doAnswer(call -> { events.add("capture"); return null; }).when(fixedCapture).startCapture(eq("p"),any(ArtifactStore.class),any(SourceModelSnapshot.class));
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
    }
    private ScopedEngineLifecycleActuator.Ports ports() {
        return new ScopedEngineLifecycleActuator.Ports((original,capacities,members,guard) -> {
            events.add("operators"); prepared.add(original); assertThat(members).containsExactly("node");
            onPrepare.run(); guard.run();
        },released::add,(original,manifest) -> rebuilds.add(original),(original,command) -> new ExecutionPlanRecorder() {
            @Override public void record(io.tapstate.core.lifecycle.ExecutionPlan plan) {}
            @Override public void forget(String pipeline) { assertThat(pipeline).isEqualTo(original.pipelineId()); forgottenPlans.add(original); }
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
}
