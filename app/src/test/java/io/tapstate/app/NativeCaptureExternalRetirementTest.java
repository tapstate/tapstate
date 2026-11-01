package io.tapstate.app;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.ringbuffer.Ringbuffer;
import io.tapstate.adapters.pdk.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.SourceCaptureIdentity;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;

/** Drives the scheduled reconciler deterministically, including its actual member callable. */
class NativeCaptureExternalRetirementTest {
    private static final CacheBusinessScope BUSINESS=new CacheBusinessScope("cluster",1,"cache");
    private static final PipelineIntentScope PIPELINE=new PipelineIntentScope("cluster",1,"cache","flow",1);
    private static final WorkloadClaimFence NODE=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,"node"),new WorkloadOwner("node","boot"),1,0,1);
    private static final String HASH="a".repeat(64);
    private final CaptureExternalRetirements configuration=mock(CaptureExternalRetirements.class);
    private final CaptureClearCache cache=mock(CaptureClearCache.class);
    private final RequiredNoteStore notes=mock(RequiredNoteStore.class);
    private final ConnectorProvisioner connectors=mock(ConnectorProvisioner.class);
    private final HazelcastInstance member=mock(HazelcastInstance.class,RETURNS_DEEP_STUBS);
    private final ScopedPipelineCaptureCoordinator captures=mock(ScopedPipelineCaptureCoordinator.class);
    private final ScheduledExecutorService scheduler=mock(ScheduledExecutorService.class);
    private final Runnable active=mock(Runnable.class);
    private final CaptureClearPreparation original=mock(CaptureClearPreparation.class);
    private final CaptureExternalRetirements.Attempt attempt=mock(CaptureExternalRetirements.Attempt.class);
    private final CaptureClearCache.Receipt frozen=mock(CaptureClearCache.Receipt.class);
    private final Member target=mock(Member.class);
    private final IExecutorService distributed=mock(IExecutorService.class);
    private final RequiredNoteOrigin origin=ConnectorNoteOrigin.capture("postgres",HASH,Map.of("database","original"));
    private final CapturePreparation prepared=new CapturePreparation(BUSINESS,CapturePreparation.Mode.SHARED_SRS,"chain","capture",SourceCaptureIdentity.physical("postgres",Map.of("database","original")),"00000000-0000-0000-0000-000000000001");
    private final RequiredNoteScope shared=new RequiredNoteScope(new RequiredNoteOwner(1,"pdk.chain.chain",RequiredNoteOwner.Kind.CHAIN,"chain",0),1);
    private final List<CaptureRetirementMember> roster=List.of(new CaptureRetirementMember("node","boot","00000000-0000-0000-0000-000000000002"));
    private MockedConstruction<PdkExternalResourceRetirement> sourceConstruction;
    private NativeCaptureExternalRetirement worker;
    private PdkExternalResourceRetirement source;
    private Runnable tick;
    private Optional<WorkloadClaimFence> node=Optional.of(NODE);
    @BeforeEach void setup() throws Exception {
        when(member.getUserContext()).thenReturn(new ConcurrentHashMap<>());
        when(member.getCluster().getMembers()).thenReturn(Set.of(target));
        when(target.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE)).thenReturn("node");
        when(target.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE)).thenReturn("boot");
        when(target.getUuid()).thenReturn(UUID.fromString(roster.getFirst().memberId()));
        when(member.getExecutorService("tapstate-native-capture-retirement")).thenReturn(distributed);
        when(original.operationId()).thenReturn("clear");when(original.originalScope()).thenReturn(PIPELINE);
        when(original.sequence()).thenReturn(1L);when(original.requestHash()).thenReturn(HASH);when(original.members()).thenReturn(roster);
        when(original.replacement()).thenReturn(prepared);when(original.notes()).thenReturn(shared);
        var captureFence=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,"capture"),new WorkloadOwner("node","boot"),1,1,1);
        var snapshot=new SharedSrsRecoverySnapshot(prepared,captureFence,HASH,List.of(),Map.of());
        when(original.observation()).thenReturn(snapshot);
        when(frozen.outcome()).thenReturn(CaptureClearCache.Outcome.FROZEN);
        when(cache.frozen(original)).thenReturn(Optional.of(frozen));
        when(cache.inspect(prepared)).thenReturn(() -> snapshot);
        when(cache.freeze(original,NODE)).thenReturn(frozen);
        when(configuration.pending(BUSINESS)).thenReturn(List.of());
        when(configuration.sharedCaptures(BUSINESS)).thenReturn(List.of(prepared));
        when(notes.open(shared.owner())).thenReturn(shared);when(notes.origin(shared)).thenReturn(origin);
        doAnswer(call -> {tick=call.getArgument(0);return null;}).when(scheduler).scheduleWithFixedDelay(any(Runnable.class),eq(1L),eq(1L),eq(TimeUnit.SECONDS));
        sourceConstruction=mockConstruction(PdkExternalResourceRetirement.class);
        try(var executors=mockStatic(Executors.class)) {
            executors.when(() -> Executors.newSingleThreadScheduledExecutor(any(ThreadFactory.class))).thenAnswer(call -> {
                var thread=((ThreadFactory)call.getArgument(0)).newThread(() -> {});
                assertThat(thread.isDaemon()).isTrue();assertThat(thread.getName()).isEqualTo("tapstate-external-capture-reconciliation");
                return scheduler;
            });
            worker=new NativeCaptureExternalRetirement(BUSINESS,configuration,cache,notes,connectors,() -> node,active,member,captures);
        }
        source=sourceConstruction.constructed().getFirst();
        when(source.supported(origin,shared)).thenReturn(true);when(source.declaresPolicy(origin)).thenReturn(true);when(source.automatic(origin)).thenReturn(true);
        when(distributed.submitToMembers(any(Callable.class),anyCollection())).thenAnswer(call -> {
            var task=(Callable<Boolean>)call.getArgument(0);((HazelcastInstanceAware)task).setHazelcastInstance(member);
            return Map.of(target,CompletableFuture.completedFuture(task.call()));
        });
    }
    @AfterEach void cleanup(){worker.close();sourceConstruction.close();}
    @Test void reservationFreezesFirstAndOnlyAnAttachmentRaceCanCancel() {
        pending("RESERVED");tick.run();verify(configuration).frozen(original,frozen);
        reset(configuration);pending("RESERVED");
        when(cache.freeze(original,NODE)).thenThrow(refusal("attachment-raced"));tick.run();verify(configuration).cancelling(original);
        reset(configuration);pending("RESERVED");doThrow(refusal("unavailable")).when(cache).freeze(original,NODE);tick.run();verify(configuration,never()).cancelling(any());
    }
    @Test void allMembersStopAndVerifyBeforeTheOriginalSourceAttemptCanBegin() {
        pending("FROZEN");when(configuration.beginExternal(original,NODE)).thenReturn(Optional.of(attempt));
        var answer=new CompletableFuture<PdkExternalResourceRetirement.Result>();when(source.begin(original,attempt)).thenReturn(answer);
        tick.run();
        var order=inOrder(captures,configuration,source);
        order.verify(captures).stopSharedLocally(prepared,original.observation().captureFence());
        order.verify(captures).requireSharedRetired(prepared,original.observation().captureFence());
        order.verify(configuration).beginExternal(original,NODE);order.verify(source).begin(original,attempt);
        pending("STARTED");tick.run();verify(configuration,never()).verified(any(),any());
        answer.complete(new PdkExternalResourceRetirement.Result(true,"confirmed-absent"));tick.run();verify(configuration).verified(original,attempt);
        tick.run();verify(configuration).recoveredUnknown(original);
    }
    @Test void unsuccessfulSourceAnswerAndShutdownPreserveUncertainty() {
        pending("FROZEN");when(configuration.beginExternal(original,NODE)).thenReturn(Optional.of(attempt));
        when(source.begin(original,attempt)).thenReturn(CompletableFuture.completedFuture(new PdkExternalResourceRetirement.Result(false,"original-slot-remains")));
        tick.run();pending("UNKNOWN");tick.run();verify(attempt).unknown("original-slot-remains");
        when(source.begin(original,attempt)).thenReturn(new CompletableFuture<>());tick.run();worker.close();
        verify(attempt).unknown("executor-closing");verify(scheduler,atLeastOnce()).shutdownNow();
        clearInvocations(configuration);tick.run();verifyNoInteractions(configuration);
        assertThat(member.getUserContext()).isEmpty();
    }
    @Test void aRecoveredUnknownMayRetryButAnUnownedStartedCallIsNeverAssumedEnded() {
        pending("STARTED");tick.run();verify(configuration).recoveredUnknown(original);verifyNoInteractions(source);
        pending("UNKNOWN");when(configuration.beginExternal(original,NODE)).thenReturn(Optional.empty());tick.run();verify(configuration).beginExternal(original,NODE);verifyNoInteractions(source);
    }
    @Test void verifiedUsesEveryCohortReceiptAndReadyDestroysOnlyOldCaptureRings() {
        var other=mock(CaptureClearPreparation.class);var existing=mock(CaptureClearCache.Receipt.class);var applied=mock(CaptureClearCache.Receipt.class);
        when(configuration.cohort(original)).thenReturn(List.of(original,other));
        when(cache.applied(original)).thenReturn(Optional.of(existing));when(cache.applied(other)).thenReturn(Optional.empty());when(cache.apply(other,NODE)).thenReturn(applied);
        pending("VERIFIED");tick.run();verify(configuration).complete(original,List.of(existing,applied));verify(cache,never()).apply(eq(original),any());
        pending("CANCELLING");when(cache.cancel(original,NODE)).thenReturn(applied);tick.run();verify(configuration).cancelled(original,applied);
        var oldRing=mock(Ringbuffer.class);var newRing=mock(Ringbuffer.class);var unrelated=mock(com.hazelcast.core.DistributedObject.class);
        when(oldRing.getName()).thenReturn("srs.chain.table");when(newRing.getName()).thenReturn("srs.other.table");when(unrelated.getName()).thenReturn("srs.chain.map");
        when(member.getDistributedObjects()).thenReturn(List.of(oldRing,newRing,unrelated));pending("READY");
        when(cache.reclaim(original,NODE)).thenReturn(false,true);tick.run();verify(configuration,never()).cleaned(any());
        tick.run();verify(oldRing).destroy();verify(newRing,never()).destroy();verify(unrelated,never()).destroy();verify(configuration).cleaned(original);
    }
    @ParameterizedTest
    @ValueSource(strings={"roster","missing-local","wrong-business","wrong-freeze","false-reply","failed-reply","timeout","no-node"})
    void incompleteMemberEvidenceNeverStartsExternalCleanup(String damage) throws Exception {
        pending("FROZEN");
        switch(damage) {
            case "roster" -> when(original.members()).thenReturn(List.of());
            case "missing-local" -> member.getUserContext().clear();
            case "wrong-business" -> when(original.originalScope()).thenReturn(new PipelineIntentScope("different",1,"cache","flow",1));
            case "wrong-freeze" -> when(cache.frozen(original)).thenReturn(Optional.empty());
            case "false-reply" -> when(distributed.submitToMembers(any(Callable.class),anyCollection())).thenReturn(Map.of(target,CompletableFuture.completedFuture(false)));
            case "failed-reply" -> when(distributed.submitToMembers(any(Callable.class),anyCollection())).thenReturn(Map.of(target,CompletableFuture.failedFuture(new IllegalStateException("offline"))));
            case "timeout" -> {var future=mock(Future.class);when(future.get(anyLong(),eq(TimeUnit.NANOSECONDS))).thenThrow(new TimeoutException());when(distributed.submitToMembers(any(Callable.class),anyCollection())).thenReturn(Map.of(target,future));}
            case "no-node" -> node=Optional.empty();
            default -> throw new AssertionError(damage);
        }
        tick.run();verify(configuration,never()).beginExternal(any(),any());verifyNoInteractions(source);
    }
    @Test void oneWaitingOperationDoesNotPreventAnotherReservedOperationFromFreezing() {
        var other=mock(CaptureClearPreparation.class);when(configuration.pending(BUSINESS)).thenReturn(List.of(new CaptureExternalRetirements.Pending(original,"RESERVED",true),new CaptureExternalRetirements.Pending(other,"RESERVED",true)));
        doThrow(refusal("unavailable")).when(cache).freeze(original,NODE);when(cache.freeze(other,NODE)).thenReturn(frozen);
        tick.run();verify(configuration).frozen(other,frozen);
        doThrow(refusal("fenced")).when(active).run();clearInvocations(cache);tick.run();verifyNoInteractions(cache);
    }
    @Test void relinquishmentReservesOnlyTheExactUnretainedSlotCohort() {
        var rebuild=rebuild();var own=resource(shared,origin,false);var aliasScope=new RequiredNoteScope(RequiredNoteOwner.forNode(1,new PipelineNode("flow","input"),PipelineNode.Purpose.CAPTURE,1),1);
        var alias=resource(aliasScope,origin,false);
        when(notes.privateContexts(PIPELINE,"original")).thenReturn(List.of(new RequiredNoteContext(aliasScope,Optional.of(origin),false)));
        when(configuration.resourceNotes(BUSINESS)).thenReturn(List.of(own,alias));when(source.savedSlot(own,List.of(own,alias))).thenReturn("original-slot");
        when(source.slot(alias,List.of(own,alias))).thenReturn("original-slot");when(source.slot(own,List.of(own,alias))).thenReturn("original-slot");
        worker.relinquished(rebuild,List.of(),null);
        var aliases=List.of(new CaptureAliasObservation(alias,"original-slot"));verify(configuration).relinquishAliases(rebuild,aliases);
        verify(configuration).reserve(eq(rebuild),argThat(v -> v.size()==1 && v.getFirst().snapshot().previous().equals(prepared)),eq(List.of(own,alias)),eq(CaptureResourceNotes.nameHash("original-slot")),eq(origin),eq(true),eq(roster));
        when(configuration.retainedPeer(prepared,"flow")).thenReturn(true);clearInvocations(configuration);worker.relinquished(rebuild,List.of(),null);verify(configuration,never()).reserve(any(PipelineRebuildPreparation.class),anyList(),anyList(),anyString(),any(),anyBoolean(),anyList());
    }
    @ParameterizedTest
    @ValueSource(strings={"completed","no-origin","non-capture","unsupported","missing-definition","changed-definition","no-slot","retained","context-mismatch","unrelated"})
    void aliasesAndCohortsCannotBeInferredFromIncompleteOrUnrelatedFacts(String damage) {
        var rebuild=rebuild();var scope=new RequiredNoteScope(RequiredNoteOwner.forNode(1,new PipelineNode("flow","input"),damage.equals("non-capture")?PipelineNode.Purpose.VIEW:PipelineNode.Purpose.CAPTURE,1),1);
        var own=resource(shared,origin,false);var alias=resource(scope,origin,damage.equals("retained"));var all=List.of(own,alias);
        when(configuration.resourceNotes(BUSINESS)).thenReturn(all);
        when(notes.privateContexts(PIPELINE,"original")).thenReturn(List.of(new RequiredNoteContext(scope,damage.equals("no-origin") || damage.equals("completed")?Optional.empty():Optional.of(origin),damage.equals("completed"))));
        when(source.savedSlot(own,all)).thenReturn("original-slot");when(source.slot(own,all)).thenReturn("original-slot");when(source.slot(alias,all)).thenReturn("original-slot");
        Map<String,SourceResource> desired=null;
        switch(damage) {
            case "unsupported" -> when(source.declaresPolicy(origin)).thenReturn(false);
            case "missing-definition" -> desired=Map.of();
            case "changed-definition" -> {
                var definition=new SourceResource("input",null,"postgres",Map.of("database","changed"),SourceMode.CDC,List.of(TableRef.literal("orders")),null,null);
                desired=Map.of(scope.owner().namespace(),definition);when(connectors.resolve("postgres")).thenReturn(new ConnectorRef(List.of(),"unused","2.0.8",null,null,HASH,false));
            }
            case "no-slot" -> when(source.savedSlot(own,all)).thenReturn(null);
            case "unrelated" -> when(source.slot(alias,all)).thenReturn("different");
            case "context-mismatch" -> {
                var other=resource(new RequiredNoteScope(shared.owner(),2),ConnectorNoteOrigin.capture("postgres",HASH,Map.of("database","different")),false);
                when(configuration.resourceNotes(BUSINESS)).thenReturn(List.of(own,alias,other));
                when(source.slot(alias,List.of(own,alias,other))).thenReturn("original-slot");when(source.savedSlot(own,List.of(own,alias,other))).thenReturn("original-slot");
                when(source.slot(own,List.of(own,alias,other))).thenReturn("original-slot");when(source.declaresPolicy(other.origin())).thenReturn(true);when(source.slot(other,List.of(own,alias,other))).thenReturn("original-slot");
            }
            default -> { }
        }
        var selected=desired;
        if(damage.equals("context-mismatch"))assertThatThrownBy(() -> worker.relinquished(rebuild,List.of(),selected)).isInstanceOfSatisfying(TapstateException.class,e -> assertThat(e.args()).containsEntry("reason","same-slot-context-independence-unproven"));
        else worker.relinquished(rebuild,List.of(),selected);
        verify(configuration,never()).reserve(any(PipelineRebuildPreparation.class),anyList(),anyList(),any(),any(),anyBoolean(),anyList());
    }
    @Test void deletionUsesItsOriginalReceiptAndClosedWorkersCannotAcceptNewWork() {
        var receipt=mock(PipelineRetirementReceipt.class,RETURNS_DEEP_STUBS);when(receipt.preparation().request().scope()).thenReturn(PIPELINE);when(receipt.preparation().request().operationId()).thenReturn("original");
        worker.deleted(receipt,List.of());verify(configuration).relinquishAliases(receipt.preparation(),List.of());
        worker.close();assertThatThrownBy(() -> worker.deleted(receipt,List.of())).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> worker.relinquished(rebuild(),List.of(),Map.of())).isInstanceOf(TapstateException.class);
    }
    @ParameterizedTest
    @ValueSource(strings={"unsupported","no-slot","not-selected","consumer","alias-origin","success"})
    void previousMembershipsSelectOnlySupportedUnretainedOriginalChains(String condition) {
        var own=resource(shared,origin,false);var inventory=List.of(own);
        when(configuration.resourceNotes(BUSINESS)).thenReturn(inventory);
        when(source.savedSlot(own,inventory)).thenReturn("original-slot");when(source.slot(own,inventory)).thenReturn("original-slot");
        var previous=mock(SrsConsumerMembership.class);
        when(previous.context()).thenReturn(new SrsConsumerContext(new SrsConsumerKey(condition.equals("not-selected")?"different":"chain","flow","input"),1));
        switch(condition) {
            case "unsupported" -> when(source.supported(origin,shared)).thenReturn(false);
            case "no-slot" -> when(source.savedSlot(own,inventory)).thenReturn(null);
            case "consumer" -> {
                var observed=mock(SharedSrsRecoverySnapshot.class);when(observed.consumers()).thenReturn(List.of(previous));
                when(cache.inspect(prepared)).thenReturn(() -> observed);
            }
            case "alias-origin" -> {
                var scope=new RequiredNoteScope(RequiredNoteOwner.forNode(1,new PipelineNode("flow","input"),PipelineNode.Purpose.CAPTURE,1),1);
                when(notes.privateContexts(PIPELINE,"original")).thenReturn(List.of(new RequiredNoteContext(scope,Optional.of(origin),false)));
                var definition=new SourceResource("input",null,"postgres",Map.of("database","original"),SourceMode.CDC,List.of(TableRef.literal("orders")),null,null);
                when(connectors.resolve("postgres")).thenReturn(new ConnectorRef(List.of(),"unused","2.0.8",null,null,HASH,false));
                var alias=resource(scope,origin,false);when(configuration.resourceNotes(BUSINESS)).thenReturn(List.of(own,alias));
                when(source.slot(alias,List.of(own,alias))).thenReturn(null);when(source.savedSlot(own,List.of(own,alias))).thenReturn("original-slot");
                when(source.slot(own,List.of(own,alias))).thenReturn("original-slot");
                worker.relinquished(rebuild(),List.of(previous),Map.of(scope.owner().namespace(),definition));
                verify(configuration).relinquishAliases(any(PipelineRebuildPreparation.class),eq(List.of()));return;
            }
            default -> { }
        }
        var receipt=mock(PipelineRetirementReceipt.class,RETURNS_DEEP_STUBS);when(receipt.preparation().request().scope()).thenReturn(PIPELINE);when(receipt.preparation().request().operationId()).thenReturn("original");
        worker.deleted(receipt,List.of(previous));
        if(condition.equals("success"))verify(configuration).reserve(eq(receipt),anyList(),eq(inventory),eq(CaptureResourceNotes.nameHash("original-slot")),eq(origin),eq(true),eq(roster));
        else verify(configuration,never()).reserve(any(PipelineRetirementReceipt.class),anyList(),anyList(),any(),any(),anyBoolean(),anyList());
    }
    @Test void interruptionPreservesTheThreadFlagAndNeverStartsRelease() throws Exception {
        pending("FROZEN");var future=mock(Future.class);when(future.get(anyLong(),eq(TimeUnit.NANOSECONDS))).thenThrow(new InterruptedException());
        when(distributed.submitToMembers(any(Callable.class),anyCollection())).thenReturn(Map.of(target,future));
        try {tick.run();assertThat(Thread.currentThread().isInterrupted()).isTrue();verify(configuration,never()).beginExternal(any(),any());}
        finally {Thread.interrupted();}
    }
    @Test void unavailableShutdownCannotConvertAnInFlightAttemptIntoAnEndedOne() {
        pending("FROZEN");when(configuration.beginExternal(original,NODE)).thenReturn(Optional.of(attempt));when(source.begin(original,attempt)).thenReturn(new CompletableFuture<>());
        tick.run();doThrow(refusal("fenced")).when(attempt).unknown("executor-closing");
        worker.close();verify(attempt,never()).ended();assertThat(member.getUserContext()).isEmpty();
    }

    private PipelineRebuildPreparation rebuild() {
        var rebuild=mock(PipelineRebuildPreparation.class,RETURNS_DEEP_STUBS);when(rebuild.request().originalScope()).thenReturn(PIPELINE);when(rebuild.request().operationId()).thenReturn("original");return rebuild;
    }
    private CaptureResourceNotes resource(RequiredNoteScope scope,RequiredNoteOrigin saved,boolean resumable) {
        var observed=mock(CaptureResourceNotes.class);when(observed.scope()).thenReturn(scope);when(observed.origin()).thenReturn(saved);when(observed.resumable()).thenReturn(resumable);return observed;
    }
    private void pending(String phase){when(configuration.pending(BUSINESS)).thenReturn(List.of(new CaptureExternalRetirements.Pending(original,phase,true)));}
    private static TapstateException refusal(String reason){return new TapstateException(IoError.CAPTURE_EXTERNAL_RETIREMENT_PENDING,Map.of("capture","capture","operation","clear","reason",reason),null);}
}
