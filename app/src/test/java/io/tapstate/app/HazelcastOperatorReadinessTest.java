package io.tapstate.app;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.*;
import io.tapstate.spi.store.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class HazelcastOperatorReadinessTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster",1,"cache","p",7);
    private static ExecutionFence fence(long execution) {
        return new ExecutionFence(SCOPE,new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"p"),
                new WorkloadOwner("a","boot-a"),3,execution,1));
    }
    private static final List<OperatorMapCapacity> CAPACITIES = List.of(
            new OperatorMapCapacity(new OperatorPlacement(SCOPE,"cache",OperatorPlacement.Resource.NEST_STATE,"nest","[]","state"),512),
            new OperatorMapCapacity(new OperatorPlacement(SCOPE,"cache",OperatorPlacement.Resource.JOIN_FACTS,"join","root","facts"),64));
    private static OperatorRunManifest manifest(long execution) {
        var original = fence(execution);
        return new OperatorRunManifest("p",original,CAPACITIES.stream().map(c -> new OperatorMapBinding(c.placement(),original,original.operatorExecutionKey())).toList());
    }

    @Test void twoActualMembersAcknowledgeTheSameSharedAttemptBeforeEachSeparateRetirementStage() {
        try(var f=new RealMembers()) {
            var plan=sharedPlan();var left=sharedReceiver(plan);var right=sharedReceiver(plan);
            var leftCaptures=mock(ScopedPipelineCaptureCoordinator.class);var rightCaptures=mock(ScopedPipelineCaptureCoordinator.class);
            HazelcastOperatorReadiness.bindSharedPreview(f.a,left,leftCaptures);HazelcastOperatorReadiness.bindSharedPreview(f.b,right,rightCaptures);
            var ticket=new HazelcastOperatorReadiness(f.a,Duration.ofSeconds(5)).freezeShared(plan,() -> {});
            ticket.stopCapture(() -> {});ticket.endOwnedTenures(() -> {});ticket.verifyEnded(() -> {});ticket.requireHeld(plan,() -> {});
            for(var pair:List.of(Map.entry(left,leftCaptures),Map.entry(right,rightCaptures))) {
                var ordered=inOrder(pair.getKey(),pair.getValue());
                ordered.verify(pair.getKey()).freeze(plan);
                ordered.verify(pair.getValue()).stopSharedLocally(plan.manifest().sharedSrs().previous(),plan.manifest().sharedSrs().captureFence());
                ordered.verify(pair.getKey()).endOwnedTenures(plan);
                ordered.verify(pair.getKey()).requireEnded(plan);
                ordered.verify(pair.getValue()).requireSharedRetired(plan.manifest().sharedSrs().previous(),plan.manifest().sharedSrs().captureFence());
            }
        }
    }

    @Test void oneActualMemberRefusingTheSharedFreezeCannotProduceAnAllMemberTicket() {
        try(var f=new RealMembers()) {
            var plan=sharedPlan();var left=sharedReceiver(plan);var right=sharedReceiver(plan);
            var captures=mock(ScopedPipelineCaptureCoordinator.class);
            doThrow(new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null)).when(right).freeze(plan);
            HazelcastOperatorReadiness.bindSharedPreview(f.a,left,captures);HazelcastOperatorReadiness.bindSharedPreview(f.b,right,captures);
            assertThatThrownBy(() -> new HazelcastOperatorReadiness(f.a,Duration.ofSeconds(5)).freezeShared(plan,() -> {}))
                    .isInstanceOfSatisfying(TapstateException.class,failed -> {
                        assertThat(failed.code()).isEqualTo(ActuationError.SHARED_RECOVERY_MEMBER_UNAVAILABLE);
                        assertThat(failed.args()).containsEntry("member","b").containsEntry("reason",IoError.WORKLOAD_CLAIM_FENCED.code());
                    });
            verify(captures,never()).stopSharedLocally(any(),any());verify(left,never()).endOwnedTenures(any());verify(right,never()).endOwnedTenures(any());
        }
    }

    @Test void anEngineReplacementInvalidatesTheOriginalSharedTicketBeforeAnotherRpc() {
        var f=new MockMembers();var plan=sharedPlan();var receiver=sharedReceiver(plan);
        HazelcastOperatorReadiness.bindSharedPreview(f.member,receiver,mock(ScopedPipelineCaptureCoordinator.class));
        var ticket=f.readiness().freezeShared(plan,() -> {});
        ticket.stopCapture(() -> {});
        String encoded=new String(f.serializedTask.get(),java.nio.charset.StandardCharsets.ISO_8859_1);
        assertThat(encoded).doesNotContain("StorageBinding$Mode","StorageRecoveryCommands$Plan","RequiredNoteOrigin");
        f.members.set(new LinkedHashSet<>(List.of(f.a,member("b","boot-b",UUID.randomUUID()))));
        var executor=f.member.getExecutorService(HazelcastOperatorReadiness.EXECUTOR);clearInvocations(executor,receiver);
        assertThatThrownBy(() -> ticket.endOwnedTenures(() -> {})).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> ticket.requireHeld(plan,() -> {})).isInstanceOf(TapstateException.class);
        verify(executor,never()).submitToMembers(any(HazelcastOperatorReadiness.SharedCall.class),anyCollection());
        verify(receiver,never()).requireHeld(any());
    }

    @Test void aSharedFreezeAloneCannotStandInForTheSeparatePhysicalAndDurableEndStages() {
        var f=new MockMembers();var plan=sharedPlan();var receiver=sharedReceiver(plan);
        HazelcastOperatorReadiness.bindSharedPreview(f.member,receiver,mock(ScopedPipelineCaptureCoordinator.class));
        var ticket=f.readiness().freezeShared(plan,() -> {});
        assertThatThrownBy(() -> ticket.requireStaged(plan,() -> {})).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> ticket.endOwnedTenures(() -> {})).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> ticket.verifyEnded(() -> {})).isInstanceOf(TapstateException.class);
        assertThat(f.dispatches).hasValue(1);
        ticket.stopCapture(() -> {});assertThatThrownBy(() -> ticket.requireStaged(plan,() -> {})).isInstanceOf(TapstateException.class);
        ticket.endOwnedTenures(() -> {});ticket.requireStaged(plan,() -> {});
        assertThatThrownBy(() -> ticket.requireVerified(plan,() -> {})).isInstanceOf(TapstateException.class);
        ticket.verifyEnded(() -> {});ticket.requireVerified(plan,() -> {});assertThat(f.dispatches).hasValue(4);
    }

    @Test void aMembershipChangeDuringTheSharedFreezeCannotBeAnAcknowledgedOriginalRoster() {
        var f=new MockMembers();var plan=sharedPlan();var receiver=sharedReceiver(plan);
        HazelcastOperatorReadiness.bindSharedPreview(f.member,receiver,mock(ScopedPipelineCaptureCoordinator.class));
        f.dispatch=() -> f.members.set(new LinkedHashSet<>(List.of(f.a,member("b","boot-b",UUID.randomUUID()))));
        assertThatThrownBy(() -> f.readiness().freezeShared(plan,() -> {})).isInstanceOfSatisfying(TapstateException.class,failed -> {
            assertThat(failed.code()).isEqualTo(ActuationError.SHARED_RECOVERY_MEMBER_UNAVAILABLE);
            assertThat(failed.args()).containsEntry("reason","membership-changed");
        });
        verify(receiver,never()).requireHeld(any());
    }

    @ParameterizedTest @ValueSource(strings={"no-answer","cancelled","timeout","identity"})
    void anIncompleteOrWrongSharedReplyCannotBecomeATicket(String fault) throws Exception {
        var f=new MockMembers();var plan=sharedPlan();var receiver=sharedReceiver(plan);
        HazelcastOperatorReadiness.bindSharedPreview(f.member,receiver,mock(ScopedPipelineCaptureCoordinator.class));
        switch(fault) {
            case "no-answer" -> f.answers.remove(f.b);
            case "cancelled" -> {var answer=new CompletableFuture<HazelcastOperatorReadiness.Result>();answer.cancel(false);f.answers.put(f.b,answer);}
            case "timeout" -> {
                Future<HazelcastOperatorReadiness.Result> answer=mock(Future.class);when(answer.get(anyLong(),eq(TimeUnit.NANOSECONDS))).thenThrow(new TimeoutException());f.answers.put(f.b,answer);
            }
            case "identity" -> f.answers.put(f.b,CompletableFuture.completedFuture(new HazelcastOperatorReadiness.Result(identity(member("b","boot-b",UUID.randomUUID())),null)));
            default -> throw new AssertionError(fault);
        }
        assertThatThrownBy(() -> f.readiness().freezeShared(plan,() -> {})).isInstanceOfSatisfying(TapstateException.class,failed -> {
            assertThat(failed.code()).isEqualTo(ActuationError.SHARED_RECOVERY_MEMBER_UNAVAILABLE);
            assertThat(failed.args()).containsEntry("member","b").containsEntry("reason",fault.equals("identity")?"identity-changed":fault);
        });
        verify(receiver,never()).requireHeld(any());
    }

    private static SharedSrsPreviewMember sharedReceiver(StorageRecoveryCommands.Plan original) {
        var receiver=mock(SharedSrsPreviewMember.class);
        when(receiver.original(original.originalScope(),original.operationId(),original.planHash(),original.controlRevision())).thenReturn(original);return receiver;
    }
    private static StorageRecoveryCommands.Plan sharedPlan() {
        var scope=new CacheBusinessScope("cluster",1,"cache");
        var original=new CapturePreparation(scope,CapturePreparation.Mode.SHARED_SRS,"mck-shared","capture-shared","mc-"+"a".repeat(64),UUID.randomUUID().toString());
        var pipeline=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"p"),new WorkloadOwner("a","boot-a"),3,1,1);
        var capture=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,original.captureId()),new WorkloadOwner("a","boot-a"),1,0,1);
        var context=new SrsConsumerContext(new SrsConsumerKey(original.chainId(),"p","input"),SCOPE.stateGeneration());
        var observed=new SharedSrsRecoverySnapshot(original,capture,"c".repeat(64),List.of(new SrsConsumerMembership(context,1,false,Set.of("orders"),pipeline)),
                Map.of("p",new SharedSrsRecoverySnapshot.Pipeline(SCOPE,pipeline)));
        var manifest=new StorageRecoveryCommands.Manifest(new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.SHARED_SRS,Set.of(),original.canonicalChainId(),original.chainId()),
                Set.of("p"),Map.of("p","a".repeat(64)),"b".repeat(64),10,observed);
        return new StorageRecoveryCommands.Plan(UUID.randomUUID().toString(),null,new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"cache",1),manifest,"d".repeat(64),11,StorageRecoveryCommands.Phase.PLANNED);
    }

    @Test void twoActualMembersPrepareTheSameOriginalBindingsThroughTheirOwnRegistries() {
        try (var f = new RealMembers()) {
            var readiness = new HazelcastOperatorReadiness(f.a,Duration.ofSeconds(5)); var original = manifest(1);
            readiness.prepare(original,CAPACITIES,List.of("a","b"),() -> {});
            for (var binding : original.bindings()) {
                Object key = binding.placement().resource() == OperatorPlacement.Resource.NEST_STATE ? List.of("key") : "key";
                var map = f.a.getMap(binding.mapName()); map.set(key,"value"); map.evictAll();
                assertThat(f.b.getMap(binding.mapName()).get(key)).isEqualTo("value");
                assertThat(f.b.getConfig().getMapConfigs()).containsKey(binding.configurationPattern());
            }
        }
    }

    @Test void exactHotReleasePreservesColdValuesAndALateOldReleaseLeavesTheReplacementAlone() {
        try (var f = new RealMembers()) {
            var readiness = new HazelcastOperatorReadiness(f.a,Duration.ofSeconds(5));
            var old = manifest(1); readiness.prepare(old,CAPACITIES,List.of("a","b"),() -> {});
            for (var binding : old.bindings()) {
                Object key = binding.placement().resource() == OperatorPlacement.Resource.NEST_STATE ? List.of("key") : "key";
                f.a.getMap(binding.mapName()).set(key,"retained");
            }
            assertThat(f.bytes.values).isNotEmpty(); int coldSize = f.bytes.values.size();
            readiness.releaseHot(old);
            for (var binding : old.bindings()) {
                assertThat(f.a.getDistributedObjects()).noneMatch(object -> object.getName().equals(binding.mapName()));
                if (binding.placement().resource() == OperatorPlacement.Resource.JOIN_FACTS)
                    assertThatThrownBy(() -> f.registryB.join(binding.mapName())).isInstanceOf(TapstateException.class);
            }
            assertThat(f.bytes.values).hasSize(coldSize);
            var next = manifest(2); readiness.prepare(next,CAPACITIES,List.of("a","b"),() -> {});
            for (var binding : next.bindings()) {
                Object key = binding.placement().resource() == OperatorPlacement.Resource.NEST_STATE ? List.of("key") : "key";
                assertThat(f.b.getMap(binding.mapName()).get(key)).isEqualTo("retained");
            }
            readiness.releaseHot(old);
            for (var binding : next.bindings()) {
                Object key = binding.placement().resource() == OperatorPlacement.Resource.NEST_STATE ? List.of("key") : "key";
                f.a.getMap(binding.mapName()).evictAll();
                assertThat(f.b.getMap(binding.mapName()).get(key)).isEqualTo("retained");
                var first = binding.placement().resource() == OperatorPlacement.Resource.NEST_STATE
                        ? f.registryA.nest(binding.mapName(),"cache") : f.registryA.join(binding.mapName());
                var second = binding.placement().resource() == OperatorPlacement.Resource.NEST_STATE
                        ? f.registryB.nest(binding.mapName(),"cache") : f.registryB.join(binding.mapName());
                assertThat(first.count(binding.mapName())).isOne(); assertThat(second.count(binding.mapName())).isOne();
            }
            assertThat(f.bytes.values).hasSize(coldSize);
        }
    }

    @Test void releaseNeverCreatesAnAbsentHotMapAndRefusesForgedCoordinatesBeforeCleanup() {
        var f = new MockMembers(); f.readiness().releaseHot(manifest(1));
        verify(f.member,never()).getMap(anyString());
        var wrong = new OperatorRunManifest("p",fence(1),List.of(new OperatorMapBinding(CAPACITIES.getFirst().placement(),fence(1),"0".repeat(64))));
        clearInvocations(f.member);
        assertThatThrownBy(() -> f.readiness().releaseHot(wrong)).isInstanceOf(TapstateException.class);
        verify(f.member,never()).getDistributedObjects();
    }

    @Test void aSynchronousCollectionDisconnectUsesTheCodedMemberBoundary() {
        var f=new MockMembers(); var executor=f.member.getExecutorService(HazelcastOperatorReadiness.EXECUTOR);
        when(executor.submitToMembers(any(HazelcastOperatorReadiness.Collect.class),anyCollection()))
                .thenThrow(new com.hazelcast.core.OperationTimeoutException("member stopped"));
        assertThatThrownBy(() -> f.readiness().isolate(SCOPE,null,() -> {}))
                .isInstanceOfSatisfying(TapstateException.class,failure -> assertThat(failure.code()).isEqualTo(ActuationError.OPERATOR_STATE_UNAVAILABLE_ON_MEMBER));
    }

    @Test void scopeIsolationCollectsBothMembersAndALateCeilingCannotReleaseTheNextRun() {
        try(var f=new RealMembers()) {
            var left=mock(ScopedPipelineCaptureCoordinator.class); var right=mock(ScopedPipelineCaptureCoordinator.class);
            HazelcastOperatorReadiness.bindRetirement(f.a,left,() -> {}); HazelcastOperatorReadiness.bindRetirement(f.b,right,() -> {});
            var readiness=new HazelcastOperatorReadiness(f.a,Duration.ofSeconds(5)); var original=manifest(1);
            readiness.prepare(original,CAPACITIES,List.of("a","b"),() -> {});
            var ceiling=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"p"),
                    new WorkloadOwner("a","boot-a"),3,1,1);
            var collected=readiness.isolate(SCOPE,ceiling,() -> {});
            assertThat(collected).containsExactly(original);
            verify(left).retireScope(SCOPE,ceiling); verify(right).retireScope(SCOPE,ceiling);
            collected.forEach(readiness::releaseHot); readiness.requireRetired(SCOPE,ceiling,() -> {});
            var next=manifest(2); readiness.prepare(next,CAPACITIES,List.of("a","b"),() -> {});
            assertThatThrownBy(() -> readiness.isolate(SCOPE,ceiling,() -> {})).isInstanceOf(TapstateException.class);
            assertThat(f.registryA.originals(SCOPE)).containsExactlyInAnyOrderElementsOf(next.bindings());
            assertThat(f.registryB.originals(SCOPE)).containsExactlyInAnyOrderElementsOf(next.bindings());
        }
    }

    @Test void oneActualMemberRefusalIsReturnedAsACanonicalCodeAndCannotBeMistakenForPreparation() {
        try (var f = new RealMembers()) {
            f.refuseB.set(true);
            assertThatThrownBy(() -> new HazelcastOperatorReadiness(f.a,Duration.ofSeconds(5)).prepare(manifest(1),CAPACITIES,List.of("a","b"),() -> {}))
                    .isInstanceOfSatisfying(TapstateException.class,failed -> {
                        assertThat(failed.code()).isEqualTo(ActuationError.OPERATOR_STATE_UNAVAILABLE_ON_MEMBER);
                        assertThat(failed.args()).containsEntry("member","b").containsEntry("reason",IoError.WORKLOAD_CLAIM_FENCED.code());
                    });
            assertThatThrownBy(() -> f.registryB.join(manifest(1).bindings().getLast().mapName())).isInstanceOf(TapstateException.class);
        }
    }

    @Test void sameNodeAndBootWithANewEngineMemberCannotReuseTheOldPreparationResult() {
        var f = new MockMembers(); var replacement = member("b","boot-b",UUID.randomUUID());
        f.dispatch = () -> f.members.set(new LinkedHashSet<>(List.of(f.a,replacement)));
        refusal(() -> f.readiness().prepare(manifest(1),CAPACITIES,List.of("a","b"),() -> {}),"membership-changed");
    }

    @Test void differentPlannedMembersAndDuplicateStableIdentitiesAreRejectedBeforeDispatch() {
        var f = new MockMembers();
        refusal(() -> f.readiness().prepare(manifest(1),CAPACITIES,List.of("a","missing"),() -> {}),"membership-changed");
        assertThat(f.dispatches.get()).isZero();
        f.members.set(new LinkedHashSet<>(List.of(f.a,member("a","another-boot",UUID.randomUUID()))));
        refusal(() -> f.readiness().prepare(manifest(1),CAPACITIES,List.of("a","b"),() -> {}),"identity-duplicate");
        assertThat(f.dispatches.get()).isZero();
    }

    @Test void aTimeoutIsCodedAndAProgrammerFailureKeepsItsOriginalException() {
        var f = new MockMembers();
        Future<HazelcastOperatorReadiness.Result> slow = mock(Future.class);
        try { when(slow.get(anyLong(),eq(TimeUnit.NANOSECONDS))).thenThrow(new TimeoutException("not-for-display")); }
        catch (Exception impossible) { throw new AssertionError(impossible); }
        f.answers.put(f.a,slow); f.answers.put(f.b,slow);
        refusal(() -> f.readiness().prepare(manifest(1),CAPACITIES,List.of("a","b"),() -> {}),"timeout");
        verify(slow,never()).cancel(anyBoolean());
        var defect = new IllegalStateException("programmer defect");
        f.answers.replaceAll((member,answer) -> CompletableFuture.failedFuture(defect));
        assertThatThrownBy(() -> f.readiness().prepare(manifest(1),CAPACITIES,List.of("a","b"),() -> {})).isSameAs(defect);
    }

    @Test void aDirectMemberDepartureWithoutAnInnerCauseIsAnExpectedMemberFailure() throws Exception {
        var f = new MockMembers(); Future<HazelcastOperatorReadiness.Result> departed = mock(Future.class);
        var left = new com.hazelcast.core.MemberLeftException(f.a);
        when(departed.get(anyLong(),eq(TimeUnit.NANOSECONDS))).thenThrow(left);
        f.answers.replaceAll((member,answer) -> departed);
        refusal(() -> f.readiness().prepare(manifest(1),CAPACITIES,List.of("a","b"),() -> {}),"member-unavailable");
    }

    @Test void aPreparationCancelledByTheExecutorIsAnExpectedRefusal() {
        var f = new MockMembers(); var cancelled = new CompletableFuture<HazelcastOperatorReadiness.Result>(); cancelled.cancel(false);
        f.answers.replaceAll((member,answer) -> cancelled);
        refusal(() -> f.readiness().prepare(manifest(1),CAPACITIES,List.of("a","b"),() -> {}),"cancelled");
    }

    @Test void aManifestWhosePositionsDoNotBelongToItsOriginalExecutionCannotDispatch() {
        var f = new MockMembers(); var original = fence(1);
        var foreign = new OperatorPlacement(new PipelineIntentScope("cluster",1,"foreign-cache","p",7),"cache",OperatorPlacement.Resource.JOIN_FACTS,"join","root","facts");
        var malformed = new OperatorRunManifest("p",original,List.of(new OperatorMapBinding(foreign,original,original.operatorExecutionKey())));
        assertThatThrownBy(() -> f.readiness().prepare(malformed,List.of(new OperatorMapCapacity(foreign,64)),List.of("a","b"),() -> {}))
                .isInstanceOfSatisfying(TapstateException.class,failed -> assertThat(failed.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        assertThat(f.dispatches.get()).isZero();
        var wrongKey = new OperatorRunManifest("p",original,List.of(new OperatorMapBinding(CAPACITIES.getFirst().placement(),original,"0".repeat(64))));
        assertThatThrownBy(() -> f.readiness().prepare(wrongKey,List.of(CAPACITIES.getFirst()),List.of("a","b"),() -> {}))
                .isInstanceOfSatisfying(TapstateException.class,failed -> assertThat(failed.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        assertThat(f.dispatches.get()).isZero();
    }

    @Test void capturedInputsAndTransportContainOnlyImmutableOriginalCoordinatesAndCanonicalFailureText() throws Exception {
        var f = new MockMembers(); var capacities = new ArrayList<>(CAPACITIES); var planned = new ArrayList<>(List.of("a","b"));
        f.readiness().prepare(manifest(1),capacities,planned,() -> { capacities.clear(); planned.clear(); });
        assertThat(f.serializedTask.get()).isNotEmpty();
        var task = new HazelcastOperatorReadiness.Install(manifest(1),CAPACITIES,Map.of("a",identity(f.a)));
        var local = mock(HazelcastInstance.class); var cluster = mock(Cluster.class);
        when(local.getCluster()).thenReturn(cluster); when(cluster.getLocalMember()).thenReturn(f.a);
        when(local.getUserContext()).thenReturn(new ConcurrentHashMap<>());
        task.setHazelcastInstance(local);
        var result = task.call(); assertThat(result.refusalCode()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED.code());
        assertThat(roundTrip(result)).isEqualTo(result);
    }

    private static void refusal(Runnable action,String reason) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(TapstateException.class,failure -> {
            assertThat(failure.code()).isEqualTo(ActuationError.OPERATOR_STATE_UNAVAILABLE_ON_MEMBER);
            assertThat(failure.args()).containsEntry("pipeline","p").containsEntry("reason",reason);
        });
    }
    private static Member member(String id,String boot,UUID uuid) {
        var member = mock(Member.class); when(member.getUuid()).thenReturn(uuid);
        when(member.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE)).thenReturn(id);
        when(member.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE)).thenReturn(boot); return member;
    }
    private static HazelcastOperatorReadiness.Identity identity(Member member) {
        return new HazelcastOperatorReadiness.Identity(member.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE),member.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE),member.getUuid());
    }
    private static Object roundTrip(Object value) throws Exception {
        var bytes = new ByteArrayOutputStream(); try (var out = new ObjectOutputStream(bytes)) { out.writeObject(value); }
        try (var in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) { return in.readObject(); }
    }
    private static final class MockMembers {
        final Member a = member("a","boot-a",UUID.randomUUID()), b = member("b","boot-b",UUID.randomUUID());
        final AtomicReference<Set<Member>> members = new AtomicReference<>(new LinkedHashSet<>(List.of(a,b)));
        final HazelcastInstance member = mock(HazelcastInstance.class); final AtomicInteger dispatches = new AtomicInteger();
        final Map<Member,Future<HazelcastOperatorReadiness.Result>> answers = new LinkedHashMap<>();
        final AtomicReference<byte[]> serializedTask = new AtomicReference<>(); Runnable dispatch = () -> {};
        MockMembers() {
            var cluster = mock(Cluster.class); when(member.getCluster()).thenReturn(cluster); when(cluster.getMembers()).thenAnswer(invocation -> members.get());
            when(member.getUserContext()).thenReturn(new ConcurrentHashMap<>());
            var executor = mock(IExecutorService.class); when(member.getExecutorService(HazelcastOperatorReadiness.EXECUTOR)).thenReturn(executor);
            answers.put(a,CompletableFuture.completedFuture(new HazelcastOperatorReadiness.Result(identity(a),null)));
            answers.put(b,CompletableFuture.completedFuture(new HazelcastOperatorReadiness.Result(identity(b),null)));
            when(executor.submitToMembers(any(HazelcastOperatorReadiness.Release.class),anyCollection())).thenAnswer(invocation -> {
                dispatches.incrementAndGet(); dispatch.run(); return answers;
            });
            when(executor.submitToMembers(any(HazelcastOperatorReadiness.Install.class),anyCollection())).thenAnswer(invocation -> {
                var bytes = new ByteArrayOutputStream(); try (var out = new ObjectOutputStream(bytes)) { out.writeObject(invocation.getArgument(0)); }
                serializedTask.set(bytes.toByteArray()); dispatches.incrementAndGet(); dispatch.run(); return answers;
            });
            when(executor.submitToMembers(any(HazelcastOperatorReadiness.SharedCall.class),anyCollection())).thenAnswer(invocation -> {
                var bytes=new ByteArrayOutputStream();try(var out=new ObjectOutputStream(bytes)){out.writeObject(invocation.getArgument(0));}
                serializedTask.set(bytes.toByteArray());dispatches.incrementAndGet();dispatch.run();return answers;
            });
        }
        HazelcastOperatorReadiness readiness() { return new HazelcastOperatorReadiness(member,Duration.ofSeconds(1)); }
    }
    private static final class RealMembers implements AutoCloseable {
        final HazelcastInstance a,b; final OperatorMapBindings registryA,registryB; final AtomicBoolean refuseB = new AtomicBoolean();
        final Bytes bytes = new Bytes();
        RealMembers() {
            var first = config("operator-rpc-"+UUID.randomUUID(),"a"); first.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
            a = Hazelcast.newHazelcastInstance(first);
            HazelcastInstance joined = null; OperatorMapBindings leftRegistry = null, rightRegistry = null;
            try {
                var second = config(first.getClusterName(),"b"); second.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true)
                        .addMember("127.0.0.1:"+a.getCluster().getLocalMember().getAddress().getPort());
                joined = Hazelcast.newHazelcastInstance(second);
                leftRegistry = OperatorMapBindings.bindTo(a,(binding,active) -> { active.run(); return bytes; });
                rightRegistry = OperatorMapBindings.bindTo(joined,(binding,active) -> { active.run(); if (refuseB.get()) throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); return bytes; });
                b = joined; registryA = leftRegistry; registryB = rightRegistry;
            } catch (RuntimeException | Error failure) {
                if (rightRegistry != null) rightRegistry.close(); if (leftRegistry != null) leftRegistry.close();
                if (joined != null) joined.shutdown(); a.shutdown(); throw failure;
            }
        }
        @Override public void close() { registryB.close(); registryA.close(); b.shutdown(); a.shutdown(); }
        private static Config config(String cluster,String id) {
            var config = new Config().setClusterName(cluster); config.setProperty("hazelcast.partition.count","17");
            config.getMemberAttributeConfig().setAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE,id).setAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE,"boot-"+id);
            config.setProperty("hazelcast.phone.home.enabled","false"); config.setProperty("hazelcast.shutdownhook.enabled","false"); config.setProperty("hazelcast.logging.type","none");
            config.setProperty("hazelcast.operation.thread.count","2"); config.setProperty("hazelcast.operation.generic.thread.count","2");
            config.setProperty("hazelcast.io.input.thread.count","1"); config.setProperty("hazelcast.io.output.thread.count","1");
            var network = config.getNetworkConfig(); network.setPort(0).setPortAutoIncrement(false); network.getInterfaces().setEnabled(true).addInterface("127.0.0.1");
            var join = network.getJoin(); join.getMulticastConfig().setEnabled(false); join.getTcpIpConfig().setEnabled(false); join.getAutoDetectionConfig().setEnabled(false); return config;
        }
    }
    private static final class Bytes implements KeyedStateStore {
        final Map<String,byte[]> values = new ConcurrentHashMap<>();
        @Override public Optional<byte[]> load(String namespace,String key) { return Optional.ofNullable(values.get(namespace+"/"+key)).map(byte[]::clone); }
        @Override public void save(String namespace,String key,byte[] value) { values.put(namespace+"/"+key,value.clone()); }
        @Override public Optional<byte[]> saveIfAbsent(String namespace,String key,byte[] value) { return Optional.ofNullable(values.putIfAbsent(namespace+"/"+key,value.clone())); }
        @Override public void delete(String namespace,String key) { values.remove(namespace+"/"+key); }
        @Override public void dropNamespace(String namespace) { values.keySet().removeIf(key -> key.startsWith(namespace+"/")); }
        @Override public long count(String namespace) { return values.keySet().stream().filter(key -> key.startsWith(namespace+"/")).count(); }
    }
}
