package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class SharedSrsPreviewMemberTest {
    private static final CacheBusinessScope BUSINESS=new CacheBusinessScope("cluster",1,"cache");
    private static final CacheAuthScope AUTH=new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"cache",1);
    private static final PipelineIntentScope SCOPE=new PipelineIntentScope("cluster",1,"cache","p",1);
    private static final PipelineIntentScope OTHER=new PipelineIntentScope("cluster",1,"cache","unrelated",1);
    private static final WorkloadOwner OWNER=new WorkloadOwner("a","boot-a"),PEER=new WorkloadOwner("b","boot-b");
    private static final Duration TTL=Duration.ofSeconds(60);

    @Test void theSameDurableAttemptCanFreezeAfterTheOriginalLocalPassHasClosed() {
        try(var f=new Fixture(OWNER)) {
            var plan=f.current.get();var original=f.raw.read(f.original.key()).orElseThrow().claim();
            try(var pass=f.ownership.enterPass(SCOPE)) {
                clearInvocations(f.claims);
                assertThatThrownBy(() -> f.member.freeze(plan)).isInstanceOf(TapstateException.class);
                verify(f.claims,never()).release(any());verify(f.claims,never()).acquire(any(),any(),anyLong(),any());
                assertThat(f.raw.read(f.original.key()).orElseThrow().claim()).isEqualTo(original);
            }
            f.member.freeze(plan);f.member.requireHeld(plan);f.member.endOwnedTenures(plan);f.member.requireEnded(plan);
            var ended=f.raw.read(f.original.key()).orElseThrow();assertThat(ended.leased()).isFalse();
            assertThat(WorkloadClaimFence.from(ended.claim())).isEqualTo(f.fence);
        }
    }

    @Test void theDurableAttemptHoldsOnlyItsActualScopeUntilItsOriginalOwnerEndsTheTenure() {
        try(var f=new Fixture(OWNER)) {
            var plan=f.current.get();f.member.freeze(plan);
            assertThat(f.ownership.permit(SCOPE).granted()).isFalse();assertThat(f.ownership.permit(OTHER).granted()).isTrue();
            assertThatThrownBy(() -> f.member.requireEnded(plan)).isInstanceOf(TapstateException.class);
            f.isolated.set(false);assertThatThrownBy(() -> f.member.endOwnedTenures(plan)).isInstanceOf(TapstateException.class);
            assertThat(f.raw.read(f.original.key()).orElseThrow().leased()).isTrue();
            f.isolated.set(true);f.member.endOwnedTenures(plan);f.member.requireEnded(plan);
            assertThat(f.raw.read(f.original.key()).orElseThrow().leased()).isFalse();
            assertThat(WorkloadClaimFence.from(f.raw.read(f.original.key()).orElseThrow().claim())).isEqualTo(f.fence);
            clearInvocations(f.commands,f.claims);int cfg=f.cfgReads.get();
            f.member.requireHeld(plan);
            verifyNoInteractions(f.commands,f.claims);assertThat(f.cfgReads).hasValue(cfg);
            assertThat(f.ownership.permit(SCOPE).granted()).isFalse();
        }
    }

    @Test void anotherMemberCanBlockButCannotEndTheOriginalOwnersLiveTenure() {
        try(var f=new Fixture(PEER)) {
            var plan=f.current.get();f.member.freeze(plan);clearInvocations(f.claims);
            f.member.endOwnedTenures(plan);verify(f.claims,never()).release(any());
            assertThat(f.raw.read(f.original.key()).orElseThrow().leased()).isTrue();
            assertThatThrownBy(() -> f.member.requireEnded(plan)).isInstanceOf(TapstateException.class);
            assertThat(f.raw.release(f.original)).isTrue();f.member.requireEnded(plan);
        }
    }

    @Test void aNewerPlanRebindsTheSameControlsWithoutLettingADelayedOldInstallReplaceThem() {
        try(var f=new Fixture(OWNER)) {
            var first=f.current.get();f.member.freeze(first);f.member.endOwnedTenures(first);
            var next=f.plan(first.controlRevision(),"d".repeat(64));f.current.set(next);f.version.set(next.controlRevision());
            f.member.maintain();assertThat(f.ownership.permit(SCOPE).granted()).isFalse();
            clearInvocations(f.claims);f.member.freeze(next);f.member.requireHeld(next);
            verifyNoInteractions(f.claims);
            assertThatThrownBy(() -> f.member.freeze(first)).isInstanceOf(TapstateException.class);
            f.member.requireHeld(next);assertThat(f.ownership.permit(SCOPE).granted()).isFalse();
        }
    }

    @ParameterizedTest @ValueSource(strings={"start","complete","operation","scope"})
    void readableStaleControlFactsReleaseTheOriginalLocalBlocker(String change) {
        try(var f=new Fixture(OWNER)) {
            var original=f.current.get();f.member.freeze(original);
            switch(change) {
                case "start" -> f.version.incrementAndGet();
                case "complete" -> f.current.set(f.phase(original,StorageRecoveryCommands.Phase.COMPLETE));
                case "operation" -> {
                    var next=f.plan(original.controlRevision(),"d".repeat(64));
                    f.current.set(new StorageRecoveryCommands.Plan(UUID.randomUUID().toString(),null,AUTH,next.manifest(),next.planHash(),next.controlRevision(),next.phase()));
                    f.version.set(next.controlRevision());
                }
                case "scope" -> f.current.set(new StorageRecoveryCommands.Plan(original.operationId(),null,
                        new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"cache",2),original.manifest(),original.planHash(),original.controlRevision(),original.phase()));
                default -> throw new AssertionError(change);
            }
            f.member.maintain();assertThatThrownBy(() -> f.member.requireHeld(original)).isInstanceOf(TapstateException.class);
            assertThat(f.ownership.permit(SCOPE).granted()).isTrue();
            assertThatThrownBy(() -> f.member.freeze(original)).isInstanceOf(TapstateException.class);
        }
    }

    @Test void aConfigurationWriteDuringInstallationCannotBeAcknowledgedAsTheOriginalPreview() {
        try(var f=new Fixture(OWNER)) {
            var original=f.current.get();var calls=new AtomicInteger();
            when(f.commands.find(AUTH,original.operationId())).thenAnswer(call -> {
                if(calls.incrementAndGet()==2)f.version.incrementAndGet();
                return Optional.of(original);
            });
            assertThatThrownBy(() -> f.member.freeze(original)).isInstanceOf(TapstateException.class);
            f.member.maintain();assertThat(f.ownership.permit(SCOPE).granted()).isTrue();
        }
    }

    @Test void runningReservationsKeepTheirOriginalBlockerAcrossLaterAuthoringWrites() {
        try(var f=new Fixture(OWNER)) {
            var planned=f.current.get();f.member.freeze(planned);f.member.endOwnedTenures(planned);
            var running=f.phase(planned,StorageRecoveryCommands.Phase.RUNNING);f.current.set(running);f.version.addAndGet(10);
            f.member.maintain();f.member.requireHeld(running);f.member.freeze(running);f.member.requireEnded(running);
            assertThat(f.ownership.permit(SCOPE).granted()).isFalse();
        }
    }

    @Test void anUnavailableConfigurationReadCannotPretendTheOriginalPreviewWasAbandoned() {
        try(var f=new Fixture(OWNER)) {
            var original=f.current.get();f.member.freeze(original);
            doThrow(new TapstateException(IoError.STORE_UNAVAILABLE,Map.of("detail","unavailable"),null)).when(f.commands).current(AUTH);
            f.member.maintain();f.member.requireHeld(original);assertThat(f.ownership.permit(SCOPE).granted()).isFalse();
            f.member.close();assertThat(f.ownership.permit(SCOPE).granted()).isTrue();
        }
    }

    private static final class Fixture implements AutoCloseable {
        final InMemoryWorkloadClaimStore raw=new InMemoryWorkloadClaimStore();
        final WorkloadClaimStore claims=mock(WorkloadClaimStore.class);
        final StorageRecoveryCommands commands=mock(StorageRecoveryCommands.class);
        final AtomicReference<StorageRecoveryCommands.Plan> current=new AtomicReference<>();
        final AtomicLong version=new AtomicLong();final AtomicInteger cfgReads=new AtomicInteger();final AtomicBoolean isolated=new AtomicBoolean(true);
        final WorkloadClaim original;final WorkloadClaimFence fence;final PipelineActuationOwnership ownership;final SharedSrsPreviewMember member;
        final CapturePreparation previous=new CapturePreparation(BUSINESS,CapturePreparation.Mode.SHARED_SRS,"mck-shared","capture-shared",
                "mc-"+"a".repeat(64),UUID.randomUUID().toString());
        final String operation=UUID.randomUUID().toString();
        Fixture(WorkloadOwner local) {
            var initial=raw.acquire(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"p"),OWNER,1,TTL).claim();
            original=raw.advanceExecution(initial,1,Set.of(OWNER.nodeId())).orElseThrow();fence=WorkloadClaimFence.from(original);
            when(claims.acquirePipeline(any(),any(),anyLong(),any())).thenAnswer(call -> {
                var scope=call.getArgument(0,PipelineIntentScope.class);
                return raw.acquire(new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,scope.pipelineId()),call.getArgument(1),call.getArgument(2),call.getArgument(3));
            });
            when(claims.readPipeline(any())).thenAnswer(call -> {
                var scope=call.getArgument(0,PipelineIntentScope.class);return raw.read(new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,scope.pipelineId()));
            });
            when(claims.renew(any(),any())).thenAnswer(call -> raw.renew(call.getArgument(0),call.getArgument(1)));
            when(claims.release(any())).thenAnswer(call -> raw.release(call.getArgument(0)));
            var properties=new ClusterProperties();properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
            var gate=new ClusterMembershipGate(properties);gate.observeNodeSession(() -> true);gate.install(new ClusterMembership("cluster",1,Set.of("a","b","c")));gate.canCommit(Set.of("a","b"));
            ownership=PipelineActuationOwnership.scoped(BUSINESS,local,gate,new ClusterWorkloadClaims(claims,gate),TTL,Duration.ofSeconds(10),System::nanoTime);
            if(local.equals(OWNER))assertThat(ownership.permit(SCOPE).granted()).isTrue();
            current.set(plan(10,"c".repeat(64)));version.set(current.get().controlRevision());
            when(commands.guardedBy(any())).thenReturn(commands);
            when(commands.find(eq(AUTH),anyString())).thenAnswer(call -> {cfgReads.incrementAndGet();return Optional.ofNullable(current.get()).filter(value -> value.operationId().equals(call.getArgument(1)));});
            when(commands.current(AUTH)).thenAnswer(call -> {cfgReads.incrementAndGet();return Optional.ofNullable(current.get());});
            member=new SharedSrsPreviewMember(commands,AUTH,ownership,claims,() -> local,() -> {cfgReads.incrementAndGet();return version.get();},
                    () -> {},(scope,claim) -> {if(!isolated.get())throw ThreeLayerStorePort.refused();});
        }
        StorageRecoveryCommands.Plan plan(long revision,String hash) {
            var context=new SrsConsumerContext(new SrsConsumerKey(previous.chainId(),"p","input"),1);
            var snapshot=new SharedSrsRecoverySnapshot(previous,new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,previous.captureId()),OWNER,1,0,1),
                    hash,List.of(new SrsConsumerMembership(context,1,false,Set.of("orders"),fence)),Map.of("p",new SharedSrsRecoverySnapshot.Pipeline(SCOPE,fence)));
            var manifest=new StorageRecoveryCommands.Manifest(new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.SHARED_SRS,Set.of(),previous.canonicalChainId(),previous.chainId()),
                    Set.of("p"),Map.of("p","a".repeat(64)),hash,revision,snapshot);
            return new StorageRecoveryCommands.Plan(operation,null,AUTH,manifest,hash,revision+1,StorageRecoveryCommands.Phase.PLANNED);
        }
        StorageRecoveryCommands.Plan phase(StorageRecoveryCommands.Plan original,StorageRecoveryCommands.Phase phase) {
            return new StorageRecoveryCommands.Plan(original.operationId(),original.previousOperationId(),original.originalScope(),original.manifest(),original.planHash(),original.controlRevision(),phase);
        }
        @Override public void close(){member.close();}
    }
}
