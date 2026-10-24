package io.tapstate.app;

import io.tapstate.control.core.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class StorageRecoveryPreviewTest {
    private static final CacheAuthScope AUTH=new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"cache",1);

    @Test void theReadableInitialPlanCannotBeConfirmedBeforeRuntimePreflightFinalizesItsExactResult() {
        var f=new Fixture();var attempted=new AtomicBoolean();
        f.finish=original -> {
            assertThat(f.transaction).isFalse();
            assertThat(f.service.status(AUTH,f.active,original.operationId())).contains(original);
            assertThatThrownBy(() -> f.service.confirm("admin",AUTH,f.active,original.operationId(),original.planHash(),original.controlRevision(),true))
                    .isInstanceOf(TapstateException.class);
            verify(f.commands,never()).confirm(any());attempted.set(true);
            var stable=f.plan(original.operationId(),original.controlRevision(),"e".repeat(64),StorageRecoveryCommands.Phase.PLANNED);
            f.current.set(stable);f.finalized=true;return stable;
        };
        var finalPlan=f.service.plan("admin",AUTH,f.active,f.manifest.selection(),null,null);
        assertThat(attempted).isTrue();assertThat(finalPlan).isEqualTo(f.current.get());
        var confirmed=f.service.confirm("admin",AUTH,f.active,finalPlan.operationId(),finalPlan.planHash(),finalPlan.controlRevision(),true);
        assertThat(confirmed.phase()).isEqualTo(StorageRecoveryCommands.Phase.CONFIRMED);
        assertThat(f.published).hasValue(1);verify(f.commands,never()).start(any());
    }

    @ParameterizedTest @ValueSource(strings={"operation","scope","selection","confirmed","running","older","same-revision-hash","same-revision-closure"})
    void runtimePreflightCannotRedirectOrConfirmItsOriginalRecovery(String change) {
        var f=new Fixture();
        f.finish=original -> switch(change) {
            case "operation" -> new StorageRecoveryCommands.Plan(UUID.randomUUID().toString(),null,AUTH,original.manifest(),original.planHash(),original.controlRevision(),original.phase());
            case "scope" -> new StorageRecoveryCommands.Plan(original.operationId(),null,new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"cache",2),original.manifest(),original.planHash(),original.controlRevision(),original.phase());
            case "selection" -> {
                var manifest=new StorageRecoveryCommands.Manifest(new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.PIPELINE,Set.of("p")),Set.of("p"),Map.of("p","a".repeat(64)),"b".repeat(64),5);
                yield new StorageRecoveryCommands.Plan(original.operationId(),null,AUTH,manifest,original.planHash(),original.controlRevision(),original.phase());
            }
            case "confirmed" -> f.phase(original,StorageRecoveryCommands.Phase.CONFIRMED);
            case "running" -> f.phase(original,StorageRecoveryCommands.Phase.RUNNING);
            case "older" -> f.plan(original.operationId(),original.manifest().configurationRevision()-1,"f".repeat(64),StorageRecoveryCommands.Phase.PLANNED);
            case "same-revision-hash" -> new StorageRecoveryCommands.Plan(original.operationId(),null,AUTH,original.manifest(),"f".repeat(64),original.controlRevision(),original.phase());
            case "same-revision-closure" -> {
                var before=original.manifest();
                var changed=new StorageRecoveryCommands.Manifest(before.selection(),before.affectedPipelineIds(),before.definitionHashes(),"f".repeat(64),before.configurationRevision(),before.sharedSrs());
                yield new StorageRecoveryCommands.Plan(original.operationId(),null,AUTH,changed,original.planHash(),original.controlRevision(),original.phase());
            }
            default -> throw new AssertionError(change);
        };
        assertThatThrownBy(() -> f.service.plan("admin",AUTH,f.active,f.manifest.selection(),null,null)).isInstanceOf(IllegalStateException.class);
        assertThat(f.published).hasValue(0);verify(f.commands,never()).confirm(any());verify(f.commands,never()).start(any());
    }

    @Test void originalAuthenticationStillRunsOnEveryConfigurationRetryBeforeAnyRecoveryWrite() {
        var f=new Fixture();f.attempts=2;f.beforeRetry=() -> f.authenticated=false;
        assertThatThrownBy(() -> f.service.plan("admin",AUTH,f.active,f.manifest.selection(),null,null)).isInstanceOf(TapstateException.class);
        assertThat(f.current).hasValue(null);assertThat(f.prepared).hasValue(0);assertThat(f.authorizationChecks.get()).isGreaterThan(1);
    }

    @Test void aProtectedCancelledContinuationStaysAuditedAndRequiresAnotherHumanConfirmation() {
        var f=new Fixture();var running=f.plan(f.operation,5,"c".repeat(64),StorageRecoveryCommands.Phase.RUNNING);f.current.set(running);
        f.continuation=Optional.of(() -> {
            assertThat(f.audits).hasValue(1);assertThat(f.transaction).isFalse();
            var next=f.plan(running.operationId(),running.controlRevision()+2,"f".repeat(64),StorageRecoveryCommands.Phase.PLANNED);f.current.set(next);return next;
        });
        var result=f.service.plan("admin",AUTH,f.active,running.manifest().selection(),running.operationId(),null);
        assertThat(result.operationId()).isEqualTo(running.operationId());assertThat(result.phase()).isEqualTo(StorageRecoveryCommands.Phase.PLANNED);
        assertThat(result.controlRevision()).isGreaterThan(running.controlRevision());
        verify(f.commands,never()).plan(any(),any(),any(),any());verify(f.commands,never()).confirm(any());assertThat(f.published).hasValue(0);
    }

    @Test void anAlreadyAppliedOriginalReceiptIsReturnedWithoutPreparingOrPublishingBusiness() {
        var f=new Fixture();var running=f.plan(f.operation,5,"c".repeat(64),StorageRecoveryCommands.Phase.RUNNING);f.current.set(running);
        var completed=f.phase(running,StorageRecoveryCommands.Phase.COMPLETE);f.continuation=Optional.of(() -> completed);
        assertThat(f.service.plan("admin",AUTH,f.active,running.manifest().selection(),running.operationId(),null)).isEqualTo(completed);
        assertThat(f.prepared).hasValue(0);assertThat(f.published).hasValue(0);verify(f.commands,never()).start(any());
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void aProtectedContinuationCannotChangeTheOriginalHashAtItsExistingRevision(boolean completed) {
        var f=new Fixture();var running=f.plan(f.operation,5,"c".repeat(64),StorageRecoveryCommands.Phase.RUNNING);f.current.set(running);
        f.continuation=Optional.of(() -> new StorageRecoveryCommands.Plan(running.operationId(),null,AUTH,running.manifest(),"f".repeat(64),running.controlRevision(),
                completed?StorageRecoveryCommands.Phase.COMPLETE:StorageRecoveryCommands.Phase.PLANNED));
        assertThatThrownBy(() -> f.service.plan("admin",AUTH,f.active,running.manifest().selection(),running.operationId(),null)).isInstanceOf(IllegalStateException.class);
        assertThat(f.prepared).hasValue(0);assertThat(f.published).hasValue(0);
    }

    @Test void authenticationRevokedDuringAnEmptyContinuationCannotReturnTheOriginalRunningPlan() {
        var f=new Fixture();var running=f.plan(f.operation,5,"c".repeat(64),StorageRecoveryCommands.Phase.RUNNING);f.current.set(running);
        f.planningHook=() -> f.authenticated=false;
        assertThatThrownBy(() -> f.service.plan("admin",AUTH,f.active,running.manifest().selection(),running.operationId(),null)).isInstanceOf(TapstateException.class);
        assertThat(f.audits).hasValue(0);assertThat(f.prepared).hasValue(0);
    }

    @Test void aFailedAuditCannotRunTheProtectedCancellationOrPrepareItsPreview() {
        var f=new Fixture();var running=f.plan(f.operation,5,"c".repeat(64),StorageRecoveryCommands.Phase.RUNNING);f.current.set(running);
        f.continuation=Optional.of(() -> {throw new AssertionError("unaudited cancellation");});
        doThrow(new TapstateException(IoError.STORE_UNAVAILABLE,Map.of("detail","unavailable"),null)).when(f.audit).record(any());
        assertThatThrownBy(() -> f.service.plan("admin",AUTH,f.active,running.manifest().selection(),running.operationId(),null))
                .isInstanceOfSatisfying(TapstateException.class,failure -> assertThat(failure.code()).isEqualTo(ControlError.AUDIT_BLOCKED));
        assertThat(f.prepared).hasValue(0);verify(f.commands,never()).plan(any(),any(),any(),any());
    }

    private static final class Fixture {
        final StorageRecoveryCommands commands=mock(StorageRecoveryCommands.class);final AuditStore audit=mock(AuditStore.class);
        final AtomicReference<StorageRecoveryCommands.Plan> current=new AtomicReference<>();final AtomicReference<Runnable> guard=new AtomicReference<>(() -> {});
        final AtomicInteger prepared=new AtomicInteger(),published=new AtomicInteger(),audits=new AtomicInteger(),authorizationChecks=new AtomicInteger();
        final String operation=UUID.randomUUID().toString();final StorageRecoveryCommands.Manifest manifest=manifest(5);
        boolean transaction,finalized,authenticated=true;int attempts=1;Runnable beforeRetry=() -> {},planningHook=() -> {};
        Function<StorageRecoveryCommands.Plan,StorageRecoveryCommands.Plan> finish=original -> {finalized=true;return original;};
        Optional<Supplier<StorageRecoveryCommands.Plan>> continuation=Optional.empty();
        final Runnable active=() -> {authorizationChecks.incrementAndGet();if(!authenticated)throw new TapstateException(ControlError.UNAUTHENTICATED,Map.of(),null);};
        final StorageRecoveryService service;
        Fixture() {
            when(commands.guardedBy(any())).thenAnswer(call -> {guard.set(call.getArgument(0));return commands;});
            when(commands.find(eq(AUTH),anyString())).thenAnswer(call -> {guard.get().run();return Optional.ofNullable(current.get()).filter(plan -> plan.operationId().equals(call.getArgument(1)));});
            when(commands.plan(any(),any(),any(),any())).thenAnswer(call -> {
                try {transaction=true;for(int i=0;i<attempts;i++){guard.get().run();if(i+1<attempts)beforeRetry.run();}}
                finally {transaction=false;}
                var next=new StorageRecoveryCommands.Plan(operation,null,AUTH,call.getArgument(1),"c".repeat(64),6,StorageRecoveryCommands.Phase.PLANNED);current.set(next);return next;
            });
            when(commands.confirm(any())).thenAnswer(call -> {
                try {transaction=true;guard.get().run();}finally {transaction=false;}
                var result=phase(call.getArgument(0),StorageRecoveryCommands.Phase.CONFIRMED);current.set(result);return result;
            });
            doAnswer(call -> {audits.incrementAndGet();return null;}).when(audit).record(any());
            var inspector=new StorageRecoveryService.Inspector() {
                @Override public StorageRecoveryCommands.Manifest inspect(CacheAuthScope scope,StorageRecoveryCommands.Selection selected){assertThat(transaction).isFalse();return manifest;}
                @Override public void requirePlanningCurrent(CacheAuthScope scope,StorageRecoveryCommands.Manifest original){assertThat(transaction).isTrue();}
                @Override public void requireCurrent(CacheAuthScope scope,StorageRecoveryCommands.Manifest original){if(!finalized || !original.equals(current.get().manifest()))throw ThreeLayerStorePort.refused();}
                @Override public void verifyExternal(CacheAuthScope scope,StorageRecoveryCommands.Manifest original){assertThat(transaction).isFalse();requireCurrent(scope,original);}
                @Override public StorageRecoveryCommands.Plan prepared(StorageRecoveryCommands.Plan original,Runnable active){assertThat(transaction).isFalse();prepared.incrementAndGet();return finish.apply(original);}
                @Override public Optional<Supplier<StorageRecoveryCommands.Plan>> replanning(StorageRecoveryCommands.Plan original,Runnable active){planningHook.run();return continuation;}
            };
            service=new StorageRecoveryService(commands,inspector,new AuditGate(audit,Clock.fixed(Instant.EPOCH,ZoneOffset.UTC)),ignored -> {published.incrementAndGet();});
        }
        StorageRecoveryCommands.Manifest manifest(long revision) {
            var business=new CacheBusinessScope("cluster",1,"cache");
            var previous=new CapturePreparation(business,CapturePreparation.Mode.SHARED_SRS,"mck-key","capture-key","mc-"+"a".repeat(64),"11111111-1111-4111-8111-111111111111");
            var pipeline=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"p"),new WorkloadOwner("node","boot"),1,1,1);
            var capture=new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,previous.captureId()),pipeline.owner(),1,0,1);
            var context=new SrsConsumerContext(new SrsConsumerKey(previous.chainId(),"p","input"),1);
            var snapshot=new SharedSrsRecoverySnapshot(previous,capture,"b".repeat(64),List.of(new SrsConsumerMembership(context,1,false,Set.of("orders"),pipeline)),
                    Map.of("p",new SharedSrsRecoverySnapshot.Pipeline(new PipelineIntentScope("cluster",1,"cache","p",1),pipeline)));
            return new StorageRecoveryCommands.Manifest(new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.SHARED_SRS,Set.of(),previous.canonicalChainId(),previous.chainId()),Set.of("p"),Map.of("p","a".repeat(64)),"b".repeat(64),revision,snapshot);
        }
        StorageRecoveryCommands.Plan plan(String id,long revision,String hash,StorageRecoveryCommands.Phase phase){return new StorageRecoveryCommands.Plan(id,null,AUTH,manifest(revision),hash,revision+1,phase);}
        StorageRecoveryCommands.Plan phase(StorageRecoveryCommands.Plan plan,StorageRecoveryCommands.Phase phase){return new StorageRecoveryCommands.Plan(plan.operationId(),plan.previousOperationId(),plan.originalScope(),plan.manifest(),plan.planHash(),plan.controlRevision(),phase);}
    }
}
