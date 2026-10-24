package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ScopedPipelineActuationOwnershipTest {
    private static final CacheBusinessScope BUSINESS = new CacheBusinessScope("cluster-a", 3, "cache-a");
    private static final PipelineIntentScope FIRST = scope(1);
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Duration RENEW = Duration.ofSeconds(10);
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
    private final InMemoryWorkloadClaimStore raw = new InMemoryWorkloadClaimStore();
    private final WorkloadClaimStore store = mock(WorkloadClaimStore.class);
    private final AtomicReference<PipelineIntentScope> current = new AtomicReference<>(FIRST);
    private final AtomicLong nanos = new AtomicLong();
    private final AtomicInteger acquisitions = new AtomicInteger();
    private final AtomicInteger reads = new AtomicInteger();
    private final ClusterMembershipGate membership = eligibleGate();

    private PipelineActuationOwnership ownership() {
        when(store.acquirePipeline(any(), any(), anyLong(), any())).thenAnswer(call -> {
            acquisitions.incrementAndGet();
            if (!call.getArgument(0).equals(current.get())) throw fenced();
            return raw.acquire(KEY, call.getArgument(1), call.getArgument(2), call.getArgument(3));
        });
        when(store.advanceExecution(any(), anyLong(), anySet())).thenAnswer(call ->
                raw.advanceExecution(call.getArgument(0), call.getArgument(1), call.getArgument(2)));
        when(store.renew(any(), any())).thenAnswer(call -> raw.renew(call.getArgument(0), call.getArgument(1)));
        when(store.release(any())).thenAnswer(call -> raw.release(call.getArgument(0)));
        when(store.readPipeline(any())).thenAnswer(call -> {
            reads.incrementAndGet();
            if (!call.getArgument(0).equals(current.get())) throw fenced();
            return raw.read(KEY);
        });
        when(store.read(any())).thenThrow(new AssertionError("a scoped holder must not adopt metadata"));
        when(store.acquire(any(), any(), anyLong(), any())).thenThrow(new AssertionError("a scoped holder must not acquire by id"));
        return PipelineActuationOwnership.scoped(BUSINESS, OWNER, membership,
                new ClusterWorkloadClaims(store, membership), TTL, RENEW, nanos::get);
    }

    @Test void aFirstCommandCanProveOnlyItsExactZeroExecutionClaim() {
        var owner = ownership(); var permit = owner.permit(FIRST);
        var expected = WorkloadClaimFence.from(permit.claim());
        assertThat(expected.executionGeneration()).isZero();
        assertThat(owner.proveClaim(FIRST, expected)).isTrue();
        var advanced = owner.beginExecution(FIRST, expected);
        assertThat(owner.proveClaim(FIRST, expected)).isFalse();
        assertThat(owner.proveClaim(FIRST, advanced.claimFence())).isTrue();
        assertThat(owner.proveClaim(scope(2), advanced.claimFence())).isFalse();
        assertThat(owner.proveClaim(FIRST, new WorkloadClaimFence(KEY, new WorkloadOwner("node-a", "other-boot"),
                expected.claimGeneration(), expected.executionGeneration(), expected.topologyRevision()))).isFalse();
    }
    @Test void aControlCommandSurvivesIntentRetentionAndCannotOverlapAStartOrAnotherControl() {
        var owner = ownership();
        WorkloadClaimFence original;
        try (var control = owner.control(FIRST)) {
            original = control.claim(); owner.retain(Set.of()); control.requireActive();
            assertThat(owner.permit(FIRST).granted()).isFalse();
            assertThat(owner.beginExecution(FIRST, original).allowed()).isFalse();
            assertThatThrownBy(() -> owner.control(FIRST)).isInstanceOf(TapstateException.class);
        }
        assertThat(owner.permit(FIRST).granted()).isTrue();
        assertThat(owner.beginExecution(FIRST, original).allowed()).isTrue();
    }

    @Test void endingTheExactActuationTenureKeepsTheLocalControlBarrierUntilClose() {
        var owner=ownership();var permit=owner.permit(FIRST);
        var run=owner.beginExecution(FIRST,WorkloadClaimFence.from(permit.claim()));
        var original=raw.read(KEY).orElseThrow().claim();
        try(var control=owner.control(FIRST)) {
            assertThat(control.claim()).isEqualTo(run.claimFence());
            assertThat(raw.read(KEY).orElseThrow().leased()).isTrue();
            control.endTenure();control.requireHeld();
            var ended=raw.read(KEY).orElseThrow();
            assertThat(ended.leased()).isFalse();
            assertThat(WorkloadClaimFence.from(ended.claim())).isEqualTo(run.claimFence());
            assertThat(ended.claim().contextExecutionGeneration()).isEqualTo(original.contextExecutionGeneration());
            assertThat(ended.claim().executionNodeIds()).isEqualTo(original.executionNodeIds());
            assertThat(owner.permit(FIRST).granted()).isFalse();
            assertThat(owner.beginExecution(FIRST,run.claimFence()).allowed()).isFalse();
            assertThatThrownBy(() -> owner.control(FIRST)).isInstanceOf(TapstateException.class);
            control.endTenure();assertThat(raw.read(KEY).orElseThrow().claim()).isEqualTo(ended.claim());
            raw.elapse(RENEW);nanos.addAndGet(RENEW.toNanos());owner.renewDue();
            control.requireHeld();verify(store,never()).renew(any(),any());
            assertThat(raw.read(KEY).orElseThrow().claim()).isEqualTo(ended.claim());
        }
        var next=owner.permit(FIRST);
        assertThat(next.granted()).isTrue();assertThat(next.claim().claimGeneration()).isEqualTo(original.claimGeneration()+1);
        assertThat(next.claim().executionGeneration()).isEqualTo(original.executionGeneration());
        assertThat(next.claim().executionNodeIds()).isEqualTo(original.executionNodeIds());
        assertThat(owner.beginExecution(FIRST,WorkloadClaimFence.from(next.claim())).allowed()).isTrue();
        assertThat(raw.read(KEY).orElseThrow().claim().executionGeneration()).isEqualTo(original.executionGeneration()+1);
        verify(store,never()).read(any());verify(store,never()).acquire(any(),any(),anyLong(),any());
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void anEndedOriginalTupleCanBeHeldWithoutAcquiringAnotherTenure(boolean foreign) {
        var original=raw.acquire(KEY,foreign?new WorkloadOwner("old-node","old-boot"):OWNER,1,TTL).claim();
        assertThat(raw.release(original)).isTrue();var ended=raw.read(KEY).orElseThrow().claim();
        var owner=ownership();int before=acquisitions.get();
        try(var control=owner.retiredControl(FIRST,WorkloadClaimFence.from(ended))) {
            control.requireHeld();control.endTenure();owner.renewDue();owner.retain(Set.of());
            assertThat(control.claim()).isEqualTo(WorkloadClaimFence.from(ended));
            assertThat(raw.read(KEY).orElseThrow().claim()).isEqualTo(ended);
            assertThat(owner.permit(FIRST).granted()).isFalse();
            assertThat(owner.beginExecution(FIRST,control.claim()).allowed()).isFalse();
            assertThatThrownBy(control::requireActive).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> owner.enterPass(FIRST)).isInstanceOf(TapstateException.class);
            assertThat(acquisitions).hasValue(before);
            verify(store,never()).acquirePipeline(any(),any(),anyLong(),any());
            verify(store,never()).release(any());verify(store,never()).renew(any(),any());
            verify(store,never()).advanceExecution(any(),anyLong(),anySet());
        }
        assertThat(owner.permit(FIRST).claim().claimGeneration()).isEqualTo(ended.claimGeneration()+1);
    }

    @ParameterizedTest @ValueSource(strings={"live","scope","business","fence","type","resource","missing","member","control","pass","read-failed","unscoped"})
    void anUnknownOrLiveTupleCannotBecomeARetiredLocalBarrier(String fault) throws Exception {
        var original=raw.acquire(KEY,OWNER,1,TTL).claim();
        if(!fault.equals("live"))assertThat(raw.release(original)).isTrue();
        var owner=ownership();var selected=FIRST;var expected=WorkloadClaimFence.from(original);
        AutoCloseable existing=null;
        try {
            switch(fault) {
                case "scope" -> selected=scope(2);
                case "business" -> selected=new PipelineIntentScope("other-cluster",3,"cache-a","orders",1);
                case "fence" -> expected=new WorkloadClaimFence(KEY,OWNER,2,0,1);
                case "type" -> expected=new WorkloadClaimFence(new WorkloadClaimKey("cluster-a",WorkloadClaimType.CAPTURE,"orders"),OWNER,1,0,1);
                case "resource" -> expected=new WorkloadClaimFence(new WorkloadClaimKey("cluster-a",WorkloadClaimType.PIPELINE_ACTUATION,"other"),OWNER,1,0,1);
                case "missing" -> when(store.readPipeline(FIRST)).thenReturn(java.util.Optional.empty());
                case "member" -> membership.canCommit(Set.of("node-a"));
                case "control" -> existing=owner.unpreparedControl(FIRST);
                case "pass" -> existing=owner.enterPass(FIRST);
                case "read-failed" -> doThrow(fenced()).when(store).readPipeline(FIRST);
                case "unscoped" -> owner=PipelineActuationOwnership.single();
                case "live" -> { }
                default -> throw new AssertionError(fault);
            }
            var checkedOwner=owner;var checkedScope=selected;var checkedFence=expected;
            var before=raw.read(KEY).orElseThrow().claim();
            assertThatThrownBy(() -> checkedOwner.retiredControl(checkedScope,checkedFence)).isInstanceOf(TapstateException.class);
            assertThat(raw.read(KEY).orElseThrow().claim()).isEqualTo(before);
            assertThat(acquisitions).hasValue(0);
            verify(store,never()).release(any());verify(store,never()).renew(any(),any());
            if(existing==null && !fault.equals("member")) {
                if(!fault.equals("unscoped"))try(var barrier=owner.unpreparedControl(FIRST)){barrier.requireHeld();}
            }
        } finally {if(existing!=null)existing.close();}
    }

    @Test void holdingAnEndedTupleRemovesOnlyItsMatchingLocalRenewer() {
        var owner=ownership();var original=owner.permit(FIRST).claim();
        assertThat(raw.release(original)).isTrue();var ended=raw.read(KEY).orElseThrow().claim();
        try(var control=owner.retiredControl(FIRST,WorkloadClaimFence.from(ended))) {
            nanos.addAndGet(RENEW.toNanos());owner.renewDue();control.requireHeld();
            assertThat(raw.read(KEY).orElseThrow().claim()).isEqualTo(ended);
            verify(store,never()).renew(any(),any());verify(store,never()).release(any());
            assertThat(acquisitions).hasValue(1);
        }
    }

    @Test void anEndedSuccessorCannotReplaceADifferentLocallyHeldTuple() {
        var owner=ownership();var original=owner.permit(FIRST).claim();
        assertThat(raw.release(original)).isTrue();
        var successor=raw.acquire(KEY,new WorkloadOwner("other-node","other-boot"),1,TTL).claim();
        assertThat(raw.release(successor)).isTrue();var ended=raw.read(KEY).orElseThrow().claim();
        assertThatThrownBy(() -> owner.retiredControl(FIRST,WorkloadClaimFence.from(ended))).isInstanceOf(TapstateException.class);
        assertThat(raw.read(KEY).orElseThrow().claim()).isEqualTo(ended);
        assertThat(acquisitions).hasValue(1);verify(store,never()).release(any());
        try(var control=owner.unpreparedControl(FIRST)){control.requireHeld();}
    }

    @Test void anExpiredExactTenureCanEndAfterTheRenewerHasDroppedItsLocalHolder() {
        var owner=ownership();
        try(var control=owner.control(FIRST)) {
            var original=raw.read(KEY).orElseThrow().claim();
            raw.elapse(TTL);nanos.addAndGet(TTL.toNanos());owner.renewDue();
            assertThat(raw.read(KEY).orElseThrow().leased()).isFalse();
            control.endTenure();control.requireHeld();
            assertThat(raw.read(KEY).orElseThrow().claim()).isEqualTo(original);
            int readCount=reads.get();control.endTenure();assertThat(reads).hasValue(readCount);
            verify(store,never()).release(any());
            assertThat(owner.permit(FIRST).granted()).isFalse();
        }
        assertThat(owner.permit(FIRST).claim().claimGeneration()).isEqualTo(2);
    }

    @ParameterizedTest @ValueSource(strings={"scope-changed","later-owner","missing-scoped-claim","closed-control","unprepared","business-lost","release-refused","release-failed"})
    void endingATenureCannotAdoptUnknownOrChangedAuthority(String fault) {
        var owner=ownership();
        var control=fault.equals("unprepared")?owner.unpreparedControl(FIRST):owner.control(FIRST);
        try(control) {
            switch(fault) {
                case "scope-changed" -> current.set(scope(2));
                case "later-owner" -> {
                    assertThat(raw.release(raw.read(KEY).orElseThrow().claim())).isTrue();
                    assertThat(raw.acquire(KEY,new WorkloadOwner("node-b","boot-b"),7,TTL).acquired()).isTrue();
                }
                case "missing-scoped-claim" -> when(store.readPipeline(FIRST)).thenReturn(java.util.Optional.empty());
                case "closed-control" -> control.close();
                case "business-lost" -> membership.canCommit(Set.of("node-a"));
                case "release-refused" -> doReturn(false).when(store).release(any());
                case "release-failed" -> doThrow(fenced()).when(store).release(any());
                case "unprepared" -> { }
                default -> throw new AssertionError(fault);
            }
            var before=raw.read(KEY).map(io.tapstate.spi.store.WorkloadClaimReading::claim);
            assertThatThrownBy(control::endTenure).isInstanceOf(TapstateException.class);
            assertThat(raw.read(KEY).map(io.tapstate.spi.store.WorkloadClaimReading::claim)).isEqualTo(before);
            if(!fault.startsWith("release-"))verify(store,never()).release(any());
            verify(store,never()).read(any());verify(store,never()).acquire(any(),any(),anyLong(),any());
            if(fault.equals("later-owner"))assertThat(raw.read(KEY).orElseThrow().claim().owner()).isEqualTo(new WorkloadOwner("node-b","boot-b"));
        }
    }

    @Test void aLostReleaseResponseResumesTheAlreadyEndedExactTenureWithoutAnotherWrite() {
        assertUnknownEndTenureResult(true);
    }

    @Test void aFailedPostreleaseReadResumesTheAlreadyEndedExactTenureWithoutAnotherWrite() {
        assertUnknownEndTenureResult(false);
    }

    private void assertUnknownEndTenureResult(boolean loseReleaseResponse) {
        var owner=ownership();var permit=owner.permit(FIRST);
        var run=owner.beginExecution(FIRST,WorkloadClaimFence.from(permit.claim()));
        var original=raw.read(KEY).orElseThrow().claim();
        var committed=new java.util.concurrent.atomic.AtomicBoolean();var failedRead=new java.util.concurrent.atomic.AtomicBoolean();
        var failure=fenced();var releases=new AtomicInteger();
        doAnswer(call -> {
            releases.incrementAndGet();assertThat(raw.release(call.getArgument(0))).isTrue();committed.set(true);
            if(loseReleaseResponse)throw failure;return true;
        }).when(store).release(any());
        doAnswer(call -> {
            reads.incrementAndGet();if(!call.getArgument(0).equals(current.get()))throw fenced();
            if(!loseReleaseResponse && committed.get() && failedRead.compareAndSet(false,true))throw failure;
            return raw.read(KEY);
        }).when(store).readPipeline(FIRST);
        try(var control=owner.control(FIRST)) {
            assertThatThrownBy(control::endTenure).isSameAs(failure);control.requireHeld();
            var ended=raw.read(KEY).orElseThrow();assertThat(ended.leased()).isFalse();
            var expected=new io.tapstate.spi.store.WorkloadClaim(original.key(),original.owner(),original.claimGeneration(),original.executionGeneration(),
                    original.topologyRevision(),ended.claim().leaseUntil(),original.contextExecutionGeneration(),original.executionClaimGeneration(),
                    original.executionNodeIds(),original.failureClaimGeneration(),original.failureAfterMemberLoss());
            assertThat(ended.claim()).isEqualTo(expected);assertThat(control.claim()).isEqualTo(run.claimFence());
            assertThat(owner.permit(FIRST).granted()).isFalse();assertThat(owner.beginExecution(FIRST,run.claimFence()).allowed()).isFalse();
            assertThatThrownBy(() -> owner.control(FIRST)).isInstanceOf(TapstateException.class);
            control.endTenure();control.requireHeld();control.endTenure();
            assertThat(raw.read(KEY).orElseThrow().claim()).isEqualTo(ended.claim());assertThat(releases).hasValue(1);
            verify(store,times(1)).release(any());verify(store,never()).read(any());
        }
        var next=owner.permit(FIRST);assertThat(next.granted()).isTrue();
        assertThat(next.claim().claimGeneration()).isEqualTo(original.claimGeneration()+1);
        assertThat(next.claim().executionGeneration()).isEqualTo(original.executionGeneration());
        assertThat(next.claim().contextExecutionGeneration()).isEqualTo(original.contextExecutionGeneration());
        assertThat(next.claim().executionNodeIds()).isEqualTo(original.executionNodeIds());
        assertThat(releases).hasValue(1);verify(store,never()).acquire(any(),any(),anyLong(),any());
    }

    @Test void completedPositionReleaseToleratesTheRenewerAlreadyDroppingItsClaim() {
        var owner = ownership();
        try (var control = owner.control(FIRST)) {
            raw.release(raw.read(KEY).orElseThrow().claim());
            nanos.addAndGet(RENEW.toNanos()); owner.renewDue();
            control.released(); control.requireHeld();
        }
        assertThat(owner.permit(FIRST).claim().claimGeneration()).isEqualTo(2);
    }

    @Test void aScopedPermitAdvancesOnlyItsOriginalClaimAndReturnsItsExactRunIdentity() {
        var owner = ownership();
        var permit = owner.permit(FIRST);
        assertThat(permit.granted()).isTrue();
        assertThat(owner.permit(FIRST).claim()).isEqualTo(permit.claim());
        var run = owner.beginExecution(FIRST, WorkloadClaimFence.from(permit.claim()));
        assertThat(run.allowed()).isTrue();
        assertThat(run.fence().scope()).isEqualTo(FIRST);
        assertThat(run.fence().ownerBootId()).isEqualTo(OWNER.bootId());
        assertThat(run.claimFence()).isEqualTo(WorkloadClaimFence.from(raw.read(KEY).orElseThrow().claim()));
        assertThat(owner.proveExecution(run.fence())).isTrue();
        assertThat(owner.beginExecution(FIRST, WorkloadClaimFence.from(permit.claim())).allowed()).isFalse();
        assertThat(acquisitions).hasValue(1);
        verify(store, never()).read(any());
        verify(store, never()).acquire(any(), any(), anyLong(), any());
    }

    @Test void idOnlyAndUncoordinatedEntriesCannotAdmitAScopedRun() {
        var owner = ownership();
        assertThat(owner.permit("orders").granted()).isFalse();
        assertThat(owner.beginExecution("orders").allowed()).isFalse();
        assertThat(PipelineActuationOwnership.single().permit(FIRST).granted()).isFalse();
        var permit = owner.permit(FIRST);
        var claim = WorkloadClaimFence.from(permit.claim());
        assertThat(owner.beginExecution("orders", claim).allowed()).isFalse();
        var run = owner.beginExecution(FIRST, claim);
        assertThat(PipelineActuationOwnership.single().proveExecution(run.fence())).isFalse();
        var legacy = new PipelineActuationOwnership("cluster-a", OWNER, membership,
                new ClusterWorkloadClaims(store, membership), TTL, RENEW, nanos::get);
        assertThat(legacy.permit(FIRST).granted()).isFalse();
        assertThat(legacy.proveExecution(run.fence())).isFalse();
    }

    @Test void foreignBindingsAndIncarnationsAreRefusedBeforeAnyAcquisition() {
        var owner = ownership();
        for (PipelineIntentScope foreign : Set.of(new PipelineIntentScope("other", 3, "cache-a", "orders", 1),
                new PipelineIntentScope("cluster-a", 4, "cache-a", "orders", 1),
                new PipelineIntentScope("cluster-a", 3, "cache-b", "orders", 1))) {
            assertThat(owner.permit(foreign).granted()).isFalse();
        }
        assertThat(acquisitions).hasValue(0);
    }

    @Test void anUnprovedHigherScopeDoesNotDiscardTheCurrentHolderOrProbeEveryTick() {
        var owner = ownership();
        var first = owner.permit(FIRST);
        assertThat(first.granted()).isTrue();
        assertThat(owner.permit(scope(2)).granted()).isFalse();
        assertThat(owner.permit(scope(2)).granted()).isFalse();
        assertThat(acquisitions).hasValue(2);
        assertThat(owner.beginExecution(FIRST, WorkloadClaimFence.from(first.claim())).allowed()).isTrue();
    }

    @Test void aNewScopeCannotBeAdoptedByAnOldPassWithTheSamePipelineId() {
        var owner = ownership();
        var first = owner.permit(FIRST);
        var old = owner.beginExecution(FIRST, WorkloadClaimFence.from(first.claim()));
        assertThat(raw.release(raw.read(KEY).orElseThrow().claim())).isTrue();
        current.set(scope(2));
        var next = owner.permit(current.get());
        assertThat(next.granted()).isTrue();
        assertThat(next.claim().claimGeneration()).isGreaterThan(old.claimFence().claimGeneration());
        assertThat(owner.permit(FIRST).granted()).isFalse();
        assertThat(owner.beginExecution(FIRST, old.claimFence()).allowed()).isFalse();
        assertThat(owner.beginExecution(FIRST, WorkloadClaimFence.from(next.claim())).allowed()).isFalse();
        assertThat(owner.proveExecution(old.fence())).isFalse();
        var run = owner.beginExecution(current.get(), WorkloadClaimFence.from(next.claim()));
        assertThat(run.allowed()).isTrue();
        owner.startRefusedBeforeItsRun(FIRST, old.claimFence());
        assertThat(owner.proveExecution(run.fence())).isTrue();
    }

    @Test void renewalsRetainTheOriginalClaimRatherThanReacquiringAScope() {
        var owner = ownership();
        var first = owner.permit(FIRST);
        var run = owner.beginExecution(FIRST, WorkloadClaimFence.from(first.claim()));
        raw.elapse(RENEW); nanos.addAndGet(RENEW.toNanos());
        owner.renewDue();
        assertThat(owner.permit(FIRST).claim().executionGeneration()).isEqualTo(run.fence().executionGeneration());
        assertThat(acquisitions).hasValue(1);
        verify(store).renew(any(), any());
    }

    @Test void aScopedSubmissionCannotBorrowANewerTopologyAfterReacquisition() {
        var owner = ownership(); var permit = owner.permit(FIRST);
        var run = owner.beginExecution(FIRST,WorkloadClaimFence.from(permit.claim()));
        membership.install(new ClusterMembership("cluster-a",8,Set.of("node-a","node-b","node-c")));
        membership.canCommit(Set.of("node-a","node-b"));
        raw.acquire(KEY, OWNER, 8, TTL);
        raw.elapse(RENEW); nanos.addAndGet(RENEW.toNanos());
        assertThat(owner.permit(FIRST).granted()).isFalse();
        raw.elapse(RENEW); nanos.addAndGet(RENEW.toNanos());
        var reacquired = owner.permit(FIRST);
        assertThat(reacquired.claim().topologyRevision()).isEqualTo(8);
        assertThat(reacquired.claim().claimGeneration()).isEqualTo(run.claimFence().claimGeneration());
        assertThat(reacquired.claim().executionGeneration()).isEqualTo(run.claimFence().executionGeneration());
        assertThat(owner.proveExecution(run.fence())).isFalse();
    }

    @Test void anotherBootCannotProveTheSameNumericRun() {
        var owner = ownership();
        var first = owner.permit(FIRST);
        var run = owner.beginExecution(FIRST, WorkloadClaimFence.from(first.claim()));
        var wrong = new ExecutionFence("orders", run.fence().claimGeneration(), run.fence().executionGeneration(),
                FIRST, "node-a", "different-boot", run.fence().topologyRevision());
        assertThat(owner.proveExecution(wrong)).isFalse();
        assertThat(reads).hasValue(0);
    }

    @Test void aClaimReplyThatOutlivedItsOwnLeaseCannotAuthorizeSubmission() {
        var owner = ownership();
        var permit = owner.permit(FIRST);
        var run = owner.beginExecution(FIRST, WorkloadClaimFence.from(permit.claim()));
        when(store.readPipeline(FIRST)).thenAnswer(call -> {
            var captured = raw.read(KEY);
            nanos.addAndGet(TTL.plusSeconds(1).toNanos());
            raw.elapse(TTL.plusSeconds(1));
            return captured;
        });
        assertThat(owner.proveExecution(run.fence())).isFalse();
    }

    @Test void losingBusinessEligibilityDuringTheProofReadCannotAuthorizeSubmission() {
        var owner = ownership();
        var permit = owner.permit(FIRST);
        var run = owner.beginExecution(FIRST, WorkloadClaimFence.from(permit.claim()));
        when(store.readPipeline(FIRST)).thenAnswer(call -> {
            var captured = raw.read(KEY);
            membership.canCommit(Set.of("node-a"));
            return captured;
        });
        assertThat(owner.proveExecution(run.fence())).isFalse();
    }

    @Test void aShutdownDuringTheProofReadCannotAuthorizeSubmission() {
        var owner = ownership();
        var permit = owner.permit(FIRST);
        var run = owner.beginExecution(FIRST, WorkloadClaimFence.from(permit.claim()));
        when(store.readPipeline(FIRST)).thenAnswer(call -> {
            var captured = raw.read(KEY);
            owner.stopForShutdown();
            return captured;
        });
        assertThat(owner.proveExecution(run.fence())).isFalse();
    }

    @Test void anIdOnlyRefusalCannotAlterAScopedRunsAttribution() {
        var owner = ownership();
        var permit = owner.permit(FIRST);
        var run = owner.beginExecution(FIRST, WorkloadClaimFence.from(permit.claim()));
        owner.startRefusedBeforeItsRun("orders");
        owner.startRefusedBeforeItsRun("orders", run.claimFence());
        owner.recordFailure("orders", TTL.toNanos(), TTL.toNanos());
        assertThat(owner.departure("orders", TTL.toNanos())).isEqualTo(PipelineActuationOwnership.Departure.NOBODY_LEFT);
        verify(store, never()).recordExecutionFailure(any(), org.mockito.ArgumentMatchers.anyBoolean());
        owner.startRefusedBeforeItsRun(FIRST, run.claimFence());
        assertThat(owner.departure("orders", TTL.toNanos())).isEqualTo(PipelineActuationOwnership.Departure.START_REFUSED);
    }

    @Test void aProofCannotOutliveAPhysicalScopeReplacementKnownToThisHolder() {
        var owner = ownership();
        var permit = owner.permit(FIRST);
        var old = owner.beginExecution(FIRST, WorkloadClaimFence.from(permit.claim()));
        when(store.readPipeline(FIRST)).thenAnswer(call -> {
            var captured = raw.read(KEY);
            assertThat(raw.release(raw.read(KEY).orElseThrow().claim())).isTrue();
            current.set(scope(2));
            assertThat(owner.permit(current.get()).granted()).isTrue();
            return captured;
        });
        assertThat(owner.proveExecution(old.fence())).isFalse();
        var next = owner.permit(current.get());
        assertThat(owner.beginExecution(current.get(), WorkloadClaimFence.from(next.claim())).allowed()).isTrue();
    }

    @Test void aProofCannotOutliveAnExecutionAdvanceKnownToThisHolder() {
        var owner = ownership();
        var permit = owner.permit(FIRST);
        var old = owner.beginExecution(FIRST, WorkloadClaimFence.from(permit.claim()));
        AtomicReference<PipelineActuationOwnership.Execution> next = new AtomicReference<>();
        when(store.readPipeline(FIRST)).thenAnswer(call -> {
            var captured = raw.read(KEY);
            next.set(owner.beginExecution(FIRST, old.claimFence()));
            assertThat(next.get().allowed()).isTrue();
            return captured;
        });
        assertThat(owner.proveExecution(old.fence())).isFalse();
        assertThat(next.get().fence().executionGeneration()).isGreaterThan(old.fence().executionGeneration());
    }

    @Test void alternatingFailedCandidatesCannotBypassTheProbeInterval() {
        var owner = ownership();
        var permit = owner.permit(FIRST);
        for (int i = 0; i < 3; i++) {
            assertThat(owner.permit(scope(2)).granted()).isFalse();
            assertThat(owner.permit(scope(3)).granted()).isFalse();
        }
        assertThat(acquisitions).hasValue(3);
        var old = owner.beginExecution(FIRST, WorkloadClaimFence.from(permit.claim()));
        assertThat(old.allowed()).isTrue();
        assertThat(raw.release(raw.read(KEY).orElseThrow().claim())).isTrue();
        current.set(scope(4));
        assertThat(owner.permit(current.get()).granted()).as("a newly observed higher candidate gets its first probe").isTrue();
    }

    @Test void aLeaseOnlyRenewalDuringTheProofReadDoesNotInventAnotherRun() {
        var owner = ownership();
        var permit = owner.permit(FIRST);
        var run = owner.beginExecution(FIRST, WorkloadClaimFence.from(permit.claim()));
        when(store.readPipeline(FIRST)).thenAnswer(call -> {
            var captured = raw.read(KEY);
            nanos.addAndGet(RENEW.toNanos()); raw.elapse(RENEW);
            owner.renewDue();
            return captured;
        });
        assertThat(owner.proveExecution(run.fence())).isTrue();
    }

    @Test void aLateProofReadCannotAdoptTheReacquiredHoldersNewTopology() {
        var owner = ownership(); var permit = owner.permit(FIRST);
        var run = owner.beginExecution(FIRST,WorkloadClaimFence.from(permit.claim())); var once = new java.util.concurrent.atomic.AtomicBoolean();
        when(store.readPipeline(FIRST)).thenAnswer(call -> {
            var captured = raw.read(KEY);
            if (once.compareAndSet(false,true)) {
                membership.install(new ClusterMembership("cluster-a",8,Set.of("node-a","node-b","node-c")));
                membership.canCommit(Set.of("node-a","node-b"));
                raw.acquire(KEY, OWNER, 8, TTL);
                raw.elapse(RENEW); nanos.addAndGet(RENEW.toNanos()); assertThat(owner.permit(FIRST).granted()).isFalse();
                raw.elapse(RENEW); nanos.addAndGet(RENEW.toNanos());
                assertThat(owner.permit(FIRST).claim().topologyRevision()).isEqualTo(8);
            }
            return captured;
        });
        assertThat(owner.proveExecution(run.fence())).isFalse();
    }

    @Test void aScopedExecutionCannotCarryAnotherTopologyOfTheSameNumericRun() {
        var owner = ownership(); var permit = owner.permit(FIRST);
        var run = owner.beginExecution(FIRST,WorkloadClaimFence.from(permit.claim()));
        var changed = new WorkloadClaimFence(KEY,OWNER,run.claimFence().claimGeneration(),run.claimFence().executionGeneration(),8);
        assertThatThrownBy(() -> new PipelineActuationOwnership.Execution(true,run.fence(),8L,changed)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void aJoinRenewsTheRunningScopedClaimWithoutChangingItsFence() {
        var owner = ownership(); var first = owner.permit(FIRST);
        var run = owner.beginExecution(FIRST, WorkloadClaimFence.from(first.claim()));
        membership.install(new ClusterMembership("cluster-a", 8, Set.of("node-a", "node-b", "node-c")));
        membership.canCommit(Set.of("node-a", "node-b", "node-c"));
        raw.elapse(RENEW); nanos.addAndGet(RENEW.toNanos()); owner.renewDue();
        assertThat(owner.permit(FIRST).granted()).isTrue();
        assertThat(WorkloadClaimFence.from(owner.permit(FIRST).claim())).isEqualTo(run.claimFence());
        assertThat(owner.proveExecution(run.fence())).isTrue();
        assertThat(acquisitions).hasValue(1);
        var next = owner.beginExecution(FIRST, run.claimFence());
        assertThat(next.allowed()).isTrue();
        assertThat(next.claimFence().topologyRevision()).isEqualTo(8);
        assertThat(next.claimFence().claimGeneration()).isEqualTo(run.claimFence().claimGeneration());
        assertThat(next.claimFence().executionGeneration()).isEqualTo(run.claimFence().executionGeneration() + 1);
        assertThat(owner.proveExecution(run.fence())).isFalse();
        assertThat(owner.proveExecution(next.fence())).isTrue();
    }

    @Test void aRunningScopedClaimStillNeedsTheCommittedMajorityToRenew() {
        var owner = ownership(); var first = owner.permit(FIRST);
        owner.beginExecution(FIRST, WorkloadClaimFence.from(first.claim()));
        membership.install(new ClusterMembership("cluster-a", 8, Set.of("node-a", "node-b", "node-c")));
        membership.canCommit(Set.of("node-a"));
        raw.elapse(RENEW); nanos.addAndGet(RENEW.toNanos()); owner.renewDue();
        assertThat(owner.permit(FIRST).granted()).isFalse();
        verify(store, never()).renew(any(), any());
    }

    @Test void aJoinRacingTheExplicitTopologyRebaseCannotSubmitAnExecution() {
        var owner = ownership(); var first = owner.permit(FIRST);
        var run = owner.beginExecution(FIRST, WorkloadClaimFence.from(first.claim()));
        membership.install(new ClusterMembership("cluster-a", 8, Set.of("node-a", "node-b", "node-c")));
        membership.canCommit(Set.of("node-a", "node-b", "node-c"));
        when(store.acquirePipeline(FIRST, OWNER, 8, TTL)).thenAnswer(call -> {
            var rebased = raw.acquire(KEY, OWNER, 8, TTL);
            membership.install(new ClusterMembership("cluster-a", 9, Set.of("node-a", "node-b", "node-c", "node-d")));
            membership.canCommit(Set.of("node-a", "node-b", "node-c", "node-d"));
            return rebased;
        });
        assertThat(owner.beginExecution(FIRST, run.claimFence()).allowed()).isFalse();
        assertThat(raw.read(KEY).orElseThrow().claim().executionGeneration()).isEqualTo(run.fence().executionGeneration());
        assertThat(owner.proveExecution(run.fence())).isFalse();
    }

    @Test void aTopologyRebaseCannotAdoptANewTenureAfterTheOriginalLeaseExpires() {
        var owner = ownership(); var first = owner.permit(FIRST);
        var run = owner.beginExecution(FIRST, WorkloadClaimFence.from(first.claim()));
        membership.install(new ClusterMembership("cluster-a", 8, Set.of("node-a", "node-b", "node-c")));
        membership.canCommit(Set.of("node-a", "node-b", "node-c"));
        raw.elapse(TTL);
        assertThat(owner.beginExecution(FIRST, run.claimFence()).allowed()).isFalse();
        assertThat(raw.read(KEY).orElseThrow().claim().executionGeneration()).isEqualTo(run.fence().executionGeneration());
    }

    @Test void aLaggedMembershipCannotRebaseTheDurableClaimBackwards() {
        var owner = ownership(); var first = owner.permit(FIRST);
        var run = owner.beginExecution(FIRST, WorkloadClaimFence.from(first.claim()));
        membership.install(new ClusterMembership("cluster-a", 6, Set.of("node-a", "node-b", "node-c")));
        membership.canCommit(Set.of("node-a", "node-b", "node-c"));
        assertThat(owner.beginExecution(FIRST, run.claimFence()).allowed()).isFalse();
        assertThat(WorkloadClaimFence.from(raw.read(KEY).orElseThrow().claim())).isEqualTo(run.claimFence());
        assertThat(acquisitions).hasValue(1);
    }

    @Test void aScopedExecutionCannotCarryAnotherClaimsOwner() {
        var owner = ownership();
        var permit = owner.permit(FIRST);
        var run = owner.beginExecution(FIRST, WorkloadClaimFence.from(permit.claim()));
        var wrong = new WorkloadClaimFence(KEY, new WorkloadOwner("node-b", "boot-b"),
                run.claimFence().claimGeneration(), run.claimFence().executionGeneration(), 7);
        assertThatThrownBy(() -> new PipelineActuationOwnership.Execution(true, run.fence(), 7L, wrong))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static PipelineIntentScope scope(long generation) {
        return new PipelineIntentScope("cluster-a", 3, "cache-a", "orders", generation);
    }

    private static ClusterMembershipGate eligibleGate() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        ClusterMembershipGate gate = new ClusterMembershipGate(properties);
        gate.install(new ClusterMembership("cluster-a", 7, Set.of("node-a", "node-b", "node-c")));
        gate.canCommit(Set.of("node-a", "node-b"));
        return gate;
    }

    private static TapstateException fenced() {
        return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
    }
}
