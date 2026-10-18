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
