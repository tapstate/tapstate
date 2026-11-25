package io.tapstate.app;

import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterWorkloadClaimsTest {

    @Test void deadlineCapabilityReceivesTheOriginalSharedAdmissionDeadlineAndCleanupBudget() {
        var gate=productionGate();gate.install(new ClusterMembership("cluster-a",1,Set.of("a","b","c")));
        gate.canCommit(Set.of("a","b"));
        var raw=new RecordingClaims();
        var store=org.mockito.Mockito.mock(io.tapstate.spi.store.DeadlineWorkloadClaimStore.class,
                org.mockito.AdditionalAnswers.delegatesTo(raw));
        var deadlines=new java.util.ArrayList<Long>();
        org.mockito.Mockito.doAnswer(call->{deadlines.add(call.getArgument(0));
            ((Runnable)call.getArgument(1)).run();
            return ((java.util.function.Supplier<?>)call.getArgument(2)).get();
        }).when(store).withDeadline(org.mockito.ArgumentMatchers.anyLong(),org.mockito.ArgumentMatchers.any(),org.mockito.ArgumentMatchers.any());
        org.mockito.Mockito.doNothing().when(store).awaitQuiescence(org.mockito.ArgumentMatchers.anyLong());
        var port=org.mockito.Mockito.mock(ThreeLayerStorePort.class);
        org.mockito.Mockito.when(port.workloadClaims()).thenReturn(store);
        var owner=org.mockito.Mockito.mock(NativeBusinessContext.class);
        org.springframework.test.util.ReflectionTestUtils.setField(owner,"port",port);
        var observed=new NativeBusinessContext.Wiring().workloadClaimStore(owner);
        assertThat(observed).isInstanceOf(io.tapstate.spi.store.DeadlineWorkloadClaimStore.class);
        var admission=new AdmissionDrain();var claims=new ClusterWorkloadClaims(observed,gate).draining(admission);
        assertThat(admission.enter()).isTrue();long original=admission.deadlineNanos();
        var key=new WorkloadClaimKey("cluster-a",WorkloadClaimType.CAPTURE,"capture");
        try {assertThat(claims.acquire(key,OWNER,1,TTL)).isPresent();claims.read(key);}
        finally {admission.leave();}
        assertThat(deadlines).containsExactly(original,original);
        long shutdown=System.nanoTime()+Duration.ofSeconds(1).toNanos();claims.awaitStopped(shutdown);
        org.mockito.Mockito.verify(store).awaitQuiescence(shutdown);
        admission.stop();
        org.assertj.core.api.Assertions.assertThatThrownBy(()->claims.acquire(key,OWNER,1,TTL))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class);
        assertThat(deadlines).hasSize(2);
    }

    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-1");
    @Test void aScalarObservationDecoratorDoesNotAdvertiseUnsupportedDeadlineOwnership() {
        assertThat(ObservedWorkloadClaims.observe(new RecordingClaims(),(write,failure)->{}))
                .isNotInstanceOf(io.tapstate.spi.store.DeadlineWorkloadClaimStore.class);
    }
    private static final Duration TTL = Duration.ofSeconds(30);

    @ParameterizedTest
    @EnumSource(value = WorkloadClaimType.class, names = {"PIPELINE_ACTUATION", "VALIDATION"})
    void businessAcquireAndRenewNeverReachTheStoreWithoutTheCommittedMajority(WorkloadClaimType type) {
        ClusterMembershipGate gate = productionGate();
        gate.install(new ClusterMembership("cluster-a", 1, Set.of("a", "b", "c", "d")));
        gate.canCommit(Set.of("a", "b"));
        RecordingClaims raw = new RecordingClaims();
        ClusterWorkloadClaims claims = new ClusterWorkloadClaims(raw, gate);
        WorkloadClaimKey key =
                new WorkloadClaimKey("cluster-a", type, "orders");
        WorkloadClaim expected = raw.claim(key);

        assertThat(claims.acquire(key, OWNER, 1, TTL)).isEmpty();
        assertThat(claims.renew(expected, TTL)).isEmpty();
        assertThat(raw.calls).hasValue(0);

        gate.canCommit(Set.of("a", "b", "c"));
        assertThat(claims.acquire(key, OWNER, 1, TTL)).isPresent();
        assertThat(claims.renew(expected, TTL)).isPresent();
        assertThat(raw.calls).hasValue(2);
    }

    @Test
    void preJoinNodeSessionDoesNotDependOnAMembershipThatCannotExistYet() {
        ClusterMembershipGate gate = unboundProductionGate();
        RecordingClaims raw = new RecordingClaims();
        ClusterWorkloadClaims claims = new ClusterWorkloadClaims(raw, gate);
        WorkloadClaimKey key =
                new WorkloadClaimKey("cluster-a", WorkloadClaimType.NODE_SESSION, "node-a");

        assertThat(claims.acquire(key, OWNER, 0, TTL)).isPresent();
        assertThat(raw.calls).hasValue(1);
    }

    @ParameterizedTest
    @EnumSource(value = WorkloadClaimType.class, names = {"PIPELINE_ACTUATION", "VALIDATION"})
    void aTopologyRevisionChangeFencesRenewalUntilTheWorkloadIsReacquired(WorkloadClaimType type) {
        ClusterMembershipGate gate = productionGate();
        gate.install(new ClusterMembership("cluster-a", 1, Set.of("a", "b", "c")));
        gate.canCommit(Set.of("a", "b"));
        RecordingClaims raw = new RecordingClaims();
        ClusterWorkloadClaims claims = new ClusterWorkloadClaims(raw, gate);
        WorkloadClaimKey key =
                new WorkloadClaimKey("cluster-a", type, "orders");
        WorkloadClaim revisionOne = raw.claim(key);

        gate.install(new ClusterMembership("cluster-a", 2, Set.of("a", "b", "c")));
        gate.canCommit(Set.of("a", "b"));

        assertThat(claims.renew(revisionOne, TTL)).isEmpty();
        assertThat(raw.calls).hasValue(0);
    }

    /**
     * A member joining moves the committed revision on and takes nothing from a source already being
     * tailed, so the capture claims held across it go on renewing. Fencing them on the revision stopped
     * every tail in the cluster within one renewal of a member joining, and the pipelines reading those
     * tails were left failed: a holder has no way to take a capture claim again short of stopping the
     * tail it guards.
     */
    @Test
    void aMemberJoiningLeavesTheCaptureClaimsAlreadyHeldRenewable() {
        ClusterMembershipGate gate = productionGate();
        gate.install(new ClusterMembership("cluster-a", 1, Set.of("a", "b", "c")));
        gate.canCommit(Set.of("a", "b", "c"));
        RecordingClaims raw = new RecordingClaims();
        ClusterWorkloadClaims claims = new ClusterWorkloadClaims(raw, gate);
        WorkloadClaimKey key = new WorkloadClaimKey("cluster-a", WorkloadClaimType.CAPTURE, "capture-a");
        WorkloadClaim heldAtRevisionOne = raw.claim(key);

        gate.install(new ClusterMembership("cluster-a", 2, Set.of("a", "b", "c", "d")));
        gate.canCommit(Set.of("a", "b", "c", "d"));

        assertThat(claims.renew(heldAtRevisionOne, TTL))
                .as("a capture claim held before the join is renewed after it")
                .isPresent();
        assertThat(raw.calls).hasValue(1);
    }

    /** The exemption is from the revision only: a capture claim still needs the committed majority. */
    @Test
    void aCaptureClaimIsNotRenewedWithoutTheCommittedMajority() {
        ClusterMembershipGate gate = productionGate();
        gate.install(new ClusterMembership("cluster-a", 1, Set.of("a", "b", "c")));
        gate.canCommit(Set.of("a"));
        RecordingClaims raw = new RecordingClaims();
        ClusterWorkloadClaims claims = new ClusterWorkloadClaims(raw, gate);
        WorkloadClaimKey key = new WorkloadClaimKey("cluster-a", WorkloadClaimType.CAPTURE, "capture-a");

        assertThat(claims.renew(raw.claim(key), TTL)).isEmpty();
        assertThat(raw.calls).hasValue(0);
    }

    @Test
    void aFailureIsNotRecordedAfterTheCommittedMajorityIsLost() {
        ClusterMembershipGate gate = productionGate();
        gate.install(new ClusterMembership("cluster-a", 1, Set.of("a", "b", "c")));
        gate.canCommit(Set.of("a"));
        RecordingClaims raw = new RecordingClaims();
        ClusterWorkloadClaims claims = new ClusterWorkloadClaims(raw, gate);
        WorkloadClaimKey key =
                new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
        WorkloadClaim running = new WorkloadClaim(key, OWNER, 1, 1, 1, Instant.now().plus(TTL),
                1, 1, Set.of("a", "b", "c"), 0, false);

        assertThat(claims.recordExecutionFailure(running, true)).isEmpty();
        assertThat(raw.calls).hasValue(0);

        gate.canCommit(Set.of("a", "b"));
        assertThat(claims.recordExecutionFailure(running, true)).contains(running);
        assertThat(raw.calls).hasValue(1);
    }

    @Test
    void stoppedSingleControlRenewalCannotGrantBusinessOrOutliveItsOriginalNode() {
        var gate = new ClusterMembershipGate(new ClusterProperties());
        var node = new WorkloadClaim(new WorkloadClaimKey("node-a", WorkloadClaimType.NODE_SESSION, "node-a"),
                OWNER, 1, 0, 0, Instant.now().plus(TTL));
        var active = new java.util.concurrent.atomic.AtomicBoolean(true);
        gate.bindSingle(node); gate.observeNodeSession(active::get); gate.observeDataPlane(() -> false);
        var raw = new RecordingClaims(); var claims = new ClusterWorkloadClaims(raw, gate);
        var pipeline = raw.claim(new WorkloadClaimKey("node-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders"));
        assertThat(claims.renewActuationControl(pipeline, TTL)).contains(pipeline);
        assertThat(claims.renew(pipeline, TTL)).isEmpty();
        assertThat(claims.acquire(pipeline.key(), OWNER, 1, TTL)).isEmpty();
        assertThat(claims.advanceExecution(pipeline, 1, Set.of("node-a"))).isEmpty();
        assertThat(claims.read(pipeline.key())).isEmpty();
        assertThat(claims.renewActuationControl(raw.claim(new WorkloadClaimKey("node-a", WorkloadClaimType.CAPTURE, "tail")), TTL)).isEmpty();
        assertThat(claims.renewActuationControl(raw.claim(new WorkloadClaimKey("another", WorkloadClaimType.PIPELINE_ACTUATION, "orders")), TTL)).isEmpty();
        assertThat(claims.renewActuationControl(new WorkloadClaim(pipeline.key(), new WorkloadOwner("node-a", "old-boot"),
                1, 0, 1, Instant.now().plus(TTL)), TTL)).isEmpty();
        assertThat(raw.calls).hasValue(1);
        active.set(false);
        assertThat(claims.renewActuationControl(pipeline, TTL)).isEmpty();
        assertThat(raw.calls).hasValue(1);
    }

    private static ClusterMembershipGate productionGate() {
        var gate = unboundProductionGate();
        gate.observeNodeSession(() -> true);
        return gate;
    }

    private static ClusterMembershipGate unboundProductionGate() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        return new ClusterMembershipGate(properties);
    }

    private static final class RecordingClaims implements WorkloadClaimStore {
        private final AtomicInteger calls = new AtomicInteger();

        private WorkloadClaim claim(WorkloadClaimKey key) {
            return new WorkloadClaim(key, OWNER, 1, 0, 1, Instant.now().plus(TTL));
        }

        @Override
        public WorkloadClaimAttempt acquire(
                WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
            calls.incrementAndGet();
            return WorkloadClaimAttempt.acquired(claim(key));
        }

        @Override
        public Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
            calls.incrementAndGet();
            return Optional.of(expected);
        }

        @Override
        public boolean release(WorkloadClaim expected) {
            calls.incrementAndGet();
            return true;
        }

        @Override
        public Optional<WorkloadClaim> advanceExecution(
                WorkloadClaim expected, long topologyRevision, Set<String> executionNodeIds) {
            calls.incrementAndGet();
            return Optional.of(expected);
        }

        @Override
        public Optional<WorkloadClaim> recordExecutionFailure(
                WorkloadClaim expected, boolean afterMemberLoss) {
            calls.incrementAndGet();
            return Optional.of(expected);
        }

        @Override
        public Optional<WorkloadClaimReading> read(WorkloadClaimKey key) {
            return Optional.of(new WorkloadClaimReading(claim(key), TTL));
        }
    }
}
