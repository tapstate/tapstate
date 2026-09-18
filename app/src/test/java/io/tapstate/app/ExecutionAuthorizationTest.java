package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a member is allowed to send outside the cluster on a run's behalf, answered from that member's own
 * reading of the run's generations rather than from a store round trip per batch.
 */
class ExecutionAuthorizationTest {

    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Duration RENEW = Duration.ofSeconds(10);
    private static final Duration WINDOW = Duration.ofSeconds(10);
    private static final WorkloadOwner NODE_A = new WorkloadOwner("node-a", "boot-a");
    private static final WorkloadOwner NODE_B = new WorkloadOwner("node-b", "boot-b");

    private final InMemoryWorkloadClaimStore claims = new InMemoryWorkloadClaimStore();
    private final ClusterMembershipGate membership = eligibleGate();
    private final AtomicLong nanos = new AtomicLong();

    @Test
    void everySubmissionTakesTheNextExecutionGenerationEvenWhenNobodyElseTookTheClaim() {
        PipelineActuationOwnership nodeA = ownership(NODE_A);
        assertThat(nodeA.permit("orders").granted()).isTrue();

        ExecutionFence first = nodeA.beginExecution("orders").fence();
        ExecutionFence second = nodeA.beginExecution("orders").fence();

        assertThat(first.executionGeneration()).isEqualTo(1);
        assertThat(second.executionGeneration())
                .as("the same holder resubmitting is still a different run").isEqualTo(2);
        assertThat(second.claimGeneration())
                .as("and it is not a change of ownership").isEqualTo(first.claimGeneration());
    }

    @Test
    void aMemberThatNoLongerHoldsThePipelineIsRefusedARunToSubmit() {
        PipelineActuationOwnership nodeA = ownership(NODE_A);
        assertThat(nodeA.permit("orders").granted()).isTrue();

        // The lease ran out and the next member took it over.
        claims.elapse(TTL.plusSeconds(1));
        assertThat(ownership(NODE_B).permit("orders").granted()).isTrue();

        PipelineActuationOwnership.Execution refused = nodeA.beginExecution("orders");

        assertThat(refused.allowed()).isFalse();
        assertThat(refused.fence()).isNull();
    }

    @Test
    void aRunWhoseGenerationsAreStillCurrentKeepsWritingAndAcknowledging() {
        ExecutionFence fence = submittedRun();
        CountingWriter target = new CountingWriter();
        AtomicInteger acked = new AtomicInteger();
        ExecutionAuthorization guard = guard(claims);

        SinkWriter writer = FencedSinkWriterFactory.guarded(target, fence, guard);
        SinkAck ack = FencedSinkAckFactory.guarded((chain, position) -> acked.incrementAndGet(), fence, guard);
        writer.write(List.of());
        writer.write(List.of());
        ack.advance("orders", new ChainPosition(new SourceOrder(1, 1), "w1"));

        assertThat(target.batches).hasValue(2);
        assertThat(acked).hasValue(1);
    }

    @Test
    void aSupersededRunStopsWritingAndAcknowledgingOnceItsOwnWindowIsOver() {
        ExecutionFence fence = submittedRun();
        CountingWriter target = new CountingWriter();
        AtomicInteger acked = new AtomicInteger();
        ExecutionAuthorization guard = guard(claims);
        SinkWriter writer = FencedSinkWriterFactory.guarded(target, fence, guard);
        SinkAck ack = FencedSinkAckFactory.guarded((chain, position) -> acked.incrementAndGet(), fence, guard);
        writer.write(List.of());

        // The pipeline changes hands and the next owner submits its own run, so both generations move.
        claims.elapse(TTL.plusSeconds(1));
        PipelineActuationOwnership nodeB = ownership(NODE_B);
        assertThat(nodeB.permit("orders").granted()).isTrue();
        assertThat(nodeB.beginExecution("orders").allowed()).isTrue();
        int writtenBefore = target.batches.get();
        int ackedBefore = acked.get();

        // Still inside this member's window, it is acting on the reading it last took.
        writer.write(List.of());
        assertThat(target.batches.get() - writtenBefore)
                .as("the local window is what bounds this, and it has not closed yet").isEqualTo(1);

        nanos.addAndGet(WINDOW.toNanos());

        assertThatThrownBy(() -> writer.write(List.of()))
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).code())
                .isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED);
        assertThatThrownBy(() -> ack.advance("orders", new ChainPosition(new SourceOrder(1, 2), "w2")))
                .isInstanceOf(TapstateException.class);
        assertThat(target.batches.get() - writtenBefore - 1)
                .as("no batch of a superseded run is sent after its local deadline").isZero();
        assertThat(acked.get() - ackedBefore)
                .as("and no durable position is advanced by it either").isZero();
    }

    @Test
    void aRunWhoseOwnerStoppedRenewingStopsAtTheLeaseEvenThoughNobodyHasTakenItOver() {
        ExecutionFence fence = submittedRun();
        CountingWriter target = new CountingWriter();
        AtomicInteger acked = new AtomicInteger();
        ExecutionAuthorization guard = guard(claims);
        SinkWriter writer = FencedSinkWriterFactory.guarded(target, fence, guard);
        SinkAck ack = FencedSinkAckFactory.guarded((chain, position) -> acked.incrementAndGet(), fence, guard);
        writer.write(List.of());

        // The member holding the pipeline goes away. It renews nothing, and no other member has yet come
        // for the claim -- so the record it left behind still carries the very generations this run was
        // submitted with, and goes on carrying them for as long as nobody takes it over.
        elapse(TTL.plusSeconds(1));
        assertThat(claims.read(new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders"))
                .orElseThrow())
                .as("the generations this member is checking against have not moved at all")
                .satisfies(reading -> {
                    assertThat(reading.claim().claimGeneration()).isEqualTo(fence.claimGeneration());
                    assertThat(reading.claim().executionGeneration()).isEqualTo(fence.executionGeneration());
                    assertThat(reading.leased()).isFalse();
                });
        int writtenBefore = target.batches.get();

        assertThatThrownBy(() -> writer.write(List.of()))
                .as("an expired lease is nobody's run, whoever ends up holding it next")
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).code())
                .isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED);
        assertThatThrownBy(() -> ack.advance("orders", new ChainPosition(new SourceOrder(1, 2), "w2")))
                .isInstanceOf(TapstateException.class);

        // Far past any refresh window: without the lease bounding it, each refresh would keep finding the
        // same matching generations and hand out another window, indefinitely.
        elapse(WINDOW.multipliedBy(20));
        assertThatThrownBy(() -> writer.write(List.of())).isInstanceOf(TapstateException.class);
        assertThat(target.batches.get() - writtenBefore).isZero();
        assertThat(acked).hasValue(0);
    }

    @Test
    void aMemberThatCannotRefreshStopsAtItsDeadlineRatherThanCarryingOn() {
        ExecutionFence fence = submittedRun();
        CountingWriter target = new CountingWriter();
        UnreachableAfterFirstRead store = new UnreachableAfterFirstRead(claims);
        ExecutionAuthorization guard = guard(store);
        SinkWriter writer = FencedSinkWriterFactory.guarded(target, fence, guard);

        writer.write(List.of());
        assertThat(target.batches).as("the first reading was answered").hasValue(1);

        nanos.addAndGet(WINDOW.toNanos());

        assertThatThrownBy(() -> writer.write(List.of()))
                .as("an unreachable store is not permission to keep writing")
                .isInstanceOf(TapstateException.class);
        assertThat(target.batches).hasValue(1);
        assertThat(store.reads.get())
                .as("and the refused batches do not each turn into a store round trip")
                .isLessThanOrEqualTo(2);
    }

    /** Moves both clocks together, the way waiting moves the store's lease and this member's deadline. */
    private void elapse(Duration elapsed) {
        claims.elapse(elapsed);
        nanos.addAndGet(elapsed.toNanos());
    }

    /** A run of `orders` submitted by node A, fenced by the generations the store handed it. */
    private ExecutionFence submittedRun() {
        PipelineActuationOwnership nodeA = ownership(NODE_A);
        assertThat(nodeA.permit("orders").granted()).isTrue();
        PipelineActuationOwnership.Execution execution = nodeA.beginExecution("orders");
        assertThat(execution.allowed()).isTrue();
        return execution.fence();
    }

    private ExecutionAuthorization guard(WorkloadClaimStore store) {
        return new ExecutionAuthorization("cluster-a", store, WINDOW, nanos::get);
    }

    private PipelineActuationOwnership ownership(WorkloadOwner owner) {
        return new PipelineActuationOwnership(
                "cluster-a", owner, membership, new ClusterWorkloadClaims(claims, membership), TTL, RENEW,
                nanos::get);
    }

    private static ClusterMembershipGate eligibleGate() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        ClusterMembershipGate gate = new ClusterMembershipGate(properties);
        gate.install(new ClusterMembership("cluster-a", 7, Set.of("node-a", "node-b", "node-c")));
        gate.canCommit(Set.of("node-a", "node-b"));
        return gate;
    }

    private static final class CountingWriter implements SinkWriter {

        private final AtomicInteger batches = new AtomicInteger();

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            batches.incrementAndGet();
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }

    /** A store that answers once and is unreachable after that, the way a partitioned member sees it. */
    private static final class UnreachableAfterFirstRead implements WorkloadClaimStore {

        private final WorkloadClaimStore delegate;
        private final AtomicInteger reads = new AtomicInteger();

        private UnreachableAfterFirstRead(WorkloadClaimStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<WorkloadClaimReading> read(WorkloadClaimKey key) {
            if (reads.incrementAndGet() > 1) {
                throw new IllegalStateException("coordination store unreachable");
            }
            return delegate.read(key);
        }

        @Override
        public WorkloadClaimAttempt acquire(
                WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
            return delegate.acquire(key, owner, topologyRevision, ttl);
        }

        @Override
        public Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
            return delegate.renew(expected, ttl);
        }

        @Override
        public boolean release(WorkloadClaim expected) {
            return delegate.release(expected);
        }

        @Override
        public Optional<WorkloadClaim> advanceExecution(WorkloadClaim expected, long topologyRevision) {
            return delegate.advanceExecution(expected, topologyRevision);
        }
    }
}
