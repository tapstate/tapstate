package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A member's own clock says when it next asks the coordination store, and nothing more. Whether a lease
 * has run out is arithmetic the store does on its own clock; how long a member may go on acting on the
 * answer it last got is monotonic elapsed since it asked for that answer.
 *
 * <p>Every case here runs with this member's wall clock deliberately minutes away from the store's --
 * ahead of it in one, behind it in another -- because that distance is the only thing that tells the two
 * implementations apart. A member comparing the deadline the store handed it against its own
 * {@code Instant.now()} would take a claim that is still somebody else's in the first case, and go on
 * writing for a run nobody owns any more in the second; with the two clocks agreeing, as they do on one
 * developer machine, it would look exactly like the right implementation.
 *
 * <p>Hosts drift and operators move clocks. A cluster that needs every member's clock to agree with the
 * store's has a way to lose data that nothing in it can detect, so none of these answers may be reached
 * through a wall clock -- neither by reading one directly, nor by subtracting one from a deadline that
 * was written by a different clock.
 */
class NodeWallClockCannotExtendAWorkloadClaimTest {

    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Duration RENEW = Duration.ofSeconds(10);
    private static final Duration WINDOW = Duration.ofSeconds(10);
    /** Far enough out that a lease compared against the wrong clock is never a near miss. */
    private static final Duration SKEW = Duration.ofMinutes(10);
    private static final WorkloadOwner NODE_A = new WorkloadOwner("node-a", "boot-a");
    private static final WorkloadOwner NODE_B = new WorkloadOwner("node-b", "boot-b");

    private final InMemoryWorkloadClaimStore claims = new InMemoryWorkloadClaimStore();
    private final ClusterMembershipGate membership = eligibleGate();
    private final AtomicLong nanos = new AtomicLong();

    @Test
    void aMemberWhoseClockRunsAheadCannotTakeAClaimTheStoreStillSaysIsHeld() {
        thisMembersWallClockRunsAheadOfTheStoreBy(SKEW);
        PipelineActuationOwnership nodeA = ownership(NODE_A);
        assertThat(nodeA.permit("orders").granted()).isTrue();
        assertThat(leaseUntilOf("orders"))
                .as("the premise: by this member's own clock node A's lease ran out minutes ago")
                .isBefore(Instant.now());

        PipelineActuationOwnership nodeB = ownership(NODE_B);

        assertThat(nodeB.permit("orders").granted())
                .as("what is left of a lease is the store's answer, not a subtraction done here")
                .isFalse();
        nanos.addAndGet(RENEW.toNanos());
        assertThat(nodeB.permit("orders").granted())
                .as("and waiting on this member's clock does not make the lease any shorter")
                .isFalse();

        // Only the store's own clock passing the lease frees it.
        claims.elapse(TTL.plusSeconds(1));
        nanos.addAndGet(RENEW.toNanos());

        assertThat(nodeB.permit("orders").granted()).isTrue();
        assertThat(ownerOf("orders")).isEqualTo(NODE_B);
    }

    @Test
    void aMemberWhoseClockRunsBehindStillTakesAClaimTheStoreSaysNobodyHolds() {
        thisMembersWallClockRunsBehindTheStoreBy(SKEW);
        PipelineActuationOwnership nodeA = ownership(NODE_A);
        assertThat(nodeA.permit("orders").granted()).isTrue();

        // Node A goes away without releasing anything, and the store's own clock passes the lease.
        claims.elapse(TTL.plusSeconds(1));
        assertThat(leaseUntilOf("orders"))
                .as("the premise: by this member's own clock node A's lease still has minutes to run")
                .isAfter(Instant.now());

        PipelineActuationOwnership nodeB = ownership(NODE_B);

        assertThat(nodeB.permit("orders").granted())
                .as("a member whose clock is behind must still come for a claim the store says is free; "
                        + "deciding not to ask is the same mistake as deciding the answer")
                .isTrue();
        assertThat(ownerOf("orders")).isEqualTo(NODE_B);
    }

    @Test
    void aMemberWhoseClockRunsBehindStopsWhenTheStoreSaysTheLeaseIsOver() {
        thisMembersWallClockRunsBehindTheStoreBy(SKEW);
        ExecutionFence fence = submittedRun();
        CountingWriter target = new CountingWriter();
        SinkWriter writer = FencedSinkWriterFactory.guarded(target, fence, guard(claims));
        writer.write(List.of());
        assertThat(target.batches).as("the run was current when it took its reading").hasValue(1);

        // The member holding the pipeline stops renewing, and the store's clock passes the lease.
        elapse(TTL.plusSeconds(1));
        assertThat(leaseUntilOf("orders"))
                .as("the premise: by this member's own clock the lease still has minutes to run")
                .isAfter(Instant.now());

        assertThatThrownBy(() -> writer.write(List.of()))
                .as("a lease this member's clock has not reached is over all the same")
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).code())
                .isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED);
        assertThat(target.batches).as("and nothing went out for it").hasValue(1);
    }

    @Test
    void theWindowAMemberActsOnIsCutToWhatTheStoreSaysIsLeftOfTheLease() {
        thisMembersWallClockRunsAheadOfTheStoreBy(SKEW);
        ExecutionFence fence = submittedRun();
        CountingWriter target = new CountingWriter();
        SinkWriter writer = FencedSinkWriterFactory.guarded(target, fence, guard(claims));
        writer.write(List.of());

        // Far enough in that what is left of the lease is shorter than a whole local window: the reading
        // taken here is good until the lease runs out, not for a window beyond it.
        elapse(TTL.minus(WINDOW).plusSeconds(2));
        writer.write(List.of());
        assertThat(target.batches)
                .as("still inside both bounds, so this batch is the run's to send").hasValue(2);

        // Past the lease, and past the deadline cut from it -- but not yet a window past that reading.
        elapse(WINDOW.minusSeconds(1));

        assertThatThrownBy(() -> writer.write(List.of()))
                .as("a member's own window may end early, never late: the lease it was cut from is gone")
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).code())
                .isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED);
        assertThat(target.batches).hasValue(2);
    }

    /** Puts the store's clock {@code by} behind this member's, so its leases read as long expired here. */
    private void thisMembersWallClockRunsAheadOfTheStoreBy(Duration by) {
        claims.clockAt(Instant.now().minus(by));
    }

    /** Puts the store's clock {@code by} ahead of this member's, so its leases read as long-lived here. */
    private void thisMembersWallClockRunsBehindTheStoreBy(Duration by) {
        claims.clockAt(Instant.now().plus(by));
    }

    /** Moves both clocks together, the way waiting moves the store's lease and this member's deadline. */
    private void elapse(Duration elapsed) {
        claims.elapse(elapsed);
        nanos.addAndGet(elapsed.toNanos());
    }

    private Instant leaseUntilOf(String pipelineId) {
        return claims.read(key(pipelineId)).orElseThrow().claim().leaseUntil();
    }

    private WorkloadOwner ownerOf(String pipelineId) {
        return claims.read(key(pipelineId)).orElseThrow().claim().owner();
    }

    private static WorkloadClaimKey key(String pipelineId) {
        return new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, pipelineId);
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
}
