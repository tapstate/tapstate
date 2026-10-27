package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.tapstate.spi.store.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ReportingExecutionProofTest {
    private static final CacheBusinessScope BUSINESS = new CacheBusinessScope("node", 1, "cache");
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("node", 1, "cache", "orders", 1);
    private static final WorkloadOwner OWNER = new WorkloadOwner("node", "boot");
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("node", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
    private final AtomicBoolean alive = new AtomicBoolean(true);
    private final AtomicBoolean node = new AtomicBoolean(true);
    private final AtomicLong nanos = new AtomicLong();
    private final InMemoryWorkloadClaimStore raw = new InMemoryWorkloadClaimStore();
    private final WorkloadClaimStore store = mock(WorkloadClaimStore.class);
    private final ClusterMembershipGate gate = new ClusterMembershipGate(new ClusterProperties());

    private PipelineActuationOwnership owner() {
        gate.bindSingle(new WorkloadClaim(new WorkloadClaimKey("node", WorkloadClaimType.NODE_SESSION, "node"),
                OWNER, 1, 0, 0, Instant.now().plusSeconds(30)));
        gate.observeNodeSession(node::get); gate.observeDataPlane(alive::get);
        var member = mock(com.hazelcast.cluster.Member.class);
        when(member.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE)).thenReturn("node");
        when(member.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE)).thenReturn("boot");
        assertThat(gate.apply(Set.of(member))).isTrue();
        when(store.acquirePipeline(eq(SCOPE), eq(OWNER), anyLong(), any())).thenAnswer(call ->
                raw.acquire(KEY, OWNER, call.getArgument(2), call.getArgument(3)));
        when(store.advanceExecution(any(), anyLong(), anySet())).thenAnswer(call ->
                raw.advanceExecution(call.getArgument(0), call.getArgument(1), call.getArgument(2)));
        when(store.readPipeline(eq(SCOPE))).thenAnswer(call -> raw.read(KEY));
        return PipelineActuationOwnership.scoped(BUSINESS, OWNER, gate, new ClusterWorkloadClaims(store, gate),
                Duration.ofSeconds(30), Duration.ofSeconds(10), nanos::get);
    }

    @Test void reportingNeedsAnExecutionAndCannotGrantABusinessWrite() {
        var owner = owner();
        var permit = owner.permit(SCOPE);
        assertThat(owner.reportingExecution(SCOPE)).isEmpty();
        var execution = owner.beginExecution(SCOPE, WorkloadClaimFence.from(permit.claim()));
        alive.set(false); clearInvocations(store);
        var proof = owner.reportingExecution(SCOPE).orElseThrow();
        assertThat(proof.execution()).isEqualTo(execution.fence());
        assertThat(proof.current()).isTrue();
        assertThat(owner.proveClaim(SCOPE, execution.claimFence())).isFalse();
        verify(store, never()).acquirePipeline(any(), any(), anyLong(), any());
        verify(store, never()).advanceExecution(any(), anyLong(), anySet());
        verify(store, never()).renew(any(), any());
    }

    @Test void replacedExecutionsAndExpiredNodeOrLeaseCannotBorrowAReportingProof() {
        var owner = owner();
        var permit = owner.permit(SCOPE);
        var first = owner.beginExecution(SCOPE, WorkloadClaimFence.from(permit.claim()));
        alive.set(false);
        var proof = owner.reportingExecution(SCOPE).orElseThrow();
        alive.set(true); owner.beginExecution(SCOPE, first.claimFence()); alive.set(false);
        assertThat(proof.current()).isFalse();
        var next = owner.reportingExecution(SCOPE).orElseThrow();
        node.set(false);
        assertThat(next.current()).isFalse(); assertThat(owner.reportingExecution(SCOPE)).isEmpty();
        node.set(true); nanos.addAndGet(Duration.ofSeconds(31).toNanos());
        assertThat(next.current()).isFalse();
        owner.stopForShutdown(); assertThat(owner.reportingExecution(SCOPE)).isEmpty();
    }
}
