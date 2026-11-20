package io.tapstate.app;

import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Claim I/O for one Pipeline must not occupy another Pipeline's observation or admission path. */
class PipelineClaimIsolationTest {
    private static final PipelineIntentScope A = new PipelineIntentScope("cluster", 1, "cache", "orders", 1);
    private static final PipelineIntentScope B = new PipelineIntentScope("cluster", 1, "cache", "shipments", 1);
    private final InMemoryWorkloadClaimStore claims = new InMemoryWorkloadClaimStore();
    private final WorkloadClaimStore boundary = mock(WorkloadClaimStore.class);
    private final ClusterMembershipGate membership = mock(ClusterMembershipGate.class);
    private final CountDownLatch entered = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);

    private PipelineActuationOwnership owner() {
        when(membership.businessEligible()).thenReturn(true);
        when(membership.committed()).thenReturn(new ClusterMembership("cluster", 7, Set.of("node")));
        when(boundary.acquirePipeline(any(), any(), anyLong(), any())).thenAnswer(call ->
                claims.acquire(key(call.getArgument(0)), call.getArgument(1), call.getArgument(2), call.getArgument(3)));
        when(boundary.readPipeline(any())).thenAnswer(call -> claims.read(key(call.getArgument(0))));
        when(boundary.release(any())).thenAnswer(call -> claims.release(call.getArgument(0)));
        return PipelineActuationOwnership.scoped(new CacheBusinessScope("cluster", 1, "cache"),
                new WorkloadOwner("node", "boot"), membership, new ClusterWorkloadClaims(boundary, membership),
                Duration.ofMinutes(1), Duration.ofSeconds(10), System::nanoTime);
    }

    @Test
    void acquiringAClaimDoesNotDelayAnotherPipelineOrRetention() throws Exception {
        var owner = owner();
        assertThat(owner.permit(B).granted()).isTrue();
        org.mockito.Mockito.doAnswer(call -> {
            PipelineIntentScope scope = call.getArgument(0);
            if (A.equals(scope)) hold();
            return claims.acquire(key(scope), call.getArgument(1), call.getArgument(2), call.getArgument(3));
        }).when(boundary).acquirePipeline(any(), any(), anyLong(), any());
        try (var executor = Executors.newFixedThreadPool(3)) {
            try {
                var acquiring = executor.submit(() -> owner.permit(A));
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                var retaining = executor.submit(() -> owner.retain(List.of(A.pipelineId(), B.pipelineId())));
                var other = executor.submit(() -> owner.permit(B));
                assertThat(other.get(1, TimeUnit.SECONDS).granted()).isTrue();
                retaining.get(1, TimeUnit.SECONDS);
                release.countDown();
                assertThat(acquiring.get(2, TimeUnit.SECONDS).granted()).isTrue();
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void advancingAClaimDoesNotDelayAnotherPipelinesAdmissionOrObservationProof() throws Exception {
        var owner = owner();
        var originalA = WorkloadClaimFence.from(owner.permit(A).claim());
        var originalB = WorkloadClaimFence.from(owner.permit(B).claim());
        when(boundary.advanceExecution(any(), anyLong(), any())).thenAnswer(call -> {
            WorkloadClaim claim = call.getArgument(0);
            if (claim.key().equals(key(A))) hold();
            return claims.advanceExecution(claim, call.getArgument(1), call.getArgument(2));
        });
        // Control establishes that the exact original fence is provable before starting the slow write.
        assertThat(owner.proveObservationClaim(B, originalB)).isTrue();
        try (var executor = Executors.newFixedThreadPool(2)) {
            try {
                var advancing = executor.submit(() -> owner.beginExecution(A, originalA));
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                var other = executor.submit(() -> {
                    owner.retain(List.of(A.pipelineId(), B.pipelineId()));
                    assertThat(owner.proveObservationClaim(A, originalA)).isFalse();
                    assertThat(owner.proveObservationClaim(B, originalB)).isTrue();
                    assertThat(owner.permit(B).granted()).isTrue();
                    assertThat(owner.permit(A).granted()).isFalse();
                    assertThat(owner.beginExecution(A, originalA).allowed()).isFalse();
                    org.assertj.core.api.Assertions.assertThatThrownBy(() -> owner.control(A))
                            .isInstanceOf(io.tapstate.core.common.TapstateException.class);
                    return owner.beginExecution(B, originalB).allowed();
                });
                assertThat(other.get(1, TimeUnit.SECONDS)).isTrue();
                release.countDown();
                assertThat(advancing.get(2, TimeUnit.SECONDS).allowed()).isTrue();
                assertThat(owner.proveObservationClaim(A, originalA)).isFalse();
            } finally {
                release.countDown();
            }
        }
    }


    @ParameterizedTest
    @CsvSource({"false,none", "true,none", "true,release-error", "true,release-refused", "true,read-error", "true,membership-lost", "true,admission-expired"})
    void aRemovedPipelineCannotPublishAnAcquisitionThatReturnsLater(boolean nodeBacked, String failure) throws Exception {
        var owner = nodeBacked ? nodeBackedOwner() : owner();
        org.mockito.Mockito.doAnswer(call -> {
            assertThat(Thread.holdsLock(owner)).isFalse();
            return claims.read(key(call.getArgument(0)));
        }).when(boundary).readPipeline(any());
        org.mockito.stubbing.Answer<Object> releaseClaim = call -> {
            assertThat(Thread.holdsLock(owner)).isFalse();
            return claims.release(call.getArgument(0));
        };
        org.mockito.Mockito.doAnswer(releaseClaim).when(boundary).release(any());
        var attempts = new java.util.concurrent.atomic.AtomicInteger();
        var deadline = new java.util.concurrent.atomic.AtomicLong(Long.MAX_VALUE);
        var unavailable = new io.tapstate.core.common.TapstateException(io.tapstate.spi.store.IoError.STORE_UNAVAILABLE,
                java.util.Map.of("detail", "fixture store outage"), null);
        if (failure.equals("release-error")) org.mockito.Mockito.doThrow(unavailable)
                .doAnswer(releaseClaim).when(boundary).release(any());
        if (failure.equals("release-refused")) org.mockito.Mockito.doReturn(false)
                .doAnswer(releaseClaim).when(boundary).release(any());
        if (failure.equals("read-error")) org.mockito.Mockito.doThrow(unavailable)
                .doAnswer(call -> claims.read(key(call.getArgument(0)))).when(boundary).readPipeline(any());
        if (failure.equals("membership-lost")) org.mockito.Mockito.doAnswer(call -> {
            when(membership.businessEligible()).thenReturn(false);
            return java.util.Optional.empty();
        }).doAnswer(call -> claims.read(key(call.getArgument(0)))).when(boundary).readPipeline(any());
        if (failure.equals("admission-expired")) org.mockito.Mockito.doAnswer(call -> {
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2))
                    .until(() -> System.nanoTime() - deadline.get() >= 0);
            return claims.read(key(call.getArgument(0)));
        }).doAnswer(call -> claims.read(key(call.getArgument(0)))).when(boundary).readPipeline(any());
        org.mockito.stubbing.Answer<Object> acquire = call -> {
            attempts.incrementAndGet();
            hold();
            var heldBy = nodeBacked ? call.<WorkloadClaimFence>getArgument(1).owner()
                    : call.<WorkloadOwner>getArgument(1);
            var ttl = nodeBacked ? Duration.ofDays(1) : call.<Duration>getArgument(3);
            return claims.acquire(key(call.getArgument(0)), heldBy, call.getArgument(2), ttl);
        };
        if (nodeBacked) org.mockito.Mockito.doAnswer(acquire).when(boundary).acquireNodeBackedPipeline(any(), any(), anyLong());
        else org.mockito.Mockito.doAnswer(acquire).when(boundary).acquirePipeline(any(), any(), anyLong(), any());
        try (var executor = Executors.newSingleThreadExecutor()) {
            try {
                deadline.set(System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
                var acquiring = executor.submit(() -> {
                    var result = new java.util.concurrent.atomic.AtomicReference<PipelineActuationOwnership.Permit>();
                    long budget = failure.equals("admission-expired") ? deadline.get()
                            : System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    new ShutdownAwait().complete(budget, () -> result.set(owner.permit(A)));
                    return result.get();
                });
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                owner.retain(List.of());
                assertThat(owner.permit(A).granted()).isFalse();
                assertThat(attempts).hasValue(1);
                release.countDown();
                assertThat(acquiring.get(2, TimeUnit.SECONDS).granted()).isFalse();
                assertThat(claims.read(key(A)).orElseThrow().leased()).isEqualTo(!failure.equals("none"));
                if (!failure.equals("none")) {
                    when(membership.businessEligible()).thenReturn(true);
                    assertThat(owner.permit(A).granted()).isFalse();
                    owner.retain(List.of(A.pipelineId()));
                }
                assertThat(claims.read(key(A)).orElseThrow().leased()).isFalse();
                assertThat(owner.proveObservationClaim(A,
                        WorkloadClaimFence.from(claims.read(key(A)).orElseThrow().claim()))).isFalse();
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void aStoreAdvanceUnderAnObsoleteTopologyNeverAuthorizesSubmission() {
        var owner = owner();
        var expected = WorkloadClaimFence.from(owner.permit(A).claim());
        when(boundary.advanceExecution(any(), anyLong(), any())).thenAnswer(call -> {
            var advanced = claims.advanceExecution(call.getArgument(0), call.getArgument(1), call.getArgument(2));
            assertThat(advanced).isPresent();
            when(membership.committed()).thenReturn(new ClusterMembership("cluster", 8, Set.of("node", "joined")));
            return advanced;
        });

        assertThat(owner.beginExecution(A, expected).allowed()).isFalse();
        var advanced = claims.read(key(A)).orElseThrow().claim();
        assertThat(advanced.executionGeneration()).isEqualTo(expected.executionGeneration() + 1);
        assertThat(owner.proveObservationClaim(A, WorkloadClaimFence.from(advanced))).isFalse();
        owner.retain(List.of(A.pipelineId()));
        assertThat(claims.read(key(A)).orElseThrow().leased()).isFalse();
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void aGrantRefusedAfterATopologyChangeIsReleasedEvenWithALocalPlaceholder(boolean nodeBacked) {
        var owner = nodeBacked ? nodeBackedOwner() : owner();
        org.mockito.stubbing.Answer<Object> acquire = call -> {
            var heldBy = nodeBacked ? call.<WorkloadClaimFence>getArgument(1).owner()
                    : call.<WorkloadOwner>getArgument(1);
            var ttl = nodeBacked ? Duration.ofDays(1) : call.<Duration>getArgument(3);
            var granted = claims.acquire(key(A), heldBy, call.getArgument(2), ttl);
            assertThat(granted.acquired()).isTrue();
            when(membership.committed()).thenReturn(new ClusterMembership("cluster", 8, Set.of("node", "joined")));
            return granted;
        };
        if (nodeBacked) org.mockito.Mockito.doAnswer(acquire).when(boundary).acquireNodeBackedPipeline(any(), any(), anyLong());
        else org.mockito.Mockito.doAnswer(acquire).when(boundary).acquirePipeline(any(), any(), anyLong(), any());

        assertThat(owner.permit(A).granted()).isFalse();
        assertThat(claims.read(key(A)).orElseThrow().leased()).isFalse();
    }

    @Test
    void closingCannotPublishAnAcquisitionThatReturnsLater() throws Exception {
        var owner = owner();
        org.mockito.Mockito.doAnswer(call -> {
            hold();
            return claims.acquire(key(call.getArgument(0)), call.getArgument(1), call.getArgument(2), call.getArgument(3));
        }).when(boundary).acquirePipeline(any(), any(), anyLong(), any());
        try (var executor = Executors.newSingleThreadExecutor()) {
            try {
                var acquiring = executor.submit(() -> owner.permit(A));
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                owner.stopForShutdown();
                release.countDown();
                assertThat(acquiring.get(2, TimeUnit.SECONDS).granted()).isFalse();
                owner.awaitStopped(System.nanoTime() + TimeUnit.SECONDS.toNanos(2));
            } finally {
                release.countDown();
            }
        }
    }

    private PipelineActuationOwnership nodeBackedOwner() {
        owner();
        var node = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.NODE_SESSION, "node"),
                new WorkloadOwner("node", "boot"), 1, 0, 7);
        return PipelineActuationOwnership.nodeBacked(new CacheBusinessScope("cluster", 1, "cache"), node,
                membership, new ClusterWorkloadClaims(boundary, membership),
                Duration.ofMinutes(1), Duration.ofSeconds(10), System::nanoTime);
    }

    private void hold() throws InterruptedException {
        entered.countDown();
        assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
    }

    private static WorkloadClaimKey key(PipelineIntentScope scope) {
        return new WorkloadClaimKey(scope.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, scope.pipelineId());
    }
}
