package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ScopedExecutionAuthorizationTest {
    private static final CacheBusinessScope BUSINESS = new CacheBusinessScope("cluster", 3, "cache-a");
    private static final PipelineIntentScope FIRST = scope(1);
    private static final Duration WINDOW = Duration.ofSeconds(10);
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "orders");

    private final InMemoryWorkloadClaimStore raw = new InMemoryWorkloadClaimStore();
    private final AtomicLong nanos = new AtomicLong();
    private final AtomicBoolean nodeActive = new AtomicBoolean(true);
    private final AtomicReference<PipelineIntentScope> current = new AtomicReference<>(FIRST);
    private final AtomicInteger reads = new AtomicInteger();
    private final WorkloadClaimStore store = mock(WorkloadClaimStore.class);

    private WorkloadClaim run() {
        return raw.advanceExecution(raw.acquire(KEY, OWNER, 7, TTL).claim(), 7, Set.of("node-a")).orElseThrow();
    }

    private ExecutionAuthorization guard() {
        when(store.readPipeline(any())).thenAnswer(call -> {
            reads.incrementAndGet();
            if (!call.getArgument(0).equals(current.get())) throw fenced();
            return raw.read(KEY);
        });
        when(store.read(any())).thenThrow(new AssertionError("metadata reads cannot authorize a scoped run"));
        when(store.recordExecutionFailure(any(), any(Boolean.class))).thenAnswer(call ->
                raw.recordExecutionFailure(call.getArgument(0), call.getArgument(1)));
        return ExecutionAuthorization.scoped(BUSINESS, store, WINDOW, nanos::get, nodeActive::get);
    }

    @Test void aScopedRunKeepsItsIdentityAndOwnerAcrossSerialization() throws Exception {
        ExecutionFence original = new ExecutionFence(FIRST, WorkloadClaimFence.from(run()));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) { out.writeObject(original); }
        ExecutionFence copy;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            copy = (ExecutionFence) in.readObject();
        }
        assertThat(copy).isEqualTo(original); assertThat(copy.topologyRevision()).isEqualTo(7L);
        try (var guard = guard()) {
            for (int i = 0; i < 20; i++) assertThat(guard.authorized(copy)).isTrue();
            assertThat(guard.require(copy)).isEqualTo(WorkloadClaimFence.from(raw.read(KEY).orElseThrow().claim()));
            assertThat(reads).hasValue(1);
            verify(store, never()).read(any());
        }
    }

    @Test void scopedAndUnscopedGuardsCannotBeSubstitutedForOneAnother() {
        WorkloadClaim run = run();
        ExecutionFence scoped = new ExecutionFence(FIRST, WorkloadClaimFence.from(run));
        ExecutionFence unscoped = new ExecutionFence("orders", run.claimGeneration(), run.executionGeneration());
        try (var scopedGuard = guard(); var legacy = new ExecutionAuthorization("cluster", store, WINDOW, nanos::get);
                var unfenced = ExecutionAuthorization.unfenced()) {
            assertThat(scopedGuard.authorized(unscoped)).isFalse();
            assertThat(legacy.authorized(scoped)).isFalse();
            assertThat(unfenced.authorized(scoped)).isFalse();
            assertThatThrownBy(() -> unfenced.require(scoped)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED));
            assertThat(reads).hasValue(0);
            verify(store, never()).read(any());
        }
    }

    @Test void aForeignClusterBindingOrIncarnationIsRefusedBeforeReadingTheStore() {
        WorkloadClaim run = run();
        try (var guard = guard()) {
            for (PipelineIntentScope foreign : Set.of(new PipelineIntentScope("other", 3, "cache-a", "orders", 1),
                    new PipelineIntentScope("cluster", 4, "cache-a", "orders", 1),
                    new PipelineIntentScope("cluster", 3, "cache-b", "orders", 1))) {
                var claim = new WorkloadClaimFence(new WorkloadClaimKey(foreign.clusterId(),
                        WorkloadClaimType.PIPELINE_ACTUATION, foreign.pipelineId()), OWNER,
                        run.claimGeneration(), run.executionGeneration(), 7);
                assertThat(guard.authorized(new ExecutionFence(foreign, claim))).isFalse();
            }
            assertThat(reads).hasValue(0);
        }
    }

    @Test void matchingNumbersDoNotAuthorizeAnotherOwnerOrBoot() {
        WorkloadClaim run = run();
        try (var guard = guard()) {
            for (WorkloadOwner other : Set.of(new WorkloadOwner("node-b", "boot-a"), new WorkloadOwner("node-a", "boot-b"))) {
                var claim = new WorkloadClaimFence(KEY, other, run.claimGeneration(), run.executionGeneration(), 7);
                assertThat(guard.authorized(new ExecutionFence(FIRST, claim))).isFalse();
            }
            assertThat(guard.authorized(new ExecutionFence(FIRST, WorkloadClaimFence.from(run)))).isTrue();
        }
    }

    @Test void aProvedNewScopeRevokesTheOldPositiveEvenWithTheSameNumericTokens() {
        WorkloadClaim run = run();
        ExecutionFence old = new ExecutionFence(FIRST, WorkloadClaimFence.from(run));
        try (var guard = guard()) {
            assertThat(guard.authorized(old)).isTrue();
            current.set(scope(2));
            ExecutionFence replacement = new ExecutionFence(current.get(), WorkloadClaimFence.from(run));
            assertThat(guard.authorized(old)).as("the old answer is still inside its bounded window").isTrue();
            assertThat(guard.authorized(replacement)).isTrue();
            assertThat(guard.authorized(old)).as("a known replacement revokes the old local answer immediately").isFalse();
            assertThat(guard.authorized(replacement)).isTrue();
        }
    }

    @Test void aProvedNewScopeWithAnExpiredClaimStillRevokesTheOldPositive() {
        WorkloadClaim run = run();
        ExecutionFence old = new ExecutionFence(FIRST, WorkloadClaimFence.from(run));
        try (var guard = guard()) {
            assertThat(guard.authorized(old)).isTrue();
            current.set(scope(2));
            assertThat(raw.release(run)).isTrue();
            assertThat(guard.authorized(new ExecutionFence(current.get(), WorkloadClaimFence.from(run)))).isFalse();
            assertThat(guard.authorized(old)).isFalse();
        }
    }

    @Test void aProvedNewScopeWithoutAClaimStillRevokesTheOldPositive() {
        WorkloadClaim run = run();
        ExecutionFence old = new ExecutionFence(FIRST, WorkloadClaimFence.from(run));
        try (var guard = guard()) {
            assertThat(guard.authorized(old)).isTrue();
            current.set(scope(2));
            when(store.readPipeline(current.get())).thenReturn(java.util.Optional.empty());
            assertThat(guard.authorized(new ExecutionFence(current.get(), WorkloadClaimFence.from(run)))).isFalse();
            assertThat(guard.authorized(old)).isFalse();
        }
    }

    @Test void anUnprovedNewScopeCannotRevokeAnExistingBoundedAnswer() {
        WorkloadClaim run = run();
        ExecutionFence old = new ExecutionFence(FIRST, WorkloadClaimFence.from(run));
        try (var guard = guard()) {
            assertThat(guard.authorized(old)).isTrue();
            assertThat(guard.authorized(new ExecutionFence(scope(2), WorkloadClaimFence.from(run)))).isFalse();
            assertThat(guard.authorized(old)).isTrue();
        }
    }

    @Test void anOldScopesUnreachableFloorCannotRefuseTheNewScope() {
        WorkloadClaim run = run();
        try (var guard = guard()) {
            current.set(scope(2));
            assertThat(guard.authorized(new ExecutionFence(FIRST, WorkloadClaimFence.from(run)))).isFalse();
            int asked = reads.get();
            assertThat(guard.authorized(new ExecutionFence(FIRST, WorkloadClaimFence.from(run)))).isFalse();
            assertThat(reads).hasValue(asked);
            assertThat(guard.authorized(new ExecutionFence(current.get(), WorkloadClaimFence.from(run)))).isTrue();
        }
    }

    @Test void aLateFailureUsesItsOriginalScopeAndCannotMarkTheReplacement() {
        WorkloadClaim run = run();
        try (var guard = guard()) {
            current.set(scope(2));
            guard.recordSinkWriteFailure(new ExecutionFence(FIRST, WorkloadClaimFence.from(run)));
            assertThat(raw.read(KEY).orElseThrow().claim().failureClaimGeneration()).isZero();
            guard.recordSinkWriteFailure(new ExecutionFence(current.get(), WorkloadClaimFence.from(run)));
            assertThat(raw.read(KEY).orElseThrow().claim().failureClaimGeneration()).isEqualTo(run.claimGeneration());
            verify(store, never()).read(any());
        }
    }

    @Test void anotherBootCannotFileAReplacementRunsFailure() {
        WorkloadClaim run = run();
        try (var guard = guard()) {
            var wrong = new WorkloadClaimFence(KEY, new WorkloadOwner("node-a", "other-boot"),
                    run.claimGeneration(), run.executionGeneration(), 7);
            guard.recordSinkWriteFailure(new ExecutionFence(FIRST, wrong));
            assertThat(raw.read(KEY).orElseThrow().claim().failureClaimGeneration()).isZero();
        }
    }

    @Test void anUnimplementedScopedReadNeverFallsBackToMetadata() {
        WorkloadClaim run = run();
        WorkloadClaimStore unsupported = mock(WorkloadClaimStore.class, CALLS_REAL_METHODS);
        when(unsupported.read(KEY)).thenReturn(raw.read(KEY));
        try (var guard = ExecutionAuthorization.scoped(BUSINESS, unsupported, WINDOW, nanos::get, nodeActive::get)) {
            assertThat(guard.authorized(new ExecutionFence(FIRST, WorkloadClaimFence.from(run)))).isFalse();
            verify(unsupported, never()).read(KEY);
        }
    }

    @Test void aReplyAfterTheOriginalLeaseBoundDoesNotAuthorize() {
        WorkloadClaim run = raw.advanceExecution(raw.acquire(KEY, OWNER, 7, Duration.ofMillis(100)).claim(),
                7, Set.of("node-a")).orElseThrow();
        try (var guard = guard()) {
            when(store.readPipeline(FIRST)).thenAnswer(call -> {
                WorkloadClaimReading captured = raw.read(KEY).orElseThrow();
                nanos.addAndGet(Duration.ofMillis(200).toNanos());
                return java.util.Optional.of(captured);
            });
            assertThat(guard.authorized(new ExecutionFence(FIRST, WorkloadClaimFence.from(run)))).isFalse();
        }
    }

    @Test void nodeLossAndCloseRefuseWithoutRefreshing() {
        WorkloadClaim run = run();
        try (var guard = guard()) {
            ExecutionFence fence = new ExecutionFence(FIRST, WorkloadClaimFence.from(run));
            assertThat(guard.authorized(fence)).isTrue();
            nodeActive.set(false);
            assertThat(guard.authorized(fence)).isFalse();
            nodeActive.set(true);
            guard.close();
            assertThat(guard.authorized(fence)).isFalse();
            assertThat(reads).hasValue(1);
        }
    }

    @Test void aScopedRunCannotBorrowTheTopologyOfALaterSameOwnerAcquisition() {
        WorkloadClaim run = run();
        try (var guard = guard()) {
            var later = raw.acquire(KEY, OWNER, 8, TTL).claim();
            assertThat(later.claimGeneration()).isEqualTo(run.claimGeneration());
            assertThat(later.executionGeneration()).isEqualTo(run.executionGeneration());
            ExecutionFence fence = new ExecutionFence(FIRST, WorkloadClaimFence.from(run));
            assertThat(guard.authorized(fence)).isFalse();
            assertThatThrownBy(() -> guard.require(fence)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED));
        }
    }

    @Test void anOldTopologyCannotMarkTheLaterClaimAsFailed() {
        WorkloadClaim run = run(); var original = new ExecutionFence(FIRST, WorkloadClaimFence.from(run));
        raw.acquire(KEY, OWNER, 8, TTL);
        try (var guard = guard()) {
            guard.recordSinkWriteFailure(original);
            assertThat(raw.read(KEY).orElseThrow().claim().failureClaimGeneration()).isZero();
        }
    }

    @Test void aFreshScopedTopologyIsRefreshedWithoutWaitingOutTheOldLocalProof() {
        WorkloadClaim run = run();
        try (var guard = guard()) {
            var original = new ExecutionFence(FIRST,WorkloadClaimFence.from(run)); assertThat(guard.authorized(original)).isTrue();
            var later = raw.acquire(KEY,OWNER,8,TTL).claim(); var actual = new ExecutionFence(FIRST,WorkloadClaimFence.from(later));
            assertThat(guard.authorized(actual)).isTrue(); assertThat(guard.require(actual)).isEqualTo(WorkloadClaimFence.from(later));
            assertThat(guard.authorized(original)).isFalse(); assertThat(reads).hasValue(2);
        }
    }

    @Test void aRunCannotBeConstructedFromAnUnrelatedClaim() {
        var wrong = new WorkloadClaimFence(new WorkloadClaimKey("other", WorkloadClaimType.PIPELINE_ACTUATION, "orders"),
                OWNER, 1, 1, 7);
        assertThatThrownBy(() -> new ExecutionFence(FIRST, wrong)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ExecutionFence("orders", 1, 1, FIRST, null, null, 7L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ExecutionFence("orders", 1, 1, null, "node", "boot", 7L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static PipelineIntentScope scope(long generation) {
        return new PipelineIntentScope("cluster", 3, "cache-a", "orders", generation);
    }

    private static TapstateException fenced() {
        return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
    }
}
