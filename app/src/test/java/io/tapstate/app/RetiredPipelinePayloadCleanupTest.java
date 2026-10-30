package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class RetiredPipelinePayloadCleanupTest {
    private static final WorkloadClaimFence NODE = new WorkloadClaimFence(
            new WorkloadClaimKey("cluster", WorkloadClaimType.NODE_SESSION, "node"), new WorkloadOwner("node", "boot"), 1, 0, 1);
    private static RetiredPipelinePayloads.Original original(String pipeline) {
        return new RetiredPipelinePayloads.Original(new PipelineIntentScope("cluster", 1, "cache", pipeline, 1),
                "retire", UUID.randomUUID().toString(), "a".repeat(64));
    }

    @Test void aFullBatchContinuesTheSameFrozenOriginalBeforeAdvancingAndWrapping() {
        var store = mock(RetiredPipelinePayloads.class); var old = original("deleted");
        when(store.listAfter(null, 1)).thenReturn(List.of(old.scope()));
        when(store.find(old.scope())).thenReturn(Optional.of(old));
        when(store.listAfter(old.scope(), 1)).thenReturn(List.of());
        when(store.reclaim(old, NODE)).thenReturn(256L, 14L, 0L);
        try (var cleanup = new RetiredPipelinePayloadCleanup(guard -> store, () -> Optional.of(NODE), () -> true)) {
            cleanup.runOnce(); cleanup.runOnce(); cleanup.runOnce(); cleanup.runOnce();
        }
        var order = inOrder(store);
        order.verify(store).listAfter(null, 1); order.verify(store).find(old.scope()); order.verify(store).reclaim(old, NODE);
        order.verify(store).listAfter(null, 1); order.verify(store).find(old.scope()); order.verify(store).reclaim(old, NODE);
        order.verify(store).listAfter(old.scope(), 1);
        order.verify(store).listAfter(null, 1); order.verify(store).find(old.scope()); order.verify(store).reclaim(old, NODE);
        verifyNoMoreInteractions(store);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void failureKeepsTheOriginalRetryableAndLetsAnotherRetiredScopeProgress(boolean duringFind) {
        var store = mock(RetiredPipelinePayloads.class); var old = original("a"); var other = original("b");
        when(store.listAfter(null, 1)).thenReturn(List.of(old.scope()));
        when(store.find(old.scope())).thenReturn(Optional.of(old)); when(store.listAfter(old.scope(), 1)).thenReturn(List.of(other.scope()));
        when(store.find(other.scope())).thenReturn(Optional.of(other));
        when(store.listAfter(other.scope(), 1)).thenReturn(List.of());
        var failure = new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "fixture"), null);
        if (duringFind) when(store.find(old.scope())).thenThrow(failure).thenReturn(Optional.of(old));
        else when(store.reclaim(old, NODE)).thenThrow(failure).thenReturn(0L);
        try (var cleanup = new RetiredPipelinePayloadCleanup(guard -> store, () -> Optional.of(NODE), () -> true)) {
            cleanup.runOnce(); cleanup.runOnce(); cleanup.runOnce(); cleanup.runOnce();
        }
        verify(store, times(duringFind ? 1 : 2)).reclaim(old, NODE); verify(store).reclaim(other, NODE);
        verify(store, times(2)).find(old.scope());
    }

    @Test void theNodeProofIsCapturedBeforeTheReadAndNeverUpgradedAfterIt() {
        var store = mock(RetiredPipelinePayloads.class); var old = original("deleted");
        var current = new AtomicReference<>(Optional.of(NODE));
        when(store.listAfter(null, 1)).thenAnswer(call -> { current.set(Optional.empty()); return List.of(old.scope()); });
        when(store.find(old.scope())).thenReturn(Optional.of(old));
        try (var cleanup = new RetiredPipelinePayloadCleanup(guard -> store, current::get, () -> true)) { cleanup.runOnce(); }
        verify(store).reclaim(old, NODE);
    }

    @Test void ineligibleOrClosedWorkersCannotReadOrReuseTheirOriginalStorageGuard() {
        var store = mock(RetiredPipelinePayloads.class); var heldGuard = new AtomicReference<Runnable>();
        var cleanup = new RetiredPipelinePayloadCleanup(guard -> { heldGuard.set(guard); return store; }, () -> Optional.of(NODE), () -> false);
        cleanup.runOnce(); cleanup.close(); cleanup.runOnce();
        assertThatThrownBy(heldGuard.get()::run).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.CACHE_BUSINESS_FENCED));
        verifyNoInteractions(store);
    }

    @Test void anInvariantFailureIsNotLaunderedIntoARetryableStorageRefusal() {
        var store = mock(RetiredPipelinePayloads.class); var failure = new IllegalStateException("fixture invariant");
        when(store.listAfter(null, 1)).thenThrow(failure);
        try (var cleanup = new RetiredPipelinePayloadCleanup(guard -> store, () -> Optional.of(NODE), () -> true)) {
            assertThatThrownBy(cleanup::runOnce).isSameAs(failure);
        }
    }
}
