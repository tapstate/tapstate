package io.tapstate.app;

import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ExecutionAuthorizationCannotOutliveItsNodeSessionTest {

    private static final Duration WINDOW = Duration.ofSeconds(10);
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey(
            "east", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
    private static final WorkloadOwner OWNER = new WorkloadOwner("a", "boot-a");

    @Test
    void aStoreResponseArrivingAfterTheAuthorizationWindowCannotAuthorizeEvenOneBatch() {
        AtomicLong nanos = new AtomicLong();
        WorkloadClaimStore store = mock(WorkloadClaimStore.class);
        WorkloadClaim claim = new WorkloadClaim(KEY, OWNER, 7, 9, 4, Instant.EPOCH.plusSeconds(30));
        when(store.read(KEY)).thenAnswer(call -> {
            nanos.addAndGet(WINDOW.multipliedBy(2).toNanos());
            return Optional.of(new WorkloadClaimReading(claim, Duration.ofSeconds(30)));
        });
        try (ExecutionAuthorization guard = new ExecutionAuthorization("east", store, WINDOW, nanos::get)) {
            assertThat(guard.authorized(new ExecutionFence("orders", 7, 9))).isFalse();
        }
    }

    @Test
    void aProcessorsOwnSessionExpiresBeforeTheControllerClaimDoes() {
        AtomicLong nanos = new AtomicLong();
        WorkloadClaimStore store = mock(WorkloadClaimStore.class);
        when(store.read(KEY)).thenReturn(Optional.of(new WorkloadClaimReading(claim(KEY, 7), WINDOW)));
        WorkloadClaim node = claim(new WorkloadClaimKey("east", WorkloadClaimType.NODE_SESSION, "a"), 7);
        try (ExecutionAuthorization guard = new ExecutionAuthorization("east", store, WINDOW, nanos::get,
                () -> new NodeSessionLease.Proof(node, 2, true))) {
            ExecutionFence execution = new ExecutionFence("orders", 7, 9, 7);
            assertThat(guard.authorized(execution)).isTrue();
            nanos.set(3);
            assertThat(guard.authorized(execution)).isFalse();
        }
    }

    @Test
    void aCachedControllerClaimCannotCrossThisMembersProfileGeneration() {
        AtomicLong nanos = new AtomicLong();
        WorkloadClaimStore store = mock(WorkloadClaimStore.class);
        when(store.read(KEY)).thenReturn(Optional.of(new WorkloadClaimReading(claim(KEY, 7), WINDOW)));
        WorkloadClaim node = claim(new WorkloadClaimKey("east", WorkloadClaimType.NODE_SESSION, "a"), 8);
        try (ExecutionAuthorization guard = new ExecutionAuthorization("east", store, WINDOW, nanos::get,
                () -> new NodeSessionLease.Proof(node, WINDOW.toNanos(), true))) {
            assertThat(guard.authorized(new ExecutionFence("orders", 7, 9, 7))).isFalse();
        }
    }

    @Test
    void aReleasedSessionRefusesCallsBeforeItsOriginalDeadline() {
        AtomicLong nanos = new AtomicLong();
        WorkloadClaimStore store = mock(WorkloadClaimStore.class);
        when(store.read(KEY)).thenReturn(Optional.of(new WorkloadClaimReading(claim(KEY, 7), WINDOW)));
        WorkloadClaim node = claim(new WorkloadClaimKey("east", WorkloadClaimType.NODE_SESSION, "a"), 7);
        try (ExecutionAuthorization guard = new ExecutionAuthorization("east", store, WINDOW, nanos::get,
                () -> new NodeSessionLease.Proof(node, WINDOW.toNanos(), false))) {
            assertThat(guard.authorized(new ExecutionFence("orders", 7, 9, 7))).isFalse();
        }
    }

    private static WorkloadClaim claim(WorkloadClaimKey key, long profile) {
        return new WorkloadClaim(key, OWNER, 7, 9, 4, Instant.EPOCH.plusSeconds(30),
                9, 7, Set.of("a"), 0, false, profile);
    }
}
