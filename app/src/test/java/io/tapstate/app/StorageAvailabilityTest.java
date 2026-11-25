package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.availability.ApplicationAvailabilityBean;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.LivenessState;
import org.springframework.boot.availability.ReadinessState;

class StorageAvailabilityTest {
    @Test
    void noBusinessOwnerKeepsTheRootLifecycleStates() {
        var root = new StorageAvailability(Optional::empty);
        root.onApplicationEvent(new AvailabilityChangeEvent<>(this, LivenessState.CORRECT));
        root.onApplicationEvent(new AvailabilityChangeEvent<>(this, ReadinessState.ACCEPTING_TRAFFIC));
        assertThat(root.getLivenessState()).isEqualTo(LivenessState.CORRECT);
        assertThat(root.getReadinessState()).isEqualTo(ReadinessState.ACCEPTING_TRAFFIC);
    }

    @Test
    void theCurrentEngineFailureIsVisibleButCannotPoisonItsReplacement() {
        var original = new ApplicationAvailabilityBean();
        var cause = new OutOfMemoryError("original engine failure");
        original.onApplicationEvent(new AvailabilityChangeEvent<>(cause, LivenessState.BROKEN));
        var current = new AtomicReference<ApplicationAvailability>(original);
        var root = new StorageAvailability(() -> Optional.ofNullable(current.get()));
        root.onApplicationEvent(new AvailabilityChangeEvent<>(this, LivenessState.CORRECT));
        assertThat(root.getLivenessState()).isEqualTo(LivenessState.BROKEN);
        assertThat(root.getLastChangeEvent(LivenessState.class).getSource()).isSameAs(cause);
        current.set(new ApplicationAvailabilityBean());
        assertThat(root.getLivenessState()).isEqualTo(LivenessState.CORRECT);
        current.set(null);
        assertThat(root.getLivenessState()).isEqualTo(LivenessState.CORRECT);
    }

    @Test
    void aHealthyChildCannotClearARootFailureOrOverrideRootReadiness() {
        var child = new ApplicationAvailabilityBean();
        child.onApplicationEvent(new AvailabilityChangeEvent<>(this, LivenessState.CORRECT));
        child.onApplicationEvent(new AvailabilityChangeEvent<>(this, ReadinessState.REFUSING_TRAFFIC));
        var root = new StorageAvailability(() -> Optional.of(child));
        root.onApplicationEvent(new AvailabilityChangeEvent<>(this, LivenessState.BROKEN));
        root.onApplicationEvent(new AvailabilityChangeEvent<>(this, ReadinessState.ACCEPTING_TRAFFIC));
        assertThat(root.getLivenessState()).isEqualTo(LivenessState.BROKEN);
        assertThat(root.getReadinessState()).isEqualTo(ReadinessState.ACCEPTING_TRAFFIC);
    }

    @Test
    void anUnpublishedCandidateCannotBlockALivenessRead() throws Exception {
        try (var runtime = new StorageRuntime(null, ignored -> null)) {
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            Thread candidate = Thread.startVirtualThread(() -> {
                synchronized (runtime) {
                    entered.countDown();
                    try { release.await(); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
                }
            });
            try {
                entered.await();
                assertTimeoutPreemptively(Duration.ofSeconds(1),
                        () -> assertThat(runtime.businessForLiveness()).isEmpty());
            } finally {
                release.countDown(); candidate.join();
            }
        }
    }
}
