package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ValidationTaskStore;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ValidationMetadataSlotsTest {
    @Test @Timeout(10) void realDiscoveryCanOwnAFrameAfterAReportHasFinishedAndAcquisitionRetainsItsOwnPartialSlots() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var hold = new AtomicBoolean(true);
        var configuration = mock(MongoConfigurationDomain.class); var tasks = mock(ValidationTaskStore.class);
        when(configuration.validationTasks(any(Runnable.class))).thenReturn(tasks);
        when(tasks.pipelines(any(), eq(1))).thenReturn(List.of());
        Runnable active = () -> {
            if (Thread.currentThread().getName().startsWith("validation-control-io") && hold.compareAndSet(true, false)) {
                entered.countDown();
                try { release.await(); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
            }
        };
        var collector = Executors.newSingleThreadExecutor();
        Future<List<ValidationMemberResources.Metadata>> future = null; List<ValidationMemberResources.Metadata> owned = List.of();
        try (var process = new ValidationMemberResources()) {
            var member = process.bind("cluster", "node", "boot", 4, () -> {});
            try (var scheduler = new NativeValidationScheduler(configuration, member, process,
                    () -> { throw new AssertionError("no schedules exist"); }, new MemoryValidationSchedules(), Clock.systemUTC(), new ValidationScheduleCalculator(), active)) {
                scheduler.start(); assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
                assertThat(member.snapshot().metadataFrames()).isOne();
                future = collector.submit(() -> ValidationMetadataSlots.acquire(member, 4, Duration.ofSeconds(2)));
                org.awaitility.Awaitility.await().atMost(Duration.ofMillis(500)).pollInterval(Duration.ofMillis(10))
                        .untilAsserted(() -> assertThat(member.snapshot().metadataFrames()).isEqualTo(4));
                assertThat(future).isNotDone();
                release.countDown(); owned = future.get(2, TimeUnit.SECONDS);
                assertThat(owned).hasSize(4); assertThat(member.snapshot().metadataFrames()).isEqualTo(4);
                assertThat(member.snapshot().processReserved().isEmpty()).isTrue();
                verify(configuration, atLeastOnce()).validationTasks(any(Runnable.class));
            } finally {
                release.countDown(); owned.forEach(ValidationMemberResources.Metadata::close);
                if (future != null) future.cancel(true);
            }
        } finally { collector.shutdownNow(); assertThat(collector.awaitTermination(2, TimeUnit.SECONDS)).isTrue(); }
    }
    @Test void aLeakedForeignFrameStillFailsAndOnlyTheCollectorsPartialAllocationsAreReturned() {
        try (var process = new ValidationMemberResources()) {
            var member = process.bind("cluster", "node", "boot", 4, () -> {});
            try (var foreign = member.admitMetadata()) {
                assertThatThrownBy(() -> ValidationMetadataSlots.acquire(member, 4, Duration.ofMillis(100)))
                        .isInstanceOf(org.awaitility.core.ConditionTimeoutException.class);
                assertThat(member.snapshot().metadataFrames()).isOne();
            }
            assertThat(member.snapshot().metadataFrames()).isZero();
        }
    }
    @Test void unrelatedAvailabilityAndBudgetFailuresPropagateAndReturnOnlyAcquiredSlots() {
        for (var failure : List.of(StorageRuntime.unavailable(), new TapstateException(io.tapstate.core.validation.ValidationError.VALUE_LIMIT_EXCEEDED,
                java.util.Map.of("limit", "validation.processBytes"), null))) {
            try (var process = new ValidationMemberResources()) {
                var calls = new java.util.concurrent.atomic.AtomicInteger();
                var member = process.bind("cluster", "node", "boot", 4, () -> { if (calls.incrementAndGet() == 3) throw failure; });
                assertThatThrownBy(() -> ValidationMetadataSlots.acquire(member, 4, Duration.ofSeconds(2))).isSameAs(failure);
                assertThat(member.snapshot().metadataFrames()).isZero();
            }
        }
    }

}
