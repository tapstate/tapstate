package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

class StoredCountSamplerTest {

    private static final class AdjustableClock extends Clock {
        private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));

        void advance(Duration duration) {
            now.updateAndGet(value -> value.plus(duration));
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return Clock.fixed(now.get(), zone);
        }

        @Override
        public Instant instant() {
            return now.get();
        }
    }

    private static StoredCountSampler bounded(StoredCountSampler.Counter counter, Clock clock,
                                              int maxKeys, int queueCapacity) {
        return new StoredCountSampler(counter, clock, maxKeys, queueCapacity,
                Duration.ofSeconds(15), Duration.ofSeconds(30), Duration.ofSeconds(5));
    }

    private static void until(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }

    private static void waitForRelease(CountDownLatch latch) {
        try {
            assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void waitForReleaseDespiteInterrupt(CountDownLatch latch) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (latch.getCount() != 0L && System.nanoTime() < deadline) {
            try {
                if (latch.await(100, TimeUnit.MILLISECONDS)) {
                    return;
                }
            } catch (InterruptedException ignored) {
                // The sampler must close even when the store does not stop on interruption.
            }
        }
        assertThat(latch.getCount()).isZero();
    }

    @Test
    void blocked_count_does_not_hold_up_sample_or_an_unrelated_key() {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (StoredCountSampler sampler = new StoredCountSampler((database, namespace) -> {
            if (namespace.equals("slow")) {
                entered.countDown();
                waitForRelease(release);
                return 4L;
            }
            return 9L;
        }, new AdjustableClock())) {
            assertThat(sampler.sample("p", 1L, "db", "slow")).isEmpty();
            await(entered);
            assertTimeoutPreemptively(Duration.ofSeconds(1),
                    () -> assertThat(sampler.sample("p", 1L, "db", "slow")).isEmpty());
            assertThat(sampler.sample("p", 1L, "db", "fast")).isEmpty();
            until(() -> sampler.sample("p", 1L, "db", "fast").isPresent());
            assertThat(sampler.sample("p", 1L, "db", "fast").orElseThrow().value()).isEqualTo(9L);
            until(() -> sampler.health().active() == 1);
            assertThat(sampler.health().active()).isEqualTo(1);
        } finally {
            release.countDown();
        }
    }

    @Test
    void refresh_is_single_flight_and_keeps_the_last_fresh_completion_available() {
        AdjustableClock clock = new AdjustableClock();
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch refreshEntered = new CountDownLatch(1);
        CountDownLatch releaseRefresh = new CountDownLatch(1);
        try (StoredCountSampler sampler = new StoredCountSampler((database, namespace) -> {
            int call = calls.incrementAndGet();
            if (call == 2) {
                refreshEntered.countDown();
                waitForRelease(releaseRefresh);
            }
            return call == 1 ? 7L : 8L;
        }, clock)) {
            assertThat(sampler.sample("p", 1L, "db", "ns")).isEmpty();
            until(() -> sampler.health().completed() == 1L);
            for (int index = 0; index < 20; index++) {
                assertThat(sampler.sample("p", 1L, "db", "ns").orElseThrow().value()).isEqualTo(7L);
            }
            assertThat(calls).hasValue(1);

            clock.advance(Duration.ofSeconds(15));
            assertThat(sampler.sample("p", 1L, "db", "ns").orElseThrow().value()).isEqualTo(7L);
            await(refreshEntered);
            for (int index = 0; index < 20; index++) {
                assertThat(sampler.sample("p", 1L, "db", "ns").orElseThrow().value()).isEqualTo(7L);
            }
            assertThat(calls).hasValue(2);
            releaseRefresh.countDown();
            until(() -> sampler.health().completed() == 2L);
            assertThat(sampler.sample("p", 1L, "db", "ns").orElseThrow().value()).isEqualTo(8L);
            assertThat(sampler.health().totalDurationNanos()).isGreaterThanOrEqualTo(0L);
        } finally {
            releaseRefresh.countDown();
        }
    }

    @Test
    void failure_retries_after_cooldown_and_expired_count_is_absent() {
        AdjustableClock clock = new AdjustableClock();
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch thirdEntered = new CountDownLatch(1);
        CountDownLatch releaseThird = new CountDownLatch(1);
        try (StoredCountSampler sampler = new StoredCountSampler((database, namespace) -> {
            int call = calls.incrementAndGet();
            if (call == 1) {
                throw new IllegalStateException("store unavailable");
            }
            if (call == 3) {
                thirdEntered.countDown();
                waitForRelease(releaseThird);
            }
            return 5L;
        }, clock)) {
            assertThat(sampler.sample("p", 1L, "db", "ns")).isEmpty();
            until(() -> sampler.health().failed() == 1L);
            for (int index = 0; index < 10; index++) {
                assertThat(sampler.sample("p", 1L, "db", "ns")).isEmpty();
            }
            assertThat(calls).hasValue(1);

            clock.advance(Duration.ofSeconds(5));
            assertThat(sampler.sample("p", 1L, "db", "ns")).isEmpty();
            until(() -> sampler.health().completed() == 1L);
            assertThat(sampler.sample("p", 1L, "db", "ns").orElseThrow().value()).isEqualTo(5L);

            clock.advance(Duration.ofSeconds(30));
            assertThat(sampler.sample("p", 1L, "db", "ns")).isEmpty();
            await(thirdEntered);
            assertThat(sampler.sample("p", 1L, "db", "ns")).isEmpty();
            assertThat(calls).hasValue(3);
            releaseThird.countDown();
            until(() -> sampler.health().completed() == 2L);
            assertThat(sampler.sample("p", 1L, "db", "ns").orElseThrow().value()).isEqualTo(5L);
        } finally {
            releaseThird.countDown();
        }
    }

    @Test
    void failed_refresh_keeps_a_still_fresh_sample_with_its_original_time() {
        AdjustableClock clock = new AdjustableClock();
        AtomicInteger calls = new AtomicInteger();
        try (StoredCountSampler sampler = new StoredCountSampler((database, namespace) -> {
            if (calls.incrementAndGet() == 2) {
                throw new IllegalStateException("refresh unavailable");
            }
            return 7L;
        }, clock)) {
            assertThat(sampler.sample("p", 1L, "db", "ns")).isEmpty();
            until(() -> sampler.health().completed() == 1L);
            StoredCountSampler.Sample first = sampler.sample("p", 1L, "db", "ns").orElseThrow();

            clock.advance(Duration.ofSeconds(15));
            assertThat(sampler.sample("p", 1L, "db", "ns")).contains(first);
            until(() -> sampler.health().failed() == 1L);
            assertThat(sampler.sample("p", 1L, "db", "ns")).contains(first);

            clock.advance(Duration.ofSeconds(15));
            assertThat(sampler.sample("p", 1L, "db", "ns")).isEmpty();
        }
    }

    @Test
    void workers_queue_and_lru_keys_stay_bounded_under_pressure() {
        AdjustableClock clock = new AdjustableClock();
        CountDownLatch workersEntered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        try (StoredCountSampler sampler = bounded((database, namespace) -> {
            workersEntered.countDown();
            waitForRelease(release);
            return 1L;
        }, clock, 3, 1)) {
            assertThat(sampler.sample("p", 1L, "db", "one")).isEmpty();
            assertThat(sampler.sample("p", 1L, "db", "two")).isEmpty();
            await(workersEntered);
            assertThat(sampler.sample("p", 1L, "db", "three")).isEmpty();
            assertThat(sampler.health().queued()).isEqualTo(1);
            assertThat(sampler.health().active()).isEqualTo(2);
            assertThat(sampler.sample("p", 1L, "db", "four")).isEmpty();
            assertThat(sampler.health().rejected()).isEqualTo(1L);
            assertThat(sampler.retainedKeys()).isEqualTo(3);

            release.countDown();
            until(() -> sampler.health().completed() == 3L);
            clock.advance(Duration.ofSeconds(5));
            assertThat(sampler.sample("p", 1L, "db", "two")).isPresent();
            assertThat(sampler.sample("p", 1L, "db", "three")).isPresent();
            assertThat(sampler.sample("p", 1L, "db", "one")).isPresent();
            assertThat(sampler.sample("p", 1L, "db", "four")).isEmpty();
            assertThat(sampler.retainedKeys()).isEqualTo(3);
            assertThat(sampler.sample("p", 1L, "db", "two")).isEmpty();
            assertThat(sampler.retainedKeys()).isEqualTo(3);
        } finally {
            release.countDown();
        }
    }

    @Test
    void forgetting_a_pipeline_discards_late_old_work_without_affecting_a_new_job() {
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        try (StoredCountSampler sampler = new StoredCountSampler((database, namespace) -> {
            int call = calls.incrementAndGet();
            if (call == 1) {
                firstEntered.countDown();
                waitForRelease(releaseFirst);
                return 11L;
            }
            return call == 2 ? 22L : 33L;
        }, new AdjustableClock())) {
            assertThat(sampler.sample("p", 1L, "db", "ns")).isEmpty();
            await(firstEntered);
            sampler.forget("p");
            assertThat(sampler.retainedKeys()).isZero();

            assertThat(sampler.sample("p", 2L, "db", "ns")).isEmpty();
            until(() -> sampler.sample("p", 2L, "db", "ns").isPresent());
            assertThat(sampler.sample("p", 2L, "db", "ns").orElseThrow().value()).isEqualTo(22L);
            releaseFirst.countDown();
            until(() -> sampler.health().completed() == 2L);
            assertThat(sampler.sample("p", 2L, "db", "ns").orElseThrow().value()).isEqualTo(22L);

            assertThat(sampler.sample("p", 1L, "db", "ns")).isEmpty();
            until(() -> sampler.sample("p", 1L, "db", "ns").isPresent());
            assertThat(sampler.sample("p", 1L, "db", "ns").orElseThrow().value()).isEqualTo(33L);
        } finally {
            releaseFirst.countDown();
        }
    }

    @Test
    void close_interrupts_workers_and_returns_without_waiting_for_a_blocked_store() {
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        StoredCountSampler sampler = bounded((database, namespace) -> {
            calls.incrementAndGet();
            entered.countDown();
            waitForReleaseDespiteInterrupt(release);
            return 3L;
        }, new AdjustableClock(), 3, 1);
        try {
            assertThat(sampler.sample("p", 1L, "db", "one")).isEmpty();
            assertThat(sampler.sample("p", 1L, "db", "two")).isEmpty();
            await(entered);
            assertThat(sampler.sample("p", 1L, "db", "three")).isEmpty();
            assertTimeoutPreemptively(Duration.ofSeconds(1), sampler::close);
            assertThat(sampler.sample("p", 1L, "db", "four")).isEmpty();
            assertThat(calls).hasValue(2);
        } finally {
            release.countDown();
            sampler.close();
        }
    }
}
