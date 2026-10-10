package io.tapstate.e2e;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnClockSamplerTest {
    private static final BenchmarkCausalClock.Identity OWNER = new BenchmarkCausalClock.Identity(17, 1000);
    private static final Duration CONTROL_DELAY = Duration.ofHours(1);

    @Test void first_and_final_reads_are_serial_actual_points_from_one_daemon_worker() {
        List<Long> sequences = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        var sampler = new BenchmarkReturnClockSampler(sequence -> {
            sequences.add(sequence); threads.add(Thread.currentThread()); return sample(sequence);
        }, CONTROL_DELAY);
        sampler.start();
        assertThat(sampler.readings()).containsExactly(sample(0));
        var result = sampler.finishAfterSuccessfulStop();
        assertThat(sequences).containsExactly(0L, 1L);
        assertThat(threads.getFirst()).isSameAs(threads.getLast());
        assertThat(threads.getFirst().isDaemon()).isTrue();
        assertThat(result).containsExactly(sample(0), sample(1));
        assertThat(sampler.evidence()).containsEntry("firstRecorded", true).containsEntry("finalRecorded", true)
                .containsEntry("workerExited", true).containsEntry("performanceAcceptanceEligible", false)
                .containsEntry("samplingCostQualified", false);
        assertThatThrownBy(result::clear).isInstanceOf(UnsupportedOperationException.class);
        sampler.close();
        assertThatThrownBy(sampler::start).isInstanceOf(AssertionError.class);
        assertThatThrownBy(sampler::finishAfterSuccessfulStop).isInstanceOf(AssertionError.class);
        assertThat(sequences).hasSize(2);
    }

    @Test void actual_default_is_fixed_at_fifty_milliseconds_and_is_recorded_without_qualification() {
        var sampler = new BenchmarkReturnClockSampler(BenchmarkReturnClockSamplerTest::sample);
        assertThat(BenchmarkReturnClockSampler.SAMPLE_DELAY).isEqualTo(Duration.ofMillis(50));
        assertThat(sampler.evidence()).containsEntry("fixedDelayNanos", 50_000_000L)
                .containsEntry("performanceAcceptanceEligible", false);
        sampler.close();
        assertThat(sampler.readings()).isEmpty();
        assertThatThrownBy(sampler::start).isInstanceOf(AssertionError.class);
    }

    @Test void equal_counter_resolution_is_retained_but_foreign_identity_and_backward_values_are_refused() {
        var flat = new BenchmarkReturnClockSampler(sequence -> new BenchmarkCausalClock.Sample(sequence, OWNER,
                sequence * 100, sequence * 100 + 10, -1000), CONTROL_DELAY);
        flat.start();
        assertThat(flat.finishAfterSuccessfulStop()).extracting(BenchmarkCausalClock.Sample::ownedNanos)
                .containsExactly(-1000L, -1000L);
        flat.close();
        List<LongFunction<BenchmarkCausalClock.Sample>> malformed = List.of(
                sequence -> new BenchmarkCausalClock.Sample(sequence, sequence == 0 ? OWNER
                        : new BenchmarkCausalClock.Identity(17, 1001), sequence * 100, sequence * 100 + 10, sequence * 1000),
                sequence -> new BenchmarkCausalClock.Sample(sequence, OWNER, sequence * 100, sequence * 100 + 10,
                        sequence == 0 ? 100 : 99),
                sequence -> new BenchmarkCausalClock.Sample(sequence, OWNER, sequence == 0 ? 100 : 109,
                        sequence == 0 ? 110 : 120, sequence * 1000),
                sequence -> sequence == 0 ? sample(0) : sample(0),
                sequence -> sequence == 0 ? sample(0) : null);
        for (var reader : malformed) {
            var sampler = new BenchmarkReturnClockSampler(reader, CONTROL_DELAY); sampler.start();
            assertThatThrownBy(sampler::finishAfterSuccessfulStop)
                    .isInstanceOf(AssertionError.class).hasMessageContaining("UNKNOWN");
            assertThat(sampler.evidence()).containsEntry("state", "UNKNOWN");
            assertThatThrownBy(sampler::close).isInstanceOf(AssertionError.class);
        }
    }

    @Test void malformed_request_brackets_and_coverage_overflow_do_not_create_final_proof() {
        for (boolean overflow : List.of(false, true)) {
            var sampler = new BenchmarkReturnClockSampler(sequence -> {
                if (!overflow) { return new BenchmarkCausalClock.Sample(sequence, OWNER, 10, 9, 100); }
                return sequence == 0
                        ? new BenchmarkCausalClock.Sample(0, OWNER, 0, 10, Long.MIN_VALUE)
                        : new BenchmarkCausalClock.Sample(1, OWNER, 100, 110, Long.MAX_VALUE);
            }, CONTROL_DELAY);
            if (overflow) {
                sampler.start();
                assertThatThrownBy(sampler::finishAfterSuccessfulStop).isInstanceOf(AssertionError.class).hasMessageContaining("UNKNOWN");
            } else {
                assertThatThrownBy(sampler::start).isInstanceOf(AssertionError.class).hasMessageContaining("UNKNOWN");
            }
            assertThat(sampler.evidence()).containsEntry("finalRecorded", false);
            assertThatThrownBy(sampler::close).isInstanceOf(AssertionError.class);
        }
    }

    @Test void reader_exceptions_are_not_retried_or_promoted_to_zero_readings() {
        AtomicInteger calls = new AtomicInteger();
        var sampler = new BenchmarkReturnClockSampler(sequence -> {
            calls.incrementAndGet(); throw new IllegalStateException("controlled unavailable getter");
        }, CONTROL_DELAY);
        assertThatThrownBy(sampler::start).isInstanceOf(AssertionError.class).hasMessageContaining("UNKNOWN");
        assertThat(sampler.readings()).isEmpty(); assertThat(calls).hasValue(1);
        assertThatThrownBy(sampler::close).isInstanceOf(AssertionError.class);
        assertThat(calls).hasValue(1);
    }

    @Test void capacity_exhaustion_refuses_final_coverage_without_a_five_hundred_thirteenth_read() throws Exception {
        AtomicInteger calls = new AtomicInteger(); CountDownLatch last = new CountDownLatch(1);
        var sampler = new BenchmarkReturnClockSampler(sequence -> {
            calls.incrementAndGet(); if (sequence == 511) { last.countDown(); } return sample(sequence);
        }, Duration.ZERO);
        try { sampler.start(); } catch (AssertionError capacity) { assertThat(capacity).hasMessageContaining("CAPACITY"); }
        assertThat(last.await(2, TimeUnit.SECONDS)).isTrue();
        assertThatThrownBy(sampler::finishAfterSuccessfulStop).isInstanceOf(AssertionError.class).hasMessageContaining("CAPACITY");
        assertThatThrownBy(sampler::close).isInstanceOf(AssertionError.class);
        assertThat(calls).hasValue(512); assertThat(sampler.readings()).hasSize(512);
    }

    @Test void a_stuck_borrowed_reader_has_bounded_refused_close_and_cannot_keep_the_JVM_alive() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), returned = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger(); AtomicReference<Thread> worker = new AtomicReference<>();
        var sampler = new BenchmarkReturnClockSampler(sequence -> {
            calls.incrementAndGet(); worker.set(Thread.currentThread());
            if (sequence == 1) {
                entered.countDown(); awaitUninterruptibly(release); returned.countDown();
            }
            return sample(sequence);
        }, Duration.ZERO);
        var closer = Executors.newSingleThreadExecutor();
        try {
            sampler.start(); assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            var refusal = closer.submit(() -> {
                try { sampler.close(); throw new AssertionError("stuck close passed"); }
                catch (AssertionError expected) { assertThat(expected).hasMessageContaining("remains stuck"); }
            });
            refusal.get(3, TimeUnit.SECONDS);
            assertThat(worker.get().isDaemon()).isTrue();
            assertThat(sampler.evidence()).containsEntry("state", "UNKNOWN").containsEntry("workerExited", false);
            assertThat(calls).hasValue(2);
            release.countDown(); assertThat(returned.await(2, TimeUnit.SECONDS)).isTrue();
            worker.get().join(2000); assertThat(worker.get().isAlive()).isFalse();
            assertThat(calls).hasValue(2); assertThat(sampler.readings()).containsExactly(sample(0));
        } finally {
            release.countDown(); closer.shutdownNow();
        }
    }

    @Test void an_interruptible_in_flight_read_is_aborted_once_and_no_later_read_is_issued() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger(); AtomicReference<Thread> worker = new AtomicReference<>();
        var sampler = new BenchmarkReturnClockSampler(sequence -> {
            calls.incrementAndGet(); worker.set(Thread.currentThread());
            if (sequence == 1) {
                entered.countDown();
                try { release.await(); } catch (InterruptedException interrupted) { throw new IllegalStateException(interrupted); }
            }
            return sample(sequence);
        }, Duration.ZERO);
        try {
            sampler.start(); assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(sampler::close).isInstanceOf(AssertionError.class).hasMessageContaining("UNKNOWN");
            worker.get().join(2000); assertThat(worker.get().isAlive()).isFalse();
            assertThat(calls).hasValue(2); assertThat(sampler.evidence()).containsEntry("finalRecorded", false);
            assertThatThrownBy(sampler::finishAfterSuccessfulStop).isInstanceOf(AssertionError.class);
        } finally { release.countDown(); }
    }

    @Test void plain_close_after_the_first_point_cannot_masquerade_as_a_successful_final_sample() {
        AtomicInteger calls = new AtomicInteger();
        var sampler = new BenchmarkReturnClockSampler(sequence -> { calls.incrementAndGet(); return sample(sequence); }, CONTROL_DELAY);
        sampler.start();
        assertThatThrownBy(sampler::close).isInstanceOf(AssertionError.class).hasMessageContaining("UNKNOWN");
        assertThat(calls).hasValue(1);
        assertThat(sampler.evidence()).containsEntry("finalRecorded", false).containsEntry("workerExited", true);
    }

    private static BenchmarkCausalClock.Sample sample(long sequence) {
        return new BenchmarkCausalClock.Sample(sequence, OWNER, sequence * 100, sequence * 100 + 10, -10_000 + sequence * 1000);
    }
    private static void awaitUninterruptibly(CountDownLatch release) {
        boolean interrupted = false;
        while (true) {
            try { release.await(); break; }
            catch (InterruptedException ignored) { interrupted = true; }
        }
        if (interrupted) { Thread.currentThread().interrupt(); }
    }
}
