package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PipelinePassExecutorTest {
    @Test void repeatedTicksNeverOverlapTheSamePipeline() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var calls = new AtomicInteger();
        try (var passes = new PipelinePassExecutor(2)) {
            try {
                assertThat(passes.submit("one", () -> {
                    calls.incrementAndGet(); entered.countDown(); await(release);
                })).isTrue();
                assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
                for (int tick = 0; tick < 20; tick++) {
                    assertThat(passes.submit("one", calls::incrementAndGet)).isFalse();
                    passes.runInline("one", calls::incrementAndGet);
                }
                assertThat(calls).hasValue(1);
                assertThat(passes.activePipelines()).containsExactly("one");
            } finally { release.countDown(); }
        }
    }

    @Test void saturationRejectsWorkWithoutQueueingAndCapacityCanBeRetried() throws Exception {
        var entered = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var later = new CountDownLatch(1);
        try (var passes = new PipelinePassExecutor(2)) {
            try {
                for (String id : new String[]{"one", "two"})
                    assertThat(passes.submit(id, () -> { entered.countDown(); await(release); })).isTrue();
                assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
                assertThat(passes.submit("later", later::countDown)).isFalse();
                assertThat(passes.activePipelines()).containsExactlyInAnyOrder("one", "two");
                assertThat(later.getCount()).isEqualTo(1);
            } finally { release.countDown(); }
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2))
                    .until(() -> passes.submit("later", later::countDown));
            assertThat(later.await(2, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test void aFailedInlinePassReleasesItsReservation() {
        try (var passes = new PipelinePassExecutor(2)) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> passes.runInline("one", () -> {
                throw new IllegalStateException("controlled failure");
            })).isInstanceOf(IllegalStateException.class);
            assertThat(passes.activePipelines()).isEmpty();
            var calls = new AtomicInteger();
            passes.runInline("one", calls::incrementAndGet);
            assertThat(calls).hasValue(1);
        }
    }

    @Test void shutdownInterruptsOwnedWorkAndRejectsLaterAdmission() throws Exception {
        var entered = new CountDownLatch(1);
        var interrupted = new CountDownLatch(1);
        var passes = new PipelinePassExecutor(2);
        try {
            assertThat(passes.submit("one", () -> {
                entered.countDown();
                try { new CountDownLatch(1).await(); }
                catch (InterruptedException stopped) { interrupted.countDown(); Thread.currentThread().interrupt(); }
            })).isTrue();
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            passes.close();
            assertThat(interrupted.getCount()).isZero();
            assertThat(passes.activePipelines()).isEmpty();
            assertThat(passes.submit("later", () -> { throw new AssertionError("closed executor ran work"); })).isFalse();
        } finally { passes.close(); }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) throw new AssertionError("test barrier timed out");
        } catch (InterruptedException stopped) {
            Thread.currentThread().interrupt();
        }
    }
}
