package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MongoPipelineWritesTest {
    @Test void aQueuedRevokedWriterNeverStartsItsTransaction() throws Exception {
        var gates = new MongoPipelineWrites();
        var held = new CountDownLatch(1); var release = new CountDownLatch(1); var checked = new CountDownLatch(1);
        var revoked = new AtomicBoolean(); var reached = new AtomicBoolean();
        try (var workers = Executors.newFixedThreadPool(2)) {
            var first = workers.submit(() -> gates.call("pipeline", () -> {}, () -> {
                held.countDown(); await(release); return 1;
            }));
            try {
                assertThat(held.await(5, TimeUnit.SECONDS)).isTrue();
                var second = workers.submit(() -> gates.call("pipeline", () -> {
                    checked.countDown();
                    if (revoked.get()) throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
                }, () -> { reached.set(true); return 2; }));
                assertThat(checked.await(5, TimeUnit.SECONDS)).isTrue(); revoked.set(true);
                assertThatThrownBy(() -> second.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class)
                        .satisfies(error -> assertThat(((TapstateException) error.getCause()).code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
                assertThat(reached).isFalse();
            } finally { release.countDown(); }
            assertThat(first.get(5, TimeUnit.SECONDS)).isEqualTo(1);
        }
    }

    @Test void aFailedWriterReleasesAdmissionAndAnotherPipelineIsIndependent() throws Exception {
        var gates = new MongoPipelineWrites();
        var held = new CountDownLatch(1); var release = new CountDownLatch(1);
        try (var workers = Executors.newFixedThreadPool(2)) {
            var first = workers.submit(() -> gates.call("a", () -> {}, () -> {
                held.countDown(); await(release); throw new IllegalStateException("witness");
            }));
            try {
                assertThat(held.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(workers.submit(() -> gates.call("b", () -> {}, () -> 2)).get(5, TimeUnit.SECONDS)).isEqualTo(2);
            } finally { release.countDown(); }
            assertThatThrownBy(() -> first.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(IllegalStateException.class);
            assertThat(gates.call("a", () -> {}, () -> 3)).isEqualTo(3);
        }
    }

    @Test void interruptionIsPreservedAndDoesNotStartAQueuedTransaction() throws Exception {
        var gates = new MongoPipelineWrites();
        var held = new CountDownLatch(1); var release = new CountDownLatch(1); var waiting = new CountDownLatch(1);
        var interrupted = new AtomicBoolean(); var reached = new AtomicBoolean();
        try (var worker = Executors.newSingleThreadExecutor()) {
            var first = worker.submit(() -> gates.call("p", () -> {}, () -> { held.countDown(); await(release); return 1; }));
            assertThat(held.await(5, TimeUnit.SECONDS)).isTrue();
            Thread queued = new Thread(() -> {
                try {
                    gates.call("p", waiting::countDown, () -> { reached.set(true); return 2; });
                } catch (TapstateException failure) {
                    interrupted.set(Thread.currentThread().isInterrupted() && failure.code() == IoError.STORE_UNAVAILABLE);
                }
            });
            try {
                queued.start(); assertThat(waiting.await(5, TimeUnit.SECONDS)).isTrue(); queued.interrupt(); queued.join(5000);
                assertThat(queued.isAlive()).isFalse(); assertThat(interrupted).isTrue(); assertThat(reached).isFalse();
            } finally { release.countDown(); queued.interrupt(); queued.join(5000); }
            first.get(5, TimeUnit.SECONDS);
        }
    }

    private static void await(CountDownLatch latch) {
        try { if (!latch.await(5, TimeUnit.SECONDS)) throw new AssertionError("barrier expired"); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
    }
}
