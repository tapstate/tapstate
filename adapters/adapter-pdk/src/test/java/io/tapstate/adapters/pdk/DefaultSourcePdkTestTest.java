package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.ConnectionTestResult;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DefaultSourcePdkTestTest {
    private static final ConnectionConfig CONFIG = new ConnectionConfig("views", "mongodb", Map.of());
    private static ConnectionTestResult passed() {
        return new ConnectionTestResult("views", "mongodb", ConnectionTestResult.Outcome.PASSED, List.of(), System.currentTimeMillis());
    }
    private static DefaultSourcePdkTest executor() { return new DefaultSourcePdkTest(Clock.systemUTC(), Duration.ofMillis(30), Duration.ofMillis(40)); }
    private static void await(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) try { if (!latch.await(3, TimeUnit.SECONDS)) throw new AssertionError("fixture not released"); break; }
        catch (InterruptedException ignored) { interrupted = true; }
        if (interrupted) Thread.currentThread().interrupt();
    }
    private static void awaitAvailable(DefaultSourcePdkTest executor) throws InterruptedException {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!executor.available() && System.nanoTime() < end) Thread.sleep(1);
        assertThat(executor.available()).isTrue();
    }
    @Test void anInterruptedObserverStillReceivesAnAlreadyPublishedProgrammingDefect() throws Exception {
        var observer = Thread.currentThread(); var release = new CountDownLatch(1);
        var observed = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        var defect = new NullPointerException("published before observer interruption");
        try (var executor = new DefaultSourcePdkTest(Clock.systemUTC(), Duration.ofSeconds(2), Duration.ofMillis(100))) {
            var call = new DefaultSourcePdkTest.Call() {
                @Override public ConnectionTestResult run() {
                    observed.set(defect); observer.interrupt(); await(release); throw defect;
                }
                @Override public void cancel() { release.countDown(); }
                @Override public boolean resourcesReleased() { return true; }
                @Override public Throwable observedFailure() { return observed.get(); }
            };
            try {
                assertThatThrownBy(() -> executor.test(CONFIG, call)).isSameAs(defect);
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
            } finally { Thread.interrupted(); release.countDown(); }
            awaitAvailable(executor);
        } finally { Thread.interrupted(); release.countDown(); }
    }

    @Test void completedTestReturnsItsReportAndReleasesTheOneSlot() {
        try (var executor = executor()) {
            var result = executor.test(CONFIG, new DefaultSourcePdkTest.Call() {
                @Override public ConnectionTestResult run() { return passed(); }
                @Override public void cancel() { throw new AssertionError("must not cancel"); }
                @Override public boolean resourcesReleased() { return true; }
            });
            assertThat(result.outcome()).isEqualTo(ConnectionTestResult.Outcome.PASSED);
            assertThat(executor.available()).isTrue();
        }
    }
    @Test void cooperativeTimeoutDiscardsALatePassAndReturnsFailedOnlyAfterExit() {
        try (var executor = executor()) {
            var cancelled = new CountDownLatch(1); var exited = new AtomicBoolean();
            var result = executor.test(CONFIG, new DefaultSourcePdkTest.Call() {
                @Override public ConnectionTestResult run() { try { await(cancelled); return passed(); } finally { exited.set(true); } }
                @Override public void cancel() { cancelled.countDown(); }
                @Override public boolean resourcesReleased() { return exited.get(); }
            });
            assertThat(result.outcome()).isEqualTo(ConnectionTestResult.Outcome.FAILED);
            assertThat(result.items().getFirst().connectorErrorCode()).isEqualTo(ConnectorError.TEST_TIMEOUT.code());
            assertThat(exited).isTrue(); assertThat(executor.available()).isTrue();
        }
    }
    @Test void ignoredInterruptKeepsTheSlotUntilTheActualCallExits() throws Exception {
        var release = new CountDownLatch(1); var exited = new AtomicBoolean();
        try (var executor = executor()) {
            var call = new DefaultSourcePdkTest.Call() {
                @Override public ConnectionTestResult run() { try { await(release); return passed(); } finally { exited.set(true); } }
                @Override public void cancel() {}
                @Override public boolean resourcesReleased() { return exited.get(); }
            };
            assertThatThrownBy(() -> executor.test(CONFIG, call)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(ConnectorError.TEST_TIMEOUT));
            assertThat(exited).isFalse(); assertThat(executor.available()).isFalse();
            assertThatThrownBy(() -> executor.test(CONFIG, call)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(ConnectorError.INSTANCES_BUSY));
            release.countDown(); awaitAvailable(executor);
        } finally { release.countDown(); }
    }
    @Test void aBlockedCleanupKeepsTheSlotEvenAfterTheWorkerExits() throws Exception {
        var releaseWorker = new CountDownLatch(1); var releaseCleanup = new CountDownLatch(1); var exited = new AtomicBoolean();
        try (var executor = executor()) {
            var call = new DefaultSourcePdkTest.Call() {
                @Override public ConnectionTestResult run() { try { await(releaseWorker); return passed(); } finally { exited.set(true); } }
                @Override public void cancel() { releaseWorker.countDown(); await(releaseCleanup); }
                @Override public boolean resourcesReleased() { return exited.get(); }
            };
            assertThatThrownBy(() -> executor.test(CONFIG, call)).isInstanceOf(TapstateException.class);
            assertThat(exited).isTrue(); assertThat(executor.available()).isFalse();
            releaseCleanup.countDown(); awaitAvailable(executor);
        } finally { releaseWorker.countDown(); releaseCleanup.countDown(); }
    }
    @Test void programmingDefectsEscapeAsTheirOriginalTypes() {
        for (Throwable defect : List.of(new NullPointerException("bug"), new IllegalStateException("invariant"), new LinkageError("link"))) {
            try (var executor = executor()) {
                var call = new DefaultSourcePdkTest.Call() {
                    @Override public ConnectionTestResult run() {
                        if (defect instanceof RuntimeException runtime) throw runtime;
                        throw (Error)defect;
                    }
                    @Override public void cancel() {}
                    @Override public boolean resourcesReleased() { return true; }
                };
                assertThatThrownBy(() -> executor.test(CONFIG, call)).isSameAs(defect);
                assertThat(executor.available()).isTrue();
            }
        }
    }
    @Test void cleanupProgrammingDefectsEscapeEvenWhileTheirResourceSlotRemainsUnknown() {
        for (Throwable defect : List.of(new NullPointerException("stop"), new IllegalStateException("stop"), new LinkageError("stop"))) {
            try (var executor = executor()) {
                var release = new CountDownLatch(1);
                var call = new DefaultSourcePdkTest.Call() {
                    @Override public ConnectionTestResult run() { await(release); return passed(); }
                    @Override public void cancel() {
                        release.countDown();
                        if (defect instanceof RuntimeException runtime) throw runtime;
                        throw (Error)defect;
                    }
                    @Override public boolean resourcesReleased() { return false; }
                };
                assertThatThrownBy(() -> executor.test(CONFIG, call)).isSameAs(defect);
                assertThat(executor.available()).isFalse();
            }
        }
    }
    @Test void aWorkerDefectIsPreservedEvenWhenNormalCloseWasNotConfirmed() {
        var defect = new IllegalStateException("close");
        try (var executor = executor()) {
            assertThatThrownBy(() -> executor.test(CONFIG, new DefaultSourcePdkTest.Call() {
                @Override public ConnectionTestResult run() { throw defect; }
                @Override public void cancel() {}
                @Override public boolean resourcesReleased() { return false; }
            })).isSameAs(defect);
            assertThat(executor.available()).isFalse();
        }
    }

    @Test void anObservedProgrammingDefectSurvivesBlockedNormalCleanup() throws Exception {
        var release = new CountDownLatch(1); var observed = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        var released = new AtomicBoolean(); var defect = new NullPointerException("original test defect");
        try (var executor = executor()) {
            var call = new DefaultSourcePdkTest.Call() {
                @Override public ConnectionTestResult run() {
                    observed.set(defect); await(release); released.set(true); throw defect;
                }
                @Override public void cancel() {}
                @Override public boolean resourcesReleased() { return released.get(); }
                @Override public Throwable observedFailure() { return observed.get(); }
            };
            assertThatThrownBy(() -> executor.test(CONFIG, call)).isSameAs(defect);
            assertThat(executor.available()).isFalse();
            release.countDown(); awaitAvailable(executor);
        } finally { release.countDown(); }
    }

    @Test void anInterruptedReturnCannotBePublishedAsSuccess() {
        try (var executor = executor()) {
            assertThatThrownBy(() -> executor.test(CONFIG, new DefaultSourcePdkTest.Call() {
                @Override public ConnectionTestResult run() { Thread.currentThread().interrupt(); return passed(); }
                @Override public void cancel() {}
                @Override public boolean resourcesReleased() { return true; }
            })).isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(ConnectorError.TEST_TIMEOUT));
            assertThat(executor.available()).isTrue();
        }
    }

    @Test void aCleanupFailureIsNotProofOfResourceRelease() {
        try (var executor = executor()) {
            assertThatThrownBy(() -> executor.test(CONFIG, new DefaultSourcePdkTest.Call() {
                @Override public ConnectionTestResult run() { throw new TapstateException(ConnectorError.TLS_MATERIAL_CLEANUP_FAILED, Map.of("connector", "mongodb"), null); }
                @Override public void cancel() {}
                @Override public boolean resourcesReleased() { return false; }
            })).isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(ConnectorError.TEST_TIMEOUT));
            assertThat(executor.available()).isFalse();
        }
    }
}
