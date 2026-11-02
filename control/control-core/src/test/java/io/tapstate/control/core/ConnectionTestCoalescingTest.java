package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.probe.ConnectionProbe;
import io.tapstate.spi.store.*;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ConnectionTestCoalescingTest {
    private static final Map<String,Object> SETTINGS = Map.of("uri", "mongodb://target/views");
    private static void releaseOrFail(CountDownLatch release) {
        try { if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("test did not release the probe"); }
        catch (InterruptedException cancelled) { Thread.currentThread().interrupt(); throw new AssertionError(cancelled); }
    }
    private static void awaitJoining(AtomicReference<Thread> follower, Future<?> request) {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < end) {
            var thread = follower.get();
            if (thread != null && thread.getState() == Thread.State.WAITING && !request.isDone()) return;
            java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        throw new AssertionError("the second request did not join the blocked managed test");
    }
    private static final class Fixture {
        final AtomicInteger probes = new AtomicInteger();
        final AtomicInteger audits = new AtomicInteger();
        final AtomicInteger saves = new AtomicInteger();
        final AtomicReference<Thread> first = new AtomicReference<>();
        final AtomicReference<Thread> follower = new AtomicReference<>();
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        ConnectionTestService service(Throwable failure) {
            ConnectionProbe probe = config -> {
                probes.incrementAndGet(); entered.countDown(); releaseOrFail(release);
                if (failure instanceof RuntimeException runtime) throw runtime;
                if (failure instanceof Error error) throw error;
                return new ConnectionTestResult(config.id(), config.connectorId(), ConnectionTestResult.Outcome.PASSED, List.of(), 1);
            };
            var store = new ConnectionTestResultStore() {
                @Override public void save(ConnectionTestResult result) { saves.incrementAndGet(); }
                @Override public Optional<ConnectionTestResult> find(String id) { return Optional.empty(); }
            };
            var observer = new ConnectionTestService.Observer() {
                @Override public boolean manages(ConnectionConfig config) {
                    var thread = Thread.currentThread();
                    if (!first.compareAndSet(null, thread) && first.get() != thread) follower.set(thread);
                    return true;
                }
                @Override public ConnectionTestService.Attempt begin(ConnectionConfig config) { return ConnectionTestService.Attempt.NONE; }
            };
            return new ConnectionTestService(probe, store, new AuditGate(ignored -> audits.incrementAndGet(), Clock.systemUTC()),
                    null, SourceConnectionResolver.forAdHocConnections(), observer);
        }
    }

    @Test void concurrentRequestsShareOneAuditedPhysicalTestAndOneReport() throws Exception {
        var f = new Fixture(); var service = f.service(null);
        try (var requests = Executors.newFixedThreadPool(2)) {
            var original = requests.submit(() -> service.test("views", "mongodb", SETTINGS, "first"));
            try {
                assertThat(f.entered.await(2, TimeUnit.SECONDS)).isTrue();
                var joined = requests.submit(() -> service.test("views", "mongodb", SETTINGS, "second"));
                awaitJoining(f.follower, joined);
                assertThat(f.probes).hasValue(1); assertThat(f.audits).hasValue(1); assertThat(f.saves).hasValue(0);
                f.release.countDown();
                assertThat(joined.get(2, TimeUnit.SECONDS)).isEqualTo(original.get(2, TimeUnit.SECONDS));
                assertThat(f.saves).hasValue(1);
            } finally { f.release.countDown(); }
        }
    }

    @Test void aJoinedRequestReceivesTheOriginalControlledFailureOrProgrammingDefect() throws Exception {
        for (Throwable failure : List.of(new TapstateException(ControlError.STORAGE_UNAVAILABLE, Map.of(), null), new LinkageError("fixture defect"))) {
            var f = new Fixture(); var service = f.service(failure);
            try (var requests = Executors.newFixedThreadPool(2)) {
                var original = requests.submit(() -> service.test("views", "mongodb", SETTINGS, "first"));
                try {
                    assertThat(f.entered.await(2, TimeUnit.SECONDS)).isTrue();
                    var joined = requests.submit(() -> service.test("views", "mongodb", SETTINGS, "second"));
                    awaitJoining(f.follower, joined); f.release.countDown();
                    assertThatThrownBy(() -> original.get(2, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                            .satisfies(error -> assertThat(error.getCause()).isSameAs(failure));
                    assertThatThrownBy(() -> joined.get(2, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                            .satisfies(error -> assertThat(error.getCause()).isSameAs(failure));
                    assertThat(f.probes).hasValue(1); assertThat(f.audits).hasValue(1); assertThat(f.saves).hasValue(0);
                } finally { f.release.countDown(); }
            }
        }
    }

    @Test void anotherConfigurationCannotJoinOrReplaceTheManagedTestInFlight() throws Exception {
        var f = new Fixture(); var service = f.service(null);
        try (var requests = Executors.newSingleThreadExecutor()) {
            var original = requests.submit(() -> service.test("views", "mongodb", SETTINGS, "first"));
            try {
                assertThat(f.entered.await(2, TimeUnit.SECONDS)).isTrue();
                var replacement = Map.<String,Object>of("uri", "mongodb://different/views");
                assertThatThrownBy(() -> service.test("views", "mongodb", replacement, "other")).isInstanceOf(TapstateException.class);
                assertThat(f.probes).hasValue(1); assertThat(f.audits).hasValue(1);
                f.release.countDown();
                assertThat(original.get(2, TimeUnit.SECONDS).outcome()).isEqualTo(ConnectionTestReport.Outcome.PASSED);
            } finally { f.release.countDown(); }
        }
    }
}
