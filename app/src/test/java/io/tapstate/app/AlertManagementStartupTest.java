package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.control.core.ControlError;
import io.tapstate.spi.store.AlertStore;
import io.tapstate.spi.store.PipelineIntentStore;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AlertManagementStartupTest {
    @Test void managementWaitsForTheBackgroundInitializationWithoutOpeningAnotherStore() throws Exception {
        try (var startup = new Startup(); var callers = Executors.newSingleThreadExecutor()) {
            var pending = startup.request(callers);
            startup.release.countDown();
            assertThat(pending.get(3, TimeUnit.SECONDS)).isSameAs(startup.services);
            assertThat(startup.opens).hasValue(1);
            startup.runtime.close();
            assertThatThrownBy(startup.runtime::managementServices).isInstanceOfSatisfying(
                    TapstateException.class, error -> assertThat(error.code()).isEqualTo(ControlError.STORAGE_UNAVAILABLE));
        }
    }

    @Test void closingWakesManagementWhileTheInitialStoreReadIsStillBlocked() throws Exception {
        try (var startup = new Startup(); var callers = Executors.newSingleThreadExecutor()) {
            var pending = startup.request(callers);
            startup.runtime.close();
            assertThatThrownBy(() -> pending.get(3, TimeUnit.SECONDS)).isInstanceOfSatisfying(
                    ExecutionException.class, error -> assertThat(error.getCause()).isInstanceOfSatisfying(
                            TapstateException.class, coded -> assertThat(coded.code()).isEqualTo(ControlError.STORAGE_UNAVAILABLE)));
        }
    }

    @Test void ownershipIsRecheckedAcrossTheStartupBoundary() throws Exception {
        try (var startup = new Startup(); var callers = Executors.newSingleThreadExecutor()) {
            var pending = callers.submit(() -> {
                startup.gatedCaller.set(Thread.currentThread());
                return startup.runtime.managementServices();
            });
            try {
                assertThat(startup.firstGuardChecked.await(3, TimeUnit.SECONDS)).isTrue();
                startup.release.countDown();
                assertThat(startup.runtime.managementServices()).isSameAs(startup.services);
                startup.active.set(false);
                startup.releaseFirstGuard.countDown();
                assertThatThrownBy(() -> pending.get(3, TimeUnit.SECONDS)).isInstanceOfSatisfying(
                        ExecutionException.class, error -> assertThat(error.getCause()).isInstanceOfSatisfying(
                                TapstateException.class, coded -> assertThat(coded.code()).isEqualTo(ControlError.STORAGE_UNAVAILABLE)));
            } finally { startup.releaseFirstGuard.countDown(); }
        }
    }

    @Test @org.junit.jupiter.api.Timeout(10)
    void aTimedOutRequestDoesNotPreventALaterInitializationFromServingManagement() throws Exception {
        try (var startup = new Startup()) {
            assertThatThrownBy(startup.runtime::managementServices).isInstanceOfSatisfying(
                    TapstateException.class, error -> assertThat(error.code()).isEqualTo(ControlError.STORAGE_UNAVAILABLE));
            startup.release.countDown();
            assertThat(startup.runtime.managementServices()).isSameAs(startup.services);
        }
    }

    @Test void interruptingManagementDoesNotInterruptTheBackgroundInitialization() throws Exception {
        try (var startup = new Startup()) {
            Thread.currentThread().interrupt();
            try {
                assertThatThrownBy(startup.runtime::managementServices).isInstanceOfSatisfying(
                        TapstateException.class, error -> assertThat(error.code()).isEqualTo(ControlError.STORAGE_UNAVAILABLE));
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
            } finally { Thread.interrupted(); }
            startup.release.countDown();
            assertThat(startup.runtime.managementServices()).isSameAs(startup.services);
        }
    }

    private static final class Startup implements AutoCloseable {
        final CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        final AtomicBoolean active = new AtomicBoolean(true);
        final AtomicInteger opens = new AtomicInteger();
        final AtomicReference<Thread> gatedCaller = new AtomicReference<>();
        final CountDownLatch firstGuardChecked = new CountDownLatch(1), releaseFirstGuard = new CountDownLatch(1);
        final PipelineAlertRuntime.Services services;
        final PipelineAlertRuntime runtime;

        Startup() throws Exception {
            var store = mock(AlertStore.class);
            when(store.evaluationSnapshot(any(), anyInt())).thenAnswer(call -> {
                entered.countDown();
                if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("initial read was not released");
                return new AlertStore.EvaluationSnapshot(
                        new AlertStore.DefinitionPage(List.of(), null), new AlertStore.EvaluationPage(List.of(), null));
            });
            services = new PipelineAlertRuntime.Services(store, mock(AlertEvaluationHost.class));
            runtime = new PipelineAlertRuntime(() -> { opens.incrementAndGet(); return services; },
                    mock(PipelineIntentStore.class), (scope, claim) -> Optional.empty(), () -> {
                        if (!active.get()) throw new TapstateException(ControlError.STORAGE_UNAVAILABLE, Map.of(), null);
                        if (gatedCaller.compareAndSet(Thread.currentThread(), null)) {
                            firstGuardChecked.countDown();
                            try {
                                if (!releaseFirstGuard.await(10, TimeUnit.SECONDS)) throw new AssertionError("first guard was not released");
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                throw new AssertionError(interrupted);
                            }
                        }
                    }, Clock.systemUTC());
            runtime.start();
            assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
        }

        Future<PipelineAlertRuntime.Services> request(ExecutorService callers) throws Exception {
            var requested = new CountDownLatch(1);
            var pending = callers.submit(() -> { requested.countDown(); return runtime.managementServices(); });
            assertThat(requested.await(3, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> pending.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            return pending;
        }

        @Override public void close() { releaseFirstGuard.countDown(); release.countDown(); runtime.close(); }
    }
}
