package io.tapstate.app;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.tapstate.control.core.CloudRuntimeStatus;
import io.tapstate.control.core.CloudStatusReporter;
import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.support.GenericApplicationContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class CloudStatusLifecycleTest {

    private static final CloudRuntimeStatus STATUS = new CloudRuntimeStatus("test-version", 100, 2, null);
    private final GenericApplicationContext owner = new GenericApplicationContext();
    private final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    private final ScheduledFuture<?> future = mock(ScheduledFuture.class);
    private final List<Runnable> scheduled = new ArrayList<>();
    private CloudStatusLifecycle lifecycle;

    @AfterEach
    void stop() {
        if (lifecycle != null) {
            lifecycle.close();
        }
        owner.close();
    }

    @Test
    void onlyTheOwningReadyEventCreatesOneSerializedThirtySecondHeartbeatTask() {
        List<String> sent = new ArrayList<>();
        AtomicInteger nonces = new AtomicInteger();
        CloudStatusReporter reporter = new CloudStatusReporter("validated-cluster", () -> STATUS,
                (cluster, nonce, status) -> sent.add(cluster + ":" + nonce),
                () -> "nonce-" + nonces.incrementAndGet());
        bind(reporter);

        verifyNoInteractions(executor);
        try (var foreign = new GenericApplicationContext()) {
            lifecycle.onApplicationEvent(ready(foreign));
        }
        verifyNoInteractions(executor);
        lifecycle.onApplicationEvent(ready(owner));
        lifecycle.onApplicationEvent(ready(owner));
        assertThat(scheduled).hasSize(1);
        assertThat(sent).isEmpty();
        scheduled.getFirst().run();
        scheduled.getFirst().run();
        assertThat(sent).containsExactly("validated-cluster:nonce-1", "validated-cluster:nonce-2");
        verify(executor, times(1)).scheduleWithFixedDelay(any(Runnable.class), eq(0L), eq(30L), eq(TimeUnit.SECONDS));
    }

    @Test
    void disabledReportingNeverCreatesAWorkerOrCallsAReporter() {
        AtomicInteger allocations = new AtomicInteger();
        lifecycle = new CloudStatusLifecycle(null, () -> {
            allocations.incrementAndGet();
            return executor;
        });
        lifecycle.setApplicationContext(owner);
        lifecycle.onApplicationEvent(ready(owner));

        assertThat(lifecycle.enabled()).isFalse();
        assertThat(allocations.get()).isZero();
        verifyNoInteractions(executor);
    }

    @Test
    void closingCancelsTheOwnedTaskAndLateTicksCannotSendOrRestartIt() {
        AtomicInteger calls = new AtomicInteger();
        bind(new CloudStatusReporter("validated-cluster", () -> STATUS,
                (cluster, nonce, status) -> calls.incrementAndGet(), () -> "nonce"));
        lifecycle.onApplicationEvent(ready(owner));
        scheduled.getFirst().run();
        lifecycle.close();
        lifecycle.close();
        scheduled.getFirst().run();
        lifecycle.onApplicationEvent(ready(owner));

        assertThat(calls.get()).isEqualTo(1);
        assertThat(scheduled).hasSize(1);
        verify(future, times(1)).cancel(true);
        verify(executor, times(1)).shutdownNow();
    }

    @Test
    void closingBeforeReadyDoesNotAllocateAWorker() {
        bind(new CloudStatusReporter("validated-cluster", () -> STATUS,
                (cluster, nonce, status) -> { throw new AssertionError("must not send"); }, () -> "nonce"));
        lifecycle.close();
        lifecycle.onApplicationEvent(ready(owner));

        assertThat(scheduled).isEmpty();
        verifyNoInteractions(executor);
    }

    @Test
    void codedProviderFailureIsIsolatedWithoutLoggingItsParamsOrCauseOrRetryingLocally() {
        AtomicInteger calls = new AtomicInteger();
        bind(new CloudStatusReporter("validated-cluster", () -> STATUS, (cluster, nonce, status) -> {
            if (calls.incrementAndGet() == 1) {
                throw new TapstateException(ControlError.MALFORMED_REQUEST,
                        Map.of("reason", "provider-param-secret-sentinel"),
                        new IllegalArgumentException("provider-cause-secret-sentinel"));
            }
        }, () -> "nonce"));
        lifecycle.onApplicationEvent(ready(owner));
        Logger logger = (Logger) LoggerFactory.getLogger(CloudStatusLifecycle.class);
        ListAppender<ILoggingEvent> logs = new ListAppender<>();
        logs.start();
        logger.addAppender(logs);
        try {
            scheduled.getFirst().run();
            assertThat(calls.get()).as("transport retry is the SDK's responsibility").isEqualTo(1);
            scheduled.getFirst().run();
            assertThat(calls.get()).isEqualTo(2);
            assertThat(logs.list).singleElement().satisfies(event -> {
                assertThat(event.getFormattedMessage()).isEqualTo("Cloud status report failed [control.malformed-request]");
                assertThat(event.getThrowableProxy()).isNull();
            });
        } finally {
            logger.detachAppender(logs);
            logs.stop();
        }
    }

    @Test
    void anUncodedProgrammingFaultIsNotLaunderedIntoAnAvailabilityDiagnostic() {
        IllegalStateException defect = new IllegalStateException("broken provider invariant");
        bind(new CloudStatusReporter("validated-cluster", () -> { throw defect; },
                (cluster, nonce, status) -> { throw new AssertionError("must not send"); }, () -> "nonce"));
        lifecycle.onApplicationEvent(ready(owner));

        assertThatThrownBy(() -> scheduled.getFirst().run()).isSameAs(defect);
    }

    @Test
    void aFailingDiagnosticHookPreservesTheOriginalWorkerDefect() {
        IllegalStateException defect = new IllegalStateException("original worker defect");
        IllegalArgumentException observerFailure = new IllegalArgumentException("diagnostic hook defect");
        bind(new CloudStatusReporter("validated-cluster", () -> { throw defect; },
                (cluster, nonce, status) -> { throw new AssertionError("must not send"); }, () -> "nonce"),
                (thread, failure) -> {
                    assertThat(failure).isSameAs(defect);
                    throw observerFailure;
                });
        lifecycle.onApplicationEvent(ready(owner));

        assertThatThrownBy(() -> scheduled.getFirst().run()).isSameAs(defect);
        assertThat(defect.getSuppressed()).containsExactly(observerFailure);
    }

    @Test
    void aRealPeriodicExecutorSurfacesAnUncodedWorkerDefectWithItsOriginalIdentity() throws Exception {
        IllegalStateException defect = new IllegalStateException("broken periodic provider invariant");
        AtomicReference<Throwable> observed = new AtomicReference<>();
        CountDownLatch attempted = new CountDownLatch(1);
        CountDownLatch diagnosed = new CountDownLatch(1);
        ScheduledExecutorService real = Executors.newSingleThreadScheduledExecutor(work -> {
            Thread worker = new Thread(work, "cloud-status-lifecycle-witness");
            worker.setDaemon(true);
            worker.setUncaughtExceptionHandler((thread, failure) -> {
                observed.set(failure);
                diagnosed.countDown();
            });
            return worker;
        });
        CloudStatusReporter reporter = new CloudStatusReporter("validated-cluster", () -> {
            attempted.countDown();
            throw defect;
        },
                (cluster, nonce, status) -> { throw new AssertionError("must not send"); }, () -> "nonce");
        lifecycle = new CloudStatusLifecycle(reporter, () -> real);
        lifecycle.setApplicationContext(owner);
        try {
            lifecycle.onApplicationEvent(ready(owner));
            assertThat(attempted.await(2, TimeUnit.SECONDS)).as("the actual periodic task executed").isTrue();
            assertThat(diagnosed.await(2, TimeUnit.SECONDS))
                    .as("ScheduledFuture failures must not silently suppress every later heartbeat").isTrue();
            assertThat(observed.get()).isSameAs(defect);
        } finally {
            lifecycle.close();
            real.shutdownNow();
            assertThat(real.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
        }
    }

    private void bind(CloudStatusReporter reporter) {
        bind(reporter, (thread, failure) -> { });
    }

    private void bind(CloudStatusReporter reporter, java.util.function.BiConsumer<Thread, Throwable> defects) {
        when(executor.scheduleWithFixedDelay(any(Runnable.class), eq(0L), eq(30L), eq(TimeUnit.SECONDS)))
                .thenAnswer(call -> {
                    scheduled.add(call.getArgument(0));
                    return future;
                });
        lifecycle = new CloudStatusLifecycle(reporter, () -> executor, defects);
        lifecycle.setApplicationContext(owner);
    }

    private static ApplicationReadyEvent ready(GenericApplicationContext context) {
        return new ApplicationReadyEvent(new SpringApplication(CloudRuntimeConfiguration.class),
                new String[0], context, Duration.ZERO);
    }
}
