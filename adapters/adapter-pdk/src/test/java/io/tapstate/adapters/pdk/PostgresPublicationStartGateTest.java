package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.capture.CaptureConfig;
import java.sql.SQLException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PostgresPublicationStartGateTest {

    @Test
    void waitingIsBoundedAndIgnoresNodeTableAndSettingsIterationOrder() throws Exception {
        PostgresPublicationStartGate gate = new PostgresPublicationStartGate(Duration.ofMillis(50));
        Map<String, Object> ordered = new LinkedHashMap<>();
        ordered.put("database", "db"); ordered.put("password", "unpublished-secret");
        ordered.put("options", Map.of("two", 2, "one", 1)); ordered.put("nullable", null);
        Map<String, Object> reversed = new LinkedHashMap<>();
        reversed.put("nullable", null); reversed.put("options", Map.of("one", 1, "two", 2));
        reversed.put("password", "unpublished-secret"); reversed.put("database", "db");
        CaptureConfig first = new CaptureConfig("postgres", ordered, List.of("orders"), new PipelineNode("a", "source"));
        CaptureConfig second = new CaptureConfig("postgres", reversed, List.of("mail"), new PipelineNode("b", "other"));
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicInteger attempted = new AtomicInteger();
        try (var callers = Executors.newSingleThreadExecutor()) {
            var held = callers.submit(() -> call(gate, first, () -> {
                entered.countDown();
                if (!release.await(5, TimeUnit.SECONDS)) { throw new AssertionError("the owner was not released"); }
                return 1;
            }));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> gate.sample(second, () -> attempted.incrementAndGet()))
                        .isInstanceOf(TapstateException.class).satisfies(thrown -> {
                            TapstateException failure = (TapstateException) thrown;
                            assertThat(failure.code()).isEqualTo(ConnectorError.CAPTURE_FAILED);
                            assertThat(failure.args()).containsEntry("connector", "postgres");
                            assertThat(failure.args().get("detail").toString()).contains("50ms");
                            assertThat(failure.toString()).doesNotContain("unpublished-secret");
                        });
                assertThat(attempted).hasValue(0);
            } finally { release.countDown(); }
            assertThat(held.get(5, TimeUnit.SECONDS)).isEqualTo(1);
            assertThat(call(gate, second, attempted::incrementAndGet)).isEqualTo(1);
        }
    }

    @Test
    void anInterruptedWaitDoesNotInvokePreparationAndRestoresItsInterrupt() throws Exception {
        PostgresPublicationStartGate gate = new PostgresPublicationStartGate(Duration.ofSeconds(5));
        CaptureConfig config = new CaptureConfig("postgres", Map.of("database", "cancel"), List.of());
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), waiting = new CountDownLatch(1);
        AtomicReference<Throwable> refused = new AtomicReference<>();
        AtomicBoolean restored = new AtomicBoolean();
        AtomicInteger attempted = new AtomicInteger();
        try (var callers = Executors.newSingleThreadExecutor()) {
            var held = callers.submit(() -> call(gate, config, () -> {
                entered.countDown();
                if (!release.await(5, TimeUnit.SECONDS)) { throw new AssertionError("the owner was not released"); }
                return null;
            }));
            Thread waiter = new Thread(() -> {
                waiting.countDown();
                try { gate.sample(config, attempted::incrementAndGet); }
                catch (Throwable failure) { refused.set(failure); restored.set(Thread.currentThread().isInterrupted()); }
            }, "publication-start-wait-control");
            waiter.setDaemon(true);
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                waiter.start(); assertThat(waiting.await(5, TimeUnit.SECONDS)).isTrue(); waiter.interrupt();
                waiter.join(5_000);
                assertThat(waiter.isAlive()).isFalse();
                assertThat(refused.get()).isInstanceOf(TapstateException.class).satisfies(thrown -> {
                    TapstateException failure = (TapstateException) thrown;
                    assertThat(failure.code()).isEqualTo(ConnectorError.CAPTURE_FAILED);
                    assertThat(failure.getCause()).isInstanceOf(InterruptedException.class);
                });
                assertThat(restored).isTrue(); assertThat(attempted).hasValue(0);
            } finally { release.countDown(); waiter.interrupt(); waiter.join(5_000); }
            held.get(5, TimeUnit.SECONDS);
            assertThat(call(gate, config, attempted::incrementAndGet)).isEqualTo(1);
        }
    }

    @Test
    void nativeFailuresIncludingInterruptionEscapeUnchangedAndReleaseTheTurn() throws Throwable {
        PostgresPublicationStartGate gate = new PostgresPublicationStartGate(Duration.ofSeconds(1));
        CaptureConfig config = new CaptureConfig("postgres", Map.of("database", "native-failure"), List.of());
        SQLException denied = new SQLException("permission denied", "42501");
        assertThatThrownBy(() -> gate.sample(config, () -> { throw denied; })).isSameAs(denied);
        InterruptedException nativeFailure = new InterruptedException("native preparation interruption");
        assertThatThrownBy(() -> gate.sample(config, () -> { throw nativeFailure; })).isSameAs(nativeFailure);
        assertThat(gate.sample(config, () -> 7)).isEqualTo(7);
    }

    private static <T> T call(PostgresPublicationStartGate gate, CaptureConfig config, PdkConnector.Action<T> action)
            throws Exception {
        try { return gate.sample(config, action); }
        catch (Exception | Error failure) { throw failure; }
        catch (Throwable unexpected) { throw new AssertionError("unexpected native throwable", unexpected); }
    }
}
