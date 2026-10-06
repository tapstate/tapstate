package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PdkConfirmedCdcEndTest {

    @Test
    void anEndedReadCannotConfirmItsStillRunningConnectorOwnedAcknowledgement(@TempDir Path directory)
            throws Exception {
        try (Fixture fixture = new Fixture(directory, "ownedAcknowledgement")) {
            AtomicReference<SourcePosition> delivered = new AtomicReference<>();
            Subscription capture = fixture.port().cdc(fixture.config(), CaptureStart.present(), (rows, position) ->
                    position.ifPresent(delivered::set));
            try {
                assertThat(fixture.latch("readerEntered").await(5, TimeUnit.SECONDS)).isTrue();
                fixture.join("reader");
                assertThat(delivered.get()).as("the acknowledgement uses the position this source actually delivered")
                        .isNotNull();
                capture.acknowledge(delivered.get());
                fixture.latch("allowFlush").countDown();
                assertThat(fixture.latch("flushEntered").await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(fixture.thread("flushThread")).isSameAs(fixture.thread("callback"))
                        .isNotSameAs(Thread.currentThread());
                assertThat(fixture.thread("reader").isAlive()).isFalse();

                assertUnfinished(capture);
                assertThat(fixture.thread("callback").isAlive()).isTrue();
                assertUnfinished(capture);
                assertThat(fixture.counter("flushInterrupts").get())
                        .as("closing wakes the downstream handover but does not interrupt a connector-owned flush")
                        .isZero();

                fixture.latch("releaseFlush").countDown();
                fixture.join("callback");
                assertThatCode(capture::close).doesNotThrowAnyException();
                assertThatCode(capture::close).doesNotThrowAnyException();
            } finally {
                fixture.latch("allowFlush").countDown();
                fixture.latch("releaseFlush").countDown();
                fixture.join("reader"); fixture.join("callback"); capture.close();
            }
        }
    }

    @Test
    void aNormallyReturningStopCannotConfirmAStreamThatIgnoresInterrupts(@TempDir Path directory) throws Exception {
        try (Fixture fixture = new Fixture(directory, "stubbornRead")) {
            Subscription capture = fixture.port().cdc(fixture.config(), CaptureStart.present(), (rows, position) -> { });
            try {
                assertThat(fixture.latch("readerEntered").await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(fixture.thread("reader").isAlive()).isTrue();
                assertUnfinished(capture);
                assertThat(fixture.counter("stops").get()).isGreaterThanOrEqualTo(1);
                assertThat(fixture.thread("reader").isAlive()).isTrue();
                assertUnfinished(capture);
                assertThat(fixture.thread("reader").isAlive()).isTrue();
                fixture.latch("releaseRead").countDown(); fixture.join("reader");
                assertThatCode(capture::close).doesNotThrowAnyException();
                assertThatCode(capture::close).doesNotThrowAnyException();
            } finally {
                fixture.latch("releaseRead").countDown(); fixture.join("reader");
                capture.close();
            }
        }
    }

    @Test
    void anEndedReadDoesNotConfirmItsStillBlockedConnectorOwnedCallback(@TempDir Path directory) throws Exception {
        try (Fixture fixture = new Fixture(directory, "ownedCallback")) {
            Subscription capture = fixture.port().cdc(fixture.config(), CaptureStart.present(), (rows, position) -> {
                assertThat(rows).hasSize(1);
                fixture.latch("callbackEntered").countDown();
                while (true) {
                    try { fixture.latch("releaseCallback").await(); break; }
                    catch (InterruptedException ignored) { }
                }
            });
            try {
                assertThat(fixture.latch("readerEntered").await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(fixture.latch("callbackEntered").await(5, TimeUnit.SECONDS)).isTrue();
                fixture.join("reader");
                assertThat(fixture.thread("reader").isAlive()).isFalse();
                assertThat(fixture.thread("callback").isAlive()).isTrue();
                assertUnfinished(capture);
                assertThat(fixture.thread("callback").isAlive()).isTrue();
                assertUnfinished(capture);
                assertThat(fixture.thread("callback").isAlive()).isTrue();
                fixture.latch("releaseCallback").countDown(); fixture.join("callback");
                assertThatCode(capture::close).doesNotThrowAnyException();
                assertThatCode(capture::close).doesNotThrowAnyException();
            } finally {
                fixture.latch("releaseRead").countDown(); fixture.latch("releaseCallback").countDown();
                fixture.join("reader"); fixture.join("callback"); capture.close();
            }
        }
    }

    private static void assertUnfinished(Subscription capture) {
        assertThatThrownBy(capture::close).isInstanceOf(TapstateException.class).satisfies(thrown -> {
            TapstateException failure = (TapstateException) thrown;
            assertThat(failure.code()).isEqualTo(ConnectorError.CAPTURE_FAILED);
            assertThat(failure.args()).containsEntry("connector", "unfinished_cdc").containsKey("detail");
        });
    }

    private static final class Fixture implements AutoCloseable {
        private final String key = "unfinished-cdc-" + UUID.randomUUID();
        private final Map<String, Object> state = new ConcurrentHashMap<>();
        private final ConnectorRef reference;
        Fixture(Path directory, String mode) {
            reference = new ConnectorRef(List.of(UnfinishedCdcJars.source(directory)),
                    "synthetic.UnfinishedCdcSource", "2.0.8", null);
            state.put("mode", mode); state.put("stops", new AtomicInteger());
            state.put("flushInterrupts", new AtomicInteger());
            state.put("reader", new AtomicReference<Thread>()); state.put("callback", new AtomicReference<Thread>());
            state.put("flushThread", new AtomicReference<Thread>());
            state.put("callbackFailure", new AtomicReference<Throwable>());
            for (String name : List.of("readerEntered", "callbackEntered", "releaseRead", "releaseCallback",
                    "allowFlush", "flushEntered", "releaseFlush")) {
                state.put(name, new CountDownLatch(1));
            }
            assertThat(System.getProperties().put(key, state)).isNull();
        }
        PdkCapturePort port() { return new PdkCapturePort(ignored -> reference); }
        CaptureConfig config() { return new CaptureConfig("unfinished_cdc", Map.of("fixtureKey", key), List.of("t1")); }
        CountDownLatch latch(String name) { return (CountDownLatch) state.get(name); }
        AtomicInteger counter(String name) { return (AtomicInteger) state.get(name); }
        @SuppressWarnings("unchecked")
        Thread thread(String name) { return ((AtomicReference<Thread>) state.get(name)).get(); }
        void join(String name) throws InterruptedException {
            Thread actual = thread(name);
            assertThat(actual).as("the actual fixture-owned " + name + " thread was captured").isNotNull();
            actual.join(5_000);
            assertThat(actual.isAlive()).as("the actual fixture-owned " + name + " thread must end").isFalse();
        }
        @Override public void close() throws InterruptedException {
            latch("releaseRead").countDown(); latch("releaseCallback").countDown();
            latch("allowFlush").countDown(); latch("releaseFlush").countDown();
            try {
                if (thread("reader") != null) { join("reader"); }
                if (thread("callback") != null) { join("callback"); }
            } finally { System.getProperties().remove(key); }
        }
    }
}
