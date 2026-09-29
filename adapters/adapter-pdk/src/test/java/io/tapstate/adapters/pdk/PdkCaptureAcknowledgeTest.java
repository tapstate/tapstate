package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * A cdc subscription told how far its source may release its change log. The position is only recorded
 * where it is acknowledged; the connector's flush function is handed it on the connector's own delivery
 * thread, read back into the connector's own offset, the latest one only, again every interval, and never
 * at the cost of the stream.
 *
 * <p>The source here reads on a thread of its own and delivers from it, as a polling connector does, and
 * delivers only when the case orders it to. So "handed over on the delivery thread" and "handed over only
 * when a delivery comes" are both observed rather than inferred from a stream that happened to be busy. Its
 * flush function records what it was handed, which loader that came from, and which thread called it; the
 * port's interval is measured on a clock that stands still until a case moves it.
 */
class PdkCaptureAcknowledgeTest {

    private static final Duration INTERVAL = Duration.ofSeconds(5);
    private static final long AWAIT_SECONDS = 10;

    /** Long enough for anything handed over off the delivery thread to have arrived. */
    private static final long IDLE_MILLIS = 300;

    private final AtomicLong clock = new AtomicLong();
    private final Channel channel = new Channel();

    @AfterEach
    void closeChannel() {
        channel.close();
    }

    @Test
    void anAcknowledgedPositionReachesTheFlushAsTheConnectorsOwnOffsetOnItsDeliveryThread(@TempDir Path dir)
            throws Exception {
        Recorder recorder = new Recorder();
        try (Subscription sub = open(Synthetic.acknowledgingSource(dir, channel.key), "AcknowledgingSource",
                recorder)) {
            SourcePosition durable = deliver(recorder, "rows").position().orElseThrow();

            sub.acknowledge(durable);
            deliver(recorder, "rows");

            assertThat(channel.flushes).as("one delivery since the acknowledgement, one flush").hasSize(1);
            FlushCall call = FlushCall.of(channel.flushes.get(0));
            assertThat(call.offset())
                    .as("the offset the connector named for that position, read back from the token -- not the "
                            + "token itself, which a connector passes over in silence")
                    .isEqualTo(channel.named.get(0));
            assertThat(call.offsetLoader())
                    .as("resolved through the connector's own loader, the only place its offset class exists")
                    .isSameAs(call.connectorLoader());
            assertThat(call.thread())
                    .as("handed over on the thread the connector delivers on, not the one that acknowledged")
                    .isSameAs(channel.deliveredOn.get(1))
                    .isNotSameAs(Thread.currentThread());
            assertThat(recorder.acknowledged).containsExactly(durable);
            assertThat(recorder.acknowledgeFailures).isEmpty();
        }
    }

    /**
     * An acknowledgement made while nothing is being delivered waits for the next delivery, and a heartbeat
     * is one.
     *
     * <p>Handing it over at once, from the thread that acknowledged or from one of its own, would drive the
     * connection the source is reading on from a second thread. Waiting instead for a change to arrive would
     * leave a quiet source unreleased for as long as it stays quiet -- which is when its log would otherwise
     * be growing for nothing -- so the heartbeat a quiet source still sends has to count.
     */
    @Test
    void anAcknowledgementMadeWhileTheStreamIsIdleWaitsForTheNextDeliveryAndAHeartbeatIsOne(@TempDir Path dir)
            throws Exception {
        Recorder recorder = new Recorder();
        try (Subscription sub = open(Synthetic.acknowledgingSource(dir, channel.key), "AcknowledgingSource",
                recorder)) {
            SourcePosition durable = deliver(recorder, "rows").position().orElseThrow();

            Thread acknowledger = new Thread(() -> sub.acknowledge(durable), "acknowledger");
            acknowledger.start();
            acknowledger.join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));
            assertThat(acknowledger.isAlive()).as("acknowledging returned without waiting for a delivery").isFalse();
            Thread.sleep(IDLE_MILLIS);
            assertThat(channel.flushes).as("nothing is handed over while nothing is delivered").isEmpty();

            Delivery heartbeat = deliver(recorder, "heartbeat");

            assertThat(heartbeat.events()).as("a heartbeat is passed on as an empty run").isEmpty();
            assertThat(channel.flushes).as("the heartbeat's delivery handed it over").hasSize(1);
            FlushCall call = FlushCall.of(channel.flushes.get(0));
            assertThat(call.offset()).isEqualTo(channel.named.get(0));
            assertThat(call.thread()).isSameAs(channel.deliveredOn.get(1)).isNotSameAs(acknowledger);
        }
    }

    /**
     * Of several positions acknowledged between two deliveries only the newest is handed over. Each covers
     * every one before it, so handing over the older ones as well would only walk the source through
     * positions it is already past.
     */
    @Test
    void ofSeveralAcknowledgementsBetweenTwoDeliveriesOnlyTheNewestIsHandedOver(@TempDir Path dir)
            throws Exception {
        Recorder recorder = new Recorder();
        try (Subscription sub = open(Synthetic.acknowledgingSource(dir, channel.key), "AcknowledgingSource",
                recorder)) {
            SourcePosition first = deliver(recorder, "rows").position().orElseThrow();
            SourcePosition second = deliver(recorder, "rows").position().orElseThrow();
            SourcePosition third = deliver(recorder, "rows").position().orElseThrow();
            assertThat(channel.flushes).as("nothing acknowledged yet, so nothing handed over").isEmpty();

            sub.acknowledge(first);
            sub.acknowledge(second);
            sub.acknowledge(third);
            deliver(recorder, "heartbeat");

            assertThat(channel.flushes).hasSize(1);
            assertThat(FlushCall.of(channel.flushes.get(0)).offset()).isEqualTo(channel.named.get(2));
            assertThat(recorder.acknowledged).containsExactly(third);
        }
    }

    /**
     * An unchanged position is handed over again once an interval, and no more often.
     *
     * <p>Again, because a connector gives no sign that it acted on a position and some drop one silently, so
     * handing each position over once could leave a source never released while everything reads as fine.
     * No more often, because every hand-over is a write to the source.
     */
    @Test
    void anUnchangedPositionIsHandedOverAgainOnceAnIntervalAndNoMoreOften(@TempDir Path dir) throws Exception {
        Recorder recorder = new Recorder();
        try (Subscription sub = open(Synthetic.acknowledgingSource(dir, channel.key), "AcknowledgingSource",
                recorder)) {
            SourcePosition durable = deliver(recorder, "rows").position().orElseThrow();
            sub.acknowledge(durable);

            deliver(recorder, "heartbeat");
            assertThat(channel.flushes).as("handed over on the first delivery after it").hasSize(1);

            clock.addAndGet(INTERVAL.toNanos() - 1);
            deliver(recorder, "heartbeat");
            assertThat(channel.flushes).as("not again within the interval").hasSize(1);

            clock.addAndGet(1);
            deliver(recorder, "heartbeat");
            assertThat(channel.flushes).as("again once the interval is up, although nothing changed").hasSize(2);
            assertThat(FlushCall.of(channel.flushes.get(1)).offset()).isEqualTo(channel.named.get(0));

            deliver(recorder, "heartbeat");
            assertThat(channel.flushes).as("and not on the delivery straight after").hasSize(2);
            assertThat(recorder.acknowledged).containsExactly(durable, durable);
        }
    }

    @Test
    void aConnectorWithoutAFlushFunctionIsLeftAloneAndNothingIsReported(@TempDir Path dir) throws Exception {
        Recorder recorder = new Recorder();
        try (Subscription sub = open(Synthetic.unacknowledgingSource(dir, channel.key), "UnacknowledgingSource",
                recorder)) {
            SourcePosition durable = deliver(recorder, "rows").position().orElseThrow();

            sub.acknowledge(durable);
            Delivery next = deliver(recorder, "rows");
            clock.addAndGet(INTERVAL.toNanos());
            deliver(recorder, "heartbeat");

            assertThat(next.events()).as("the stream delivers as it always did").hasSize(1);
            assertThat(recorder.acknowledged).isEmpty();
            assertThat(recorder.acknowledgeFailures)
                    .as("a connector that cannot be told is not one that failed to be")
                    .isEmpty();
            assertThat(recorder.errors).isEmpty();
        }
    }

    /**
     * A flush that throws is reported, coded, and the stream delivers on.
     *
     * <p>What the source not being told costs is some log kept a while longer. The batch the attempt was made
     * before is still delivered, the stream is not failed, and the attempt counts: the source is asked again
     * after an interval rather than on the very next delivery.
     */
    @Test
    void aFlushThatThrowsIsReportedCodedAndTheStreamDeliversOn(@TempDir Path dir) throws Exception {
        Recorder recorder = new Recorder();
        try (Subscription sub = open(Synthetic.refusingAcknowledgementSource(dir, channel.key),
                "RefusingAcknowledgementSource", recorder)) {
            SourcePosition durable = deliver(recorder, "rows").position().orElseThrow();
            sub.acknowledge(durable);

            Delivery delivered = deliver(recorder, "rows");

            assertThat(channel.flushes).as("the flush function was reached").hasSize(1);
            assertThat(delivered.events()).as("the batch it was tried before is still delivered").hasSize(1);
            assertThat(recorder.acknowledgeFailures).singleElement().satisfies(failure -> {
                assertThat(failure).isInstanceOf(TapstateException.class);
                TapstateException coded = (TapstateException) failure;
                assertThat(coded.code()).isEqualTo(ConnectorError.ACKNOWLEDGE_FAILED);
                assertThat(coded.args()).containsEntry("connector", "demo").containsKey("detail");
                assertThat(coded.getCause())
                        .as("the connector's own failure, kept underneath the code")
                        .hasMessage("flush boom");
            });
            assertThat(recorder.acknowledged).isEmpty();
            assertThat(recorder.errors).as("a failed acknowledgement is not a failed stream").isEmpty();

            deliver(recorder, "heartbeat");
            assertThat(channel.flushes).as("a failed attempt counts: not retried within the interval").hasSize(1);

            clock.addAndGet(INTERVAL.toNanos());
            Delivery later = deliver(recorder, "rows");
            clock.addAndGet(Duration.ofMinutes(1).toNanos());
            deliver(recorder, "heartbeat");

            assertThat(channel.flushes).as("retried once each interval is up").hasSize(3);
            assertThat(recorder.acknowledgeFailures).hasSize(3);
            assertThat(later.events()).hasSize(1);
            assertThat(recorder.errors).isEmpty();
        }
    }

    /**
     * A position the connector cannot read back is reported, coded, and never reaches the connector. The
     * token is only ever this adapter's rendering of an offset the connector issued, and a connector handed
     * anything else passes it over in silence -- the failure would then be a source that never releases.
     */
    @Test
    void aPositionTheConnectorCannotReadBackIsReportedCodedAndTheStreamDeliversOn(@TempDir Path dir)
            throws Exception {
        Recorder recorder = new Recorder();
        try (Subscription sub = open(Synthetic.acknowledgingSource(dir, channel.key), "AcknowledgingSource",
                recorder)) {
            deliver(recorder, "rows");

            sub.acknowledge(new SourcePosition("not-a-position"));
            Delivery delivered = deliver(recorder, "rows");

            assertThat(channel.flushes).as("nothing but the connector's own offset is handed to it").isEmpty();
            assertThat(delivered.events()).as("the batch is still delivered").hasSize(1);
            assertThat(recorder.acknowledgeFailures).singleElement().satisfies(failure -> {
                assertThat(failure).isInstanceOf(TapstateException.class);
                TapstateException coded = (TapstateException) failure;
                assertThat(coded.code()).isEqualTo(ConnectorError.ACKNOWLEDGE_FAILED);
                assertThat(coded.getCause()).isInstanceOfSatisfying(TapstateException.class,
                        unreadable -> assertThat(unreadable.code()).isEqualTo(ConnectorError.POSITION_UNREADABLE));
            });
            assertThat(recorder.errors).isEmpty();
        }
    }

    /**
     * Once the subscription is closed nothing more is handed over, even to a delivery the source still makes
     * on its way down: the connector is being stopped, and releasing on its way out would be the one act
     * taken after it was told to stop.
     */
    @Test
    void aClosedSubscriptionHandsOverNothingEvenToADeliveryMadeOnTheWayDown(@TempDir Path dir) throws Exception {
        Recorder recorder = new Recorder();
        Subscription sub = open(Synthetic.acknowledgingSource(dir, channel.key), "AcknowledgingSource", recorder);
        SourcePosition durable;
        try {
            durable = deliver(recorder, "rows").position().orElseThrow();
            sub.acknowledge(durable);
        } finally {
            sub.close();
        }

        Delivery onTheWayDown = recorder.next();
        assertThat(onTheWayDown.events()).as("the source's last delivery, made while it stopped").isEmpty();
        assertThat(channel.flushes).as("a delivery made after close hands nothing over").isEmpty();
        assertThat(recorder.acknowledged).isEmpty();
        assertThatCode(() -> sub.acknowledge(durable))
                .as("acknowledging a closed subscription does nothing, and says nothing")
                .doesNotThrowAnyException();
    }

    // ---- harness -----------------------------------------------------------------------------------------

    /** Starts a stream at the present over the synthetic {@code simpleName}, on the case's clock. */
    private Subscription open(Path jar, String simpleName, CaptureListener listener) {
        ConnectorRef ref = new ConnectorRef(List.of(jar), "synthetic." + simpleName, "2.0.8", null);
        PdkCapturePort port = new PdkCapturePort(connectorId -> ref, null,
                PdkCapturePort.DEFAULT_PREFLIGHT_TIMEOUT, INTERVAL, clock::get);
        return port.cdc(new CaptureConfig("demo", Map.of(), List.of("t1")), CaptureStart.present(), listener);
    }

    /** Orders one delivery and waits until it has reached the listener. */
    private Delivery deliver(Recorder recorder, String order) throws InterruptedException {
        channel.orders.add(order);
        return recorder.next();
    }

    /**
     * The case's end of the channel the source takes its orders from and reports through, kept in the system
     * properties because the connector runs in a loader of its own and those are one thing both sides reach.
     */
    private static final class Channel implements AutoCloseable {

        private final String key = "synthetic.polling." + System.nanoTime();
        private final BlockingQueue<String> orders = new LinkedBlockingQueue<>();
        /** The offsets the source named, in the order it named them. */
        private final List<Object> named = new CopyOnWriteArrayList<>();
        /** The thread each delivery was made on, in order. */
        private final List<Thread> deliveredOn = new CopyOnWriteArrayList<>();
        /** One entry per flush call, as the source recorded it. */
        private final List<Map<String, Object>> flushes = new CopyOnWriteArrayList<>();

        private Channel() {
            System.getProperties().put(key, Map.of(
                    "orders", orders, "named", named, "deliveredOn", deliveredOn, "flushes", flushes));
        }

        @Override
        public void close() {
            System.getProperties().remove(key);
        }
    }

    /** One call the source's flush function recorded. */
    private record FlushCall(Object offset, ClassLoader offsetLoader, ClassLoader connectorLoader, Thread thread) {

        static FlushCall of(Map<String, Object> recorded) {
            return new FlushCall(recorded.get("offset"), (ClassLoader) recorded.get("offsetLoader"),
                    (ClassLoader) recorded.get("connectorLoader"), (Thread) recorded.get("thread"));
        }
    }

    /** One run the stream handed to the listener. */
    private record Delivery(List<Envelope> events, Optional<SourcePosition> position) {
    }

    /** The listener the stream is started with: what it was handed, and what it heard about releases. */
    private static final class Recorder implements CaptureListener {

        private final BlockingQueue<Delivery> deliveries = new LinkedBlockingQueue<>();
        private final List<SourcePosition> acknowledged = new CopyOnWriteArrayList<>();
        private final List<Throwable> acknowledgeFailures = new CopyOnWriteArrayList<>();
        private final List<Throwable> errors = new CopyOnWriteArrayList<>();

        @Override
        public void onBatch(List<Envelope> events, Optional<SourcePosition> position) {
            deliveries.add(new Delivery(events, position));
        }

        @Override
        public void onAcknowledged(SourcePosition position) {
            acknowledged.add(position);
        }

        @Override
        public void onAcknowledgeFailed(Throwable failure) {
            acknowledgeFailures.add(failure);
        }

        @Override
        public void onError(Throwable error) {
            errors.add(error);
        }

        /** The next run the stream delivered, waiting for it to arrive. */
        Delivery next() throws InterruptedException {
            Delivery delivery = deliveries.poll(AWAIT_SECONDS, TimeUnit.SECONDS);
            assertThat(delivery).as("the source delivered within %d seconds", AWAIT_SECONDS).isNotNull();
            return delivery;
        }
    }
}
