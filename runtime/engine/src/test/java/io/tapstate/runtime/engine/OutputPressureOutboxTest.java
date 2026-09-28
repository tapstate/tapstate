package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.lifecycle.Stage;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutputPressureOutboxTest {

    @Test
    void everyOfferOverloadKeepsItsArgumentsAndOutcomeWithoutDoubleCounting() {
        long[] now = {0};
        OutputPressureMetrics metrics = new OutputPressureMetrics(Stage.NEST, () -> now[0]);
        metrics.startScope(100);
        ScriptedOutbox original = new ScriptedOutbox(false, true, false, true);
        Outbox measured = new OutputPressureOutbox(original, metrics);
        Object row = new Object();
        int[] ordinals = {2, 0};
        assertThat(measured.bucketCount()).isEqualTo(3);
        assertThat(measured.offer(2, row)).isFalse();
        assertThat(original.ordinal).isEqualTo(2);
        assertThat(original.item).isSameAs(row);
        now[0] = 10;
        assertThat(measured.offer(ordinals, row)).isTrue();
        assertThat(original.ordinals).isSameAs(ordinals);
        assertThat(measured.offer(row)).isFalse();
        now[0] = 20;
        assertThat(measured.offer(row)).isTrue();
        assertThat(original.routes).containsExactly("ordinal", "array", "all", "all");
        assertThat(metrics.snapshot().refused()).isEqualTo(2);
        assertThat(metrics.snapshot().completed()).isEqualTo(2);
        assertThat(metrics.snapshot().sumNanos()).isEqualTo(20);
        original.unfinished = true;
        assertThat(measured.hasUnfinishedItem()).isTrue();
    }

    @Test
    void snapshotPersistenceCannotStartOrFinishAnOrdinaryOutputRetry() {
        long[] now = {0};
        OutputPressureMetrics metrics = new OutputPressureMetrics(Stage.SOURCE, () -> now[0]);
        metrics.startScope(100);
        ScriptedOutbox original = new ScriptedOutbox(false, true);
        Outbox measured = new OutputPressureOutbox(original, metrics);
        Watermark bound = new Watermark(100);
        assertThat(measured.offer(bound)).isFalse();
        original.snapshotAccepted = false;
        assertThat(measured.offerToSnapshot("key", "value")).isFalse();
        original.snapshotAccepted = true;
        assertThat(measured.offerToSnapshot("key", "value")).isTrue();
        assertThat(original.snapshotKey).isEqualTo("key");
        assertThat(original.snapshotValue).isEqualTo("value");
        assertThat(metrics.snapshot().completed()).isZero();
        assertThat(metrics.snapshot().refused()).isEqualTo(1);
        now[0] = 100;
        assertThat(measured.offer(bound)).isTrue();
        assertThat(metrics.snapshot().completed()).isEqualTo(1);
        assertThat(metrics.snapshot().sumNanos()).isEqualTo(100);
    }

    @Test
    void anOfferExceptionKeepsTheSameFailureAndDoesNotInventARefusalOrSuccessfulRetry() {
        OutputPressureMetrics metrics = new OutputPressureMetrics(Stage.JOIN, () -> 1);
        metrics.startScope(100);
        ScriptedOutbox original = new ScriptedOutbox(false);
        Outbox measured = new OutputPressureOutbox(original, metrics);
        assertThat(measured.offer("row")).isFalse();
        IllegalStateException failure = new IllegalStateException("test offer failed");
        original.failure = failure;
        assertThatThrownBy(() -> measured.offer("row")).isSameAs(failure);
        assertThat(metrics.snapshot().refused()).isEqualTo(1);
        assertThat(metrics.snapshot().completed()).isZero();
    }

    @Test
    void aFlatmapRetryCanCloseOneWaitAndOpenTheNextBeforeItsCallbackReturnsTrue() throws Exception {
        long[] now = {0};
        AtomicInteger transformed = new AtomicInteger();
        OutputPressureMetrics metrics = new OutputPressureMetrics(Stage.TRANSFORM, () -> now[0]);
        metrics.startScope(100);
        ScriptedOutbox original = new ScriptedOutbox(false, true, false, true, true);
        Envelope first = event(1);
        Envelope second = event(2);
        Envelope third = event(3);
        TransformProcessor processor = new TransformProcessor(input -> {
            transformed.incrementAndGet();
            return List.of(first, second, third);
        });
        processor.init(new OutputPressureOutbox(original, metrics), new TestProcessorContext());
        TestInbox inbox = new TestInbox();
        inbox.add(event(0));
        processor.process(0, inbox);
        assertThat(inbox.size()).isEqualTo(1);
        now[0] = 20;
        processor.process(0, inbox);
        assertThat(inbox.size()).isEqualTo(1);
        assertThat(metrics.snapshot().completed()).as("the old refused output has been accepted").isEqualTo(1);
        assertThat(metrics.snapshot().refused()).as("the following output is now refused").isEqualTo(2);
        now[0] = 30;
        processor.process(0, inbox);
        assertThat(inbox.size()).isZero();
        assertThat(original.accepted).containsExactly(first, second, third);
        assertThat(transformed).hasValue(1);
        assertThat(metrics.snapshot().completed()).isEqualTo(2);
        assertThat(metrics.snapshot().sumNanos()).isEqualTo(30);
    }

    @Test
    void noOutputAndDroppedTransformRowsRemainQuiet() throws Exception {
        OutputPressureMetrics metrics = new OutputPressureMetrics(Stage.TRANSFORM, () -> {
            throw new AssertionError("quiet output must not read a retry clock");
        });
        metrics.startScope(100);
        ScriptedOutbox original = new ScriptedOutbox();
        TransformProcessor processor = new TransformProcessor(input -> List.of());
        processor.init(new OutputPressureOutbox(original, metrics), new TestProcessorContext());
        TestInbox inbox = new TestInbox();
        processor.process(0, inbox);
        inbox.add(event(1));
        processor.process(0, inbox);
        assertThat(inbox.size()).isZero();
        assertThat(original.routes).isEmpty();
        assertThat(metrics.snapshot().refused()).isZero();
        assertThat(metrics.snapshot().completed()).isZero();
    }

    @Test
    void anOfferReturningAfterCloseAndResetCannotCountIntoTheReplacementScope() throws Exception {
        CountDownLatch offering = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<Boolean> accepted = new AtomicReference<>();
        OutputPressureMetrics metrics = new OutputPressureMetrics(Stage.SOURCE, () -> 1);
        metrics.startScope(100);
        ScriptedOutbox original = new ScriptedOutbox(false, true) {
            @Override public boolean offer(Object item) {
                if (!routes.isEmpty()) {
                    offering.countDown();
                    try {
                        if (!finish.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("delayed offer was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(interrupted);
                    }
                }
                return super.offer(item);
            }
        };
        Outbox measured = new OutputPressureOutbox(original, metrics);
        assertThat(measured.offer("row")).isFalse();
        Thread worker = new Thread(() -> {
            try {
                accepted.set(measured.offer("row"));
            } catch (Throwable problem) {
                failure.set(problem);
            }
        }, "output-pressure-test-delayed-offer");
        worker.start();
        try {
            assertThat(offering.await(5, TimeUnit.SECONDS)).isTrue();
            metrics.closeScope();
            assertThat(metrics.snapshot()).isNull();
            metrics.startScope(200);
        } finally {
            finish.countDown();
            worker.join(5_000);
        }
        assertThat(worker.isAlive()).isFalse();
        assertThat(failure.get()).isNull();
        assertThat(accepted.get()).isTrue();
        assertThat(metrics.snapshot().sinceMillis()).isEqualTo(200);
        assertThat(metrics.snapshot().refused()).isZero();
        assertThat(metrics.snapshot().completed()).isZero();
    }

    private static Envelope event(int id) {
        return Envelope.insert(id, "orders", Map.of("id", id), null);
    }

    private static class ScriptedOutbox implements Outbox {
        private final ArrayDeque<Boolean> results = new ArrayDeque<>();
        protected final List<String> routes = new ArrayList<>();
        private final List<Object> accepted = new ArrayList<>();
        private Object item;
        private int ordinal;
        private int[] ordinals;
        private Object snapshotKey;
        private Object snapshotValue;
        private boolean snapshotAccepted;
        private boolean unfinished;
        private RuntimeException failure;

        private ScriptedOutbox(boolean... outcomes) {
            for (boolean value : outcomes) { results.add(value); }
        }
        @Override public int bucketCount() { return 3; }
        @Override public boolean offer(int ordinal, Object item) {
            this.ordinal = ordinal;
            return offered("ordinal", item);
        }
        @Override public boolean offer(int[] ordinals, Object item) {
            this.ordinals = ordinals;
            return offered("array", item);
        }
        @Override public boolean offer(Object item) { return offered("all", item); }
        private boolean offered(String route, Object item) {
            this.item = item;
            routes.add(route);
            if (failure != null) { throw failure; }
            boolean result = results.removeFirst();
            if (result) { accepted.add(item); }
            return result;
        }
        @Override public boolean offerToSnapshot(Object key, Object value) {
            snapshotKey = key;
            snapshotValue = value;
            return snapshotAccepted;
        }
        @Override public boolean hasUnfinishedItem() { return unfinished; }
    }
}
