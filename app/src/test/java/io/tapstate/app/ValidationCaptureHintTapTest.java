package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationMode;
import io.tapstate.core.validation.ValidationOwner;
import io.tapstate.core.validation.ValidationRootHintBatch;
import io.tapstate.core.validation.ValidationRootHintBatch.Change;
import io.tapstate.core.validation.ValidationRunIdentity;
import io.tapstate.runtime.engine.validation.ValidationRootHintInbox;
import io.tapstate.runtime.engine.validation.ValidationRootHintOutbox;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CaptureStartedListener;
import io.tapstate.spi.capture.SourcePosition;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class ValidationCaptureHintTapTest {
    private static final ValidationCaptureHintTap.Limits LIMITS = new ValidationCaptureHintTap.Limits(1000, 4, 1_048_576);

    @Test
    void normalThousandEventBatchIsSplitIntoBoundedPacketsWithoutChangingSourceDelivery() {
        var events = IntStream.range(0, 1000).mapToObj(i -> insert("orders", i)).toList();
        var position = Optional.of(new SourcePosition("opaque-original"));
        var calls = new AtomicInteger();
        try (var outbox = outbox("one", 16, 128, 32768)) {
            CaptureListener original = (batch, token) -> {
                calls.incrementAndGet(); assertThat(batch).isSameAs(events); assertThat(token).isSameAs(position);
            };
            var tap = ValidationCaptureHintTap.wrap(original, List.of(binding(outbox)), LIMITS);
            tap.onBatch(events, position);
            assertThat(calls).hasValue(1); assertThat(outbox.failure()).isEmpty();
            assertThat(outbox.snapshot().packets()).isEqualTo(8);
            var received = new ArrayList<ValidationKey>();
            while (true) {
                var next = outbox.poll(); if (next.isEmpty()) break;
                var lease = next.orElseThrow();
                assertThat(lease.batch().changes()).hasSizeLessThanOrEqualTo(128);
                for (Change change : lease.batch().changes()) received.add(change.after());
                outbox.settle(lease, accepted(lease));
            }
            assertThat(received).containsExactlyElementsOf(IntStream.range(0, 1000).mapToObj(ValidationCaptureHintTapTest::key).toList());
        }
    }

    @Test
    void actualMoveNeverSplitsItsOldAndNewKeysBetweenPackets() {
        try (var outbox = outbox("one", 4, 1, 32768)) {
            var tap = ValidationCaptureHintTap.wrap((batch, token) -> {}, List.of(binding(outbox)), LIMITS);
            tap.onBatch(List.of(Envelope.update(0, "orders", Map.of("id", 1L), Map.of("id", 2L), Map.of()),
                    Envelope.update(0, "orders", Map.of("id", 2L), Map.of("id", 3L), Map.of())), Optional.empty());
            var first = outbox.poll().orElseThrow();
            assertThat(first.batch().changes()).containsExactly(new Change(ValidationRootHintBatch.ChangeKind.UPDATE, key(1), key(2)));
            outbox.settle(first, accepted(first));
            var second = outbox.poll().orElseThrow();
            assertThat(second.batch().changes()).containsExactly(new Change(ValidationRootHintBatch.ChangeKind.UPDATE, key(2), key(3)));
            outbox.settle(second, accepted(second));
        }
    }

    @Test
    void ordinaryExtractionFailureIsLocalWhileAnotherBindingAndSourceProceed() {
        var cause = new IllegalStateException("fixture extractor fault"); var calls = new AtomicInteger();
        try (var bad = outbox("bad", 4, 4, 32768); var good = outbox("good", 4, 4, 32768)) {
            var tap = ValidationCaptureHintTap.wrap((batch, token) -> calls.incrementAndGet(),
                    List.of(new ValidationCaptureHintTap.Binding(bad, (kind, before, after) -> { throw cause; }), binding(good)), LIMITS);
            tap.onBatch(List.of(insert("orders", 1)), Optional.empty());
            assertThat(bad.failure().orElseThrow()).isSameAs(cause); assertThat(calls).hasValue(1);
            assertThat(good.snapshot().packets()).isEqualTo(1); assertThat(good.failure()).isEmpty();
        }
    }

    @Test
    void bufferFullEndsHintCoverageButDoesNotFailOrTruncateNormalDelivery() {
        var events = List.of(insert("orders", 1), insert("orders", 2), insert("orders", 3));
        var calls = new AtomicInteger();
        try (var outbox = outbox("one", 1, 1, 32768)) {
            var tap = ValidationCaptureHintTap.wrap((batch, token) -> {
                calls.incrementAndGet(); assertThat(batch).isSameAs(events);
            }, List.of(binding(outbox)), LIMITS);
            tap.onBatch(events, Optional.empty());
            assertThat(calls).hasValue(1); assertThat(outbox.failure().orElseThrow()).hasMessageContaining("hint.senderFull");
            assertThat(outbox.snapshot().packets()).isEqualTo(1);
            assertThat(outbox.poll()).isEmpty();
        }
    }

    @Test
    void eventBudgetRejectsWithoutTraversingAnOversizedSourceBatch() {
        List<Envelope> events = new AbstractList<>() {
            @Override public Envelope get(int index) { throw new AssertionError("oversized batch traversal"); }
            @Override public int size() { return 1001; }
        };
        var calls = new AtomicInteger();
        try (var outbox = outbox("one", 4, 4, 32768)) {
            var tap = ValidationCaptureHintTap.wrap((batch, token) -> {
                assertThat(batch).isSameAs(events); calls.incrementAndGet();
            }, List.of(binding(outbox)), LIMITS);
            tap.onBatch(events, Optional.empty());
            assertThat(calls).hasValue(1); assertThat(outbox.failure().orElseThrow()).hasMessageContaining("callbackEvents");
            assertThat(outbox.snapshot().packets()).isZero();
        }
    }

    @Test
    void callbackByteBudgetEndsCoverageWithoutASecondUnboundedExtraction() {
        var extracts = new AtomicInteger(); var calls = new AtomicInteger();
        try (var first = outbox("one", 4, 4, 32768); var second = outbox("two", 4, 4, 32768)) {
            var binding = new ValidationCaptureHintTap.Binding(first, (kind, before, after) -> {
                extracts.incrementAndGet(); return keys(kind, before, after);
            });
            var secondBinding = new ValidationCaptureHintTap.Binding(second, (kind, before, after) -> { throw new AssertionError("over-budget extraction"); });
            var tap = ValidationCaptureHintTap.wrap((batch, token) -> calls.incrementAndGet(), List.of(binding, secondBinding),
                    new ValidationCaptureHintTap.Limits(1000, 4, 1));
            tap.onBatch(List.of(insert("orders", 1)), Optional.empty());
            assertThat(extracts).hasValue(1); assertThat(calls).hasValue(1);
            assertThat(first.failure().orElseThrow()).hasMessageContaining("callbackKeyBytes");
            assertThat(second.failure().orElseThrow()).hasMessageContaining("callbackKeyBytes");
        }
    }

    @Test
    void packetByteBudgetSplitsBeforeTheOutboxHardLimit() {
        try (var outbox = outbox("one", 8, 4, 4096)) {
            var key = new ValidationKey(List.of(ValidationKey.Part.text("x".repeat(300))));
            var textSender = new ValidationRootHintBatch.Sender("text", "execution", "source", "orders", "capture", "boot",
                    1, 1, "route", "key-contract", "mapping", "output", List.of(new ValidationRootHintBatch.Domain(ValidationKey.Kind.TEXT)));
            try (var text = new ValidationRootHintOutbox(outbox.run(), textSender, outbox.limits(), () -> 0)) {
                var tap = ValidationCaptureHintTap.wrap((batch, token) -> {}, List.of(new ValidationCaptureHintTap.Binding(text,
                        (kind, before, after) -> new Change(kind, null, key))), LIMITS);
                tap.onBatch(List.of(insert("orders", 1), insert("orders", 2), insert("orders", 3), insert("orders", 4)), Optional.empty());
                assertThat(text.failure()).isEmpty(); assertThat(text.snapshot().packets()).isGreaterThan(1);
                int received = 0;
                while (true) {
                    var next = text.poll(); if (next.isEmpty()) break;
                    var lease = next.orElseThrow(); received += lease.batch().changes().size();
                    assertThat(lease.batch().retainedBytes()).isLessThanOrEqualTo(text.limits().batchBytes());
                    text.settle(lease, accepted(lease));
                }
                assertThat(received).isEqualTo(4);
            }
        }
    }

    @Test
    void closedBindingsAndUnrelatedTablesNeverInvokeTheirExtractor() {
        try (var closed = outbox("closed", 4, 4, 32768); var other = outbox("other", 4, 4, 32768)) {
            closed.close();
            ValidationCaptureHintTap.Keys forbidden = (kind, before, after) -> { throw new AssertionError("unrelated extraction"); };
            var tap = ValidationCaptureHintTap.wrap((batch, token) -> {}, List.of(
                    new ValidationCaptureHintTap.Binding(closed, forbidden), new ValidationCaptureHintTap.Binding(other, forbidden)), LIMITS);
            tap.onBatch(List.of(insert("unrelated", 1)), Optional.empty());
            assertThat(closed.failure()).isEmpty(); assertThat(other.failure()).isEmpty();
            assertThat(other.snapshot().packets()).isZero();
        }
    }

    @Test
    void sourceDdlAndUnexpectedSnapshotRetireOnlyTheirAffectedBindings() {
        for (Envelope event : List.of(Envelope.ddl(0, "orders", Map.of()), Envelope.read(0, "orders", Map.of("id", 1L), Map.of()))) {
            var calls = new AtomicInteger();
            try (var outbox = outbox("one", 4, 4, 32768); var other = outbox("two", "other", 4, 4, 32768)) {
                var tap = ValidationCaptureHintTap.wrap((batch, token) -> calls.incrementAndGet(), List.of(binding(outbox), binding(other)), LIMITS);
                tap.onBatch(List.of(event, insert("other", 1)), Optional.empty());
                assertThat(calls).hasValue(1); assertThat(outbox.failure().orElseThrow()).isInstanceOf(TapstateException.class);
                assertThat(outbox.snapshot().packets()).isZero();
                assertThat(other.snapshot().packets()).isEqualTo(1); assertThat(other.failure()).isEmpty();
            }
        }
    }

    @Test
    void closeDuringExtractionCannotEnqueueLateKeysOrWaitForTheExtractor() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var calls = new AtomicInteger();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor(); var outbox = outbox("one", 4, 4, 32768)) {
            var tap = ValidationCaptureHintTap.wrap((batch, token) -> calls.incrementAndGet(),
                    List.of(new ValidationCaptureHintTap.Binding(outbox, (kind, before, after) -> {
                        entered.countDown();
                        try { if (!release.await(3, TimeUnit.SECONDS)) throw new AssertionError("fixture deadline"); }
                        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
                        return keys(kind, before, after);
                    })), LIMITS);
            var captured = executor.submit(() -> tap.onBatch(List.of(insert("orders", 1)), Optional.empty()));
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            try { executor.submit(outbox::close).get(1, TimeUnit.SECONDS); }
            finally { release.countDown(); }
            captured.get(2, TimeUnit.SECONDS);
            assertThat(calls).hasValue(1); assertThat(outbox.failure()).isEmpty();
            assertThat(outbox.snapshot().packets()).isZero(); assertThat(outbox.snapshot().bytes()).isZero();
        } finally { release.countDown(); }
    }

    @Test
    void originalListenerFailuresAreNeverSwallowedOrReplaced() {
        var original = new IllegalStateException("original source delivery failed");
        try (var outbox = outbox("one", 4, 4, 32768)) {
            var tap = ValidationCaptureHintTap.wrap((batch, token) -> { throw original; }, List.of(binding(outbox)), LIMITS);
            assertThatThrownBy(() -> tap.onBatch(List.of(insert("orders", 1)), Optional.empty())).isSameAs(original);
            assertThat(outbox.snapshot().packets()).isEqualTo(1);
        }
    }

    @Test
    void startingPositionAndAcknowledgementNotificationsKeepTheirOriginalContract() {
        var start = new SourcePosition("start"); var ack = new SourcePosition("ack");
        var failure = new IllegalStateException("source error"); var callbacks = new ArrayList<Object>();
        CaptureStartedListener original = new CaptureStartedListener() {
            @Override public void onStart(SourcePosition position) { callbacks.add(position); }
            @Override public void onBatch(List<Envelope> events, Optional<SourcePosition> position) { callbacks.add(events); }
            @Override public void onAcknowledged(SourcePosition position) { callbacks.add(position); }
            @Override public void onAcknowledgeFailed(Throwable problem) { callbacks.add(problem); }
            @Override public void onError(Throwable problem) { callbacks.add(problem); }
        };
        try (var outbox = outbox("one", 4, 4, 32768)) {
            var tap = ValidationCaptureHintTap.wrap(original, List.of(binding(outbox)), LIMITS);
            assertThat(tap).isInstanceOf(CaptureStartedListener.class);
            ((CaptureStartedListener) tap).onStart(start); tap.onAcknowledged(ack); tap.onAcknowledgeFailed(failure);
            assertThat(outbox.failure()).isEmpty();
            tap.onError(failure);
            assertThat(callbacks).containsExactly(start, ack, failure, failure);
            assertThat(outbox.failure().orElseThrow()).isSameAs(failure);
            var plain = ValidationCaptureHintTap.wrap((batch, token) -> {}, List.of(binding(outbox)), LIMITS);
            assertThat(plain).isNotInstanceOf(CaptureStartedListener.class);
        }
    }

    @Test
    void fatalExtractorErrorRemainsFatalAndFanoutCannotReuseOneOutboxTwice() {
        var fatal = new AssertionError("fatal key codec");
        try (var outbox = outbox("one", 4, 4, 32768)) {
            assertThatThrownBy(() -> ValidationCaptureHintTap.wrap((batch, token) -> {},
                    List.of(binding(outbox), binding(outbox)), LIMITS)).isInstanceOf(TapstateException.class);
            var tap = ValidationCaptureHintTap.wrap((batch, token) -> {}, List.of(new ValidationCaptureHintTap.Binding(outbox,
                    (kind, before, after) -> { throw fatal; })), LIMITS);
            assertThatThrownBy(() -> tap.onBatch(List.of(insert("orders", 1)), Optional.empty())).isSameAs(fatal);
            assertThat(outbox.failure().orElseThrow()).isSameAs(fatal);
        }
    }

    private static ValidationCaptureHintTap.Binding binding(ValidationRootHintOutbox outbox) {
        return new ValidationCaptureHintTap.Binding(outbox, ValidationCaptureHintTapTest::keys);
    }
    private static Change keys(ValidationRootHintBatch.ChangeKind kind, Map<String, Object> before, Map<String, Object> after) {
        return new Change(kind, before == null ? null : key(((Number) before.get("id")).longValue()),
                after == null ? null : key(((Number) after.get("id")).longValue()));
    }
    private static Envelope insert(String table, long id) { return Envelope.insert(0, table, Map.of("id", id), Map.of()); }
    private static ValidationKey key(long n) { return new ValidationKey(List.of(ValidationKey.Part.number(n))); }
    private static ValidationRootHintInbox.Receipt accepted(ValidationRootHintOutbox.Lease lease) {
        return new ValidationRootHintInbox.Receipt(lease.batch().run(), lease.batch().sender(), lease.batch().sequence(), ValidationRootHintInbox.Disposition.ACCEPTED);
    }
    private static ValidationRootHintOutbox outbox(String id, int packets, int changes, long batchBytes) {
        return outbox(id, "orders", packets, changes, batchBytes);
    }
    private static ValidationRootHintOutbox outbox(String id, String table, int packets, int changes, long batchBytes) {
        var run = new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "owner", "boot", 1, 1, 1),
                "task", id, ValidationMode.INCREMENTAL, "spec", 1, "rules");
        var sender = new ValidationRootHintBatch.Sender(id, "execution", "source", table, "capture", "boot", 1, 1,
                "route", "key-contract", "mapping", "output", List.of(new ValidationRootHintBatch.Domain(ValidationKey.Kind.EXACT_NUMBER)));
        return new ValidationRootHintOutbox(run, sender, new ValidationRootHintOutbox.Limits(packets, 1_048_576, changes, batchBytes, 2, 0, 100), () -> 0);
    }
}
