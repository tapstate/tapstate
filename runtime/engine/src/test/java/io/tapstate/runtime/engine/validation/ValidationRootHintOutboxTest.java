package io.tapstate.runtime.engine.validation;

import static io.tapstate.core.validation.ValidationRootHintBatch.*;
import static io.tapstate.runtime.engine.validation.ValidationRootHintOutbox.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationMode;
import io.tapstate.core.validation.ValidationOwner;
import io.tapstate.core.validation.ValidationRunIdentity;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ValidationRootHintOutboxTest {
    private static final Limits LIMITS = new Limits(4, 65536, 2, 32768, 2, 10, 100);

    @Test
    void lostReceiptRetriesTheSamePacketAndOwnerDoesNotRedirtyIt() {
        var clock = new AtomicLong();
        try (var outbox = outbox(clock, LIMITS); var queue = queue();
             var inbox = new ValidationRootHintInbox(queue, List.of(sender()), new ValidationRootHintInbox.Limits(1, 65536, 2, 32768))) {
            assertThat(outbox.tryOffer(move(1, 2))).isEqualTo(Offer.ACCEPTED);
            var first = outbox.poll().orElseThrow();
            assertThat(inbox.tryAccept(first.batch()).disposition()).isEqualTo(ValidationRootHintInbox.Disposition.ACCEPTED);
            assertThat(outbox.settle(first, null)).isEqualTo(Settlement.RETRY);
            clock.set(9); assertThat(outbox.poll()).isEmpty();
            clock.set(10); var retry = outbox.poll().orElseThrow();
            assertThat(retry.batch()).isSameAs(first.batch());
            var receipt = inbox.tryAccept(retry.batch());
            assertThat(receipt.disposition()).isEqualTo(ValidationRootHintInbox.Disposition.DUPLICATE);
            assertThat(outbox.settle(retry, receipt)).isEqualTo(Settlement.ACCEPTED);
            assertThat(outbox.snapshot().packets()).isZero(); assertThat(outbox.snapshot().bytes()).isZero();
            assertThat(queue.snapshot().roots()).isEqualTo(2); assertThat(queue.snapshot().revision()).isEqualTo(1);
            assertThat(outbox.tryOffer(move(2, 3))).isEqualTo(Offer.ACCEPTED);
            assertThat(outbox.poll().orElseThrow().batch().sequence()).isEqualTo(2);
        }
    }

    @Test
    void slowTransportDoesNotBlockCaptureOrCreateASecondPhysicalRequest() {
        var clock = new AtomicLong();
        try (var outbox = outbox(clock, LIMITS)) {
            outbox.tryOffer(move(1, 2)); var first = outbox.poll().orElseThrow();
            assertThat(outbox.tryOffer(move(2, 3))).isEqualTo(Offer.ACCEPTED);
            assertThat(outbox.poll()).isEmpty();
            assertThat(outbox.snapshot().packets()).isEqualTo(2);
            assertThat(outbox.settle(first, accepted(first))).isEqualTo(Settlement.ACCEPTED);
            var second = outbox.poll().orElseThrow();
            assertThat(second.batch().sequence()).isEqualTo(2);
            assertThat(outbox.settle(first, accepted(first))).isEqualTo(Settlement.ALREADY_SETTLED);
            assertThat(outbox.snapshot().inFlight()).isTrue();
            outbox.settle(second, accepted(second));
        }
    }

    @Test
    void fullBufferLatchesFailureWithoutCallbacksOnCaptureThread() {
        var clock = new AtomicLong(); var callback = new AtomicBoolean();
        try (var outbox = outbox(clock, new Limits(1, 65536, 2, 32768, 2, 10, 100))) {
            outbox.drained().thenRun(() -> callback.set(true));
            outbox.tryOffer(move(1, 2));
            assertThat(outbox.tryOffer(move(2, 3))).isEqualTo(Offer.FAILED);
            assertThat(outbox.failure().orElseThrow()).hasMessageContaining("hint.senderFull");
            assertThat(callback).isFalse();
            assertThat(outbox.snapshot().bytes()).isPositive();
            assertThat(outbox.poll()).isEmpty();
            assertThat(callback).isTrue(); assertThat(outbox.snapshot().bytes()).isZero();
        }
    }

    @Test
    void timeoutAndCloseRetainPhysicalRequestUntilActualSettlement() {
        var clock = new AtomicLong();
        try (var outbox = outbox(clock, LIMITS)) {
            outbox.tryOffer(move(1, 2)); var pending = outbox.poll().orElseThrow();
            outbox.tryOffer(move(2, 3));
            clock.set(100); assertThat(outbox.poll()).isEmpty();
            assertThat(outbox.failure().orElseThrow()).hasMessageContaining("hint.deliveryWindow");
            assertThat(outbox.snapshot().packets()).isEqualTo(1);
            assertThat(outbox.snapshot().bytes()).isPositive();
            outbox.close();
            assertThat(outbox.drained().toCompletableFuture()).isNotDone();
            assertThat(outbox.settle(pending, accepted(pending))).isEqualTo(Settlement.FAILED);
            assertThat(outbox.snapshot().bytes()).isZero();
            assertThat(outbox.drained().toCompletableFuture()).isCompleted();
        }
    }

    @Test
    void retriesConsumeOriginalAttemptAndTimeBudgets() {
        var clock = new AtomicLong();
        try (var outbox = outbox(clock, LIMITS)) {
            outbox.tryOffer(move(1, 2)); var first = outbox.poll().orElseThrow();
            outbox.settle(first, null); clock.set(10);
            var retry = outbox.poll().orElseThrow();
            assertThat(outbox.settle(retry, null)).isEqualTo(Settlement.FAILED);
            assertThat(outbox.failure().orElseThrow()).hasMessageContaining("hint.deliveryAttempts");
            assertThat(outbox.tryOffer(move(2, 3))).isEqualTo(Offer.FAILED);
        }
    }

    @Test
    void busyReceiptRetriesButOwnerFailureAndForeignReceiptsCannotAdvanceSequence() {
        var clock = new AtomicLong();
        try (var outbox = outbox(clock, LIMITS)) {
            outbox.tryOffer(move(1, 2)); var first = outbox.poll().orElseThrow();
            assertThat(outbox.settle(first, receipt(first, ValidationRootHintInbox.Disposition.BUSY))).isEqualTo(Settlement.RETRY);
            clock.set(10); var retry = outbox.poll().orElseThrow();
            assertThat(outbox.settle(retry, receipt(retry, ValidationRootHintInbox.Disposition.FAILED))).isEqualTo(Settlement.FAILED);
            assertThat(outbox.failure().orElseThrow()).hasMessageContaining("hint.rejectedDelivery");
        }
        try (var outbox = outbox(clock, LIMITS)) {
            outbox.tryOffer(move(1, 2)); var first = outbox.poll().orElseThrow();
            assertThat(outbox.settle(first, new ValidationRootHintInbox.Receipt(run(), sender(), 2,
                    ValidationRootHintInbox.Disposition.ACCEPTED))).isEqualTo(Settlement.FAILED);
            assertThat(outbox.failure().orElseThrow()).hasMessageContaining("hint.foreignReceipt");
        }
    }

    @Test
    void unknownDeliveryCannotOutliveTheOriginalWindowThroughRetries() {
        var clock = new AtomicLong();
        try (var outbox = outbox(clock, LIMITS)) {
            outbox.tryOffer(move(1, 2)); var first = outbox.poll().orElseThrow();
            clock.set(90); outbox.settle(first, null);
            clock.set(100); assertThat(outbox.poll()).isEmpty();
            assertThat(outbox.failure().orElseThrow()).hasMessageContaining("hint.deliveryWindow");
        }
    }

    @Test
    void senderByteAndChangeLimitsNeverAcceptAPartialMove() {
        var clock = new AtomicLong();
        try (var outbox = outbox(clock, new Limits(4, 1, 2, 32768, 2, 10, 100))) {
            assertThat(outbox.tryOffer(move(1, 2))).isEqualTo(Offer.FAILED);
            assertThat(outbox.snapshot().packets()).isZero(); assertThat(outbox.snapshot().bytes()).isZero();
            assertThat(outbox.failure().orElseThrow()).hasMessageContaining("hint.senderBytes");
        }
        try (var outbox = outbox(clock, LIMITS)) {
            assertThat(outbox.tryOffer(List.of(move(1, 2).getFirst(), move(2, 3).getFirst(), move(3, 4).getFirst()))).isEqualTo(Offer.FAILED);
            assertThat(outbox.snapshot().packets()).isZero();
        }
        assertThat(LIMITS.reservationBytes()).isGreaterThan(LIMITS.bytes());
    }

    @Test
    void lockContentionDoesNotWaitOrSilentlyDropAnOfferedBatch() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor();
             var outbox = new ValidationRootHintOutbox(run(), sender(), LIMITS, () -> {
                 entered.countDown(); await(release); return 0;
             })) {
            var first = executor.submit(() -> outbox.tryOffer(move(1, 2)));
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            try {
                assertThat(executor.submit(() -> outbox.tryOffer(move(2, 3))).get(1, TimeUnit.SECONDS)).isEqualTo(Offer.FAILED);
            } finally { release.countDown(); }
            assertThat(first.get(2, TimeUnit.SECONDS)).isEqualTo(Offer.FAILED);
            assertThat(outbox.failure().orElseThrow()).hasMessageContaining("hint.senderBusy");
            assertThat(outbox.snapshot().packets()).isZero();
        } finally { release.countDown(); }
    }

    @Test
    void sourceSideFailureAlreadyLatchedDuringPollPreventsDispatch() throws Exception {
        var calls = new AtomicInteger(); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor();
             var outbox = new ValidationRootHintOutbox(run(), sender(), LIMITS, () -> {
                 if (calls.incrementAndGet() == 2) { entered.countDown(); await(release); }
                 return 0;
             })) {
            outbox.tryOffer(move(1, 2));
            var polling = executor.submit(outbox::poll);
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            try { assertThat(outbox.tryOffer(move(2, 3))).isEqualTo(Offer.FAILED); }
            finally { release.countDown(); }
            assertThat(polling.get(2, TimeUnit.SECONDS)).isEmpty();
            assertThat(outbox.snapshot().inFlight()).isFalse();
            assertThat(outbox.snapshot().bytes()).isZero();
        } finally { release.countDown(); }
    }

    @Test
    void drainCallbackCanInspectTheOutboxFromAnotherThread() throws Exception {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor(); var outbox = outbox(new AtomicLong(), LIMITS)) {
            outbox.tryOffer(move(1, 2)); var pending = outbox.poll().orElseThrow();
            var callback = outbox.drained().thenApply(unused -> {
                try { return executor.submit(() -> outbox.snapshot().bytes()).get(1, TimeUnit.SECONDS); }
                catch (Exception failure) { throw new AssertionError(failure); }
            });
            outbox.close(); assertThat(callback.toCompletableFuture()).isNotDone();
            assertThat(outbox.settle(pending, null)).isEqualTo(Settlement.CLOSED);
            assertThat(callback.toCompletableFuture().get(2, TimeUnit.SECONDS)).isZero();
        }
    }

    @Test
    void laterFatalErrorSupersedesAnOrdinaryConcurrentFailure() throws Exception {
        var calls = new AtomicInteger(); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var fatal = new AssertionError("fatal poll clock");
        try (var executor = Executors.newVirtualThreadPerTaskExecutor();
             var outbox = new ValidationRootHintOutbox(run(), sender(), LIMITS, () -> {
                 if (calls.incrementAndGet() == 2) { entered.countDown(); await(release); throw fatal; }
                 return 0;
             })) {
            outbox.tryOffer(move(1, 2));
            var polling = executor.submit(outbox::poll);
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            Throwable ordinary;
            try {
                assertThat(outbox.tryOffer(move(2, 3))).isEqualTo(Offer.FAILED);
                ordinary = outbox.failure().orElseThrow();
            } finally { release.countDown(); }
            assertThatThrownBy(() -> polling.get(2, TimeUnit.SECONDS)).hasCause(fatal);
            assertThat(outbox.failure().orElseThrow()).isSameAs(fatal);
            assertThat(fatal.getSuppressed()).contains(ordinary);
        } finally { release.countDown(); }
    }

    @Test
    void fatalErrorsRemainUnwrappedAndLatchedForController() {
        var fatal = new AssertionError("clock invariant");
        try (var outbox = new ValidationRootHintOutbox(run(), sender(), LIMITS, () -> { throw fatal; })) {
            assertThatThrownBy(() -> outbox.tryOffer(move(1, 2))).isSameAs(fatal);
            assertThat(outbox.failure().orElseThrow()).isSameAs(fatal);
            assertThat(outbox.snapshot().closed()).isTrue();
        }
    }

    private static void await(CountDownLatch latch) {
        try { if (!latch.await(3, TimeUnit.SECONDS)) throw new AssertionError("latch deadline"); }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
    }
    private static ValidationRootHintInbox.Receipt accepted(Lease lease) { return receipt(lease, ValidationRootHintInbox.Disposition.ACCEPTED); }
    private static ValidationRootHintInbox.Receipt receipt(Lease lease, ValidationRootHintInbox.Disposition disposition) {
        return new ValidationRootHintInbox.Receipt(lease.batch().run(), lease.batch().sender(), lease.batch().sequence(), disposition);
    }
    private static List<Change> move(long before, long after) { return List.of(new Change(ChangeKind.UPDATE, key(before), key(after))); }
    private static ValidationKey key(long n) { return new ValidationKey(List.of(ValidationKey.Part.number(n))); }
    private static ValidationRootHintOutbox outbox(AtomicLong clock, Limits limits) {
        return new ValidationRootHintOutbox(run(), sender(), limits, clock::get);
    }
    private static ValidationCandidateQueue queue() {
        return new ValidationCandidateQueue(run(), new ValidationCandidateQueue.Limits(8, 65536, 4, 2, 0, 100), () -> 0);
    }
    private static Sender sender() {
        return new Sender("sender", "execution", "source", "orders", "capture", "boot", 1, 2,
                "route", "key-contract", "mapping", "output", List.of(new Domain(ValidationKey.Kind.EXACT_NUMBER)));
    }
    private static ValidationRunIdentity run() {
        return new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "owner", "owner-boot", 1, 1, 1),
                "task", "run", ValidationMode.INCREMENTAL, "spec", 1, "rules");
    }
}
