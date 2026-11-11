package io.tapstate.runtime.engine.validation;

import static io.tapstate.core.validation.ValidationRootHintBatch.*;
import static io.tapstate.runtime.engine.validation.ValidationRootHintInbox.Disposition.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationMode;
import io.tapstate.core.validation.ValidationOwner;
import io.tapstate.core.validation.ValidationRoot;
import io.tapstate.core.validation.ValidationRootHintBatch;
import io.tapstate.core.validation.ValidationRunIdentity;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ValidationRootHintInboxTest {
    private static final ValidationRootHintInbox.Limits LIMITS = new ValidationRootHintInbox.Limits(2, 65536, 2, 65536);

    @Test
    void movesAreAtomicAndDuplicateReceiptDoesNotRedirtyAnInFlightRoot() {
        try (var queue = queue(8); var inbox = new ValidationRootHintInbox(queue, List.of(sender()), LIMITS)) {
            var moved = batch(1, new Change(ChangeKind.UPDATE, key(1), key(2)));
            var receipt = inbox.tryAccept(moved);
            assertThat(receipt.disposition()).isEqualTo(ACCEPTED);
            assertThat(receipt.run()).isEqualTo(run("run")); assertThat(receipt.sender()).isEqualTo(sender());
            assertThat(receipt.sequence()).isEqualTo(1);
            var first = queue.poll().orElseThrow(); var second = queue.poll().orElseThrow();
            assertThat(first.observation().root().rootKey()).isEqualTo(key(1));
            assertThat(second.observation().root().rootKey()).isEqualTo(key(2));
            assertThat(inbox.tryAccept(moved).disposition()).isEqualTo(DUPLICATE);
            assertThat(queue.snapshot().revision()).isEqualTo(1);
            assertThat(queue.settle(first, ValidationCandidateQueue.Completion.SETTLED))
                    .isEqualTo(ValidationCandidateQueue.Settlement.REMOVED);
            queue.settle(second, ValidationCandidateQueue.Completion.SETTLED);
            assertThat(inbox.tryAccept(moved).disposition()).isEqualTo(DUPLICATE);
            assertThat(queue.snapshot().roots()).isZero();
        }
    }

    @Test
    void nextPacketKeepsNewDirtyWorkWhileOlderRetriesAreExplicitlyStale() {
        try (var queue = queue(8); var inbox = new ValidationRootHintInbox(queue, List.of(sender()), LIMITS)) {
            inbox.tryAccept(insert(1, 1)); var lease = queue.poll().orElseThrow();
            assertThat(inbox.tryAccept(insert(2, 1)).disposition()).isEqualTo(ACCEPTED);
            assertThat(inbox.tryAccept(insert(1, 99)).disposition()).isEqualTo(STALE);
            assertThat(queue.snapshot().roots()).isEqualTo(1);
            assertThat(queue.settle(lease, ValidationCandidateQueue.Completion.SETTLED))
                    .isEqualTo(ValidationCandidateQueue.Settlement.SUPERSEDED);
            assertThat(queue.poll().orElseThrow().observation().revision()).isEqualTo(2);
        }
    }

    @Test
    void missingPacketEndsCoverageAndCannotLaterBeFilledIn() {
        try (var queue = queue(8); var inbox = new ValidationRootHintInbox(queue, List.of(sender()), LIMITS)) {
            assertThat(inbox.tryAccept(insert(2, 2)).disposition()).isEqualTo(FAILED);
            assertThat(inbox.failure().toCompletableFuture().join()).hasMessageContaining("hint.sequenceGap");
            assertThat(inbox.tryAccept(insert(1, 1)).disposition()).isEqualTo(FAILED);
            assertThat(queue.snapshot().closed()).isTrue(); assertThat(queue.snapshot().revision()).isZero();
        }
    }

    @Test
    void changedContentUnderCurrentSequenceFailsInsteadOfBeingDeduplicated() {
        try (var queue = queue(8); var inbox = new ValidationRootHintInbox(queue, List.of(sender()), LIMITS)) {
            inbox.tryAccept(insert(1, 1));
            assertThat(inbox.tryAccept(insert(1, 2)).disposition()).isEqualTo(FAILED);
            assertThat(inbox.snapshot().failure()).hasMessageContaining("hint.conflictingRetry");
            assertThat(queue.snapshot().closed()).isTrue(); assertThat(queue.snapshot().revision()).isEqualTo(1);
        }
    }

    @Test
    void oldRunOwnerExecutionEpochAndRouteCannotEnterCurrentQueue() {
        try (var queue = queue(8); var inbox = new ValidationRootHintInbox(queue, List.of(sender()), LIMITS)) {
            var current = insert(1, 1);
            assertThat(inbox.tryAccept(new ValidationRootHintBatch(run("old"), sender(), 1, current.changes())).disposition()).isEqualTo(STALE);
            var owner = new ValidationOwner("cluster", "pipeline", "owner", "old-boot", 1, 1, 1);
            var other = new ValidationRunIdentity(owner, "task", "run", ValidationMode.INCREMENTAL, "spec", 1, "rules");
            assertThat(inbox.tryAccept(new ValidationRootHintBatch(other, sender(), 1, current.changes())).disposition()).isEqualTo(STALE);
            var scopes = List.of(sender("old", 1, "route"), sender("execution", 2, "route"), sender("execution", 1, "old-route"),
                    new Sender("sender", "execution", "source", "orders", "capture", "old-boot", 1, 2,
                            "route", "key-contract", "mapping", "output", sender().domains()),
                    new Sender("sender", "execution", "source", "different-table", "capture", "boot", 1, 2,
                            "route", "key-contract", "mapping", "output", sender().domains()));
            for (Sender scope : scopes) {
                assertThat(inbox.tryAccept(new ValidationRootHintBatch(run("run"), scope, 1, current.changes())).disposition()).isEqualTo(STALE);
            }
            assertThat(queue.snapshot().revision()).isZero(); assertThat(inbox.snapshot().failure()).isNull();
            assertThat(inbox.tryAccept(current).disposition()).isEqualTo(ACCEPTED);
        }
    }

    @Test
    void distinctRegisteredSendersHaveIndependentSequencesAndSharedRootDirtyRevisions() {
        var a = sender();
        var b = new Sender("second", "execution", "source", "orders", "other-capture", "boot", 1, 2,
                "route", "key-contract", "mapping", "output", a.domains());
        try (var queue = queue(8); var inbox = new ValidationRootHintInbox(queue, List.of(a, b), LIMITS)) {
            inbox.tryAccept(insert(1, 1));
            assertThat(inbox.tryAccept(new ValidationRootHintBatch(run("run"), b, 1, insert(1, 1).changes())).disposition()).isEqualTo(ACCEPTED);
            assertThat(queue.snapshot().roots()).isEqualTo(1); assertThat(queue.snapshot().revision()).isEqualTo(2);
            assertThat(inbox.tryAccept(insert(1, 1)).disposition()).isEqualTo(DUPLICATE);
        }
    }

    @Test
    void queueExhaustionRejectsBothSidesAndEndsCoverage() {
        try (var queue = queue(1); var inbox = new ValidationRootHintInbox(queue, List.of(sender()), LIMITS)) {
            assertThat(inbox.tryAccept(batch(1, new Change(ChangeKind.UPDATE, key(1), key(2)))).disposition()).isEqualTo(FAILED);
            assertThat(inbox.snapshot().failure()).hasMessageContaining("hint.candidateFull");
            assertThat(queue.snapshot().revision()).isZero(); assertThat(queue.snapshot().roots()).isZero();
        }
    }

    @Test
    void configuredBatchBudgetAndRegisteredEnvelopeAreHardBounds() {
        try (var queue = queue(8)) {
            assertThatThrownBy(() -> new ValidationRootHintInbox(queue, List.of(sender(), sender()), LIMITS))
                    .isInstanceOf(TapstateException.class).hasMessageContaining("hint.duplicateSender");
            assertThatThrownBy(() -> new ValidationRootHintInbox(queue, List.of(sender()),
                    new ValidationRootHintInbox.Limits(1, 1, 2, 65536)))
                    .isInstanceOf(TapstateException.class).hasMessageContaining("hint.registrationBytes");
            try (var inbox = new ValidationRootHintInbox(queue, List.of(sender()),
                    new ValidationRootHintInbox.Limits(1, 65536, 1, 65536))) {
                assertThat(inbox.tryAccept(batch(1, new Change(ChangeKind.INSERT, null, key(1)),
                        new Change(ChangeKind.INSERT, null, key(2)))).disposition()).isEqualTo(FAILED);
                assertThat(inbox.snapshot().failure()).hasMessageContaining("hint.batchBudget");
                assertThat(queue.snapshot().revision()).isZero();
            }
        }
    }

    @Test
    void contentionReturnsBusyWithoutAcceptingOrWaiting() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor();
             var queue = new ValidationCandidateQueue(run("run"), queueLimits(8), () -> {
                 entered.countDown(); await(release); return 0;
             }); var inbox = new ValidationRootHintInbox(queue, List.of(sender()), LIMITS)) {
            var first = executor.submit(() -> inbox.tryAccept(insert(1, 1)));
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            try {
                assertThat(executor.submit(() -> inbox.tryAccept(insert(1, 1))).get(1, TimeUnit.SECONDS).disposition()).isEqualTo(BUSY);
            } finally { release.countDown(); }
            assertThat(first.get(2, TimeUnit.SECONDS).disposition()).isEqualTo(ACCEPTED);
            assertThat(inbox.tryAccept(insert(1, 1)).disposition()).isEqualTo(DUPLICATE);
        } finally { release.countDown(); }
    }

    @Test
    void busyCandidateQueueDoesNotConsumeSequenceAndCanRetryAfterContention() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor();
             var queue = new ValidationCandidateQueue(run("run"), queueLimits(8), () -> {
                 entered.countDown(); await(release); return 0;
             }); var inbox = new ValidationRootHintInbox(queue, List.of(sender()), LIMITS)) {
            var occupied = executor.submit(() -> queue.tryOffer(List.of(new ValidationRoot("other", "output", key(9), List.of()))));
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            try {
                assertThat(executor.submit(() -> inbox.tryAccept(insert(1, 1))).get(1, TimeUnit.SECONDS).disposition()).isEqualTo(BUSY);
                assertThat(inbox.snapshot().failure()).isNull();
            } finally { release.countDown(); }
            assertThat(occupied.get(2, TimeUnit.SECONDS)).isEqualTo(ValidationCandidateQueue.Offer.ACCEPTED);
            assertThat(inbox.tryAccept(insert(1, 1)).disposition()).isEqualTo(ACCEPTED);
            assertThat(inbox.tryAccept(insert(1, 1)).disposition()).isEqualTo(DUPLICATE);
            assertThat(queue.snapshot().revision()).isEqualTo(2);
        } finally { release.countDown(); }
    }

    @Test
    void failureCallbacksRunOutsideBothLocksAndLeasesRetainTheirCharge() throws Exception {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor(); var queue = queue(8);
             var inbox = new ValidationRootHintInbox(queue, List.of(sender()), LIMITS)) {
            inbox.tryAccept(insert(1, 1)); var lease = queue.poll().orElseThrow();
            var observed = inbox.failure().thenApply(problem -> {
                try { return executor.submit(() -> inbox.snapshot().closed() && queue.snapshot().closed()).get(1, TimeUnit.SECONDS); }
                catch (Exception failure) { throw new AssertionError(failure); }
            });
            inbox.tryAccept(insert(3, 3));
            assertThat(observed.toCompletableFuture().get(2, TimeUnit.SECONDS)).isTrue();
            assertThat(queue.snapshot().leased()).isEqualTo(1); assertThat(queue.snapshot().bytes()).isPositive();
            assertThat(queue.drained().toCompletableFuture()).isNotDone();
            queue.settle(lease, ValidationCandidateQueue.Completion.SETTLED);
            assertThat(queue.drained().toCompletableFuture()).isCompleted();
        }
    }

    @Test
    void invariantErrorsStayUnwrappedAndCloseFurtherAdmission() {
        var fatal = new AssertionError("clock invariant");
        try (var queue = new ValidationCandidateQueue(run("run"), queueLimits(8), () -> { throw fatal; });
             var inbox = new ValidationRootHintInbox(queue, List.of(sender()), LIMITS)) {
            assertThatThrownBy(() -> inbox.tryAccept(insert(1, 1))).isSameAs(fatal);
            assertThat(inbox.failure().toCompletableFuture().join()).isSameAs(fatal);
            assertThat(queue.snapshot().closed()).isTrue();
            assertThat(inbox.tryAccept(insert(1, 1)).disposition()).isEqualTo(FAILED);
        }
    }

    @Test
    void normalCloseCannotAdmitMoreAndDoesNotManufactureAFailure() {
        var queue = queue(8);
        var inbox = new ValidationRootHintInbox(queue, List.of(sender()), LIMITS);
        inbox.close(); inbox.close();
        assertThat(inbox.tryAccept(insert(1, 1)).disposition()).isEqualTo(CLOSED);
        assertThat(inbox.snapshot().failure()).isNull();
        assertThat(inbox.failure().toCompletableFuture()).isNotDone();
        assertThat(queue.snapshot().closed()).isTrue();
    }

    private static void await(CountDownLatch latch) {
        try { if (!latch.await(3, TimeUnit.SECONDS)) throw new AssertionError("latch deadline"); }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
    }
    private static ValidationRootHintBatch insert(long sequence, long value) {
        return batch(sequence, new Change(ChangeKind.INSERT, null, key(value)));
    }
    private static ValidationRootHintBatch batch(long sequence, Change... changes) {
        return new ValidationRootHintBatch(run("run"), sender(), sequence, List.of(changes));
    }
    private static ValidationKey key(long n) { return new ValidationKey(List.of(ValidationKey.Part.number(n))); }
    private static ValidationCandidateQueue queue(int roots) { return new ValidationCandidateQueue(run("run"), queueLimits(roots), () -> 0); }
    private static ValidationCandidateQueue.Limits queueLimits(int roots) { return new ValidationCandidateQueue.Limits(roots, 65536, 4, 2, 0, 100); }
    private static Sender sender() { return sender("execution", 1, "route"); }
    private static Sender sender(String execution, long epoch, String route) {
        return new Sender("sender", execution, "source", "orders", "capture", "boot", epoch, 2,
                route, "key-contract", "mapping", "output", List.of(new Domain(ValidationKey.Kind.EXACT_NUMBER)));
    }
    private static ValidationRunIdentity run(String id) {
        return new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "owner", "owner-boot", 1, 1, 1),
                "task", id, ValidationMode.INCREMENTAL, "spec", 1, "rules");
    }
}
