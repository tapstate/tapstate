package io.tapstate.runtime.engine.validation;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationRootProcessorTest {
    @Test void backpressureKeepsOneOperationOrResultAndNeverResubmitsConsumedInput() throws Exception {
        var started = new AtomicInteger();
        var exited = new AtomicInteger();
        var unblockFirst = new CountDownLatch(1);
        var firstEntered = new CountDownLatch(1);
        try (var executor = executor()) {
            var outbox = new TestOutbox(new int[]{1}, 1);
            var processor = new ValidationRootProcessor(observation(1).run(), (observation, guard, bytes) -> {
                started.incrementAndGet();
                return ValidationRootWork.submit(observation, executor, (scope, active) -> {
                    if (scope.revision() == 1) { firstEntered.countDown(); await(unblockFirst); }
                    return ValidationRootWorkTest.result(scope);
                }, guard, () -> {}, exited::incrementAndGet, bytes);
            }, ignored -> {}, 32768);
            processor.init(outbox, new TestProcessorContext());
            var inbox = new TestInbox(); inbox.addAll(List.of(observation(1), observation(2), observation(3)));
            processor.process(0, inbox);
            await(firstEntered);
            assertThat(processor.isCooperative()).isTrue();
            assertThat(inbox.size()).isEqualTo(2);
            assertThat(started).hasValue(1);
            assertThat(processor.tryProcess()).isFalse();
            unblockFirst.countDown(); barrier(executor);
            processor.process(0, inbox); barrier(executor);
            processor.process(0, inbox);
            assertThat(inbox.size()).isEqualTo(1);
            assertThat(started).hasValue(2);
            assertThat(exited).hasValue(2);
            var revisions = new ArrayList<Long>();
            revisions.add(((ValidationRootCheck)outbox.queue(0).remove()).observation().revision()); outbox.reset();
            processor.process(0, inbox); barrier(executor);
            assertThat(inbox.isEmpty()).isTrue();
            assertThat(processor.complete()).isFalse();
            revisions.add(((ValidationRootCheck)outbox.queue(0).remove()).observation().revision()); outbox.reset();
            assertThat(processor.complete()).isTrue();
            revisions.add(((ValidationRootCheck)outbox.queue(0).remove()).observation().revision());
            assertThat(revisions).containsExactly(1L, 2L, 3L);
            assertThat(started).hasValue(3);
            assertThat(exited).hasValue(3);
            processor.close();
        } finally { unblockFirst.countDown(); }
    }

    @Test void lossOfCurrentAuthorityDiscardsReadyAndBackpressuredResults() throws Exception {
        for (boolean queued : List.of(false, true)) {
            var current = new AtomicBoolean(true);
            var lost = new TapstateException(ValidationError.OWNER_NOT_AUTHORIZED, Map.of("pipeline", "pipeline"), null);
            try (var executor = executor()) {
                var outbox = new TestOutbox(new int[]{1}, 1);
                if (queued) outbox.offer("occupied");
                var processor = processor(executor, observation -> { if (!current.get()) throw lost; }, outbox);
                var inbox = new TestInbox(); inbox.addAll(List.of(observation(1))); processor.process(0, inbox); barrier(executor);
                if (queued) assertThat(processor.tryProcess()).isFalse();
                current.set(false);
                assertThat(catchThrowable(processor::tryProcess)).isSameAs(lost);
                assertThat(outbox.queue(0)).allMatch(item -> item.equals("occupied"));
                assertThat(catchThrowable(processor::complete)).isSameAs(lost);
                processor.close();
            }
        }
    }

    @Test void closeDoesNotReleaseARunningWorkersReservationOrEmitItsLateResult() throws Exception {
        var entered = new CountDownLatch(1);
        var unblock = new CountDownLatch(1);
        var released = new AtomicInteger();
        var cancelled = new AtomicInteger();
        var holder = new AtomicReference<ValidationRootWork>();
        try (var executor = executor()) {
            var outbox = new TestOutbox(new int[]{1}, 1);
            var processor = new ValidationRootProcessor(observation(1).run(), (scope, guard, bytes) -> {
                var work = ValidationRootWork.submit(scope, executor, (observation, active) -> {
                    entered.countDown(); await(unblock); return ValidationRootWorkTest.result(observation);
                }, guard, cancelled::incrementAndGet, released::incrementAndGet, bytes);
                holder.set(work); return work;
            }, ignored -> {}, 32768);
            processor.init(outbox, new TestProcessorContext());
            var inbox = new TestInbox(); inbox.addAll(List.of(observation(1))); processor.process(0, inbox); await(entered);
            processor.close(); processor.close();
            assertThat(released).hasValue(0); assertThat(cancelled).hasValue(1);
            assertThat(holder.get().hasExited()).isFalse();
            unblock.countDown(); barrier(executor);
            assertThat(released).hasValue(1);
            assertThat(holder.get().hasExited()).isTrue();
            assertThatThrownBy(holder.get()::takeResult).isInstanceOf(TapstateException.class);
            assertThat(outbox.queue(0)).isEmpty();
        } finally { unblock.countDown(); }
    }

    @Test void wrongRunAndNonValidationInputsAreRejectedBeforeStartingAnyIo() throws Exception {
        for (Object input : List.of("production-event", new ValidationObservation(new ValidationRunIdentity(observation(1).run().owner(),
                "other-task", "other-run", ValidationMode.FULL, "spec", 1, "rules"), observation(1).root(), 1))) {
            var calls = new AtomicInteger();
            var processor = new ValidationRootProcessor(observation(1).run(), (scope, guard, bytes) -> { calls.incrementAndGet(); throw new AssertionError("unexpected work"); },
                    ignored -> {}, 32768);
            processor.init(new TestOutbox(new int[]{1}, 1), new TestProcessorContext());
            var inbox = new TestInbox(); inbox.addAll(List.of(input));
            assertThatThrownBy(() -> processor.process(0, inbox)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(ValidationError.INVALID_CONTRACT));
            assertThat(calls).hasValue(0);
            processor.close();
        }
    }

    @Test void workerFailureCannotBeReplacedByAnEmptySuccessfulCompletion() throws Exception {
        var failed = new TapstateException(ValidationError.READ_FAILED, Map.of("table", "source"), null);
        try (var executor = executor()) {
            var outbox = new TestOutbox(new int[]{1}, 1);
            var processor = new ValidationRootProcessor(observation(1).run(), (scope, guard, bytes) -> ValidationRootWork.submit(scope,
                    executor, (observation, active) -> { throw failed; }, guard, () -> {}, () -> {}, bytes), ignored -> {}, 32768);
            processor.init(outbox, new TestProcessorContext());
            var inbox = new TestInbox(); inbox.addAll(List.of(observation(1))); processor.process(0, inbox); barrier(executor);
            assertThat(catchThrowable(processor::complete)).isSameAs(failed);
            assertThat(outbox.queue(0)).isEmpty();
            processor.close();
        }
    }

    @Test void workerCompletionBetweenPollAndAuthorityCheckIsStillAValidPendingResult() throws Exception {
        var entered = new CountDownLatch(1);
        var unblock = new CountDownLatch(1);
        var finishDuringGuard = new AtomicBoolean();
        Thread processorThread = Thread.currentThread();
        try (var executor = executor()) {
            var outbox = new TestOutbox(new int[]{1}, 1);
            var processor = new ValidationRootProcessor(observation(1).run(), (scope, guard, bytes) -> ValidationRootWork.submit(scope, executor,
                    (observation, active) -> { entered.countDown(); await(unblock); return ValidationRootWorkTest.result(observation); },
                    guard, () -> {}, () -> {}, bytes), ignored -> {
                if (Thread.currentThread() == processorThread && finishDuringGuard.compareAndSet(true, false)) {
                    unblock.countDown();
                    try { barrier(executor); } catch (Exception error) { throw new AssertionError(error); }
                }
            }, 32768);
            processor.init(outbox, new TestProcessorContext());
            var inbox = new TestInbox(); inbox.addAll(List.of(observation(1))); processor.process(0, inbox); await(entered);
            finishDuringGuard.set(true);
            assertThat(processor.tryProcess()).isFalse();
            assertThat(processor.tryProcess()).isTrue();
            assertThat(outbox.queue(0)).singleElement().isInstanceOf(ValidationRootCheck.class);
            processor.close();
        } finally { unblock.countDown(); }
    }

    private static ValidationRootProcessor processor(ThreadPoolExecutor executor, java.util.function.Consumer<ValidationObservation> current,
            TestOutbox outbox) throws Exception {
        var processor = new ValidationRootProcessor(observation(1).run(), (scope, guard, bytes) -> ValidationRootWork.submit(scope,
                executor, (observation, active) -> ValidationRootWorkTest.result(observation), guard, () -> {}, () -> {}, bytes), current, 32768);
        processor.init(outbox, new TestProcessorContext()); return processor;
    }
    private static ThreadPoolExecutor executor() {
        return new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(2), new ThreadPoolExecutor.AbortPolicy());
    }
    private static ValidationObservation observation(long revision) { return ValidationRootWorkTest.observation(revision); }
    private static void barrier(ThreadPoolExecutor executor) throws Exception { executor.submit(() -> {}).get(5, TimeUnit.SECONDS); }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(5, TimeUnit.SECONDS)) throw new AssertionError("fixture latch timed out"); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
    }
}
