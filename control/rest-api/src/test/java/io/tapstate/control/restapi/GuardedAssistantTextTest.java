package io.tapstate.control.restapi;

import io.tapstate.control.core.AiProviderService;
import io.tapstate.control.core.ControlError;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class GuardedAssistantTextTest {
    @Test void aClosedRequestCannotBeInterruptedByALateWorkerFailure() throws Exception {
        var failed = new CountDownLatch(1);
        var stream = new GuardedAssistantText(() -> {
            failed.countDown(); throw AiProviderService.error(ControlError.UNAUTHENTICATED);
        }, batch -> {});
        var failureField = GuardedAssistantText.class.getDeclaredField("failureLock");
        failureField.setAccessible(true);
        var workerField = GuardedAssistantText.class.getDeclaredField("worker");
        workerField.setAccessible(true);
        Thread worker = (Thread) workerField.get(stream);
        try {
            synchronized (failureField.get(stream)) {
                stream.accept("text");
                assertThat(failed.await(3, TimeUnit.SECONDS)).isTrue();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                while (worker.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) Thread.sleep(1);
                assertThat(worker.getState()).isEqualTo(Thread.State.BLOCKED);
                stream.close();
            }
            worker.join(3000);
            assertThat(worker.isAlive()).isFalse();
            assertThat(Thread.currentThread().isInterrupted()).isFalse();
        } finally { stream.close(); Thread.interrupted(); }
    }

    @Test void aLateAdmittedCallbackCannotWriteAfterTheTerminalFrame() throws Exception {
        var output = new java.io.ByteArrayOutputStream();
        var events = new PipelineAssistantController.AssistantStream(output, output::flush);
        var admitted = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var stream = new GuardedAssistantText(() -> {}, batch -> {
            admitted.countDown();
            boolean waiting = true;
            while (waiting) {
                try { release.await(); waiting = false; }
                catch (InterruptedException ignored) { }
            }
            events.event(java.util.Map.of("type", "text-delta", "id", "text", "delta", batch));
        });
        try {
            stream.accept("late text");
            assertThat(admitted.await(3, TimeUnit.SECONDS)).isTrue();
            stream.close();
            events.failure(ControlError.AI_PROVIDER_FAILED.code());
            release.countDown();
            stream.finish();
            assertThat(output.toString(java.nio.charset.StandardCharsets.UTF_8))
                    .contains("error", "[DONE]").doesNotContain("late text", "text-delta");
        } finally { release.countDown(); stream.close(); }
    }

    @Test void cancellationInterruptsABlockedPublicationAndReleasesTheProducer() throws Exception {
        var writing = new CountDownLatch(1);
        var interrupted = new CountDownLatch(1);
        var cleaned = new CountDownLatch(1);
        var outputBarrier = new CountDownLatch(1);
        var producer = Thread.ofPlatform().daemon().start(() -> {
            try (var stream = new GuardedAssistantText(() -> {}, batch -> {
                writing.countDown();
                try { outputBarrier.await(); }
                catch (InterruptedException cancelled) { interrupted.countDown(); }
            })) {
                stream.accept("prefix");
                assertThat(writing.await(3, TimeUnit.SECONDS)).isTrue();
                stream.finish();
            } catch (java.io.UncheckedIOException cancelled) {
                // Request cancellation exits without trying to publish another error frame.
            } catch (InterruptedException unexpected) {
                throw new AssertionError(unexpected);
            } finally { cleaned.countDown(); }
        });
        try {
            assertThat(writing.await(3, TimeUnit.SECONDS)).isTrue();
            producer.interrupt();
            assertThat(interrupted.await(3, TimeUnit.SECONDS)).as("cancellation reaches the blocked writer").isTrue();
            assertThat(cleaned.await(3, TimeUnit.SECONDS)).as("producer cleanup does not wait for client reads").isTrue();
        } finally { outputBarrier.countDown(); producer.interrupt(); producer.join(3000); }
    }

    @Test void slowAuthorizationCoalescesFragmentsWithoutBlockingTheProvider() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var checks = new AtomicInteger();
        var output = new ArrayList<String>();
        try (var stream = new GuardedAssistantText(() -> {
            checks.incrementAndGet(); entered.countDown(); await(release);
        }, output::add)) {
            try {
                stream.accept("first");
                assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
                for (int i = 0; i < 1000; i++) stream.accept(".");
                assertThat(output).isEmpty();
                release.countDown();
                stream.finish();
                assertThat(checks).hasValue(1);
                assertThat(output).containsExactly("first" + ".".repeat(1000));
            } finally { release.countDown(); }
        }
    }

    @Test void aRefusedBatchNeverPublishesAndPreservesTheOriginalFailure() {
        var refusal = AiProviderService.error(ControlError.UNAUTHENTICATED);
        var output = new ArrayList<String>();
        try (var stream = new GuardedAssistantText(() -> { throw refusal; }, output::add)) {
            stream.accept("private text");
            assertThatThrownBy(stream::finish).isSameAs(refusal);
            assertThat(output).isEmpty();
            assertThat(Thread.currentThread().isInterrupted()).as("the owned wake-up has been consumed").isFalse();
        }
    }

    @Test void closePreventsAnInFlightCheckFromPublishingLateText() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var exited = new CountDownLatch(1);
        var output = new ArrayList<String>();
        var stream = new GuardedAssistantText(() -> {
            entered.countDown();
            try { release.await(); } catch (InterruptedException ignored) { }
            exited.countDown();
        }, output::add);
        try {
            stream.accept("late text");
            assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
            stream.close();
            release.countDown();
            assertThat(exited.await(3, TimeUnit.SECONDS)).isTrue();
            stream.finish();
            assertThat(output).isEmpty();
        } finally { release.countDown(); stream.close(); }
    }

    private static void await(CountDownLatch latch) {
        try { latch.await(); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException(interrupted); }
    }
}
