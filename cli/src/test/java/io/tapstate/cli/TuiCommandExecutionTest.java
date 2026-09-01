package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class TuiCommandExecutionTest {

    private static final long TIMEOUT_SECONDS = 5;

    @Test
    void runsDispatchOffTheUiCallerAndPostsAnImmutableCompletion() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch released = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<TuiCommandExecution.Completion> received = new AtomicReference<>();
        AtomicReference<Thread> dispatchThread = new AtomicReference<>();
        AtomicReference<Thread> outputThread = new AtomicReference<>();
        AtomicReference<Thread> completionThread = new AtomicReference<>();
        TuiCommandExecution execution = new TuiCommandExecution(completion -> {
            received.set(completion);
            completionThread.set(Thread.currentThread());
            completed.countDown();
        });
        try {
            assertThat(execution.start("tui-1", () -> {
                dispatchThread.set(Thread.currentThread());
                started.countDown();
                await(released);
                return new CommandResult(true, Cli.EXIT_OK);
            }, () -> {
                outputThread.set(Thread.currentThread());
                return "ok";
            })).isTrue();

            assertThat(started.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
            assertThat(execution.isRunning()).isTrue();
            released.countDown();
            assertThat(completed.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
            TuiCommandExecution.Completion completion = received.get();
            assertThat(completion).isNotNull();
            assertThat(completion.operationId()).isEqualTo("tui-1");
            assertThat(completion.result()).isEqualTo(new CommandResult(true, Cli.EXIT_OK));
            assertThat(completion.output()).isEqualTo("ok");
            assertThat(completion.failure()).isNull();
            assertThat(dispatchThread.get()).isSameAs(completionThread.get());
            assertThat(outputThread.get()).isSameAs(completionThread.get());
            assertThat(completionThread.get()).isNotEqualTo(Thread.currentThread());
            assertThat(execution.isRunning()).isFalse();
        } finally {
            execution.close();
        }
    }

    @Test
    void rejectsASecondCommandWhileTheFirstDispatchIsRunning() throws Exception {
        CountDownLatch dispatchStarted = new CountDownLatch(1);
        CountDownLatch releaseDispatch = new CountDownLatch(1);
        CountDownLatch firstCompletion = new CountDownLatch(1);
        TuiCommandExecution execution = new TuiCommandExecution(completion -> firstCompletion.countDown());
        try {
            assertThat(execution.start("tui-1", () -> {
                dispatchStarted.countDown();
                await(releaseDispatch);
                return new CommandResult(true, Cli.EXIT_OK);
            }, () -> "first")).isTrue();
            assertThat(dispatchStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
            assertThat(execution.isRunning()).isTrue();
            assertThat(execution.start("tui-2",
                    () -> new CommandResult(true, Cli.EXIT_OK), () -> "second")).isFalse();

            releaseDispatch.countDown();
            assertThat(firstCompletion.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
            awaitNotRunning(execution);
        } finally {
            releaseDispatch.countDown();
            execution.close();
        }
    }

    @Test
    void keepsWorkerOwnedUntilCompletionIsPublished() throws Exception {
        CountDownLatch completionEntered = new CountDownLatch(1);
        CountDownLatch releaseCompletion = new CountDownLatch(1);
        TuiCommandExecution execution = new TuiCommandExecution(completion -> {
            completionEntered.countDown();
            await(releaseCompletion);
        });
        try {
            assertThat(execution.start("tui-1", () -> new CommandResult(true, Cli.EXIT_OK), () -> "first"))
                    .isTrue();
            assertThat(completionEntered.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

            assertThat(execution.isRunning()).isTrue();
            assertThat(execution.start("tui-2",
                    () -> new CommandResult(true, Cli.EXIT_OK), () -> "second")).isFalse();

            releaseCompletion.countDown();
            awaitNotRunning(execution);
        } finally {
            releaseCompletion.countDown();
            execution.close();
        }
    }

    @Test
    void publishesDispatchFailureAndStillCapturesOutput() throws Exception {
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<TuiCommandExecution.Completion> received = new AtomicReference<>();
        IllegalStateException failure = new IllegalStateException("dispatch failed");
        TuiCommandExecution execution = new TuiCommandExecution(completion -> {
            received.set(completion);
            completed.countDown();
        });
        try {
            assertThat(execution.start("tui-failure", () -> {
                throw failure;
            }, () -> "diagnostic")).isTrue();
            assertThat(completed.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

            assertThat(received.get().result()).isEqualTo(new CommandResult(true, Cli.EXIT_DIAGNOSTIC));
            assertThat(received.get().output()).isEqualTo("diagnostic");
            assertThat(received.get().failure()).isSameAs(failure);
        } finally {
            execution.close();
        }
    }

    @Test
    void publishesOutputFailureWhenDispatchSucceeds() throws Exception {
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<TuiCommandExecution.Completion> received = new AtomicReference<>();
        IllegalArgumentException failure = new IllegalArgumentException("output failed");
        TuiCommandExecution execution = new TuiCommandExecution(completion -> {
            received.set(completion);
            completed.countDown();
        });
        try {
            assertThat(execution.start("tui-output-failure",
                    () -> new CommandResult(true, Cli.EXIT_OK), () -> {
                        throw failure;
                    })).isTrue();
            assertThat(completed.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

            assertThat(received.get().result()).isEqualTo(new CommandResult(true, Cli.EXIT_OK));
            assertThat(received.get().output()).isEmpty();
            assertThat(received.get().failure()).isSameAs(failure);
        } finally {
            execution.close();
        }
    }

    @Test
    void retainsDispatchFailureAndSuppressesOutputFailure() throws Exception {
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<TuiCommandExecution.Completion> received = new AtomicReference<>();
        IllegalStateException dispatchFailure = new IllegalStateException("dispatch failed");
        IllegalArgumentException outputFailure = new IllegalArgumentException("output failed");
        TuiCommandExecution execution = new TuiCommandExecution(completion -> {
            received.set(completion);
            completed.countDown();
        });
        try {
            assertThat(execution.start("tui-both-failed", () -> {
                throw dispatchFailure;
            }, () -> {
                throw outputFailure;
            })).isTrue();
            assertThat(completed.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

            assertThat(received.get().failure()).isSameAs(dispatchFailure);
            assertThat(received.get().failure().getSuppressed()).containsExactly(outputFailure);
        } finally {
            execution.close();
        }
    }

    @Test
    void publishesInterruptedDispatchAsACompletion() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<TuiCommandExecution.Completion> received = new AtomicReference<>();
        TuiCommandExecution execution = new TuiCommandExecution(completion -> {
            received.set(completion);
            completed.countDown();
        });
        try {
            assertThat(execution.start("tui-interrupted", () -> {
                started.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException interrupted) {
                    return sneakyThrow(interrupted);
                }
                return new CommandResult(true, Cli.EXIT_OK);
            }, () -> "partial")).isTrue();
            assertThat(started.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

            execution.close();
            assertThat(completed.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
            assertThat(received.get().failure()).isInstanceOf(InterruptedException.class);
            assertThat(received.get().output()).isEqualTo("partial");
        } finally {
            execution.close();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("test worker was interrupted", interrupted);
        }
    }

    private static void awaitNotRunning(TuiCommandExecution execution) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (execution.isRunning() && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        assertThat(execution.isRunning()).isFalse();
    }

    @SuppressWarnings("unchecked")
    private static <T, E extends Throwable> T sneakyThrow(Throwable failure) throws E {
        throw (E) failure;
    }
}
