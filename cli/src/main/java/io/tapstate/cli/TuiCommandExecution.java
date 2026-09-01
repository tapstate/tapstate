package io.tapstate.cli;

import java.util.Objects;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Runs one command outside the terminal loop and returns only immutable completion data. */
final class TuiCommandExecution implements AutoCloseable {

    private static final Duration CLOSE_WAIT = Duration.ofSeconds(1);

    record Completion(String operationId, CommandResult result, String output, Throwable failure) {
        Completion {
            operationId = Objects.requireNonNull(operationId, "operation id is required");
            result = result == null ? new CommandResult(true, Cli.EXIT_DIAGNOSTIC) : result;
            output = output == null ? "" : output;
        }
    }

    private final Consumer<Completion> completionSink;
    private volatile Thread worker;

    TuiCommandExecution(Consumer<Completion> completionSink) {
        this.completionSink = Objects.requireNonNull(completionSink, "completion sink is required");
    }

    synchronized boolean start(String operationId, Supplier<CommandResult> dispatch, Supplier<String> output) {
        Objects.requireNonNull(operationId, "operation id is required");
        Objects.requireNonNull(dispatch, "dispatch is required");
        Objects.requireNonNull(output, "output is required");
        if (isRunning()) {
            return false;
        }
        Thread task = Thread.ofVirtual().unstarted(() -> {
            CommandResult result = null;
            Throwable failure = null;
            try {
                result = dispatch.get();
            } catch (Throwable thrown) {
                failure = thrown;
            }
            String captured;
            try {
                captured = output.get();
            } catch (Throwable thrown) {
                captured = "";
                if (failure == null) {
                    failure = thrown;
                } else if (failure != thrown) {
                    failure.addSuppressed(thrown);
                }
            }
            Completion completion = new Completion(operationId, result, captured, failure);
            try {
                completionSink.accept(completion);
            } finally {
                synchronized (TuiCommandExecution.this) {
                    if (worker == Thread.currentThread()) {
                        worker = null;
                    }
                }
            }
        });
        worker = task;
        try {
            task.start();
        } catch (RuntimeException | Error failure) {
            worker = null;
            throw failure;
        }
        return true;
    }

    boolean isRunning() {
        return worker != null;
    }

    void interrupt() {
        Thread active = worker;
        if (active != null) {
            active.interrupt();
        }
    }

    @Override
    public void close() {
        Thread active = worker;
        if (active == null) {
            return;
        }
        active.interrupt();
        try {
            active.join(CLOSE_WAIT);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
