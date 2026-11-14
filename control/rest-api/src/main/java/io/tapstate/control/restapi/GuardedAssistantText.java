package io.tapstate.control.restapi;

import io.tapstate.control.core.AiProviderService;
import io.tapstate.control.core.ControlError;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.function.Consumer;

/** Coalesces text while a publication check runs, without blocking the provider on each token. */
final class GuardedAssistantText implements Consumer<String>, AutoCloseable {
    private static final int MAX_PENDING_CHARS = 524288;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final StringBuilder pending = new StringBuilder();
    private final Runnable guard;
    private final Consumer<String> output;
    private final Thread producer = Thread.currentThread();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private final Object failureLock = new Object();
    private final Thread worker;
    private volatile Throwable failure;
    private boolean interruptedProducer;
    private boolean finished;
    private volatile boolean closed;

    GuardedAssistantText(Runnable guard, Consumer<String> output) {
        this.guard = guard;
        this.output = output;
        worker = Thread.ofVirtual().name("pipeline-assistant-text").start(this::publish);
    }

    @Override public void accept(String text) {
        if (text.isEmpty()) return;
        if (text.length() > MAX_PENDING_CHARS) throw AiProviderService.error(ControlError.AI_PROVIDER_FAILED);
        acquire();
        try {
            while (!closed && failure == null && pending.length() + text.length() > MAX_PENDING_CHARS) awaitPending();
            throwIfFailed();
            if (closed || finished) throw new IllegalStateException("assistant text stream is closed");
            pending.append(text);
            changed.signalAll();
        } finally { lock.unlock(); }
    }

    void finish() {
        acquire();
        try { finished = true; changed.signalAll(); }
        finally { lock.unlock(); }
        try { drained.get(); }
        catch (InterruptedException interrupted) { throw interrupted(); }
        catch (ExecutionException failed) { throwIfFailed(); throw new IllegalStateException(failed); }
    }

    void throwIfFailed() {
        synchronized (failureLock) {
            if (failure != null && interruptedProducer && Thread.currentThread() == producer) {
                Thread.interrupted();
                interruptedProducer = false;
            }
            if (failure instanceof RuntimeException runtime) throw runtime;
            if (failure instanceof Error error) throw error;
        }
    }

    private void publish() {
        try {
            while (true) {
                lock.lockInterruptibly();
                try {
                    while (pending.isEmpty() && !finished && !closed) changed.await();
                    if (closed || pending.isEmpty() && finished) break;
                } finally { lock.unlock(); }
                // Keep the original workspace fence immediately before each released batch.
                guard.run();
                lock.lockInterruptibly();
                try {
                    if (closed) break;
                    String batch = pending.toString();
                    pending.setLength(0);
                    changed.signalAll();
                    output.accept(batch);
                } finally { lock.unlock(); }
            }
            drained.complete(null);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            fail(AiProviderService.error(ControlError.AI_PROVIDER_FAILED));
        } catch (RuntimeException | Error failed) { fail(failed); }
    }

    private void fail(Throwable failed) {
        lock.lock();
        try {
            synchronized (failureLock) {
                if (closed) { drained.complete(null); return; }
                failure = failed;
                if (!producer.isInterrupted()) {
                    interruptedProducer = true;
                    producer.interrupt();
                }
                drained.completeExceptionally(failed);
            }
            changed.signalAll();
        } finally { lock.unlock(); }
    }

    private void awaitPending() {
        try { changed.await(); }
        catch (InterruptedException interrupted) { throw interrupted(); }
    }

    private void acquire() {
        try { lock.lockInterruptibly(); }
        catch (InterruptedException interrupted) { throw interrupted(); }
    }

    private java.io.UncheckedIOException interrupted() {
        Thread.currentThread().interrupt();
        throwIfFailed();
        return new java.io.UncheckedIOException(new java.io.InterruptedIOException("assistant stream cancelled"));
    }

    @Override public void close() {
        // Cancellation must reach a blocked writer before attempting to acquire its lock.
        synchronized (failureLock) { closed = true; }
        worker.interrupt();
        if (lock.tryLock()) {
            try { pending.setLength(0); changed.signalAll(); }
            finally { lock.unlock(); }
        }
    }
}
