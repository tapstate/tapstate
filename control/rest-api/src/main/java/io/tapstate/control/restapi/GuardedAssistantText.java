package io.tapstate.control.restapi;

import io.tapstate.control.core.AiProviderService;
import io.tapstate.control.core.AiProviderClient;
import io.tapstate.control.core.ControlError;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.function.Consumer;

/** Coalesces ordered text and summary parts without blocking the provider on each token. */
final class GuardedAssistantText implements AiProviderClient.Output, AutoCloseable {
    private static final int MAX_PENDING_CHARS = 524288;
    private static final int MAX_PENDING_SEGMENTS = 1024;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final ArrayDeque<Segment> pending = new ArrayDeque<>();
    private int pendingChars;
    private final Runnable guard;
    private final Consumer<String> output;
    private final Consumer<AiProviderClient.Reasoning> reasoning;
    private final Thread producer = Thread.currentThread();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private final Object failureLock = new Object();
    private final Thread worker;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private boolean interruptedProducer;
    private boolean finished;
    private volatile boolean closed;

    GuardedAssistantText(Runnable guard, Consumer<String> output) {
        this(guard, output, part -> { });
    }

    GuardedAssistantText(Runnable guard, Consumer<String> output, Consumer<AiProviderClient.Reasoning> reasoning) {
        this.guard = guard;
        this.output = output;
        this.reasoning = reasoning;
        worker = Thread.ofVirtual().name("pipeline-assistant-text")
                .uncaughtExceptionHandler((thread, failed) -> fail(failed)).start(this::publish);
    }

    @Override public void accept(String text) {
        if (text.isEmpty()) return;
        enqueue(text, null);
    }

    @Override public void reasoning(AiProviderClient.Reasoning part) { enqueue(part.text(), part); }

    private void enqueue(String text, AiProviderClient.Reasoning part) {
        int size = text.length() + (part == null ? 0 : part.id().length() + 1);
        if (size > MAX_PENDING_CHARS) throw AiProviderService.error(ControlError.AI_PROVIDER_FAILED);
        acquire();
        try {
            while (!closed && failure.get() == null && (pendingChars + size > MAX_PENDING_CHARS
                    || pending.size() >= MAX_PENDING_SEGMENTS && !canAppend(part))) changed.await();
            throwIfFailed();
            if (closed || finished) throw new IllegalStateException("assistant text stream is closed");
            if (canAppend(part)) pending.getLast().text.append(text);
            else pending.addLast(new Segment(part, text));
            pendingChars += size;
            changed.signalAll();
        } catch (InterruptedException interrupted) { throw interrupted(); }
        finally { lock.unlock(); }
    }

    private boolean canAppend(AiProviderClient.Reasoning part) {
        Segment last = pending.peekLast();
        if (last == null) return false;
        if (part == null) return last.reasoning == null;
        return part.phase() == AiProviderClient.Reasoning.Phase.DELTA && last.reasoning != null
                && last.reasoning.phase() == part.phase() && Objects.equals(last.reasoning.id(), part.id());
    }

    private static final class Segment {
        private final AiProviderClient.Reasoning reasoning;
        private final StringBuilder text;
        private Segment(AiProviderClient.Reasoning reasoning, String text) {
            this.reasoning = reasoning;
            this.text = new StringBuilder(text);
        }
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
            Throwable failed = failure.get();
            if (failed != null && interruptedProducer && Thread.currentThread() == producer) {
                Thread.interrupted();
                interruptedProducer = false;
            }
            if (failed instanceof RuntimeException runtime) throw runtime;
            if (failed instanceof Error error) throw error;
        }
    }

    private void publish() {
        try {
            while (awaitText()) {
                // Keep the original workspace fence immediately before each released batch.
                guard.run();
                if (!publishBatch()) break;
            }
            drained.complete(null);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            fail(AiProviderService.error(ControlError.AI_PROVIDER_FAILED));
        } catch (RuntimeException failed) { fail(failed); }
    }

    private boolean awaitText() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (pending.isEmpty() && !finished && !closed) changed.await();
            return !closed && (!pending.isEmpty() || !finished);
        } finally { lock.unlock(); }
    }

    private boolean publishBatch() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            if (closed) return false;
            List<Segment> batch = List.copyOf(pending);
            pending.clear();
            pendingChars = 0;
            changed.signalAll();
            for (Segment segment : batch) {
                if (closed) return false;
                if (segment.reasoning == null) output.accept(segment.text.toString());
                else reasoning.accept(new AiProviderClient.Reasoning(segment.reasoning.id(),
                        segment.reasoning.phase(), segment.text.toString()));
            }
            return true;
        } finally { lock.unlock(); }
    }

    private void fail(Throwable failed) {
        lock.lock();
        try {
            synchronized (failureLock) {
                if (closed) { drained.complete(null); return; }
                failure.set(failed);
                if (!producer.isInterrupted()) {
                    interruptedProducer = true;
                    producer.interrupt();
                }
                drained.completeExceptionally(failed);
            }
            changed.signalAll();
        } finally { lock.unlock(); }
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
            try { pending.clear(); pendingChars = 0; changed.signalAll(); }
            finally { lock.unlock(); }
        }
    }
}
