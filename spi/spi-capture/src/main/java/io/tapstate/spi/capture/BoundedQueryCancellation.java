package io.tapstate.spi.capture;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/** Request-scoped cancellation for finite source reads. */
public final class BoundedQueryCancellation {

    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final CopyOnWriteArrayList<Runnable> callbacks = new CopyOnWriteArrayList<>();

    /** Cancels the query and invokes every registered connector stop hook once. */
    public void cancel() {
        if (!cancelled.compareAndSet(false, true)) {
            return;
        }
        for (Runnable callback : callbacks) {
            try {
                callback.run();
            } catch (RuntimeException ignored) {
                // Cancellation is best-effort; the query thread still owns final connector cleanup.
            }
        }
        callbacks.clear();
    }

    /** Whether cancellation has been requested. */
    public boolean isCancelled() {
        return cancelled.get();
    }

    /** Fails the current query path after observing cancellation. */
    public void throwIfCancelled() {
        if (isCancelled()) {
            throw new CancellationException("bounded source query was cancelled");
        }
    }

    /** Registers a connector stop hook, including the race where cancellation came first. */
    public Registration onCancel(Runnable callback) {
        Objects.requireNonNull(callback, "callback");
        if (cancelled.get()) {
            callback.run();
            return () -> { };
        }
        callbacks.add(callback);
        if (cancelled.get() && callbacks.remove(callback)) {
            callback.run();
        }
        return () -> callbacks.remove(callback);
    }

    @FunctionalInterface
    public interface Registration {
        void close();
    }
}
