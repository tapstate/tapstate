package io.tapstate.core.validation;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** First ordinary diagnosis wins; an Error must still escape without being hidden by that diagnosis. */
public final class ValidationFailureLatch {
    private static final int MAX_SECONDARY_CAUSES = 8;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    /** Returns true only when this was the first recorded failure, not when an Error superseded one. */
    public boolean record(Throwable reported) {
        Objects.requireNonNull(reported, "reported");
        while (true) {
            Throwable previous = failure.get();
            if (previous == reported) return false;
            if (previous == null) {
                if (failure.compareAndSet(null, reported)) return true;
            } else if (reported instanceof Error && !(previous instanceof Error)) {
                if (failure.compareAndSet(previous, reported)) {
                    addContext(reported, previous);
                    return false;
                }
            } else {
                return false;
            }
        }
    }

    public Throwable get() { return failure.get(); }

    /** Bounded, identity-deduplicated context; never build direct cause/suppression cycles. */
    public static void addContext(Throwable primary, Throwable secondary) {
        Objects.requireNonNull(primary, "primary");
        if (secondary == null || primary == secondary || primary.getCause() == secondary
                || secondary.getCause() == primary) return;
        synchronized (primary) {
            Throwable[] existing = primary.getSuppressed();
            if (existing.length >= MAX_SECONDARY_CAUSES) return;
            for (Throwable previous : existing) if (previous == secondary) return;
            primary.addSuppressed(secondary);
        }
    }
}
