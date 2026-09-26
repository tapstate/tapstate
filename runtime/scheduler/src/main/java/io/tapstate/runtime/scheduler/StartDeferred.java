package io.tapstate.runtime.scheduler;

/** An internal admission result: no data-plane job was submitted and a later pass may retry. */
public final class StartDeferred extends RuntimeException {

    public enum Reason {
        CAPACITY,
        DEPENDENCY
    }

    private final Reason reason;

    public StartDeferred(Reason reason) {
        super("pipeline start is waiting for " + reason.name().toLowerCase(java.util.Locale.ROOT),
                null, false, false);
        this.reason = java.util.Objects.requireNonNull(reason, "reason");
    }

    public Reason reason() {
        return reason;
    }
}
