package io.tapstate.spi.store;

/** One original writer report's result after its owning transaction has completed. */
public record WriterProgressOutcome(boolean committed, RuntimeException failure) {
    public boolean committedOrThrow() {
        if (failure != null) throw failure;
        return committed;
    }
}
