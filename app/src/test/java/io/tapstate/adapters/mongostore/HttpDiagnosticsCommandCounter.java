package io.tapstate.adapters.mongostore;

import java.util.function.Consumer;

/** Counts starts without byte encoding, command bodies or stack capture. */
public final class HttpDiagnosticsCommandCounter implements AutoCloseable {
    private final Consumer<String> previous = MongoCommandObservation.CURRENT.get();
    private int count;
    public HttpDiagnosticsCommandCounter() { MongoCommandObservation.CURRENT.set(ignored -> count++); }
    public int count() { return count; }
    @Override public void close() {
        if (previous == null) MongoCommandObservation.CURRENT.remove(); else MongoCommandObservation.CURRENT.set(previous);
    }
}
