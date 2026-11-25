package io.tapstate.adapters.mongostore;

import java.util.ArrayList;
import java.util.List;

/** Observes only the HTTP request thread through the existing driver diagnostics hook. */
public final class CloudLatencyCommandProbe implements AutoCloseable {
    private final List<String> commands = new ArrayList<>();
    private int bindingReads;
    private int completed;
    private boolean requestBytes;

    public CloudLatencyCommandProbe() {
        MongoCommandObservation.CURRENT.set(command -> {
            commands.add(command);
            requestBytes = false;
        });
        MongoCommandObservation.COLLECTIONS.set((command, collection) -> {
            if (command.equals("find") && collection.equals(SystemCollections.STORAGE_BINDING.collectionName()))
                bindingReads++;
        });
        MongoCommandObservation.BYTES.set(bytes -> {
            if (requestBytes) completed++;
            else requestBytes = true;
        });
    }

    public Observation observation() {
        return new Observation(commands.size(), completed, bindingReads);
    }

    public record Observation(int started, int completed, int bindingReads) {}

    @Override public void close() {
        MongoCommandObservation.CURRENT.remove();
        MongoCommandObservation.COLLECTIONS.remove();
        MongoCommandObservation.BYTES.remove();
    }
}
