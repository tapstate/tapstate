package io.tapstate.adapters.mongostore;

import java.util.Map;
import java.util.TreeMap;

/** Counts complete synchronous driver commands, including aggregate and getMore, without retaining bodies. */
public final class CloudReadCommandProbe implements AutoCloseable {
    private final Map<String, Integer> commands = new TreeMap<>();
    private int completed;
    private boolean request;
    private long bytes;

    public CloudReadCommandProbe() {
        MongoCommandObservation.CURRENT.set(command -> {
            commands.merge(command, 1, Integer::sum);
            request = true;
        });
        MongoCommandObservation.BYTES.set(size -> {
            bytes += size;
            if (request) request = false;
            else completed++;
        });
    }

    public Observation observation() { return new Observation(Map.copyOf(commands), completed, bytes); }
    public record Observation(Map<String, Integer> commands, int completed, long bytes) {
        public int started() { return commands.values().stream().mapToInt(Integer::intValue).sum(); }
    }

    @Override public void close() {
        MongoCommandObservation.CURRENT.remove();
        MongoCommandObservation.BYTES.remove();
    }
}
