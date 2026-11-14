package io.tapstate.adapters.mongostore;

import java.util.*;

/** Test-only synchronous attribution. Never records database payloads or connection settings. */
public final class PipelineLaunchCommandProbe implements AutoCloseable {
    private final Map<String, Integer> counts = new TreeMap<>();
    private final Map<String, Long> nanos = new TreeMap<>();
    private String stage;
    private long started;
    private boolean requestBytes;

    public PipelineLaunchCommandProbe() {
        MongoCommandObservation.CURRENT.set(command -> {
            started = System.nanoTime(); requestBytes = true;
            var frames = StackWalker.getInstance().walk(stream -> stream
                    .filter(frame -> frame.getClassName().startsWith("io.tapstate."))
                    .map(frame -> frame.getClassName().substring(frame.getClassName().lastIndexOf('.') + 1) + "." + frame.getMethodName())
                    .toList());
            stage = frames.stream().anyMatch(value -> value.startsWith("StoreBackedDerivedSchemas.derivePublishedDraft")) ? "derive"
                    : frames.stream().anyMatch(value -> value.startsWith("MongoConfigurationDraftStore.publishWithGeneration")) ? "commit"
                    : frames.stream().anyMatch(value -> value.startsWith("ApplyService.")) ? "validate"
                    : frames.stream().anyMatch(value -> value.startsWith("PipelineDraftService.")) ? "draft"
                    : "other";
            String caller = frames.stream().filter(value -> value.startsWith("Mongo") && !value.startsWith("MongoCommandObservation"))
                    .findFirst().orElse("none");
            counts.merge(stage + ":" + command + ":" + caller, 1, Integer::sum);
        });
        MongoCommandObservation.BYTES.set(bytes -> {
            if (requestBytes) requestBytes = false;
            else nanos.merge(stage, System.nanoTime() - started, Long::sum);
        });
    }

    public Map<String, Integer> counts() { return Map.copyOf(counts); }
    public Map<String, Double> successfulCommandWallMillis() {
        var result = new TreeMap<String, Double>(); nanos.forEach((stage, elapsed) -> result.put(stage, elapsed / 1_000_000.0));
        return Map.copyOf(result);
    }
    @Override public void close() { MongoCommandObservation.CURRENT.remove(); MongoCommandObservation.BYTES.remove(); }
}
