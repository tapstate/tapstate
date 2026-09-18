package io.tapstate.app;

import io.tapstate.spi.store.KeyedStateStore;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * The durable generation assigned to each chainless snapshot run of one pipeline.
 *
 * <p>A snapshot-only read deliberately opens no change chain: it has no tail, no resume position and no
 * durable delivery confirmation, so a resume conservatively reads it again. Its rows still enter stateful
 * operators whose state survives that rebuild. Giving every run the same made-up generation would make the
 * reread lose every strict ordering comparison against the state it is meant to refresh. This counter is
 * the missing order source: it rises once before each physical snapshot-only start and is kept with the
 * capture-side state a later drive reads again.
 *
 * <p>The lifecycle owner is the single writer for a pipeline. The state store therefore needs no new
 * compare-and-swap surface here; its ordinary durable keyed write has the same ownership contract as the
 * operator entries kept under the neighboring namespaces.
 */
final class SnapshotRunOrder {

    private static final String NAMESPACE_PREFIX = "snapshot.order.";
    private static final String KEY = "generation";

    private SnapshotRunOrder() {
    }

    /** Advances and durably returns the generation for the next physical run of {@code pipelineId}. */
    static long next(KeyedStateStore store, String pipelineId) {
        Objects.requireNonNull(store, "store");
        long next = Math.incrementExact(read(store, pipelineId, 0L));
        store.save(namespaceOf(pipelineId), KEY,
                Long.toString(next).getBytes(StandardCharsets.US_ASCII));
        return next;
    }

    /**
     * The generation already assigned to the run being assembled. One is the absent value so a DAG may
     * still be inspected without starting its capture first; with no buffered rows it orders nothing.
     */
    static long current(KeyedStateStore store, String pipelineId) {
        Objects.requireNonNull(store, "store");
        return read(store, pipelineId, 1L);
    }

    /** The namespace dropped with this pipeline's capture-side state when a stop asks to clear it. */
    static String namespaceOf(String pipelineId) {
        return NAMESPACE_PREFIX + Objects.requireNonNull(pipelineId, "pipelineId");
    }

    private static long read(KeyedStateStore store, String pipelineId, long absent) {
        return store.load(namespaceOf(pipelineId), KEY)
                .map(bytes -> Long.parseLong(new String(bytes, StandardCharsets.US_ASCII)))
                .map(generation -> {
                    if (generation < 1) {
                        throw new IllegalStateException(
                                "snapshot generation must be positive, got " + generation);
                    }
                    return generation;
                })
                .orElse(absent);
    }
}
