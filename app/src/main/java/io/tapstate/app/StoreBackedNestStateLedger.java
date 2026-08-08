package io.tapstate.app;

import io.tapstate.runtime.engine.nest.NestStateLedger;
import io.tapstate.spi.store.KeyedStateStore;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Where a nest's state paths are written down: the same store the state itself is in, so the record and
 * what it describes are lost and kept together. A record kept anywhere else could outlive a store that
 * was wiped, and would then refuse a start that had nothing left to abandon.
 *
 * <p>One entry per nest node, in a namespace of the pipeline's own - which is what lets a pipeline being
 * taken down for good drop its record the same way it drops its state, by naming a namespace rather than
 * enumerating keys.
 *
 * <p>The paths are stored as one line each, sorted so that the bytes do not depend on the order the tree
 * was walked in. A path containing a newline would come back as two, and the comparison would then refuse
 * a start it should have allowed - which is the direction to fail in, and it fails saying so with both
 * sets printed rather than silently resuming onto state it cannot address.
 */
final class StoreBackedNestStateLedger implements NestStateLedger {

    private static final long serialVersionUID = 1L;

    /** The namespace a pipeline's nest records live in, kept apart from the namespaces its state lives in. */
    private static final String NAMESPACE_PREFIX = "nest.shape.";

    private static final String SEPARATOR = "\n";

    private final transient KeyedStateStore store;

    StoreBackedNestStateLedger(KeyedStateStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    @Override
    public Set<String> recall(String pipelineId, String nodeId) {
        return store.load(namespaceOf(pipelineId), nodeId)
                .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                .filter(recorded -> !recorded.isEmpty())
                .<Set<String>>map(recorded -> new LinkedHashSet<>(List.of(recorded.split(SEPARATOR, -1))))
                .orElseGet(Set::of);
    }

    @Override
    public void record(String pipelineId, String nodeId, Set<String> paths) {
        store.save(namespaceOf(pipelineId), nodeId,
                String.join(SEPARATOR, new TreeSet<>(paths)).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * The namespace this pipeline's records live in. Named here rather than spelled out by whoever needs
     * it, so that what writes the records and what drops them cannot come to disagree about where they are.
     */
    static String namespaceOf(String pipelineId) {
        return NAMESPACE_PREFIX + pipelineId;
    }
}
