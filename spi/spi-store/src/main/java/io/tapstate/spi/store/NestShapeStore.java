package io.tapstate.spi.store;

import java.util.Optional;
import java.util.Set;

/** One immutable set of state paths per Nest step in a fixed physical Pipeline generation. */
public interface NestShapeStore {
    Optional<Set<String>> read(String stepId);

    /** Returns the retained winner, or empty when this command published the candidate. */
    Optional<Set<String>> recordIfAbsent(String stepId, Set<String> paths);
}
