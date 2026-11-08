package io.tapstate.control.core;

import io.tapstate.core.lifecycle.NestTree;

/**
 * Compiles a nest step's tree as a start would, for a read made before the pipeline runs. Implemented where
 * the compiler and the discovered models live; a definition that does not compile refuses with the code the
 * compiler throws, exactly as a start would.
 */
public interface NestTreeReader {

    /** A reader for an assembly with no compiler in it; asking it anything is a wiring defect. */
    NestTreeReader NONE = (pipelineId, step) -> {
        throw new IllegalStateException("no nest compiler is wired into this process");
    };

    /** The compiled tree of {@code step} of {@code pipelineId}. */
    NestTree tree(String pipelineId, String step);
}
